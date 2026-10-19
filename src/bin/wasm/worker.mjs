export function installVersalWorker(
  scope = self,
  factory = () => new BrowserVersal(),
  wallNow = () => Date.now(),
  monotonicNow = () => performance.now(),
) {
  let versal = null;
  let timer = null;
  let aofPort = null;

  const cancelTimer = () => {
    if (timer === null) return;
    scope.clearTimeout(timer);
    timer = null;
  };

  const deliver = (step) => {
    for (const output of step.outputs) {
      scope.postMessage({ id: output.request_id, ok: true, value: output.value });
    }
  };

  // Relay durable host requests to the AOF I/O worker. Completions arrive
  // asynchronously and are fed back into drive() as fresh runtime input.
  const forward = (step) => {
    if (!step.host_requests || step.host_requests.length === 0) return;
    if (!aofPort) {
      // Without an attached AOF worker a durable request can never complete;
      // fail it locally so the withheld reply is still released.
      for (const request of step.host_requests) {
        drive({
          type: "host_completion",
          operation: request.operation,
          result: { type: "error", message: "AOF I/O worker is not attached" },
        });
      }
      return;
    }
    for (const request of step.host_requests) {
      aofPort.postMessage(request);
    }
  };

  const drive = (input) => {
    if (!versal) return;
    const step = versal.dispatch(input, monotonicNow(), wallNow());
    deliver(step);
    forward(step);
    cancelTimer();
    if (step.wait.type === "immediate") {
      scope.queueMicrotask(() => drive({ type: "wake" }));
    } else if (step.wait.type === "event" && step.wait.deadline_ms !== undefined && step.wait.deadline_ms !== null) {
      timer = scope.setTimeout(() => {
        timer = null;
        drive({ type: "wake" });
      }, Math.max(0, step.wait.deadline_ms - monotonicNow()));
    }
  };

  const attachAof = (port) => {
    aofPort = port;
    port.onmessage = (event) => {
      const message = event.data;
      drive({
        type: "host_completion",
        operation: message.operation,
        result: message.result,
      });
    };
    if (typeof port.start === "function") port.start();
    if (versal) versal.enable_persistence();
  };

  scope.onmessage = async (event) => {
    const message = event.data;
    try {
      if (message.kind === "attach_aof") {
        attachAof(message.port);
        return;
      }
      if (message.kind === "create") {
        versal = await factory();
        if (aofPort) versal.enable_persistence();
        scope.postMessage({ id: message.id, ok: true });
      } else if (message.kind === "execute") {
        if (!versal) throw new Error("Versal runtime has not been created");
        drive({ type: "execute", request_id: message.id, argv: message.argv });
      } else if (message.kind === "destroy") {
        if (versal) drive({ type: "shutdown" });
        versal = null;
        cancelTimer();
        scope.postMessage({ id: message.id, ok: true });
      } else {
        throw new Error(`Unsupported Versal worker request: ${message.kind}`);
      }
    } catch (error) {
      scope.postMessage({ id: message.id, ok: false, error: String(error?.message ?? error) });
    }
  };
}
