export function installVersalWorker(
  scope = self,
  factory = () => new BrowserVersal(),
  wallNow = () => Date.now(),
  monotonicNow = () => performance.now(),
) {
  let versal = null;
  let timer = null;

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

  const drive = (input) => {
    if (!versal) return;
    const step = versal.dispatch(input, monotonicNow(), wallNow());
    deliver(step);
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

  scope.onmessage = async (event) => {
    const message = event.data;
    try {
      if (message.kind === "create") {
        versal = await factory();
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
