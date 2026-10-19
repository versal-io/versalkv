import { OpfsAofBackend } from "./aof-opfs.mjs";

// Fixed-width generation naming for the single active increment (design §8).
// The single-worker phase keeps one generation; later phases add rewrite.
const ACTIVE_GENERATION = 1;
const ACTIVE_INCREMENT = "appendonly.aof.0000000001.incr.aof";

export function installVersalWorker(
  scope = self,
  factory = () => new BrowserVersal(),
  wallNow = () => Date.now(),
  monotonicNow = () => performance.now(),
  backendFactory = () => new OpfsAofBackend(),
) {
  let versal = null;
  let timer = null;
  let backend = null;

  const cancelTimer = () => {
    if (timer === null) return;
    scope.clearTimeout(timer);
    timer = null;
  };

  // Release each reply, persisting its durable AOF frame first. Because the OPFS
  // sync access handle is already open, append+flush are synchronous and happen
  // before the reply is posted, so a reply never outruns its persisted write.
  const deliver = (step) => {
    for (const output of step.outputs) {
      if (output.durable_frame) {
        const error = persist(output.durable_frame);
        if (error) {
          scope.postMessage({
            id: output.request_id,
            ok: true,
            value: { type: "error", value: `MISCONF AOF durable write failed: ${error}` },
          });
          continue;
        }
      }
      scope.postMessage({ id: output.request_id, ok: true, value: output.value });
    }
  };

  // Append and flush a durable frame synchronously. Returns an error message on
  // failure, or null on success.
  const persist = (frame) => {
    if (!backend) return "persistence backend is not ready";
    try {
      const bytes = frame instanceof Uint8Array ? frame : Uint8Array.from(frame);
      const seq = (sequence += 1);
      backend.append(seq, bytes);
      backend.flush(seq);
      return null;
    } catch (error) {
      return String(error?.message ?? error);
    }
  };

  let sequence = 0;

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
        // Durable persistence is best-effort: if OPFS is unavailable the
        // runtime still serves commands as an in-memory instance.
        try {
          backend = backendFactory();
          await backend.acquireLock();
          await backend.openActiveIncrement(ACTIVE_GENERATION, ACTIVE_INCREMENT);
          versal.enable_persistence();
        } catch {
          backend = null;
        }
        scope.postMessage({ id: message.id, ok: true });
      } else if (message.kind === "execute") {
        if (!versal) throw new Error("Versal runtime has not been created");
        drive({ type: "execute", request_id: message.id, argv: message.argv });
      } else if (message.kind === "destroy") {
        if (versal) drive({ type: "shutdown" });
        if (backend) {
          try {
            backend.close();
          } catch {
            // Best-effort close; teardown proceeds regardless.
          }
        }
        versal = null;
        backend = null;
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
