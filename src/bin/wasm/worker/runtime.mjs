import { OpfsAofBackend } from "./aof-opfs.mjs";

// Fixed-width generation naming for the single active increment (design §8).
// The single-worker phase keeps one generation; later phases add rewrite.
const ACTIVE_GENERATION = 1;
const ACTIVE_BASE = "appendonly.aof.0000000001.base.rdb";
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

  // Release every reply immediately. Durable frames no longer gate replies:
  // the runtime hands each frame to the installed sink (`persist`) during
  // dispatch, and that append+flush happens best-effort without blocking the
  // reply, so a reply may now be posted before its frame reaches OPFS.
  const deliver = (step) => {
    for (const output of step.outputs) {
      scope.postMessage({ id: output.request_id, ok: true, value: output.value });
    }
  };

  // Durable-frame sink installed on the runtime. Appends and flushes a frame
  // synchronously; failures are logged and swallowed because the reply has
  // already been (or will be) released regardless.
  const persist = (frame) => {
    if (!backend) return;
    try {
      const bytes = frame instanceof Uint8Array ? frame : Uint8Array.from(frame);
      const seq = (sequence += 1);
      backend.append(seq, bytes);
      backend.flush(seq);
    } catch (error) {
      console.error("AOF durable write failed:", String(error?.message ?? error));
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

  // Startup recovery for a single generation (design §12). Replays the base
  // RDB preamble and the active increment into the runtime before persistence
  // is enabled, then reopens the increment for appending. Any failure
  // propagates so the caller can fall back to an in-memory instance.
  const recover = async () => {
    await backend.acquireLock();
    const base = await backend.readFile(ACTIVE_BASE);
    if (base && base.length > 0) {
      versal.load_base_rdb(base);
    }
    const increment = await backend.readFile(ACTIVE_INCREMENT);
    if (increment && increment.length > 0) {
      versal.replay_aof(increment);
    }
    await backend.openActiveIncrement(ACTIVE_GENERATION, ACTIVE_INCREMENT);
    // Install the durable-frame sink before enabling persistence so the first
    // write command's frame already has somewhere to go.
    versal.set_durable_sink(persist);
    versal.enable_persistence();
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
          await recover();
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
