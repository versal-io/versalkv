// AOF I/O worker handler (design §5, §6.4).
//
// Receives durable AOF requests relayed from the runtime worker over a
// transferred `MessagePort`, decodes the MessagePack payload, executes it
// against the OPFS backend, and posts a completion back. Completion shape
// matches the runtime host-completion contract: `{ operation, result }` where
// result is `{ type: "ack" | "data" | "error", ... }`.
//
// `installAofWorker` takes the backend as a dependency so tests can drive it
// with a fake OPFS backend and a fake port.

import { decodeRequest } from "./aof-protocol.mjs";
import { OpfsAofBackend, AofStorageError } from "./aof-opfs.mjs";

export function installAofWorker(scope = self, backend = new OpfsAofBackend()) {
  let runtimePort = null;

  const handleRequest = async (message) => {
    const { operation, service, payload } = message;
    try {
      if (service !== "versal.aof") {
        throw new Error(`unsupported host service: ${service}`);
      }
      const request = decodeRequest(payload);
      const result = await execute(backend, request);
      runtimePort.postMessage({ operation, result });
    } catch (cause) {
      runtimePort.postMessage({
        operation,
        result: { type: "error", message: errorMessage(cause) },
      });
    }
  };

  const attach = (port) => {
    runtimePort = port;
    port.onmessage = (event) => {
      void handleRequest(event.data);
    };
    if (typeof port.start === "function") port.start();
  };

  scope.onmessage = (event) => {
    const message = event.data;
    if (message?.kind === "attach_runtime") {
      attach(message.port);
    }
  };

  // Expose attach for environments where the port is delivered directly.
  return { attach };
}

async function execute(backend, request) {
  switch (request.type) {
    case "acquire_lock":
      await backend.acquireLock();
      return { type: "ack" };
    case "open_active_increment":
      await backend.openActiveIncrement(request.generation, request.filename);
      return { type: "ack" };
    case "append":
      backend.append(request.sequence, request.bytes);
      return { type: "ack" };
    case "flush":
      backend.flush(request.sequence);
      return { type: "ack" };
    case "close":
      backend.close();
      return { type: "ack" };
    default:
      throw new Error(`unsupported AOF request: ${request.type}`);
  }
}

function errorMessage(cause) {
  if (cause instanceof AofStorageError) {
    switch (cause.variant.type) {
      case "lock_unavailable":
        return "AOF writer lock unavailable";
      case "quota_exceeded":
        return "AOF storage quota exceeded";
      case "file_missing":
        return `AOF file missing: ${cause.variant.filename}`;
      default:
        return cause.variant.message ?? cause.message;
    }
  }
  return String(cause?.message ?? cause);
}
