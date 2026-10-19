import assert from "node:assert/strict";
import test from "node:test";

import { installAofWorker } from "./aof-worker.mjs";
import { encodeRequest } from "./aof-protocol.mjs";

// A fake MessagePort capturing posted completions and exposing a push helper.
function fakePort() {
  const sent = [];
  return {
    sent,
    started: false,
    onmessage: null,
    postMessage(message) {
      sent.push(message);
    },
    start() {
      this.started = true;
    },
    deliver(data) {
      return this.onmessage({ data });
    },
  };
}

// A fake OPFS backend recording calls in order.
function fakeBackend() {
  const calls = [];
  return {
    calls,
    async acquireLock() {
      calls.push(["acquireLock"]);
      return { type: "lock_acquired" };
    },
    async openActiveIncrement(generation, filename) {
      calls.push(["openActiveIncrement", generation, filename]);
      return { type: "opened", size: 0 };
    },
    append(sequence, bytes) {
      calls.push(["append", sequence, Array.from(bytes)]);
      return { type: "appended", sequence, size: bytes.length };
    },
    flush(sequence) {
      calls.push(["flush", sequence]);
      return { type: "flushed", sequence };
    },
    close() {
      calls.push(["close"]);
      return { type: "closed" };
    },
  };
}

async function flush() {
  await new Promise((resolve) => setImmediate(resolve));
}

test("worker executes an append and acknowledges it", async () => {
  const scope = { onmessage: null };
  const backend = fakeBackend();
  installAofWorker(scope, backend);

  const port = fakePort();
  scope.onmessage({ data: { kind: "attach_runtime", port } });
  assert.equal(port.started, true);

  const payload = encodeRequest({
    type: "append",
    sequence: 5,
    bytes: new TextEncoder().encode("*1\r\n$4\r\nPING\r\n"),
  });
  await port.deliver({ operation: 11, service: "versal.aof", payload });
  await flush();

  assert.deepEqual(backend.calls.at(-1), [
    "append",
    5,
    Array.from(new TextEncoder().encode("*1\r\n$4\r\nPING\r\n")),
  ]);
  assert.deepEqual(port.sent.at(-1), { operation: 11, result: { type: "ack" } });
});

test("worker reports a decode failure as an error completion", async () => {
  const scope = { onmessage: null };
  const backend = fakeBackend();
  installAofWorker(scope, backend);
  const port = fakePort();
  scope.onmessage({ data: { kind: "attach_runtime", port } });

  await port.deliver({ operation: 7, service: "versal.aof", payload: new Uint8Array() });
  await flush();

  assert.equal(port.sent.at(-1).operation, 7);
  assert.equal(port.sent.at(-1).result.type, "error");
});

test("worker surfaces a backend fault as an error completion", async () => {
  const scope = { onmessage: null };
  const backend = fakeBackend();
  backend.append = () => {
    throw new Error("disk full");
  };
  installAofWorker(scope, backend);
  const port = fakePort();
  scope.onmessage({ data: { kind: "attach_runtime", port } });

  const payload = encodeRequest({
    type: "append",
    sequence: 1,
    bytes: new TextEncoder().encode("x"),
  });
  await port.deliver({ operation: 3, service: "versal.aof", payload });
  await flush();

  assert.deepEqual(port.sent.at(-1), {
    operation: 3,
    result: { type: "error", message: "disk full" },
  });
});

test("worker rejects an unsupported service", async () => {
  const scope = { onmessage: null };
  installAofWorker(scope, fakeBackend());
  const port = fakePort();
  scope.onmessage({ data: { kind: "attach_runtime", port } });

  await port.deliver({ operation: 9, service: "versal.other", payload: new Uint8Array([0x91, 0xcc, 0x00]) });
  await flush();

  assert.equal(port.sent.at(-1).result.type, "error");
});
