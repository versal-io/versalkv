import assert from "node:assert/strict";
import test from "node:test";

import { installVersalWorker } from "./worker.mjs";

function fakeScope() {
  let nextTimer = 1;
  const timers = new Map();
  const microtasks = [];
  const messages = [];
  return {
    messages,
    microtasks,
    timers,
    postMessage(message) {
      messages.push(message);
    },
    setTimeout(callback, delay) {
      const id = nextTimer++;
      timers.set(id, { callback, delay });
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
    queueMicrotask(callback) {
      microtasks.push(callback);
    },
  };
}

test("worker realizes runtime deadline without choosing semantic work", async () => {
  const scope = fakeScope();
  const inputs = [];
  const runtime = {
    dispatch(input, monotonicMs, unixMs) {
      inputs.push({ input, monotonicMs, unixMs });
      if (input.type === "execute") {
        return {
          outputs: [{ request_id: input.request_id, value: { type: "simple", value: "OK" } }],
          wait: { type: "event", deadline_ms: 150 },
        };
      }
      return { outputs: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  installVersalWorker(scope, async () => runtime, () => 1_000, () => 100);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  await scope.onmessage({ data: { id: 2, kind: "execute", argv: ["SET", "key", "value"] } });

  assert.deepEqual(inputs[0], {
    input: { type: "execute", request_id: 2, argv: ["SET", "key", "value"] },
    monotonicMs: 100,
    unixMs: 1_000,
  });
  assert.deepEqual(scope.messages[1], {
    id: 2,
    ok: true,
    value: { type: "simple", value: "OK" },
  });
  assert.equal(scope.timers.size, 1);
  const [{ callback, delay }] = scope.timers.values();
  assert.equal(delay, 50);

  callback();
  assert.deepEqual(inputs[1].input, { type: "wake" });
});

test("worker realizes immediate work on a fresh microtask", async () => {
  const scope = fakeScope();
  const inputs = [];
  const runtime = {
    dispatch(input) {
      inputs.push(input);
      if (input.type === "execute") {
        return { outputs: [], wait: { type: "immediate" } };
      }
      return {
        outputs: [{ request_id: 7, value: { type: "integer", value: 1 } }],
        wait: { type: "event", deadline_ms: null },
      };
    },
  };
  installVersalWorker(scope, async () => runtime, () => 0, () => 0);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  await scope.onmessage({ data: { id: 7, kind: "execute", argv: ["PING"] } });
  assert.equal(scope.microtasks.length, 1);
  assert.equal(scope.messages.length, 1);

  scope.microtasks.shift()();
  assert.deepEqual(inputs[1], { type: "wake" });
  assert.deepEqual(scope.messages[1], {
    id: 7,
    ok: true,
    value: { type: "integer", value: 1 },
  });
});

test("destroy sends shutdown and cancels the armed timer", async () => {
  const scope = fakeScope();
  const inputs = [];
  const runtime = {
    dispatch(input) {
      inputs.push(input);
      return {
        outputs: [],
        wait:
          input.type === "shutdown"
            ? { type: "stopped" }
            : { type: "event", deadline_ms: 10 },
      };
    },
  };
  installVersalWorker(scope, async () => runtime, () => 0, () => 0);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  await scope.onmessage({ data: { id: 2, kind: "execute", argv: ["PING"] } });
  assert.equal(scope.timers.size, 1);

  await scope.onmessage({ data: { id: 3, kind: "destroy" } });
  assert.deepEqual(inputs.at(-1), { type: "shutdown" });
  assert.equal(scope.timers.size, 0);
  assert.deepEqual(scope.messages.at(-1), { id: 3, ok: true });
});

// A fake OPFS backend recording durable appends/flushes in order. `files`
// seeds readFile() for recovery tests; absent entries read as null.
function fakeBackend(files = {}) {
  const calls = [];
  return {
    calls,
    async acquireLock() {
      calls.push(["acquireLock"]);
    },
    async readFile(filename) {
      calls.push(["readFile", filename]);
      const seeded = files[filename];
      return seeded ? Uint8Array.from(seeded) : null;
    },
    async openActiveIncrement(generation, filename) {
      calls.push(["open", generation, filename]);
    },
    append(sequence, bytes) {
      calls.push(["append", sequence, Array.from(bytes)]);
    },
    flush(sequence) {
      calls.push(["flush", sequence]);
    },
    close() {
      calls.push(["close"]);
    },
  };
}

test("worker persists a durable frame before releasing its reply", async () => {
  const scope = fakeScope();
  const frame = [42, 1, 2, 3];
  const runtime = {
    enabled: false,
    enable_persistence() {
      this.enabled = true;
    },
    dispatch(input) {
      if (input.type === "execute") {
        return {
          outputs: [
            { request_id: input.request_id, value: { type: "simple", value: "OK" }, durable_frame: frame },
          ],
          wait: { type: "event", deadline_ms: null },
        };
      }
      return { outputs: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  const backend = fakeBackend();
  installVersalWorker(scope, async () => runtime, () => 0, () => 0, () => backend);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  assert.equal(runtime.enabled, true);
  assert.deepEqual(backend.calls, [
    ["acquireLock"],
    ["readFile", "appendonly.aof.0000000001.base.rdb"],
    ["readFile", "appendonly.aof.0000000001.incr.aof"],
    ["open", 1, "appendonly.aof.0000000001.incr.aof"],
  ]);

  await scope.onmessage({ data: { id: 7, kind: "execute", argv: ["SET", "k", "v"] } });

  // The append and flush happen before the reply is posted.
  assert.deepEqual(backend.calls.slice(4), [
    ["append", 1, frame],
    ["flush", 1],
  ]);
  assert.deepEqual(scope.messages.at(-1), {
    id: 7,
    ok: true,
    value: { type: "simple", value: "OK" },
  });
});

test("worker replays the base RDB and increment on create", async () => {
  const scope = fakeScope();
  const replayed = [];
  const runtime = {
    enabled: false,
    enable_persistence() {
      this.enabled = true;
    },
    load_base_rdb(bytes) {
      replayed.push(["base", Array.from(bytes)]);
      return bytes.length;
    },
    replay_aof(bytes) {
      replayed.push(["aof", Array.from(bytes)]);
      return 1;
    },
    dispatch() {
      return { outputs: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  const backend = fakeBackend({
    "appendonly.aof.0000000001.base.rdb": [10, 20],
    "appendonly.aof.0000000001.incr.aof": [30, 40, 50],
  });
  installVersalWorker(scope, async () => runtime, () => 0, () => 0, () => backend);

  await scope.onmessage({ data: { id: 1, kind: "create" } });

  // Base RDB is replayed before the increment, then persistence is enabled.
  assert.deepEqual(replayed, [
    ["base", [10, 20]],
    ["aof", [30, 40, 50]],
  ]);
  assert.equal(runtime.enabled, true);
  assert.deepEqual(backend.calls, [
    ["acquireLock"],
    ["readFile", "appendonly.aof.0000000001.base.rdb"],
    ["readFile", "appendonly.aof.0000000001.incr.aof"],
    ["open", 1, "appendonly.aof.0000000001.incr.aof"],
  ]);
});

test("worker skips replay when no base or increment exists", async () => {
  const scope = fakeScope();
  const runtime = {
    enabled: false,
    enable_persistence() {
      this.enabled = true;
    },
    load_base_rdb() {
      throw new Error("load_base_rdb should not be called");
    },
    replay_aof() {
      throw new Error("replay_aof should not be called");
    },
    dispatch() {
      return { outputs: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  const backend = fakeBackend();
  installVersalWorker(scope, async () => runtime, () => 0, () => 0, () => backend);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  assert.equal(runtime.enabled, true);
});

test("worker maps a persistence failure to a MISCONF reply", async () => {
  const scope = fakeScope();
  const runtime = {
    enable_persistence() {},
    dispatch(input) {
      if (input.type === "execute") {
        return {
          outputs: [
            { request_id: input.request_id, value: { type: "simple", value: "OK" }, durable_frame: [1] },
          ],
          wait: { type: "event", deadline_ms: null },
        };
      }
      return { outputs: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  const backend = fakeBackend();
  backend.append = () => {
    throw new Error("quota exceeded");
  };
  installVersalWorker(scope, async () => runtime, () => 0, () => 0, () => backend);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  await scope.onmessage({ data: { id: 9, kind: "execute", argv: ["SET", "k", "v"] } });

  assert.deepEqual(scope.messages.at(-1), {
    id: 9,
    ok: true,
    value: { type: "error", value: "MISCONF AOF durable write failed: quota exceeded" },
  });
});

test("worker still serves commands when the OPFS backend is unavailable", async () => {
  const scope = fakeScope();
  const runtime = {
    enabled: false,
    enable_persistence() {
      this.enabled = true;
    },
    dispatch(input) {
      if (input.type === "execute") {
        return {
          outputs: [{ request_id: input.request_id, value: { type: "simple", value: "PONG" } }],
          wait: { type: "event", deadline_ms: null },
        };
      }
      return { outputs: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  installVersalWorker(scope, async () => runtime, () => 0, () => 0, () => {
    throw new Error("OPFS unavailable");
  });

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  assert.equal(runtime.enabled, false);
  await scope.onmessage({ data: { id: 2, kind: "execute", argv: ["PING"] } });
  assert.deepEqual(scope.messages.at(-1), {
    id: 2,
    ok: true,
    value: { type: "simple", value: "PONG" },
  });
});
