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

// A fake MessagePort that records what the runtime worker forwards and can push
// completions back into the worker's drive loop.
function fakeAofPort() {
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
      this.onmessage({ data });
    },
  };
}

test("worker forwards host requests and releases the reply on completion", async () => {
  const scope = fakeScope();
  const runtime = {
    enabled: false,
    enable_persistence() {
      this.enabled = true;
    },
    dispatch(input) {
      if (input.type === "execute") {
        return {
          outputs: [],
          host_requests: [{ operation: 1, service: "versal.aof", name: "request", payload: [1, 2, 3] }],
          wait: { type: "event", deadline_ms: null },
        };
      }
      if (input.type === "host_completion") {
        return {
          outputs: [{ request_id: 42, value: { type: "simple", value: "OK" } }],
          host_requests: [],
          wait: { type: "event", deadline_ms: null },
        };
      }
      return { outputs: [], host_requests: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  installVersalWorker(scope, async () => runtime, () => 0, () => 0);

  const port = fakeAofPort();
  scope.onmessage({ data: { kind: "attach_aof", port } });
  assert.equal(port.started, true);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  assert.equal(runtime.enabled, true);

  await scope.onmessage({ data: { id: 42, kind: "execute", argv: ["SET", "k", "v"] } });
  assert.deepEqual(port.sent, [
    { operation: 1, service: "versal.aof", name: "request", payload: [1, 2, 3] },
  ]);
  // No reply until the durable completion arrives.
  assert.equal(scope.messages.length, 1); // only the create ack

  port.deliver({ operation: 1, result: { type: "ack" } });
  assert.deepEqual(scope.messages.at(-1), {
    id: 42,
    ok: true,
    value: { type: "simple", value: "OK" },
  });
});

test("worker fails host requests locally when no AOF worker is attached", async () => {
  const scope = fakeScope();
  const completions = [];
  const runtime = {
    enable_persistence() {},
    dispatch(input) {
      if (input.type === "execute") {
        return {
          outputs: [],
          host_requests: [{ operation: 5, service: "versal.aof", name: "request", payload: [] }],
          wait: { type: "event", deadline_ms: null },
        };
      }
      if (input.type === "host_completion") {
        completions.push(input);
        return {
          outputs: [{ request_id: 9, value: { type: "error", value: "MISCONF" } }],
          host_requests: [],
          wait: { type: "event", deadline_ms: null },
        };
      }
      return { outputs: [], host_requests: [], wait: { type: "event", deadline_ms: null } };
    },
  };
  installVersalWorker(scope, async () => runtime, () => 0, () => 0);

  await scope.onmessage({ data: { id: 1, kind: "create" } });
  await scope.onmessage({ data: { id: 9, kind: "execute", argv: ["SET", "k", "v"] } });

  assert.equal(completions.length, 1);
  assert.equal(completions[0].operation, 5);
  assert.equal(completions[0].result.type, "error");
  assert.deepEqual(scope.messages.at(-1), {
    id: 9,
    ok: true,
    value: { type: "error", value: "MISCONF" },
  });
});
