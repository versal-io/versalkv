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
