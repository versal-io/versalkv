import assert from "node:assert/strict";
import test from "node:test";

import {
  decodeRequest,
  encodeRequest,
  encodeResult,
  encodeError,
} from "./aof-protocol.mjs";

function hex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function fromHex(text) {
  const bytes = new Uint8Array(text.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = parseInt(text.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

// Golden byte strings captured from the Rust `aof_io_codec` encoder (the `rmp`
// crate). These lock the JS codec to the exact wire form the runtime expects.
test("request encoding matches the Rust golden bytes", () => {
  assert.equal(hex(encodeRequest({ type: "acquire_lock" })), "91cc00");
  assert.equal(hex(encodeRequest({ type: "close" })), "91cc0b");
  assert.equal(
    hex(
      encodeRequest({
        type: "open_active_increment",
        generation: 7,
        filename: "appendonly.aof.0000000007.incr.aof",
      }),
    ),
    "93cc02cf0000000000000007d922617070656e646f6e6c792e616f662e303030303030303030372e696e63722e616f66",
  );
  assert.equal(
    hex(
      encodeRequest({
        type: "append",
        sequence: 42,
        bytes: new TextEncoder().encode("*1\r\n$4\r\nPING\r\n"),
      }),
    ),
    "93cc03cf000000000000002ac40e2a310d0a24340d0a50494e470d0a",
  );
  assert.equal(
    hex(
      encodeRequest({
        type: "append",
        sequence: 1,
        bytes: Uint8Array.of(0x00, 0x80, 0xff, 0xfe),
      }),
    ),
    "93cc03cf0000000000000001c4040080fffe",
  );
  assert.equal(hex(encodeRequest({ type: "flush", sequence: 9 })), "92cc04cf0000000000000009");
});

test("result encoding matches the Rust golden bytes", () => {
  assert.equal(hex(encodeResult({ type: "lock_acquired" })), "91cc00");
  assert.equal(
    hex(encodeResult({ type: "appended", sequence: 11, size: 2048 })),
    "93cc03cf000000000000000bcf0000000000000800",
  );
  assert.equal(hex(encodeResult({ type: "opened", size: 4096 })), "92cc02cf0000000000001000");
});

test("requests decode from the Rust golden bytes", () => {
  const open = decodeRequest(
    fromHex(
      "93cc02cf0000000000000007d922617070656e646f6e6c792e616f662e303030303030303030372e696e63722e616f66",
    ),
  );
  assert.deepEqual(open, {
    type: "open_active_increment",
    generation: 7,
    filename: "appendonly.aof.0000000007.incr.aof",
  });

  const append = decodeRequest(fromHex("93cc03cf000000000000002ac40e2a310d0a24340d0a50494e470d0a"));
  assert.equal(append.type, "append");
  assert.equal(append.sequence, 42);
  assert.deepEqual(Array.from(append.bytes), Array.from(new TextEncoder().encode("*1\r\n$4\r\nPING\r\n")));

  assert.deepEqual(decodeRequest(fromHex("91cc00")), { type: "acquire_lock" });
  assert.deepEqual(decodeRequest(fromHex("91cc0b")), { type: "close" });
  assert.deepEqual(decodeRequest(fromHex("92cc04cf0000000000000009")), {
    type: "flush",
    sequence: 9,
  });
});

test("append preserves non-UTF-8 payload bytes", () => {
  const payload = Uint8Array.of(0x00, 0x80, 0xff, 0xfe, 0x0d, 0x0a);
  const encoded = encodeRequest({ type: "append", sequence: 3, bytes: payload });
  const decoded = decodeRequest(encoded);
  assert.deepEqual(Array.from(decoded.bytes), Array.from(payload));
});

test("error encoding round-trips through the shared tags", () => {
  assert.equal(hex(encodeError({ type: "lock_unavailable" })), "91cc00");
  assert.equal(hex(encodeError({ type: "quota_exceeded" })), "91cc01");
  assert.equal(
    hex(encodeError({ type: "file_missing", filename: "a.aof" })),
    "92cc03a5612e616f66",
  );
});

test("decoding rejects malformed payloads", () => {
  assert.throws(() => decodeRequest(new Uint8Array()));
  assert.throws(() => decodeRequest(fromHex("91cc63"))); // unknown tag 99
  const truncated = encodeRequest({
    type: "append",
    sequence: 1,
    bytes: new TextEncoder().encode("frame"),
  });
  assert.throws(() => decodeRequest(truncated.subarray(0, truncated.length - 2)));
});
