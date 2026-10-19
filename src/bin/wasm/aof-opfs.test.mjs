import assert from "node:assert/strict";
import test from "node:test";

import { OpfsAofBackend, AofStorageError } from "./aof-opfs.mjs";

// A fake OPFS file backed by a growable byte array and a synchronous handle.
function fakeFile() {
  const file = { bytes: new Uint8Array(0), handleOpen: false };
  file.handle = {
    getSize() {
      return file.bytes.length;
    },
    write(chunk, options) {
      const at = options?.at ?? file.bytes.length;
      const end = at + chunk.length;
      if (end > file.bytes.length) {
        const grown = new Uint8Array(end);
        grown.set(file.bytes, 0);
        file.bytes = grown;
      }
      file.bytes.set(chunk, at);
      return chunk.length;
    },
    flush() {
      file.flushed = true;
    },
    truncate(size) {
      file.bytes = file.bytes.subarray(0, size);
    },
    close() {
      file.handleOpen = false;
    },
  };
  return file;
}

// A fake OPFS directory handle that vends fake files on demand.
function fakeDirectory() {
  const files = new Map();
  return {
    files,
    async getDirectoryHandle(_name) {
      return this;
    },
    async getFileHandle(name, options) {
      if (!files.has(name)) {
        if (!options?.create) {
          const error = new Error(`missing ${name}`);
          error.name = "NotFoundError";
          throw error;
        }
        files.set(name, fakeFile());
      }
      const file = files.get(name);
      return {
        async createSyncAccessHandle() {
          file.handleOpen = true;
          return file.handle;
        },
      };
    },
  };
}

test("acquire, open, append, flush, close persists bytes to the active file", async () => {
  const directory = fakeDirectory();
  const backend = new OpfsAofBackend(directory);

  await backend.acquireLock();
  const opened = await backend.openActiveIncrement(1, "appendonly.aof.0000000001.incr.aof");
  assert.deepEqual(opened, { type: "opened", size: 0 });

  const first = new TextEncoder().encode("*1\r\n$4\r\nPING\r\n");
  const appended = backend.append(1, first);
  assert.deepEqual(appended, { type: "appended", sequence: 1, size: first.length });

  const second = new TextEncoder().encode("*1\r\n$4\r\nPONG\r\n");
  backend.append(2, second);

  const flushed = backend.flush(2);
  assert.deepEqual(flushed, { type: "flushed", sequence: 2 });

  const file = directory.files.get("appendonly.aof.0000000001.incr.aof");
  const expected = new Uint8Array(first.length + second.length);
  expected.set(first, 0);
  expected.set(second, first.length);
  assert.deepEqual(Array.from(file.bytes), Array.from(expected));
  assert.equal(file.flushed, true);

  const closed = backend.close();
  assert.deepEqual(closed, { type: "closed" });
});

test("reopening an active increment resumes at its current size", async () => {
  const directory = fakeDirectory();
  const backend = new OpfsAofBackend(directory);
  await backend.acquireLock();
  await backend.openActiveIncrement(1, "appendonly.aof.0000000001.incr.aof");
  backend.append(1, new TextEncoder().encode("abc"));
  backend.close();

  const reopened = await backend.openActiveIncrement(1, "appendonly.aof.0000000001.incr.aof");
  assert.deepEqual(reopened, { type: "opened", size: 3 });
  backend.append(2, new TextEncoder().encode("de"));
  const file = directory.files.get("appendonly.aof.0000000001.incr.aof");
  assert.deepEqual(Array.from(file.bytes), Array.from(new TextEncoder().encode("abcde")));
});

test("appending without an open increment is a backend fault", async () => {
  const backend = new OpfsAofBackend(fakeDirectory());
  await backend.acquireLock();
  assert.throws(
    () => backend.append(1, new TextEncoder().encode("x")),
    (error) => error instanceof AofStorageError && error.variant.type === "backend",
  );
});
