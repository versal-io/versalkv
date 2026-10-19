// OPFS storage backend for the AOF I/O worker (design §8, §10).
//
// This backend owns the exclusive OPFS handles for the AOF directory. It runs
// inside the dedicated AOF I/O worker where `createSyncAccessHandle` offers
// synchronous `write`/`flush`/`getSize`/`truncate`/`close`, matching the
// durable append contract without a `SharedArrayBuffer`.
//
// Phase 3 implements the single active-increment append path:
// AcquireLock, OpenActiveIncrement, Append, Flush, Close. The remaining
// catalog, rewrite, and manifest operations arrive in later phases.

const AOF_DIRECTORY = "versal-aof";
const WRITER_LOCK_FILE = "writer.lock";

// A storage fault mapped to an AofIoError variant the runtime understands.
export class AofStorageError extends Error {
  constructor(variant, message) {
    super(message ?? variant.type);
    this.name = "AofStorageError";
    this.variant = variant;
  }
}

function backendError(cause) {
  const message = String(cause?.message ?? cause);
  return new AofStorageError({ type: "backend", message }, message);
}

export class OpfsAofBackend {
  // `root` lets tests inject a fake directory handle; production resolves the
  // real OPFS root lazily on first use.
  constructor(root = null) {
    this.root = root;
    this.directory = null;
    this.lockHandle = null;
    this.activeHandle = null;
    this.activeFilename = null;
    this.activeSize = 0;
    this.highestAppended = 0;
    this.highestFlushed = 0;
  }

  async #resolveDirectory() {
    if (this.directory) return this.directory;
    try {
      const root = this.root ?? (await navigator.storage.getDirectory());
      this.directory = await root.getDirectoryHandle(AOF_DIRECTORY, { create: true });
      return this.directory;
    } catch (cause) {
      throw backendError(cause);
    }
  }

  async #openSyncHandle(filename, { create }) {
    const directory = await this.#resolveDirectory();
    try {
      const fileHandle = await directory.getFileHandle(filename, { create });
      return await fileHandle.createSyncAccessHandle();
    } catch (cause) {
      if (cause?.name === "NotFoundError") {
        throw new AofStorageError({ type: "file_missing", filename }, cause.message);
      }
      if (cause?.name === "QuotaExceededError") {
        throw new AofStorageError({ type: "quota_exceeded" }, cause.message);
      }
      throw backendError(cause);
    }
  }

  // Acquire the single-writer lock by holding an exclusive sync handle on the
  // lock file. A second tab/worker fails to create its own handle.
  async acquireLock() {
    if (this.lockHandle) return { type: "lock_acquired" };
    try {
      this.lockHandle = await this.#openSyncHandle(WRITER_LOCK_FILE, { create: true });
    } catch (cause) {
      if (cause instanceof AofStorageError && cause.variant.type === "backend") {
        // A busy sync handle surfaces as a backend fault; treat it as the
        // writer lock being held elsewhere.
        throw new AofStorageError({ type: "lock_unavailable" }, cause.message);
      }
      throw cause;
    }
    return { type: "lock_acquired" };
  }

  // Open (creating if absent) the active increment file for appending and
  // report its current size so the runtime can resume at the correct offset.
  async openActiveIncrement(_generation, filename) {
    if (this.activeHandle) {
      this.activeHandle.close();
      this.activeHandle = null;
    }
    this.activeHandle = await this.#openSyncHandle(filename, { create: true });
    this.activeFilename = filename;
    this.activeSize = this.activeHandle.getSize();
    return { type: "opened", size: this.activeSize };
  }

  // Append encoded frames at the current end of the active increment.
  append(sequence, bytes) {
    const handle = this.#requireActive();
    try {
      let written = 0;
      while (written < bytes.length) {
        const count = handle.write(bytes.subarray(written), { at: this.activeSize + written });
        if (count <= 0) {
          throw new AofStorageError(
            { type: "backend", message: "sync access handle wrote zero bytes" },
            "sync access handle wrote zero bytes",
          );
        }
        written += count;
      }
      this.activeSize += bytes.length;
      this.highestAppended = sequence;
      return { type: "appended", sequence, size: this.activeSize };
    } catch (cause) {
      throw this.#mapWriteFault(cause);
    }
  }

  // Persist previously appended bytes up to `sequence`.
  flush(sequence) {
    const handle = this.#requireActive();
    try {
      handle.flush();
      this.highestFlushed = sequence;
      return { type: "flushed", sequence };
    } catch (cause) {
      throw this.#mapWriteFault(cause);
    }
  }

  // Flush and release every handle so another writer may take the lock.
  close() {
    if (this.activeHandle) {
      try {
        this.activeHandle.flush();
      } catch {
        // Best-effort final flush; closing still proceeds.
      }
      this.activeHandle.close();
      this.activeHandle = null;
    }
    if (this.lockHandle) {
      this.lockHandle.close();
      this.lockHandle = null;
    }
    this.activeFilename = null;
    this.activeSize = 0;
    return { type: "closed" };
  }

  #requireActive() {
    if (!this.activeHandle) {
      throw new AofStorageError(
        { type: "backend", message: "no active increment file is open" },
        "no active increment file is open",
      );
    }
    return this.activeHandle;
  }

  #mapWriteFault(cause) {
    if (cause instanceof AofStorageError) return cause;
    if (cause?.name === "QuotaExceededError") {
      return new AofStorageError({ type: "quota_exceeded" }, cause.message);
    }
    return backendError(cause);
  }
}
