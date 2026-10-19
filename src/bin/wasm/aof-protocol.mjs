// MessagePack codec for the host-neutral AOF I/O protocol (design §6.2).
//
// This mirrors `src/foundation/io/persistence/aof_io_codec.rs` byte-for-byte so
// the browser host can decode `AofIoRequest` payloads relayed from the runtime
// worker and encode `AofIoResult`/`AofIoError` replies the runtime can read.
//
// Each value is a MessagePack array whose first element is a u8 discriminant
// tag followed by the variant's fields in declaration order. The encoder
// reproduces the exact markers emitted by the Rust `rmp` crate: u8 as 0xcc,
// u64 as a full-width 0xcf, strings as fixstr/str8/str16/str32, and binary as
// bin8/bin16/bin32. AOF frame bytes travel as MessagePack `bin` so raw binary
// command frames are never forced through UTF-8.

export const RequestTag = Object.freeze({
  AcquireLock: 0,
  LoadCatalog: 1,
  OpenActiveIncrement: 2,
  Append: 3,
  Flush: 4,
  PrepareRewrite: 5,
  WriteRewriteChunk: 6,
  FinishRewriteFiles: 7,
  CommitManifest: 8,
  AbortRewrite: 9,
  GarbageCollect: 10,
  Close: 11,
});

export const ResultTag = Object.freeze({
  LockAcquired: 0,
  CatalogLoaded: 1,
  Opened: 2,
  Appended: 3,
  Flushed: 4,
  RewritePrepared: 5,
  RewriteChunkWritten: 6,
  RewriteFilesFinished: 7,
  ManifestCommitted: 8,
  RewriteAborted: 9,
  GarbageCollected: 10,
  Closed: 11,
});

export const ErrorTag = Object.freeze({
  LockUnavailable: 0,
  QuotaExceeded: 1,
  CatalogCorrupt: 2,
  FileMissing: 3,
  Backend: 4,
});

const FILE_KIND_BASE = 0;
const FILE_KIND_INCREMENT = 1;

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

class Writer {
  constructor() {
    this.chunks = [];
    this.length = 0;
  }

  push(bytes) {
    this.chunks.push(bytes);
    this.length += bytes.length;
  }

  byte(value) {
    this.push(Uint8Array.of(value & 0xff));
  }

  // Mirror rmp `write_array_len`: fixarray below 16, then array16/array32.
  arrayLen(count) {
    if (count < 16) {
      this.byte(0x90 | count);
    } else if (count < 0x10000) {
      this.byte(0xdc);
      this.push(u16(count));
    } else {
      this.byte(0xdd);
      this.push(u32(count));
    }
  }

  // Mirror rmp `write_u8`: always the 0xcc marker plus one byte.
  u8(value) {
    this.byte(0xcc);
    this.byte(value);
  }

  // Mirror rmp `write_u64`: always the 0xcf marker plus eight big-endian bytes.
  u64(value) {
    this.byte(0xcf);
    const view = new DataView(new ArrayBuffer(8));
    view.setBigUint64(0, BigInt(value), false);
    this.push(new Uint8Array(view.buffer));
  }

  // Mirror rmp `write_str`: smallest of fixstr/str8/str16/str32.
  str(value) {
    const bytes = utf8Encoder.encode(value);
    const len = bytes.length;
    if (len < 32) {
      this.byte(0xa0 | len);
    } else if (len < 0x100) {
      this.byte(0xd9);
      this.byte(len);
    } else if (len < 0x10000) {
      this.byte(0xda);
      this.push(u16(len));
    } else {
      this.byte(0xdb);
      this.push(u32(len));
    }
    this.push(bytes);
  }

  // Mirror rmp `write_bin`: bin8/bin16/bin32 (never the deprecated raw format).
  bin(bytes) {
    const len = bytes.length;
    if (len < 0x100) {
      this.byte(0xc4);
      this.byte(len);
    } else if (len < 0x10000) {
      this.byte(0xc5);
      this.push(u16(len));
    } else {
      this.byte(0xc6);
      this.push(u32(len));
    }
    this.push(bytes);
  }

  nil() {
    this.byte(0xc0);
  }

  finish() {
    const out = new Uint8Array(this.length);
    let offset = 0;
    for (const chunk of this.chunks) {
      out.set(chunk, offset);
      offset += chunk.length;
    }
    return out;
  }
}

function u16(value) {
  const view = new DataView(new ArrayBuffer(2));
  view.setUint16(0, value, false);
  return new Uint8Array(view.buffer);
}

function u32(value) {
  const view = new DataView(new ArrayBuffer(4));
  view.setUint32(0, value, false);
  return new Uint8Array(view.buffer);
}

class Reader {
  constructor(bytes) {
    this.bytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    this.offset = 0;
  }

  fault(message) {
    return new Error(`AOF I/O decode error: ${message}`);
  }

  take(count) {
    if (this.offset + count > this.bytes.length) {
      throw this.fault("payload ended before all bytes read");
    }
    const slice = this.bytes.subarray(this.offset, this.offset + count);
    this.offset += count;
    return slice;
  }

  marker() {
    if (this.offset >= this.bytes.length) {
      throw this.fault("payload ended before marker");
    }
    return this.bytes[this.offset++];
  }

  peek() {
    return this.offset < this.bytes.length ? this.bytes[this.offset] : null;
  }

  arrayLen() {
    const marker = this.marker();
    if ((marker & 0xf0) === 0x90) return marker & 0x0f;
    if (marker === 0xdc) return readU16(this.take(2));
    if (marker === 0xdd) return readU32(this.take(4));
    throw this.fault(`expected array marker, found 0x${marker.toString(16)}`);
  }

  u8() {
    const marker = this.marker();
    if (marker === 0xcc) return this.marker();
    if (marker < 0x80) return marker; // positive fixint, accepted for robustness
    throw this.fault(`expected u8 marker, found 0x${marker.toString(16)}`);
  }

  u64() {
    const marker = this.marker();
    if (marker < 0x80) return marker; // positive fixint
    if (marker === 0xcc) return this.marker();
    if (marker === 0xcd) return readU16(this.take(2));
    if (marker === 0xce) return readU32(this.take(4));
    if (marker === 0xcf) {
      const view = new DataView(this.take(8).slice().buffer);
      const value = view.getBigUint64(0, false);
      return value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
    }
    throw this.fault(`expected uint marker, found 0x${marker.toString(16)}`);
  }

  str() {
    const marker = this.marker();
    let len;
    if ((marker & 0xe0) === 0xa0) {
      len = marker & 0x1f;
    } else if (marker === 0xd9) {
      len = this.marker();
    } else if (marker === 0xda) {
      len = readU16(this.take(2));
    } else if (marker === 0xdb) {
      len = readU32(this.take(4));
    } else {
      throw this.fault(`expected str marker, found 0x${marker.toString(16)}`);
    }
    try {
      return utf8Decoder.decode(this.take(len));
    } catch {
      throw this.fault("string field is not valid UTF-8");
    }
  }

  bin() {
    const marker = this.marker();
    let len;
    if (marker === 0xc4) {
      len = this.marker();
    } else if (marker === 0xc5) {
      len = readU16(this.take(2));
    } else if (marker === 0xc6) {
      len = readU32(this.take(4));
    } else {
      throw this.fault(`expected bin marker, found 0x${marker.toString(16)}`);
    }
    // Copy so later reuse of the backing buffer cannot alias the payload.
    return this.take(len).slice();
  }

  nil() {
    const marker = this.marker();
    if (marker !== 0xc0) {
      throw this.fault(`expected nil marker, found 0x${marker.toString(16)}`);
    }
  }
}

function readU16(bytes) {
  return new DataView(bytes.slice().buffer).getUint16(0, false);
}

function readU32(bytes) {
  return new DataView(bytes.slice().buffer).getUint32(0, false);
}

function tagged(writer, tag, fieldCount) {
  writer.arrayLen(fieldCount + 1);
  writer.u8(tag);
}

function readTag(reader) {
  const len = reader.arrayLen();
  if (len === 0) {
    throw reader.fault("payload array is empty");
  }
  return { tag: reader.u8(), fields: len - 1 };
}

function expectFields(reader, found, expected) {
  if (found !== expected) {
    throw reader.fault(`expected ${expected} fields, found ${found}`);
  }
}

// Encode an AOF I/O request. The browser host only needs decode today; encode is
// provided for symmetric round-trip testing and forward use.
export function encodeRequest(request) {
  const writer = new Writer();
  switch (request.type) {
    case "acquire_lock":
      tagged(writer, RequestTag.AcquireLock, 0);
      break;
    case "load_catalog":
      tagged(writer, RequestTag.LoadCatalog, 0);
      break;
    case "open_active_increment":
      tagged(writer, RequestTag.OpenActiveIncrement, 2);
      writer.u64(request.generation);
      writer.str(request.filename);
      break;
    case "append":
      tagged(writer, RequestTag.Append, 2);
      writer.u64(request.sequence);
      writer.bin(request.bytes);
      break;
    case "flush":
      tagged(writer, RequestTag.Flush, 1);
      writer.u64(request.sequence);
      break;
    case "close":
      tagged(writer, RequestTag.Close, 0);
      break;
    default:
      throw new Error(`unsupported request type: ${request.type}`);
  }
  return writer.finish();
}

export function decodeRequest(bytes) {
  const reader = new Reader(bytes);
  const { tag, fields } = readTag(reader);
  switch (tag) {
    case RequestTag.AcquireLock:
      expectFields(reader, fields, 0);
      return { type: "acquire_lock" };
    case RequestTag.LoadCatalog:
      expectFields(reader, fields, 0);
      return { type: "load_catalog" };
    case RequestTag.OpenActiveIncrement:
      expectFields(reader, fields, 2);
      return {
        type: "open_active_increment",
        generation: reader.u64(),
        filename: reader.str(),
      };
    case RequestTag.Append:
      expectFields(reader, fields, 2);
      return { type: "append", sequence: reader.u64(), bytes: reader.bin() };
    case RequestTag.Flush:
      expectFields(reader, fields, 1);
      return { type: "flush", sequence: reader.u64() };
    case RequestTag.PrepareRewrite:
      expectFields(reader, fields, 3);
      return {
        type: "prepare_rewrite",
        generation: reader.u64(),
        baseFilename: reader.str(),
        incrementFilename: reader.str(),
      };
    case RequestTag.WriteRewriteChunk:
      expectFields(reader, fields, 3);
      return {
        type: "write_rewrite_chunk",
        generation: reader.u64(),
        offset: reader.u64(),
        bytes: reader.bin(),
      };
    case RequestTag.FinishRewriteFiles:
      expectFields(reader, fields, 1);
      return { type: "finish_rewrite_files", generation: reader.u64() };
    case RequestTag.CommitManifest:
      expectFields(reader, fields, 2);
      return { type: "commit_manifest", slot: reader.u8(), envelope: reader.bin() };
    case RequestTag.AbortRewrite:
      expectFields(reader, fields, 1);
      return { type: "abort_rewrite", generation: reader.u64() };
    case RequestTag.GarbageCollect: {
      expectFields(reader, fields, 1);
      const count = reader.arrayLen();
      const retained = [];
      for (let index = 0; index < count; index += 1) retained.push(reader.u64());
      return { type: "garbage_collect", retainedGenerations: retained };
    }
    case RequestTag.Close:
      expectFields(reader, fields, 0);
      return { type: "close" };
    default:
      throw reader.fault(`unknown request tag ${tag}`);
  }
}

// Encode an AOF I/O result reply the runtime worker will decode.
export function encodeResult(result) {
  const writer = new Writer();
  switch (result.type) {
    case "lock_acquired":
      tagged(writer, ResultTag.LockAcquired, 0);
      break;
    case "catalog_loaded":
      tagged(writer, ResultTag.CatalogLoaded, 1);
      encodeCatalogSnapshot(writer, result.snapshot);
      break;
    case "opened":
      tagged(writer, ResultTag.Opened, 1);
      writer.u64(result.size);
      break;
    case "appended":
      tagged(writer, ResultTag.Appended, 2);
      writer.u64(result.sequence);
      writer.u64(result.size);
      break;
    case "flushed":
      tagged(writer, ResultTag.Flushed, 1);
      writer.u64(result.sequence);
      break;
    case "closed":
      tagged(writer, ResultTag.Closed, 0);
      break;
    default:
      throw new Error(`unsupported result type: ${result.type}`);
  }
  return writer.finish();
}

function encodeCatalogSnapshot(writer, snapshot) {
  writer.arrayLen(3);
  writer.u64(snapshot.generation);
  if (snapshot.slot === null || snapshot.slot === undefined) {
    writer.nil();
  } else {
    writer.u8(snapshot.slot);
  }
  writer.arrayLen(snapshot.files.length);
  for (const file of snapshot.files) {
    writer.arrayLen(4);
    writer.str(file.filename);
    writer.u64(file.sequence);
    writer.u8(file.kind === "base" ? FILE_KIND_BASE : FILE_KIND_INCREMENT);
    writer.u64(file.size);
  }
}

// Encode an AOF I/O error reply the runtime worker will surface to the client.
export function encodeError(error) {
  const writer = new Writer();
  switch (error.type) {
    case "lock_unavailable":
      tagged(writer, ErrorTag.LockUnavailable, 0);
      break;
    case "quota_exceeded":
      tagged(writer, ErrorTag.QuotaExceeded, 0);
      break;
    case "catalog_corrupt":
      tagged(writer, ErrorTag.CatalogCorrupt, 0);
      break;
    case "file_missing":
      tagged(writer, ErrorTag.FileMissing, 1);
      writer.str(error.filename);
      break;
    case "backend":
      tagged(writer, ErrorTag.Backend, 1);
      writer.str(error.message);
      break;
    default:
      throw new Error(`unsupported error type: ${error.type}`);
  }
  return writer.finish();
}
