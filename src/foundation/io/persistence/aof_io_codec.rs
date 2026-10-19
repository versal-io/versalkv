//! MessagePack codec for the host-neutral AOF I/O protocol.
//!
//! `AofIoRequest`/`AofIoResult`/`AofIoError` cross the runtime host boundary as
//! an opaque payload (design §6.2: `service = "versal.aof"`,
//! `operation = "request"`). The browser host relays that payload to the OPFS
//! worker over `postMessage`, so the wire form must be portable and
//! byte-exact across both ends.
//!
//! Each value is encoded as a MessagePack array whose first element is a `u8`
//! discriminant tag followed by the variant's fields in declaration order. AOF
//! payload bytes are carried as MessagePack `bin` so raw binary command frames
//! are never forced through UTF-8.
//!
//! This module stays free of `std::fs`, `wasm_bindgen`, DOM, and runtime-layer
//! types so it compiles on both the native and `wasm` targets.

use bytes::Bytes;
use rmp::decode::{read_array_len, read_bin_len, read_u64, read_u8};
use rmp::encode::{write_array_len, write_bin, write_u64, write_u8};

use super::aof_io::{
    AofCatalogFile, AofCatalogSnapshot, AofFileKind, AofIoError, AofIoRequest, AofIoResult,
    ManifestSlot, RewriteFiles,
};

const REQUEST_ACQUIRE_LOCK: u8 = 0;
const REQUEST_LOAD_CATALOG: u8 = 1;
const REQUEST_OPEN_ACTIVE_INCREMENT: u8 = 2;
const REQUEST_APPEND: u8 = 3;
const REQUEST_FLUSH: u8 = 4;
const REQUEST_PREPARE_REWRITE: u8 = 5;
const REQUEST_WRITE_REWRITE_CHUNK: u8 = 6;
const REQUEST_FINISH_REWRITE_FILES: u8 = 7;
const REQUEST_COMMIT_MANIFEST: u8 = 8;
const REQUEST_ABORT_REWRITE: u8 = 9;
const REQUEST_GARBAGE_COLLECT: u8 = 10;
const REQUEST_CLOSE: u8 = 11;

const RESULT_LOCK_ACQUIRED: u8 = 0;
const RESULT_CATALOG_LOADED: u8 = 1;
const RESULT_OPENED: u8 = 2;
const RESULT_APPENDED: u8 = 3;
const RESULT_FLUSHED: u8 = 4;
const RESULT_REWRITE_PREPARED: u8 = 5;
const RESULT_REWRITE_CHUNK_WRITTEN: u8 = 6;
const RESULT_REWRITE_FILES_FINISHED: u8 = 7;
const RESULT_MANIFEST_COMMITTED: u8 = 8;
const RESULT_REWRITE_ABORTED: u8 = 9;
const RESULT_GARBAGE_COLLECTED: u8 = 10;
const RESULT_CLOSED: u8 = 11;

const ERROR_LOCK_UNAVAILABLE: u8 = 0;
const ERROR_QUOTA_EXCEEDED: u8 = 1;
const ERROR_CATALOG_CORRUPT: u8 = 2;
const ERROR_FILE_MISSING: u8 = 3;
const ERROR_BACKEND: u8 = 4;

const SLOT_ZERO: u8 = 0;
const SLOT_ONE: u8 = 1;

const KIND_BASE: u8 = 0;
const KIND_INCREMENT: u8 = 1;

/// A failure while decoding an AOF I/O protocol payload. Decode errors indicate
/// a corrupt or incompatible host message rather than a storage fault.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AofIoDecodeError {
    message: String,
}

impl AofIoDecodeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl core::fmt::Display for AofIoDecodeError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "AOF I/O decode error: {}", self.message)
    }
}

/// Encode an `AofIoRequest` into a MessagePack byte vector.
pub fn encode_request(request: &AofIoRequest) -> Vec<u8> {
    let mut out = Vec::new();
    match request {
        AofIoRequest::AcquireLock => tagged(&mut out, REQUEST_ACQUIRE_LOCK, 0),
        AofIoRequest::LoadCatalog => tagged(&mut out, REQUEST_LOAD_CATALOG, 0),
        AofIoRequest::OpenActiveIncrement {
            generation,
            filename,
        } => {
            tagged(&mut out, REQUEST_OPEN_ACTIVE_INCREMENT, 2);
            put_u64(&mut out, *generation);
            put_str(&mut out, filename);
        }
        AofIoRequest::Append { sequence, bytes } => {
            tagged(&mut out, REQUEST_APPEND, 2);
            put_u64(&mut out, *sequence);
            put_bin(&mut out, bytes);
        }
        AofIoRequest::Flush { sequence } => {
            tagged(&mut out, REQUEST_FLUSH, 1);
            put_u64(&mut out, *sequence);
        }
        AofIoRequest::PrepareRewrite { generation, files } => {
            tagged(&mut out, REQUEST_PREPARE_REWRITE, 3);
            put_u64(&mut out, *generation);
            put_str(&mut out, &files.base_filename);
            put_str(&mut out, &files.increment_filename);
        }
        AofIoRequest::WriteRewriteChunk {
            generation,
            offset,
            bytes,
        } => {
            tagged(&mut out, REQUEST_WRITE_REWRITE_CHUNK, 3);
            put_u64(&mut out, *generation);
            put_u64(&mut out, *offset);
            put_bin(&mut out, bytes);
        }
        AofIoRequest::FinishRewriteFiles { generation } => {
            tagged(&mut out, REQUEST_FINISH_REWRITE_FILES, 1);
            put_u64(&mut out, *generation);
        }
        AofIoRequest::CommitManifest { slot, envelope } => {
            tagged(&mut out, REQUEST_COMMIT_MANIFEST, 2);
            put_u8(&mut out, encode_slot(*slot));
            put_bin(&mut out, envelope);
        }
        AofIoRequest::AbortRewrite { generation } => {
            tagged(&mut out, REQUEST_ABORT_REWRITE, 1);
            put_u64(&mut out, *generation);
        }
        AofIoRequest::GarbageCollect {
            retained_generations,
        } => {
            tagged(&mut out, REQUEST_GARBAGE_COLLECT, 1);
            put_u64_array(&mut out, retained_generations);
        }
        AofIoRequest::Close => tagged(&mut out, REQUEST_CLOSE, 0),
    }
    out
}

/// Decode an `AofIoRequest` from a MessagePack byte slice.
pub fn decode_request(bytes: &[u8]) -> Result<AofIoRequest, AofIoDecodeError> {
    let mut cursor = bytes;
    let (tag, fields) = read_tag(&mut cursor)?;
    let request = match tag {
        REQUEST_ACQUIRE_LOCK => {
            expect_fields(fields, 0)?;
            AofIoRequest::AcquireLock
        }
        REQUEST_LOAD_CATALOG => {
            expect_fields(fields, 0)?;
            AofIoRequest::LoadCatalog
        }
        REQUEST_OPEN_ACTIVE_INCREMENT => {
            expect_fields(fields, 2)?;
            AofIoRequest::OpenActiveIncrement {
                generation: get_u64(&mut cursor)?,
                filename: get_str(&mut cursor)?,
            }
        }
        REQUEST_APPEND => {
            expect_fields(fields, 2)?;
            AofIoRequest::Append {
                sequence: get_u64(&mut cursor)?,
                bytes: get_bin(&mut cursor)?,
            }
        }
        REQUEST_FLUSH => {
            expect_fields(fields, 1)?;
            AofIoRequest::Flush {
                sequence: get_u64(&mut cursor)?,
            }
        }
        REQUEST_PREPARE_REWRITE => {
            expect_fields(fields, 3)?;
            let generation = get_u64(&mut cursor)?;
            let base_filename = get_str(&mut cursor)?;
            let increment_filename = get_str(&mut cursor)?;
            AofIoRequest::PrepareRewrite {
                generation,
                files: RewriteFiles {
                    base_filename,
                    increment_filename,
                },
            }
        }
        REQUEST_WRITE_REWRITE_CHUNK => {
            expect_fields(fields, 3)?;
            AofIoRequest::WriteRewriteChunk {
                generation: get_u64(&mut cursor)?,
                offset: get_u64(&mut cursor)?,
                bytes: get_bin(&mut cursor)?,
            }
        }
        REQUEST_FINISH_REWRITE_FILES => {
            expect_fields(fields, 1)?;
            AofIoRequest::FinishRewriteFiles {
                generation: get_u64(&mut cursor)?,
            }
        }
        REQUEST_COMMIT_MANIFEST => {
            expect_fields(fields, 2)?;
            let slot = decode_slot(get_u8(&mut cursor)?)?;
            let envelope = get_bin(&mut cursor)?;
            AofIoRequest::CommitManifest { slot, envelope }
        }
        REQUEST_ABORT_REWRITE => {
            expect_fields(fields, 1)?;
            AofIoRequest::AbortRewrite {
                generation: get_u64(&mut cursor)?,
            }
        }
        REQUEST_GARBAGE_COLLECT => {
            expect_fields(fields, 1)?;
            AofIoRequest::GarbageCollect {
                retained_generations: get_u64_array(&mut cursor)?,
            }
        }
        REQUEST_CLOSE => {
            expect_fields(fields, 0)?;
            AofIoRequest::Close
        }
        other => {
            return Err(AofIoDecodeError::new(format!(
                "unknown request tag {other}"
            )))
        }
    };
    Ok(request)
}

/// Encode an `AofIoResult` into a MessagePack byte vector.
pub fn encode_result(result: &AofIoResult) -> Vec<u8> {
    let mut out = Vec::new();
    match result {
        AofIoResult::LockAcquired => tagged(&mut out, RESULT_LOCK_ACQUIRED, 0),
        AofIoResult::CatalogLoaded(snapshot) => {
            tagged(&mut out, RESULT_CATALOG_LOADED, 1);
            encode_catalog_snapshot(&mut out, snapshot);
        }
        AofIoResult::Opened { size } => {
            tagged(&mut out, RESULT_OPENED, 1);
            put_u64(&mut out, *size);
        }
        AofIoResult::Appended { sequence, size } => {
            tagged(&mut out, RESULT_APPENDED, 2);
            put_u64(&mut out, *sequence);
            put_u64(&mut out, *size);
        }
        AofIoResult::Flushed { sequence } => {
            tagged(&mut out, RESULT_FLUSHED, 1);
            put_u64(&mut out, *sequence);
        }
        AofIoResult::RewritePrepared => tagged(&mut out, RESULT_REWRITE_PREPARED, 0),
        AofIoResult::RewriteChunkWritten { next_offset } => {
            tagged(&mut out, RESULT_REWRITE_CHUNK_WRITTEN, 1);
            put_u64(&mut out, *next_offset);
        }
        AofIoResult::RewriteFilesFinished => tagged(&mut out, RESULT_REWRITE_FILES_FINISHED, 0),
        AofIoResult::ManifestCommitted { generation } => {
            tagged(&mut out, RESULT_MANIFEST_COMMITTED, 1);
            put_u64(&mut out, *generation);
        }
        AofIoResult::RewriteAborted => tagged(&mut out, RESULT_REWRITE_ABORTED, 0),
        AofIoResult::GarbageCollected => tagged(&mut out, RESULT_GARBAGE_COLLECTED, 0),
        AofIoResult::Closed => tagged(&mut out, RESULT_CLOSED, 0),
    }
    out
}

/// Decode an `AofIoResult` from a MessagePack byte slice.
pub fn decode_result(bytes: &[u8]) -> Result<AofIoResult, AofIoDecodeError> {
    let mut cursor = bytes;
    let (tag, fields) = read_tag(&mut cursor)?;
    let result = match tag {
        RESULT_LOCK_ACQUIRED => {
            expect_fields(fields, 0)?;
            AofIoResult::LockAcquired
        }
        RESULT_CATALOG_LOADED => {
            expect_fields(fields, 1)?;
            AofIoResult::CatalogLoaded(decode_catalog_snapshot(&mut cursor)?)
        }
        RESULT_OPENED => {
            expect_fields(fields, 1)?;
            AofIoResult::Opened {
                size: get_u64(&mut cursor)?,
            }
        }
        RESULT_APPENDED => {
            expect_fields(fields, 2)?;
            AofIoResult::Appended {
                sequence: get_u64(&mut cursor)?,
                size: get_u64(&mut cursor)?,
            }
        }
        RESULT_FLUSHED => {
            expect_fields(fields, 1)?;
            AofIoResult::Flushed {
                sequence: get_u64(&mut cursor)?,
            }
        }
        RESULT_REWRITE_PREPARED => {
            expect_fields(fields, 0)?;
            AofIoResult::RewritePrepared
        }
        RESULT_REWRITE_CHUNK_WRITTEN => {
            expect_fields(fields, 1)?;
            AofIoResult::RewriteChunkWritten {
                next_offset: get_u64(&mut cursor)?,
            }
        }
        RESULT_REWRITE_FILES_FINISHED => {
            expect_fields(fields, 0)?;
            AofIoResult::RewriteFilesFinished
        }
        RESULT_MANIFEST_COMMITTED => {
            expect_fields(fields, 1)?;
            AofIoResult::ManifestCommitted {
                generation: get_u64(&mut cursor)?,
            }
        }
        RESULT_REWRITE_ABORTED => {
            expect_fields(fields, 0)?;
            AofIoResult::RewriteAborted
        }
        RESULT_GARBAGE_COLLECTED => {
            expect_fields(fields, 0)?;
            AofIoResult::GarbageCollected
        }
        RESULT_CLOSED => {
            expect_fields(fields, 0)?;
            AofIoResult::Closed
        }
        other => return Err(AofIoDecodeError::new(format!("unknown result tag {other}"))),
    };
    Ok(result)
}

/// Encode an `AofIoError` into a MessagePack byte vector.
pub fn encode_error(error: &AofIoError) -> Vec<u8> {
    let mut out = Vec::new();
    match error {
        AofIoError::LockUnavailable => tagged(&mut out, ERROR_LOCK_UNAVAILABLE, 0),
        AofIoError::QuotaExceeded => tagged(&mut out, ERROR_QUOTA_EXCEEDED, 0),
        AofIoError::CatalogCorrupt => tagged(&mut out, ERROR_CATALOG_CORRUPT, 0),
        AofIoError::FileMissing { filename } => {
            tagged(&mut out, ERROR_FILE_MISSING, 1);
            put_str(&mut out, filename);
        }
        AofIoError::Backend { message } => {
            tagged(&mut out, ERROR_BACKEND, 1);
            put_str(&mut out, message);
        }
    }
    out
}

/// Decode an `AofIoError` from a MessagePack byte slice.
pub fn decode_error(bytes: &[u8]) -> Result<AofIoError, AofIoDecodeError> {
    let mut cursor = bytes;
    let (tag, fields) = read_tag(&mut cursor)?;
    let error = match tag {
        ERROR_LOCK_UNAVAILABLE => {
            expect_fields(fields, 0)?;
            AofIoError::LockUnavailable
        }
        ERROR_QUOTA_EXCEEDED => {
            expect_fields(fields, 0)?;
            AofIoError::QuotaExceeded
        }
        ERROR_CATALOG_CORRUPT => {
            expect_fields(fields, 0)?;
            AofIoError::CatalogCorrupt
        }
        ERROR_FILE_MISSING => {
            expect_fields(fields, 1)?;
            AofIoError::FileMissing {
                filename: get_str(&mut cursor)?,
            }
        }
        ERROR_BACKEND => {
            expect_fields(fields, 1)?;
            AofIoError::Backend {
                message: get_str(&mut cursor)?,
            }
        }
        other => return Err(AofIoDecodeError::new(format!("unknown error tag {other}"))),
    };
    Ok(error)
}

fn encode_catalog_snapshot(out: &mut Vec<u8>, snapshot: &AofCatalogSnapshot) {
    // generation, slot (nil or u8), files array.
    write_array_len(out, 3).expect("vec write is infallible");
    put_u64(out, snapshot.generation);
    match snapshot.slot {
        Some(slot) => put_u8(out, encode_slot(slot)),
        None => rmp::encode::write_nil(out).expect("vec write is infallible"),
    }
    write_array_len(out, snapshot.files.len() as u32).expect("vec write is infallible");
    for file in &snapshot.files {
        write_array_len(out, 4).expect("vec write is infallible");
        put_str(out, &file.filename);
        put_u64(out, file.sequence);
        put_u8(out, encode_kind(file.kind));
        put_u64(out, file.size);
    }
}

fn decode_catalog_snapshot(cursor: &mut &[u8]) -> Result<AofCatalogSnapshot, AofIoDecodeError> {
    let fields = read_array_len(cursor).map_err(decode_fault)?;
    if fields != 3 {
        return Err(AofIoDecodeError::new(format!(
            "catalog snapshot expected 3 fields, found {fields}"
        )));
    }
    let generation = get_u64(cursor)?;
    let slot = decode_optional_slot(cursor)?;
    let file_count = read_array_len(cursor).map_err(decode_fault)?;
    let mut files = Vec::with_capacity(file_count as usize);
    for _ in 0..file_count {
        let entry = read_array_len(cursor).map_err(decode_fault)?;
        if entry != 4 {
            return Err(AofIoDecodeError::new(format!(
                "catalog file expected 4 fields, found {entry}"
            )));
        }
        files.push(AofCatalogFile {
            filename: get_str(cursor)?,
            sequence: get_u64(cursor)?,
            kind: decode_kind(get_u8(cursor)?)?,
            size: get_u64(cursor)?,
        });
    }
    Ok(AofCatalogSnapshot {
        generation,
        slot,
        files,
    })
}

fn decode_optional_slot(cursor: &mut &[u8]) -> Result<Option<ManifestSlot>, AofIoDecodeError> {
    // Peek the marker; a nil marker means no slot was selected.
    if cursor.first() == Some(&0xc0) {
        *cursor = &cursor[1..];
        return Ok(None);
    }
    Ok(Some(decode_slot(get_u8(cursor)?)?))
}

fn encode_slot(slot: ManifestSlot) -> u8 {
    match slot {
        ManifestSlot::Zero => SLOT_ZERO,
        ManifestSlot::One => SLOT_ONE,
    }
}

fn decode_slot(value: u8) -> Result<ManifestSlot, AofIoDecodeError> {
    match value {
        SLOT_ZERO => Ok(ManifestSlot::Zero),
        SLOT_ONE => Ok(ManifestSlot::One),
        other => Err(AofIoDecodeError::new(format!(
            "unknown manifest slot {other}"
        ))),
    }
}

fn encode_kind(kind: AofFileKind) -> u8 {
    match kind {
        AofFileKind::Base => KIND_BASE,
        AofFileKind::Increment => KIND_INCREMENT,
    }
}

fn decode_kind(value: u8) -> Result<AofFileKind, AofIoDecodeError> {
    match value {
        KIND_BASE => Ok(AofFileKind::Base),
        KIND_INCREMENT => Ok(AofFileKind::Increment),
        other => Err(AofIoDecodeError::new(format!("unknown file kind {other}"))),
    }
}

fn tagged(out: &mut Vec<u8>, tag: u8, field_count: u32) {
    write_array_len(out, field_count + 1).expect("vec write is infallible");
    put_u8(out, tag);
}

fn read_tag(cursor: &mut &[u8]) -> Result<(u8, u32), AofIoDecodeError> {
    let len = read_array_len(cursor).map_err(decode_fault)?;
    if len == 0 {
        return Err(AofIoDecodeError::new("payload array is empty"));
    }
    let tag = get_u8(cursor)?;
    Ok((tag, len - 1))
}

fn expect_fields(found: u32, expected: u32) -> Result<(), AofIoDecodeError> {
    if found == expected {
        Ok(())
    } else {
        Err(AofIoDecodeError::new(format!(
            "expected {expected} fields, found {found}"
        )))
    }
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    write_u8(out, value).expect("vec write is infallible");
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    write_u64(out, value).expect("vec write is infallible");
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    rmp::encode::write_str(out, value).expect("vec write is infallible");
}

fn put_bin(out: &mut Vec<u8>, value: &[u8]) {
    write_bin(out, value).expect("vec write is infallible");
}

fn put_u64_array(out: &mut Vec<u8>, values: &[u64]) {
    write_array_len(out, values.len() as u32).expect("vec write is infallible");
    for value in values {
        put_u64(out, *value);
    }
}

fn get_u8(cursor: &mut &[u8]) -> Result<u8, AofIoDecodeError> {
    read_u8(cursor).map_err(decode_fault)
}

fn get_u64(cursor: &mut &[u8]) -> Result<u64, AofIoDecodeError> {
    read_u64(cursor).map_err(decode_fault)
}

fn get_str(cursor: &mut &[u8]) -> Result<String, AofIoDecodeError> {
    let len = read_str_len(cursor)?;
    let bytes = take(cursor, len)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| AofIoDecodeError::new("string field is not valid UTF-8"))
}

fn get_bin(cursor: &mut &[u8]) -> Result<Bytes, AofIoDecodeError> {
    let len = read_bin_len(cursor).map_err(decode_fault)? as usize;
    let bytes = take(cursor, len)?;
    Ok(Bytes::copy_from_slice(bytes))
}

fn get_u64_array(cursor: &mut &[u8]) -> Result<Vec<u64>, AofIoDecodeError> {
    let count = read_array_len(cursor).map_err(decode_fault)?;
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        values.push(get_u64(cursor)?);
    }
    Ok(values)
}

fn read_str_len(cursor: &mut &[u8]) -> Result<usize, AofIoDecodeError> {
    rmp::decode::read_str_len(cursor)
        .map(|len| len as usize)
        .map_err(decode_fault)
}

fn take<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8], AofIoDecodeError> {
    if cursor.len() < len {
        return Err(AofIoDecodeError::new("payload ended before all bytes read"));
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head)
}

fn decode_fault<E: core::fmt::Debug>(error: E) -> AofIoDecodeError {
    AofIoDecodeError::new(format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(request: AofIoRequest) {
        let encoded = encode_request(&request);
        let decoded = decode_request(&encoded).expect("request decodes");
        assert_eq!(decoded, request);
    }

    fn roundtrip_result(result: AofIoResult) {
        let encoded = encode_result(&result);
        let decoded = decode_result(&encoded).expect("result decodes");
        assert_eq!(decoded, result);
    }

    fn roundtrip_error(error: AofIoError) {
        let encoded = encode_error(&error);
        let decoded = decode_error(&encoded).expect("error decodes");
        assert_eq!(decoded, error);
    }

    #[test]
    fn requests_round_trip() {
        roundtrip_request(AofIoRequest::AcquireLock);
        roundtrip_request(AofIoRequest::LoadCatalog);
        roundtrip_request(AofIoRequest::OpenActiveIncrement {
            generation: 7,
            filename: "appendonly.aof.0000000007.incr.aof".to_string(),
        });
        roundtrip_request(AofIoRequest::Append {
            sequence: 42,
            bytes: Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
        });
        roundtrip_request(AofIoRequest::Flush { sequence: 9 });
        roundtrip_request(AofIoRequest::PrepareRewrite {
            generation: 3,
            files: RewriteFiles {
                base_filename: "appendonly.aof.0000000003.base.rdb".to_string(),
                increment_filename: "appendonly.aof.0000000003.incr.aof".to_string(),
            },
        });
        roundtrip_request(AofIoRequest::WriteRewriteChunk {
            generation: 3,
            offset: 1024,
            bytes: Bytes::from_static(&[0u8, 1, 2, 3, 0xff]),
        });
        roundtrip_request(AofIoRequest::FinishRewriteFiles { generation: 3 });
        roundtrip_request(AofIoRequest::CommitManifest {
            slot: ManifestSlot::One,
            envelope: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        });
        roundtrip_request(AofIoRequest::AbortRewrite { generation: 4 });
        roundtrip_request(AofIoRequest::GarbageCollect {
            retained_generations: vec![3, 4],
        });
        roundtrip_request(AofIoRequest::Close);
    }

    #[test]
    fn append_payload_preserves_non_utf8_bytes() {
        let payload = Bytes::from_static(&[0x00, 0x80, 0xff, 0xfe, 0x0d, 0x0a]);
        let request = AofIoRequest::Append {
            sequence: 1,
            bytes: payload.clone(),
        };
        let decoded = decode_request(&encode_request(&request)).expect("decodes");
        let AofIoRequest::Append { bytes, .. } = decoded else {
            panic!("expected Append");
        };
        assert_eq!(bytes, payload);
    }

    #[test]
    fn results_round_trip() {
        roundtrip_result(AofIoResult::LockAcquired);
        roundtrip_result(AofIoResult::CatalogLoaded(AofCatalogSnapshot {
            generation: 2,
            slot: Some(ManifestSlot::Zero),
            files: vec![
                AofCatalogFile {
                    filename: "appendonly.aof.0000000002.base.rdb".to_string(),
                    sequence: 2,
                    kind: AofFileKind::Base,
                    size: 512,
                },
                AofCatalogFile {
                    filename: "appendonly.aof.0000000002.incr.aof".to_string(),
                    sequence: 2,
                    kind: AofFileKind::Increment,
                    size: 128,
                },
            ],
        }));
        roundtrip_result(AofIoResult::CatalogLoaded(AofCatalogSnapshot::default()));
        roundtrip_result(AofIoResult::Opened { size: 4096 });
        roundtrip_result(AofIoResult::Appended {
            sequence: 11,
            size: 2048,
        });
        roundtrip_result(AofIoResult::Flushed { sequence: 11 });
        roundtrip_result(AofIoResult::RewritePrepared);
        roundtrip_result(AofIoResult::RewriteChunkWritten { next_offset: 8192 });
        roundtrip_result(AofIoResult::RewriteFilesFinished);
        roundtrip_result(AofIoResult::ManifestCommitted { generation: 5 });
        roundtrip_result(AofIoResult::RewriteAborted);
        roundtrip_result(AofIoResult::GarbageCollected);
        roundtrip_result(AofIoResult::Closed);
    }

    #[test]
    fn errors_round_trip() {
        roundtrip_error(AofIoError::LockUnavailable);
        roundtrip_error(AofIoError::QuotaExceeded);
        roundtrip_error(AofIoError::CatalogCorrupt);
        roundtrip_error(AofIoError::FileMissing {
            filename: "appendonly.aof.0000000002.incr.aof".to_string(),
        });
        roundtrip_error(AofIoError::Backend {
            message: "disk full".to_string(),
        });
    }

    #[test]
    fn unknown_tag_is_rejected() {
        let mut payload = Vec::new();
        tagged(&mut payload, 200, 0);
        assert!(decode_request(&payload).is_err());
        assert!(decode_result(&payload).is_err());
        assert!(decode_error(&payload).is_err());
    }

    #[test]
    fn truncated_payload_is_rejected() {
        let encoded = encode_request(&AofIoRequest::Append {
            sequence: 1,
            bytes: Bytes::from_static(b"frame"),
        });
        assert!(decode_request(&encoded[..encoded.len() - 2]).is_err());
    }

    #[test]
    fn empty_payload_is_rejected() {
        assert!(decode_request(&[]).is_err());
    }
}
