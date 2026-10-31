pub mod compression;
mod encoder;
pub mod frozen;
pub mod serialize;
mod stream;
#[cfg(test)]
mod tests;

pub use encoder::{save_millisecond_time, save_string};
pub(crate) use encoder::{save_raw_string, RdbVecWriter};
pub(crate) use serialize::save_key_value;
pub use serialize::{
    serialize_all, serialize_all_with_functions, serialize_all_with_functions_and_replication,
    serialize_all_with_functions_and_replication_and_checksum,
    serialize_all_with_functions_and_replication_streaming,
    serialize_all_with_functions_and_replication_streaming_with_options,
    serialize_all_with_functions_and_replication_with_options,
    serialize_all_with_functions_streaming, serialize_all_with_functions_with_options,
    serialize_all_with_options, RdbReplicationInfo, RdbSerializeOptions,
};
pub use stream::{
    ExpiredHashFields, ExpiredKeyPolicy, RdbDecodePolicy, RdbDecodedMetadata, RdbDecoder,
    RdbExpiryEvent, RdbStep, RdbStreamDecoder, StreamStep,
};

#[cfg(test)]
pub use encoder::save_binary_double;

use crate::foundation::io::persistence::codec::types::{
    RDB_FOREIGN_VERSION_MAX, RDB_FOREIGN_VERSION_MIN, RDB_VERSION,
};

#[derive(Debug)]
pub enum LoadError {
    IncompatibleVersion(i32),
    ForeignType { type_byte: u8, version: i32 },
    UnknownType,
    CrcError,
    Corrupt,
    Io(std::io::Error),
}

/// Validate the fixed RDB header before dispatching the potentially expensive
/// dataset load. Startup uses this to report an incompatible version without
/// depending on a loader thread getting scheduled within the harness timeout.
pub fn validate_rdb_header(bytes: &[u8], strict: bool) -> Result<(), LoadError> {
    if bytes.len() < 9 {
        return Err(LoadError::Corrupt);
    }
    let is_redis_magic = &bytes[..6] == b"REDIS0";
    let is_valkey_magic = &bytes[..6] == b"VALKEY";
    if !is_redis_magic && !is_valkey_magic {
        return Err(LoadError::IncompatibleVersion(-1));
    }
    let rdbver = std::str::from_utf8(&bytes[6..9])
        .ok()
        .and_then(|value| value.trim().parse::<i32>().ok())
        .unwrap_or(-1);
    if !is_version_accepted(rdbver, is_valkey_magic, is_redis_magic, strict) {
        return Err(LoadError::IncompatibleVersion(rdbver));
    }
    Ok(())
}

#[allow(clippy::result_unit_err)]
pub fn load_rdb_bytes(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
) -> Result<(usize, usize), ()> {
    load_rdb_bytes_checked(bytes, dbs, true).map_err(|_| ())
}

pub fn load_rdb_bytes_checked(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
) -> Result<(usize, usize), LoadError> {
    load_rdb_bytes_checked_with_functions(bytes, dbs, strict)
        .map(|(loaded, consumed, _expired, _function_codes)| (loaded, consumed))
}

/// Load an RDB and return the opaque source of every persisted FUNCTION library.
/// Callers that own the scripting runtime are responsible for validating and
/// installing those libraries after the database payload has been decoded.
pub fn load_rdb_bytes_checked_with_functions(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
) -> Result<(usize, usize, usize, Vec<Vec<u8>>), LoadError> {
    load_rdb_bytes_checked_with_function_codes(bytes, dbs, strict, 0)
        .map(|(loaded, consumed, expired, functions, _, _)| (loaded, consumed, expired, functions))
}

/// Like [`load_rdb_bytes_checked_with_functions`] but honors `key-load-delay`:
/// sleeps `key_load_delay_us` per loaded key (C's `debugDelay` in rdb.c). Used
/// by the replica full-sync apply path so the slow/fast-replica sections can
/// pace the load; `0` is a strict no-op.
pub fn load_rdb_bytes_checked_with_functions_and_delay(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
    key_load_delay_us: i64,
) -> Result<(usize, usize, usize, Vec<Vec<u8>>), LoadError> {
    load_rdb_bytes_checked_with_function_codes(bytes, dbs, strict, key_load_delay_us)
        .map(|(loaded, consumed, expired, functions, _, _)| (loaded, consumed, expired, functions))
}

pub fn load_rdb_bytes_checked_with_delay(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
    key_load_delay_us: i64,
) -> Result<(usize, usize), LoadError> {
    load_rdb_bytes_checked_with_function_codes(bytes, dbs, strict, key_load_delay_us)
        .map(|(loaded, consumed, _expired, _function_codes, _, _)| (loaded, consumed))
}

pub fn load_rdb_bytes_checked_with_delay_stats(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
    key_load_delay_us: i64,
) -> Result<(usize, usize, usize), LoadError> {
    load_rdb_bytes_checked_with_function_codes(bytes, dbs, strict, key_load_delay_us)
        .map(|(loaded, consumed, expired, _function_codes, _, _)| (loaded, consumed, expired))
}

pub fn load_rdb_bytes_checked_with_delay_stats_and_replication_info(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
    key_load_delay_us: i64,
) -> Result<(usize, usize, usize, Option<RdbReplicationInfo>), LoadError> {
    load_rdb_bytes_checked_with_function_codes(bytes, dbs, strict, key_load_delay_us).map(
        |(loaded, consumed, expired, _function_codes, replication, _expired_keys)| {
            (loaded, consumed, expired, replication)
        },
    )
}

pub fn load_rdb_bytes_checked_with_delay_stats_and_replication_events(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
    key_load_delay_us: i64,
) -> Result<
    (
        usize,
        usize,
        usize,
        Option<RdbReplicationInfo>,
        Vec<(u32, Vec<u8>)>,
    ),
    LoadError,
> {
    load_rdb_bytes_checked_with_function_codes(bytes, dbs, strict, key_load_delay_us).map(
        |(loaded, consumed, expired, _function_codes, replication, expired_keys)| {
            (loaded, consumed, expired, replication, expired_keys)
        },
    )
}

fn load_rdb_bytes_checked_with_function_codes(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    strict: bool,
    key_load_delay_us: i64,
) -> Result<
    (
        usize,
        usize,
        usize,
        Vec<Vec<u8>>,
        Option<RdbReplicationInfo>,
        Vec<(u32, Vec<u8>)>,
    ),
    LoadError,
> {
    let mut policy = RdbDecodePolicy::persisted(strict, key_load_delay_us);
    // Compatibility callers include DEBUG RELOAD NOFLUSH MERGE, which has
    // always replaced existing keys while decoding into the live dataset.
    policy.allow_duplicate_keys = true;
    let metadata = load_rdb_bytes_with_policy(bytes, dbs, policy)?;
    let expired_keys = metadata
        .expiry_events
        .into_iter()
        .filter_map(|event| match event {
            RdbExpiryEvent::Key { db, key } => Some((db, key)),
            RdbExpiryEvent::HashFields(_) => None,
        })
        .collect();
    Ok((
        metadata.loaded,
        metadata.consumed,
        metadata.expired,
        metadata.function_codes,
        metadata.replication,
        expired_keys,
    ))
}

pub fn load_rdb_bytes_with_policy(
    bytes: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    policy: RdbDecodePolicy,
) -> Result<RdbDecodedMetadata, LoadError> {
    let decoded = compression::decode_rdb_stream(bytes, false).map_err(|error| {
        log::warn!("{}", error.message());
        LoadError::Corrupt
    })?;
    let bytes = if let Some(decoded) = &decoded {
        log::info!("Logical RDB CRC64 skipped for streaming-compressed input");
        decoded.bytes.as_slice()
    } else {
        bytes
    };
    let mut decoder = RdbDecoder::with_policy(policy);
    if decoded.is_some() {
        decoder.skip_checksum();
    }
    decoder.feed(bytes);
    loop {
        match decoder.step(dbs)? {
            RdbStep::NeedInput => return Err(LoadError::Corrupt),
            RdbStep::RecordApplied => {}
            RdbStep::Finished(metadata) => {
                if metadata.empty_keys > 0 {
                    log::warn!("empty keys skipped: {}", metadata.empty_keys);
                }
                return Ok(metadata);
            }
        }
    }
}

pub(crate) fn is_standard_aux_field(key: &[u8]) -> bool {
    [
        b"redis-ver".as_slice(),
        b"valkey-ver".as_slice(),
        b"redis-bits".as_slice(),
        b"ctime".as_slice(),
        b"used-mem".as_slice(),
        b"repl-stream-db".as_slice(),
        b"repl-id".as_slice(),
        b"repl-offset".as_slice(),
        b"aof-base".as_slice(),
    ]
    .iter()
    .any(|standard| key.eq_ignore_ascii_case(standard))
}

pub(crate) fn is_version_accepted(
    rdbver: i32,
    is_valkey: bool,
    is_redis: bool,
    strict: bool,
) -> bool {
    if rdbver < 1 {
        return false;
    }
    if is_valkey && rdbver <= RDB_FOREIGN_VERSION_MAX as i32 {
        return false;
    }
    if is_redis && rdbver > RDB_FOREIGN_VERSION_MAX as i32 {
        return false;
    }
    if strict {
        if rdbver > RDB_VERSION as i32 {
            return false;
        }
        if is_foreign_version(rdbver) {
            return false;
        }
    }
    true
}

fn is_foreign_version(rdbver: i32) -> bool {
    (RDB_FOREIGN_VERSION_MIN as i32..=RDB_FOREIGN_VERSION_MAX as i32).contains(&rdbver)
}

pub fn is_rdb_magic(bytes: &[u8]) -> bool {
    bytes.len() >= 6 && (&bytes[..6] == b"REDIS0" || &bytes[..6] == b"VALKEY")
}

#[allow(clippy::result_unit_err)]
pub fn load_rdb(
    buf: &[u8],
    dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
) -> Result<(), ()> {
    load_rdb_bytes(buf, dbs).map(|_| ())
}
