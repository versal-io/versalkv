#![forbid(unsafe_code)]

#[cfg(not(feature = "wasm"))]
pub mod aof;
pub mod aof_io;
pub mod codec;
#[cfg(not(feature = "wasm"))]
pub mod journal;
#[cfg(not(feature = "wasm"))]
mod persistence_traits;
#[cfg(not(feature = "wasm"))]
pub mod rdb;
#[cfg(not(feature = "wasm"))]
pub mod snapshot;

#[cfg(feature = "wasm")]
#[path = "rdb/encoder.rs"]
mod rdb_encoder;

#[cfg(feature = "wasm")]
pub mod rdb {
    pub(crate) use super::rdb_encoder::RdbVecWriter;
    pub use super::rdb_encoder::{save_millisecond_time, save_string};
}

#[cfg(not(feature = "wasm"))]
pub use persistence_traits::{Journal, Snapshot};

pub use aof_io::{
    AofCatalogFile, AofCatalogSnapshot, AofFileKind, AofIoError, AofIoRequest, AofIoResult,
    ManifestSlot, RewriteFiles,
};

#[cfg(test)]
pub use codec::decoder_compact;
pub use codec::types;
pub use codec::{
    load_typed_object, load_typed_object_with_hash_field_expires,
    load_typed_object_with_hash_field_expires_detailed, save_robj,
    save_robj_with_hash_field_expires, type_byte, type_byte_with_hash_field_expires,
    ObjectLoadError,
};

#[cfg(not(feature = "wasm"))]
pub use rdb::serialize;
#[cfg(not(feature = "wasm"))]
pub use rdb::{
    is_rdb_magic, load_rdb, load_rdb_bytes_checked, load_rdb_bytes_checked_with_delay,
    load_rdb_bytes_checked_with_delay_stats,
    load_rdb_bytes_checked_with_delay_stats_and_replication_events,
    load_rdb_bytes_checked_with_delay_stats_and_replication_info,
    load_rdb_bytes_checked_with_functions, load_rdb_bytes_checked_with_functions_and_delay,
    load_rdb_bytes_with_policy, serialize_all, serialize_all_with_functions,
    serialize_all_with_functions_and_replication,
    serialize_all_with_functions_and_replication_and_checksum,
    serialize_all_with_functions_and_replication_streaming,
    serialize_all_with_functions_and_replication_streaming_with_options,
    serialize_all_with_functions_and_replication_with_options,
    serialize_all_with_functions_streaming, serialize_all_with_functions_with_options,
    serialize_all_with_options, validate_rdb_header, ExpiredHashFields, ExpiredKeyPolicy,
    LoadError, RdbDecodePolicy, RdbDecodedMetadata, RdbDecoder, RdbExpiryEvent, RdbReplicationInfo,
    RdbSerializeOptions, RdbStep, RdbStreamDecoder, StreamStep,
};
pub use rdb::{save_millisecond_time, save_string};
pub use types::{
    crc64, save_len, RDB_OPCODE_AUX, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_RESIZEDB,
    RDB_OPCODE_SELECTDB, RDB_VERSION,
};

#[cfg(not(feature = "wasm"))]
pub use aof::rewrite as aof_rewrite;
#[cfg(not(feature = "wasm"))]
pub use aof::{
    ensure_aof_increment, format_manifest_token, gc_aof_history, install_synced_rdb_as_aof_base,
    load_aof_bytes, next_rewrite_sequences, prepare_synced_rdb_as_aof_base, read_manifest_entries,
    resolve_aof_read_path, resolve_aof_read_paths, resolve_aof_read_paths_checked,
    rotate_aof_increment, AofAppender, AofCommandPlayer, AofDataStore, AofDecoder, AofFsync,
    AofLoadError, AofLoadStats, AofManifestEntry, AofOpenMode, AofStep, PreparedSyncedRdbAofBase,
};

#[cfg(test)]
pub use codec::decoder::{load_binary_double, load_string};
#[cfg(test)]
pub use codec::load_robj;
#[cfg(all(test, not(feature = "wasm")))]
pub use rdb::{load_rdb_bytes, save_binary_double};
#[cfg(test)]
pub use types::{load_len, RDB_TYPE_STRING};
