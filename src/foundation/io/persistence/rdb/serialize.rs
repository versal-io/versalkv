use super::compression::{
    new_rdb_frame_encoder, RdbCompression, RdbSnapshotOptions, VCS_RDB_HEADER,
};
use super::{save_raw_string, RdbVecWriter};
use crate::foundation::io::persistence::{
    crc64, save_len, save_millisecond_time, type_byte_with_hash_field_expires, RDB_OPCODE_AUX,
    RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB,
    RDB_VERSION,
};
use crate::foundation::io::store::kv_engine::{now_ms, DbSet};
use crate::foundation::io::store::Robj;
use crate::foundation::memory::util_version as version;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

struct SnapshotSink<F>(F);

impl<F: FnMut(&[u8]) -> bool> Write for SnapshotSink<F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if (self.0)(bytes) {
            Ok(bytes.len())
        } else {
            Err(std::io::Error::other("RDB stream sink rejected output"))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn serialize_replication_snapshot_streaming(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    replication: Option<&RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    options: RdbSnapshotOptions,
    emit: impl FnMut(&[u8]) -> bool,
) -> bool {
    let logical = RdbSerializeOptions {
        compression: options.compression.uses_lzf(),
        checksum: options.checksum && options.compression != RdbCompression::Lz4,
    };
    let mut sink = SnapshotSink(emit);
    if options.compression != RdbCompression::Lz4 {
        return serialize_all_with_functions_and_replication_streaming_with_options(
            dbs,
            key_delay_us,
            function_codes,
            include_keys,
            replication,
            additional_datasets,
            logical,
            |bytes| sink.write_all(bytes).is_ok(),
        );
    }
    if sink.write_all(VCS_RDB_HEADER).is_err() {
        return false;
    }
    let mut encoder = new_rdb_frame_encoder(sink, options.checksum);
    let completed = serialize_all_with_functions_and_replication_streaming_with_options(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        replication,
        additional_datasets,
        logical,
        |bytes| encoder.write_all(bytes).is_ok(),
    );
    completed && encoder.finish().is_ok()
}

pub fn serialize_replication_snapshot(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    replication: Option<&RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    options: RdbSnapshotOptions,
) -> Vec<u8> {
    let mut output = Vec::new();
    serialize_replication_snapshot_streaming(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        replication,
        additional_datasets,
        options,
        |bytes| {
            output.extend_from_slice(bytes);
            true
        },
    );
    output
}

static KEY_SAVE_DELAY_INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RdbSerializeOptions {
    pub compression: bool,
    pub checksum: bool,
}

impl Default for RdbSerializeOptions {
    fn default() -> Self {
        Self {
            compression: true,
            checksum: true,
        }
    }
}

pub fn reset_key_save_delay_interrupt() {
    KEY_SAVE_DELAY_INTERRUPTED.store(false, Ordering::Relaxed);
}

pub fn interrupt_key_save_delay() {
    KEY_SAVE_DELAY_INTERRUPTED.store(true, Ordering::Relaxed);
}

pub fn serialize_all(dbs: &DbSet, key_delay_us: i64) -> Vec<u8> {
    serialize_all_with_options(dbs, key_delay_us, RdbSerializeOptions::default())
}

pub fn serialize_all_with_options(
    dbs: &DbSet,
    key_delay_us: i64,
    options: RdbSerializeOptions,
) -> Vec<u8> {
    serialize_all_with_functions_and_replication_with_options(
        dbs,
        key_delay_us,
        &[],
        true,
        None,
        &[],
        options,
    )
}

/// Serialize an RDB and optional FUNCTION libraries captured by the caller.
/// The persistence layer only handles opaque library bytes so it does not depend
/// on the scripting engine.
pub fn serialize_all_with_functions(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
) -> Vec<u8> {
    serialize_all_with_functions_with_options(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        RdbSerializeOptions::default(),
    )
}

pub fn serialize_all_with_functions_with_options(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    options: RdbSerializeOptions,
) -> Vec<u8> {
    serialize_all_with_functions_and_replication_with_options(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        None,
        &[],
        options,
    )
}

/// Replication metadata persisted in a primary RDB so a restart can retain
/// PSYNC2 continuity. It stays opaque to this foundation layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdbReplicationInfo {
    pub repl_stream_db: i64,
    pub repl_id: String,
    pub repl_offset: i64,
    /// Replication-only subsystem metadata encoded as opaque RDB AUX pairs.
    pub aux_fields: Vec<(Vec<u8>, Vec<u8>)>,
}

pub fn serialize_all_with_functions_and_replication(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    replication: Option<&RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
) -> Vec<u8> {
    serialize_all_with_functions_and_replication_with_options(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        replication,
        additional_datasets,
        RdbSerializeOptions::default(),
    )
}

pub fn serialize_all_with_functions_and_replication_with_options(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    replication: Option<&RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    options: RdbSerializeOptions,
) -> Vec<u8> {
    // Pre-reserve capacity to avoid repeated Vec growth reallocations during
    // small-field encoding: magic(9) + aux(~256) + per-key ~64B avg estimate.
    let total_keys: u64 = if include_keys {
        std::iter::once(dbs)
            .chain(additional_datasets.iter().copied())
            .flat_map(|dataset| (0..dataset.num_dbs() as u32).filter_map(|i| dataset.get(i)))
            .map(|db| db.dbsize() as u64)
            .sum()
    } else {
        0
    };
    let estimate = 9 + 256 + total_keys.saturating_mul(64);
    let mut out = Vec::with_capacity(estimate as usize);

    out.extend_from_slice(format!("VALKEY{:03}", RDB_VERSION).as_bytes());

    save_aux_str(
        &mut out,
        b"valkey-ver",
        version::REDIS_VERSION.as_bytes(),
        options.compression,
    );
    save_aux_str(&mut out, b"redis-bits", b"64", options.compression);
    save_aux_str(
        &mut out,
        b"ctime",
        (now_ms() / 1000).to_string().as_bytes(),
        options.compression,
    );
    save_aux_str(
        &mut out,
        b"used-mem",
        crate::foundation::memory::util_alloc::total_allocated()
            .to_string()
            .as_bytes(),
        options.compression,
    );
    if let Some(replication) = replication {
        save_aux_str(
            &mut out,
            b"repl-stream-db",
            replication.repl_stream_db.to_string().as_bytes(),
            options.compression,
        );
        save_aux_str(
            &mut out,
            b"repl-id",
            replication.repl_id.as_bytes(),
            options.compression,
        );
        save_aux_str(
            &mut out,
            b"repl-offset",
            replication.repl_offset.to_string().as_bytes(),
            options.compression,
        );
        for (key, value) in &replication.aux_fields {
            save_aux_str(&mut out, key, value, options.compression);
        }
    }
    save_aux_str(&mut out, b"aof-base", b"0", options.compression);

    for code in function_codes {
        out.push(crate::foundation::io::persistence::codec::types::RDB_OPCODE_FUNCTION2);
        save_raw_string(&mut out, code, options.compression);
    }

    if include_keys {
        for dataset in std::iter::once(dbs).chain(additional_datasets.iter().copied()) {
            append_dataset(&mut out, dataset, key_delay_us, options.compression);
        }
    }

    out.push(RDB_OPCODE_EOF);
    let crc = if options.checksum { crc64(0, &out) } else { 0 };
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Serialize a persisted RDB with an explicit CRC64 trailer policy.
/// Persistent snapshots use the server's `rdbchecksum` setting. Replication
/// keeps the default checked path because its checksum behavior is negotiated
/// independently through replica capabilities.
pub fn serialize_all_with_functions_and_replication_and_checksum(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    replication: Option<&RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    checksum: bool,
) -> Vec<u8> {
    serialize_all_with_functions_and_replication_with_options(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        replication,
        additional_datasets,
        RdbSerializeOptions {
            compression: true,
            checksum,
        },
    )
}

const RDB_STREAM_BATCH_BYTES: usize = 16 * 1024;

fn flush_stream_chunk<F>(chunk: &mut Vec<u8>, crc: &mut u64, emit: &mut F) -> bool
where
    F: FnMut(&[u8]) -> bool,
{
    if chunk.is_empty() {
        return true;
    }
    *crc = crc64(*crc, chunk);
    if !emit(chunk) {
        return false;
    }
    chunk.clear();
    true
}

/// Stream an RDB snapshot in buffered chunks so diskless replication matches
/// C's `PROTO_IOBUF_LEN` RIO writes rather than issuing a socket write per key.
pub fn serialize_all_with_functions_streaming<F>(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    emit: F,
) -> bool
where
    F: FnMut(&[u8]) -> bool,
{
    serialize_all_with_functions_and_replication_streaming(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        None,
        &[],
        emit,
    )
}

pub fn serialize_all_with_functions_and_replication_streaming<F>(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    replication: Option<&RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    emit: F,
) -> bool
where
    F: FnMut(&[u8]) -> bool,
{
    serialize_all_with_functions_and_replication_streaming_with_options(
        dbs,
        key_delay_us,
        function_codes,
        include_keys,
        replication,
        additional_datasets,
        RdbSerializeOptions::default(),
        emit,
    )
}

pub fn serialize_all_with_functions_and_replication_streaming_with_options<F>(
    dbs: &DbSet,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    include_keys: bool,
    replication: Option<&RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    options: RdbSerializeOptions,
    mut emit: F,
) -> bool
where
    F: FnMut(&[u8]) -> bool,
{
    let mut chunk = Vec::with_capacity(RDB_STREAM_BATCH_BYTES);
    let mut crc = 0u64;
    chunk.extend_from_slice(format!("VALKEY{:03}", RDB_VERSION).as_bytes());
    let ctime = (now_ms() / 1000).to_string();
    let used_mem = crate::foundation::memory::util_alloc::total_allocated().to_string();
    for (key, value) in [
        (b"valkey-ver".as_slice(), version::REDIS_VERSION.as_bytes()),
        (b"redis-bits".as_slice(), b"64".as_slice()),
        (b"ctime".as_slice(), ctime.as_bytes()),
        (b"used-mem".as_slice(), used_mem.as_bytes()),
    ] {
        save_aux_str(&mut chunk, key, value, options.compression);
        if chunk.len() >= RDB_STREAM_BATCH_BYTES
            && !flush_stream_chunk(&mut chunk, &mut crc, &mut emit)
        {
            return false;
        }
    }
    if let Some(replication) = replication {
        let repl_stream_db = replication.repl_stream_db.to_string();
        let repl_offset = replication.repl_offset.to_string();
        for (key, value) in [
            (b"repl-stream-db".as_slice(), repl_stream_db.as_bytes()),
            (b"repl-id".as_slice(), replication.repl_id.as_bytes()),
            (b"repl-offset".as_slice(), repl_offset.as_bytes()),
        ] {
            save_aux_str(&mut chunk, key, value, options.compression);
            if chunk.len() >= RDB_STREAM_BATCH_BYTES
                && !flush_stream_chunk(&mut chunk, &mut crc, &mut emit)
            {
                return false;
            }
        }
        for (key, value) in &replication.aux_fields {
            save_aux_str(&mut chunk, key, value, options.compression);
            if chunk.len() >= RDB_STREAM_BATCH_BYTES
                && !flush_stream_chunk(&mut chunk, &mut crc, &mut emit)
            {
                return false;
            }
        }
    }
    save_aux_str(&mut chunk, b"aof-base", b"0", options.compression);
    if chunk.len() >= RDB_STREAM_BATCH_BYTES && !flush_stream_chunk(&mut chunk, &mut crc, &mut emit)
    {
        return false;
    }
    for code in function_codes {
        chunk.push(crate::foundation::io::persistence::codec::types::RDB_OPCODE_FUNCTION2);
        save_raw_string(&mut chunk, code, options.compression);
        if chunk.len() >= RDB_STREAM_BATCH_BYTES
            && !flush_stream_chunk(&mut chunk, &mut crc, &mut emit)
        {
            return false;
        }
    }
    if include_keys {
        for dataset in std::iter::once(dbs).chain(additional_datasets.iter().copied()) {
            for i in 0..dataset.num_dbs() as u32 {
                let Some(db) = dataset.get(i) else { continue };
                if db.dbsize() == 0 {
                    continue;
                }
                chunk.push(RDB_OPCODE_SELECTDB);
                save_len(&mut chunk, i as u64);
                chunk.push(RDB_OPCODE_RESIZEDB);
                save_len(&mut chunk, db.dbsize() as u64);
                save_len(&mut chunk, db.expires_count() as u64);
                let mut ok = true;
                db.for_each_entry_with_hash_field_expires(|key, val, expire, hfe| {
                    if !ok {
                        return;
                    }
                    save_key_value(&mut chunk, key, val, expire, hfe, options.compression);
                    if chunk.len() >= RDB_STREAM_BATCH_BYTES {
                        ok = flush_stream_chunk(&mut chunk, &mut crc, &mut emit);
                    }
                    if ok {
                        delay_after_key_save(key_delay_us);
                    }
                });
                if !ok {
                    return false;
                }
            }
        }
    }
    chunk.push(RDB_OPCODE_EOF);
    if !flush_stream_chunk(&mut chunk, &mut crc, &mut emit) {
        return false;
    }
    let checksum = if options.checksum { crc } else { 0 };
    emit(&checksum.to_le_bytes())
}

fn append_dataset(out: &mut Vec<u8>, dbs: &DbSet, key_delay_us: i64, compression: bool) {
    for i in 0..dbs.num_dbs() as u32 {
        let Some(db) = dbs.get(i) else { continue };
        if db.dbsize() == 0 {
            continue;
        }
        out.push(RDB_OPCODE_SELECTDB);
        save_len(out, i as u64);
        out.push(RDB_OPCODE_RESIZEDB);
        save_len(out, db.dbsize() as u64);
        save_len(out, db.expires_count() as u64);
        db.for_each_entry_with_hash_field_expires(|key, val, expire, hfe| {
            save_key_value(out, key, val, expire, hfe, compression);
            delay_after_key_save(key_delay_us);
        });
    }
}

pub(crate) fn delay_after_key_save(key_delay_us: i64) {
    // C's child-info progress pipe detects an orphan while saving. A disk
    // snapshot has no output pipe, so check the post-fork parent's identity at
    // each key and during long debug delays without affecting serving threads.
    crate::foundation::io::sys::process::exit_if_persistence_child_parent_changed();
    crate::foundation::io::sys::time::sleep_debug_delay_interruptible(key_delay_us, || {
        crate::foundation::io::sys::process::exit_if_persistence_child_parent_changed();
        KEY_SAVE_DELAY_INTERRUPTED.load(Ordering::Relaxed)
    });
}

fn save_aux_str(out: &mut Vec<u8>, key: &[u8], val: &[u8], compression: bool) {
    out.push(RDB_OPCODE_AUX);
    save_raw_string(out, key, compression);
    save_raw_string(out, val, compression);
}

pub(crate) fn save_key_value(
    out: &mut Vec<u8>,
    key: &[u8],
    val: &Robj,
    expire: Option<u64>,
    hash_field_expires: Option<&HashMap<Vec<u8>, u64>>,
    compression: bool,
) {
    if let Some(exp) = expire {
        out.push(RDB_OPCODE_EXPIRETIME_MS);
        save_millisecond_time(out, exp as i64);
    }
    out.push(type_byte_with_hash_field_expires(val, hash_field_expires));
    save_raw_string(out, key, compression);
    let mut writer = RdbVecWriter::new(out, compression);
    crate::foundation::io::persistence::codec::save_robj_with_hash_field_expires(
        val,
        hash_field_expires,
        &mut writer,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn metadata_only_serialization_does_not_reserve_per_key_capacity() {
        let mut dbs = DbSet::new(1);
        for index in 0..20_000 {
            dbs.get_mut(0).unwrap().set(
                format!("key-{index}").into_bytes(),
                Robj::string_raw(Bytes::from_static(b"value")),
            );
        }

        let header = serialize_all_with_functions_and_replication_with_options(
            &dbs,
            0,
            &[],
            false,
            None,
            &[],
            RdbSerializeOptions::default(),
        );

        assert!(
            header.capacity() <= 4_096,
            "metadata-only serialization reserved {} bytes",
            header.capacity()
        );
    }
}
