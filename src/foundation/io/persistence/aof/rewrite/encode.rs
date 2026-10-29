use crate::foundation::io::protocol::serializer::format_double;
use crate::foundation::io::store::kv_engine::Db;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::foundation::io::store::Robj;
use std::io::Write;

const COMMAND_SNAPSHOT_BUFFER_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy)]
pub(super) struct DbCommandSnapshotOptions {
    dbid: u32,
    include_select: bool,
    key_delay_us: i64,
}

impl DbCommandSnapshotOptions {
    pub(super) fn new(dbid: u32, include_select: bool, key_delay_us: i64) -> Self {
        Self {
            dbid,
            include_select,
            key_delay_us,
        }
    }
}

#[derive(Clone, Copy)]
enum SnapshotEntryMode {
    Live,
    Physical,
}

struct DbCommandSnapshotBuffer {
    bytes: Vec<u8>,
    count: usize,
    options: DbCommandSnapshotOptions,
}

impl DbCommandSnapshotBuffer {
    fn append(
        &mut self,
        key: &[u8],
        value: &Robj,
        expire: Option<u64>,
        hash_field_expires: Option<&std::collections::HashMap<Vec<u8>, u64>>,
    ) {
        if self.count == 0 && self.options.include_select {
            self.bytes.extend_from_slice(&encode_command(&[
                b"SELECT",
                self.options.dbid.to_string().as_bytes(),
            ]));
        }
        serialize_key_value(&mut self.bytes, key, value, hash_field_expires);
        if let Some(expire_ms) = expire {
            self.bytes.extend_from_slice(&encode_command(&[
                b"PEXPIREAT",
                key,
                expire_ms.to_string().as_bytes(),
            ]));
        }
        self.count += 1;
    }
}

pub(super) fn write_command_snapshot(
    dbs: &DbSet,
    writer: &mut impl Write,
    key_delay_us: i64,
) -> std::io::Result<()> {
    let mut last_db = -1;
    for dbid in 0..dbs.num_dbs() as u32 {
        let Some(db) = dbs.get(dbid) else {
            continue;
        };
        let count = write_db_command_entries(
            db,
            |_key| true,
            writer,
            DbCommandSnapshotOptions::new(dbid, last_db != dbid as i32, key_delay_us),
        )?;
        if count != 0 {
            last_db = dbid as i32;
        }
    }
    Ok(())
}

pub(super) fn write_db_command_entries(
    db: &Db,
    include: impl FnMut(&[u8]) -> bool,
    writer: &mut impl Write,
    options: DbCommandSnapshotOptions,
) -> std::io::Result<usize> {
    write_db_command_entries_with_mode(db, include, writer, options, SnapshotEntryMode::Live)
}

pub(super) fn write_db_command_entries_including_expired(
    db: &Db,
    include: impl FnMut(&[u8]) -> bool,
    writer: &mut impl Write,
    options: DbCommandSnapshotOptions,
) -> std::io::Result<usize> {
    write_db_command_entries_with_mode(db, include, writer, options, SnapshotEntryMode::Physical)
}

fn write_db_command_entries_with_mode(
    db: &Db,
    mut include: impl FnMut(&[u8]) -> bool,
    writer: &mut impl Write,
    options: DbCommandSnapshotOptions,
    mode: SnapshotEntryMode,
) -> std::io::Result<usize> {
    let mut snapshot = DbCommandSnapshotBuffer {
        bytes: Vec::new(),
        count: 0,
        options,
    };
    let mut write_error = None;
    let mut encode_entry =
        |key: &[u8],
         value: &Robj,
         expire: Option<u64>,
         hash_field_expires: Option<&std::collections::HashMap<Vec<u8>, u64>>| {
            if write_error.is_none() && include(key) {
                snapshot.append(key, value, expire, hash_field_expires);
                // Keep the snapshot moving before delaying the next key, as
                // C's rio fd writer does for migration and AOF output.
                if snapshot.bytes.len() >= COMMAND_SNAPSHOT_BUFFER_BYTES {
                    if let Err(error) = writer.write_all(&snapshot.bytes) {
                        write_error = Some(error);
                        return;
                    }
                    snapshot.bytes.clear();
                }
                crate::foundation::io::persistence::serialize::delay_after_key_save(
                    options.key_delay_us,
                );
            }
        };
    match mode {
        SnapshotEntryMode::Live => db.for_each_entry_with_hash_field_expires(&mut encode_entry),
        SnapshotEntryMode::Physical => {
            db.for_each_entry_including_expired_with_hash_field_expires(&mut encode_entry)
        }
    }
    if let Some(error) = write_error {
        return Err(error);
    }
    if !snapshot.bytes.is_empty() {
        writer.write_all(&snapshot.bytes)?;
    }
    Ok(snapshot.count)
}

pub(super) fn write_function_snapshot(
    function_codes: &[Vec<u8>],
    writer: &mut impl Write,
) -> std::io::Result<()> {
    for code in function_codes {
        writer.write_all(&encode_command(&[b"FUNCTION", b"LOAD", code]))?;
    }
    Ok(())
}

fn serialize_key_value(
    out: &mut Vec<u8>,
    key: &[u8],
    value: &Robj,
    hash_field_expires: Option<&std::collections::HashMap<Vec<u8>, u64>>,
) {
    match value {
        Robj::StrInt(value) => append_string(out, key, value.as_bytes()),
        Robj::StrEmbstr(value) => append_string(out, key, value.as_bytes()),
        Robj::StrRaw(value) => append_string(out, key, value.as_bytes()),
        Robj::ListQuicklist(list) => {
            let mut parts: Vec<&[u8]> = vec![b"RPUSH", key];
            parts.extend(list.iter().map(|value| value.as_ref()));
            out.extend_from_slice(&encode_command(&parts));
        }
        Robj::ListListpack(list) => {
            let mut parts: Vec<&[u8]> = vec![b"RPUSH", key];
            let values = list.all_elements();
            parts.extend(values.iter().map(Vec::as_slice));
            out.extend_from_slice(&encode_command(&parts));
        }
        Robj::HashHashtable(hash) => {
            let mut parts: Vec<&[u8]> = vec![b"HSET", key];
            for (field, value) in hash.iter() {
                if let Some(timestamp) =
                    hash_field_expires.and_then(|expires| expires.get(field.as_ref()))
                {
                    let timestamp = timestamp.to_string();
                    out.extend_from_slice(&encode_command(&[
                        b"HSETEX",
                        key,
                        b"PXAT",
                        timestamp.as_bytes(),
                        b"FIELDS",
                        b"1",
                        field,
                        value,
                    ]));
                } else {
                    parts.push(field);
                    parts.push(value);
                }
            }
            if parts.len() > 2 {
                out.extend_from_slice(&encode_command(&parts));
            }
        }
        Robj::HashListpack(hash) => {
            let mut parts: Vec<&[u8]> = vec![b"HSET", key];
            let pairs = hash.all_pairs();
            for (field, value) in &pairs {
                parts.push(field);
                parts.push(value);
            }
            out.extend_from_slice(&encode_command(&parts));
        }
        Robj::SetHashtable(set) => {
            let mut parts: Vec<&[u8]> = vec![b"SADD", key];
            parts.extend(set.data.keys().map(|member| member.as_ref()));
            out.extend_from_slice(&encode_command(&parts));
        }
        Robj::SetIntset(set) => {
            let mut parts = vec![b"SADD".to_vec(), key.to_vec()];
            parts.extend(set.iter().map(|value| value.to_string().into_bytes()));
            let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
            out.extend_from_slice(&encode_command(&refs));
        }
        Robj::SetListpack(set) => {
            let mut parts: Vec<&[u8]> = vec![b"SADD", key];
            let members = set.all_members();
            parts.extend(members.iter().map(Vec::as_slice));
            out.extend_from_slice(&encode_command(&parts));
        }
        Robj::ZSetSkiplist(zset) => serialize_zset(out, key, zset),
        Robj::ZSetListpack(zset) => {
            let pairs = zset.all_pairs();
            let scores: Vec<Vec<u8>> = pairs
                .iter()
                .map(|(_, score)| score.to_string().into_bytes())
                .collect();
            let mut parts: Vec<&[u8]> = vec![b"ZADD", key];
            for ((member, _), score) in pairs.iter().zip(&scores) {
                parts.push(score);
                parts.push(member);
            }
            out.extend_from_slice(&encode_command(&parts));
        }
        Robj::StreamListpack(stream) => serialize_stream(out, key, stream),
        Robj::StreamRadix(_) => {}
    }
}

fn append_string(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    out.extend_from_slice(&encode_command(&[b"SET", key, value]));
}

fn serialize_zset(
    out: &mut Vec<u8>,
    key: &[u8],
    zset: &crate::foundation::io::store::structure::zset::ZSetSl,
) {
    let scores: Vec<Vec<u8>> = zset
        .dict()
        .values()
        .map(|score| format_double(*score).into_bytes())
        .collect();
    let mut parts: Vec<&[u8]> = vec![b"ZADD", key];
    for (index, (member, _)) in zset.dict().iter().enumerate() {
        parts.push(&scores[index]);
        parts.push(member);
    }
    out.extend_from_slice(&encode_command(&parts));
}

fn serialize_stream(
    out: &mut Vec<u8>,
    key: &[u8],
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
) {
    for (id, fields) in stream.iter() {
        let id = id.fmt_id();
        let mut parts: Vec<&[u8]> = vec![b"XADD", key, id.as_bytes()];
        for (field, value) in fields {
            parts.push(field.as_ref());
            parts.push(value.as_ref());
        }
        out.extend_from_slice(&encode_command(&parts));
    }
    append_stream_metadata(out, key, stream);
}

fn append_stream_metadata(
    out: &mut Vec<u8>,
    key: &[u8],
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
) {
    if stream.is_empty() {
        out.extend_from_slice(&encode_command(&[
            b"XADD",
            key,
            b"0-1",
            b"__valkey_empty_stream__",
            b"",
        ]));
        out.extend_from_slice(&encode_command(&[b"XTRIM", key, b"MAXLEN", b"=", b"0"]));
    }

    let last_id = stream.last_id.fmt_id();
    let entries_added = stream.entries_added.to_string();
    let max_deleted_id = stream.max_deleted_entry_id.fmt_id();
    out.extend_from_slice(&encode_command(&[
        b"XSETID",
        key,
        last_id.as_bytes(),
        b"ENTRIESADDED",
        entries_added.as_bytes(),
        b"MAXDELETEDID",
        max_deleted_id.as_bytes(),
    ]));
    append_stream_groups(out, key, stream);
}

fn append_stream_groups(
    out: &mut Vec<u8>,
    key: &[u8],
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
) {
    for group in stream.groups() {
        let last_id = group.last_id.fmt_id();
        let mut parts: Vec<&[u8]> = vec![
            b"XGROUP",
            b"CREATE",
            key,
            group.name.as_ref(),
            last_id.as_bytes(),
            b"MKSTREAM",
        ];
        let entries_read;
        if let Some(read) = group.entries_read {
            entries_read = read.to_string();
            parts.extend([b"ENTRIESREAD", entries_read.as_bytes()]);
        }
        out.extend_from_slice(&encode_command(&parts));

        for consumer in group.consumers.values() {
            out.extend_from_slice(&encode_command(&[
                b"XGROUP",
                b"CREATECONSUMER",
                key,
                group.name.as_ref(),
                consumer.name.as_ref(),
            ]));
        }
    }
}

fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{encode_command, write_db_command_entries, DbCommandSnapshotOptions};
    use crate::foundation::io::store::kv_engine::{now_ms, Db};
    use bytes::Bytes;

    #[test]
    fn command_snapshot_encodes_volatile_hash_fields_as_hsetex_pxat() {
        let mut db = Db::new();
        let expires_at = now_ms().saturating_add(60_000);
        let hash = db.get_or_create_hash(b"hash").expect("hash can be created");
        hash.insert(Bytes::from_static(b"volatile"), Bytes::from_static(b"v1"));
        hash.insert(Bytes::from_static(b"persistent"), Bytes::from_static(b"v2"));
        db.set_hash_field_expire(b"hash", b"volatile", expires_at);
        let mut output = Vec::new();

        write_db_command_entries(
            &db,
            |_| true,
            &mut output,
            DbCommandSnapshotOptions::new(0, false, 0),
        )
        .unwrap();

        let expected = encode_command(&[
            b"HSETEX",
            b"hash",
            b"PXAT",
            expires_at.to_string().as_bytes(),
            b"FIELDS",
            b"1",
            b"volatile",
            b"v1",
        ]);
        assert!(output
            .windows(expected.len())
            .any(|window| window == expected));
        assert!(!output.windows(10).any(|window| window == b"HPEXPIREAT"));
    }

    #[test]
    fn ordinary_command_snapshot_omits_expired_keys() {
        let mut db = Db::new();
        db.set(
            b"expired".to_vec(),
            crate::foundation::io::store::Robj::string("value"),
        );
        db.set_expire(b"expired", now_ms().saturating_sub(1));
        let mut output = Vec::new();

        let count = write_db_command_entries(
            &db,
            |_| true,
            &mut output,
            DbCommandSnapshotOptions::new(0, false, 0),
        )
        .unwrap();

        assert_eq!(count, 0);
        assert!(output.is_empty());
    }

    #[test]
    fn command_snapshot_writes_before_finishing_and_stops_on_write_error() {
        struct SnapshotWriter<'a> {
            processed: &'a std::cell::Cell<usize>,
            first_write_after: Option<usize>,
            should_fail: bool,
            bytes: Vec<u8>,
        }

        impl std::io::Write for SnapshotWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.first_write_after.get_or_insert(self.processed.get());
                if self.should_fail {
                    return Err(std::io::ErrorKind::BrokenPipe.into());
                }
                self.bytes.extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut db = Db::new();
        for index in 0..64 {
            db.set(
                format!("key-{index}").into_bytes(),
                crate::foundation::io::store::Robj::string("v".repeat(1024)),
            );
        }
        for should_fail in [false, true] {
            let processed = std::cell::Cell::new(0);
            let mut writer = SnapshotWriter {
                processed: &processed,
                first_write_after: None,
                should_fail,
                bytes: Vec::new(),
            };
            let result = write_db_command_entries(
                &db,
                |_| {
                    processed.set(processed.get() + 1);
                    true
                },
                &mut writer,
                DbCommandSnapshotOptions::new(0, true, 0),
            );

            assert!(writer.first_write_after.is_some_and(|count| count < 64));
            if should_fail {
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
                assert!(processed.get() < 64);
            } else {
                assert_eq!(result.unwrap(), 64);
                assert!(!writer.bytes.is_empty());
            }
        }
    }

    #[test]
    fn zset_snapshot_score_round_trips_exact_double() {
        use crate::foundation::memory::value::ZSetOps;
        let mut db = Db::new();
        let zset = db.get_or_create_zset(b"z").expect("zset can be created");
        let score = 0.014192543464802459_f64;
        ZSetOps::add(zset, b"m", score).expect("add member");

        let mut output = Vec::new();
        write_db_command_entries(
            &db,
            |_| true,
            &mut output,
            DbCommandSnapshotOptions::new(0, false, 0),
        )
        .unwrap();

        let text = String::from_utf8_lossy(&output);
        assert!(text.contains(score.to_string().as_str()));
        assert!(!text.contains("0.01419254346480246\r"));
    }
}
