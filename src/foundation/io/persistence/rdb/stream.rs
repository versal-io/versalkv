//! Resumable, object-boundary RDB decoder for the replica streaming full-sync
//! load. Reuses the buffer-based object loaders unchanged; decodes one complete
//! top-level record at a time out of a growing buffer and drains the consumed
//! prefix, so a paced load (`key-load-delay`) throttles how fast the caller
//! reads the socket. See ADR 2026-08-19-replica-streaming-rdb-load.

use super::LoadError;
use crate::foundation::io::persistence::codec::types::crc64;
use crate::foundation::io::store::kv_engine::DbSet;
use bytes::Bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiredKeyPolicy {
    Retain,
    Skip,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RdbDecodePolicy {
    pub strict_version: bool,
    pub allow_duplicate_keys: bool,
    pub expired_keys: ExpiredKeyPolicy,
    pub key_load_delay_us: i64,
}

impl RdbDecodePolicy {
    pub fn replication(strict_version: bool, key_load_delay_us: i64) -> Self {
        Self {
            strict_version,
            allow_duplicate_keys: false,
            expired_keys: ExpiredKeyPolicy::Retain,
            key_load_delay_us,
        }
    }

    pub fn persisted(strict_version: bool, key_load_delay_us: i64) -> Self {
        Self {
            strict_version,
            allow_duplicate_keys: false,
            expired_keys: ExpiredKeyPolicy::Skip,
            key_load_delay_us,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredHashFields {
    pub db: u32,
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RdbExpiryEvent {
    HashFields(ExpiredHashFields),
    Key { db: u32, key: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RdbDecodedMetadata {
    pub loaded: usize,
    pub consumed: usize,
    pub expired: usize,
    pub empty_keys: usize,
    pub function_codes: Vec<Vec<u8>>,
    pub replication: Option<super::serialize::RdbReplicationInfo>,
    pub expiry_events: Vec<RdbExpiryEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RdbStep {
    NeedInput,
    RecordApplied,
    Finished(RdbDecodedMetadata),
}

#[derive(Debug, PartialEq, Eq)]
pub enum StreamStep {
    /// Buffer exhausted before a full record; caller must `feed` more bytes.
    NeedMore,
    /// Reached RDB_OPCODE_EOF and verified the trailing CRC.
    Finished,
}

pub struct RdbDecoder {
    buf: Vec<u8>,
    cursor: usize,
    policy: RdbDecodePolicy,
    header_done: bool,
    rdbver: i32,
    is_redis_magic: bool,
    cur_db: u32,
    expire_ms: Option<u64>,
    crc: u64,
    loaded: usize,
    function_codes: Vec<Vec<u8>>,
    aux_fields: Vec<(Vec<u8>, Vec<u8>)>,
    repl_stream_db: Option<u32>,
    repl_id: Option<String>,
    repl_offset: Option<i64>,
    expired: usize,
    empty_keys: usize,
    expiry_events: Vec<RdbExpiryEvent>,
    discarded_prefix: usize,
    finished: bool,
}

impl RdbDecoder {
    pub fn new(strict: bool) -> Self {
        Self::with_policy(RdbDecodePolicy::replication(strict, 0))
    }

    pub fn with_policy(policy: RdbDecodePolicy) -> Self {
        Self {
            buf: Vec::new(),
            cursor: 0,
            policy,
            header_done: false,
            rdbver: 0,
            is_redis_magic: false,
            cur_db: 0,
            expire_ms: None,
            crc: 0,
            loaded: 0,
            function_codes: Vec::new(),
            aux_fields: Vec::new(),
            repl_stream_db: None,
            repl_id: None,
            repl_offset: None,
            expired: 0,
            empty_keys: 0,
            expiry_events: Vec::new(),
            discarded_prefix: 0,
            finished: false,
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        if self.cursor == self.buf.len() {
            self.discarded_prefix = self.discarded_prefix.saturating_add(self.cursor);
            self.buf.clear();
            self.cursor = 0;
        } else if self.cursor > 0 {
            self.discarded_prefix = self.discarded_prefix.saturating_add(self.cursor);
            self.buf.drain(..self.cursor);
            self.cursor = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    pub fn loaded(&self) -> usize {
        self.loaded
    }

    pub fn function_codes(&self) -> &[Vec<u8>] {
        &self.function_codes
    }

    /// Move the accumulated FUNCTION library sources out of the decoder for the
    /// terminal streaming commit, leaving the decoder's list empty.
    pub fn take_function_codes(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.function_codes)
    }

    /// Move subsystem-specific replication AUX fields out of the decoder.
    pub fn take_aux_fields(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        std::mem::take(&mut self.aux_fields)
    }

    pub fn repl_stream_db(&self) -> Option<u32> {
        self.repl_stream_db
    }

    /// Bytes fed but not yet consumed by `drive`. Test-only, used by the link
    /// framing tests to assert exactly the RDB body (never the EOF marker) is
    /// forwarded to the decoder.
    #[cfg(test)]
    pub fn buffered(&self) -> &[u8] {
        &self.buf[self.cursor..]
    }

    #[cfg(test)]
    pub fn consumed_prefix_len(&self) -> usize {
        self.cursor
    }

    /// Parse the 9-byte magic+version header. Mirrors core/rdb.rs:103-117.
    /// Returns Ok(true) when the header was consumed, Ok(false) when more bytes
    /// are needed, Err on a bad/incompatible header.
    fn parse_header(&mut self) -> Result<bool, LoadError> {
        let input = &self.buf[self.cursor..];
        if input.len() < 9 {
            return Ok(false);
        }
        let is_redis = &input[..6] == b"REDIS0";
        let is_valkey = &input[..6] == b"VALKEY";
        if !is_redis && !is_valkey {
            return Err(LoadError::IncompatibleVersion(-1));
        }
        let rdbver: i32 = std::str::from_utf8(&input[6..9])
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
            .unwrap_or(-1);
        if !super::is_version_accepted(rdbver, is_valkey, is_redis, self.policy.strict_version) {
            return Err(LoadError::IncompatibleVersion(rdbver));
        }
        self.crc = crc64(self.crc, &input[..9]);
        self.cursor += 9;
        self.rdbver = rdbver;
        self.is_redis_magic = is_redis;
        self.header_done = true;
        Ok(true)
    }

    fn metadata(&self) -> RdbDecodedMetadata {
        let replication = self
            .repl_stream_db
            .zip(self.repl_id.clone())
            .zip(self.repl_offset)
            .map(
                |((repl_stream_db, repl_id), repl_offset)| super::serialize::RdbReplicationInfo {
                    repl_stream_db: i64::from(repl_stream_db),
                    repl_id,
                    repl_offset,
                    aux_fields: self.aux_fields.clone(),
                },
            );
        RdbDecodedMetadata {
            loaded: self.loaded,
            consumed: self.discarded_prefix.saturating_add(self.cursor),
            expired: self.expired,
            empty_keys: self.empty_keys,
            function_codes: self.function_codes.clone(),
            replication,
            expiry_events: self.expiry_events.clone(),
        }
    }

    fn apply_aux_field(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if key.eq_ignore_ascii_case(b"redis-ver") {
            #[cfg(not(feature = "wasm"))]
            crate::server_log!(
                notice,
                "Loading RDB produced by Redis version {}",
                String::from_utf8_lossy(&value)
            );
        } else if key.eq_ignore_ascii_case(b"valkey-ver") {
            #[cfg(not(feature = "wasm"))]
            crate::server_log!(
                notice,
                "Loading RDB produced by Valkey version {}",
                String::from_utf8_lossy(&value)
            );
        } else if key.eq_ignore_ascii_case(b"repl-stream-db") {
            self.repl_stream_db = std::str::from_utf8(&value)
                .ok()
                .and_then(|value| value.parse::<u32>().ok());
        } else if key.eq_ignore_ascii_case(b"repl-id") {
            self.repl_id = std::str::from_utf8(&value)
                .ok()
                .filter(|value| {
                    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                .map(str::to_owned);
        } else if key.eq_ignore_ascii_case(b"repl-offset") {
            self.repl_offset = std::str::from_utf8(&value)
                .ok()
                .and_then(|value| value.parse::<i64>().ok());
        } else if !super::is_standard_aux_field(&key) {
            self.aux_fields.push((key, value));
        }
    }

    fn step_object_record(
        &mut self,
        dbs: &mut DbSet,
        type_byte: u8,
        now: u64,
    ) -> Result<RdbStep, LoadError> {
        use crate::foundation::io::persistence::codec;
        use crate::foundation::io::persistence::codec::decoder::load_string;
        use crate::foundation::io::persistence::codec::types::is_known_object_type;

        if !is_known_object_type(type_byte) {
            return Err(LoadError::UnknownType);
        }

        let input = &self.buf[self.cursor..];
        let mut pos = 1usize;
        let Some(key) = load_string(input, &mut pos) else {
            return Ok(RdbStep::NeedInput);
        };
        let hash_field_expiry_cutoff = match self.policy.expired_keys {
            ExpiredKeyPolicy::Retain => None,
            ExpiredKeyPolicy::Skip => Some(now),
        };
        let Some(loaded) = codec::load_robj_with_hash_field_expires_detailed(
            input,
            &mut pos,
            type_byte,
            hash_field_expiry_cutoff,
        ) else {
            return Ok(RdbStep::NeedInput);
        };

        let expired_hash_fields =
            (!loaded.skipped_expired_hash_fields.is_empty()).then(|| ExpiredHashFields {
                db: self.cur_db,
                key: Bytes::copy_from_slice(&key),
                fields: loaded.skipped_expired_hash_fields,
            });
        let val = loaded.object;
        let hfe = loaded.hash_field_expires;
        let expire = self.expire_ms.take();

        // A replica must retain keys that are already expired in the primary's
        // snapshot. The primary owns expiration and will stream the corresponding
        // deletion; dropping the key here would make the in-flight snapshot differ.
        let mut expired_key = None;
        let applied_key = if val.is_empty_rdb_key() {
            self.empty_keys += 1;
            log::warn!("skipping empty key: {}", String::from_utf8_lossy(&key));
            false
        } else if expire.is_some_and(|deadline| deadline <= now)
            && self.policy.expired_keys == ExpiredKeyPolicy::Skip
        {
            self.expired += 1;
            expired_key = Some((self.cur_db, key));
            false
        } else {
            let db = dbs.get_mut(self.cur_db).ok_or(LoadError::Corrupt)?;
            if !self.policy.allow_duplicate_keys && db.get_allow_expired(&key).is_some() {
                return Err(LoadError::Corrupt);
            }
            db.load_entry_with_hash_field_expires(key, val, expire, hfe);
            self.loaded += 1;
            true
        };

        if let Some(expired_hash_fields) = expired_hash_fields {
            self.expiry_events
                .push(RdbExpiryEvent::HashFields(expired_hash_fields));
        }
        if let Some((db, key)) = expired_key {
            self.expiry_events.push(RdbExpiryEvent::Key { db, key });
        }
        self.crc = crc64(self.crc, &input[..pos]);
        self.cursor += pos;

        if applied_key && self.policy.key_load_delay_us != 0 {
            #[cfg(not(feature = "wasm"))]
            crate::foundation::io::sys::time::sleep_debug_delay_interruptible(
                self.policy.key_load_delay_us,
                || false,
            );
        }
        Ok(RdbStep::RecordApplied)
    }

    /// Decode at most one complete top-level record from the current input.
    /// Resumable hoist of `core/rdb.rs:126-223`. Snapshots `pos` per record; on a
    /// short buffer a loader returns `None`, so we restore `pos` and return
    /// `NeedMore` without mutating decoder state. On a full record we advance the
    /// rolling CRC over the consumed run, advance a cursor, and (for applied
    /// keys) sleep `key_load_delay_us` inline — so the caller naturally stops
    /// reading the socket while it sleeps.
    pub fn step(&mut self, dbs: &mut DbSet) -> Result<RdbStep, LoadError> {
        use crate::foundation::io::persistence::codec::decoder::load_string;
        use crate::foundation::io::persistence::codec::types::{
            load_len, RDB_FOREIGN_TYPE_MAX, RDB_FOREIGN_TYPE_MIN, RDB_OPCODE_AUX, RDB_OPCODE_EOF,
            RDB_OPCODE_EXPIRETIME, RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_FREQ, RDB_OPCODE_FUNCTION2,
            RDB_OPCODE_IDLE, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB,
        };

        if !self.header_done && !self.parse_header()? {
            return Ok(RdbStep::NeedInput);
        }
        if self.finished {
            return Ok(RdbStep::Finished(self.metadata()));
        }
        let now = crate::foundation::io::store::kv_engine::now_ms();

        {
            // Try to decode exactly one top-level record starting at the cursor.
            let input = &self.buf[self.cursor..];
            let mut pos = 0usize;
            let Some(&op) = input.first() else {
                return Ok(RdbStep::NeedInput);
            };
            pos += 1;
            if self.is_redis_magic && (RDB_FOREIGN_TYPE_MIN..=RDB_FOREIGN_TYPE_MAX).contains(&op) {
                return Err(LoadError::ForeignType {
                    type_byte: op,
                    version: self.rdbver,
                });
            }

            // `need_more!()` returns from the whole function; it must only fire
            // before any decoder-state mutation so a re-drive replays the record.
            macro_rules! need_more {
                () => {{
                    return Ok(RdbStep::NeedInput);
                }};
            }

            let mut pending_aux_field = None;
            match op {
                RDB_OPCODE_EOF => {
                    // Trailing CRC (rdbver >= 5): need 8 more bytes.
                    if self.rdbver >= 5 {
                        if input.len() < pos + 8 {
                            need_more!();
                        }
                        let stored = u64::from_le_bytes(input[pos..pos + 8].try_into().unwrap());
                        // Advance CRC over the EOF opcode byte only, then compare.
                        let crc = crc64(self.crc, &input[..pos]);
                        if stored != 0 && crc != stored {
                            return Err(LoadError::CrcError);
                        }
                        pos += 8;
                    } else {
                        self.crc = crc64(self.crc, &input[..pos]);
                    }
                    self.cursor += pos;
                    self.finished = true;
                    return Ok(RdbStep::Finished(self.metadata()));
                }
                RDB_OPCODE_SELECTDB => {
                    let Some((n, enc)) = load_len(input, &mut pos) else {
                        need_more!()
                    };
                    if enc {
                        return Err(LoadError::Corrupt);
                    }
                    self.cur_db = n as u32;
                }
                RDB_OPCODE_RESIZEDB => {
                    if load_len(input, &mut pos).is_none() {
                        need_more!()
                    }
                    if load_len(input, &mut pos).is_none() {
                        need_more!()
                    }
                }
                RDB_OPCODE_AUX => {
                    let Some(key) = load_string(input, &mut pos) else {
                        need_more!()
                    };
                    let Some(value) = load_string(input, &mut pos) else {
                        need_more!()
                    };
                    pending_aux_field = Some((key, value));
                }
                RDB_OPCODE_FUNCTION2 => {
                    let Some(code) = load_string(input, &mut pos) else {
                        need_more!()
                    };
                    self.function_codes.push(code);
                }
                RDB_OPCODE_EXPIRETIME_MS => {
                    if input.len() < pos + 8 {
                        need_more!()
                    }
                    let raw: [u8; 8] = input[pos..pos + 8].try_into().unwrap();
                    pos += 8;
                    self.expire_ms = Some(i64::from_le_bytes(raw) as u64);
                }
                RDB_OPCODE_EXPIRETIME => {
                    if input.len() < pos + 4 {
                        need_more!()
                    }
                    let raw: [u8; 4] = input[pos..pos + 4].try_into().unwrap();
                    pos += 4;
                    self.expire_ms = Some((i32::from_le_bytes(raw) as u64) * 1000);
                }
                RDB_OPCODE_IDLE => {
                    if load_len(input, &mut pos).is_none() {
                        need_more!()
                    }
                }
                RDB_OPCODE_FREQ => {
                    if input.len() < pos + 1 {
                        need_more!()
                    }
                    pos += 1;
                }
                type_byte => {
                    return self.step_object_record(dbs, type_byte, now);
                }
            }

            // Record fully consumed: advance CRC and cursor. `feed` compacts
            // this prefix once before appending more network bytes.
            self.crc = crc64(self.crc, &input[..pos]);
            self.cursor += pos;
            if let Some((key, value)) = pending_aux_field {
                self.apply_aux_field(key, value);
            }

            Ok(RdbStep::RecordApplied)
        }
    }

    /// Compatibility driver for replication callers that want to consume all
    /// complete records currently buffered before returning to socket IO.
    pub fn drive(
        &mut self,
        dbs: &mut DbSet,
        key_load_delay_us: i64,
    ) -> Result<StreamStep, LoadError> {
        self.policy.key_load_delay_us = key_load_delay_us;
        loop {
            match self.step(dbs)? {
                RdbStep::NeedInput => return Ok(StreamStep::NeedMore),
                RdbStep::RecordApplied => {}
                RdbStep::Finished(_) => return Ok(StreamStep::Finished),
            }
        }
    }
}

pub type RdbStreamDecoder = RdbDecoder;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::{
        load_rdb_bytes_with_policy, serialize_all, ExpiredHashFields, RdbDecodedMetadata,
        RdbExpiryEvent,
    };
    use crate::foundation::io::store::kv_engine::DbSet;
    use bytes::Bytes;

    fn hash_snapshot_with_expired_field() -> Vec<u8> {
        let mut source = DbSet::new(1);
        let db = source.get_mut(0).expect("DB 0 exists");
        let hash = db
            .get_or_create_hash(b"hash")
            .expect("new hash can be created");
        hash.insert(Bytes::from_static(b"expired"), Bytes::from_static(b"old"));
        hash.insert(Bytes::from_static(b"live"), Bytes::from_static(b"new"));
        db.set_hash_field_expire(b"hash", b"expired", 1);
        db.set_hash_field_expire(
            b"hash",
            b"live",
            crate::foundation::io::store::kv_engine::now_ms() + 60_000,
        );
        serialize_all(&source, 0)
    }

    fn decode_hash_snapshot(policy: RdbDecodePolicy) -> (DbSet, RdbDecodedMetadata) {
        let bytes = hash_snapshot_with_expired_field();
        let mut destination = DbSet::new(1);
        let metadata = load_rdb_bytes_with_policy(&bytes, &mut destination, policy)
            .expect("HASH_2 snapshot should decode");
        (destination, metadata)
    }

    fn expired_hash_snapshot_with_expired_field() -> Vec<u8> {
        let mut source = DbSet::new(1);
        let db = source.get_mut(0).expect("DB 0 exists");
        let hash = db
            .get_or_create_hash(b"hash")
            .expect("new hash can be created");
        hash.insert(Bytes::from_static(b"expired"), Bytes::from_static(b"old"));
        hash.insert(Bytes::from_static(b"live"), Bytes::from_static(b"new"));
        db.set_hash_field_expire(b"hash", b"expired", 1);
        db.set_hash_field_expire(
            b"hash",
            b"live",
            crate::foundation::io::store::kv_engine::now_ms() + 60_000,
        );
        db.set_no_lazy_expire(true);
        db.set_expire(b"hash", 1);
        serialize_all(&source, 0)
    }

    fn hash_snapshot_with_multiple_expired_fields() -> Vec<u8> {
        let mut source = DbSet::new(1);
        let db = source.get_mut(0).expect("DB 0 exists");
        let hash = db
            .get_or_create_hash(b"hash")
            .expect("new hash can be created");
        hash.insert(
            Bytes::from_static(b"expired-first"),
            Bytes::from_static(b"old-1"),
        );
        hash.insert(Bytes::from_static(b"live"), Bytes::from_static(b"new"));
        hash.insert(
            Bytes::from_static(b"expired-second"),
            Bytes::from_static(b"old-2"),
        );
        db.set_hash_field_expire(b"hash", b"expired-first", 1);
        db.set_hash_field_expire(
            b"hash",
            b"live",
            crate::foundation::io::store::kv_engine::now_ms() + 60_000,
        );
        db.set_hash_field_expire(b"hash", b"expired-second", 2);
        serialize_all(&source, 0)
    }

    #[test]
    fn header_needs_nine_bytes_then_validates() {
        let mut d = RdbStreamDecoder::new(true);
        let mut dbs = DbSet::new(16);
        // Fewer than 9 bytes: NeedMore, no error.
        d.feed(b"REDIS");
        assert!(matches!(d.drive(&mut dbs, 0), Ok(StreamStep::NeedMore)));
        // Bad magic once we have 9 bytes: Err.
        let mut bad = RdbStreamDecoder::new(true);
        bad.feed(b"NOPE00011");
        assert!(bad.drive(&mut dbs, 0).is_err());
    }

    #[test]
    fn decodes_single_string_key_when_fully_fed() {
        use crate::foundation::io::store::Robj;
        // Build a real RDB via the existing serializer so the bytes are canonical.
        let mut src = DbSet::new(16);
        src.get_mut(0)
            .unwrap()
            .set(b"k".to_vec(), Robj::string("v"));
        let bytes = crate::foundation::io::persistence::serialize_all(&src, 0);

        let mut d = RdbStreamDecoder::new(true);
        let mut dst = DbSet::new(16);
        d.feed(&bytes);
        let step = d.drive(&mut dst, 0).unwrap();
        assert_eq!(step, StreamStep::Finished);
        assert_eq!(d.loaded(), 1);
        assert_eq!(
            dst.get(0).unwrap().get_no_expire(b"k").unwrap().as_bytes(),
            b"v"
        );
    }

    #[test]
    fn decodes_identically_when_fed_one_byte_at_a_time() {
        use crate::foundation::io::store::Robj;
        let mut src = DbSet::new(16);
        {
            let db = src.get_mut(0).unwrap();
            db.set(b"s".to_vec(), Robj::string("hello"));
            // A large raw string forces the raw path / multi-record buffer.
            db.set(b"s2".to_vec(), Robj::string_raw("x".repeat(5000)));
        }
        let bytes = crate::foundation::io::persistence::serialize_all(&src, 0);

        let mut d = RdbStreamDecoder::new(true);
        let mut dst = DbSet::new(16);
        let mut step = StreamStep::NeedMore;
        for b in &bytes {
            d.feed(&[*b]);
            step = d.drive(&mut dst, 0).unwrap();
        }
        assert_eq!(step, StreamStep::Finished);
        let db = dst.get(0).unwrap();
        assert_eq!(db.get_no_expire(b"s").unwrap().as_bytes(), b"hello");
        assert_eq!(db.get_no_expire(b"s2").unwrap().as_bytes().len(), 5000);
    }

    #[test]
    fn complete_records_advance_a_cursor_without_shifting_the_input_buffer() {
        use crate::foundation::io::store::Robj;

        let mut src = DbSet::new(16);
        let db = src.get_mut(0).unwrap();
        for index in 0..512 {
            db.set(
                format!("key-{index:04}").into_bytes(),
                Robj::string("value"),
            );
        }
        let bytes = crate::foundation::io::persistence::serialize_all(&src, 0);
        let records_without_eof = &bytes[..bytes.len() - 9];

        let mut decoder = RdbStreamDecoder::new(true);
        let mut dst = DbSet::new(16);
        decoder.feed(records_without_eof);

        assert_eq!(decoder.drive(&mut dst, 0).unwrap(), StreamStep::NeedMore);
        assert!(
            decoder.consumed_prefix_len() > 0,
            "the decoder must advance a cursor for complete records instead of draining each record"
        );
        assert_eq!(dst.get(0).unwrap().dbsize(), 512);
    }

    #[test]
    fn rdb_decoder_tracks_expired_hash_fields_for_persisted_load() {
        let (destination, metadata) = decode_hash_snapshot(RdbDecodePolicy::persisted(true, 0));
        let cloned_metadata = metadata.clone();

        assert_eq!(
            metadata.expiry_events,
            vec![RdbExpiryEvent::HashFields(ExpiredHashFields {
                db: 0,
                key: Bytes::from_static(b"hash"),
                fields: vec![Bytes::from_static(b"expired")],
            })]
        );
        let RdbExpiryEvent::HashFields(metadata_event) = &metadata.expiry_events[0] else {
            panic!("expected grouped hash-field expiry event");
        };
        let RdbExpiryEvent::HashFields(cloned_event) = &cloned_metadata.expiry_events[0] else {
            panic!("expected cloned grouped hash-field expiry event");
        };
        assert_eq!(metadata_event.key.as_ptr(), cloned_event.key.as_ptr());
        assert_eq!(
            metadata_event.fields[0].as_ptr(),
            cloned_event.fields[0].as_ptr()
        );
        let hash = destination
            .get(0)
            .expect("DB 0 exists")
            .get_allow_expired(b"hash")
            .expect("hash restored");
        assert!(!hash.hash_contains(b"expired"));
        assert!(hash.hash_contains(b"live"));
    }

    #[test]
    fn rdb_decoder_tracks_expired_hash_fields_for_replication_load() {
        let (destination, metadata) = decode_hash_snapshot(RdbDecodePolicy::replication(true, 0));

        assert!(metadata.expiry_events.is_empty());
        let hash = destination
            .get(0)
            .expect("DB 0 exists")
            .get_allow_expired(b"hash")
            .expect("hash restored");
        assert!(hash.hash_contains(b"expired"));
        assert!(hash.hash_contains(b"live"));
    }

    #[test]
    fn rdb_decoder_orders_hash_field_expiry_before_its_expired_key() {
        let bytes = expired_hash_snapshot_with_expired_field();
        let mut destination = DbSet::new(1);
        let metadata = load_rdb_bytes_with_policy(
            &bytes,
            &mut destination,
            RdbDecodePolicy::persisted(true, 0),
        )
        .expect("expired HASH_2 snapshot should decode");

        assert_eq!(
            metadata.expiry_events,
            vec![
                RdbExpiryEvent::HashFields(ExpiredHashFields {
                    db: 0,
                    key: Bytes::from_static(b"hash"),
                    fields: vec![Bytes::from_static(b"expired")],
                }),
                RdbExpiryEvent::Key {
                    db: 0,
                    key: b"hash".to_vec(),
                },
            ]
        );
        assert!(destination
            .get(0)
            .expect("DB 0 exists")
            .get_allow_expired(b"hash")
            .is_none());
    }

    #[test]
    fn rdb_decoder_tracks_expired_hash_fields_across_fragmented_input() {
        let bytes = hash_snapshot_with_multiple_expired_fields();
        let mut decoder = RdbDecoder::with_policy(RdbDecodePolicy::persisted(true, 0));
        let mut destination = DbSet::new(1);
        let mut finished = None;

        for byte in bytes {
            decoder.feed(&[byte]);
            loop {
                match decoder.step(&mut destination).expect("decode RDB byte") {
                    RdbStep::NeedInput => break,
                    RdbStep::RecordApplied => {}
                    RdbStep::Finished(metadata) => {
                        finished = Some(metadata);
                        break;
                    }
                }
            }
            if finished.is_some() {
                break;
            }
        }

        let metadata = finished.expect("fragmented RDB should finish");
        assert_eq!(
            metadata.expiry_events,
            vec![RdbExpiryEvent::HashFields(ExpiredHashFields {
                db: 0,
                key: Bytes::from_static(b"hash"),
                fields: vec![
                    Bytes::from_static(b"expired-first"),
                    Bytes::from_static(b"expired-second"),
                ],
            })]
        );
        let hash = destination
            .get(0)
            .expect("DB 0 exists")
            .get_allow_expired(b"hash")
            .expect("hash restored");
        assert!(!hash.hash_contains(b"expired-first"));
        assert!(hash.hash_contains(b"live"));
        assert!(!hash.hash_contains(b"expired-second"));
    }
}
