//! Streaming full-sync RDB receive state, owned by `ReplicaLink` for the
//! duration of an RDB phase. `ReplicaLink` framing forwards RDB-body bytes here
//! (never the diskless EOF marker); the backend supplies the target `DbSet` and
//! drives the paced decode. Disk-backed loads retain the whole body until the
//! transfer completes before decoding, matching C's file download/load split.
//! `raw` is retained for disk-backed persistence and non-diskless transfers.
//! Ownership seam: the decoder needs a `DbSet` (owned by `ServerContext`, not
//! the link), so the actual `drive`/commit happens in the backend receive path
//! (`runtime/distributed/replication/rdb_recv_path.rs`); the link only `feed`s bytes.

use crate::foundation::io::compression::{StreamDecoder, StreamKind, REPLICATION_BATCH_BYTES};
use crate::foundation::io::persistence::rdb::compression::{
    decode_rdb_stream, encode_rdb_stream, RdbCompression, RdbSnapshotOptions,
};
use crate::foundation::io::persistence::{LoadError, RdbStep, RdbStreamDecoder, StreamStep};
use crate::foundation::io::store::kv_engine::DbSet;
use crate::framework::distributed::replication::RdbLoadMode;

/// Where decoded records land. Chosen by the backend on the first drive once it
/// can read `repl-diskless-load` (mirrors replica_apply's swapdb vs. flush+load
/// split): `Swap` decodes into a fresh DbSet swapped in atomically at EOF;
/// `Direct` decodes into the serving DbSet after a one-shot flush.
enum RdbTarget {
    /// Not yet configured; the backend flushes/allocates on the first drive.
    Uninit,
    /// `repl-diskless-load swapdb`: decode into this temp, swap in at EOF.
    Swap(DbSet),
    /// Default: decode straight into the serving DbSet (already flushed).
    Direct,
}

/// The committed outcome of a streaming full-sync load, handed to the backend's
/// terminal commit once the decoder reaches EOF.
pub struct RdbCommit {
    pub function_codes: Vec<Vec<u8>>,
    pub aux_fields: Vec<(Vec<u8>, Vec<u8>)>,
    pub repl_stream_db: u32,
    pub diskless: bool,
    pub disk_backed: bool,
    /// `Some` only for the AOF-preamble-reuse path; otherwise `None`.
    pub raw: Option<Vec<u8>>,
    /// `Some` for `swapdb`: the freshly loaded DbSet to swap in.
    pub swapped: Option<DbSet>,
}

pub struct RdbReceive {
    decoder: RdbStreamDecoder,
    compression: StreamDecoder,
    wire: Vec<u8>,
    snapshot_options: Option<RdbSnapshotOptions>,
    skip_checksums: bool,
    decode_budget: usize,
    diskless: bool,
    raw: Option<Vec<u8>>,
    load_mode: RdbLoadMode,
    target: RdbTarget,
    received_payload: bool,
    transfer_started: bool,
    payload_bytes: u64,
    compression_logged: bool,
    input_complete: bool,
}

impl RdbReceive {
    pub fn new(diskless: bool, load_mode: RdbLoadMode) -> Self {
        Self {
            // Replica full-sync load uses non-strict version acceptance, matching
            // the buffer path (`load_..._and_delay(.., false, ..)`).
            decoder: RdbStreamDecoder::new(false),
            compression: StreamDecoder::new(StreamKind::Rdb, false),
            wire: Vec::new(),
            snapshot_options: None,
            skip_checksums: false,
            decode_budget: REPLICATION_BATCH_BYTES,
            diskless,
            raw: if load_mode == RdbLoadMode::DiskBacked || !diskless {
                Some(Vec::new())
            } else {
                None
            },
            load_mode,
            target: RdbTarget::Uninit,
            received_payload: false,
            transfer_started: false,
            payload_bytes: 0,
            compression_logged: false,
            input_complete: false,
        }
    }

    /// Feed newly framed RDB-body bytes; mirror into `raw` when retained.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.received_payload |= !bytes.is_empty();
        self.payload_bytes = self.payload_bytes.saturating_add(bytes.len() as u64);
        if let Some(raw) = self.raw.as_mut() {
            raw.extend_from_slice(bytes);
        }
        self.wire.extend_from_slice(bytes);
    }

    pub fn set_snapshot_options(&mut self, options: RdbSnapshotOptions) {
        self.snapshot_options.get_or_insert(options);
    }

    pub fn with_skip_checksums(mut self, skip_checksums: bool) -> Self {
        self.skip_checksums = skip_checksums;
        self.compression = StreamDecoder::new(StreamKind::Rdb, skip_checksums);
        if skip_checksums {
            self.decoder.skip_checksum();
        }
        self
    }

    pub fn has_pending_decompression(&self) -> bool {
        !self.wire.is_empty() || self.compression.has_pending_decode()
    }

    pub fn compression_memory_bytes(&self) -> usize {
        self.compression.memory_bytes() + self.wire.capacity()
    }

    fn decompress(&mut self) -> Result<(), LoadError> {
        let input = std::mem::take(&mut self.wire);
        let decoded =
            self.compression
                .feed(&input, self.decode_budget)
                .map_err(|error| match error {
                    crate::foundation::io::compression::DecodeError::Incompatible => {
                        LoadError::IncompatibleVersion(-1)
                    }
                    _ => LoadError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Corrupt compressed RDB stream",
                    )),
                })?;
        self.decode_budget = self.decode_budget.saturating_sub(decoded.len());
        if self.compression.is_compressed() {
            if !self.compression_logged {
                self.compression_logged = true;
                if self.load_mode == RdbLoadMode::DiskBacked {
                    crate::server_log!(
                        notice,
                        "Loading compressed RDB (algo=lz4) from temp-replication.rdb"
                    );
                } else {
                    crate::server_log!(notice, "Loading compressed RDB (algo=lz4) from primary");
                }
            }
            self.decoder.skip_checksum();
        }
        self.decoder.feed(&decoded);
        Ok(())
    }

    pub fn begin_decode_turn(&mut self) {
        self.decode_budget = REPLICATION_BATCH_BYTES;
    }

    pub fn finish_input(&mut self) {
        self.input_complete = true;
    }

    pub fn validate_complete(&mut self) -> Result<(), LoadError> {
        self.compression.finish().map_err(|_| {
            LoadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Compressed RDB stream ended before the announced transfer size",
            ))
        })?;
        if self.compression.is_compressed()
            && (self.decoder.unconsumed_bytes() != 0
                || self.compression.physical_bytes() as u64 != self.payload_bytes)
        {
            return Err(LoadError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Compressed RDB stream ended before the announced transfer size",
            )));
        }
        let Some(raw) = self.raw.as_mut() else {
            return Ok(());
        };
        let Some(options) = self.snapshot_options else {
            return Ok(());
        };
        let incoming_lz4 = raw.starts_with(b"VCS");
        if incoming_lz4 == (options.compression == RdbCompression::Lz4) {
            return Ok(());
        }
        let mut logical = if incoming_lz4 {
            decode_rdb_stream(raw, self.skip_checksums)
                .map_err(|_| LoadError::Corrupt)?
                .ok_or(LoadError::Corrupt)?
                .bytes
        } else {
            std::mem::take(raw)
        };
        let end = logical.len().checked_sub(8).ok_or(LoadError::Corrupt)?;
        let checksum = if options.checksum && options.compression != RdbCompression::Lz4 {
            crate::foundation::io::persistence::crc64(0, &logical[..end])
        } else {
            0
        };
        logical[end..].copy_from_slice(&checksum.to_le_bytes());
        *raw = if options.compression == RdbCompression::Lz4 {
            encode_rdb_stream(&logical, options.checksum).map_err(LoadError::Io)?
        } else {
            logical
        };
        Ok(())
    }

    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub fn is_uninit(&self) -> bool {
        matches!(self.target, RdbTarget::Uninit)
    }

    pub fn preserves_serving_db(&self) -> bool {
        !matches!(self.target, RdbTarget::Direct)
    }

    /// A full-sync header alone only enters transfer state. Socket loading
    /// starts with the first payload; disk-backed loading waits for the complete
    /// transfer. The backend selects the loading gate from the pinned load mode.
    pub fn has_received_payload(&self) -> bool {
        self.received_payload
    }

    /// Report the first payload transition once, independently of whether the
    /// backend has begun decoding or installed a target database.
    pub fn mark_transfer_started(&mut self) -> bool {
        if !self.received_payload || self.transfer_started {
            return false;
        }
        self.transfer_started = true;
        true
    }

    pub fn set_swap(&mut self, temp: DbSet) {
        self.target = RdbTarget::Swap(temp);
    }

    pub fn set_direct(&mut self) {
        self.target = RdbTarget::Direct;
    }

    pub fn set_zset_limits(&mut self, max_entries: usize, max_value_size: usize) {
        self.decoder.set_zset_limits(max_entries, max_value_size);
    }

    /// Decode one record so the backend can service events between records.
    pub fn step(
        &mut self,
        serving: &mut DbSet,
        key_load_delay_us: i64,
    ) -> Result<RdbStep, LoadError> {
        self.decoder.set_key_load_delay(key_load_delay_us);
        loop {
            let step = match &mut self.target {
                RdbTarget::Swap(temp) => self.decoder.step(temp),
                RdbTarget::Direct => self.decoder.step(serving),
                RdbTarget::Uninit => return Ok(RdbStep::NeedInput),
            }?;
            if matches!(step, RdbStep::Finished(_)) && self.compression.is_compressed() {
                if self.decoder.unconsumed_bytes() != 0 {
                    return Err(LoadError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Corrupt compressed RDB stream",
                    )));
                }
                if self.compression.is_done() {
                    return Ok(step);
                }
            } else if !matches!(step, RdbStep::NeedInput) {
                return Ok(step);
            }
            if !self.has_pending_decompression() {
                if self.input_complete {
                    self.compression.finish().map_err(|_| {
                        LoadError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Corrupt compressed RDB stream",
                        ))
                    })?;
                    return Err(LoadError::Corrupt);
                }
                return Ok(RdbStep::NeedInput);
            }
            if self.decode_budget == 0 {
                return Ok(RdbStep::NeedInput);
            }
            self.decompress()?;
        }
    }

    pub fn consumed_bytes(&self) -> usize {
        self.decoder.consumed_bytes()
    }

    /// Decode as many complete records as the buffer allows into the configured
    /// target. `serving` is the process DbSet, used only by `Direct`.
    pub fn drive(
        &mut self,
        serving: &mut DbSet,
        key_load_delay_us: i64,
    ) -> Result<StreamStep, LoadError> {
        self.begin_decode_turn();
        loop {
            match self.step(serving, key_load_delay_us)? {
                RdbStep::NeedInput => return Ok(StreamStep::NeedMore),
                RdbStep::RecordApplied => {}
                RdbStep::Finished(_) => return Ok(StreamStep::Finished),
            }
        }
    }

    pub fn loaded(&self) -> usize {
        self.decoder.loaded()
    }

    /// Consume the receive state at EOF, yielding everything the terminal commit
    /// needs (function libraries, raw bytes, and the swapped-in DbSet).
    pub fn finish(self) -> RdbCommit {
        let mut decoder = self.decoder;
        let function_codes = decoder.take_function_codes();
        let aux_fields = decoder.take_aux_fields();
        let repl_stream_db = decoder.repl_stream_db().unwrap_or(0);
        let swapped = match self.target {
            RdbTarget::Swap(temp) => Some(temp),
            RdbTarget::Uninit | RdbTarget::Direct => None,
        };
        RdbCommit {
            function_codes,
            aux_fields,
            repl_stream_db,
            diskless: self.diskless,
            disk_backed: self.load_mode == RdbLoadMode::DiskBacked,
            raw: self.raw,
            swapped,
        }
    }

    /// Bytes fed but not yet consumed. Test-only, used by the link framing tests
    /// to assert exactly the RDB body — never the diskless EOF marker — reaches
    /// the decoder.
    #[cfg(test)]
    pub fn buffered(&self) -> &[u8] {
        &self.wire
    }
}

#[cfg(test)]
mod tests {
    use super::RdbReceive;
    use crate::foundation::io::persistence::{
        serialize_all_with_functions_and_replication, RdbReplicationInfo, StreamStep,
    };
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::framework::distributed::replication::RdbLoadMode;

    #[test]
    fn payload_flag_stays_clear_until_rdb_body_arrives() {
        let mut receive = RdbReceive::new(true, RdbLoadMode::Direct);

        assert!(
            !receive.has_received_payload(),
            "the RDB framing header must not make the replica enter blocking loading"
        );
        receive.feed(b"REDIS0011");
        assert!(receive.has_received_payload());
    }

    #[test]
    fn payload_bytes_count_only_bytes_fed_to_the_decoder() {
        let mut receive = RdbReceive::new(true, RdbLoadMode::Direct);
        receive.feed(b"abc");
        receive.feed(b"defg");
        assert_eq!(receive.payload_bytes(), 7);
    }

    #[test]
    fn disk_backed_decision_cannot_change_after_payload_starts() {
        let mut receive = RdbReceive::new(true, RdbLoadMode::Direct);
        receive.feed(b"first chunk");
        receive.feed(b"second chunk");

        let commit = receive.finish();
        assert!(!commit.disk_backed);
        assert!(commit.raw.is_none());
    }

    #[test]
    fn finish_preserves_the_rdb_replication_stream_db() {
        let source = DbSet::new(16);
        let replication = RdbReplicationInfo {
            repl_stream_db: 9,
            repl_id: "0123456789abcdef0123456789abcdef01234567".to_string(),
            repl_offset: 42,
            aux_fields: Vec::new(),
        };
        let bytes = serialize_all_with_functions_and_replication(
            &source,
            0,
            &[],
            true,
            Some(&replication),
            &[],
        );
        let mut receive = RdbReceive::new(true, RdbLoadMode::Swap);
        receive.set_swap(DbSet::new(16));
        receive.feed(&bytes);

        assert_eq!(
            receive.drive(&mut DbSet::new(16), 0).unwrap(),
            StreamStep::Finished
        );
        assert_eq!(receive.finish().repl_stream_db, 9);
    }

    #[test]
    fn codec_fix_complete_input_preserves_budget_and_frame_boundaries() {
        use crate::foundation::io::store::Robj;

        let mut source = DbSet::new(1);
        source.get_mut(0).unwrap().set(
            b"large".to_vec(),
            Robj::string_raw(vec![
                b'x';
                crate::foundation::io::compression::REPLICATION_BATCH_BYTES
                    + 64
            ]),
        );
        let logical = crate::foundation::io::persistence::serialize_all_with_options(
            &source,
            0,
            crate::foundation::io::persistence::RdbSerializeOptions {
                compression: false,
                checksum: true,
            },
        );
        assert!(logical.len() > crate::foundation::io::compression::REPLICATION_BATCH_BYTES);
        let wire = super::encode_rdb_stream(&logical, true).unwrap();
        let mut receive = RdbReceive::new(true, RdbLoadMode::Direct).with_skip_checksums(true);
        receive.set_direct();
        receive.feed(&wire);
        receive.finish_input();
        let mut destination = DbSet::new(1);
        assert_eq!(
            receive.drive(&mut destination, 0).unwrap(),
            StreamStep::NeedMore
        );
        assert!(receive.has_pending_decompression());
        assert_eq!(
            receive.drive(&mut destination, 0).unwrap(),
            StreamStep::Finished
        );
        receive.validate_complete().unwrap();
        assert_eq!(
            destination
                .get(0)
                .unwrap()
                .get_no_expire(b"large")
                .unwrap()
                .as_bytes()
                .len(),
            crate::foundation::io::compression::REPLICATION_BATCH_BYTES + 64
        );

        let mut receive = RdbReceive::new(true, RdbLoadMode::Direct).with_skip_checksums(true);
        receive.set_direct();
        receive.feed(&wire[..wire.len() - 1]);
        assert_eq!(
            receive.drive(&mut DbSet::new(1), 0).unwrap(),
            StreamStep::NeedMore
        );
        assert_eq!(
            receive.drive(&mut DbSet::new(1), 0).unwrap(),
            StreamStep::NeedMore
        );
        receive.finish_input();
        assert!(receive.drive(&mut DbSet::new(1), 0).is_err());

        let mut outside = wire;
        outside.push(b'X');
        let mut receive = RdbReceive::new(true, RdbLoadMode::Direct).with_skip_checksums(true);
        receive.set_direct();
        receive.feed(&outside);
        receive.finish_input();
        let mut destination = DbSet::new(1);
        assert_eq!(
            receive.drive(&mut destination, 0).unwrap(),
            StreamStep::NeedMore
        );
        assert_eq!(
            receive.drive(&mut destination, 0).unwrap(),
            StreamStep::Finished
        );
        let error = receive.validate_complete().unwrap_err();
        assert!(
            matches!(error, crate::foundation::io::persistence::LoadError::Io(error)
            if error.kind() == std::io::ErrorKind::UnexpectedEof)
        );
    }
}
