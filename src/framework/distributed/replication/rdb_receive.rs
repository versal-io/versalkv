//! Streaming full-sync RDB receive state, owned by `ReplicaLink` for the
//! duration of an RDB phase. `ReplicaLink` framing forwards RDB-body bytes here
//! (never the diskless EOF marker); the backend supplies the target `DbSet` and
//! drives the paced decode. The whole body is no longer buffered — only the
//! `aof_use_rdb_preamble && !diskless` case retains `raw`, because installing
//! the sync RDB as the AOF base intrinsically needs the full bytes.
//! Ownership seam: the decoder needs a `DbSet` (owned by `ServerContext`, not
//! the link), so the actual `drive`/commit happens in the backend receive path
//! (`runtime/distributed/replication/rdb_recv_path.rs`); the link only `feed`s bytes.

use crate::foundation::io::persistence::{LoadError, RdbStreamDecoder, StreamStep};
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
    diskless: bool,
    raw: Option<Vec<u8>>,
    load_mode: RdbLoadMode,
    target: RdbTarget,
    received_payload: bool,
    payload_bytes: u64,
}

impl RdbReceive {
    pub fn new(diskless: bool, load_mode: RdbLoadMode) -> Self {
        Self {
            // Replica full-sync load uses non-strict version acceptance, matching
            // the buffer path (`load_..._and_delay(.., false, ..)`).
            decoder: RdbStreamDecoder::new(false),
            diskless,
            raw: if load_mode == RdbLoadMode::DiskBacked || !diskless {
                Some(Vec::new())
            } else {
                None
            },
            load_mode,
            target: RdbTarget::Uninit,
            received_payload: false,
            payload_bytes: 0,
        }
    }

    /// Feed newly framed RDB-body bytes; mirror into `raw` when retained.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.received_payload |= !bytes.is_empty();
        self.payload_bytes = self.payload_bytes.saturating_add(bytes.len() as u64);
        if let Some(raw) = self.raw.as_mut() {
            raw.extend_from_slice(bytes);
        }
        self.decoder.feed(bytes);
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

    /// A full-sync header alone only enters transfer state. C starts blocking
    /// loading after the first RDB payload bytes arrive, not at framing time.
    pub fn has_received_payload(&self) -> bool {
        self.received_payload
    }

    pub fn set_swap(&mut self, temp: DbSet) {
        self.target = RdbTarget::Swap(temp);
    }

    pub fn set_direct(&mut self) {
        self.target = RdbTarget::Direct;
    }

    /// Decode as many complete records as the buffer allows into the configured
    /// target. `serving` is the process DbSet, used only by `Direct`.
    pub fn drive(
        &mut self,
        serving: &mut DbSet,
        key_load_delay_us: i64,
    ) -> Result<StreamStep, LoadError> {
        let Self {
            decoder, target, ..
        } = self;
        match target {
            RdbTarget::Swap(temp) => decoder.drive(temp, key_load_delay_us),
            RdbTarget::Direct => decoder.drive(serving, key_load_delay_us),
            // Uninit means the backend has not chosen a target yet; nothing to do.
            RdbTarget::Uninit => Ok(StreamStep::NeedMore),
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
        self.decoder.buffered()
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
}
