use super::ServerWorkerState;
use crate::engine::extension::config::persistence::BgsaveMethod;
use crate::foundation::io::persistence::rdb::{RdbDecoder, RdbStep};
use crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::{now_ms, DbSet};
use crate::foundation::io::store::Robj;
use crate::framework::capabilities::io::Token;
use crate::framework::context::PauseAction;
use crate::runtime::server::worker::connection::ClientReadMode;
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

const SHARED_VALUE_BYTES: usize = 2 * 1024 * 1024;
const EXPIRING_KEYS: usize = 1_024;

struct Fixture {
    runtime: ServerWorkerState,
    client: TcpStream,
    token: Token,
    dir: PathBuf,
    expire_at_ms: u64,
}

impl Fixture {
    fn new() -> Self {
        Self::new_with_import(false)
    }

    fn new_with_import(has_import: bool) -> Self {
        let mut runtime = ServerWorkerState::new(3).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let peer = runtime.transport.connect_tcp(peer_addr).unwrap();
        let (client, _) = listener.accept().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        runtime.add_connection(
            crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer {
                token: peer.token,
                peer_addr,
            },
        );
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-lazy-pause-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config = &mut runtime.ctx.config_mut().persistence;
        config.dir = dir.to_string_lossy().into_owned();
        config.dbfilename = "dump.rdb".into();
        config.bgsave_default_method = BgsaveMethod::Forkless;
        config.rdbcompression =
            crate::foundation::io::persistence::rdb::compression::RdbCompression::No;
        config.rdbchecksum = true;
        config.saveparams.clear();

        // All deadlines are fixed before the real save starts. The feed's
        // bounded turn leaves DB 2 uncaptured across background cycles.
        let expire_at_ms = now_ms() + 250;
        runtime.ctx.db_mut(0).unwrap().load_entry(
            b"shared-expired".to_vec(),
            Robj::string_raw(Bytes::from(vec![b'x'; SHARED_VALUE_BYTES])),
            Some(expire_at_ms),
        );
        runtime.ctx.db_mut(0).unwrap().load_entry(
            b"live".to_vec(),
            Robj::string("retained-value"),
            Some(expire_at_ms + 60_000),
        );
        for index in 0..EXPIRING_KEYS {
            runtime.ctx.db_mut(1).unwrap().load_entry(
                format!("expiring-{index}").into_bytes(),
                Robj::string("value"),
                Some(expire_at_ms),
            );
        }
        runtime.ctx.db_mut(2).unwrap().load_entry(
            b"lazy-expired".to_vec(),
            Robj::string("uncaptured-value"),
            Some(expire_at_ms),
        );
        for db_id in [0, 2] {
            let db = runtime.ctx.db_mut(db_id).unwrap();
            db.hash_set_with_limits(b"hash", b"field", b"field-value", 512, 64)
                .unwrap();
            db.set_expire(b"hash", expire_at_ms + 60_000);
            db.set_hash_field_expire(b"hash", b"field", expire_at_ms);
        }
        if has_import {
            use crate::engine::distributed::cluster::migration::{MigrationJobId, SlotRanges};
            let id = MigrationJobId::generate();
            runtime
                .ctx
                .create_slot_import(id.clone(), &SlotRanges::default());
            let deadline = expire_at_ms.to_string();
            let key_deadline = (expire_at_ms + 60_000).to_string();
            for parts in [
                vec![b"SET".as_slice(), b"imported", b"imported-value"],
                vec![b"PEXPIREAT".as_slice(), b"imported", deadline.as_bytes()],
                vec![
                    b"HSET".as_slice(),
                    b"imported-hash",
                    b"field",
                    b"imported-field",
                ],
                vec![
                    b"PEXPIREAT".as_slice(),
                    b"imported-hash",
                    key_deadline.as_bytes(),
                ],
                vec![
                    b"HPEXPIREAT".as_slice(),
                    b"imported-hash",
                    deadline.as_bytes(),
                    b"FIELDS",
                    b"1",
                    b"field",
                ],
            ] {
                let args = crate::framework::CommandArgs::from_parts(
                    parts
                        .iter()
                        .map(|part| Bytes::copy_from_slice(part))
                        .collect(),
                );
                runtime
                    .ctx
                    .apply_slot_import(&id, args, &runtime.registry)
                    .unwrap();
            }
        }
        assert!(now_ms() < expire_at_ms, "save must start before expiration");
        crate::framework::commands::server::save::start_bgsave(&mut runtime.ctx).unwrap();
        assert!(runtime.ctx.persistence().forkless_capture_in_progress());
        assert!(runtime
            .ctx
            .persistence()
            .forkless_key_captured(0, b"shared-expired"));
        assert!(runtime.ctx.persistence().forkless_key_captured(0, b"hash"));
        assert!(!runtime
            .ctx
            .persistence()
            .forkless_key_captured(2, b"lazy-expired"));
        assert!(!runtime.ctx.persistence().forkless_key_captured(2, b"hash"));
        // One wait crosses the existing deadline; it never changes metadata
        // or retries an assertion to obtain a passing result.
        std::thread::sleep(Duration::from_millis(
            expire_at_ms.saturating_sub(now_ms()) + 10,
        ));
        assert!(now_ms() >= expire_at_ms);
        Self {
            runtime,
            client,
            token: peer.token,
            dir,
            expire_at_ms,
        }
    }

    fn command(&mut self, parts: &[&[u8]], expected: Value) {
        let request = crate::engine::distributed::replication::protocol::encode_command(parts);
        self.client.write_all(&request).unwrap();
        self.runtime.transport.wait_readable_for_test(self.token);
        self.runtime.event_batch.begin_next_turn();
        self.runtime.writes.ordinary.begin_turn();
        let commands_before = self.runtime.ctx.stats().total_commands_processed;
        self.runtime
            .dispatch_client_read(self.token.0, ClientReadMode::Fair);
        self.runtime.drive_pending_ordinary_writes();
        let expected = crate::foundation::io::protocol::serializer::serialize_to_vec(&expected);
        let mut actual = vec![0; expected.len()];
        self.client.read_exact(&mut actual).unwrap();
        assert_eq!(actual, expected, "unexpected command reply");
        assert_eq!(
            self.runtime.ctx.stats().total_commands_processed,
            commands_before + 1
        );
    }

    fn assert_uncaptured_preserved(&self) {
        assert!(self
            .runtime
            .ctx
            .persistence()
            .forkless_capture_in_progress());
        assert!(self.runtime.ctx.persistence().forkless_save_in_progress());
        assert!(!self
            .runtime
            .ctx
            .persistence()
            .forkless_key_captured(2, b"lazy-expired"));
        assert!(!self
            .runtime
            .ctx
            .persistence()
            .forkless_key_captured(2, b"hash"));
        let db = self.runtime.ctx.db(2).unwrap();
        assert_eq!(
            db.get_allow_expired(b"lazy-expired").map(Robj::as_bytes),
            Some(b"uncaptured-value".as_slice()),
            "uncaptured expired key lost its physical value"
        );
        assert_eq!(db.get_expire(b"lazy-expired"), Some(self.expire_at_ms));
        self.assert_hash_preserved(2);
    }

    fn assert_hash_preserved(&self, db_id: u32) {
        let db = self.runtime.ctx.db(db_id).unwrap();
        assert_eq!(
            db.get_allow_expired(b"hash")
                .and_then(|value| value.hash_get(b"field"))
                .as_deref(),
            Some(b"field-value".as_slice())
        );
        assert_eq!(db.get_expire(b"hash"), Some(self.expire_at_ms + 60_000));
        assert_eq!(
            db.get_hash_field_expire(b"hash", b"field"),
            Some(self.expire_at_ms)
        );
    }

    fn finish_save(&mut self) {
        // Drive the real producer to EOF, then synchronously join the writer.
        // This is producer work, not a retry of a completion assertion.
        while self
            .runtime
            .ctx
            .persistence()
            .forkless_capture_in_progress()
        {
            self.runtime.ctx.drive_forkless_feed();
        }
        assert!(matches!(
            self.runtime.ctx.finish_primary_forkless_save_for_test(),
            Some(RdbSaveOutcome::Saved)
        ));
        assert!(!self.runtime.ctx.persistence().forkless_save_in_progress());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.runtime.ctx.abort_forkless_save(0);
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

#[test]
fn forkless_lazy_pause_dispatch_preserves_snapshot_and_crc() {
    let mut fixture = Fixture::new();
    fixture.command(&[b"SELECT", b"2"], Value::ok());
    fixture.command(&[b"GET", b"lazy-expired"], Value::Null);
    fixture.assert_uncaptured_preserved();
    fixture.command(&[b"HGET", b"hash", b"field"], Value::Null);
    fixture.assert_uncaptured_preserved();
    fixture.command(&[b"CLIENT", b"UNPAUSE"], Value::ok());
    let expired_before = fixture.runtime.ctx.stats().expired_keys;
    for _ in 0..2 {
        fixture.runtime.run_background_tasks();
        fixture.assert_uncaptured_preserved();
        fixture.assert_hash_preserved(0);
    }
    assert!(fixture.runtime.ctx.stats().expired_keys > expired_before);
    assert!(fixture
        .runtime
        .ctx
        .db(0)
        .unwrap()
        .get_allow_expired(b"shared-expired")
        .is_none());
    fixture.command(&[b"GET", b"lazy-expired"], Value::Null);
    fixture.assert_uncaptured_preserved();
    fixture.finish_save();

    let bytes = std::fs::read(fixture.dir.join("dump.rdb")).unwrap();
    let payload_end = bytes.len() - 8;
    let expected_crc = u64::from_le_bytes(bytes[payload_end..].try_into().unwrap());
    assert_ne!(expected_crc, 0);
    assert_eq!(
        expected_crc,
        crate::foundation::io::persistence::crc64(0, &bytes[..payload_end])
    );
    let mut decoder = RdbDecoder::new(true);
    let mut restored = DbSet::new(3);
    decoder.feed(&bytes);
    loop {
        match decoder.step(&mut restored).unwrap() {
            RdbStep::RecordApplied => {}
            RdbStep::Finished(_) => break,
            RdbStep::NeedInput => panic!("complete snapshot requested more input"),
        }
    }
    assert_eq!(decoder.consumed_bytes(), bytes.len());
    let first = restored.get(0).unwrap();
    assert_eq!(first.dbsize(), 3);
    assert_eq!(
        first
            .get_allow_expired(b"shared-expired")
            .unwrap()
            .as_bytes(),
        vec![b'x'; SHARED_VALUE_BYTES]
    );
    assert_eq!(
        first.get_expire(b"shared-expired"),
        Some(fixture.expire_at_ms)
    );
    assert_eq!(
        first.get_allow_expired(b"live").unwrap().as_bytes(),
        b"retained-value"
    );
    assert_eq!(
        first.get_expire(b"live"),
        Some(fixture.expire_at_ms + 60_000)
    );
    let second = restored.get(1).unwrap();
    assert_eq!(second.dbsize(), EXPIRING_KEYS);
    for index in 0..EXPIRING_KEYS {
        let key = format!("expiring-{index}");
        assert_eq!(
            second.get_allow_expired(key.as_bytes()).unwrap().as_bytes(),
            b"value"
        );
        assert_eq!(
            second.get_expire(key.as_bytes()),
            Some(fixture.expire_at_ms)
        );
    }
    let third = restored.get(2).unwrap();
    assert_eq!(third.dbsize(), 2);
    assert_eq!(
        third.get_allow_expired(b"lazy-expired").unwrap().as_bytes(),
        b"uncaptured-value"
    );
    assert_eq!(
        third.get_expire(b"lazy-expired"),
        Some(fixture.expire_at_ms)
    );
    for db_id in [0, 2] {
        let db = restored.get(db_id).unwrap();
        assert_eq!(
            db.get_allow_expired(b"hash")
                .unwrap()
                .hash_get(b"field")
                .as_deref(),
            Some(b"field-value".as_slice())
        );
        assert_eq!(db.get_expire(b"hash"), Some(fixture.expire_at_ms + 60_000));
        assert_eq!(
            db.get_hash_field_expire(b"hash", b"field"),
            Some(fixture.expire_at_ms)
        );
    }
    fixture.command(&[b"GET", b"lazy-expired"], Value::Null);
    assert!(fixture
        .runtime
        .ctx
        .db(2)
        .unwrap()
        .get_allow_expired(b"lazy-expired")
        .is_none());
    fixture.command(&[b"HGET", b"hash", b"field"], Value::Null);
    fixture.assert_hash_preserved(2);
    fixture.runtime.run_background_tasks();
    assert!(fixture
        .runtime
        .ctx
        .db(2)
        .unwrap()
        .get_allow_expired(b"hash")
        .is_none());
}

#[test]
fn forkless_lazy_pause_background_preserves_uncaptured_keys() {
    let mut fixture = Fixture::new();
    for _ in 0..2 {
        fixture.runtime.run_background_tasks();
        fixture.assert_uncaptured_preserved();
        fixture.assert_hash_preserved(0);
    }
    fixture.command(&[b"SELECT", b"2"], Value::ok());
    fixture.command(&[b"GET", b"lazy-expired"], Value::Null);
    fixture.assert_uncaptured_preserved();
}

#[test]
fn forkless_lazy_pause_abort_restores_lazy_expiration() {
    let mut fixture = Fixture::new();
    fixture.runtime.ctx.abort_forkless_save(0);
    assert!(!fixture
        .runtime
        .ctx
        .persistence()
        .forkless_capture_in_progress());
    assert!(!fixture
        .runtime
        .ctx
        .persistence()
        .forkless_save_in_progress());
    assert!(!fixture.dir.join("dump.rdb").exists());
    fixture.command(&[b"SELECT", b"2"], Value::ok());
    fixture.command(&[b"GET", b"lazy-expired"], Value::Null);
    assert!(fixture
        .runtime
        .ctx
        .db(2)
        .unwrap()
        .get_allow_expired(b"lazy-expired")
        .is_none());
    fixture.command(&[b"HGET", b"hash", b"field"], Value::Null);
    fixture.assert_hash_preserved(2);
    fixture.runtime.run_background_tasks();
    assert!(fixture
        .runtime
        .ctx
        .db(2)
        .unwrap()
        .get_allow_expired(b"hash")
        .is_none());
}

#[test]
fn forkless_lazy_pause_completion_keeps_existing_pause_reasons() {
    for should_finish in [false, true] {
        for reason in ["pause-all", "pause-write", "read-only"] {
            let mut fixture = Fixture::new();
            if reason == "read-only" {
                fixture.runtime.ctx.replication_mut().switch_to_replica();
                fixture
                    .runtime
                    .ctx
                    .config_mut()
                    .replication
                    .replica_read_only = true;
            } else {
                let pause = fixture.runtime.ctx.client_pause_mut();
                pause.action = if reason == "pause-all" {
                    PauseAction::All
                } else {
                    PauseAction::Write
                };
                pause.end_time_ms = i64::MAX;
            }
            if should_finish {
                fixture.finish_save();
            } else {
                fixture.runtime.ctx.abort_forkless_save(0);
            }
            // Check immediately after the Framework completion boundary,
            // before a command can re-arm an accidentally cleared guard.
            assert!(fixture
                .runtime
                .ctx
                .db_mut(2)
                .unwrap()
                .get(b"lazy-expired")
                .is_none());
            assert_eq!(
                fixture
                    .runtime
                    .ctx
                    .db(2)
                    .unwrap()
                    .get_allow_expired(b"lazy-expired")
                    .map(Robj::as_bytes),
                Some(b"uncaptured-value".as_slice()),
                "{reason}, finish={should_finish}"
            );
            assert_eq!(
                fixture
                    .runtime
                    .ctx
                    .db(2)
                    .unwrap()
                    .get_expire(b"lazy-expired"),
                Some(fixture.expire_at_ms)
            );
            let expired_before = fixture.runtime.ctx.stats().expired_keys;
            fixture.runtime.run_background_tasks();
            assert_eq!(fixture.runtime.ctx.stats().expired_keys, expired_before);
            fixture.assert_hash_preserved(0);
            fixture.assert_hash_preserved(2);
            fixture.runtime.ctx.client_pause_mut().action = PauseAction::None;
            fixture.runtime.ctx.client_pause_mut().end_time_ms = 0;
            fixture.runtime.ctx.replication_mut().switch_to_primary();
            fixture.command(&[b"SELECT", b"2"], Value::ok());
            fixture.command(&[b"GET", b"lazy-expired"], Value::Null);
            assert!(fixture
                .runtime
                .ctx
                .db(2)
                .unwrap()
                .get_allow_expired(b"lazy-expired")
                .is_none());
        }
    }
}

#[test]
fn forkless_lazy_pause_additional_dataset_keeps_import_guards_on_abort() {
    let mut fixture = Fixture::new_with_import(true);
    fixture.command(&[b"SELECT", b"2"], Value::ok());
    fixture.command(&[b"GET", b"lazy-expired"], Value::Null);
    fixture.assert_uncaptured_preserved();
    fixture.runtime.run_background_tasks();
    fixture.assert_uncaptured_preserved();
    let pause = fixture.runtime.ctx.client_pause_mut();
    pause.action = PauseAction::Write;
    pause.end_time_ms = i64::MAX;
    fixture.runtime.ctx.abort_forkless_save(0);
    assert!(!fixture
        .runtime
        .ctx
        .persistence()
        .forkless_capture_in_progress());
    assert!(fixture
        .runtime
        .ctx
        .db_mut(2)
        .unwrap()
        .get(b"lazy-expired")
        .is_none());
    assert_eq!(
        fixture
            .runtime
            .ctx
            .db(2)
            .unwrap()
            .get_allow_expired(b"lazy-expired")
            .map(Robj::as_bytes),
        Some(b"uncaptured-value".as_slice())
    );
    let imports = fixture.runtime.ctx.slot_import_dbs_with_ids();
    assert_eq!(imports.len(), 1);
    let db = imports[0].1.get(0).unwrap();
    assert_eq!(
        db.get_allow_expired(b"imported").unwrap().as_bytes(),
        b"imported-value"
    );
    assert_eq!(db.get_expire(b"imported"), Some(fixture.expire_at_ms));
    assert_eq!(
        db.get_allow_expired(b"imported-hash")
            .unwrap()
            .hash_get(b"field")
            .as_deref(),
        Some(b"imported-field".as_slice())
    );
    assert_eq!(
        db.get_expire(b"imported-hash"),
        Some(fixture.expire_at_ms + 60_000)
    );
    assert_eq!(
        db.get_hash_field_expire(b"imported-hash", b"field"),
        Some(fixture.expire_at_ms)
    );
}
