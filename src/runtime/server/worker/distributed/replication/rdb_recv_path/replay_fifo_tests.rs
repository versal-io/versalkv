use super::*;
use crate::engine::distributed::replication::{protocol, ReplicationOffset};
use crate::foundation::io::compression::REPLICATION_BATCH_BYTES;
use crate::foundation::io::persistence::save_string;
use crate::framework::distributed::replication::{MasterInfo, RdbLoadMode};
use crate::runtime::server::worker::reactor::blocked_events::ReplayProgress;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;

const REPLID: &str = "0123456789abcdef0123456789abcdef01234567";
const SNAPSHOT_OFFSET: i64 = 100;

struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove isolated replay directory");
    }
}

struct ReplayFixture {
    runtime: ServerWorkerState,
    main: usize,
    rdb: usize,
    main_peer: TcpStream,
    _rdb_peer: TcpStream,
    directory: TestDirectory,
}

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).unwrap()
}

fn connect_peer(runtime: &mut ServerWorkerState) -> (usize, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let connected = runtime.transport.connect_tcp(peer_addr).unwrap();
    let (peer, _) = listener.accept().unwrap();
    runtime.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: connected.token,
            peer_addr,
        },
    );
    (connected.token.0, peer)
}

impl ReplayFixture {
    fn new(aof_enabled: bool, scenario: &str) -> Self {
        let directory = TestDirectory(std::env::temp_dir().join(format!(
            "versal-replay-fifo-{}-{:?}-{scenario}-{aof_enabled}",
            std::process::id(),
            std::thread::current().id()
        )));
        std::fs::create_dir_all(&directory.0).unwrap();
        let mut runtime = ServerWorkerState::new(2).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.config_mut().persistence.dir = directory.0.to_string_lossy().into_owned();
        runtime.ctx.config_mut().persistence.aof_enabled = aof_enabled;
        runtime.ctx.config_mut().persistence.aof_use_rdb_preamble = true;
        runtime.ctx.config_mut().replication.repl_diskless_load = "disabled".to_string();
        runtime
            .ctx
            .config_mut()
            .persistence
            .loading_process_events_interval_bytes = 0;
        let (main, main_peer) = connect_peer(&mut runtime);
        let (rdb, rdb_peer) = connect_peer(&mut runtime);
        let mut link = ReplicaLink::new(
            ConnectionId(main),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.pin_rdb_load_mode(RdbLoadMode::DiskBacked);
        link.set_resume_position(REPLID.to_string(), offset(SNAPSHOT_OFFSET), 0);
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379).unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        link.on_rdb_bytes(b"+OK\r\n").unwrap();
        link.on_rdb_bytes(&protocol::encode_end_offset(
            offset(SNAPSHOT_OFFSET),
            REPLID,
            0,
            rdb as u64,
        ))
        .unwrap();
        link.on_rdb_bytes(format!("$EOF:{REPLID}\r\n").as_bytes())
            .unwrap();
        link.on_bytes(b"+OK\r\n+CONTINUE\r\n", 6379).unwrap();
        assert!(link.is_streaming());
        runtime.replication_runtime.set_replica_link(link);
        runtime
            .replication_runtime
            .set_replica_rdb_link(ConnectionId(rdb), ConnectionId(main))
            .unwrap();
        Self {
            runtime,
            main,
            rdb,
            main_peer,
            _rdb_peer: rdb_peer,
            directory,
        }
    }

    fn load_first_turn(&mut self) {
        let mut body = b"VALKEY080".to_vec();
        for index in 0..1536 {
            body.push(0);
            save_string(&mut body, format!("snapshot-{index}").as_bytes());
            save_string(&mut body, &[b'x'; 1024]);
        }
        body.push(0xff);
        body.extend_from_slice(&[0; 8]);
        assert!(body.len() > REPLICATION_BATCH_BYTES);
        assert!(body.len() < 2 * REPLICATION_BATCH_BYTES);
        let mut link = self
            .runtime
            .replication_runtime
            .take_replica_link()
            .unwrap();
        link.on_rdb_bytes(&body).unwrap();
        link.on_rdb_bytes(REPLID.as_bytes()).unwrap();
        self.runtime.account_replica_wire_input(&mut link);
        assert!(link.rdb_framing_is_complete());
        let operation = BlockedOperation::DualRdb {
            main: ConnectionId(self.main),
            rdb: ConnectionId(self.rdb),
        };
        let ResumeDecision::Resume(mut link) = self
            .runtime
            .drive_replica_rdb_decode(link, self.rdb, operation)
        else {
            panic!("first disk-backed decode turn must remain resumable");
        };
        assert_eq!(link.rdb_load_mode(), Some(RdbLoadMode::DiskBacked));
        let receive = link.rdb_receive_mut().unwrap();
        assert!(receive.loaded() > 0);
        assert!(receive.loaded() < 1536);
        assert!(receive.has_pending_decompression());
        assert_eq!(
            link.rdb_channel_state(),
            ReplicaRdbChannelState::ReceivingRdb
        );
        self.runtime.replication_runtime.set_replica_link(link);
    }

    fn publish_prefix(&mut self, bytes: &[u8]) {
        let mut link = self
            .runtime
            .replication_runtime
            .take_replica_link()
            .unwrap();
        let (events, actions) = link.on_bytes(bytes, 6379).unwrap();
        assert!(actions.is_empty());
        self.runtime
            .buffer_replica_events_into_pending(self.main, &mut link, events)
            .unwrap();
        self.runtime.replication_runtime.set_replica_link(link);
    }

    fn finish_decode_with_local(&mut self, bytes: &[u8]) {
        let link = self
            .runtime
            .replication_runtime
            .take_replica_link()
            .unwrap();
        let context = ClassicChunkContext {
            token_value: self.main,
            connection_token: self.main,
            listening_port: 6379,
            replica_announce_ip: "",
        };
        let chunk = self
            .runtime
            .process_classic_rdb_chunk(link, &context, bytes)
            .unwrap();
        self.runtime.finish_classic_rdb_dispatch(
            self.main,
            self.main,
            ClassicDispatchResult {
                link: chunk.link,
                events: chunk.events,
                actions: chunk.actions,
                peer_closed: false,
            },
        );
    }

    fn assert_replay_offsets(&mut self, expected: &[ReplicationOffset]) {
        let session = self
            .runtime
            .replication_runtime
            .replica_link()
            .unwrap()
            .session_id();
        let mut pending = self
            .runtime
            .replication_runtime
            .take_replica_pending_buffer_for(session)
            .unwrap();
        let mut events = Vec::new();
        while let Some(entry) = pending.pop_front() {
            events.push(entry.event);
        }
        let actual: Vec<_> = events
            .iter()
            .map(|event| {
                let ReplicaEvent::Apply { end_offset, .. } = event;
                *end_offset
            })
            .collect();
        assert_eq!(
            actual, expected,
            "local older frames must precede handoff socket frames"
        );
        assert!(actual.windows(2).all(|pair| pair[0] < pair[1]));
        for event in events {
            pending.push(event).unwrap();
        }
        assert!(self
            .runtime
            .replication_runtime
            .set_replica_pending_buffer(session, pending)
            .is_none());
    }
}

fn batch(commands: &[&[&[u8]]], position: &mut i64) -> (Vec<u8>, Vec<ReplicationOffset>) {
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for command in commands {
        let frame = protocol::encode_command(command);
        *position += frame.len() as i64;
        offsets.push(offset(*position));
        bytes.extend_from_slice(&frame);
    }
    (bytes, offsets)
}

fn assert_fifo_handoff(aof_enabled: bool) {
    let mut fixture = ReplayFixture::new(aof_enabled, "local-and-socket");
    let mut position = SNAPSHOT_OFFSET;
    let (prefix, mut expected) = batch(
        &[&[b"SET", b"shared", b"prefix"], &[b"INCR", b"counter"]],
        &mut position,
    );
    fixture.publish_prefix(&prefix);
    fixture.load_first_turn();
    let (older, offsets) = batch(
        &[
            &[b"SELECT", b"1"],
            &[b"SET", b"shared", b"older-1"],
            &[b"INCR", b"counter"],
            &[b"SELECT", b"0"],
            &[b"SET", b"shared", b"older-0"],
            &[b"INCR", b"counter"],
        ],
        &mut position,
    );
    expected.extend(offsets);
    let (newer, offsets) = batch(
        &[
            &[b"SELECT", b"1"],
            &[b"SET", b"shared", b"newer-1"],
            &[b"INCR", b"counter"],
            &[b"SELECT", b"0"],
            &[b"SET", b"shared", b"newer-0"],
            &[b"INCR", b"counter"],
        ],
        &mut position,
    );
    expected.extend(offsets);
    fixture.main_peer.write_all(&newer).unwrap();
    fixture
        .runtime
        .transport
        .wait_readable_for_test(Token(fixture.main));
    fixture.finish_decode_with_local(&older);
    fixture.assert_replay_offsets(&expected);
    assert!(fixture
        .runtime
        .replication_runtime
        .replica_main_read_paused());
    assert!(fixture.runtime.pending_replication_replay.is_some());
    assert_eq!(
        fixture.runtime.ctx.replication().slave_repl_offset,
        offset(SNAPSHOT_OFFSET)
    );
    assert!(!fixture.runtime.connection_registry[Token(fixture.main)].has_pending_output());
    fixture.runtime.drive_one_pending_replication_replay();
    let link = fixture.runtime.replication_runtime.replica_link().unwrap();
    assert_eq!(link.applied_position(), offset(position));
    assert_eq!(link.received_position(), offset(position));
    assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
    assert_eq!(link.selected_db(), 0);
    assert!(!fixture
        .runtime
        .replication_runtime
        .replica_main_read_paused());
    assert!(fixture.runtime.pending_replication_replay.is_none());
    assert!(fixture
        .runtime
        .replication_runtime
        .replica_pending_buffer()
        .is_none());
    assert!(!fixture.runtime.transport.is_registered(Token(fixture.rdb)));
    assert!(fixture.runtime.connection_registry[Token(fixture.main)].has_pending_output());
    for (db, value, count) in [
        (0, b"newer-0".as_slice(), b"3".as_slice()),
        (1, b"newer-1".as_slice(), b"2".as_slice()),
    ] {
        assert_eq!(
            fixture
                .runtime
                .ctx
                .db(db)
                .unwrap()
                .get_no_expire(b"shared")
                .unwrap()
                .as_bytes(),
            value
        );
        assert_eq!(
            fixture
                .runtime
                .ctx
                .db(db)
                .unwrap()
                .get_no_expire(b"counter")
                .unwrap()
                .as_bytes(),
            count
        );
    }
    assert_eq!(fixture.runtime.ctx.stats().cmd_stats["set"].calls, 5);
    assert_eq!(fixture.runtime.ctx.stats().cmd_stats["incr"].calls, 5);
    fixture.runtime.drive_one_pending_replication_replay();
    assert_eq!(fixture.runtime.ctx.stats().cmd_stats["set"].calls, 5);
    assert_eq!(fixture.runtime.ctx.stats().cmd_stats["incr"].calls, 5);
    assert_eq!(fixture.runtime.ctx.aof().journal.is_some(), aof_enabled);
    if aof_enabled {
        let journal = fixture.runtime.ctx.aof_mut().journal.as_mut().unwrap();
        journal.appender.flush(true).unwrap();
        let persisted = std::fs::read(journal.appender.path()).unwrap();
        let mut expected_aof = protocol::encode_command(&[b"SELECT", b"0"]);
        expected_aof.extend_from_slice(&prefix);
        expected_aof.extend_from_slice(&older);
        expected_aof.extend_from_slice(&newer);
        assert_eq!(
            persisted, expected_aof,
            "AOF must preserve each replay frame exactly once in FIFO order"
        );
    }
    assert!(fixture
        .directory
        .0
        .join(&fixture.runtime.ctx.config().persistence.dbfilename)
        .exists());
}

#[test]
fn disk_backed_local_frames_precede_socket_frames_without_aof() {
    assert_fifo_handoff(false);
}

#[test]
fn disk_backed_local_frames_precede_socket_frames_with_aof() {
    assert_fifo_handoff(true);
}

#[test]
fn existing_replay_handoff_preserves_progress_and_paused_socket() {
    let mut fixture = ReplayFixture::new(false, "existing");
    let mut position = SNAPSHOT_OFFSET;
    let (prefix, _) = batch(
        &[&[b"SET", b"shared", b"prefix"], &[b"INCR", b"counter"]],
        &mut position,
    );
    fixture.publish_prefix(&prefix);
    fixture.load_first_turn();
    fixture.finish_decode_with_local(&[]);
    fixture
        .runtime
        .ctx
        .config_mut()
        .persistence
        .loading_process_events_interval_bytes = 1;
    fixture
        .runtime
        .pending_replication_replay
        .as_mut()
        .unwrap()
        .progress = ReplayProgress::new(crate::foundation::io::sys::time::mstime() - 101);
    fixture.runtime.drive_one_pending_replication_replay();
    let continuation = fixture.runtime.pending_replication_replay.as_ref().unwrap();
    assert!(continuation.selected_db_applied);
    assert!(continuation.progress.processed_bytes() > 0);
    let progress = continuation.progress.processed_bytes();
    let received = fixture
        .runtime
        .replication_runtime
        .replica_link()
        .unwrap()
        .received_position();
    let (newer, _) = batch(&[&[b"SET", b"shared", b"newer"]], &mut position);
    fixture.main_peer.write_all(&newer).unwrap();
    for _ in 0..3 {
        fixture.runtime.apply_deferred_replica_events();
        let mut link = fixture
            .runtime
            .replication_runtime
            .take_replica_link()
            .unwrap();
        fixture
            .runtime
            .commit_and_replay_dual_handoff_only(fixture.main, &mut link)
            .unwrap();
        assert_eq!(
            link.received_position(),
            received,
            "existing replay must not read newer socket bytes"
        );
        fixture.runtime.replication_runtime.set_replica_link(link);
        let continuation = fixture.runtime.pending_replication_replay.as_ref().unwrap();
        assert!(continuation.selected_db_applied);
        assert_eq!(continuation.progress.processed_bytes(), progress);
        assert_eq!(
            fixture
                .runtime
                .replication_runtime
                .replica_pending_buffer()
                .unwrap()
                .len(),
            1
        );
        assert!(fixture
            .runtime
            .replication_runtime
            .replica_main_read_paused());
    }
    fixture.runtime.drive_one_pending_replication_replay();
    assert!(!fixture
        .runtime
        .replication_runtime
        .replica_main_read_paused());
    fixture
        .runtime
        .transport
        .wait_readable_for_test(Token(fixture.main));
    fixture
        .runtime
        .recv_replication_stream(fixture.main, fixture.main);
    let link = fixture.runtime.replication_runtime.replica_link().unwrap();
    assert_eq!(link.applied_position(), offset(position));
    assert_eq!(
        fixture
            .runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"shared")
            .unwrap()
            .as_bytes(),
        b"newer"
    );
    assert_eq!(
        fixture
            .runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"counter")
            .unwrap()
            .as_bytes(),
        b"1"
    );
    assert_eq!(fixture.runtime.ctx.stats().cmd_stats["set"].calls, 2);
    assert_eq!(fixture.runtime.ctx.stats().cmd_stats["incr"].calls, 1);
}

#[test]
fn disk_backed_eof_pending_decode_completes_without_new_socket_bytes() {
    for aof_enabled in [false, true] {
        let mut fixture = ReplayFixture::new(aof_enabled, "eof-no-new-bytes");
        fixture.load_first_turn();
        let wire_before = fixture.runtime.ctx.stats().total_net_repl_input_bytes;
        fixture
            .runtime
            .recv_streaming_rdb_chunk(fixture.main, fixture.main);
        let link = fixture.runtime.replication_runtime.replica_link().unwrap();
        assert!(
            !link.has_pending_rdb_receive(),
            "buffered plain RDB must progress after framing EOF without another socket read"
        );
        assert_eq!(
            fixture.runtime.ctx.stats().total_net_repl_input_bytes,
            wire_before
        );
        assert_eq!(link.rdb_channel_state(), ReplicaRdbChannelState::None);
        assert_eq!(link.applied_position(), offset(SNAPSHOT_OFFSET));
        assert_eq!(fixture.runtime.ctx.db(0).unwrap().dbsize(), 1536);
        assert_eq!(fixture.runtime.ctx.aof().journal.is_some(), aof_enabled);
        assert!(!fixture.runtime.ctx.is_loading());
    }
}
