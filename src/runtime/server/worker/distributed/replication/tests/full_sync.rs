use super::*;

struct AtomicFullSyncFixture {
    backend: ServerWorkerState,
    connection_token: usize,
    link: ReplicaLink,
    commit: Option<crate::framework::distributed::replication::RdbCommit>,
    old_replid: String,
    old_slots: Vec<u8>,
    old_journal_path: std::path::PathBuf,
}

fn atomic_full_sync_fixture(
    dir: &std::path::Path,
    old_journal_path: &std::path::Path,
) -> AtomicFullSyncFixture {
    use crate::foundation::io::persistence::{AofAppender, AofFsync};
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;

    let mut backend = ServerWorkerState::new(16).unwrap();
    backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    backend.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    backend.ctx.config_mut().persistence.aof_enabled = true;
    backend.ctx.config_mut().persistence.aof_use_rdb_preamble = true;
    backend.ctx.config_mut().persistence.aof_filename = "appendonly.aof".to_string();
    let old_appender = AofAppender::open_append_path(old_journal_path, AofFsync::No).unwrap();
    backend.ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(old_appender));
    backend.ctx.set_repl_seldb(10);
    backend.ctx.persistence_mut().set_aof_selected_db(11);
    backend.ctx.persistence_mut().record_aof_size(7);
    backend.ctx.replication_mut().slave_repl_offset = offset(17);
    backend.ctx.replication_mut().slave_read_repl_offset = offset(18);
    backend.ctx.set_loading(true);
    let connection_token = add_connection(&mut backend, 84);
    backend
        .ctx
        .db_mut(0)
        .unwrap()
        .set(b"old".to_vec(), Robj::string("value"));
    let old_replid = backend.ctx.replication().backlog.replid().to_string();
    let old_function = "#!lua name=oldatomic\nserver.register_function('oldatomicfn', function() return 'old' end)";
    backend
        .ctx
        .script_mut()
        .function_registry
        .load_library("oldatomic", old_function)
        .unwrap();
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16_380);
    let old_slots = b"8>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,".to_vec();
    cluster.apply_open_slots_aux_field(&old_slots).unwrap();
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
    let mut replacement = DbSet::new(16);
    replacement
        .get_mut(0)
        .unwrap()
        .set(b"new".to_vec(), Robj::string("value"));
    let raw = crate::foundation::io::persistence::serialize_all(&replacement, 0);
    let mut link = ReplicaLink::new(
        ConnectionId(84),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.set_resume_position(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        offset(19),
        0,
    );
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: vec![b"#!lua name=newatomic\nserver.register_function('newatomicfn', function() return 'new' end)".to_vec()],
        aux_fields: vec![(b"cluster-slot-states".to_vec(), b"9>cccccccccccccccccccccccccccccccccccccccc,".to_vec())],
        repl_stream_db: 0,
        diskless: true,
        disk_backed: true,
        raw: Some(raw),
        swapped: Some(replacement),
    };
    AtomicFullSyncFixture {
        backend,
        connection_token,
        link,
        commit: Some(commit),
        old_replid,
        old_slots,
        old_journal_path: old_journal_path.to_path_buf(),
    }
}

fn assert_atomic_full_sync_state_unchanged(fixture: &AtomicFullSyncFixture) {
    let backend = &fixture.backend;
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"old").is_some());
    assert!(backend.ctx.db(0).unwrap().get_no_expire(b"new").is_none());
    assert_eq!(
        backend.ctx.replication().backlog.replid(),
        fixture.old_replid
    );
    assert!(backend
        .ctx
        .script()
        .function_registry
        .get_library("oldatomic")
        .is_some());
    assert!(backend
        .ctx
        .script()
        .function_registry
        .get_library("newatomic")
        .is_none());
    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .encode_open_slots_aux_field(),
        Some(fixture.old_slots.clone())
    );
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(17));
    assert_eq!(backend.ctx.replication().slave_read_repl_offset, offset(18));
    assert!(backend.ctx.is_loading());
    assert_eq!(backend.ctx.repl_seldb(), 10);
    assert_eq!(backend.ctx.persistence().aof_selected_db(), 11);
    assert_eq!(backend.ctx.aof().current_size, 7);
    assert_eq!(
        backend.ctx.aof().journal.as_ref().unwrap().path(),
        fixture.old_journal_path.to_string_lossy()
    );
}

fn assert_rejected_rdb_apply_position_preserves_state(
    label: &str,
    requested_offset: Option<crate::engine::distributed::replication::ReplicationOffset>,
) {
    let dir = std::env::temp_dir().join(format!(
        "valkey-replica-invalid-boundary-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::write(dir.join("dump.rdb"), b"old-rdb").unwrap();
    let old_manifest = "file appendonly.aof.1.base.rdb seq 1 type b\n\
                        file appendonly.aof.1.incr.aof seq 1 type i\n";
    let old_base = app_dir.join("appendonly.aof.1.base.rdb");
    let old_incr = app_dir.join("appendonly.aof.1.incr.aof");
    std::fs::write(&old_base, b"old-base").unwrap();
    std::fs::write(&old_incr, b"old-tail").unwrap();
    std::fs::write(app_dir.join("appendonly.aof.manifest"), old_manifest).unwrap();
    let mut fixture = atomic_full_sync_fixture(&dir, &old_incr);
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .aof_appenddirname = "appendonlydir".to_string();
    if let Some(requested_offset) = requested_offset {
        fixture.link.set_resume_position(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            requested_offset,
            0,
        );
    } else {
        fixture.link =
            ReplicaLink::new(fixture.link.connection_id(), fixture.link.master().clone());
    }
    fixture.backend.ctx.replication_mut().backlog.activate();
    fixture
        .backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"old-history")
        .unwrap();
    let old_boundary = fixture
        .backend
        .ctx
        .replication()
        .backlog
        .snapshot_boundary();
    let old_first_cursor = fixture
        .backend
        .ctx
        .replication()
        .backlog
        .first_byte_cursor();
    let old_history = fixture
        .backend
        .ctx
        .replication()
        .backlog
        .read_from(old_first_cursor)
        .unwrap();
    let old_applied = fixture.link.applied_position();
    let old_acknowledged = fixture.link.acknowledged_position();
    let old_can_ack = fixture.link.can_ack_primary();
    let old_fsynced_repl_offset = fixture.backend.ctx.aof().fsynced_repl_offset;
    let old_output = fixture.backend.connection_registry
        [crate::framework::capabilities::io::Token(fixture.connection_token)]
    .output
    .queue
    .clone();
    let temp_rdb = dir.join(format!(
        "temp-repl-{}-{}.rdb",
        std::process::id(),
        fixture.link.session_id()
    ));

    assert!(fixture
        .backend
        .commit_streamed_rdb(
            fixture.connection_token,
            &mut fixture.link,
            fixture.commit.take().unwrap(),
        )
        .is_err());

    assert_atomic_full_sync_state_unchanged(&fixture);
    assert!(fixture.backend.ctx.replication().backlog.is_active());
    assert_eq!(
        fixture
            .backend
            .ctx
            .replication()
            .backlog
            .snapshot_boundary(),
        old_boundary
    );
    assert_eq!(
        fixture
            .backend
            .ctx
            .replication()
            .backlog
            .read_from(old_first_cursor),
        Some(old_history)
    );
    assert_eq!(std::fs::read(dir.join("dump.rdb")).unwrap(), b"old-rdb");
    assert_eq!(
        std::fs::read_to_string(app_dir.join("appendonly.aof.manifest")).unwrap(),
        old_manifest
    );
    assert_eq!(std::fs::read(&old_base).unwrap(), b"old-base");
    assert_eq!(std::fs::read(&old_incr).unwrap(), b"old-tail");
    assert!(!app_dir.join("appendonly.aof.2.base.rdb").exists());
    assert!(!app_dir.join("appendonly.aof.2.incr.aof").exists());
    assert!(!temp_rdb.exists());
    assert_eq!(fixture.link.applied_position(), old_applied);
    assert_eq!(fixture.link.acknowledged_position(), old_acknowledged);
    assert_eq!(fixture.link.can_ack_primary(), old_can_ack);
    assert_eq!(
        fixture.backend.ctx.aof().fsynced_repl_offset,
        old_fsynced_repl_offset
    );
    assert_eq!(
        fixture.backend.connection_registry
            [crate::framework::capabilities::io::Token(fixture.connection_token)]
        .output
        .queue,
        old_output
    );
    if let Some(mut journal) = fixture.backend.ctx.aof_mut().journal.take() {
        journal.shutdown().unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn invalid_rdb_boundaries_do_not_publish_durable_or_in_memory_state() {
    assert_rejected_rdb_apply_position_preserves_state("overflow", Some(offset(i64::MAX)));
}

#[test]
fn missing_requested_rdb_apply_position_preserves_durable_and_in_memory_state() {
    assert_rejected_rdb_apply_position_preserves_state("missing", None);
}

#[test]
fn aof_base_prepare_failure_keeps_full_sync_state_unpublished() {
    let dir = std::env::temp_dir().join(format!(
        "valkey-replica-aof-prepare-failure-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("dump.rdb"), b"old-rdb").unwrap();
    std::fs::write(dir.join("blocked-aof-dir"), b"not-a-directory").unwrap();
    let mut fixture = atomic_full_sync_fixture(&dir, &dir.join("old.aof"));
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .aof_appenddirname = "blocked-aof-dir".to_string();

    assert!(fixture
        .backend
        .commit_streamed_rdb(
            fixture.connection_token,
            &mut fixture.link,
            fixture.commit.take().unwrap(),
        )
        .is_err());

    assert_atomic_full_sync_state_unchanged(&fixture);
    assert_eq!(std::fs::read(dir.join("dump.rdb")).unwrap(), b"old-rdb");
    if let Some(mut journal) = fixture.backend.ctx.aof_mut().journal.take() {
        journal.shutdown().unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn disk_rdb_commit_failure_rolls_back_prepared_aof_manifest() {
    let dir = std::env::temp_dir().join(format!(
        "valkey-replica-aof-rollback-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::create_dir_all(dir.join("dump.rdb")).unwrap();
    let old_manifest = "file appendonly.aof.1.base.rdb seq 1 type b\n\
                        file appendonly.aof.1.incr.aof seq 1 type i\n";
    let old_base = app_dir.join("appendonly.aof.1.base.rdb");
    let old_incr = app_dir.join("appendonly.aof.1.incr.aof");
    std::fs::write(&old_base, b"old-base").unwrap();
    std::fs::write(&old_incr, b"old-tail").unwrap();
    std::fs::write(app_dir.join("appendonly.aof.manifest"), old_manifest).unwrap();
    let mut fixture = atomic_full_sync_fixture(&dir, &old_incr);
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .aof_appenddirname = "appendonlydir".to_string();

    assert!(fixture
        .backend
        .commit_streamed_rdb(
            fixture.connection_token,
            &mut fixture.link,
            fixture.commit.take().unwrap(),
        )
        .is_err());

    assert_atomic_full_sync_state_unchanged(&fixture);
    assert_eq!(
        std::fs::read_to_string(app_dir.join("appendonly.aof.manifest")).unwrap(),
        old_manifest
    );
    assert_eq!(std::fs::read(&old_base).unwrap(), b"old-base");
    assert_eq!(std::fs::read(&old_incr).unwrap(), b"old-tail");
    assert!(!app_dir.join("appendonly.aof.2.base.rdb").exists());
    assert!(!app_dir.join("appendonly.aof.2.incr.aof").exists());
    assert!(dir.join("dump.rdb").is_dir());
    if let Some(mut journal) = fixture.backend.ctx.aof_mut().journal.take() {
        journal.shutdown().unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn full_sync_aof_base_success_resets_cursors_and_installs_journal() {
    let dir = std::env::temp_dir().join(format!(
        "valkey-replica-aof-success-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("dump.rdb"), b"old-rdb").unwrap();
    let mut fixture = atomic_full_sync_fixture(&dir, &dir.join("old.aof"));
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .aof_appenddirname = "appendonlydir".to_string();
    fixture.backend.ctx.config_mut().persistence.aof_fsync =
        crate::foundation::io::persistence::AofFsync::No;

    fixture
        .backend
        .commit_streamed_rdb(
            fixture.connection_token,
            &mut fixture.link,
            fixture.commit.take().unwrap(),
        )
        .unwrap();

    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"new")
        .is_some());
    assert!(fixture
        .backend
        .ctx
        .script()
        .function_registry
        .get_library("newatomic")
        .is_some());
    assert_eq!(fixture.backend.ctx.repl_seldb(), -1);
    assert_eq!(fixture.backend.ctx.persistence().aof_selected_db(), -1);
    let journal_path = fixture
        .backend
        .ctx
        .aof()
        .journal
        .as_ref()
        .unwrap()
        .path()
        .to_string();
    assert!(journal_path.ends_with(".incr.aof"));
    assert_ne!(journal_path, fixture.old_journal_path.to_string_lossy());
    assert_eq!(
        fixture.backend.ctx.aof().fsynced_repl_offset,
        Some(fixture.link.applied_position().get()),
        "a synced RDB installed as the AOF base must publish its durable replication offset"
    );
    let queued: Vec<u8> = fixture.backend.connection_registry
        [crate::framework::capabilities::io::Token(fixture.connection_token)]
    .output
    .queue
    .iter()
    .flat_map(|frame| frame.iter().copied())
    .collect();
    assert_eq!(
        queued,
        crate::engine::distributed::replication::protocol::build_replconf_ack(
            fixture.link.applied_position(),
            None,
        ),
        "C sends the classic full-sync terminal ACK before publishing the new AOF base watermark"
    );
    let manifest =
        std::fs::read_to_string(dir.join("appendonlydir").join("appendonly.aof.manifest")).unwrap();
    assert!(manifest.contains(
        std::path::Path::new(&journal_path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
    ));
    assert!(!fixture.backend.ctx.is_loading());
    assert!(dir.join("dump.rdb").is_file());
    if let Some(mut journal) = fixture.backend.ctx.aof_mut().journal.take() {
        journal.shutdown().unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn large_rdb_object_services_events_before_install() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend
        .ctx
        .config_mut()
        .persistence
        .loading_process_events_interval_bytes = 1024;
    backend.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();

    let master_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let master_addr = master_listener.local_addr().unwrap();
    let master_peer = backend.transport.connect_tcp(master_addr).unwrap();
    let (mut master_stream, _) = master_listener.accept().unwrap();
    let master_slab = {
        backend
            .connection_registry
            .insert(Connection::new(master_peer.token, master_addr, false))
    };
    let mut link = ReplicaLink::new(
        ConnectionId(master_peer.token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n", 6379).unwrap();
    link.on_bytes(b"+OK\r\n+OK\r\n", 6379).unwrap();
    backend.replication_runtime.set_replica_link(link);

    let client_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    let client_peer = backend.transport.connect_tcp(client_addr).unwrap();
    let (mut client, _) = client_listener.accept().unwrap();
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: client_peer.token,
            peer_addr: client_addr,
        },
    );
    client.write_all(b"*1\r\n$4\r\nINFO\r\n").unwrap();
    backend.transport.wait_readable_for_test(client_peer.token);
    backend.transport.queue_test_transport_event_batch(&[(
        client_peer.token,
        true,
        false,
        false,
        false,
    )]);

    let mut rdb = b"VALKEY080\x00".to_vec();
    crate::foundation::io::persistence::save_string(&mut rdb, b"large");
    crate::foundation::io::persistence::save_string(&mut rdb, &vec![b'x'; 4096]);
    rdb.push(0xff);
    rdb.extend_from_slice(&[0; 8]);
    let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n".to_vec();
    frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    frame.extend_from_slice(&rdb);
    master_stream.write_all(&frame).unwrap();
    backend
        .transport
        .wait_pending_bytes_for_test(master_peer.token, frame.len());

    backend.recv_streaming_rdb_chunk(master_peer.token.0, master_slab);

    let mut reply = vec![0; 8192];
    let n = client.read(&mut reply).unwrap();
    let reply = String::from_utf8_lossy(&reply[..n]);
    assert!(
        reply.contains("loading:1"),
        "blocked service must run while the large RDB value is still loading: {reply}"
    );
    let installed = backend
        .ctx
        .db(0)
        .and_then(|db| db.get_no_expire(b"large"))
        .expect("the valid large RDB value must be installed after event servicing");
    assert_eq!(installed.as_bytes(), &[b'x'; 4096]);
}

#[test]
fn classic_framing_bytes_do_not_delay_first_payload_boundary() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();
    backend
        .ctx
        .config_mut()
        .persistence
        .loading_process_events_interval_bytes = 1024;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind classic RDB peer");
    let peer_addr = listener.local_addr().expect("classic RDB peer address");
    let peer = backend
        .transport
        .connect_tcp(peer_addr)
        .expect("connect classic RDB peer");
    let (mut primary_stream, _) = listener.accept().expect("accept classic RDB peer");
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: peer.token,
            peer_addr,
        },
    );
    let connection_token = peer.token.0;

    let aborted = ReplicaLink::new(
        ConnectionId(peer.token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    backend
        .classic_rdb_loading_progress
        .begin_session(aborted.session_id());
    assert!(!backend
        .classic_rdb_loading_progress
        .record_payload(1000, 1024));

    let mut link = ReplicaLink::new(
        ConnectionId(peer.token.0),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    link.connected(None, None);
    link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379)
        .expect("advance classic link to PSYNC reply");
    backend.replication_runtime.set_replica_link(link);
    backend.ctx.set_loading(true);

    let mut rdb = b"VALKEY080\x00".to_vec();
    crate::foundation::io::persistence::save_string(&mut rdb, b"large");
    crate::foundation::io::persistence::save_string(&mut rdb, &vec![b'x'; 997]);
    rdb.push(0xff);
    rdb.extend_from_slice(&[0; 8]);
    assert_eq!(rdb.len(), 1024);
    let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n".to_vec();
    frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
    let framing_bytes = frame.len();
    frame.extend_from_slice(&rdb[..1024 - framing_bytes]);
    assert_eq!(frame.len(), 1024);
    primary_stream
        .write_all(&frame)
        .expect("send one read-budget-sized prefix");
    backend
        .transport
        .wait_pending_bytes_for_test(peer.token, frame.len());

    backend.recv_streaming_rdb_chunk(peer.token.0, connection_token);

    let payload_bytes = backend
        .replication_runtime
        .replica_link()
        .expect("classic RDB session remains active")
        .rdb_payload_bytes();
    assert_eq!(payload_bytes, (1024 - framing_bytes) as u64);
    assert_eq!(
        backend
            .classic_rdb_loading_progress
            .read_budget(crate::runtime::server::worker::IO_BUF_SIZE, 1024),
        framing_bytes,
        "the next socket read must stop exactly at the first payload boundary"
    );

    let client_listener = TcpListener::bind("127.0.0.1:0").expect("bind client peer");
    let client_addr = client_listener.local_addr().expect("client peer address");
    let client_peer = backend
        .transport
        .connect_tcp(client_addr)
        .expect("connect client peer");
    let (mut client, _) = client_listener.accept().expect("accept client peer");
    client
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set client read timeout");
    backend.add_connection(
        crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
            token: client_peer.token,
            peer_addr: client_addr,
        },
    );
    client
        .write_all(b"*1\r\n$4\r\nINFO\r\n")
        .expect("queue INFO during exact-boundary load");
    backend.transport.wait_readable_for_test(client_peer.token);
    backend.transport.queue_test_transport_event_batch(&[(
        client_peer.token,
        true,
        false,
        false,
        false,
    )]);
    primary_stream
        .write_all(&rdb[1024 - framing_bytes..])
        .expect("complete the RDB at the exact payload boundary");
    backend
        .transport
        .wait_pending_bytes_for_test(peer.token, rdb.len() - (1024 - framing_bytes));

    backend.recv_streaming_rdb_chunk(peer.token.0, connection_token);

    let mut reply = vec![0; 8192];
    let n = client
        .read(&mut reply)
        .expect("the exact payload boundary must service pending client input");
    let reply = String::from_utf8_lossy(&reply[..n]);
    assert!(
        reply.contains("loading:1"),
        "the boundary event must run before the completed value is installed: {reply}"
    );
}

#[test]
fn replicated_cluster_setslot_updates_replica_open_slot_state() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    let connection_token = add_connection(&mut backend, 41);
    let mut link = ReplicaLink::new(
        ConnectionId(41),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let target_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut state =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16_380);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16_379,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.slots[0] = true;
    primary.numslots = 1;
    let mut target = ClusterNode::new(
        target_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16_381,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(primary_id.to_string(), primary);
    state.nodes.insert(target_id.to_string(), target);
    *backend.ctx.cluster_state_mut() = Some(Box::new(state));
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"CLUSTER"),
        Bytes::from_static(b"SETSLOT"),
        Bytes::from_static(b"0"),
        Bytes::from_static(b"MIGRATING"),
        Bytes::from_static(target_id.as_bytes()),
    ]);
    backend
        .apply_replication_command(
            connection_token,
            &mut link,
            ReplicationApply::Command(args),
            CommandOrigin::Replication,
        )
        .unwrap();
    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrating_slot_target(0),
        Some(target_id)
    );
}

#[test]
fn classic_full_sync_commit_reports_master_link_up_without_dual_handoff() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.aof_mut().fsynced_repl_offset = Some(99);
    let token_value = 41;
    let connection_token = add_connection(&mut backend, token_value);
    let mut link = ReplicaLink::streaming(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    backend.ctx.set_loading(true);
    backend.ctx.replication_mut().async_loading = true;
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: Vec::new(),
        aux_fields: Vec::new(),
        repl_stream_db: 0,
        diskless: true,
        disk_backed: false,
        raw: None,
        swapped: None,
    };
    backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .expect("classic full-sync commit should succeed");
    assert_eq!(backend.ctx.config().replication.master_link_status, "up");
    assert!(!backend.ctx.replication().async_loading);
    assert_eq!(
        backend.ctx.aof().fsynced_repl_offset,
        None,
        "a new full-sync session without a durable AOF base must invalidate the old FACK watermark"
    );
    assert!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .output.queue
            .iter()
            .any(|frame| frame.starts_with(b"*3\r\n$8\r\nREPLCONF")),
        "C sends an immediate REPLCONF ACK after a successful full sync so a short repl-timeout cannot close the new replica link"
    );
}

#[test]
fn full_sync_commit_preserves_the_rdb_replication_stream_db() {
    let mut backend = ServerWorkerState::new(16).unwrap();
    let token_value = 42;
    let connection_token = add_connection(&mut backend, token_value);
    let mut link = ReplicaLink::streaming(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    link.set_selected_db(9);
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: Vec::new(),
        aux_fields: Vec::new(),
        repl_stream_db: 9,
        diskless: true,
        disk_backed: false,
        raw: None,
        swapped: None,
    };

    backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .expect("full-sync commit should succeed");

    assert_eq!(
        backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .selected_db,
        9
    );
    assert_eq!(link.selected_db(), 9);
}

#[test]
fn full_sync_commit_restores_local_cluster_replica_offset() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41;
    let connection_token = add_connection(&mut backend, token_value);
    backend.ctx.replication_mut().switch_to_replica();
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16_380);
    cluster.myself.flags &= !CLUSTER_NODE_PRIMARY;
    cluster.myself.flags |= CLUSTER_NODE_REPLICA;
    cluster.myself.repl_offset = 0;
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
    let mut link = ReplicaLink::streaming(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(42),
    );
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: Vec::new(),
        aux_fields: Vec::new(),
        repl_stream_db: 0,
        diskless: true,
        disk_backed: false,
        raw: None,
        swapped: None,
    };

    backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .expect("full-sync commit should succeed");

    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(42));
    assert_eq!(
        backend
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .myself
            .repl_offset,
        42
    );
}

#[test]
fn full_sync_commit_replaces_replica_cluster_open_slots_from_aux() {
    const SOURCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TARGET: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 42;
    let connection_token = add_connection(&mut backend, token_value);
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16_380);
    cluster.seed_migrating_slot(99, SOURCE);
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
    let mut link = ReplicaLink::streaming(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: Vec::new(),
        aux_fields: vec![(
            b"cluster-slot-states".to_vec(),
            format!("13<{SOURCE},7>{TARGET},").into_bytes(),
        )],
        repl_stream_db: 0,
        diskless: true,
        disk_backed: false,
        raw: None,
        swapped: None,
    };
    backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .expect("cluster open-slot AUX should install at full-sync commit");
    let cluster = backend.ctx.cluster_state().as_deref().unwrap();
    assert_eq!(cluster.importing_slot_source(13), Some(SOURCE));
    assert_eq!(cluster.migrating_slot_target(7), Some(TARGET));
    assert!(cluster.migrating_slot_target(99).is_none());
}

#[test]
fn full_sync_without_slot_import_aux_discards_stale_hidden_imports() {
    use crate::engine::distributed::cluster::migration::{
        MigrationJob, MigrationJobId, MigrationOperation, SlotRanges,
    };
    use crate::engine::distributed::cluster::{NodeId, SlotRange};
    const SOURCE: &[u8] = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TARGET: &[u8] = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let token_value = 43;
    let connection_token = add_connection(&mut backend, token_value);
    let mut cluster =
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16_380);
    let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
    let key = b"{atomic}stale";
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
    let ranges = SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap();
    cluster
        .migrations
        .start(MigrationJob::new(
            id.clone(),
            NodeId::parse(SOURCE).unwrap(),
            NodeId::parse(TARGET).unwrap(),
            ranges,
            MigrationOperation::Import,
            1,
        ))
        .unwrap();
    *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));
    backend.ctx.create_slot_import(
        id.clone(),
        &SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
    );
    backend
        .ctx
        .apply_slot_import(
            &id,
            CommandArgs::from_parts(vec![
                Bytes::from_static(b"SET"),
                Bytes::copy_from_slice(key),
                Bytes::from_static(b"value"),
            ]),
            &backend.registry,
        )
        .unwrap();
    assert_eq!(backend.ctx.slot_import_key_count(0, slot as usize), 1);
    let mut link = ReplicaLink::streaming(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: Vec::new(),
        aux_fields: Vec::new(),
        repl_stream_db: 0,
        diskless: true,
        disk_backed: false,
        raw: None,
        swapped: Some(backend.ctx.dbs().empty_like()),
    };
    backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .expect("full sync without import metadata should commit");
    assert_eq!(backend.ctx.slot_import_key_count(0, slot as usize), 0);
    assert!(!backend
        .ctx
        .cluster_state()
        .as_deref()
        .unwrap()
        .migrations
        .has_active_imports());
}

#[test]
fn full_sync_slot_import_aux_rebuilds_hidden_tracking_dataset() {
    use crate::engine::distributed::cluster::migration::{ImportPhase, MigrationPhase};
    use crate::foundation::io::store::Robj;
    const JOB: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const SOURCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TARGET: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    })
    .unwrap();
    let token_value = 44;
    let connection_token = add_connection(&mut backend, token_value);
    *backend.ctx.cluster_state_mut() = Some(Box::new(
        crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16_380),
    ));
    let key = b"{atomic}restored";
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
    let mut incoming = backend.ctx.dbs().empty_like();
    incoming
        .get_mut(0)
        .unwrap()
        .set(key.to_vec(), Robj::string("value"));
    let mut link = ReplicaLink::streaming(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
        backend.ctx.replication().backlog.replid().to_string(),
        offset(0),
    );
    let commit = crate::framework::distributed::replication::RdbCommit {
        function_codes: Vec::new(),
        aux_fields: vec![(
            b"cluster-slot-imports".to_vec(),
            format!("v1|{JOB}|{SOURCE}|{TARGET}|{slot}-{slot}").into_bytes(),
        )],
        repl_stream_db: 0,
        diskless: true,
        disk_backed: false,
        raw: None,
        swapped: Some(incoming),
    };
    backend
        .commit_streamed_rdb(connection_token, &mut link, commit)
        .expect("full sync with import metadata should commit");
    assert_eq!(backend.ctx.dbs().count_keys_in_slot(slot as usize), 0);
    assert_eq!(backend.ctx.slot_import_key_count(0, slot as usize), 1);
    let id = crate::engine::distributed::cluster::migration::MigrationJobId::parse(JOB.as_bytes())
        .unwrap();
    let job = backend
        .ctx
        .cluster_state()
        .as_deref()
        .unwrap()
        .migrations
        .get(&id)
        .unwrap();
    assert_eq!(
        job.phase,
        MigrationPhase::Import(ImportPhase::OccurringOnPrimary)
    );
}

#[test]
fn receiving_rdb_timeout_disconnects_master_link_and_clears_async_loading() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
    backend.ctx.config_mut().replication.master_port = 6379;
    backend.ctx.config_mut().replication.master_link_status = "sync".to_string();
    backend.ctx.config_mut().replication.repl_timeout = 1;
    backend.ctx.replication_mut().async_loading = true;
    backend.ctx.replication_mut().slave_repl_offset = offset(11_605);
    backend.ctx.replication_mut().slave_read_repl_offset = offset(11_605);
    let token_value = 41;
    add_connection(&mut backend, token_value);
    let mut link = ReplicaLink::new(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let _ = link.connected(None, None);
    let psync = b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n$5\r\nhe";
    let _ = link.on_bytes(psync, 6380).expect("handshake should decode");
    assert!(link.is_receiving_rdb());
    link.set_last_io_time_ms(crate::foundation::io::sys::time::mstime() - 2_000);
    backend.replication_runtime.set_replica_link(link);
    backend.drive_replication();
    assert!(!backend.ctx.replication().async_loading);
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
    assert_eq!(backend.ctx.replication().slave_read_repl_offset, offset(0));
    assert_eq!(backend.ctx.config().replication.master_link_status, "down");
    assert!(backend.replication_runtime.replica_link().is_none());
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(token_value)));
    assert!(
        backend
            .replication_runtime
            .master_connect_allowed(crate::foundation::io::sys::time::mstime()),
        "a replication timeout must allow one immediate reconnect attempt"
    );
}

#[test]
fn eof_framed_rdb_disconnect_before_commit_clears_replica_offsets() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 41;
    add_connection(&mut backend, token_value);
    backend.ctx.replication_mut().switch_to_replica();
    backend.ctx.replication_mut().slave_repl_offset = offset(11_605);
    backend.ctx.replication_mut().slave_read_repl_offset = offset(11_605);
    let mut link = ReplicaLink::new(
        ConnectionId(token_value),
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        },
    );
    let _ = link.connected(None, None);
    let marker = "0123456789abcdef0123456789abcdef01234567";
    let frame = format!("+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 11605\r\n$EOF:{marker}\r\n{marker}");
    let _ = link
        .on_bytes(frame.as_bytes(), 6380)
        .expect("EOF-framed full sync should decode");
    assert!(link.is_streaming());
    assert!(link.has_pending_rdb_receive());
    backend.replication_runtime.set_replica_link(link);
    backend.remove_connection(token_value);
    assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
    assert_eq!(backend.ctx.replication().slave_read_repl_offset, offset(0));
}

#[test]
fn dual_replica_waiting_for_terminal_ack_ignores_streaming_ack_timeout() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let token_value = 42;
    add_connection(&mut backend, token_value);
    backend.ctx.config_mut().replication.repl_timeout = 1;
    let replica_id = backend.replication_runtime.register_primary(
        ConnectionId(token_value),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(replica_id)
        .expect("primary link must be registered");
    link.set_state(PrimaryLinkState::Streaming);
    link.transition_to_waiting_for_rdb_channel().unwrap();
    link.transition_to_waiting_for_main_psync().unwrap();
    link.transition_dual_state(PrimaryDualChannelState::WaitingForReplicaAck)
        .unwrap();
    link.acknowledge(
        offset(0),
        crate::foundation::io::sys::time::mstime() - 2_000,
    );
    backend.disconnect_timedout_replicas();
    assert!(
        backend
            .replication_runtime
            .primary_link(replica_id)
            .is_some(),
        "dual RDB loading must not be disconnected for its intentionally suppressed terminal ACK"
    );
}

#[test]
fn grace_psync_wait_expiry_refreshes_cached_view() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    let main = ConnectionId(41);
    backend
        .replication_runtime
        .insert_grace_psync_wait_for_test(main, crate::foundation::io::sys::time::mstime() - 1);
    backend.refresh_replication_view();
    assert_eq!(
        backend.ctx.replication().view.replicas_waiting_psync,
        1,
        "cached view must first reflect the grace-held wait"
    );

    backend.drive_replication();

    assert_eq!(
        backend.ctx.replication().view.replicas_waiting_psync,
        0,
        "INFO replication must observe the grace release on the same cron tick; \
         C derives this count from the live rax and never shows stale state"
    );
}
