use super::*;
use std::fs::File;

#[test]
fn classic_rdb_only_marker_waits_for_producer_success_before_close() {
    use crate::runtime::server::worker::event_loop::sources::{ChildExitOutcome, ChildOwner};
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    for exit_code in [0, 1] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (main, mut peer) = add_runtime_connection(&mut backend);
        peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let id = backend.replication_runtime.register_primary(
            ConnectionId(main),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: vec!["eof".to_string()],
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .set_rdb_only(ConnectionId(main), true);
        backend
            .replication_runtime
            .primary_link_mut(id)
            .unwrap()
            .set_state(PrimaryLinkState::PreparingFullResync);
        let (child_input, mut finish_child) = std::io::pipe().unwrap();
        let child_input_fd: OwnedFd = child_input.into();
        let mut process = Command::new("/bin/sh")
            .args(["-c", &format!("read -r ignored; exit {exit_code}")])
            .stdin(Stdio::from(File::from(child_input_fd)))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = process.id() as i32;
        let child = backend
            .child_registry
            .register_test_child(ChildOwner::DisklessReplication, pid);
        backend.child_registry.mark_pidfd_registered(child);
        backend
            .ctx
            .persistence_mut()
            .begin_diskless_child(pid)
            .unwrap();
        let (reader, writer) = std::io::pipe().unwrap();
        let reader_fd: OwnedFd = reader.into();
        drop(writer);
        let (release, mut child_peer) = UnixStream::pair().unwrap();
        child_peer
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let release_fd: OwnedFd = release.into();
        let marker = b"terminal-marker";
        backend.store_diskless_relay(
            File::from(reader_fd),
            File::from(release_fd),
            marker.to_vec(),
            vec![id],
            true,
            0,
        );
        let transfer = backend
            .full_sync
            .diskless_relay
            .as_ref()
            .unwrap()
            .transfer_id;

        backend.on_diskless_pipe_readable(transfer);

        let mut notification = [0];
        child_peer.read_exact(&mut notification).unwrap();
        assert_eq!(notification, [1]);
        let mut wire = vec![0; marker.len()];
        peer.read_exact(&mut wire).unwrap();
        assert_eq!(wire, marker);
        assert!(backend.connection_registry.contains(Token(main)));
        assert_eq!(backend.ctx.persistence().diskless_child_pid(), Some(pid));
        assert!(backend.child_registry.contains(child));
        assert_eq!(
            backend
                .replication_runtime
                .primary_link(id)
                .unwrap()
                .state(),
            PrimaryLinkState::PreparingFullResync,
            "RDB-only marker completion must neither await ACK nor close a live producer"
        );
        assert!(backend.full_sync.diskless_relay.is_some());
        assert!(backend.try_begin_diskless_fork(&[]).is_none());

        finish_child.write_all(b"finish\n").unwrap();
        drop(finish_child);
        let status = process.wait().unwrap();
        assert_eq!(
            status.code(),
            Some(exit_code),
            "marker flush must not kill the producer"
        );
        backend
            .child_registry
            .record_exit_for_test(child, ChildExitOutcome::Exited(exit_code));
        backend.collect_idle_diskless_child_exit();

        assert!(!backend.connection_registry.contains(Token(main)));
        assert!(backend.replication_runtime.primary_link(id).is_none());
        assert_eq!(backend.ctx.persistence().diskless_child_pid(), None);
        assert!(!backend.child_registry.contains(child));
        assert!(backend.full_sync.diskless_relay.is_none());
        assert_eq!(
            peer.read(&mut [0]).unwrap(),
            0,
            "no commands may follow the RDB-only marker"
        );
    }
}

#[test]
fn classic_rdb_only_reaped_producer_waits_for_marker_flush() {
    use crate::runtime::server::worker::event_loop::sources::{
        ChildExitOutcome, ChildOwner, TransferId,
    };
    use crate::runtime::server::worker::replication::diskless_child::DisklessRelay;
    use std::time::Duration;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main, mut peer) = add_runtime_connection(&mut backend);
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let id = backend.replication_runtime.register_primary(
        ConnectionId(main),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .set_rdb_only(ConnectionId(main), true);
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .set_state(PrimaryLinkState::PreparingFullResync);
    let pid = i32::MAX;
    let child = backend
        .child_registry
        .register_test_child(ChildOwner::DisklessReplication, pid);
    backend.child_registry.mark_pidfd_registered(child);
    backend
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let marker = b"terminal-marker";
    backend.connection_registry[Token(main)].queue_reply(marker.to_vec());
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: TransferId(1),
        source: None,
        child_release_write_end: None,
        targets: vec![id.into()],
        marker: marker.to_vec(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: [id.into()].into_iter().collect(),
        marker_sent_targets: Default::default(),
        completion_announced: false,
        completion_state: Default::default(),
    });

    backend.collect_idle_diskless_child_exit();

    assert_eq!(backend.ctx.persistence().diskless_child_pid(), None);
    assert!(!backend.child_registry.contains(child));
    assert!(backend.connection_registry[Token(main)].has_pending_output());
    assert!(backend.full_sync.diskless_relay.is_some());

    backend.on_diskless_connection_writable(Token(main));

    assert!(!backend.connection_registry.contains(Token(main)));
    assert!(backend.full_sync.diskless_relay.is_none());
    let mut wire = Vec::new();
    peer.read_to_end(&mut wire).unwrap();
    assert_eq!(
        wire, marker,
        "the queued marker must flush exactly once before close"
    );
}

#[test]
fn classic_rdb_only_settles_while_ordinary_replica_keeps_ack_and_backlog_gate() {
    use crate::runtime::server::worker::event_loop::sources::{ChildExitOutcome, ChildOwner};
    use std::os::fd::OwnedFd;
    use std::time::Duration;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"before")
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    let mut ids = Vec::new();
    let mut peers = Vec::new();
    let mut tokens = Vec::new();
    for is_rdb_only in [true, false] {
        let (main, peer) = add_runtime_connection(&mut backend);
        peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let id = backend.replication_runtime.register_primary(
            ConnectionId(main),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: vec!["eof".to_string()],
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .set_rdb_only(ConnectionId(main), is_rdb_only);
        let link = backend.replication_runtime.primary_link_mut(id).unwrap();
        link.set_state(PrimaryLinkState::PreparingFullResync);
        link.set_full_resync_boundary(boundary).unwrap();
        ids.push(id);
        peers.push(peer);
        tokens.push(main);
    }
    let tail = protocol::encode_command(&[b"SET", b"after", b"value"]);
    backend.ctx.replication_mut().backlog.append(&tail).unwrap();
    let pid = i32::MAX;
    let child = backend
        .child_registry
        .register_test_child(ChildOwner::DisklessReplication, pid);
    backend.child_registry.mark_pidfd_registered(child);
    backend
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let (reader, writer) = std::io::pipe().unwrap();
    let reader_fd: OwnedFd = reader.into();
    drop(writer);
    let marker = b"terminal-marker";
    backend.store_diskless_relay(
        File::from(reader_fd),
        File::open("/dev/null").unwrap(),
        marker.to_vec(),
        ids.clone(),
        true,
        0,
    );
    let transfer = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .unwrap()
        .transfer_id;

    backend.on_diskless_pipe_readable(transfer);

    assert!(!backend.connection_registry.contains(Token(tokens[0])));
    assert!(backend.replication_runtime.primary_link(ids[0]).is_none());
    assert!(backend.connection_registry.contains(Token(tokens[1])));
    assert_eq!(
        backend.full_sync.diskless_relay.as_ref().unwrap().targets,
        vec![ids[1].into()]
    );
    let backlog = &backend.ctx.replication().backlog;
    let link = backend
        .replication_runtime
        .primary_link_mut(ids[1])
        .unwrap();
    assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
    assert_eq!(link.next_send_cursor(), boundary.stream_cursor());
    assert!(link.pump(backlog).unwrap().is_empty());
    assert_eq!(link.next_send_cursor(), boundary.stream_cursor());
    assert!(backend.try_begin_diskless_fork(&[]).is_none());
    for peer in &mut peers {
        let mut wire = vec![0; marker.len()];
        peer.read_exact(&mut wire).unwrap();
        assert_eq!(wire, marker);
    }
    assert_eq!(peers[0].read(&mut [0]).unwrap(), 0);
    peers[1].set_nonblocking(true).unwrap();
    assert_eq!(
        peers[1].read(&mut [0]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let link = backend
        .replication_runtime
        .primary_link_mut(ids[1])
        .unwrap();
    link.acknowledge(
        boundary.reported_offset(),
        crate::foundation::io::sys::time::mstime(),
    );
    assert!(link.promote_full_resync_on_ack());

    backend.on_diskless_terminal_ack(ids[1]);

    assert!(backend.full_sync.diskless_relay.is_none());
    let backlog = &backend.ctx.replication().backlog;
    assert_eq!(
        backend
            .replication_runtime
            .primary_link_mut(ids[1])
            .unwrap()
            .pump(backlog)
            .unwrap(),
        tail
    );
}

#[test]
fn revoking_rdb_only_after_marker_flush_restores_terminal_ack_gate() {
    use crate::runtime::server::worker::event_loop::sources::{ChildExitOutcome, ChildOwner};
    use std::os::fd::OwnedFd;
    use std::process::{Command, Stdio};

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.ctx.replication_mut().backlog.activate();
    backend
        .ctx
        .replication_mut()
        .backlog
        .append(b"before")
        .unwrap();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    let (main, mut peer) = add_runtime_connection(&mut backend);
    let id = backend.replication_runtime.register_primary(
        ConnectionId(main),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .set_rdb_only(ConnectionId(main), true);
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    link.set_state(PrimaryLinkState::PreparingFullResync);
    link.set_full_resync_boundary(boundary).unwrap();
    let tail = protocol::encode_command(&[b"SET", b"after", b"value"]);
    backend.ctx.replication_mut().backlog.append(&tail).unwrap();
    let (child_input, mut finish_child) = std::io::pipe().unwrap();
    let child_input_fd: OwnedFd = child_input.into();
    let mut process = Command::new("/bin/sh")
        .args(["-c", "read -r ignored"])
        .stdin(Stdio::from(File::from(child_input_fd)))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = process.id() as i32;
    let child = backend
        .child_registry
        .register_test_child(ChildOwner::DisklessReplication, pid);
    backend.child_registry.mark_pidfd_registered(child);
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let (reader, writer) = std::io::pipe().unwrap();
    let reader_fd: OwnedFd = reader.into();
    drop(writer);
    let marker = b"terminal-marker";
    backend.store_diskless_relay(
        File::from(reader_fd),
        File::open("/dev/null").unwrap(),
        marker.to_vec(),
        vec![id],
        true,
        0,
    );
    let transfer = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .unwrap()
        .transfer_id;
    backend.on_diskless_pipe_readable(transfer);
    let mut wire = vec![0; marker.len()];
    peer.read_exact(&mut wire).unwrap();
    assert_eq!(wire, marker);
    finish_child.write_all(b"finish\n").unwrap();
    drop(finish_child);
    assert!(process.wait().unwrap().success());
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(id)
            .unwrap()
            .state(),
        PrimaryLinkState::PreparingFullResync
    );

    backend
        .replication_runtime
        .set_rdb_only(ConnectionId(main), false);
    backend
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    backend.finish_diskless_relay_if_ready();

    assert!(backend.connection_registry.contains(Token(main)));
    assert!(backend.full_sync.diskless_relay.is_some());
    let backlog = &backend.ctx.replication().backlog;
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
    assert!(link.pump(backlog).unwrap().is_empty());
    assert_eq!(link.next_send_cursor(), boundary.stream_cursor());
    link.acknowledge(
        boundary.reported_offset(),
        crate::foundation::io::sys::time::mstime(),
    );
    assert!(link.promote_full_resync_on_ack());
    backend.on_diskless_terminal_ack(id);
    assert!(backend.full_sync.diskless_relay.is_none());
    let backlog = &backend.ctx.replication().backlog;
    assert_eq!(
        backend
            .replication_runtime
            .primary_link_mut(id)
            .unwrap()
            .pump(backlog)
            .unwrap(),
        tail
    );
}

#[test]
fn classic_rdb_only_filtered_batches_release_before_any_client_reads() {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    use crate::foundation::io::store::Robj;
    use crate::framework::distributed::replication::RdbFilterMode;
    use std::time::{Duration, Instant};

    let _fork_guard = crate::framework::commands::server::save::diskless_fork_test_guard();
    let function =
        "#!lua name=settlement\nserver.register_function('settlementfn', function() return 1 end)";
    for method in [BgsaveMethod::Fork, BgsaveMethod::Forkless] {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.config_mut().replication.repl_diskless_sync = true;
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 30;
        backend
            .ctx
            .config_mut()
            .replication
            .dual_channel_replication = false;
        backend.ctx.config_mut().persistence.bgsave_default_method = method;
        backend
            .ctx
            .script_mut()
            .function_registry
            .load_library("settlement", function)
            .unwrap();
        backend
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"before".to_vec(), Robj::string(b"value".to_vec()));
        let mut ids = Vec::new();
        let mut peers = Vec::new();
        for filter in [
            RdbFilterMode::Empty,
            RdbFilterMode::All,
            RdbFilterMode::Functions,
        ] {
            let (main, peer) = add_runtime_connection(&mut backend);
            peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let id = backend.replication_runtime.register_primary(
                ConnectionId(main),
                ReplicaConnectionInfo {
                    ip: "127.0.0.1".to_string(),
                    port: 6381,
                    capability: vec!["eof".to_string(), "inband-repl".to_string()],
                    rdb_filter_mode: filter,
                },
            );
            backend
                .replication_runtime
                .set_rdb_only(ConnectionId(main), true);
            assert!(run_psync(&mut backend, main, main, b"PSYNC").is_empty());
            ids.push(id);
            peers.push((peer, filter));
        }
        for index in 0..ids.len() {
            backend.fire_deferred_full_resyncs(ids[index..].to_vec());
            let relay = backend
                .full_sync
                .diskless_relay
                .as_ref()
                .expect("next filter must start");
            assert_eq!(relay.targets, vec![ids[index].into()]);
            let transfer = relay.transfer_id;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                backend.on_diskless_pipe_readable(transfer);
                backend.finish_diskless_relay_if_ready();
                backend.collect_idle_diskless_child_exit();
                let is_drained = backend
                    .full_sync
                    .diskless_relay
                    .as_ref()
                    .is_none_or(|relay| {
                        relay.eof_reached
                            && relay
                                .targets
                                .iter()
                                .all(|id| relay.marker_sent_targets.contains(id))
                    });
                if is_drained && !backend.ctx.persistence().has_active_child() {
                    break;
                }
                assert!(Instant::now() < deadline, "producer did not drain and exit");
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(
                backend.full_sync.diskless_relay.is_none(),
                "completed RDB-only cohort must release without ACK"
            );
            assert!(backend
                .replication_runtime
                .primary_link(ids[index])
                .is_none());
        }
        assert_eq!(
            backend.ctx.stats().total_forks,
            if method == BgsaveMethod::Fork { 3 } else { 2 }
        );
        // Read the first-started peer last, matching a client waiting on a later filter.
        for (mut peer, filter) in peers.into_iter().rev() {
            let mut wire = Vec::new();
            peer.read_to_end(&mut wire).unwrap();
            let header_end = wire.windows(2).position(|bytes| bytes == b"\r\n").unwrap() + 2;
            assert!(wire[..header_end].starts_with(b"+FULLRESYNC "));
            let (loaded, dbs, functions) =
                ServerWorkerState::decode_eof_rdb_for_test(&wire[header_end..], 1);
            assert_eq!(loaded, usize::from(filter == RdbFilterMode::All));
            assert_eq!(dbs.db_size(0), usize::from(filter == RdbFilterMode::All));
            assert_eq!(
                functions,
                if filter == RdbFilterMode::Empty {
                    Vec::new()
                } else {
                    vec![function.as_bytes().to_vec()]
                }
            );
        }
    }
}

#[test]
fn pipe_eof_keeps_a_reaped_classic_relay_until_terminal_ack() {
    use crate::runtime::server::worker::event_loop::sources::{ChildExitOutcome, ChildOwner};
    use std::os::fd::OwnedFd;
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main, _peer) = add_runtime_connection(&mut backend);
    let id = backend.replication_runtime.register_primary(
        ConnectionId(main),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    backend
        .replication_runtime
        .primary_link_mut(id)
        .unwrap()
        .set_state(PrimaryLinkState::PreparingFullResync);
    let pid = i32::MAX;
    let child = backend
        .child_registry
        .register_test_child(ChildOwner::DisklessReplication, pid);
    backend.child_registry.mark_pidfd_registered(child);
    backend
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    let (reader, writer) = std::io::pipe().unwrap();
    let reader_fd: OwnedFd = reader.into();
    drop(writer);
    backend.store_diskless_relay(
        File::from(reader_fd),
        File::open("/dev/null").unwrap(),
        b"terminal-marker".to_vec(),
        vec![id],
        true,
        0,
    );
    let transfer = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .unwrap()
        .transfer_id;

    backend.on_diskless_pipe_readable(transfer);

    assert_eq!(backend.ctx.persistence().diskless_child_pid(), None);
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(id)
            .unwrap()
            .state(),
        PrimaryLinkState::WaitingForReplicaAck
    );
    assert!(
        backend.full_sync.diskless_relay.is_some(),
        "EOF must not bypass terminal ACK after reaping the completed child"
    );
    assert!(backend.try_begin_diskless_fork(&[]).is_none());
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    link.acknowledge(offset(0), crate::foundation::io::sys::time::mstime());
    assert!(link.promote_full_resync_on_ack());
    backend.finish_diskless_relay_if_ready();
    assert!(backend.full_sync.diskless_relay.is_none());
}

#[test]
fn classic_diskless_child_is_released_after_markers_without_waiting_for_ack() {
    use crate::runtime::server::worker::replication::diskless_child::DisklessRelay;
    use std::io::Read;
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main, _peer) = add_runtime_connection(&mut backend);
    let id = backend.replication_runtime.register_primary(
        ConnectionId(main),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    link.set_state(PrimaryLinkState::PreparingFullResync);
    link.finish_full_resync();
    let (release, mut child) = UnixStream::pair().unwrap();
    child
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let release_fd: OwnedFd = release.into();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::event_loop::sources::TransferId(0),
        source: None,
        child_release_write_end: Some(File::from(release_fd)),
        targets: vec![id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: [id.into()].into_iter().collect(),
        marker_sent_targets: [id.into()].into_iter().collect(),
        completion_announced: false,
        completion_state: crate::runtime::server::worker::replication::diskless_child::DisklessCompletionState::default(),
    });

    backend.finish_diskless_relay_if_ready();

    let mut notification = [0];
    assert_eq!(
        child.read(&mut notification).unwrap(),
        1,
        "C releases its completed producer at pipe EOF before terminal ACK"
    );
    assert_eq!(notification, [1]);
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(id)
            .unwrap()
            .state(),
        PrimaryLinkState::WaitingForReplicaAck,
        "releasing the producer must not start the unacknowledged command stream"
    );
    assert!(backend.full_sync.diskless_relay.is_some());
    assert!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .unwrap()
            .completion_announced,
        "C announces drained RDB transport before the terminal ACK"
    );
    backend.finish_diskless_relay_if_ready();
    assert!(!backend
        .full_sync
        .diskless_relay
        .as_ref()
        .unwrap()
        .completion_state
        .published());
}

#[test]
fn completed_diskless_child_retires_signal_source_while_waiting_for_ack() {
    use crate::foundation::io::transport::OwnedPollSource;
    use crate::runtime::server::worker::event_loop::sources::{
        ChildExitOutcome, ChildOwner, ChildSignalOwner, SourceOwner,
    };
    use crate::runtime::server::worker::replication::diskless_child::DisklessRelay;
    use std::os::fd::OwnedFd;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let pid = i32::MAX;
    let child = backend
        .child_registry
        .register_test_child(ChildOwner::DisklessReplication, pid);
    backend.child_registry.mark_pidfd_registered(child);
    let (reader, _writer) = std::io::pipe().unwrap();
    let reader_fd: OwnedFd = reader.into();
    let signal = backend
        .install_runtime_source(
            OwnedPollSource::completion(File::from(reader_fd)),
            SourceOwner::ChildSignal(ChildSignalOwner::PidFd(child)),
        )
        .unwrap();
    backend.child_registry.set_pidfd_source(child, signal);
    backend
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::event_loop::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: Vec::new(),
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: Default::default(),
        marker_sent_targets: Default::default(),
        completion_announced: false,
        completion_state: Default::default(),
    });

    backend.collect_idle_diskless_child_exit();

    assert!(
        backend.runtime_sources.resolve(signal).is_none(),
        "a readable completed pidfd must not spin while terminal ACK is pending"
    );
    assert!(!backend.child_registry.contains(child));
    assert!(backend.full_sync.diskless_relay.is_some());
    assert_eq!(backend.ctx.persistence().diskless_child_pid(), None);
}

#[test]
fn successful_diskless_exit_releases_process_owner_before_terminal_ack() {
    use crate::runtime::server::worker::event_loop::sources::{ChildExitOutcome, ChildOwner};
    use crate::runtime::server::worker::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    let (main, _peer) = add_runtime_connection(&mut backend);
    let id = backend.replication_runtime.register_primary(
        ConnectionId(main),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    link.set_state(PrimaryLinkState::PreparingFullResync);
    link.finish_full_resync();
    let pid = i32::MAX;
    let child = backend
        .child_registry
        .register_test_child(ChildOwner::DisklessReplication, pid);
    backend.child_registry.mark_pidfd_registered(child);
    backend
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(pid)
        .unwrap();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::event_loop::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: vec![id.into()],
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: [id.into()].into_iter().collect(),
        marker_sent_targets: [id.into()].into_iter().collect(),
        completion_announced: false,
        completion_state: Default::default(),
    });
    backend.finish_diskless_relay_if_ready();

    backend.collect_idle_diskless_child_exit();

    assert_eq!(
        backend.ctx.persistence().diskless_child_pid(),
        None,
        "a reaped process must not retain an owner that can signal its reused PID"
    );
    assert!(backend.ctx.persistence().rdb().last_bgsave_duration_sec >= 0);
    assert!(!backend.child_registry.contains(child));
    assert!(backend.full_sync.diskless_relay.is_some());
    let later_pid = i32::MAX - 1;
    backend
        .ctx
        .persistence_mut()
        .begin_disk_sync_child(later_pid)
        .unwrap();
    let link = backend.replication_runtime.primary_link_mut(id).unwrap();
    assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
    link.acknowledge(offset(0), crate::foundation::io::sys::time::mstime());
    assert!(link.promote_full_resync_on_ack());

    backend.finish_diskless_relay_if_ready();

    assert!(backend.full_sync.diskless_relay.is_none());
    assert_eq!(
        backend.ctx.persistence().disk_sync_child_pid(),
        Some(later_pid)
    );
    assert_eq!(
        backend
            .replication_runtime
            .primary_link(id)
            .unwrap()
            .state(),
        PrimaryLinkState::Streaming
    );
}

#[test]
fn pending_diskless_relay_blocks_another_producer_after_child_exit() {
    use crate::runtime::server::worker::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::event_loop::sources::TransferId(0),
        source: None,
        child_release_write_end: None,
        targets: Vec::new(),
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: true,
        marker_queued_targets: Default::default(),
        marker_sent_targets: Default::default(),
        completion_announced: false,
        completion_state: Default::default(),
    });
    assert!(!backend.ctx.persistence().has_active_child());

    let producer = backend.try_begin_diskless_fork(&[]);

    assert!(
        producer.is_none(),
        "a new producer must not occupy a relay still owned by unacknowledged replicas"
    );
    backend.ctx.config_mut().replication.repl_diskless_sync = true;
    backend
        .ctx
        .config_mut()
        .replication
        .repl_diskless_sync_delay = 0;
    let (main, _peer) = add_runtime_connection(&mut backend);
    backend.replication_runtime.register_primary(
        ConnectionId(main),
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6381,
            capability: vec!["eof".to_string()],
            rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
        },
    );
    assert!(
        run_psync(&mut backend, main, main, b"PSYNC").is_empty(),
        "PSYNC must remain queued instead of falling back to an inline snapshot"
    );
    backend.drive_deferred_full_resyncs();
    assert!(backend
        .replication_runtime
        .primary_for_connection(ConnectionId(main))
        .unwrap()
        .is_deferred_diskless());
    assert!(!backend.connection_registry[Token(main)].has_pending_output());
}
