use super::*;
use std::fs::File;

#[test]
fn pipe_eof_keeps_a_reaped_classic_relay_until_terminal_ack() {
    use crate::runtime::server::worker::reactor::sources::{ChildExitOutcome, ChildOwner};
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
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
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
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
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
        completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
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
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::reactor::sources::{
        ChildExitOutcome, ChildOwner, ChildSignalOwner, SourceOwner,
    };
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
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
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
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
    use crate::runtime::server::worker::reactor::sources::{ChildExitOutcome, ChildOwner};

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
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
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
    use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

    let mut backend = ServerWorkerState::new(1).unwrap();
    backend.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
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
