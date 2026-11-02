use super::*;

#[test]
fn pending_main_psync_survives_shared_backlog_output_limit() {
    let main = ConnectionId(9_220);
    let rdb = ConnectionId(9_221);
    let (mut backend, replica_id) = paired_output_limit_runtime(main, rdb);
    backend
        .ctx
        .persistence_mut()
        .begin_diskless_child(0)
        .unwrap();
    let child = backend.install_test_child(
        crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
        0,
    );
    backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
        pid: 0,
        child,
        rdb_connections: vec![rdb],
        targets: vec![replica_id],
        shared_replication_buffer_bytes: 0,
        reaped_successfully: false,
        metric_read_end: File::open("/dev/null").unwrap(),
        phase_read_end: File::open("/dev/null").unwrap(),
        phase_source: None,
        phases: Vec::new(),
        completion_state: Default::default(),
    });
    backend
        .ctx
        .config_mut()
        .replication
        .replica_output_buffer_hard_limit = 4;
    backend.ctx.replication_mut().backlog.activate();
    let frame = crate::engine::distributed::replication::protocol::encode_command(&[b"PING"]);
    backend.propagate_to_replicas(&frame);

    assert!(
        backend.connection_registry.contains(Token(main.0)),
        "the pending main PSYNC must receive the backlog rejection"
    );
    assert!(!backend.connection_registry.contains(Token(rdb.0)));
    assert!(
        backend.full_sync.direct_dual_rdb_child.is_none(),
        "the failed snapshot child must retire without closing the main"
    );
    assert!(backend
        .replication_runtime
        .primary_link(replica_id)
        .is_some());
    assert_eq!(backend.replication_runtime.channel_pair_for(main), None);
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert_eq!(
        backend.replication_runtime.min_protected_backlog_cursor(),
        None,
        "an output-limit failure must release the snapshot backlog pin"
    );
    assert_eq!(
        backend
            .ctx
            .stats()
            .client_output_buffer_limit_disconnections,
        1
    );
}
