use super::*;

#[test]
fn delayed_main_association_receives_the_entire_rdb_cohort_stream() {
    check_delayed_main_association(false);
}

#[test]
fn completed_unpaired_rdb_disconnect_preserves_late_main_association() {
    check_delayed_main_association(true);
}

fn check_delayed_main_association(should_disconnect_rdb: bool) {
    let _fork_guard = crate::framework::commands::server::save::diskless_fork_test_guard();
    let mut backend = ServerWorkerState::new(1).unwrap();
    backend
        .ctx
        .config_mut()
        .replication
        .repl_diskless_sync_max_replicas = 2;
    let (first_main, _) = setup_primary(&mut backend, 160, dual_channel_caps(), true, true);
    let (second_main, _) = setup_primary(&mut backend, 161, dual_channel_caps(), true, true);
    run_psync(&mut backend, first_main, 160, b"PSYNC");
    run_psync(&mut backend, second_main, 161, b"PSYNC");
    backend.ctx.db_mut(0).unwrap().set(
        b"cohort-key".to_vec(),
        crate::foundation::io::store::Robj::string(b"snapshot-value".as_slice()),
    );
    let (first_rdb, mut first_peer) = add_runtime_connection(&mut backend);
    let (second_rdb, mut second_peer) = add_runtime_connection(&mut backend);
    let second_public_id = backend.connection_registry[Token(second_rdb)].client_id();
    for token in [first_rdb, second_rdb] {
        backend
            .replication_runtime
            .register_rdb_candidate(ConnectionId(token))
            .unwrap();
        backend.admit_rdb_sync_for_test(ConnectionId(token));
    }
    backend.drive_pending_dual_rdb_snapshots();
    let relay = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .expect("cohort starts a relay");
    let transfer_id = relay.transfer_id;
    let marker = relay.marker.clone();
    let output_before_body = backend.ctx.stats().total_net_repl_output_bytes;
    backend
        .replication_runtime
        .associate_rdb_channel(ConnectionId(160), ConnectionId(first_rdb))
        .unwrap();
    backend
        .replication_runtime
        .finalize_dual_channel_rdb_association(ConnectionId(160), ConnectionId(first_rdb))
        .unwrap();

    // Advance the real child pipe before the second main socket associates.
    // Both dedicated RDB sockets must already own the complete stream.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !backend
        .full_sync
        .diskless_relay
        .as_ref()
        .is_some_and(|relay| relay.eof_reached)
        && std::time::Instant::now() < deadline
    {
        backend.on_diskless_pipe_readable(transfer_id);
        std::thread::yield_now();
    }
    assert!(backend
        .full_sync
        .diskless_relay
        .as_ref()
        .is_some_and(|relay| relay.eof_reached));
    let read_available = |peer: &mut TcpStream| {
        peer.set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buf = [0; 4096];
        loop {
            match peer.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    bytes.extend_from_slice(&buf[..n]);
                    if bytes.ends_with(&marker) {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("read cohort stream: {error}"),
            }
        }
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .unwrap();
        assert!(bytes.starts_with(b"$ENDOFF:"));
        bytes[line_end + 2..].to_vec()
    };
    let first = read_available(&mut first_peer);
    let second = read_available(&mut second_peer);
    assert!(first.windows(6).any(|window| window == b"VALKEY"));
    assert!(first.ends_with(&marker));
    assert_eq!(
        second, first,
        "an unassociated cohort member must receive every RDB byte before its main PSYNC arrives"
    );
    let header_len = 5 + marker.len() + 2;
    let body = &second[header_len..second.len() - marker.len()];
    let mut restored = crate::foundation::io::store::kv_engine::DbSet::new(1);
    assert_eq!(
        crate::foundation::io::persistence::load_rdb_bytes_checked(body, &mut restored, true)
            .expect("every cohort member must receive a valid RDB and checksum"),
        (1, body.len()),
    );
    assert!(restored
        .get(0)
        .unwrap()
        .get_no_expire(b"cohort-key")
        .is_some());
    assert_eq!(
        backend.ctx.stats().total_net_repl_output_bytes - output_before_body,
        2 * (first.len() - header_len) as u64,
        "wire statistics must include body and marker writes to both cohort members"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while backend.full_sync.diskless_relay.is_some() && std::time::Instant::now() < deadline {
        backend.collect_idle_diskless_child_exit();
        std::thread::yield_now();
    }
    assert!(backend.full_sync.diskless_relay.is_none());

    if should_disconnect_rdb {
        drop(second_peer);
        backend.handle_readable(
            second_rdb,
            crate::runtime::server::worker::sessions::ClientReadMode::DrainAfterHup,
        );
        assert!(!backend.connection_registry.contains(Token(second_rdb)));
        assert_eq!(
            backend.replication_runtime.psync_wait_owner_count(),
            2,
            "a completed unpaired RDB wait must survive its transport's EOF"
        );
        // The transport allocates fresh tokens; explicitly reuse the business
        // registry slot to exercise identity validation independently.
        add_connection(&mut backend, second_rdb);
        let reused_token = second_rdb;
        let reused_public_id = backend.connection_registry[Token(reused_token)].client_id();
        assert_ne!(reused_public_id, second_public_id);
        boundary_replconf(
            &mut backend,
            161,
            vec![
                Bytes::from_static(b"set-rdb-client-id"),
                Bytes::from(second_public_id.to_string()),
            ],
        );
        assert!(backend
            .replication_runtime
            .unassociated_retained_rdb_owners()
            .is_empty());
        assert_eq!(
            backend.connection_registry[Token(reused_token)].client_id(),
            reused_public_id,
            "late association must not acquire the reused RDB transport"
        );
        assert_eq!(
            backend
                .replication_runtime
                .channel_pair_for(ConnectionId(161)),
            None
        );
    } else {
        backend
            .replication_runtime
            .associate_rdb_channel(ConnectionId(161), ConnectionId(second_rdb))
            .unwrap();
        backend
            .replication_runtime
            .finalize_dual_channel_rdb_association(ConnectionId(161), ConnectionId(second_rdb))
            .unwrap();
    }
    assert!(backend
        .replication_runtime
        .primary_for_connection(ConnectionId(161))
        .unwrap()
        .dual_rdb_complete());
    backend.abort_diskless_relay_for_dataset_flush();
}

fn unpaired_transport_cohort() -> (ServerWorkerState, [Token; 2], [TcpStream; 2]) {
    use super::super::diskless_child::DisklessTarget;
    use crate::foundation::io::sys::fd::{pipe, PipeFlags};
    let mut backend = ServerWorkerState::new(1).unwrap();
    let (first, first_peer) = add_runtime_connection(&mut backend);
    let (second, second_peer) = add_runtime_connection(&mut backend);
    let mut targets = Vec::new();
    for token in [first, second] {
        backend
            .replication_runtime
            .register_rdb_candidate(ConnectionId(token))
            .unwrap();
        targets.push(DisklessTarget::Rdb {
            connection_id: ConnectionId(token),
            client_id: backend.connection_registry[Token(token)].client_id(),
        });
    }
    let (read_end, mut write_end) = pipe(PipeFlags::NONE).unwrap().into_parts();
    write_end.write_all(b"cohort-body").unwrap();
    drop(write_end);
    let (release_read, release_write) = pipe(PipeFlags::NONE).unwrap().into_parts();
    drop(release_read);
    backend.store_diskless_transport_relay(
        read_end,
        release_write,
        b"cohort-marker".to_vec(),
        targets,
        false,
        0,
    );
    (
        backend,
        [Token(first), Token(second)],
        [first_peer, second_peer],
    )
}

#[test]
fn unpaired_rdb_disconnect_retires_only_its_snapshot_target() {
    let (mut backend, [first, second], [_first_peer, mut second_peer]) =
        unpaired_transport_cohort();
    backend.remove_connection(first.0);
    let relay = backend.full_sync.diskless_relay.as_ref().unwrap();
    assert_eq!(relay.targets.len(), 1);
    let transfer_id = relay.transfer_id;
    backend.on_diskless_pipe_readable(transfer_id);
    backend.on_diskless_pipe_readable(transfer_id);
    second_peer
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .unwrap();
    let mut expected = [0; 24];
    second_peer.read_exact(&mut expected).unwrap();
    assert_eq!(&expected, b"cohort-bodycohort-marker");
    backend.remove_connection(second.0);
    assert!(backend.full_sync.diskless_relay.is_none());
}

#[test]
fn incomplete_unpaired_rdb_disconnect_releases_wait_owner() {
    let (mut backend, [first, second], [first_peer, _second_peer]) = unpaired_transport_cohort();
    let boundary = backend.ctx.replication().backlog.snapshot_boundary();
    backend
        .replication_runtime
        .insert_unpaired_rdb_psync_wait_for_test(ConnectionId(first.0), boundary);
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 1);
    drop(first_peer);
    backend.handle_readable(
        first.0,
        crate::runtime::server::worker::sessions::ClientReadMode::DrainAfterHup,
    );
    assert!(!backend.connection_registry.contains(first));
    assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
    assert!(backend
        .replication_runtime
        .unassociated_retained_rdb_owners()
        .is_empty());
    backend.remove_connection(second.0);
    assert!(backend.full_sync.diskless_relay.is_none());
}

#[test]
fn reused_rdb_transport_slot_cannot_receive_an_old_snapshot() {
    let (mut backend, [first, second], [mut first_peer, _second_peer]) =
        unpaired_transport_cohort();
    let old = backend.connection_registry.remove(first).unwrap();
    backend
        .replication_runtime
        .remove_connection(ConnectionId(first.0));
    backend
        .connection_registry
        .register(Connection::new(first, old.peer_addr, false))
        .unwrap();
    assert_ne!(
        backend.connection_registry[first].client_id(),
        old.client_id()
    );
    let transfer_id = backend
        .full_sync
        .diskless_relay
        .as_ref()
        .unwrap()
        .transfer_id;
    backend.on_diskless_pipe_readable(transfer_id);
    first_peer.set_nonblocking(true).unwrap();
    assert_eq!(
        first_peer.read(&mut [0; 64]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        backend
            .full_sync
            .diskless_relay
            .as_ref()
            .unwrap()
            .targets
            .len(),
        1,
        "a stale session must be retired so it cannot strand EOF completion"
    );
    backend.remove_connection(second.0);
    assert!(backend.full_sync.diskless_relay.is_none());
    assert!(backend.connection_registry.contains(first));
}
