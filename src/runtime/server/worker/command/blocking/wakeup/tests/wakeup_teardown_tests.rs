use super::*;

#[test]
fn teardown_isolates_blpop_before_lpush_and_retries_l1_only() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    let peer_addr = "127.0.0.1:0".parse().unwrap();
    let listener = backend.transport.bind_tcp(peer_addr).unwrap();
    let token = listener.token;
    backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
    let connection_token = token.0;
    let key = Bytes::from_static(b"pending-teardown-list");
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .blocked = Some(BlockedState {
        cmd: BlockedCmd::Blpop,
        keys: vec![key.clone()],
        timeout_ms: 0,
        start_time_ms: mstime(),
        orig_args: CommandArgs::from_parts(vec![
            Bytes::from_static(b"BLPOP"),
            key.clone(),
            Bytes::from_static(b"0"),
        ]),
        req_bytes: 32,
        orig_wrapped: None,
    });
    let session = backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .session_id();
    backend
        .ctx
        .blocking_registry_mut()
        .register_client(0, std::slice::from_ref(&key), session);
    backend
        .transport
        .deregister_for_test(token)
        .expect("deregister client transport");
    backend.remove_connection(token.0);

    // Centralized teardown isolates the client at once: unblocked, out of the
    // blocking registry and slab. Only the retained L1 registration + retry entry survive.
    assert!(backend.is_pending_transport_removal(token));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(token.0)));
    assert!(!backend.ctx.blocking_registry().is_client_blocked(&session));

    let mut lpush_client = Connection::new_fake();
    let lpush_args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"LPUSH"),
        key.clone(),
        Bytes::from_static(b"value"),
    ]);
    let lpush = backend
        .registry
        .lookup(&lpush_args.name)
        .expect("LPUSH command registered")
        .clone();
    assert_eq!(
        lpush.execute(&mut lpush_client, &lpush_args, &mut backend.ctx),
        Value::integer(1)
    );

    backend.transport.clear_interest_requests_for_test();
    let (mut replication, mut aof) = (Vec::new(), Vec::new());
    backend.wakeup_blocked_on_keys(&[(0, key.clone())], Some(&mut replication), Some(&mut aof));

    // The isolated client is not woken by the LPUSH; value stays, nothing propagates.
    assert_eq!(
        backend
            .ctx
            .db(0)
            .and_then(|db| db.get_no_expire(&key))
            .and_then(|value| match value {
                crate::foundation::io::store::Robj::ListQuicklist(list) => {
                    Some(list.len())
                }
                _ => None,
            }),
        Some(1)
    );
    assert!(replication.is_empty());
    assert!(aof.is_empty());
    assert!(backend.transport.interest_requests_for_test().is_empty());

    backend
        .transport
        .reregister_for_test(token)
        .expect("reregister client transport");
    backend.run_background_tasks();

    assert!(!backend.is_pending_transport_removal(token));
    assert!(!backend
        .connection_registry
        .contains(crate::framework::capabilities::io::Token(token.0)));
    assert!(!backend.ctx.blocking_registry().is_client_blocked(&session));
}
