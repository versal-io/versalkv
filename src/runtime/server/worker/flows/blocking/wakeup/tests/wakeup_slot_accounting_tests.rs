use super::*;

#[test]
fn blocked_completion_records_request_and_reply_for_the_key_slot() {
    let (mut backend, connection_token, _) = _backend_with_connection();
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let args = CommandArgs::from_parts(vec![
        Bytes::from_static(b"BLPOP"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"0"),
    ]);
    let reply = Value::Array(vec![Value::bulk("key"), Value::bulk("value")]);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_request_bytes_for_test(31);
    backend.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
        .set_custom_reply_enabled(false);
    let mut effects = backend.connection_registry
        [crate::framework::capabilities::io::Token(connection_token)]
    .take_command_effects(CommandEffectDrain::Completed)
    .expect("seeded command effects");

    let propagation_rejected = backend.wakeup_finalize_reply(
        WakeupReplyContext {
            connection_token,
            args: &args,
            blocked_start_ms: mstime().saturating_sub(2),
            rejected: false,
        },
        reply,
        &mut effects,
    );

    assert!(!propagation_rejected.propagation_rejected);
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"key") as usize;
    let counters = backend.ctx.slot_metrics().counters(slot).unwrap();
    assert!(counters.cpu_usec >= 2_000);
    assert_eq!(counters.network_bytes_in, 31);
    assert_eq!(counters.network_bytes_out, 24);
}
