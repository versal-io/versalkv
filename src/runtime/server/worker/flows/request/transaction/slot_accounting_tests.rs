use super::*;
use crate::engine::command::context::CommandConn;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::flows::request::dispatch::SpecialCommandCompletion;
use crate::runtime::server::worker::flows::request::invocation::CommandTarget;
use crate::runtime::server::worker::flows::request::outcome::{
    CommandDisposition, SpecialCommandResult,
};

#[test]
fn suppressed_exec_aggregate_reply_records_zero_slot_egress() {
    for reply_skip in [false, true] {
        let mut backend = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
        let key = Bytes::from_static(b"{exec-slot}key");
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(&key);
        let mut cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        cluster
            .add_slot(slot as usize)
            .expect("assign transaction slot");
        cluster.state = crate::engine::distributed::cluster::state::CLUSTER_OK;
        *backend.ctx.cluster_state_mut() = Some(Box::new(cluster));

        let token = Token(9);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        connection.set_custom_reply_enabled(false);
        connection.set_skip_propagation(false);
        assert!(connection.transaction.begin(0, 48));
        assert!(connection.transaction.queue(CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            key,
            Bytes::from_static(b"value"),
        ])));
        if reply_skip {
            connection.set_reply_skip(true);
        } else {
            connection.set_reply_off(true);
        }
        backend.connection_registry.insert(connection);
        let mut batch =
            crate::runtime::server::worker::flows::request::dispatch::ReadBatchEffects::default();

        backend.process_parsed_command(
            token,
            CommandArgs::from_parts(vec![Bytes::from_static(b"EXEC")]),
            "exec".to_string(),
            false,
            &mut batch,
        );

        let counters = backend
            .ctx
            .slot_metrics()
            .counters(slot as usize)
            .expect("slot metrics recorded");
        assert_eq!(counters.network_bytes_out, 0);
        assert!(batch.replies.is_empty());
        if reply_skip {
            assert!(!backend.connection_registry[token].reply_skip());
        }
        backend.connection_registry[token].discard_command_cycle();
    }
}

#[test]
fn exec_records_transaction_wire_bytes_for_its_single_slot() {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.config_mut().server.cluster_enabled = true;
    backend.ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let mut connection = Connection::new_fake();
    connection.set_token_for_test(Token(9));
    connection.set_custom_reply_enabled(false);
    assert!(connection.transaction.begin(0, 48));
    connection.set_request_bytes_for_test(14);
    assert!(connection.transaction.queue(CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"value"),
    ])));
    let _connection_token = backend.connection_registry.insert(connection);
    let mut effects = CommandEffects::default();

    let result = backend.try_handle_exec(CommandTarget::new(Token(9)), "exec", false, &mut effects);
    assert_eq!(
        result,
        SpecialCommandResult::Handled(CommandDisposition::Completed)
    );
    let reply_end = effects.replies().len();
    effects.attribute_replies(0, reply_end);
    assert_eq!(
        backend.complete_special_command(
            Token(9),
            SpecialCommandCompletion {
                disposition: CommandDisposition::Completed,
                reply_start: 0,
                reply_end,
            },
            &mut effects,
        ),
        CommandDisposition::Completed
    );

    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"key") as usize;
    let counters = backend.ctx.slot_metrics().counters(slot).unwrap();
    assert_eq!(counters.network_bytes_in, 62);
    assert_eq!(
        counters.network_bytes_out,
        effects.replies()[0].len() as u64
    );
    assert_eq!(
        counters.cpu_usec,
        backend.ctx.stats().cmd_stats["exec"].usec
    );
}
