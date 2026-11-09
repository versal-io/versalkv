use super::*;
use crate::framework::capabilities::connection::{BlockedCmd, Connection};
use crate::runtime::server::worker::command::dispatch::ReadBatchEffects;

fn command(parts: &[&str]) -> CommandArgs {
    CommandArgs::from_parts(
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect(),
    )
}

fn throttled_transaction() -> (ServerWorkerState, Token) {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut runtime.registry);
    runtime.ctx.config_mut().replication.repl_throttling_enabled = true;
    runtime
        .ctx
        .replication_mut()
        .throttle
        .adjust(true, 32_768, true, &[(123, 65_536)]);
    runtime.ctx.throttled_clients_mut().insert(Token(123));
    let token = Token(9);
    let mut connection = Connection::new(token, "127.0.0.1:6380".parse().unwrap(), false);
    assert!(connection.transaction.begin(0, 0));
    runtime.connection_registry.insert(connection);
    (runtime, token)
}

#[test]
fn throttled_multi_write_reenters_the_transaction_queue_once() {
    let (mut runtime, token) = throttled_transaction();
    let mut batch = ReadBatchEffects::default();
    runtime.process_parsed_command(
        token,
        command(&["SET", "key", "value"]),
        "set".to_string(),
        false,
        &mut batch,
    );
    assert!(batch.replies.is_empty());
    assert!(matches!(
        runtime.connection_registry[token]
            .blocked
            .as_ref()
            .map(|blocked| &blocked.cmd),
        Some(BlockedCmd::Throttled)
    ));
    assert!(runtime.ctx.paused_clients().is_empty());
    assert_eq!(runtime.ctx.blocking_registry().count(), 0);
    assert!(runtime.connection_registry[token]
        .transaction
        .queued_commands()
        .is_empty());

    runtime.ctx.config_mut().replication.repl_throttling_enabled = false;
    runtime
        .ctx
        .replication_mut()
        .throttle
        .adjust(false, 32_768, true, &[]);
    runtime.release_throttled_clients();
    assert!(runtime.connection_registry[token].blocked.is_none());
    assert_eq!(
        runtime.connection_registry[token]
            .transaction
            .queued_commands()
            .len(),
        1
    );
    assert!(!runtime.connection_registry[token].throttle_checked);
    assert!(!runtime.ctx.db_mut(0).unwrap().exists(b"key"));
}

#[test]
fn throttled_exec_preserves_the_transaction_until_release() {
    let (mut runtime, token) = throttled_transaction();
    assert!(runtime.connection_registry[token]
        .transaction
        .queue(1, command(&["SET", "key", "value"])));
    let mut batch = ReadBatchEffects::default();
    runtime.process_parsed_command(
        token,
        command(&["EXEC"]),
        "exec".to_string(),
        false,
        &mut batch,
    );
    assert!(batch.replies.is_empty());
    assert!(matches!(
        runtime.connection_registry[token]
            .blocked
            .as_ref()
            .map(|blocked| &blocked.cmd),
        Some(BlockedCmd::Throttled)
    ));
    assert!(runtime.connection_registry[token].transaction.is_active());
    assert!(!runtime.ctx.db_mut(0).unwrap().exists(b"key"));

    runtime.ctx.config_mut().replication.repl_throttling_enabled = false;
    runtime
        .ctx
        .replication_mut()
        .throttle
        .adjust(false, 32_768, true, &[]);
    runtime.release_throttled_clients();
    assert!(runtime.connection_registry[token].blocked.is_none());
    assert!(!runtime.connection_registry[token].transaction.is_active());
    assert!(runtime.ctx.db_mut(0).unwrap().exists(b"key"));
    assert!(!runtime.connection_registry[token].throttle_checked);
}
