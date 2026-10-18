use super::*;
use crate::engine::command::context::CommandConn;

fn dispatch_command(
    backend: &mut ServerWorkerState,
    token: Token,
    batch: &mut ReadBatchEffects,
    parts: &[&[u8]],
) -> LoopAction {
    let args = make_args(parts);
    let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
    backend.process_parsed_command(token, args, name, false, batch)
}

fn joined_replies(batch: &ReadBatchEffects) -> Vec<u8> {
    batch
        .replies
        .iter()
        .flat_map(|reply| reply.iter().copied())
        .collect()
}

fn backend_with_connection() -> (ServerWorkerState, Token) {
    let mut backend = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut backend.registry);
    let token = Token(add_fake_connection(&mut backend));
    backend.connection_registry[token].set_custom_reply_enabled(false);
    backend.connection_registry[token].set_skip_propagation(false);
    (backend, token)
}

fn joined_connection_output(backend: &ServerWorkerState, token: Token) -> Vec<u8> {
    backend.connection_registry[token]
        .output
        .queue
        .iter()
        .flat_map(|frame| frame.iter().copied())
        .collect()
}

fn run_confirmation_sequence(
    subscribe: &[&[u8]],
    unsubscribe: &[&[u8]],
    mode: &[u8],
) -> (ServerWorkerState, Token, Vec<u8>, usize) {
    let (mut backend, token) = backend_with_connection();
    let mut batch = ReadBatchEffects::default();
    dispatch_command(
        &mut backend,
        token,
        &mut batch,
        &[b"CLIENT", b"REPLY", mode],
    );
    dispatch_command(&mut backend, token, &mut batch, subscribe);
    if mode.eq_ignore_ascii_case(b"SKIP") {
        dispatch_command(
            &mut backend,
            token,
            &mut batch,
            &[b"CLIENT", b"REPLY", b"SKIP"],
        );
    }
    dispatch_command(&mut backend, token, &mut batch, unsubscribe);
    let output = joined_connection_output(&backend, token);
    let last_bytes = backend.connection_registry[token]
        .output
        .queue
        .back()
        .map(Vec::len)
        .expect("unsubscribe confirmation");
    (backend, token, output, last_bytes)
}

fn assert_contains_confirmation(output: &[u8], name: &[u8]) {
    assert!(
        output.windows(name.len()).any(|part| part == name),
        "missing confirmation {:?} in {}",
        String::from_utf8_lossy(name),
        String::from_utf8_lossy(output)
    );
}

fn assert_confirmations_bypass_reply_mode(
    subscribe: &[&[u8]],
    unsubscribe: &[&[u8]],
    mode: &[u8],
    subscribe_name: &[u8],
    unsubscribe_name: &[u8],
) {
    let (mut backend, token, output, last_confirmation_bytes) =
        run_confirmation_sequence(subscribe, unsubscribe, mode);
    assert_contains_confirmation(&output, subscribe_name);
    assert_contains_confirmation(&output, unsubscribe_name);
    assert_eq!(
        backend.connection_registry[token].reply_bytes_current_command(),
        last_confirmation_bytes
    );
    assert_eq!(
        backend.connection_registry[token].tot_net_out,
        output.len() as u64
    );
    if mode.eq_ignore_ascii_case(b"SKIP") {
        assert!(!backend.connection_registry[token].reply_skip());
    }
    backend.connection_registry[token].discard_command_cycle();
}

#[test]
fn normal_subscription_confirmations_bypass_reply_off() {
    assert_confirmations_bypass_reply_mode(
        &[b"SUBSCRIBE", b"channel"],
        &[b"UNSUBSCRIBE", b"channel"],
        b"OFF",
        b"subscribe",
        b"unsubscribe",
    );
}

#[test]
fn normal_subscription_confirmations_bypass_reply_skip() {
    assert_confirmations_bypass_reply_mode(
        &[b"SUBSCRIBE", b"channel"],
        &[b"UNSUBSCRIBE", b"channel"],
        b"SKIP",
        b"subscribe",
        b"unsubscribe",
    );
}

#[test]
fn shard_subscription_confirmations_bypass_reply_off() {
    assert_confirmations_bypass_reply_mode(
        &[b"SSUBSCRIBE", b"{slot}channel"],
        &[b"SUNSUBSCRIBE", b"{slot}channel"],
        b"OFF",
        b"ssubscribe",
        b"sunsubscribe",
    );
}

#[test]
fn shard_subscription_confirmations_bypass_reply_skip() {
    assert_confirmations_bypass_reply_mode(
        &[b"SSUBSCRIBE", b"{slot}channel"],
        &[b"SUNSUBSCRIBE", b"{slot}channel"],
        b"SKIP",
        b"ssubscribe",
        b"sunsubscribe",
    );
}

#[test]
fn client_reply_skip_suppresses_publish_without_leaking_to_ping() {
    let (mut backend, token) = backend_with_connection();
    let mut batch = ReadBatchEffects::default();

    assert_eq!(
        dispatch_command(
            &mut backend,
            token,
            &mut batch,
            &[b"CLIENT", b"REPLY", b"SKIP"],
        ),
        LoopAction::Continue
    );
    assert_eq!(
        dispatch_command(
            &mut backend,
            token,
            &mut batch,
            &[b"PUBLISH", b"channel", b"message"],
        ),
        LoopAction::Continue
    );
    assert_eq!(
        dispatch_command(&mut backend, token, &mut batch, &[b"PING"]),
        LoopAction::Continue
    );

    assert_eq!(joined_replies(&batch), b"+PONG\r\n");
    assert!(!backend.connection_registry[token].reply_skip());
    backend.connection_registry[token].discard_command_cycle();
}

#[test]
fn client_reply_off_suppresses_publish_until_reply_on() {
    let (mut backend, token) = backend_with_connection();
    let mut batch = ReadBatchEffects::default();

    for parts in [
        [b"CLIENT".as_slice(), b"REPLY", b"OFF"].as_slice(),
        [b"PUBLISH".as_slice(), b"channel", b"message"].as_slice(),
        [b"CLIENT".as_slice(), b"REPLY", b"ON"].as_slice(),
    ] {
        assert_eq!(
            dispatch_command(&mut backend, token, &mut batch, parts),
            LoopAction::Continue
        );
    }
    assert_eq!(
        dispatch_command(&mut backend, token, &mut batch, &[b"PING"]),
        LoopAction::Continue
    );

    assert_eq!(joined_replies(&batch), b"+OK\r\n+PONG\r\n");
    backend.connection_registry[token].discard_command_cycle();
}

#[test]
fn client_reply_skip_suppresses_client_id_without_leaking_to_ping() {
    let (mut backend, token) = backend_with_connection();
    let mut batch = ReadBatchEffects::default();

    dispatch_command(
        &mut backend,
        token,
        &mut batch,
        &[b"CLIENT", b"REPLY", b"SKIP"],
    );
    dispatch_command(&mut backend, token, &mut batch, &[b"CLIENT", b"ID"]);
    dispatch_command(&mut backend, token, &mut batch, &[b"PING"]);

    assert_eq!(joined_replies(&batch), b"+PONG\r\n");
    assert!(!backend.connection_registry[token].reply_skip());
    backend.connection_registry[token].discard_command_cycle();
}

#[test]
fn exec_with_psubscribe_custom_push_completes_outer_cycle() {
    let (mut backend, token) = backend_with_connection();
    let mut batch = ReadBatchEffects::default();

    for parts in [
        [b"MULTI".as_slice()].as_slice(),
        [b"PSUBSCRIBE".as_slice(), b"pattern*"].as_slice(),
        [b"EXEC".as_slice()].as_slice(),
    ] {
        assert_eq!(
            dispatch_command(&mut backend, token, &mut batch, parts),
            LoopAction::Continue
        );
    }

    let push = joined_connection_output(&backend, token);
    assert!(push.windows(10).any(|part| part == b"psubscribe"));
    assert_eq!(joined_replies(&batch), b"+OK\r\n+QUEUED\r\n");
    assert!(!backend.connection_registry[token].has_custom_reply());
    backend.connection_registry[token].discard_command_cycle();
}
