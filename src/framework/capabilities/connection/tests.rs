use super::*;
use crate::engine::command::context::{CommandConn, WatchedKey};
use crate::engine::extension::script::{PropagationOp, PropagationTargets};

fn addr() -> SocketAddr {
    ([127, 0, 0, 1], 6379).into()
}

#[test]
fn new_connection_initializes_auth_state_from_requirepass() {
    let authed = Connection::new(Token(1), addr(), false);
    let unauth = Connection::new(Token(2), addr(), true);

    assert_eq!(authed.token, Token(1));
    assert_eq!(authed.peer_addr, addr());
    assert!(authed.authenticated);
    assert!(authed.ever_authenticated);
    assert_eq!(authed.username, "default");
    assert_eq!(authed.resp_version, 2);
    assert_eq!(authed.state, ConnectionState::Connected);
    assert!(!unauth.authenticated);
    assert!(!unauth.ever_authenticated);
}

#[test]
fn output_queue_tracks_bytes() {
    let mut conn = Connection::new(Token(1), addr(), false);

    conn.queue_command_reply(b"hello".to_vec());
    conn.queue_replication_reply(b"world".to_vec());

    assert!(conn.has_pending_output());
    assert_eq!(conn.reply_bytes_curr_cmd(), 5);
    assert_eq!(conn.output_buffer_bytes(), 10);
    assert_eq!(conn.output.queue.len(), 2);
    assert_eq!(conn.output.queue[0], b"hello");
    assert_eq!(conn.output.queue[1], b"world");
}

#[test]
fn asynchronous_output_does_not_enter_command_reply_accounting() {
    let mut conn = Connection::new(Token(1), addr(), false);

    conn.queue_reply(b"monitor".to_vec());
    conn.queue_replication_reply(b"replication".to_vec());
    conn.queue_pre_client_handshake_reply(b"handshake".to_vec());
    conn.queue_slot_snapshot_reply(b"snapshot".to_vec());

    assert_eq!(conn.reply_bytes_current_command(), 0);
    assert_eq!(conn.output_buffer_bytes(), 35);
}

#[test]
fn explicit_command_reply_is_scoped_to_the_current_command() {
    let mut conn = Connection::new(Token(1), addr(), false);
    conn.parser.feed(b"*1\r\n$4\r\nPING\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.begin_admitted_command("ping");

    conn.queue_command_reply(b"+PONG\r\n".to_vec());
    assert_eq!(conn.reply_bytes_current_command(), 7);
    conn.complete_command();
    assert_eq!(conn.reply_bytes_current_command(), 0);

    conn.queue_reply(b"async".to_vec());
    conn.parser.feed(b"*1\r\n$4\r\nPING\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.begin_admitted_command("ping");
    assert_eq!(conn.reply_bytes_current_command(), 0);
}

#[test]
fn zero_capacity_empty_output_frame_is_ignored() {
    let mut conn = Connection::new_fake();

    conn.queue_reply(Vec::new());

    assert!(conn.output.queue.is_empty());
    assert!(!conn.has_pending_output());
    assert_eq!(conn.output_buffer_bytes(), 0);
    assert_eq!(conn.pending_output_memory_usage(), 0);
    assert_eq!(conn.output_buffer_allocation_bytes(), 0);
}

#[test]
fn reserved_capacity_empty_output_frame_is_ignored() {
    let mut conn = Connection::new_fake();

    conn.queue_replication_reply(Vec::with_capacity(4096));

    assert!(conn.output.queue.is_empty());
    assert!(!conn.has_pending_output());
    assert_eq!(conn.output_buffer_bytes(), 0);
    assert_eq!(conn.pending_output_memory_usage(), 0);
    assert_eq!(conn.output_buffer_allocation_bytes(), 0);
}

#[test]
fn pending_output_memory_counts_frame_capacity_and_not_empty_queue_reserve() {
    let mut conn = Connection::new_fake();
    assert_eq!(conn.pending_output_memory_usage(), 0);

    let mut frame = Vec::with_capacity(4096);
    frame.extend_from_slice(b"tiny");
    let frame_allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&frame);
    conn.queue_replication_reply(frame);

    assert_eq!(
        conn.pending_output_memory_usage(),
        frame_allocation
            + std::mem::size_of::<Vec<u8>>()
            + std::mem::size_of::<OutputFrameAccounting>()
    );
    conn.clear_output_queue();
    assert_eq!(conn.pending_output_memory_usage(), 0);
    assert!(conn.output_buffer_allocation_bytes() > 0);
}

#[test]
fn small_ordinary_replies_use_the_unaccounted_c_inline_buffer() {
    let mut conn = Connection::new_fake();

    conn.queue_reply(b"+OK\r\n".to_vec());

    assert_eq!(conn.pending_output_memory_usage(), 0);
    assert_eq!(conn.output_buffer_bytes(), 5);
}

#[test]
fn client_total_memory_includes_c_inline_reply_buffer_without_omem() {
    let conn = Connection::new(Token(1), addr(), false);
    let usage = conn.client_memory_usage();

    assert_eq!(usage.output, 0);
    assert!(
        usage.total >= std::mem::size_of::<Connection>() + CLIENT_INLINE_OUTPUT_CAPACITY,
        "total client memory must include C's fixed inline reply buffer"
    );
}

#[test]
fn pending_dispatch_replies_follow_inline_and_list_memory_projection() {
    let conn = Connection::new_fake();
    let baseline = conn.client_memory_usage();
    let small = vec![b's'; 1024];

    let inline = conn.client_memory_usage_with_pending_replies(std::slice::from_ref(&small));

    assert_eq!(inline, baseline);

    let mut large = Vec::with_capacity(64 * 1024);
    large.resize(32 * 1024, b'l');
    let frame_allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&large);
    let listed = conn.client_memory_usage_with_pending_replies(&[small, large]);
    let expected_extra = frame_allocation
        + std::mem::size_of::<Vec<u8>>()
        + std::mem::size_of::<OutputFrameAccounting>();

    assert_eq!(listed.output, baseline.output + expected_extra);
    assert_eq!(listed.total, baseline.total + expected_extra);
    assert_eq!(conn.client_memory_usage(), baseline);
}

#[test]
fn accounted_normal_memory_replaces_and_takes_cached_contribution() {
    let mut conn = Connection::new(Token(1), addr(), false);

    assert_eq!(conn.accounted_normal_memory(), 0);
    assert_eq!(conn.replace_accounted_normal_memory(4096), 0);
    assert_eq!(conn.accounted_normal_memory(), 4096);
    assert_eq!(conn.replace_accounted_normal_memory(1024), 4096);
    assert_eq!(conn.take_accounted_normal_memory(), 1024);
    assert_eq!(conn.accounted_normal_memory(), 0);
}

#[test]
fn partial_consumption_retains_the_complete_frame_allocation() {
    let mut conn = Connection::new_fake();
    let mut frame = Vec::with_capacity(4096);
    frame.extend_from_slice(b"mutation");
    conn.queue_replication_reply(frame);
    let before = conn.pending_output_memory_usage();

    conn.output.queue.front_mut().unwrap().drain(..3);
    assert_eq!(conn.consume_written_bytes(3, 8), Ok(3));
    assert_eq!(conn.pending_output_memory_usage(), before);
    assert_eq!(conn.replication_output_bytes(), 5);

    conn.output.queue.pop_front();
    assert_eq!(conn.consume_written_bytes(5, 5), Ok(5));
    assert_eq!(conn.pending_output_memory_usage(), 0);
    assert_eq!(conn.replication_output_bytes(), 0);
}

#[test]
fn slot_snapshot_output_is_excluded_from_client_reply_projections() {
    let mut conn = Connection::new_fake();
    let mut reply = Vec::with_capacity(4096);
    reply.extend_from_slice(b"control");
    conn.queue_reply(reply);
    let reply_bytes = conn.pending_reply_bytes();
    let output_memory = conn.pending_output_memory_usage();
    let allocation_bytes = conn.output_buffer_allocation_bytes();

    let mut snapshot = Vec::with_capacity(8192);
    snapshot.extend_from_slice(b"snapshot");
    conn.queue_slot_snapshot_reply(snapshot);

    assert_eq!(conn.pending_reply_bytes(), reply_bytes);
    assert_eq!(conn.pending_output_memory_usage(), output_memory);
    assert!(conn.output_buffer_allocation_bytes() > allocation_bytes);
    assert_eq!(
        conn.output_buffer_bytes(),
        b"control".len() + b"snapshot".len()
    );
}

#[test]
fn empty_slot_snapshot_frame_is_ignored() {
    let mut conn = Connection::new_fake();

    conn.queue_slot_snapshot_reply(Vec::with_capacity(4096));

    assert!(!conn.has_pending_output());
    assert_eq!(conn.pending_reply_bytes(), 0);
    assert_eq!(conn.pending_output_memory_usage(), 0);
    assert_eq!(conn.output_buffer_allocation_bytes(), 0);
}

#[test]
fn pre_client_handshake_output_is_only_physical_allocator_ownership() {
    let mut conn = Connection::new_fake();
    let mut auth = Vec::with_capacity(4096);
    auth.extend_from_slice(b"AUTH");

    conn.queue_pre_client_handshake_reply(auth);

    assert_eq!(conn.pending_reply_bytes(), 0);
    assert_eq!(conn.pending_output_memory_usage(), 0);
    assert_eq!(conn.output_buffer_bytes(), b"AUTH".len());
    assert!(conn.output_buffer_allocation_bytes() >= 4096);

    let mut empty = Connection::new_fake();
    empty.queue_pre_client_handshake_reply(Vec::with_capacity(4096));
    assert!(!empty.has_pending_output());
    assert_eq!(empty.pending_reply_bytes(), 0);
    assert_eq!(empty.pending_output_memory_usage(), 0);
    assert_eq!(empty.output_buffer_allocation_bytes(), 0);
}

#[test]
fn written_output_accounting_follows_queue_order() {
    let mut conn = Connection::new(Token(1), addr(), false);

    conn.queue_replication_reply(b"replication".to_vec());
    conn.queue_reply(b"ordinary".to_vec());

    assert_eq!(conn.output_buffer_bytes(), 19);
    assert_eq!(conn.consume_written_bytes(3, 19), Ok(3));
    assert_eq!(conn.output_buffer_bytes(), 16);
    assert_eq!(conn.consume_written_bytes(8, 16), Ok(8));
    assert_eq!(conn.output_buffer_bytes(), 8);
    assert_eq!(conn.consume_written_bytes(8, 8), Ok(0));
    assert_eq!(conn.output_buffer_bytes(), 0);
}

#[test]
fn written_output_accounting_rejects_overconsumption_without_mutation() {
    let mut conn = Connection::new(Token(1), addr(), false);
    conn.queue_replication_reply(b"tail".to_vec());
    let before_replication = conn.replication_output_bytes();
    let before_frame = conn.output.frame_accounting.front().map(|frame| {
        (
            frame.remaining_bytes,
            frame.allocation_bytes,
            frame.counts_toward_client_output,
        )
    });

    assert!(conn.consume_written_bytes(5, 4).is_err());

    assert_eq!(conn.replication_output_bytes(), before_replication);
    assert_eq!(conn.output.frame_accounting.len(), 1);
    assert_eq!(
        conn.output.frame_accounting.front().map(|frame| {
            (
                frame.remaining_bytes,
                frame.allocation_bytes,
                frame.counts_toward_client_output,
            )
        }),
        before_frame
    );
    assert_eq!(conn.output_buffer_bytes(), 4);
}

#[test]
fn mixed_output_accounting_reports_only_replication_bytes() {
    let mut conn = Connection::new(Token(1), addr(), false);
    conn.queue_reply(b"rdb".to_vec());
    conn.queue_reply(b"command".to_vec());
    conn.queue_replication_reply(b"tail".to_vec());

    assert_eq!(conn.consume_written_bytes(14, 14), Ok(4));
    assert_eq!(conn.replication_output_bytes(), 0);
    assert_eq!(conn.output_buffer_bytes(), 0);
}

#[test]
fn threaded_write_snapshot_excludes_later_frames() {
    let mut conn = Connection::new_fake();
    conn.queue_reply(b"first".to_vec());

    let snapshot = conn.take_threaded_write_snapshot();
    conn.queue_reply(b"second".to_vec());

    assert_eq!(snapshot.logical_len(), b"first".len());
    assert_eq!(snapshot.frames()[0], b"first");
    assert_eq!(conn.pending_reply_bytes(), b"firstsecond".len());

    conn.restore_threaded_write_snapshot(snapshot.into_frames());
    assert_eq!(conn.output.queue[0], b"first");
    assert_eq!(conn.output.queue[1], b"second");
}

#[test]
fn query_buffer_accounting_tracks_growth_and_shrink() {
    let mut conn = Connection::new(Token(1), addr(), false);
    assert_eq!(conn.query_buffer_total(), 0);

    conn.parser.feed(b"*1\r\n$4\r\nPING\r\n");
    conn.refresh_query_buffer_capacity();
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.set_request_bytes_for_test(conn.parser.last_cmd_len());
    conn.resize_query_buffer_after_command();
    assert!(conn.query_buffer_total() >= 16 * 1024);

    conn.last_interaction = std::time::Instant::now() - std::time::Duration::from_secs(3);
    assert_eq!(conn.query_buffer_total(), 0);

    conn.last_interaction = std::time::Instant::now();
    let big_value = vec![b'a'; 400_000];
    let mut cmd = b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$400000\r\n".to_vec();
    cmd.extend_from_slice(&big_value);
    cmd.extend_from_slice(b"\r\n");
    conn.parser.feed(&cmd);
    conn.refresh_query_buffer_capacity();
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.set_request_bytes_for_test(conn.parser.last_cmd_len());
    conn.resize_query_buffer_after_command();
    let large_qbuf = conn.query_buffer_total();
    assert!(large_qbuf > 32 * 1024);

    conn.parser.feed(b"*2\r\n$3\r\nGET\r\n$1\r\nx\r\n");
    conn.refresh_query_buffer_capacity();
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.set_request_bytes_for_test(conn.parser.last_cmd_len());
    conn.resize_query_buffer_after_command();
    let shrunk_qbuf = conn.query_buffer_total();
    assert!(shrunk_qbuf > 0);
    assert!(shrunk_qbuf < large_qbuf);
}

#[test]
fn command_lifecycle_intents_preserve_accounting_and_blocked_input() {
    let mut conn = Connection::new(Token(9), addr(), true);
    conn.parser.feed(b"*1\r\n$4\r\nPING\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.queue_reply(b"old".to_vec());
    let out_before = conn.tot_net_out;

    let command_seq = conn.record_received_command();
    conn.begin_admitted_command("ping");
    conn.set_skip_propagation(true);

    assert_eq!(command_seq, 1);
    assert_eq!(conn.req_bytes_curr_cmd(), 14);
    assert_eq!(conn.reply_bytes_curr_cmd(), 0);
    assert_eq!(conn.out_before(), out_before);
    assert_eq!(conn.tot_net_in, 14);
    assert_eq!(conn.cmd_count, 1);

    conn.blocked = Some(BlockedState {
        cmd: BlockedCmd::Paused,
        keys: Vec::new(),
        timeout_ms: 0,
        start_time_ms: 0,
        orig_args: crate::engine::command::CommandArgs::from_parts(vec![Bytes::from_static(
            b"PING",
        )]),
        req_bytes: 14,
        orig_wrapped: None,
    });
    conn.complete_command();
    assert_eq!(conn.req_bytes_curr_cmd(), 14);
    conn.blocked = None;
    conn.complete_command();
    assert_eq!(conn.req_bytes_curr_cmd(), 0);
}

#[test]
fn admitted_transaction_command_accumulates_input_once() {
    let mut conn = Connection::new(Token(9), addr(), false);
    assert!(conn.transaction.begin(0, 20));
    conn.parser.feed(b"*1\r\n$4\r\nPING\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());

    conn.record_received_command();
    conn.begin_admitted_command("ping");

    assert_eq!(conn.transaction.input_bytes_with_exec(0), Some(34));
}

#[test]
fn incomplete_query_accounting_ignores_vec_growth_slack() {
    let mut conn = Connection::new(Token(1), addr(), false);
    const PAYLOAD_LEN: usize = 300_000;
    let header = format!("*1\r\n${PAYLOAD_LEN}\r\n");

    conn.parser.feed(header.as_bytes());
    conn.parser.feed(&vec![b'x'; 200_000]);
    assert!(conn.parser.try_parse().unwrap().is_none());
    conn.refresh_query_buffer_capacity();
    conn.parser.feed(&vec![b'x'; PAYLOAD_LEN - 200_000]);
    assert!(conn.parser.try_parse().unwrap().is_none());
    conn.refresh_query_buffer_capacity();

    assert_eq!(conn.query_buffer_used(), PAYLOAD_LEN);
    assert_eq!(conn.query_buffer_total(), PAYLOAD_LEN);
}

#[test]
fn idle_incomplete_bulk_shrinks_qbuf_without_dropping_memory_commitment() {
    let mut conn = Connection::new(Token(1), addr(), false);
    const PAYLOAD_LEN: usize = 1_000_000;
    let header = format!("*3\r\n$3\r\nSET\r\n$1\r\na\r\n${PAYLOAD_LEN}\r\n");

    conn.parser.feed(header.as_bytes());
    conn.parser.feed(b"a");
    assert!(conn.parser.try_parse().unwrap().is_none());
    conn.refresh_query_buffer_capacity();
    assert_eq!(conn.query_buffer_total(), PAYLOAD_LEN);

    conn.last_interaction = std::time::Instant::now() - std::time::Duration::from_secs(3);
    conn.shrink_idle_query_buffer();

    assert!(conn.parser.has_inflight_request());
    assert_eq!(
        conn.parser.inflight_memory_usage().query_buffer,
        PAYLOAD_LEN
    );
    assert_eq!(conn.query_buffer_total(), 16 * 1024);
}

#[test]
fn incomplete_multibulk_client_memory_counts_consumed_query_and_partial_argv() {
    let mut conn = Connection::new(Token(1), addr(), false);
    let baseline = conn.client_memory_usage().total;
    let mut request = b"*250\r\n".to_vec();
    for _ in 0..249 {
        request.extend_from_slice(b"$1000\r\n");
        request.extend_from_slice(&[b'x'; 1000]);
        request.extend_from_slice(b"\r\n");
    }

    conn.parser.feed(&request);
    assert!(conn.parser.try_parse().unwrap().is_none());
    conn.refresh_query_buffer_capacity();

    let usage = conn.client_memory_usage().total.saturating_sub(baseline);
    assert!(usage >= request.len() + 249 * 1000);
    assert!(usage.saturating_mul(30) > 3 * 1024 * 1024);
}

#[test]
fn client_memory_usage_counts_argv_multi_watch_pubsub_tracking_and_output() {
    let mut conn = Connection::new_fake();
    let baseline = conn.client_memory_usage();

    conn.set_request_bytes_for_test(4096);
    assert!(conn.transaction.begin(0, 0));
    conn.transaction
        .queue(crate::framework::CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from(vec![b'x'; 8192]),
        ]));
    conn.transaction.watch_many(vec![WatchedKey::new(
        0,
        Bytes::from_static(b"watched"),
        false,
    )]);
    conn.sub_channel_count = 2;
    conn.psub_channel_count = 3;
    conn.ssub_channel_count = 4;
    conn.set_tracking_prefixes(vec![Bytes::from_static(b"prefix")]);
    let frame = vec![b'r'; 16_385];
    conn.queue_reply(frame);

    let usage = conn.client_memory_usage();

    assert!(usage.argv >= 4096);
    assert!(usage.transaction >= 8192);
    assert_eq!(usage.output, conn.pending_output_memory_usage());
    assert!(usage.total > baseline.total + 28_000);
}

#[test]
fn tracking_prefix_memory_uses_c_rax_node_projection() {
    let mut conn = Connection::new_fake();
    let before = conn.client_memory_usage().total;
    let prefixes = (0..10_240)
        .map(|i| Bytes::from(format!("PREFIX-{i}-{i}-{i}-{i}-{i}-{i}-{i}").into_bytes()))
        .collect();

    CommandConn::set_tracking_prefixes(&mut conn, prefixes);

    let delta = conn.client_memory_usage().total.saturating_sub(before);
    let c_test_limit = if std::mem::size_of::<usize>() == 8 {
        300_000
    } else {
        200_000
    };
    assert!(
        delta < c_test_limit,
        "tracking prefix projection {delta} must match C rax node accounting"
    );
}

#[test]
fn remembers_a_bounded_query_snapshot_for_diagnostics() {
    let mut conn = Connection::new(Token(1), addr(), false);
    conn.remember_query_bytes(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$6\r\nblabla\r\n");
    assert!(conn.last_query_snapshot.windows(6).any(|w| w == b"blabla"));
    let huge = vec![b'x'; 1024 * 1024 + 1];
    conn.remember_query_bytes(&huge);
    assert_eq!(conn.last_query_snapshot.len(), 64);
    conn.remember_query_bytes(b"*2\r\n$5\r\nDEBUG\r\n$8\r\nSEGFAULT\r\n");
    assert!(conn.last_query_snapshot.iter().all(|byte| *byte == b'x'));
}

#[test]
fn diagnostic_snapshot_keeps_invalid_query_tail_before_debug_command() {
    let mut conn = Connection::new(Token(1), addr(), false);
    conn.remember_query_bytes(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$bla");
    conn.remember_query_bytes(b"bla\r\n*2\r\n$5\r\nDEBUG\r\n$8\r\nSEGFAULT\r\n");

    assert!(conn
        .last_query_snapshot
        .windows(b"$blabla".len())
        .any(|window| window == b"$blabla"));
    assert!(!conn
        .last_query_snapshot
        .windows(b"DEBUG".len())
        .any(|window| window.eq_ignore_ascii_case(b"DEBUG")));
}

#[test]
fn fake_connection_sets_special_flags() {
    let fake = Connection::new_fake();
    assert_eq!(fake.token, Token(usize::MAX));
    assert!(fake.authenticated);
    assert!(fake.skip_propagate());
    assert!(fake.custom_reply());
}

#[test]
fn enter_blocking_records_state_but_transactions_return_null_array() {
    let mut conn = Connection::new(Token(1), addr(), false);
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"BLPOP")]);

    let value = conn.enter_blocking(
        BlockedCmd::Blpop,
        vec![Bytes::from_static(b"k")],
        1000,
        &args,
        32,
    );

    assert_eq!(value, Value::Null);
    let blocked = conn.blocked.as_ref().unwrap();
    assert_eq!(blocked.keys, vec![Bytes::from_static(b"k")]);
    assert_eq!(blocked.timeout_ms, 1000);
    assert_eq!(blocked.orig_args.name, Bytes::from_static(b"BLPOP"));
    assert_eq!(blocked.req_bytes, 32);
    assert!(conn.skip_propagate());

    conn.set_skip_propagation(false);
    let trait_conn: &mut dyn CommandConn = &mut conn;
    assert_eq!(
        trait_conn.enter_blocking(
            BlockedCmd::Brpop,
            vec![Bytes::from_static(b"k2")],
            1000,
            &args,
            32
        ),
        Value::Null
    );
    assert!(conn.skip_propagate());

    assert!(conn.begin_transaction());
    assert_eq!(
        conn.enter_blocking(BlockedCmd::Paused, Vec::new(), 0, &args, 0),
        Value::NullArray
    );
}

#[test]
fn command_conn_trait_round_trips_session_fields() {
    let mut conn = Connection::new(Token(1), addr(), false);
    let now = Instant::now();

    conn.set_resp_version(3);
    conn.set_authenticated(false);
    conn.set_ever_authenticated(false);
    conn.set_username("alice".to_string());
    conn.set_selected_db(5);
    conn.set_last_interaction(now);
    conn.set_name(Some(Bytes::from_static(b"client")));
    conn.set_lib_name("lib".to_string());
    conn.set_lib_ver("1.0".to_string());
    conn.set_capa_flags(vec!["resp3".to_string()]);
    conn.set_request_bytes_for_test(11);
    conn.set_reply_bytes_for_test(12);
    conn.set_no_touch(true);
    conn.set_import_source(true);
    conn.set_readonly(true);
    conn.set_cmd_count(7);
    conn.set_reply_off(true);
    conn.set_reply_skip(true);
    conn.set_reply_skip_next(true);
    conn.suppress_reply();
    conn.set_tracking(true);
    conn.set_tracking_optout(true);
    conn.set_monitoring(true);
    conn.set_sub_channel_count(2);
    conn.set_psub_channel_count(3);
    conn.set_ssub_channel_count(4);
    assert!(conn.begin_transaction());
    conn.set_custom_reply(true);
    conn.set_skip_propagate(true);
    conn.mark_transaction_queue_error();
    conn.watch_keys(vec![WatchedKey::new(
        5,
        Bytes::from_static(b"watched"),
        false,
    )]);
    conn.transaction.touch_watched_key(5, b"watched", true);
    conn.set_swapdb_dbs(Some((1, 2)));

    assert_eq!(conn.token(), Token(1));
    assert_eq!(conn.peer_addr(), addr());
    assert_eq!(conn.resp_version(), 3);
    assert!(!conn.authenticated());
    assert!(!conn.ever_authenticated());
    assert_eq!(conn.username(), "alice");
    assert_eq!(conn.selected_db(), 5);
    assert_eq!(conn.last_interaction(), now);
    assert!(conn.create_time() <= now);
    assert_eq!(conn.name(), Some(&Bytes::from_static(b"client")));
    assert_eq!(conn.lib_name(), "lib");
    assert_eq!(conn.lib_ver(), "1.0");
    assert_eq!(conn.capa_flags(), &["resp3".to_string()]);
    assert_eq!(conn.req_bytes_curr_cmd(), 11);
    assert_eq!(conn.reply_bytes_curr_cmd(), 12);
    assert!(conn.no_touch());
    assert!(conn.import_source());
    assert!(conn.readonly());
    assert_eq!(conn.tot_net_in(), 0);
    assert_eq!(conn.tot_net_out(), 0);
    assert_eq!(conn.cmd_count(), 7);
    assert!(conn.reply_off());
    assert!(conn.reply_skip());
    assert!(conn.reply_skip_next());
    assert!(conn.reply_suppressed());
    assert!(conn.tracking());
    assert!(conn.tracking_optout());
    assert!(conn.monitoring());
    assert_eq!(conn.sub_channel_count(), 2);
    assert_eq!(conn.psub_channel_count(), 3);
    assert_eq!(conn.ssub_channel_count(), 4);
    assert!(conn.in_transaction());
    assert!(conn.custom_reply());
    assert!(conn.skip_propagate());
    assert!(conn.transaction.has_queue_error());
    assert!(conn.transaction.is_watch_dirty());
    assert_eq!(conn.swapdb_dbs(), Some((1, 2)));
}

#[test]
fn cluster_config_save_request_is_consumed_once() {
    let mut conn = Connection::new(Token(1), addr(), false);

    conn.request_cluster_config_save();
    assert_eq!(
        conn.take_command_effects(CommandEffectDrain::Completed)
            .and_then(|mut effects| effects.take_cluster_config_save()),
        Some(Value::ok())
    );
    assert!(conn
        .take_command_effects(CommandEffectDrain::Completed)
        .is_none());
}

#[test]
fn command_conn_trait_records_cycle_intents_and_blocked_state() {
    let mut conn = Connection::new(Token(1), addr(), false);
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"PING")]);
    let operation = PropagationOp::new(10, PropagationTargets::Both, b"frame".to_vec());

    conn.watch_keys(vec![WatchedKey::new(0, Bytes::from_static(b"k"), false)]);
    conn.record_custom_propagation(b"*1\r\n$4\r\nPING\r\n".to_vec());
    conn.record_script_propagation(operation.clone());
    conn.record_dirty_key((1, Bytes::from_static(b"d")));
    conn.record_notification((Bytes::from_static(b"ch"), Bytes::from_static(b"msg")));
    conn.set_blocked(Some(BlockedState {
        cmd: BlockedCmd::Script,
        keys: vec![Bytes::from_static(b"k")],
        timeout_ms: 0,
        start_time_ms: 1,
        orig_args: args.clone(),
        req_bytes: 4,
        orig_wrapped: None,
    }));

    assert_eq!(
        conn.watched_keys(),
        &[WatchedKey::new(0, Bytes::from_static(b"k"), false)]
    );
    assert_eq!(conn.custom_propagation().len(), 1);
    assert_eq!(conn.script_propagation(), &vec![operation]);
    assert_eq!(conn.dirty_keys(), &vec![(1, Bytes::from_static(b"d"))]);
    assert_eq!(conn.pending_notifications().len(), 1);
    assert!(matches!(conn.blocked().unwrap().cmd, BlockedCmd::Script));

    conn.reset_parser();
    conn.mark_closing();
    assert_eq!(conn.state, ConnectionState::Closing);
    CommandConn::queue_reply(&mut conn, b"ok".to_vec());
    assert_eq!(conn.output.queue[0], b"ok");
    assert_eq!(
        CommandConn::enter_blocking(&mut conn, BlockedCmd::Paused, Vec::new(), 0, &args, 0),
        Value::Null
    );
}
#[test]
fn client_id_is_distinct_from_transport_token_and_stable() {
    let conn = Connection::new_with_client_id(Token(9), 42, addr(), false);
    assert_eq!(conn.token(), Token(9));
    assert_eq!(conn.client_id(), 42);
    assert_eq!(conn.command_seq, 0);
}

#[test]
fn completed_command_effects_are_drained_exactly_once() {
    let mut conn = Connection::new(Token(9), addr(), false);
    conn.parser.feed(b"*1\r\n$4\r\nPING\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.begin_admitted_command("ping");
    conn.set_skip_propagation(true);
    conn.record_custom_propagation(b"custom".to_vec());
    conn.record_script_propagation(PropagationOp::both(2, b"script".to_vec()));
    conn.record_dirty_key((2, Bytes::from_static(b"key")));
    conn.record_notification((
        Bytes::from_static(b"channel"),
        Bytes::from_static(b"message"),
    ));
    conn.set_swapdb_dbs(Some((1, 2)));
    assert!(conn.redact_command_argument(1));

    let effects = conn
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("a completed admitted command owns one effect drain");

    assert_eq!(effects.request_bytes(), 14);
    assert!(effects.skip_propagation());
    assert_eq!(effects.custom_propagation(), &[b"custom".to_vec()]);
    assert_eq!(effects.script_propagation().len(), 1);
    assert_eq!(effects.dirty_keys(), &[(2, Bytes::from_static(b"key"))]);
    assert_eq!(
        effects.notifications(),
        &[(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"message")
        )]
    );
    assert_eq!(effects.swapdb(), Some((1, 2)));
    assert!(effects.is_argument_redacted(1));
    assert!(conn
        .take_command_effects(CommandEffectDrain::Completed)
        .is_none());
}

#[test]
fn postponed_attempt_retires_effects_but_retains_accounting_and_redaction() {
    let mut conn = Connection::new(Token(9), addr(), false);
    conn.record_direct_output_bytes(17);
    conn.parser
        .feed(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.begin_admitted_command("set");
    let request_bytes = conn.request_bytes_current_command();
    conn.set_reply_bytes_for_test(9);
    conn.set_skip_propagation(true);
    conn.record_custom_propagation(b"custom".to_vec());
    conn.record_script_propagation(PropagationOp::both(0, b"script".to_vec()));
    conn.record_dirty_key((0, Bytes::from_static(b"key")));
    conn.record_notification((
        Bytes::from_static(b"channel"),
        Bytes::from_static(b"message"),
    ));
    conn.record_shard_notification((Bytes::from_static(b"shard"), Bytes::from_static(b"message")));
    conn.record_cluster_notification((Bytes::from_static(b"cluster"), Bytes::new()));
    conn.record_cluster_shard_notification((Bytes::from_static(b"cluster-shard"), Bytes::new()));
    conn.defer_command_lazyfree_object(crate::foundation::io::store::Robj::string("lazy"));
    conn.set_swapdb_dbs(Some((0, 1)));
    conn.request_cluster_config_save_with_reply(Value::ok());
    conn.set_custom_reply(true);
    conn.suppress_reply();
    assert!(conn.redact_command_argument(2));

    conn.retire_command_attempt_for_postpone();
    conn.resume_postponed_command_effects();
    let mut effects = conn
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("postponed command opens one clean effect segment");

    assert_eq!(effects.request_bytes(), request_bytes);
    assert_eq!(effects.reply_bytes(), 9);
    assert_eq!(effects.output_before(), 17);
    assert!(effects.is_argument_redacted(2));
    assert!(!effects.skip_propagation());
    assert!(effects.take_custom_propagation().is_empty());
    assert!(effects.take_script_propagation().is_empty());
    assert!(effects.take_dirty_keys().is_empty());
    assert!(effects.take_notifications().is_empty());
    assert!(effects.take_shard_notifications().is_empty());
    assert!(effects.take_cluster_notifications().is_empty());
    assert!(effects.take_cluster_shard_notifications().is_empty());
    assert!(effects.take_lazyfree_objects().is_empty());
    assert_eq!(effects.take_swapdb(), None);
    assert_eq!(effects.take_cluster_config_save(), None);
    assert!(!effects.has_custom_reply());
    assert!(!effects.should_suppress_reply());
}

#[test]
fn blocked_command_retains_effects_until_terminal_completion() {
    let mut conn = Connection::new(Token(9), addr(), false);
    conn.parser.feed(b"*2\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.begin_admitted_command("blpop");
    conn.record_dirty_key((0, Bytes::from_static(b"key")));
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"BLPOP")]);
    assert_eq!(
        conn.enter_blocking(
            BlockedCmd::Blpop,
            vec![Bytes::from_static(b"k")],
            0,
            &args,
            24
        ),
        Value::Null
    );

    assert!(conn
        .take_command_effects(CommandEffectDrain::Completed)
        .is_none());
    let retained_request_bytes = conn.req_bytes_curr_cmd();
    assert!(retained_request_bytes > 0);
    assert_eq!(conn.dirty_keys(), &[(0, Bytes::from_static(b"key"))]);

    conn.blocked = None;
    let effects = conn
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("unblocked command completes the retained cycle once");
    assert_eq!(effects.request_bytes(), retained_request_bytes);
    assert_eq!(effects.dirty_keys(), &[(0, Bytes::from_static(b"key"))]);
    assert!(conn
        .take_command_effects(CommandEffectDrain::Completed)
        .is_none());
}

#[test]
fn effect_drain_does_not_finish_outer_accounting() {
    let mut conn = Connection::new(Token(9), addr(), false);
    conn.parser.feed(b"*1\r\n$4\r\nEXEC\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.begin_admitted_command("exec");
    conn.record_custom_propagation(b"nested".to_vec());

    let effects = conn
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("nested handler effects are owned after re-entry");
    assert_eq!(effects.request_bytes(), 14);
    assert_eq!(conn.req_bytes_curr_cmd(), 14);

    conn.complete_command();
    assert_eq!(conn.req_bytes_curr_cmd(), 0);
}

#[test]
fn nested_effect_drain_preserves_outer_reply_policy_until_final_completion() {
    let mut conn = Connection::new(Token(9), addr(), false);
    conn.parser.feed(b"*1\r\n$4\r\nEXEC\r\n");
    assert!(conn.parser.try_parse().unwrap().is_some());
    conn.begin_admitted_command("exec");
    conn.set_custom_reply(true);
    conn.suppress_reply();
    conn.record_custom_propagation(b"nested".to_vec());

    let nested = conn
        .take_command_effects(CommandEffectDrain::Nested)
        .expect("nested command effects");
    let mut nested = nested;
    assert!(nested.has_custom_reply());
    assert!(nested.should_suppress_reply());
    conn.finish_nested_reply_state(&mut nested);
    assert!(!nested.has_custom_reply());
    assert!(!nested.should_suppress_reply());
    assert!(conn.custom_reply());
    assert!(conn.reply_suppressed());

    conn.record_dirty_key((0, Bytes::from_static(b"key")));
    let completed = conn
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("outer command effects");
    assert!(completed.has_custom_reply());
    assert!(completed.should_suppress_reply());
    assert_eq!(completed.dirty_keys(), &[(0, Bytes::from_static(b"key"))]);
    assert!(conn
        .take_command_effects(CommandEffectDrain::Completed)
        .is_none());
}

#[test]
fn nested_reply_policy_settles_before_outer_state_is_preserved() {
    let mut conn = Connection::new(Token(9), addr(), false);
    conn.begin_nested_command_effects();
    conn.set_custom_reply(true);
    let mut first = conn
        .take_command_effects(CommandEffectDrain::Nested)
        .expect("first nested command effects");
    conn.finish_nested_reply_state(&mut first);

    conn.begin_nested_command_effects();
    conn.set_custom_reply(true);
    conn.request_cluster_config_save_with_reply(Value::ok());

    let mut nested = conn
        .take_command_effects(CommandEffectDrain::Nested)
        .expect("nested command effects");
    nested.allow_default_reply();
    conn.finish_nested_reply_state(&mut nested);

    assert!(conn.custom_reply());
    conn.begin_nested_command_effects();
    let completed = conn
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("outer command effects");
    assert!(completed.has_custom_reply());
}
