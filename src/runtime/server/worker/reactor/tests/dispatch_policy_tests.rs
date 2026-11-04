use crate::foundation::io::transport::OwnedPollSource;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
use crate::runtime::server::worker::reactor::blocked_events::{
    BlockedOperation, DeferredReadyEvent,
};
use crate::runtime::server::worker::reactor::sources::{InternalPipeOwner, SourceOwner};
use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
use crate::runtime::server::worker::reactor::{
    decide_dispatch, BlockedRoundPhase, DispatchContext, DispatchDecision, DispatchOutcome,
    EventDependencies, EventPayloadKind, ProtectedResources, ReadinessEvent, ReadinessOrigin,
    ReadyEvent,
};
use crate::runtime::server::worker::ServerWorkerState;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn connected_client(runtime: &mut ServerWorkerState) -> (TcpStream, Token) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind client peer");
    let peer_addr = listener.local_addr().expect("read client peer address");
    let peer = runtime
        .transport
        .connect_tcp(peer_addr)
        .expect("connect client transport");
    let (client, _) = listener.accept().expect("accept client transport");
    runtime.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr,
    });
    (client, peer.token)
}

fn command_context(token: Token) -> DispatchContext {
    DispatchContext::CommandBlocked {
        protected_client: token,
    }
}

#[test]
fn command_blocked_policy_defers_only_the_protected_read() {
    let protected = Token(7);
    let context = DispatchContext::CommandBlocked {
        protected_client: protected,
    };

    assert_eq!(
        decide_dispatch(
            &context,
            &EventDependencies::client_read(protected),
            EventPayloadKind::Readiness,
        ),
        DispatchDecision::Defer,
    );
    assert_eq!(
        decide_dispatch(
            &context,
            &EventDependencies::client_write(protected),
            EventPayloadKind::Readiness,
        ),
        DispatchDecision::Dispatch,
    );
    assert_eq!(
        decide_dispatch(
            &context,
            &EventDependencies::safe_producer(),
            EventPayloadKind::Readiness,
        ),
        DispatchDecision::Dispatch,
    );
    assert_eq!(
        decide_dispatch(
            &context,
            &EventDependencies::shutdown_control(),
            EventPayloadKind::Readiness,
        ),
        DispatchDecision::Dispatch,
    );
    assert_eq!(
        decide_dispatch(
            &context,
            &EventDependencies::none(),
            EventPayloadKind::Timer,
        ),
        DispatchDecision::Suppress,
    );
}

#[test]
fn hosted_blocked_policy_defers_its_precise_rdb_read() {
    let rdb = Token(17);
    let context = DispatchContext::HostedBlocked {
        protected_resources: ProtectedResources::for_hosted_operation(BlockedOperation::DualRdb {
            main: crate::framework::distributed::replication::ConnectionId(16),
            rdb: crate::framework::distributed::replication::ConnectionId(rdb.0),
        }),
    };

    assert_eq!(
        decide_dispatch(
            &context,
            &EventDependencies::client_read(rdb),
            EventPayloadKind::Readiness,
        ),
        DispatchDecision::Defer,
    );
}

#[test]
fn protected_command_readable_writable_defers_read_and_services_write_once() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let (mut client, token) = connected_client(&mut runtime);
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set client timeout");
    runtime.writes.ordinary.begin_turn();
    assert!(runtime.queue_client_reply(token, b"reply".to_vec()));
    let before = (
        runtime.ctx.stats().eventloop_events_deferred,
        runtime.ctx.stats().eventloop_events_stale,
    );

    let outcome = runtime.dispatch_readiness_now(
        command_context(token),
        ReadinessEvent {
            token,
            origin: ReadinessOrigin::Polled,
            readable: true,
            writable: true,
            read_closed: false,
            error: false,
        },
    );

    assert_eq!(outcome, DispatchOutcome::Deferred);
    let mut reply = [0_u8; 5];
    client
        .read_exact(&mut reply)
        .expect("protected writable reply");
    assert_eq!(&reply, b"reply");
    let deferred = runtime
        .blocked_events
        .pop_front_deferred()
        .expect("protected read deferred");
    assert!(deferred.readable);
    assert!(!deferred.writable);
    assert_eq!(runtime.ctx.stats().eventloop_events_deferred, before.0 + 1);
    assert_eq!(runtime.ctx.stats().eventloop_events_stale, before.1);
}

#[test]
fn protected_command_error_dispatches_terminal_handling() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let (_client, token) = connected_client(&mut runtime);

    let outcome = runtime.dispatch_readiness_now(
        command_context(token),
        ReadinessEvent {
            token,
            origin: ReadinessOrigin::Polled,
            readable: false,
            writable: false,
            read_closed: false,
            error: true,
        },
    );

    assert_eq!(outcome, DispatchOutcome::OwnerClosed);
    assert!(!runtime.transport.is_registered(token));
}

#[test]
fn protected_command_hup_only_dispatches_terminal_handling() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let (_client, token) = connected_client(&mut runtime);

    let outcome = runtime.dispatch_readiness_now(
        command_context(token),
        ReadinessEvent {
            token,
            origin: ReadinessOrigin::Polled,
            readable: false,
            writable: false,
            read_closed: true,
            error: false,
        },
    );

    assert_eq!(outcome, DispatchOutcome::OwnerClosed);
    assert!(!runtime.transport.is_registered(token));
}

#[test]
fn protected_command_readable_hup_defers_the_terminal_bit_with_the_read() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let (_client, token) = connected_client(&mut runtime);

    let outcome = runtime.dispatch_readiness_now(
        command_context(token),
        ReadinessEvent {
            token,
            origin: ReadinessOrigin::Polled,
            readable: true,
            writable: false,
            read_closed: true,
            error: false,
        },
    );

    assert_eq!(outcome, DispatchOutcome::Deferred);
    let deferred = runtime
        .blocked_events
        .pop_front_deferred()
        .expect("protected read deferred");
    assert!(deferred.readable);
    assert!(deferred.read_hup);
    assert!(runtime.transport.is_registered(token));
}

#[test]
fn normal_pending_data_uses_the_unified_dispatch_metric_path() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut runtime.registry);
    let (mut client, token) = connected_client(&mut runtime);
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set client timeout");
    client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write PING");
    let ready = runtime.transport.wait_readable_for_test(token);
    assert!(ready
        .iter()
        .any(|event| event.token == token && event.readable));
    runtime.transport.queue_test_pending_data_event(token);
    runtime.writes.ordinary.begin_turn();
    let collected_before = runtime.ctx.stats().eventloop_events_collected;
    let handled_before = runtime.ctx.stats().eventloop_events_handled;

    assert!(!runtime.drain_pending_input());
    runtime.drain_current_ordinary_writes();

    let mut reply = [0_u8; 7];
    client.read_exact(&mut reply).expect("read PING reply");
    assert_eq!(&reply, b"+PONG\r\n");
    assert_eq!(
        runtime.ctx.stats().eventloop_events_handled,
        handled_before + 2
    );
    assert_eq!(
        runtime.ctx.stats().eventloop_events_collected,
        collected_before
    );
}

#[test]
fn normal_deferred_replay_uses_the_unified_dispatch_metric_path() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    crate::framework::commands::register_all(&mut runtime.registry);
    let (mut client, token) = connected_client(&mut runtime);
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set client timeout");
    client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write PING");
    let ready = runtime.transport.wait_readable_for_test(token);
    assert!(ready
        .iter()
        .any(|event| event.token == token && event.readable));
    runtime
        .blocked_events
        .defer(DeferredReadyEvent::new(token, true, false, false, false));
    runtime.writes.ordinary.begin_turn();
    let collected_before = runtime.ctx.stats().eventloop_events_collected;
    let handled_before = runtime.ctx.stats().eventloop_events_handled;

    runtime.replay_one_deferred_readiness();
    runtime.drain_current_ordinary_writes();

    let mut reply = [0_u8; 7];
    client
        .read_exact(&mut reply)
        .expect("read replayed PING reply");
    assert_eq!(&reply, b"+PONG\r\n");
    assert_eq!(
        runtime.ctx.stats().eventloop_events_handled,
        handled_before + 2
    );
    assert_eq!(
        runtime.ctx.stats().eventloop_events_collected,
        collected_before
    );
}

#[test]
fn command_blocked_empty_poll_keeps_the_fixed_round_contract() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    runtime.transport.queue_test_transport_event_batch(&[]);
    runtime.clear_blocked_round_trace_for_test();

    runtime.process_events_while_command_blocked(Token(usize::MAX));

    assert_eq!(
        runtime.blocked_round_trace_for_test(),
        [
            BlockedRoundPhase::InitialCompletions,
            BlockedRoundPhase::PendingData,
            BlockedRoundPhase::AofBarrier,
            BlockedRoundPhase::EligibleWrites,
            BlockedRoundPhase::Poll,
            BlockedRoundPhase::RestrictedAfterSleep,
            BlockedRoundPhase::FinalCompletions,
        ],
        "an empty poll must still perform the final completion drain without blocked cron work",
    );
}

#[test]
fn hosted_blocked_nonempty_polls_follow_four_fixed_rounds() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let (_client, token) = connected_client(&mut runtime);
    for _ in 0..4 {
        runtime
            .transport
            .queue_test_transport_event_batch(&[(token, true, false, false, false)]);
    }
    runtime.clear_blocked_round_trace_for_test();

    let result = runtime.process_events_while_blocked(BlockedOperation::ClassicRdb {
        main: crate::framework::distributed::replication::ConnectionId(usize::MAX),
    });

    assert_eq!(result.poll_rounds, 4);
    let one_round = [
        BlockedRoundPhase::InitialCompletions,
        BlockedRoundPhase::PendingData,
        BlockedRoundPhase::AofBarrier,
        BlockedRoundPhase::EligibleWrites,
        BlockedRoundPhase::Poll,
        BlockedRoundPhase::RestrictedAfterSleep,
        BlockedRoundPhase::PolledReadiness,
        BlockedRoundPhase::FinalCompletions,
    ];
    assert_eq!(runtime.blocked_round_trace_for_test(), one_round.repeat(4));
}

#[test]
fn diskless_rdb_read_hup_only_dispatches_eof_instead_of_source_failure() {
    let mut runtime = ServerWorkerState::new(1).expect("create runtime");
    let pipe =
        crate::foundation::io::sys::fd::pipe(crate::foundation::io::sys::fd::PipeFlags::NONE)
            .expect("create diskless pipe");
    let (read_end, write_end) = pipe.into_parts();
    drop(write_end);
    let transfer_id = crate::runtime::server::worker::reactor::sources::TransferId(9);
    let source = runtime
        .install_runtime_source(
            OwnedPollSource::readable(read_end),
            SourceOwner::InternalPipe(InternalPipeOwner::DisklessRdb { transfer_id }),
        )
        .expect("install diskless source");
    runtime.full_sync.diskless_relay = Some(DisklessRelay {
        transfer_id,
        source: Some(source),
        child_release_write_end: None,
        targets: Vec::new(),
        marker: Vec::new(),
        complete_full_resync: true,
        not_before_ms: 0,
        eof_reached: false,
        marker_queued_targets: std::collections::HashSet::new(),
        marker_sent_targets: std::collections::HashSet::new(),
        completion_announced: false,
        completion_state:
            crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
    });

    runtime
        .full_sync
        .diskless_relay
        .as_mut()
        .unwrap()
        .completion_state
        .apply_fact(
            crate::runtime::server::worker::distributed::replication::diskless_child::DisklessFact::ChildExited(
                crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0),
            ),
        );
    runtime.dispatch_transport_event(
        ReadyEvent {
            token: source.token,
            readable: false,
            writable: false,
            error: false,
            read_hup: true,
        },
        false,
    );

    assert!(
        runtime.full_sync.diskless_relay.is_none(),
        "read-hup-only readiness must still drive diskless EOF"
    );
}
