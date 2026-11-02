use crate::foundation::io::transport::{
    ConnectedPeer, OwnedPollSource, PollSourceMode, TransportId,
};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::sentinel::io::SentinelIo;
use crate::runtime::server::worker::reactor::sources::{
    BlockedPolicy, ChildExitOutcome, ChildHandle, ChildOwner, ChildRegistry, ChildSignalOwner,
    RuntimeSourceRegistry, SourceOwner, TransferId,
};
use crate::runtime::server::worker::reactor::transport::{
    FakeTransportRuntime, RuntimeSentinelIo, RuntimeTransport,
};
use crate::runtime::server::worker::reactor::{
    DispatchContext, DispatchOutcome, EventPayload, ReadyEvent,
};
use crate::runtime::server::worker::PendingRemovalOwner;
use crate::runtime::server::worker::ServerWorkerState;
use std::fs::File;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
use std::time::Duration;
use std::{collections::HashMap, net::TcpListener};

fn owned_pipe_source(mode: PollSourceMode) -> (OwnedPollSource, std::io::PipeWriter) {
    let (reader, writer) = std::io::pipe().expect("create pipe");
    let owned_reader: OwnedFd = reader.into();
    (
        OwnedPollSource {
            file: File::from(owned_reader),
            mode,
        },
        writer,
    )
}

#[test]
fn registry_installs_and_retires_every_existing_pollable_owner() {
    let cases = [
        (SourceOwner::Listener, BlockedPolicy::Always),
        (SourceOwner::Connection, BlockedPolicy::DependencyChecked),
        (SourceOwner::ClusterBus, BlockedPolicy::DependencyChecked),
        (SourceOwner::Sentinel, BlockedPolicy::DependencyChecked),
        (SourceOwner::IoWorkerWake, BlockedPolicy::Always),
        (
            SourceOwner::ChildSignal(ChildSignalOwner::SelfPipe),
            BlockedPolicy::Always,
        ),
        (
            SourceOwner::ChildSignal(ChildSignalOwner::PidFd(ChildHandle {
                id: 99,
                generation: 1,
            })),
            BlockedPolicy::Always,
        ),
    ];
    let mut registry = RuntimeSourceRegistry::default();

    for (index, (owner, blocked_policy)) in cases.into_iter().enumerate() {
        let token = Token(index + 1);
        let handle = registry
            .install(token, owner.clone())
            .expect("unique source route");
        let route = registry.resolve(handle).expect("installed source route");
        assert_eq!(&route.owner, &owner);
        assert_eq!(route.blocked_policy, blocked_policy);
        assert_eq!(registry.resolve_token(token), Some(route));
        registry.retire(handle).expect("active source route");
        assert!(registry.resolve(handle).is_none());
    }
}

#[test]
fn stable_future_source_handles_remain_copyable() {
    let transfer = TransferId(7);
    let copied_transfer = transfer;
    let mut registry = ChildRegistry::default();
    let child = registry.register_test_child(ChildOwner::RdbSave, 1234);
    let copied_child = child;

    assert_eq!(transfer, copied_transfer);
    assert_eq!(child, copied_child);
}

#[test]
fn completed_child_is_collected_once_and_finalized_only_in_normal_context() {
    let mut registry = ChildRegistry::default();
    let child = registry.register_test_child(ChildOwner::RdbSave, 1234);

    registry.record_exit_for_test(child, ChildExitOutcome::Exited(0));

    let completions = registry.drain_completions();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].child, child);
    assert_eq!(completions[0].outcome, ChildExitOutcome::Exited(0));
    assert!(registry.drain_completions().is_empty());

    registry.defer_completion(child);
    assert!(registry.contains(child));
    registry.finalize_completion(child).unwrap();
    assert!(!registry.contains(child));
}

#[test]
fn child_registry_ignores_pid_reuse_from_stale_generation() {
    let mut registry = ChildRegistry::default();
    let old_child = registry.register_test_child(ChildOwner::RdbSave, 4321);
    registry.record_exit_for_test(old_child, ChildExitOutcome::Exited(0));
    assert_eq!(registry.drain_completions().len(), 1);
    registry.finalize_completion(old_child).unwrap();
    let new_child = registry.register_test_child(ChildOwner::AofRewrite, 4321);

    registry.record_exit_for_test(old_child, ChildExitOutcome::Signaled(9));

    assert!(registry.drain_completions().is_empty());
    assert!(registry.contains(new_child));
    assert_eq!(registry.child_for_pid(4321), Some(new_child));
}

#[test]
fn child_registry_prunes_synchronously_reaped_child_after_owner_abort() {
    let mut registry = ChildRegistry::default();
    let child = registry.register_test_child(ChildOwner::RdbSave, 4321);
    assert!(registry.contains(child));

    registry.retain_only_pid(None);

    assert!(!registry.contains(child));
    assert!(registry.child_for_pid(4321).is_none());
}

#[test]
fn child_exit_before_pidfd_registration_is_recorded() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let child = runtime.install_test_child(ChildOwner::DisklessReplication, 9876);

    runtime
        .child_registry
        .record_exit_for_pid(9876, ChildExitOutcome::Exited(0));

    let completions = runtime.child_registry.drain_completions();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].child, child);
}

#[test]
fn pidfd_open_failure_keeps_sigchld_fallback_registration() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let missing_pid = i32::MAX;

    let child = runtime.register_child_process(ChildOwner::AofRewrite, missing_pid);
    assert_eq!(
        runtime.child_registry.child_for_pid(missing_pid),
        Some(child)
    );

    runtime
        .child_registry
        .record_exit_for_pid(missing_pid, ChildExitOutcome::Exited(0));

    let completions = runtime.child_registry.drain_completions();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].child, child);
    assert_eq!(completions[0].outcome, ChildExitOutcome::Exited(0));
}

#[test]
fn repeated_child_registration_does_not_install_duplicate_pidfd_sources() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let mut child = crate::framework::capabilities::process::fork_owned_child(|| {
        std::thread::sleep(Duration::from_millis(50));
        0
    })
    .expect("fork child");
    let pid = child.pid();
    let before = runtime.runtime_sources.len();

    runtime.register_child_process(ChildOwner::RdbSave, pid);
    let after_first = runtime.runtime_sources.len();
    runtime.register_child_process(ChildOwner::RdbSave, pid);
    let after_second = runtime.runtime_sources.len();

    assert_eq!(after_first, after_second);
    assert!(
        after_first == before || after_first == before + 1,
        "pidfd support may add one source, but never more"
    );

    let _ = child.wait();
}

#[test]
fn child_completion_event_finalizes_only_in_normal_context() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let child = runtime.install_test_child(ChildOwner::RdbSave, 2468);
    runtime
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));

    let collected_before = runtime.ctx.stats().eventloop_events_collected;
    runtime.collect_child_completion_events();
    assert_eq!(
        runtime.ctx.stats().eventloop_events_collected,
        collected_before + 1
    );
    let events = runtime.event_batch.drain_ready_for_test();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0].payload,
        EventPayload::ChildCompletion(_)
    ));

    let EventPayload::ChildCompletion(completion) = &events[0].payload else {
        panic!("expected child completion event");
    };
    runtime.dispatch_child_completion_for_test(
        completion.clone(),
        DispatchContext::CommandBlocked {
            protected_client: Token(1),
        },
    );
    assert!(runtime.child_registry.contains(child));

    runtime.dispatch_child_completion_for_test(completion.clone(), DispatchContext::Normal);
    assert!(
        runtime.child_registry.contains(child),
        "normal generic dispatch leaves owner finalization to compatibility reapers"
    );
    runtime.child_registry.finalize_completion(child).unwrap();
    assert!(!runtime.child_registry.contains(child));
}

#[test]
fn deferred_script_debugger_completion_replays_in_normal_context() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let child = runtime.install_test_child(ChildOwner::ScriptDebugger, 2469);
    runtime
        .child_registry
        .record_exit_for_test(child, ChildExitOutcome::Exited(0));
    runtime.collect_child_completion_events();
    let events = runtime.event_batch.drain_ready_for_test();
    let EventPayload::ChildCompletion(completion) = &events[0].payload else {
        panic!("expected child completion event");
    };

    let outcome = runtime.dispatch_child_completion_for_test(
        completion.clone(),
        DispatchContext::CommandBlocked {
            protected_client: Token(1),
        },
    );

    assert_eq!(outcome, DispatchOutcome::Requeued);
    assert!(runtime.child_registry.contains(child));
    assert_eq!(
        runtime
            .event_batch
            .next_turn_queued_sequence_count_for_test(),
        1
    );

    runtime.event_batch.begin_next_turn();
    runtime
        .dispatch_ready_phase(DispatchContext::Normal)
        .expect("dispatch deferred child completion");

    assert!(!runtime.child_registry.contains(child));
}

#[test]
fn child_registry_tracks_every_planned_owner_kind() {
    let mut registry = ChildRegistry::default();
    let slot_job = crate::engine::distributed::cluster::migration::MigrationJobId::generate();
    let cases = [
        (ChildOwner::RdbSave, 2001),
        (ChildOwner::AofRewrite, 2002),
        (ChildOwner::DisklessReplication, 2003),
        (ChildOwner::DiskSyncReplication, 2004),
        (ChildOwner::SlotSnapshot(slot_job), 2005),
        (ChildOwner::DirectDualRdb, 2006),
    ];

    for (owner, pid) in cases {
        let child = registry.register_test_child(owner.clone(), pid);
        assert_eq!(registry.owner(child), Some(&owner));
        assert_eq!(registry.child_for_pid(pid), Some(child));
    }
}

#[test]
fn duplicate_install_is_rejected_without_overwriting_the_route() {
    let mut registry = RuntimeSourceRegistry::default();
    let handle = registry
        .install(Token(7), SourceOwner::Listener)
        .expect("first source route");

    assert!(registry.install(Token(7), SourceOwner::Connection).is_err());
    assert_eq!(
        registry.resolve(handle).map(|route| route.owner.clone()),
        Some(SourceOwner::Listener)
    );
}

#[test]
fn retired_source_cannot_route_a_tail_readiness_event() {
    let mut registry = RuntimeSourceRegistry::default();
    let handle = registry
        .install(Token(11), SourceOwner::Connection)
        .expect("source route");

    registry.retire(handle).expect("active route");

    assert!(registry.resolve(handle).is_none());
    assert!(registry.resolve_token(Token(11)).is_none());
    assert!(registry.retire(handle).is_err());
}

#[test]
fn monotonic_runtime_token_prevents_transport_reuse_from_routing_to_old_owner() {
    let transport_id = TransportId::new(5).expect("nonzero transport id");
    let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
    let mut sources = RuntimeSourceRegistry::default();
    let old_token = transport.register_test_transport(transport_id);
    let old_handle = sources
        .install(old_token, SourceOwner::Listener)
        .expect("old route");
    sources.retire(old_handle).expect("retire old route");
    transport
        .remove_registered(old_token)
        .expect("remove old transport");

    let new_token = transport.register_test_transport(transport_id);
    let new_handle = sources
        .install(new_token, SourceOwner::Connection)
        .expect("replacement route");

    assert_ne!(old_token, new_token);
    assert!(sources.resolve(old_handle).is_none());
    assert_eq!(
        sources.resolve(new_handle).map(|route| route.owner.clone()),
        Some(SourceOwner::Connection)
    );
}

#[test]
fn runtime_construction_routes_the_io_worker_wake_source() {
    let runtime = ServerWorkerState::new(1).expect("runtime");
    let route = runtime
        .runtime_sources
        .resolve_token(runtime.io_threads.wake_token())
        .expect("IO worker wake route");

    assert_eq!(route.owner, SourceOwner::IoWorkerWake);
}

#[test]
fn runtime_construction_routes_the_child_signal_source() {
    let runtime = ServerWorkerState::new(1).expect("runtime");
    let route = runtime
        .runtime_sources
        .resolve_token(runtime.child_signal_token)
        .expect("child signal route");

    assert_eq!(
        route.owner,
        SourceOwner::ChildSignal(ChildSignalOwner::SelfPipe)
    );
}

#[test]
fn sentinel_adapter_connect_installs_and_remove_retires_the_route() {
    let transport_id = TransportId::new(8).expect("nonzero transport id");
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 26379));
    let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
    transport.inner_mut().push_peer(ConnectedPeer {
        id: transport_id,
        peer_addr,
    });
    let mut sources = RuntimeSourceRegistry::default();

    let token = {
        let mut pending = HashMap::new();
        let mut sentinel = RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending);
        sentinel
            .connect_tcp("127.0.0.1:26379")
            .expect("connect Sentinel transport")
    };
    assert_eq!(
        sources
            .resolve_token(token)
            .map(|route| route.owner.clone()),
        Some(SourceOwner::Sentinel)
    );

    let mut pending = HashMap::new();
    RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending)
        .remove(token)
        .expect("remove Sentinel transport");
    assert!(sources.resolve_token(token).is_none());
    assert!(!transport.is_registered(token));
}

#[test]
fn sentinel_route_is_retired_before_failed_transport_removal() {
    let transport_id = TransportId::new(9).expect("nonzero transport id");
    let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
    let token = transport.register_test_transport(transport_id);
    let mut sources = RuntimeSourceRegistry::default();
    sources
        .install(token, SourceOwner::Sentinel)
        .expect("Sentinel route");
    transport.inner_mut().fail_remove();

    let mut pending = HashMap::new();
    assert!(
        RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending)
            .remove(token)
            .is_err()
    );

    assert!(sources.resolve_token(token).is_none());
    assert!(transport.is_registered(token));
    assert_eq!(
        pending.get(&token).map(|removal| removal.owner_kind),
        Some(PendingRemovalOwner::Sentinel)
    );
}

#[test]
fn sentinel_retry_remove_clears_pending_after_stale_route() {
    let transport_id = TransportId::new(12).expect("nonzero transport id");
    let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
    let token = transport.register_test_transport(transport_id);
    let mut sources = RuntimeSourceRegistry::default();
    sources
        .install(token, SourceOwner::Sentinel)
        .expect("Sentinel route");
    let mut pending = HashMap::new();

    transport.inner_mut().fail_remove();
    assert!(
        RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending)
            .remove(token)
            .is_err()
    );

    RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending)
        .remove(token)
        .expect("retry remove Sentinel transport");

    assert!(sources.resolve_token(token).is_none());
    assert!(!transport.is_registered(token));
    assert!(!pending.contains_key(&token));
}

#[test]
fn runtime_registration_failure_quarantines_transport_until_retry() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer");
    let peer_addr = listener.local_addr().expect("peer address");
    let peer = runtime
        .transport
        .connect_tcp(peer_addr)
        .expect("connect runtime transport");
    let (_accepted, _) = listener.accept().expect("accept runtime transport");
    runtime
        .runtime_sources
        .install(peer.token, SourceOwner::Listener)
        .expect("conflicting route");
    runtime
        .transport
        .deregister_for_test(peer.token)
        .expect("inject failed rollback remove");

    let error = runtime
        .route_runtime_source(peer.token, SourceOwner::Connection)
        .expect_err("duplicate route must fail");

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(runtime.runtime_sources.resolve_token(peer.token).is_none());
    assert!(runtime.transport.is_registered(peer.token));
    assert_eq!(
        runtime
            .pending_transport_removals
            .get(&peer.token)
            .map(|removal| removal.owner_kind),
        Some(PendingRemovalOwner::FailedRegistration)
    );

    runtime
        .transport
        .reregister_for_test(peer.token)
        .expect("restore registration before retry");
    runtime.retry_pending_transport_removals();

    assert!(!runtime.transport.is_registered(peer.token));
    assert!(!runtime.pending_transport_removals.contains_key(&peer.token));
}

#[test]
fn sentinel_connect_rolls_back_transport_when_route_install_fails() {
    let transport_id = TransportId::new(10).expect("nonzero transport id");
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 26379));
    let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
    transport.inner_mut().push_peer(ConnectedPeer {
        id: transport_id,
        peer_addr,
    });
    let mut sources = RuntimeSourceRegistry::default();
    let existing = sources
        .install(Token(1), SourceOwner::Listener)
        .expect("conflicting route");

    let mut pending = HashMap::new();
    assert!(
        RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending)
            .connect_tcp("127.0.0.1:26379")
            .is_err()
    );

    assert!(!transport.is_registered(Token(1)));
    assert_eq!(transport.inner_mut().removed(), &[transport_id]);
    assert!(sources.resolve(existing).is_none());
}

#[test]
fn sentinel_connect_quarantines_transport_until_retry_when_remove_fails() {
    let transport_id = TransportId::new(11).expect("nonzero transport id");
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 26379));
    let mut transport = RuntimeTransport::new(FakeTransportRuntime::default());
    transport.inner_mut().push_peer(ConnectedPeer {
        id: transport_id,
        peer_addr,
    });
    transport.inner_mut().fail_remove();
    let mut sources = RuntimeSourceRegistry::default();
    let mut pending = HashMap::new();
    sources
        .install(Token(1), SourceOwner::Listener)
        .expect("conflicting route");

    let error = RuntimeSentinelIo::new(&mut transport, &mut sources, &mut pending)
        .connect_tcp("127.0.0.1:26379")
        .expect_err("duplicate Sentinel route must fail");

    assert!(error.contains("transport rollback also failed"));
    assert!(sources.resolve_token(Token(1)).is_none());
    assert!(transport.is_registered(Token(1)));
    assert_eq!(
        pending.get(&Token(1)).map(|removal| removal.owner_kind),
        Some(PendingRemovalOwner::FailedRegistration)
    );

    crate::runtime::server::worker::reactor::transport::retry_pending_transport_removals(
        &mut transport,
        &mut pending,
    );

    assert!(!transport.is_registered(Token(1)));
    assert!(!pending.contains_key(&Token(1)));
}

#[test]
fn retired_io_worker_wake_route_makes_the_event_stale() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let wake_token = runtime.io_threads.wake_token();

    runtime.retire_runtime_source(wake_token);

    assert!(runtime.dispatch_transport_event_is_stale_for_test(
        ReadyEvent {
            token: wake_token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    ));
}

#[test]
fn runtime_owned_source_register_failure_leaves_no_route_or_handle() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    runtime.transport.fail_register_source_for_test();
    let (source, _writer) = owned_pipe_source(PollSourceMode::Readable);

    let error = runtime
        .install_runtime_source(source, SourceOwner::Connection)
        .expect_err("register failure must leave no route installed");

    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert!(runtime.pending_transport_removals.is_empty());
    assert!(
        runtime
            .runtime_sources
            .resolve_token(runtime.io_threads.wake_token())
            .is_some(),
        "existing wake route remains intact after register failure"
    );
}

#[test]
fn runtime_owned_source_disable_failure_leaves_enabled_state_unchanged() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let (source, _writer) = owned_pipe_source(PollSourceMode::Readable);
    let handle = runtime
        .install_runtime_source(source, SourceOwner::Connection)
        .expect("install runtime-owned source");
    assert!(runtime
        .transport
        .source_enabled_for_test(handle.token)
        .expect("query initial source state"));

    runtime.transport.fail_set_source_enabled_for_test(false);
    let disable_error = runtime
        .transport
        .set_source_enabled(handle.token, false)
        .expect_err("disable failure must propagate");
    assert_eq!(disable_error.kind(), std::io::ErrorKind::Other);
    assert!(runtime
        .transport
        .source_enabled_for_test(handle.token)
        .expect("query source state after failed disable"));
}

#[test]
fn runtime_owned_source_reenable_failure_keeps_source_disabled() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let (source, _writer) = owned_pipe_source(PollSourceMode::Readable);
    let handle = runtime
        .install_runtime_source(source, SourceOwner::Connection)
        .expect("install runtime-owned source");

    runtime
        .transport
        .set_source_enabled(handle.token, false)
        .expect("disable source before re-enable failure");
    assert!(!runtime
        .transport
        .source_enabled_for_test(handle.token)
        .expect("query disabled source state"));

    runtime.transport.fail_set_source_enabled_for_test(true);
    let enable_error = runtime
        .transport
        .set_source_enabled(handle.token, true)
        .expect_err("re-enable failure must propagate");
    assert_eq!(enable_error.kind(), std::io::ErrorKind::Other);
    assert!(!runtime
        .transport
        .source_enabled_for_test(handle.token)
        .expect("query source state after failed re-enable"));
    assert!(
        runtime
            .runtime_sources
            .resolve_token(handle.token)
            .is_some(),
        "route stays installed until explicit teardown"
    );
}

#[test]
fn runtime_owned_source_remove_failure_retires_route_and_retries_transport_mapping() {
    let mut runtime = ServerWorkerState::new(1).expect("runtime");
    let (source, _writer) = owned_pipe_source(PollSourceMode::Readable);
    let handle = runtime
        .install_runtime_source(source, SourceOwner::Connection)
        .expect("install runtime-owned source");
    runtime.transport.fail_remove_for_test();

    let error = runtime
        .deregister_or_defer_with_result(handle.token, PendingRemovalOwner::FailedRegistration)
        .expect_err("remove failure must be retained");

    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    runtime.retire_runtime_source(handle.token);
    assert!(runtime
        .runtime_sources
        .resolve_token(handle.token)
        .is_none());
    assert!(runtime.transport.is_registered(handle.token));
    assert_eq!(
        runtime
            .pending_transport_removals
            .get(&handle.token)
            .map(|removal| removal.owner_kind),
        Some(PendingRemovalOwner::FailedRegistration)
    );
    assert!(runtime.dispatch_transport_event_is_stale_for_test(
        ReadyEvent {
            token: handle.token,
            readable: true,
            writable: false,
            error: false,
            read_hup: false,
        },
        false,
    ));

    runtime.retry_pending_transport_removals();
    assert!(!runtime.transport.is_registered(handle.token));
    assert!(!runtime
        .pending_transport_removals
        .contains_key(&handle.token));
}
