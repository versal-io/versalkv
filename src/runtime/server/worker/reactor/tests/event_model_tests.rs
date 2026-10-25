use std::time::Duration;

use crate::foundation::io::store::Robj;
use crate::foundation::io::sys::time::MonotonicInstant;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::queue::EventBatch;
use crate::runtime::server::worker::reactor::timers::{
    RuntimeTimeEvent, TimeEventId, TimeEventRegistry,
};
use crate::runtime::server::worker::reactor::{
    BeforePollSlot, DispatchContext, EventLane, EventLimits, EventLoopEvent, EventOwner,
    EventPayload, EventPhase, ReadinessEvent, ReadinessOrigin, RunnableKey, TimerEvent,
};
use crate::runtime::server::worker::ServerWorkerState;
use bytes::Bytes;

fn readiness(event: EventLoopEvent) -> ReadinessEvent {
    let EventPayload::Readiness(readiness) = event.payload else {
        panic!("expected readiness event");
    };
    readiness
}

fn timer_event(id: TimeEventId, payload: TimerEvent) -> EventLoopEvent {
    EventLoopEvent::timer(id, payload)
}

#[test]
fn timer_constructor_assigns_canonical_metadata() {
    let mut registry = TimeEventRegistry::default();
    let id = registry.create_time_event(0_i64, Duration::ZERO, None, RuntimeTimeEvent::Test(1));

    let event = EventLoopEvent::timer(id, TimerEvent::ServerCron);

    assert_eq!(event.sequence, 0);
    assert_eq!(event.phase, EventPhase::Time);
    assert_eq!(event.lane, EventLane::Normal);
    assert_eq!(event.owner, EventOwner::Timer(id));
    assert!(matches!(
        event.payload,
        EventPayload::Timer(TimerEvent::ServerCron)
    ));
}

#[test]
fn eviction_reschedules_a_due_timer_instead_of_forcing_zero_poll() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.maxmemory = 1;
    runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
    runtime.ctx.db_mut(0).unwrap().set(
        b"key".to_vec(),
        Robj::string_raw(Bytes::from(vec![b'x'; 4096])),
    );

    runtime.schedule_eviction();

    assert!(runtime
        .time_events
        .has_due_kind(TimerEvent::EvictionContinuation));
    assert!(!runtime.ctx.needs_eviction());
}

#[test]
fn cancelling_eviction_removes_an_already_collected_timer() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime.schedule_eviction();
    runtime.collect_due_time_events();
    assert!(runtime.event_batch.has_due_time_events());

    runtime.cancel_eviction_timer();

    assert!(!runtime.event_batch.has_due_time_events());
    assert!(runtime.pending_due_time_events.is_empty());
}

fn pending_eviction_timer_count(runtime: &ServerWorkerState) -> usize {
    runtime
        .pending_due_time_events
        .values()
        .filter(|event| event.kind == TimerEvent::EvictionContinuation)
        .count()
}

#[test]
fn collected_eviction_timer_blocks_a_second_registration() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime.schedule_eviction();
    runtime.collect_due_time_events();
    assert_eq!(pending_eviction_timer_count(&runtime), 1);

    runtime.schedule_eviction();

    assert!(!runtime
        .time_events
        .has_due_kind(TimerEvent::EvictionContinuation));
    assert_eq!(pending_eviction_timer_count(&runtime), 1);
    let handled_before = runtime.ctx.stats().eventloop_events_handled;
    runtime.dispatch_time_phase().unwrap();
    assert_eq!(
        runtime.ctx.stats().eventloop_events_handled,
        handled_before + 1
    );
    assert_eq!(pending_eviction_timer_count(&runtime), 0);
    assert!(!runtime.event_batch.has_due_time_events());
}

fn runtime_with_bounded_eviction_work() -> ServerWorkerState {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.maxmemory = 1;
    runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
    runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 0;
    for index in 0..64 {
        runtime.ctx.db_mut(0).unwrap().set(
            format!("eviction-key-{index}").into_bytes(),
            Robj::string_raw(Bytes::from(vec![b'x'; 4096])),
        );
    }
    runtime
}

#[test]
fn eviction_time_dispatch_reschedules_one_zero_delay_timer_after_a_bounded_slice() {
    let mut runtime = runtime_with_bounded_eviction_work();
    runtime.schedule_eviction();
    runtime.collect_due_time_events();
    runtime.schedule_eviction();
    let handled_before = runtime.ctx.stats().eventloop_events_handled;

    runtime.dispatch_time_phase().unwrap();

    assert_eq!(
        runtime.ctx.stats().eventloop_events_handled,
        handled_before + 1
    );
    assert!(runtime.ctx.stats().evicted_keys >= 16);
    assert!(runtime
        .time_events
        .has_due_kind(TimerEvent::EvictionContinuation));
    assert_eq!(pending_eviction_timer_count(&runtime), 0);
    runtime.collect_due_time_events();
    assert!(!runtime
        .time_events
        .has_due_kind(TimerEvent::EvictionContinuation));
    assert_eq!(pending_eviction_timer_count(&runtime), 1);
    runtime.cancel_eviction_timer();
}

#[test]
fn server_cron_and_eviction_timer_dispatch_only_one_bounded_slice() {
    let mut runtime = runtime_with_bounded_eviction_work();
    runtime.register_server_cron();
    runtime.schedule_eviction();
    runtime.collect_due_time_events();
    let evicted_before = runtime.ctx.stats().evicted_keys;

    runtime.dispatch_time_phase().unwrap();

    assert_eq!(runtime.ctx.stats().evicted_keys - evicted_before, 16);
    assert!(runtime
        .time_events
        .has_due_kind(TimerEvent::EvictionContinuation));
    runtime.collect_due_time_events();
    assert_eq!(pending_eviction_timer_count(&runtime), 1);
    runtime.cancel_eviction_timer();
}

#[test]
fn paused_eviction_time_dispatch_cancels_the_collected_timer() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.maxmemory = 1;
    runtime.ctx.client_pause_mut().action = crate::framework::context::PauseAction::All;
    runtime.ctx.client_pause_mut().end_time_ms = i64::MAX;
    runtime.schedule_eviction();
    runtime.collect_due_time_events();

    runtime.dispatch_time_phase().unwrap();

    assert!(!runtime
        .time_events
        .has_due_kind(TimerEvent::EvictionContinuation));
    assert_eq!(pending_eviction_timer_count(&runtime), 0);
    assert!(!runtime.event_batch.has_due_time_events());
}

#[test]
fn converged_eviction_time_dispatch_cancels_the_registered_timer() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    runtime.ctx.config_mut().server.maxmemory = 1;
    runtime.schedule_eviction();
    runtime.ctx.config_mut().server.maxmemory = 0;
    runtime.collect_due_time_events();

    runtime.dispatch_time_phase().unwrap();

    assert!(!runtime
        .time_events
        .has_due_kind(TimerEvent::EvictionContinuation));
    assert_eq!(pending_eviction_timer_count(&runtime), 0);
    assert!(!runtime.event_batch.has_due_time_events());
}

#[test]
fn pending_runtime_work_is_deduplicated_by_runnable_key_and_slot() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let collected_before = runtime.ctx.stats().eventloop_events_collected;
    let scheduled = [
        RunnableKey::ClientRead(Token(11)),
        RunnableKey::ClientRead(Token(11)),
        RunnableKey::ClientWrite(Token(12)),
        RunnableKey::ClientWrite(Token(12)),
        RunnableKey::SlotExportWrite(Token(13)),
        RunnableKey::SlotExportWrite(Token(13)),
    ];
    for key in scheduled {
        runtime.schedule_runtime_runnable(key);
    }
    assert_eq!(
        runtime.ctx.stats().eventloop_events_collected,
        collected_before
    );

    let events = runtime.event_batch.drain_before_poll_for_test();
    let keys: Vec<_> = events
        .into_iter()
        .map(|event| {
            let EventPayload::Runnable(runnable) = event.payload else {
                panic!("expected runnable event");
            };
            runnable.key
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            RunnableKey::ClientRead(Token(11)),
            RunnableKey::ClientWrite(Token(12)),
            RunnableKey::SlotExportWrite(Token(13)),
        ]
    );
}

#[test]
fn readiness_for_one_token_is_coalesced_without_losing_polled_origin() {
    let mut batch = EventBatch::default();
    let mut pending_readiness = ReadinessEvent::readable(Token(9), ReadinessOrigin::PendingData);
    pending_readiness.read_closed = true;
    let mut polled_readiness = ReadinessEvent::writable(Token(9), ReadinessOrigin::Polled);
    polled_readiness.error = true;
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Normal,
        pending_readiness,
    ));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Normal,
        polled_readiness,
    ));

    let coalesced = readiness(batch.pop_ready().expect("coalesced readiness"));
    assert!(coalesced.readable);
    assert!(coalesced.writable);
    assert!(coalesced.read_closed);
    assert!(coalesced.error);
    assert_eq!(coalesced.origin, ReadinessOrigin::Polled);
    assert!(batch.pop_ready().is_none());

    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Normal,
        ReadinessEvent::readable(Token(10), ReadinessOrigin::Polled),
    ));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Normal,
        ReadinessEvent::writable(Token(10), ReadinessOrigin::PendingData),
    ));
    let coalesced = readiness(batch.pop_ready().expect("reverse-origin readiness"));
    assert_eq!(coalesced.origin, ReadinessOrigin::Polled);
}

#[test]
fn readiness_for_one_token_remains_separate_across_phases() {
    let mut batch = EventBatch::default();
    batch.push(EventLoopEvent::readiness(
        EventPhase::BeforePoll,
        EventLane::Normal,
        ReadinessEvent::readable(Token(11), ReadinessOrigin::PendingData),
    ));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Control,
        ReadinessEvent::writable(Token(11), ReadinessOrigin::Polled),
    ));

    let before_poll = batch
        .pop_before_poll()
        .expect("before-poll readiness remains queued");
    assert_eq!(before_poll.phase, EventPhase::BeforePoll);
    let before_poll = readiness(before_poll);
    assert!(before_poll.readable);
    assert!(!before_poll.writable);

    let ready = batch.pop_ready().expect("ready readiness remains queued");
    assert_eq!(ready.phase, EventPhase::Ready);
    let ready = readiness(ready);
    assert!(!ready.readable);
    assert!(ready.writable);
}

#[test]
fn ready_events_are_fifo_within_one_lane() {
    let mut batch = EventBatch::default();
    for token in [3, 1, 2] {
        batch.push(EventLoopEvent::readiness(
            EventPhase::Ready,
            EventLane::Normal,
            ReadinessEvent::readable(Token(token), ReadinessOrigin::Polled),
        ));
    }

    let events = [
        batch.pop_ready().expect("first ready event"),
        batch.pop_ready().expect("second ready event"),
        batch.pop_ready().expect("third ready event"),
    ];
    let tokens: Vec<_> = events
        .into_iter()
        .map(|event| (event.sequence, readiness(event).token))
        .collect();
    assert_eq!(tokens, vec![(1, Token(3)), (2, Token(1)), (3, Token(2))]);
}

#[test]
fn event_batch_overwrites_external_sequence_values() {
    let mut batch = EventBatch::default();
    let mut first = EventLoopEvent::runnable_for_test(51);
    first.sequence = u64::MAX;
    let mut second = EventLoopEvent::runnable_for_test(52);
    second.sequence = 1;
    batch.push(first);
    batch.push(second);

    assert_eq!(batch.pop_before_poll().expect("first event").sequence, 1);
    assert_eq!(batch.pop_before_poll().expect("second event").sequence, 2);
}

#[test]
fn ready_lanes_dispatch_control_then_upstream_then_normal() {
    let mut batch = EventBatch::default();
    for (lane, token) in [
        (EventLane::Normal, 30),
        (EventLane::Control, 10),
        (EventLane::Upstream, 20),
    ] {
        batch.push(EventLoopEvent::readiness(
            EventPhase::Ready,
            lane,
            ReadinessEvent::readable(Token(token), ReadinessOrigin::Polled),
        ));
    }

    let tokens: Vec<_> = (0..3)
        .map(|_| readiness(batch.pop_ready().expect("ready lane event")).token)
        .collect();
    assert_eq!(tokens, vec![Token(10), Token(20), Token(30)]);
}

#[test]
fn coalesced_readiness_is_promoted_to_the_higher_priority_lane() {
    let mut batch = EventBatch::default();
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Normal,
        ReadinessEvent::readable(Token(31), ReadinessOrigin::PendingData),
    ));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Control,
        ReadinessEvent::readable(Token(32), ReadinessOrigin::Polled),
    ));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Control,
        ReadinessEvent::writable(Token(31), ReadinessOrigin::Polled),
    ));

    let existing = batch.pop_ready().expect("existing control readiness");
    assert_eq!(existing.lane, EventLane::Control);
    assert_eq!(readiness(existing).token, Token(32));
    let promoted = batch.pop_ready().expect("promoted control readiness");
    assert_eq!(promoted.lane, EventLane::Control);
    assert_eq!(readiness(promoted).token, Token(31));
}

#[test]
fn next_turn_lane_promotion_appends_after_existing_destination_events() {
    let mut batch = EventBatch::default();
    batch.finish_phase(EventPhase::Ready);
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Normal,
        ReadinessEvent::readable(Token(33), ReadinessOrigin::PendingData),
    ));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Control,
        ReadinessEvent::readable(Token(34), ReadinessOrigin::Polled),
    ));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Control,
        ReadinessEvent::writable(Token(33), ReadinessOrigin::Polled),
    ));

    batch.begin_next_turn();
    assert_eq!(
        readiness(batch.pop_ready().expect("existing control readiness")).token,
        Token(34)
    );
    assert_eq!(
        readiness(batch.pop_ready().expect("promoted control readiness")).token,
        Token(33)
    );
}

#[test]
fn before_poll_slots_follow_the_fixed_barrier_order() {
    let mut batch = EventBatch::default();
    let slots = [
        BeforePollSlot::PollPreparation,
        BeforePollSlot::OrdinaryWrites,
        BeforePollSlot::PendingInput,
        BeforePollSlot::DistributedWrites,
        BeforePollSlot::AofBarrier,
        BeforePollSlot::Maintenance,
        BeforePollSlot::PreAof,
    ];
    for (key, slot) in slots.into_iter().enumerate() {
        batch.push_before_poll(slot, EventLoopEvent::runnable_for_test(key));
    }

    let keys: Vec<_> = (0..slots.len())
        .map(|_| {
            let event = batch.pop_before_poll().expect("before-poll event");
            let EventPayload::Runnable(runnable) = event.payload else {
                panic!("expected runnable event");
            };
            runnable.key
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            RunnableKey::Test(2),
            RunnableKey::Test(6),
            RunnableKey::Test(5),
            RunnableKey::Test(4),
            RunnableKey::Test(1),
            RunnableKey::Test(3),
            RunnableKey::Test(0),
        ]
    );
}

#[test]
fn before_poll_events_are_fifo_within_one_slot() {
    let mut batch = EventBatch::default();
    batch.push_before_poll(
        BeforePollSlot::Maintenance,
        EventLoopEvent::runnable_for_test(41),
    );
    batch.push_before_poll(
        BeforePollSlot::Maintenance,
        EventLoopEvent::runnable_for_test(42),
    );

    let keys: Vec<_> = (0..2)
        .map(|_| {
            let event = batch.pop_before_poll().expect("same-slot event");
            let EventPayload::Runnable(runnable) = event.payload else {
                panic!("expected runnable event");
            };
            runnable.key
        })
        .collect();
    assert_eq!(keys, vec![RunnableKey::Test(41), RunnableKey::Test(42)]);
}

#[test]
fn coalesced_readiness_is_promoted_to_the_earlier_before_poll_slot() {
    let mut batch = EventBatch::default();
    batch.push_before_poll(
        BeforePollSlot::PollPreparation,
        EventLoopEvent::readiness(
            EventPhase::BeforePoll,
            EventLane::Normal,
            ReadinessEvent::readable(Token(41), ReadinessOrigin::PendingData),
        ),
    );
    batch.push_before_poll(
        BeforePollSlot::Maintenance,
        EventLoopEvent::readiness(
            EventPhase::BeforePoll,
            EventLane::Normal,
            ReadinessEvent::readable(Token(42), ReadinessOrigin::Polled),
        ),
    );
    batch.push_before_poll(
        BeforePollSlot::Maintenance,
        EventLoopEvent::readiness(
            EventPhase::BeforePoll,
            EventLane::Normal,
            ReadinessEvent::writable(Token(41), ReadinessOrigin::Polled),
        ),
    );

    assert_eq!(
        readiness(
            batch
                .pop_before_poll()
                .expect("existing maintenance readiness")
        )
        .token,
        Token(42)
    );
    assert_eq!(
        readiness(
            batch
                .pop_before_poll()
                .expect("promoted maintenance readiness")
        )
        .token,
        Token(41)
    );
}

#[test]
fn next_turn_slot_promotion_appends_after_existing_destination_events() {
    let mut batch = EventBatch::default();
    batch.finish_phase(EventPhase::BeforePoll);
    batch.push_before_poll(
        BeforePollSlot::PollPreparation,
        EventLoopEvent::readiness(
            EventPhase::BeforePoll,
            EventLane::Normal,
            ReadinessEvent::readable(Token(43), ReadinessOrigin::PendingData),
        ),
    );
    batch.push_before_poll(
        BeforePollSlot::Maintenance,
        EventLoopEvent::readiness(
            EventPhase::BeforePoll,
            EventLane::Normal,
            ReadinessEvent::readable(Token(44), ReadinessOrigin::Polled),
        ),
    );
    batch.push_before_poll(
        BeforePollSlot::Maintenance,
        EventLoopEvent::readiness(
            EventPhase::BeforePoll,
            EventLane::Normal,
            ReadinessEvent::writable(Token(43), ReadinessOrigin::Polled),
        ),
    );

    batch.begin_next_turn();
    assert_eq!(
        readiness(
            batch
                .pop_before_poll()
                .expect("existing maintenance readiness")
        )
        .token,
        Token(44)
    );
    assert_eq!(
        readiness(
            batch
                .pop_before_poll()
                .expect("promoted maintenance readiness")
        )
        .token,
        Token(43)
    );
}

#[test]
fn timers_are_ordered_by_deadline_then_event_id() {
    let mut registry = TimeEventRegistry::default();
    let first_id =
        registry.create_time_event(0_i64, Duration::ZERO, None, RuntimeTimeEvent::Test(1));
    let second_id =
        registry.create_time_event(0_i64, Duration::ZERO, None, RuntimeTimeEvent::Test(2));
    let third_id =
        registry.create_time_event(0_i64, Duration::ZERO, None, RuntimeTimeEvent::Test(3));
    let mut batch = EventBatch::default();
    batch.push_time(
        MonotonicInstant::from_millis(20),
        timer_event(first_id, TimerEvent::ServerCron),
    );
    batch.push_time(
        MonotonicInstant::from_millis(10),
        timer_event(third_id, TimerEvent::EvictionContinuation),
    );
    batch.push_time(
        MonotonicInstant::from_millis(10),
        timer_event(second_id, TimerEvent::ModuleTimer),
    );

    let owners: Vec<_> = (0..3)
        .map(|_| batch.pop_time().expect("time event").owner)
        .collect();
    assert_eq!(
        owners,
        vec![
            EventOwner::Timer(second_id),
            EventOwner::Timer(third_id),
            EventOwner::Timer(first_id),
        ]
    );
}

#[test]
fn runnable_keys_are_deduplicated_until_dispatch() {
    let mut batch = EventBatch::default();
    batch.push(EventLoopEvent::runnable_for_test(7));
    batch.push(EventLoopEvent::runnable_for_test(7));

    assert!(batch.pop_before_poll().is_some());
    assert!(batch.pop_before_poll().is_none());

    batch.push(EventLoopEvent::runnable_for_test(7));
    assert!(batch.pop_before_poll().is_some());
}

#[test]
fn cancelling_many_runnables_physically_removes_current_and_next_turn_sequences() {
    const RUNNABLE_COUNT: usize = 4_096;

    let mut current = EventBatch::default();
    for key in 0..RUNNABLE_COUNT {
        current.push(EventLoopEvent::runnable_for_test(key));
    }
    for key in 0..RUNNABLE_COUNT {
        assert!(current.remove_runnable(RunnableKey::Test(key)));
    }
    assert_eq!(
        current.current_queued_sequence_count_for_test(),
        0,
        "current-turn cancellation must physically unlink every sequence"
    );
    assert!(
        current.sequence_unlink_work_for_test() <= RUNNABLE_COUNT * 4,
        "each current cancellation may perform only fixed map and neighbor updates"
    );
    assert!((0..RUNNABLE_COUNT).all(|key| !current.contains_runnable(RunnableKey::Test(key))));
    assert!(current.drain_before_poll_for_test().is_empty());
    assert_eq!(current.current_queued_sequence_count_for_test(), 0);

    let mut next_turn = EventBatch::default();
    for key in 0..RUNNABLE_COUNT {
        next_turn.push_before_poll_next_turn(
            BeforePollSlot::Maintenance,
            EventLoopEvent::runnable_for_test(key),
        );
    }
    for key in 0..RUNNABLE_COUNT {
        assert!(next_turn.remove_runnable(RunnableKey::Test(key)));
    }
    assert_eq!(
        next_turn.next_turn_queued_sequence_count_for_test(),
        0,
        "next-turn cancellation must physically unlink every sequence"
    );
    assert!(
        next_turn.sequence_unlink_work_for_test() <= RUNNABLE_COUNT * 3,
        "each next-turn cancellation may perform only fixed map and neighbor updates"
    );
    assert!((0..RUNNABLE_COUNT).all(|key| !next_turn.contains_runnable(RunnableKey::Test(key))));
    next_turn.begin_next_turn();
    assert!(next_turn.drain_before_poll_for_test().is_empty());
    assert_eq!(next_turn.current_queued_sequence_count_for_test(), 0);
}

#[test]
fn current_runnable_cancel_then_readd_dispatches_new_sequence_once() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let key = RunnableKey::Test(7);
    runtime.schedule_runtime_runnable(key);
    assert!(runtime.cancel_runtime_runnable(key));
    runtime.schedule_runtime_runnable(key);
    assert_eq!(
        runtime.event_batch.current_queued_sequence_count_for_test(),
        1
    );
    let handled_before = runtime.ctx.stats().eventloop_events_handled;

    runtime
        .dispatch_before_poll_slot(BeforePollSlot::Maintenance, DispatchContext::Normal)
        .unwrap();

    assert_eq!(
        runtime.ctx.stats().eventloop_events_handled,
        handled_before + 1
    );
    assert!(!runtime.event_batch.contains_runnable(key));
}

#[test]
fn physical_middle_cancellation_preserves_fifo_and_rollover_order() {
    let mut batch = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 1,
        max_same_owner_events_per_turn: 64,
    });
    for key in 0..5 {
        batch.push(EventLoopEvent::runnable_for_test(key));
    }
    assert!(batch.remove_runnable(RunnableKey::Test(1)));
    assert!(batch.remove_runnable(RunnableKey::Test(3)));
    assert_eq!(batch.current_queued_sequence_count_for_test(), 3);

    let first = batch.pop_before_poll().expect("first live runnable");
    let EventPayload::Runnable(first) = first.payload else {
        panic!("expected runnable event");
    };
    assert_eq!(first.key, RunnableKey::Test(0));
    assert!(batch.pop_before_poll().is_none());

    batch.begin_next_turn();
    let second = batch.pop_before_poll().expect("second live runnable");
    let EventPayload::Runnable(second) = second.payload else {
        panic!("expected runnable event");
    };
    assert_eq!(second.key, RunnableKey::Test(2));

    batch.begin_next_turn();
    let third = batch.pop_before_poll().expect("third live runnable");
    let EventPayload::Runnable(third) = third.payload else {
        panic!("expected runnable event");
    };
    assert_eq!(third.key, RunnableKey::Test(4));
    assert!(batch.pop_before_poll().is_none());
}

#[test]
fn next_turn_runnable_cancel_then_readd_dispatches_new_sequence_once() {
    let mut runtime = ServerWorkerState::new(1).unwrap();
    let key = RunnableKey::Test(8);
    runtime.schedule_runtime_runnable_next_turn(key);
    assert!(runtime.cancel_runtime_runnable(key));
    runtime.schedule_runtime_runnable_next_turn(key);
    assert_eq!(
        runtime
            .event_batch
            .next_turn_queued_sequence_count_for_test(),
        1
    );
    runtime.event_batch.begin_next_turn();
    let handled_before = runtime.ctx.stats().eventloop_events_handled;

    runtime
        .dispatch_before_poll_slot(BeforePollSlot::Maintenance, DispatchContext::Normal)
        .unwrap();

    assert_eq!(
        runtime.ctx.stats().eventloop_events_handled,
        handled_before + 1
    );
    assert!(!runtime.event_batch.contains_runnable(key));
}

#[test]
fn event_emitted_for_a_completed_phase_moves_to_next_turn() {
    let mut batch = EventBatch::default();
    batch.finish_phase(EventPhase::BeforePoll);
    batch.push(EventLoopEvent::runnable_for_test(7));

    assert!(batch.pop_before_poll().is_none());
    batch.begin_next_turn();
    assert!(batch.pop_before_poll().is_some());
}

#[test]
fn dispatch_budget_retains_the_undispatched_tail() {
    let mut batch = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 2,
        max_same_owner_events_per_turn: 1,
    });
    batch.push(EventLoopEvent::runnable_for_test(1));
    batch.push(EventLoopEvent::runnable_for_test(2));
    batch.push(EventLoopEvent::runnable_for_test(3));

    assert_eq!(batch.drain_before_poll_for_test().len(), 2);
    batch.begin_next_turn();
    assert_eq!(batch.drain_before_poll_for_test().len(), 1);
}

#[test]
fn enqueue_after_budget_rollover_follows_the_deferred_tail() {
    let mut batch = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 1,
        max_same_owner_events_per_turn: 64,
    });
    batch.push(EventLoopEvent::runnable_for_test(1));
    batch.push(EventLoopEvent::runnable_for_test(2));

    assert_eq!(batch.drain_before_poll_for_test().len(), 1);
    batch.push(EventLoopEvent::runnable_for_test(3));
    batch.begin_next_turn();

    let event = batch.pop_before_poll().expect("deferred tail event");
    let EventPayload::Runnable(runnable) = event.payload else {
        panic!("expected runnable event");
    };
    assert_eq!(runnable.key, RunnableKey::Test(2));

    batch.begin_next_turn();
    let event = batch.pop_before_poll().expect("later enqueued event");
    let EventPayload::Runnable(runnable) = event.payload else {
        panic!("expected runnable event");
    };
    assert_eq!(runnable.key, RunnableKey::Test(3));
}

#[test]
fn later_phases_progress_after_before_poll_budget_exhaustion() {
    let mut registry = TimeEventRegistry::default();
    let timer_id =
        registry.create_time_event(0_i64, Duration::ZERO, None, RuntimeTimeEvent::Test(1));
    let mut batch = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 1,
        max_same_owner_events_per_turn: 64,
    });
    batch.push(EventLoopEvent::runnable_for_test(1));
    batch.push(EventLoopEvent::runnable_for_test(2));
    batch.push(EventLoopEvent::readiness(
        EventPhase::Ready,
        EventLane::Normal,
        ReadinessEvent::readable(Token(20), ReadinessOrigin::Polled),
    ));
    batch.push_time(
        MonotonicInstant::from_millis(1),
        timer_event(timer_id, TimerEvent::ServerCron),
    );

    assert_eq!(batch.drain_before_poll_for_test().len(), 1);
    assert!(batch.pop_ready().is_some());
    assert!(batch.pop_time().is_some());
}

#[test]
#[should_panic(expected = "max_events_per_phase_per_turn must be greater than zero")]
fn zero_per_phase_event_limit_is_rejected() {
    let _ = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 0,
        max_same_owner_events_per_turn: 1,
    });
}

#[test]
#[should_panic(expected = "max_same_owner_events_per_turn must be greater than zero")]
fn zero_same_owner_event_limit_is_rejected() {
    let _ = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 1,
        max_same_owner_events_per_turn: 0,
    });
}

#[test]
fn same_owner_budget_defers_only_that_owner() {
    let mut registry = TimeEventRegistry::default();
    let repeated_id =
        registry.create_time_event(0_i64, Duration::ZERO, None, RuntimeTimeEvent::Test(1));
    let trailing_id =
        registry.create_time_event(0_i64, Duration::ZERO, None, RuntimeTimeEvent::Test(2));
    let mut batch = EventBatch::with_limits(EventLimits {
        max_events_per_phase_per_turn: 10,
        max_same_owner_events_per_turn: 1,
    });
    batch.push_time(
        MonotonicInstant::from_millis(1),
        timer_event(repeated_id, TimerEvent::ServerCron),
    );
    batch.push_time(
        MonotonicInstant::from_millis(2),
        timer_event(repeated_id, TimerEvent::ModuleTimer),
    );
    batch.push_time(
        MonotonicInstant::from_millis(3),
        timer_event(trailing_id, TimerEvent::EvictionContinuation),
    );

    assert_eq!(
        batch.pop_time().expect("first owner event").owner,
        EventOwner::Timer(repeated_id)
    );
    assert_eq!(
        batch.pop_time().expect("unrelated owner event").owner,
        EventOwner::Timer(trailing_id)
    );
    assert!(batch.pop_time().is_none());

    batch.begin_next_turn();
    assert_eq!(
        batch.pop_time().expect("retained owner event").owner,
        EventOwner::Timer(repeated_id)
    );
    assert!(batch.pop_time().is_none());
}
