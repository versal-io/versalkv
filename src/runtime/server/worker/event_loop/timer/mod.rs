use std::collections::HashMap;
use std::time::Duration;

use crate::foundation::io::sys::time::MonotonicInstant;
use crate::runtime::server::worker::event_loop::TimerEvent;
use crate::runtime::server::worker::ServerWorkerState;

pub(crate) trait EventClock: Copy {
    fn monotonic(self) -> MonotonicInstant;
    fn wall_ms(self) -> i64;
}

impl EventClock for MonotonicInstant {
    fn monotonic(self) -> MonotonicInstant {
        self
    }

    fn wall_ms(self) -> i64 {
        crate::foundation::io::sys::time::mstime()
    }
}

impl EventClock for i64 {
    fn monotonic(self) -> MonotonicInstant {
        MonotonicInstant::from_millis(self.max(0) as u64)
    }

    fn wall_ms(self) -> i64 {
        self
    }
}

impl EventClock for crate::runtime::driver::RuntimeClock {
    fn monotonic(self) -> MonotonicInstant {
        MonotonicInstant::from_micros(self.monotonic().as_micros())
    }

    fn wall_ms(self) -> i64 {
        self.unix_ms()
    }
}

pub(crate) type RuntimeTimeEvent = TimerEvent;

struct TimeEvent {
    deadline: MonotonicInstant,
    interval: Option<Duration>,
    kind: RuntimeTimeEvent,
}

#[derive(Default)]
pub(crate) struct TimeEventRegistry {
    events: HashMap<TimeEventId, TimeEvent>,
    next_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TimeEventId(u64);

pub(crate) struct DueTimeEvent {
    pub(crate) id: TimeEventId,
    pub(crate) kind: RuntimeTimeEvent,
    interval: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// These actions are part of the generic registration contract; not every
// runtime domain uses every action yet.
#[allow(dead_code)]
pub(crate) enum TimeEventAction {
    Delete,
    RescheduleAfter(Duration),
    Repeat,
}

impl TimeEventRegistry {
    pub(crate) fn create_time_event<C: EventClock>(
        &mut self,
        now: C,
        delay: Duration,
        interval: Option<Duration>,
        kind: RuntimeTimeEvent,
    ) -> TimeEventId {
        self.next_id = self.next_id.saturating_add(1).max(1);
        let id = TimeEventId(self.next_id);
        let interval = interval.filter(|duration| !duration.is_zero());
        let now = now.monotonic();
        self.events.insert(
            id,
            TimeEvent {
                deadline: now.saturating_add(delay),
                interval,
                kind,
            },
        );
        id
    }

    pub(crate) fn delete_time_event(&mut self, id: TimeEventId) -> bool {
        self.events.remove(&id).is_some()
    }

    pub(crate) fn delete_kind(&mut self, kind: TimerEvent) -> usize {
        let before = self.events.len();
        self.events.retain(|_, event| event.kind != kind);
        before.saturating_sub(self.events.len())
    }

    pub(crate) fn schedule_unique<C: EventClock>(
        &mut self,
        now: C,
        delay: Duration,
        kind: TimerEvent,
    ) -> TimeEventId {
        if let Some((&id, _)) = self.events.iter().find(|(_, event)| event.kind == kind) {
            return id;
        }
        self.create_time_event(now, delay, None, kind)
    }

    #[cfg(test)]
    pub(crate) fn has_due_kind(&self, kind: TimerEvent) -> bool {
        let now = MonotonicInstant::now();
        self.events
            .values()
            .any(|event| event.kind == kind && event.deadline <= now)
    }

    pub(crate) fn next_timeout<C: EventClock>(&self, now: C) -> Option<Duration> {
        let now = now.monotonic();
        self.events
            .values()
            .map(|event| event.deadline.duration_until(now))
            .min()
    }

    pub(crate) fn take_due<C: EventClock>(&mut self, now: C) -> Vec<DueTimeEvent> {
        let now = now.monotonic();
        let mut due: Vec<_> = self
            .events
            .iter()
            .filter_map(|(&id, event)| (event.deadline <= now).then_some((event.deadline, id)))
            .collect();
        due.sort_unstable();

        due.into_iter()
            .filter_map(|(_, id)| {
                self.events.remove(&id).map(|event| DueTimeEvent {
                    id,
                    kind: event.kind,
                    interval: event.interval,
                })
            })
            .collect()
    }

    pub(crate) fn complete<C: EventClock>(
        &mut self,
        event: DueTimeEvent,
        completion_time: C,
        action: TimeEventAction,
    ) {
        let completion_time = completion_time.monotonic();
        let deadline = match action {
            TimeEventAction::Delete => return,
            TimeEventAction::RescheduleAfter(delay) => completion_time.saturating_add(delay),
            TimeEventAction::Repeat => {
                let Some(interval) = event.interval else {
                    return;
                };
                completion_time.saturating_add(interval)
            }
        };
        self.events.insert(
            event.id,
            TimeEvent {
                deadline,
                interval: event.interval,
                kind: event.kind,
            },
        );
    }
}

fn min_poll_deadline(first: Option<Duration>, second: Option<Duration>) -> Option<Duration> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.min(second)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

fn choose_poll_timeout(
    has_immediate_work: bool,
    timer_deadline: Option<Duration>,
    blocked_deadline: Option<Duration>,
) -> Option<Duration> {
    if has_immediate_work {
        Some(Duration::ZERO)
    } else {
        min_poll_deadline(timer_deadline, blocked_deadline)
    }
}

impl ServerWorkerState {
    pub(crate) fn next_poll_timeout<C: EventClock>(&self, now: C) -> Option<Duration> {
        let now_ms = now.wall_ms();
        let has_immediate_work = self.transport.has_pending_data()
            || self.event_batch.has_pending_transport_data()
            || self.event_batch.has_due_time_events()
            // Keep the loop spinning while a forkless capture is active so the
            // before-poll phase can refill the feed every turn instead of
            // blocking for the full cron interval.
            || self.ctx.persistence().forkless_capture_in_progress();
        let blocked_deadline = (self.ctx.blocking_registry().count() > 0)
            .then(|| self.next_blocked_timeout(now_ms))
            .flatten();
        choose_poll_timeout(
            has_immediate_work,
            self.time_events.next_timeout(now),
            blocked_deadline,
        )
    }

    pub(crate) fn schedule_eviction(&mut self) {
        self.ctx.set_needs_eviction(false);
        let kind = TimerEvent::EvictionContinuation;
        if self
            .pending_due_time_events
            .values()
            .any(|event| event.kind == kind)
            || self.event_batch.contains_timer(kind)
        {
            return;
        }
        self.time_events
            .schedule_unique(MonotonicInstant::now(), Duration::ZERO, kind);
    }

    pub(crate) fn cancel_eviction_timer(&mut self) {
        self.ctx.set_needs_eviction(false);
        self.time_events
            .delete_kind(TimerEvent::EvictionContinuation);
        let pending_ids: Vec<_> = self
            .pending_due_time_events
            .iter()
            .filter_map(|(&id, event)| {
                (event.kind == TimerEvent::EvictionContinuation).then_some(id)
            })
            .collect();
        for id in pending_ids {
            self.pending_due_time_events.remove(&id);
            self.event_batch.remove_timer(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::sys::time::MonotonicInstant;

    #[test]
    fn one_shot_event_is_taken_once_and_deleted() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(100);
        registry.create_time_event(
            origin,
            Duration::from_millis(5),
            None,
            RuntimeTimeEvent::Test(1),
        );
        assert!(registry
            .take_due(origin.saturating_add(Duration::from_millis(4)))
            .is_empty());
        let event = registry
            .take_due(origin.saturating_add(Duration::from_millis(5)))
            .pop()
            .expect("event is due");
        assert_eq!(event.kind, RuntimeTimeEvent::Test(1));
        registry.complete(
            event,
            origin.saturating_add(Duration::from_millis(5)),
            TimeEventAction::Delete,
        );
        assert!(registry
            .take_due(origin.saturating_add(Duration::from_millis(100)))
            .is_empty());
        assert!(registry
            .next_timeout(origin.saturating_add(Duration::from_millis(100)))
            .is_none());
    }

    #[test]
    fn periodic_event_reschedules_after_fire() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(0);
        registry.create_time_event(
            origin,
            Duration::from_millis(10),
            Some(Duration::from_millis(20)),
            RuntimeTimeEvent::Test(1),
        );
        assert!(registry
            .take_due(origin.saturating_add(Duration::from_millis(9)))
            .is_empty());
        let event = registry
            .take_due(origin.saturating_add(Duration::from_millis(10)))
            .pop()
            .expect("periodic event is due");
        registry.complete(
            event,
            origin.saturating_add(Duration::from_millis(10)),
            TimeEventAction::Repeat,
        );
        assert_eq!(
            registry.next_timeout(origin.saturating_add(Duration::from_millis(10))),
            Some(Duration::from_millis(20))
        );
        assert_eq!(
            registry
                .take_due(origin.saturating_add(Duration::from_millis(30)))
                .len(),
            1
        );
    }

    #[test]
    fn delete_event_returns_false_after_first_delete() {
        let mut registry = TimeEventRegistry::default();
        let id = registry.create_time_event(
            MonotonicInstant::now(),
            Duration::ZERO,
            None,
            RuntimeTimeEvent::Test(1),
        );
        assert!(registry.delete_time_event(id));
        assert!(!registry.delete_time_event(id));
    }

    #[test]
    fn equal_deadlines_execute_in_id_order() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(0);
        registry.create_time_event(origin, Duration::ZERO, None, RuntimeTimeEvent::Test(1));
        registry.create_time_event(origin, Duration::ZERO, None, RuntimeTimeEvent::Test(2));
        let order: Vec<_> = registry
            .take_due(origin)
            .into_iter()
            .map(|event| event.kind)
            .collect();
        assert_eq!(
            order,
            [RuntimeTimeEvent::Test(1), RuntimeTimeEvent::Test(2)]
        );
    }

    #[test]
    fn next_timeout_returns_zero_for_due_event() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(0);
        registry.create_time_event(origin, Duration::ZERO, None, RuntimeTimeEvent::Test(1));
        assert_eq!(registry.next_timeout(origin), Some(Duration::ZERO));
    }

    #[test]
    fn next_timeout_returns_remaining_duration_for_future_event() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(100);
        registry.create_time_event(
            origin,
            Duration::from_millis(25),
            None,
            RuntimeTimeEvent::Test(1),
        );
        assert_eq!(
            registry.next_timeout(origin.saturating_add(Duration::from_millis(10))),
            Some(Duration::from_millis(15))
        );
    }

    #[test]
    fn zero_delay_event_registered_by_callback_is_deferred() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(0);
        registry.create_time_event(origin, Duration::ZERO, None, RuntimeTimeEvent::Test(1));
        let current = registry.take_due(origin);
        registry.create_time_event(origin, Duration::ZERO, None, RuntimeTimeEvent::Test(2));
        registry.complete(
            current.into_iter().next().unwrap(),
            origin,
            TimeEventAction::Delete,
        );
        assert_eq!(
            registry.take_due(origin)[0].kind,
            RuntimeTimeEvent::Test(2),
            "events registered after take_due are deferred to the next batch"
        );
    }

    #[test]
    fn reschedule_after_updates_deadline() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(0);
        registry.create_time_event(origin, Duration::ZERO, None, RuntimeTimeEvent::Test(1));
        let event = registry.take_due(origin).pop().unwrap();
        registry.complete(
            event,
            origin.saturating_add(Duration::from_millis(40)),
            TimeEventAction::RescheduleAfter(Duration::from_millis(25)),
        );
        assert_eq!(
            registry.next_timeout(origin.saturating_add(Duration::from_millis(40))),
            Some(Duration::from_millis(25))
        );
    }

    #[test]
    fn late_periodic_event_skips_unbounded_catch_up() {
        let mut registry = TimeEventRegistry::default();
        let origin = MonotonicInstant::from_millis(0);
        registry.create_time_event(
            origin,
            Duration::from_millis(10),
            Some(Duration::from_millis(10)),
            RuntimeTimeEvent::Test(1),
        );
        let completion = origin.saturating_add(Duration::from_secs(1));
        let event = registry.take_due(completion).pop().unwrap();
        registry.complete(event, completion, TimeEventAction::Repeat);
        assert_eq!(
            registry.next_timeout(completion),
            Some(Duration::from_millis(10))
        );
    }
}

#[cfg(test)]
mod poll_deadline_tests {
    use super::{choose_poll_timeout, min_poll_deadline, RuntimeTimeEvent};
    use crate::engine::command::context::{BlockedCmd, BlockedState};
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::sys::time::MonotonicInstant;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::ServerWorkerState;
    use bytes::Bytes;
    use std::time::Duration;

    fn empty_slot_snapshot_relay(
    ) -> crate::runtime::server::worker::cluster::migration::SlotSnapshotRelay {
        crate::runtime::server::worker::cluster::migration::SlotSnapshotRelay {
            job_id: crate::engine::distributed::cluster::migration::MigrationJobId::parse(
                b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .expect("valid migration id"),
            pid: i32::MAX,
            source: None,
            completion_state:
                crate::runtime::server::worker::cluster::migration::SlotSnapshotCompletionState::default(),
        }
    }

    #[test]
    fn earliest_deadline_prefers_the_shorter_source() {
        assert_eq!(
            min_poll_deadline(
                Some(Duration::from_millis(100)),
                Some(Duration::from_millis(25)),
            ),
            Some(Duration::from_millis(25)),
        );
    }

    #[test]
    fn no_deadline_allows_an_indefinite_poll() {
        assert_eq!(min_poll_deadline(None, None), None);
    }

    #[test]
    fn immediate_work_returns_zero_without_consulting_deadlines() {
        assert_eq!(
            choose_poll_timeout(
                true,
                Some(Duration::from_millis(100)),
                Some(Duration::from_millis(25)),
            ),
            Some(Duration::ZERO),
        );
    }

    #[test]
    fn due_eviction_timer_returns_zero_poll_timeout() {
        let mut runtime = ServerWorkerState::new(1).expect("create event loop runtime");
        runtime.schedule_eviction();

        assert_eq!(
            runtime.next_poll_timeout(MonotonicInstant::now()),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn transport_pending_data_forces_zero_timeout_until_drained() {
        let mut runtime = ServerWorkerState::new(1).expect("create event loop runtime");
        runtime.time_events.create_time_event(
            1_000,
            Duration::from_millis(100),
            None,
            RuntimeTimeEvent::Test(0),
        );
        runtime
            .transport
            .queue_test_pending_data_event(Token(usize::MAX));

        assert_eq!(runtime.next_poll_timeout(1_000), Some(Duration::ZERO));

        runtime.transport.process_pending_data();
        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100))
        );
    }

    fn runtime_with_blocked(timeout_ms: u64, start_time_ms: i64) -> ServerWorkerState {
        let mut runtime = ServerWorkerState::new(1).expect("create event loop runtime");
        let mut connection = Connection::new_fake();
        connection.blocked = Some(BlockedState {
            cmd: BlockedCmd::Blpop,
            keys: vec![Bytes::from_static(b"key")],
            timeout_ms,
            start_time_ms,
            orig_args: CommandArgs::from_parts(vec![Bytes::from_static(b"BLPOP")]),
            req_bytes: 0,
            orig_wrapped: None,
        });
        let _token_value = connection.token().0;
        let _connection_token = runtime.connection_registry.insert(connection);
        runtime
    }

    #[test]
    fn blocked_deadline_returns_remaining_due_and_infinite_states() {
        assert_eq!(
            runtime_with_blocked(1_000, 9_500).next_blocked_timeout(10_000),
            Some(Duration::from_millis(500)),
        );
        assert_eq!(
            runtime_with_blocked(1_000, 9_000).next_blocked_timeout(10_000),
            Some(Duration::ZERO),
        );
        assert_eq!(
            runtime_with_blocked(0, 9_000).next_blocked_timeout(10_000),
            None,
        );
    }

    #[test]
    fn runtime_poll_timeout_ignores_before_poll_runnables() {
        let mut runtime = ServerWorkerState::new(1).expect("create event loop runtime");
        runtime.time_events.create_time_event(
            1_000,
            Duration::from_millis(100),
            None,
            RuntimeTimeEvent::Test(0),
        );
        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100)),
        );

        runtime.schedule_runtime_runnable(
            crate::runtime::server::worker::event_loop::RunnableKey::ClientRead(Token(1)),
        );
        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100))
        );

        runtime.schedule_runtime_runnable_next_turn(
            crate::runtime::server::worker::event_loop::RunnableKey::ClientWrite(Token(4)),
        );
        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100)),
            "next-turn ordinary work must not affect the current poll deadline"
        );

        runtime.schedule_runtime_runnable_next_turn(
            crate::runtime::server::worker::event_loop::RunnableKey::SlotExportWrite(Token(3)),
        );
        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100)),
            "next-turn slot-export work must not affect the current poll deadline"
        );
    }

    #[test]
    fn blocked_hup_continuation_does_not_force_busy_polling() {
        let mut runtime = runtime_with_blocked(0, 1_000);
        runtime.client_hup_drains.insert(Token(usize::MAX));
        runtime.schedule_runtime_runnable_next_turn(
            crate::runtime::server::worker::event_loop::RunnableKey::ClientRead(Token(usize::MAX)),
        );
        runtime.time_events.create_time_event(
            1_000,
            Duration::from_millis(100),
            None,
            RuntimeTimeEvent::Test(0),
        );

        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn budget_yielded_blocked_normal_read_does_not_couple_poll_timeout() {
        let mut runtime = runtime_with_blocked(0, 1_000);
        runtime.schedule_runtime_runnable_next_turn(
            crate::runtime::server::worker::event_loop::RunnableKey::ClientRead(Token(usize::MAX)),
        );
        runtime.time_events.create_time_event(
            1_000,
            Duration::from_millis(100),
            None,
            RuntimeTimeEvent::Test(0),
        );

        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn runtime_poll_timeout_ignores_unregistered_blocked_state() {
        let mut runtime = runtime_with_blocked(25, 1_000);
        runtime.time_events.create_time_event(
            1_000,
            Duration::from_millis(100),
            None,
            RuntimeTimeEvent::Test(0),
        );

        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100)),
        );

        runtime.ctx.blocking_registry_mut().increment_count();
        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(25))
        );
    }

    #[test]
    fn active_runtime_source_does_not_cap_poll_deadline() {
        let mut runtime = ServerWorkerState::new(1).expect("create event loop runtime");
        runtime.time_events.create_time_event(
            1_000,
            Duration::from_millis(100),
            None,
            RuntimeTimeEvent::Test(0),
        );
        runtime.slot_migration_snapshot = Some(empty_slot_snapshot_relay());

        assert_eq!(
            runtime.next_poll_timeout(1_000),
            Some(Duration::from_millis(100)),
        );
    }
}
