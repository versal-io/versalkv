use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;

use super::{
    ContinuationRegistry, ExternalEventKey, RuntimeClock, RuntimeConfigurationError,
    RuntimeDispatch, RuntimeDriver, RuntimeDriverError, RuntimeInput, RuntimeInstant,
    RuntimeKernel, RuntimeMachine, RuntimeProgress, RuntimeTurnLimits, RuntimeWait,
    SuspendedCommand, WaitCondition,
};
use crate::engine::command::SessionId;
use bytes::Bytes;

#[derive(Default)]
struct TestMachine {
    queued: VecDeque<u64>,
    deadline: Option<RuntimeInstant>,
    stopped: bool,
    progress_before_output: usize,
}

impl RuntimeMachine for TestMachine {
    type ExternalEvent = Vec<u64>;
    type Output = u64;
    type Error = Infallible;

    fn accept(
        &mut self,
        dispatch: RuntimeDispatch<Self::ExternalEvent>,
    ) -> Result<(), Self::Error> {
        match dispatch.input {
            RuntimeInput::External(values) => self.queued.extend(values),
            RuntimeInput::Wake => {}
            RuntimeInput::Shutdown => self.stopped = true,
        }
        Ok(())
    }

    fn advance(
        &mut self,
        _now: RuntimeClock,
    ) -> Result<RuntimeProgress<Self::Output>, Self::Error> {
        if self.stopped {
            return Ok(RuntimeProgress::Stopped);
        }
        if self.progress_before_output > 0 {
            self.progress_before_output -= 1;
            return Ok(RuntimeProgress::Progress);
        }
        Ok(self
            .queued
            .pop_front()
            .map(RuntimeProgress::Output)
            .unwrap_or(RuntimeProgress::Pending))
    }

    fn next_deadline(&self, _now: RuntimeClock) -> Option<RuntimeInstant> {
        self.deadline
    }

    fn has_immediate_work(&self) -> bool {
        self.progress_before_output > 0 || !self.queued.is_empty()
    }

    fn is_stopped(&self) -> bool {
        self.stopped
    }
}

fn clock() -> RuntimeClock {
    RuntimeClock::new(100, 1_000)
}

#[test]
fn clock_keeps_wall_and_monotonic_domains_separate() {
    let clock = clock();
    assert_eq!(clock.monotonic(), RuntimeInstant::from_millis(100));
    assert_eq!(clock.unix_ms(), 1_000);
    assert_eq!(
        clock.deadline_after(Duration::from_millis(25)),
        RuntimeInstant::from_millis(125)
    );
    assert_eq!(
        clock.deadline_for_unix_ms(1_025),
        RuntimeInstant::from_millis(125)
    );
}

#[test]
fn past_wall_deadline_is_immediately_due_in_monotonic_domain() {
    assert_eq!(
        clock().deadline_for_unix_ms(900),
        RuntimeInstant::from_millis(100)
    );
}

#[test]
fn established_monotonic_deadline_ignores_wall_clock_jump() {
    let deadline = RuntimeClock::new(100, 1_000).deadline_after(Duration::from_millis(25));
    let jumped_wall = RuntimeClock::new(110, -50_000);

    assert_eq!(
        deadline.duration_until(jumped_wall.monotonic()),
        Duration::from_millis(15)
    );
}

#[test]
fn deadline_conversion_saturates_on_overflow() {
    let clock = RuntimeClock::from_micros(u64::MAX - 5, i64::MAX - 1);
    assert_eq!(
        clock.deadline_after(Duration::from_micros(10)),
        RuntimeInstant::from_micros(u64::MAX)
    );
    assert_eq!(
        clock.deadline_for_unix_ms(i64::MAX),
        RuntimeInstant::from_micros(u64::MAX)
    );
}

#[test]
fn sub_millisecond_native_deadline_keeps_microsecond_precision() {
    let clock = RuntimeClock::from_micros(100_500, 1_000);
    let deadline = clock.deadline_after(Duration::from_micros(750));
    assert_eq!(deadline, RuntimeInstant::from_micros(101_250));
    assert_eq!(
        deadline.duration_until(clock.monotonic()),
        Duration::from_micros(750)
    );
}

#[test]
fn kernel_preserves_output_order() {
    let mut kernel = RuntimeKernel::with_default_limits(TestMachine::default());
    let step = kernel
        .dispatch(RuntimeDispatch::new(
            clock(),
            RuntimeInput::External(vec![3, 1, 2]),
        ))
        .unwrap();
    assert_eq!(step.outputs, vec![3, 1, 2]);
    assert_eq!(step.wait, RuntimeWait::Event { deadline: None });
}

#[test]
fn retained_work_returns_immediate_after_quantum_budget() {
    let limits = RuntimeTurnLimits {
        max_quanta_per_turn: 2,
        max_outputs_per_turn: 8,
    };
    let mut kernel = RuntimeKernel::new(TestMachine::default(), limits).unwrap();
    let first = kernel
        .dispatch(RuntimeDispatch::new(
            clock(),
            RuntimeInput::External(vec![1, 2, 3]),
        ))
        .unwrap();
    assert_eq!(first.outputs, vec![1, 2]);
    assert_eq!(first.wait, RuntimeWait::Immediate);

    let second = kernel
        .dispatch(RuntimeDispatch::new(clock(), RuntimeInput::Wake))
        .unwrap();
    assert_eq!(second.outputs, vec![3]);
    assert_eq!(second.wait, RuntimeWait::Event { deadline: None });
}

#[test]
fn retained_work_returns_immediate_after_output_budget() {
    let limits = RuntimeTurnLimits {
        max_quanta_per_turn: 8,
        max_outputs_per_turn: 1,
    };
    let mut kernel = RuntimeKernel::new(TestMachine::default(), limits).unwrap();
    let step = kernel
        .dispatch(RuntimeDispatch::new(
            clock(),
            RuntimeInput::External(vec![1, 2]),
        ))
        .unwrap();
    assert_eq!(step.outputs, vec![1]);
    assert_eq!(step.wait, RuntimeWait::Immediate);
}

#[test]
fn pending_machine_returns_its_earliest_deadline() {
    let machine = TestMachine {
        deadline: Some(RuntimeInstant::from_millis(150)),
        ..TestMachine::default()
    };
    let mut kernel = RuntimeKernel::with_default_limits(machine);
    let step = kernel
        .dispatch(RuntimeDispatch::new(clock(), RuntimeInput::Wake))
        .unwrap();
    assert_eq!(
        step.wait,
        RuntimeWait::Event {
            deadline: Some(RuntimeInstant::from_millis(150))
        }
    );
}

#[test]
fn host_event_preempts_a_future_deadline() {
    let machine = TestMachine {
        deadline: Some(RuntimeInstant::from_millis(150)),
        ..TestMachine::default()
    };
    let mut kernel = RuntimeKernel::with_default_limits(machine);
    let pending = kernel
        .dispatch(RuntimeDispatch::new(clock(), RuntimeInput::Wake))
        .unwrap();
    assert_eq!(
        pending.wait,
        RuntimeWait::Event {
            deadline: Some(RuntimeInstant::from_millis(150))
        }
    );

    let preempted = kernel
        .dispatch(RuntimeDispatch::new(
            RuntimeClock::new(110, 1_010),
            RuntimeInput::External(vec![7]),
        ))
        .unwrap();
    assert_eq!(preempted.outputs, vec![7]);
    assert_eq!(
        preempted.wait,
        RuntimeWait::Event {
            deadline: Some(RuntimeInstant::from_millis(150))
        }
    );
}

#[test]
fn progress_without_output_is_bounded() {
    let machine = TestMachine {
        progress_before_output: 3,
        queued: VecDeque::from([9]),
        ..TestMachine::default()
    };
    let limits = RuntimeTurnLimits {
        max_quanta_per_turn: 2,
        max_outputs_per_turn: 2,
    };
    let mut kernel = RuntimeKernel::new(machine, limits).unwrap();
    let step = kernel
        .dispatch(RuntimeDispatch::new(clock(), RuntimeInput::Wake))
        .unwrap();
    assert!(step.outputs.is_empty());
    assert_eq!(step.wait, RuntimeWait::Immediate);
}

#[test]
fn shutdown_stops_and_rejects_later_dispatch() {
    let mut kernel = RuntimeKernel::with_default_limits(TestMachine::default());
    let stopped = kernel
        .dispatch(RuntimeDispatch::new(clock(), RuntimeInput::Shutdown))
        .unwrap();
    assert_eq!(stopped.wait, RuntimeWait::Stopped);
    assert_eq!(
        kernel.dispatch(RuntimeDispatch::new(clock(), RuntimeInput::Wake)),
        Err(RuntimeDriverError::Stopped)
    );
}

#[test]
fn zero_budgets_are_rejected() {
    assert!(matches!(
        RuntimeKernel::new(
            TestMachine::default(),
            RuntimeTurnLimits {
                max_quanta_per_turn: 0,
                max_outputs_per_turn: 1,
            }
        ),
        Err(RuntimeConfigurationError::ZeroQuantumBudget)
    ));
    assert!(matches!(
        RuntimeKernel::new(
            TestMachine::default(),
            RuntimeTurnLimits {
                max_quanta_per_turn: 1,
                max_outputs_per_turn: 0,
            }
        ),
        Err(RuntimeConfigurationError::ZeroOutputBudget)
    ));
}

#[test]
fn continuation_registry_resumes_exactly_once() {
    let mut registry = ContinuationRegistry::default();
    let id = registry.register(
        SessionId(7),
        WaitCondition::Keys {
            db: 0,
            keys: vec![Bytes::from_static(b"key")],
        },
        "state",
    );
    assert_eq!(registry.len(), 1);
    assert_eq!(registry.get(id).unwrap().session, SessionId(7));
    assert_eq!(registry.take(id).unwrap().state, "state");
    assert!(registry.take(id).is_none());
    assert!(registry.is_empty());
}

#[test]
fn continuation_registry_cancels_only_one_session() {
    let mut registry = ContinuationRegistry::default();
    registry.register(
        SessionId(1),
        WaitCondition::External(ExternalEventKey::from_u64(9)),
        1,
    );
    let retained = registry.register(
        SessionId(2),
        WaitCondition::External(ExternalEventKey::from_u64(10)),
        2,
    );
    let cancelled = registry.cancel_session(SessionId(1));
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].session, SessionId(1));
    assert_eq!(registry.get(retained).unwrap().state, 2);
}

#[test]
fn continuation_registry_signals_owned_waits_in_registration_order() {
    let mut registry = ContinuationRegistry::default();
    registry.register(
        SessionId(1),
        WaitCondition::Keys {
            db: 0,
            keys: vec![Bytes::from_static(b"key")],
        },
        1,
    );
    registry.register(
        SessionId(2),
        WaitCondition::Keys {
            db: 0,
            keys: vec![Bytes::from_static(b"key")],
        },
        2,
    );
    registry.register(
        SessionId(3),
        WaitCondition::External(ExternalEventKey::from_u64(9)),
        3,
    );

    let signaled = registry.signal_key(0, b"key");
    assert_eq!(
        signaled
            .into_iter()
            .map(|entry| entry.state)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        registry.signal_external(ExternalEventKey::from_u64(9))[0].state,
        3
    );
    assert!(registry.is_empty());
}

#[test]
fn continuation_registry_times_out_only_due_waits() {
    let mut registry = ContinuationRegistry::default();
    registry.register(
        SessionId(1),
        WaitCondition::Deadline(RuntimeInstant::from_millis(100)),
        1,
    );
    let future = registry.register(
        SessionId(2),
        WaitCondition::Deadline(RuntimeInstant::from_millis(200)),
        2,
    );

    let due = registry.take_due(RuntimeInstant::from_millis(150));
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].state, 1);
    assert_eq!(registry.get(future).unwrap().state, 2);
}

#[test]
fn suspended_command_is_fully_owned() {
    let mut registry = ContinuationRegistry::default();
    let id = registry.register(
        SessionId(5),
        WaitCondition::Deadline(RuntimeInstant::from_millis(1_200)),
        Bytes::from_static(b"owned"),
    );
    let SuspendedCommand { state, wait, .. } = registry.take(id).unwrap();
    assert_eq!(state, Bytes::from_static(b"owned"));
    assert_eq!(
        wait,
        WaitCondition::Deadline(RuntimeInstant::from_millis(1_200))
    );
}
