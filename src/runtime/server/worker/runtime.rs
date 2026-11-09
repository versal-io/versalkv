use std::io;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::foundation::io::sys::time::{mstime, MonotonicInstant};
#[cfg(test)]
use crate::runtime::driver::RuntimeCapabilities;
use crate::runtime::driver::{
    RuntimeClock, RuntimeDispatch, RuntimeDriver, RuntimeDriverError, RuntimeInput, RuntimeInstant,
    RuntimeKernel, RuntimeMachine, RuntimeProgress, RuntimeWait,
};
use crate::runtime::server::message::{WorkerControl, WorkerId};
use crate::runtime::server::shared::ServerShared;
use crate::runtime::server::worker::event_loop::transport::RuntimeEvent;
use crate::runtime::server::worker::event_loop::{DispatchContext, LoopControl};
use crate::runtime::server::worker::ServerWorkerState;

pub(crate) struct ServerWorkerRuntime {
    id: WorkerId,
    shared: ServerShared,
    kernel: RuntimeKernel<ServerWorkerMachine>,
}

struct ServerWorkerMachine {
    runtime: ServerWorkerState,
    phase: WorkerPhase,
    ready_batch: Option<WorkerReadyBatch>,
    clock: RuntimeClock,
}

enum WorkerEvent {
    WorkerReadyBatch(WorkerReadyBatch),
}

struct WorkerReadyBatch {
    cycle_start: std::time::Instant,
    events: Vec<RuntimeEvent>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerPhase {
    BeforePoll,
    AwaitingReady,
    Ready,
    Time,
    Stopped,
}

impl ServerWorkerRuntime {
    pub(in crate::runtime::server) fn from_state(
        id: WorkerId,
        shared: ServerShared,
        runtime: ServerWorkerState,
    ) -> Self {
        debug_assert!(shared.contains(id));
        Self {
            id,
            shared,
            kernel: RuntimeKernel::with_default_limits(ServerWorkerMachine {
                runtime,
                phase: WorkerPhase::BeforePoll,
                ready_batch: None,
                clock: native_clock(),
            }),
        }
    }

    pub(in crate::runtime::server) const fn id(&self) -> WorkerId {
        self.id
    }

    pub(in crate::runtime::server) fn shares_topology_with(&self, shared: &ServerShared) -> bool {
        self.shared.shares_topology_with(shared)
    }

    pub(in crate::runtime::server) fn state(&self) -> &ServerWorkerState {
        &self.kernel.machine().runtime
    }

    pub(in crate::runtime::server) fn state_mut(&mut self) -> &mut ServerWorkerState {
        &mut self.kernel.machine_mut().runtime
    }

    #[cfg(test)]
    const fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::server()
    }

    pub fn run(&mut self) -> io::Result<()> {
        crate::server_log!(notice, "Event loop started");
        self.kernel.machine_mut().initialize();
        let mut input = control_input(WorkerControl::Wake);
        loop {
            let step = self
                .kernel
                .dispatch(RuntimeDispatch::new(native_clock(), input))
                .map_err(driver_error)?;
            let timeout = match step.wait {
                RuntimeWait::Immediate => Some(Duration::ZERO),
                RuntimeWait::Event { deadline } => deadline.map(deadline_timeout),
                RuntimeWait::Stopped => return Ok(()),
            };
            let ready = self.kernel.machine_mut().wait_ready(timeout)?;
            input = match ready {
                Some(ready) => RuntimeInput::External(WorkerEvent::WorkerReadyBatch(ready)),
                None => control_input(WorkerControl::Shutdown),
            };
        }
    }
}

impl Deref for ServerWorkerRuntime {
    type Target = ServerWorkerState;

    fn deref(&self) -> &Self::Target {
        self.state()
    }
}

impl DerefMut for ServerWorkerRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state_mut()
    }
}

impl RuntimeMachine for ServerWorkerMachine {
    type ExternalEvent = WorkerEvent;
    type Output = ();
    type Error = io::Error;

    fn accept(
        &mut self,
        dispatch: RuntimeDispatch<Self::ExternalEvent>,
    ) -> Result<(), Self::Error> {
        self.clock = dispatch.now;
        match dispatch.input {
            RuntimeInput::External(WorkerEvent::WorkerReadyBatch(batch)) => {
                if self.phase != WorkerPhase::AwaitingReady {
                    return Err(io::Error::other(
                        "runtime received readiness outside the wait phase",
                    ));
                }
                self.ready_batch = Some(batch);
                self.phase = WorkerPhase::Ready;
            }
            RuntimeInput::Wake => {}
            RuntimeInput::HostCompletion(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "server runtime has no pending owned host operation",
                ));
            }
            RuntimeInput::Shutdown => self.phase = WorkerPhase::Stopped,
        }
        Ok(())
    }

    fn advance(&mut self, now: RuntimeClock) -> Result<RuntimeProgress<Self::Output>, Self::Error> {
        self.clock = now;
        match self.phase {
            WorkerPhase::BeforePoll => {
                if self.runtime.run_before_poll_phase()? == LoopControl::Stop {
                    self.phase = WorkerPhase::Stopped;
                    return Ok(RuntimeProgress::Stopped);
                }
                self.phase = WorkerPhase::AwaitingReady;
                Ok(RuntimeProgress::Progress)
            }
            WorkerPhase::AwaitingReady => Ok(RuntimeProgress::Pending),
            WorkerPhase::Ready => {
                let batch = self
                    .ready_batch
                    .take()
                    .ok_or_else(|| io::Error::other("runtime ready phase has no event batch"))?;
                let cycle_cmd_start = self.runtime.ctx.stats().active_time_usec;
                let cycle_processed_start = self.runtime.ctx.stats().total_commands_processed;
                self.runtime.start_event_loop_cycle(
                    batch.cycle_start,
                    cycle_cmd_start,
                    cycle_processed_start,
                );
                self.runtime
                    .collect_after_poll_events(Some((batch.cycle_start, batch.events)));
                if self.runtime.ctx.cluster_state().is_some() {
                    // C accumulates failover work through file and time events,
                    // then resolves it in the next beforeSleep callback.
                    self.runtime.begin_cluster_bus_receive_batch();
                }
                if self.runtime.dispatch_ready_phase(DispatchContext::Normal)? == LoopControl::Stop
                {
                    self.runtime.finish_turn_pending_outputs();
                    self.phase = WorkerPhase::Stopped;
                    return Ok(RuntimeProgress::Stopped);
                }
                self.phase = WorkerPhase::Time;
                Ok(RuntimeProgress::Progress)
            }
            WorkerPhase::Time => {
                self.runtime.collect_due_time_events();
                let control = self.runtime.dispatch_time_phase()?;
                self.runtime.finish_turn_pending_outputs();
                if control == LoopControl::Stop {
                    self.phase = WorkerPhase::Stopped;
                    return Ok(RuntimeProgress::Stopped);
                }
                self.runtime.event_batch.begin_next_turn();
                self.runtime.writes.begin_turn();
                self.phase = WorkerPhase::BeforePoll;
                Ok(RuntimeProgress::Progress)
            }
            WorkerPhase::Stopped => Ok(RuntimeProgress::Stopped),
        }
    }

    fn next_deadline(&self, now: RuntimeClock) -> Option<RuntimeInstant> {
        self.runtime
            .next_poll_timeout(now)
            .map(|delay| now.deadline_after(delay))
    }

    fn has_immediate_work(&self) -> bool {
        match self.phase {
            WorkerPhase::BeforePoll | WorkerPhase::Ready | WorkerPhase::Time => true,
            WorkerPhase::AwaitingReady => self
                .runtime
                .next_poll_timeout(self.clock)
                .is_some_and(|timeout| timeout.is_zero()),
            WorkerPhase::Stopped => false,
        }
    }

    fn is_stopped(&self) -> bool {
        self.phase == WorkerPhase::Stopped
    }
}

impl ServerWorkerMachine {
    fn initialize(&mut self) {
        self.runtime.register_server_cron();
        self.runtime.writes.begin_turn();
    }

    fn wait_ready(&mut self, timeout: Option<Duration>) -> io::Result<Option<WorkerReadyBatch>> {
        self.runtime.poll_ready_with_timeout(timeout).map(|result| {
            result.map(|(cycle_start, events)| WorkerReadyBatch {
                cycle_start,
                events,
            })
        })
    }
}

fn native_clock() -> RuntimeClock {
    RuntimeClock::from_micros(MonotonicInstant::now().as_micros(), mstime())
}

fn control_input(control: WorkerControl) -> RuntimeInput<WorkerEvent> {
    match control {
        WorkerControl::Wake => RuntimeInput::Wake,
        WorkerControl::Shutdown => RuntimeInput::Shutdown,
    }
}

fn deadline_timeout(deadline: RuntimeInstant) -> Duration {
    deadline.duration_until(native_clock().monotonic())
}

fn driver_error(error: RuntimeDriverError<io::Error>) -> io::Error {
    match error {
        RuntimeDriverError::Machine(error) => error,
        RuntimeDriverError::Stopped => io::Error::other("runtime driver stopped before dispatch"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_runtime_exposes_native_capabilities() {
        let shared = ServerShared::single_worker();
        let runtime = ServerWorkerRuntime::from_state(
            WorkerId::PRIMARY,
            shared,
            ServerWorkerState::new(1).unwrap(),
        );
        assert_eq!(runtime.capabilities(), RuntimeCapabilities::server());
    }

    #[test]
    fn machine_prepares_wait_without_blocking() {
        let mut machine = ServerWorkerMachine {
            runtime: ServerWorkerState::new(1).unwrap(),
            phase: WorkerPhase::BeforePoll,
            ready_batch: None,
            clock: RuntimeClock::new(1_000, 1_000),
        };
        machine.initialize();
        machine
            .accept(RuntimeDispatch::new(
                RuntimeClock::new(1_000, 1_000),
                RuntimeInput::Wake,
            ))
            .unwrap();
        assert_eq!(
            machine.advance(RuntimeClock::new(1_000, 1_000)).unwrap(),
            RuntimeProgress::Progress
        );
        assert_eq!(machine.phase, WorkerPhase::AwaitingReady);
    }

    #[test]
    fn cluster_bus_receive_batch_spans_ready_and_time_until_before_poll() {
        let mut machine = ServerWorkerMachine {
            runtime: ServerWorkerState::new(1).unwrap(),
            phase: WorkerPhase::Ready,
            ready_batch: Some(WorkerReadyBatch {
                cycle_start: std::time::Instant::now(),
                events: Vec::new(),
            }),
            clock: RuntimeClock::new(1_000, 1_000),
        };
        *machine.runtime.ctx.cluster_state_mut() = Some(Box::new(
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379),
        ));

        assert_eq!(
            machine.advance(RuntimeClock::new(1_000, 1_000)).unwrap(),
            RuntimeProgress::Progress
        );
        assert_eq!(
            machine.runtime.cluster_bus_receive_batch_depth, 1,
            "Ready-phase election results must wait for same-turn maintenance reads"
        );

        assert_eq!(
            machine.advance(RuntimeClock::new(1_001, 1_001)).unwrap(),
            RuntimeProgress::Progress
        );
        assert_eq!(machine.phase, WorkerPhase::BeforePoll);
        assert_eq!(machine.runtime.cluster_bus_receive_batch_depth, 1);

        assert_eq!(
            machine.advance(RuntimeClock::new(1_002, 1_002)).unwrap(),
            RuntimeProgress::Progress
        );
        assert_eq!(machine.runtime.cluster_bus_receive_batch_depth, 0);
    }

    #[test]
    fn native_machine_rejects_unowned_host_completion() {
        let mut machine = ServerWorkerMachine {
            runtime: ServerWorkerState::new(1).unwrap(),
            phase: WorkerPhase::AwaitingReady,
            ready_batch: None,
            clock: RuntimeClock::new(1_000, 1_000),
        };
        let result = machine.accept(RuntimeDispatch::new(
            RuntimeClock::new(1_000, 1_000),
            RuntimeInput::HostCompletion(crate::runtime::driver::OwnedHostCompletion {
                operation: crate::runtime::driver::HostOperationId::from_u64(1),
                result: crate::runtime::driver::HostOperationResult::Ack,
            }),
        ));

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Unsupported);
    }
}
