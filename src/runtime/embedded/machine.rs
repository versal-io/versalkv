use crate::framework::commands::command_runtime::EmbeddedCommandRuntime;
use crate::runtime::driver::{
    CommandEffectSink, CommandTransition, RuntimeCapabilities, RuntimeClock, RuntimeDispatch,
    RuntimeInput, RuntimeInstant, RuntimeMachine, RuntimeProgress,
};

use super::effects::EmbeddedEffectSink;
use super::output::EmbeddedOutput;
use super::session::EmbeddedEvent;
use super::state::EmbeddedState;
use super::EmbeddedRuntimeError;

#[derive(Default)]
pub(super) struct EmbeddedRuntimeMachine {
    state: EmbeddedState,
}

impl EmbeddedRuntimeMachine {
    pub(super) const fn capabilities(&self) -> RuntimeCapabilities {
        self.state.capabilities
    }

    /// Enable durable persistence so completed write commands are relayed to
    /// the storage host before their reply is released. The host calls this
    /// once the AOF I/O worker is attached (design §6.3).
    pub(super) fn enable_persistence(&mut self) {
        self.state.capabilities.persistence = true;
    }
}

impl RuntimeMachine for EmbeddedRuntimeMachine {
    type ExternalEvent = EmbeddedEvent;
    type Output = EmbeddedOutput;
    type Error = EmbeddedRuntimeError;

    fn accept(
        &mut self,
        dispatch: RuntimeDispatch<Self::ExternalEvent>,
    ) -> Result<(), Self::Error> {
        self.state.clock = dispatch.now;
        match dispatch.input {
            RuntimeInput::External(event) => {
                self.state.pending.push_back(event);
                self.state.wake_pending = true;
            }
            RuntimeInput::Wake => self.state.wake_pending = true,
            RuntimeInput::HostCompletion(completion) => {
                match self.state.persistence.complete(&completion) {
                    Some(output) => self.state.completed.push_back(output),
                    None => return Err(EmbeddedRuntimeError::UnsupportedHostCompletion),
                }
            }
            RuntimeInput::Shutdown => self.state.stopped = true,
        }
        Ok(())
    }

    fn advance(
        &mut self,
        _now: RuntimeClock,
    ) -> Result<RuntimeProgress<Self::Output>, Self::Error> {
        if self.state.stopped {
            return Ok(RuntimeProgress::Stopped);
        }
        if self.state.wake_pending {
            self.state.wake_pending = false;
            self.state.delete_expired();
            return Ok(RuntimeProgress::Progress);
        }
        if let Some(output) = self.state.completed.pop_front() {
            return Ok(RuntimeProgress::Output(output));
        }
        let Some(event) = self.state.pending.pop_front() else {
            return Ok(RuntimeProgress::Pending);
        };
        match event {
            EmbeddedEvent::Execute {
                request_id,
                session,
                args,
            } => {
                let selected_db = self.state.session.selected_db;
                let mut runtime = EmbeddedCommandRuntime::new(
                    &mut self.state.dbs,
                    &mut self.state.session.selected_db,
                    self.state.clock.unix_ms().max(0) as u64,
                );
                let outcome =
                    crate::framework::commands::command_runtime::execute(&mut runtime, &args);
                let mut sink = EmbeddedEffectSink::new(self.state.capabilities);
                let commit = sink.commit(session, CommandTransition::Complete(outcome));
                Ok(RuntimeProgress::Output(self.state.finish_command(
                    request_id,
                    session,
                    selected_db,
                    &args,
                    commit,
                )))
            }
        }
    }

    fn next_deadline(&self, now: RuntimeClock) -> Option<RuntimeInstant> {
        self.state.next_deadline(now)
    }

    fn has_immediate_work(&self) -> bool {
        self.state.wake_pending
            || !self.state.pending.is_empty()
            || !self.state.completed.is_empty()
    }

    fn is_stopped(&self) -> bool {
        self.state.stopped
    }
}
