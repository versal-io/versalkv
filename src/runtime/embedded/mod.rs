use std::fmt;

use bytes::Bytes;

use crate::engine::command::{CommandArgs, SessionId};
use crate::runtime::driver::{
    RuntimeCapabilities, RuntimeDispatch, RuntimeDriver, RuntimeDriverError, RuntimeInput,
    RuntimeKernel, RuntimeStep,
};

mod effects;
mod machine;
mod output;
mod scheduler;
mod session;
mod state;

use machine::EmbeddedRuntimeMachine;
pub use output::{EmbeddedOutput, Reply};
pub use session::EmbeddedEvent;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddedRuntimeError {}

impl fmt::Display for EmbeddedRuntimeError {
    fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

impl std::error::Error for EmbeddedRuntimeError {}

pub struct EmbeddedRuntime {
    kernel: RuntimeKernel<EmbeddedRuntimeMachine>,
}

impl Default for EmbeddedRuntime {
    fn default() -> Self {
        Self {
            kernel: RuntimeKernel::with_default_limits(EmbeddedRuntimeMachine::default()),
        }
    }
}

impl EmbeddedRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn dispatch(
        &mut self,
        dispatch: RuntimeDispatch<EmbeddedEvent>,
    ) -> Result<RuntimeStep<EmbeddedOutput>, RuntimeDriverError<EmbeddedRuntimeError>> {
        self.kernel.dispatch(dispatch)
    }

    pub fn capabilities(&self) -> RuntimeCapabilities {
        self.kernel.machine().capabilities()
    }

    pub fn execute(&mut self, args: &[String], now_ms: u64) -> Reply {
        let command = CommandArgs::from_parts(
            args.iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                .collect(),
        );
        let dispatch = RuntimeDispatch::new(
            crate::runtime::driver::RuntimeClock::new(now_ms, now_ms.min(i64::MAX as u64) as i64),
            RuntimeInput::External(EmbeddedEvent::Execute {
                request_id: 0,
                session: SessionId(1),
                args: command,
            }),
        );
        match self.dispatch(dispatch) {
            Ok(step) => step
                .outputs
                .into_iter()
                .next()
                .map(EmbeddedOutput::into_reply)
                .unwrap_or_else(|| Reply::Error("ERR runtime produced no reply".to_owned())),
            Err(error) => Reply::Error(format!("ERR {error}")),
        }
    }
}

#[cfg(test)]
mod tests;
