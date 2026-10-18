mod capabilities;
mod clock;
mod command;
mod continuation;
mod host;
mod input;
mod kernel;
mod machine;
mod step;

pub use capabilities::RuntimeCapabilities;
pub use clock::{RuntimeClock, RuntimeInstant};
pub use command::{CommandEffectSink, CommandTransition, OwnedCommandContinuation};
pub use continuation::{
    ContinuationId, ContinuationRegistry, ExternalEventKey, HostOperationId, SuspendedCommand,
    WaitCondition,
};
pub use host::{HostOperationResult, OwnedHostCompletion, OwnedHostRequest};
pub use input::{RuntimeDispatch, RuntimeInput};
pub use kernel::{RuntimeConfigurationError, RuntimeDriverError, RuntimeKernel, RuntimeTurnLimits};
pub use machine::{RuntimeMachine, RuntimeProgress};
pub use step::{RuntimeStep, RuntimeWait};

pub trait RuntimeDriver {
    type ExternalEvent;
    type Output;
    type Error;

    fn dispatch(
        &mut self,
        dispatch: RuntimeDispatch<Self::ExternalEvent>,
    ) -> Result<RuntimeStep<Self::Output>, Self::Error>;
}

#[cfg(test)]
mod tests;
