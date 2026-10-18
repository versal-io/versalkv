use crate::engine::command::{CommandArgs, SessionId};
use crate::engine::core::CommandOutcome;

use super::SuspendedCommand;

#[derive(Clone)]
pub struct OwnedCommandContinuation {
    pub original_args: CommandArgs,
}

pub enum CommandTransition {
    Complete(CommandOutcome),
    Suspend(SuspendedCommand<OwnedCommandContinuation>),
    Close(CommandOutcome),
}

pub trait CommandEffectSink {
    type Output;

    fn commit(&mut self, session: SessionId, transition: CommandTransition) -> Self::Output;
}
