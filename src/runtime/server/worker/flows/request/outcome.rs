//! Explicit control-flow result for a single command.

use super::dispatch::CommandEffects;
use super::invocation::CommandInvocation;
use crate::foundation::io::protocol::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandExecutionStatus {
    Executed,
    Rejected,
}

impl CommandExecutionStatus {
    pub(crate) fn is_rejected(self) -> bool {
        matches!(self, Self::Rejected)
    }
}

pub(crate) struct CommandExecution {
    pub(crate) reply: Value,
    pub(crate) status: CommandExecutionStatus,
    pub(crate) duration_us: Option<u64>,
}

impl CommandExecution {
    pub(crate) fn executed(reply: Value, duration_us: Option<u64>) -> Self {
        Self {
            reply,
            status: CommandExecutionStatus::Executed,
            duration_us,
        }
    }

    pub(crate) fn rejected(reply: Value) -> Self {
        Self {
            reply,
            status: CommandExecutionStatus::Rejected,
            duration_us: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandDisposition {
    Completed,
    Blocked,
    Closing,
    ConnectionGone,
}

impl CommandDisposition {
    pub(crate) fn stops_read_loop(self) -> bool {
        !matches!(self, Self::Completed)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpecialCommandResult {
    NotHandled,
    Handled(CommandDisposition),
}

pub(crate) struct CommandCompletion<'a> {
    pub(crate) invocation: CommandInvocation<'a>,
    pub(crate) reply: Value,
    pub(crate) status: CommandExecutionStatus,
    pub(crate) duration_us: u64,
    pub(crate) was_blocked_before: bool,
}

pub(crate) struct CommandOutcome {
    disposition: CommandDisposition,
    effects: CommandEffects,
}

impl CommandOutcome {
    pub(crate) fn new(disposition: CommandDisposition, effects: CommandEffects) -> Self {
        Self {
            disposition,
            effects,
        }
    }

    pub(crate) fn into_parts(self) -> (CommandDisposition, CommandEffects) {
        (self.disposition, self.effects)
    }
}

#[cfg(test)]
mod tests {
    use super::CommandDisposition;

    #[test]
    fn only_completed_commands_continue_the_read_loop() {
        assert!(!CommandDisposition::Completed.stops_read_loop());
        assert!(CommandDisposition::Blocked.stops_read_loop());
        assert!(CommandDisposition::Closing.stops_read_loop());
        assert!(CommandDisposition::ConnectionGone.stops_read_loop());
    }
}
