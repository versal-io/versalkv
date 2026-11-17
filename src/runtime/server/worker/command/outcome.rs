//! Explicit control-flow result for a single command.

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
}

impl CommandExecution {
    pub(crate) fn executed(reply: Value) -> Self {
        Self {
            reply,
            status: CommandExecutionStatus::Executed,
        }
    }

    pub(crate) fn rejected(reply: Value) -> Self {
        Self {
            reply,
            status: CommandExecutionStatus::Rejected,
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
