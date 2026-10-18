//! Typed identity and immutable metadata for one command execution.

use crate::engine::command::{CommandArgs, DispatchGroup};
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::ServerWorkerState;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CommandTarget {
    token: Token,
}

impl CommandTarget {
    pub(crate) fn new(token: Token) -> Self {
        Self { token }
    }

    pub(crate) fn token(self) -> Token {
        self.token
    }

    pub(crate) fn token_value(self) -> usize {
        self.token.0
    }

    /// Revalidate the stable token at each execution boundary instead of
    /// retaining any registry-internal storage position across re-entry.
    pub(crate) fn resolve(self, runtime: &ServerWorkerState) -> Option<Token> {
        runtime
            .connection_registry
            .contains(self.token)
            .then_some(self.token)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandOrigin {
    Direct,
    TransactionItem,
}

#[derive(Clone, Copy)]
pub(crate) struct CommandInvocation<'a> {
    pub(crate) target: CommandTarget,
    pub(crate) args: &'a CommandArgs,
    pub(crate) name: &'a str,
    pub(crate) flags: u64,
    pub(crate) dispatch_group: DispatchGroup,
    pub(crate) origin: CommandOrigin,
    pub(crate) pending_replies: &'a [Vec<u8>],
}

impl<'a> CommandInvocation<'a> {
    pub(crate) fn direct(
        target: CommandTarget,
        args: &'a CommandArgs,
        name: &'a str,
        flags: u64,
        dispatch_group: DispatchGroup,
        pending_replies: &'a [Vec<u8>],
    ) -> Self {
        Self {
            target,
            args,
            name,
            flags,
            dispatch_group,
            origin: CommandOrigin::Direct,
            pending_replies,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandOrigin, CommandTarget};
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::ServerWorkerState;

    #[test]
    fn command_target_resolves_through_the_runtime_registry() {
        let runtime = ServerWorkerState::new(1).unwrap();
        let target = CommandTarget::new(Token(17));

        assert_eq!(target.token_value(), 17);
        assert_eq!(target.resolve(&runtime), None);
        assert_eq!(CommandOrigin::Direct, CommandOrigin::Direct);
    }
}
