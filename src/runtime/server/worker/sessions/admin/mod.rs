mod filters;
mod helpers;
mod kill;
mod queries;
mod unblock;

use super::super::ServerWorkerState;
use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::Value;

impl ServerWorkerState {
    /// Execute a queued `CLIENT` subcommand during EXEC replay, returning its
    /// reply `Value`, or `None` when the subcommand has no event-loop-only
    /// behavior and should fall through to the regular `client` handler.
    /// `CLIENT` is a `DispatchGroup::ClientAdmin` command: KILL / LIST are
    /// handled at the event-loop level (they touch other connections), so on the
    /// live path they never reach the regular handler. Inside MULTI/EXEC the
    /// queued command is replayed via the handler, which knows no `kill`/`list`
    /// subcommand — so we must route those forms through the same Value cores
    /// here. Mirrors C where CLIENT KILL/LIST run identically in a transaction.
    pub(crate) fn exec_client_admin_value(
        &mut self,
        token_value: usize,
        args: &CommandArgs,
    ) -> Option<Value> {
        let sub = args
            .args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
            .unwrap_or_default();
        match sub.as_str() {
            "kill" => Some(self.client_kill_value(token_value, args, "client").0),
            "list" => Some(self.client_list_value(token_value, args).0),
            _ => None,
        }
    }

    /// Dispatch CLIENT subcommands to per-subcommand handlers.
    /// Returns `None` if the command is not a CLIENT command (fall through),
    /// otherwise `Some(handled)` matching the caller's expected protocol.
    pub(crate) fn try_handle_client_admin(
        &mut self,
        connection_token: usize,
        token_value: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
        pending_unblock: &mut Vec<(usize, bool)>,
    ) -> Option<bool> {
        if name_lower != "client" {
            return None;
        }

        // ACL check for CLIENT command (ClientAdmin bypasses pre_dispatch_exec)
        if let Some(rejected) =
            self.check_client_acl(connection_token, args, name_lower, resp3, replies)
        {
            return Some(rejected);
        }

        let sub = args
            .args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
            .unwrap_or_default();

        // Align with C Valkey's unpauseActions() → updatePausedActions() →
        // unblockPostponedClients() chain: as soon as CLIENT UNPAUSE has
        // lowered the client-pause action mask, postponed/paused clients are
        // unblocked synchronously *before* subsequent commands in the same
        // event-loop iteration (e.g. the CLIENT UNBLOCK calls right after
        // unpause in unit/pause.tcl) are dispatched.
        if sub == "unpause" {
            return None;
        }
        if self.ctx.pending_unpause() {
            self.drain_pending_unpause_if_any(true);
        }

        match sub.as_str() {
            "list" => self.handle_client_list(token_value, args, name_lower, resp3, replies),
            "id" => self.handle_client_id(token_value, name_lower, resp3, replies),
            "kill" => self.handle_client_kill(token_value, args, name_lower, resp3, replies),
            "unblock" => {
                self.handle_client_unblock(args, name_lower, resp3, replies, pending_unblock)
            }
            _ => None,
        }
    }
}
