use super::super::super::ServerWorkerState;
use crate::engine::command::context::BlockedCmd;
use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;

fn cmd_may_timeout(cmd: &BlockedCmd) -> bool {
    !matches!(
        cmd,
        BlockedCmd::Paused
            | BlockedCmd::Throttled
            | BlockedCmd::SlotMigration(_)
            | BlockedCmd::Shutdown
            | BlockedCmd::Script
            | BlockedCmd::SetSlot { .. }
    )
}

impl ServerWorkerState {
    /// CLIENT UNBLOCK — unblock a client blocked on a timeout-capable
    /// blocking command (BLPOP/BRPOP/…). Returns 1 on success, 0 otherwise.
    /// Alignment with C Valkey `clientUnblockCommand` (networking.c:5469) and
    /// `blockedClientMayTimeout` (blocked.c:260):
    /// * Only clients whose *real* blocking btype is timeout-able (LIST / ZSET
    ///   / STREAM / WAIT = our Blpop/Brpop/Bzpop*/Xread/…) count.
    /// * BLOCKED_POSTPONE (outer CLIENT PAUSE) wrappers do NOT count — the
    ///   postpone is a command-level postponement, not the real block.
    ///
    /// Unlike C Valkey, on the Rust side the single `BlockedState` struct can
    /// cover BOTH roles: before the BLPOP handler runs we set Paused (pure
    /// postpone), and after the handler runs (pause wraps an already-registered
    /// BLPOP block) we save the original timeout-able state in
    /// `orig_wrapped`. `effective_blocking_cmd` resolves this to the btype
    /// C-side `blockedClientMayTimeout` would actually inspect.
    pub(super) fn handle_client_unblock(
        &mut self,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
        pending_unblock: &mut Vec<(usize, bool)>,
    ) -> Option<bool> {
        // CommandArgs for `CLIENT UNBLOCK <id> [TIMEOUT|ERROR]`:
        //   name = b"client", args = [b"unblock", b"<id>", b"mode"...]
        let client_id = match args
            .args
            .get(1)
            .and_then(|s| String::from_utf8_lossy(s).parse::<i64>().ok())
        {
            Some(id) if id >= 0 => id as usize,
            _ => {
                self.ctx.end_cmd(name_lower, true);
                replies.push(if resp3 {
                    serialize_to_vec_resp3(&Value::error(
                        "ERR value is not an integer or out of range",
                    ))
                } else {
                    serialize_to_vec_resp2(&Value::error(
                        "ERR value is not an integer or out of range",
                    ))
                });
                return Some(true);
            }
        };
        let is_error = match args.args.get(2) {
            Some(mode) => {
                let m = String::from_utf8_lossy(mode);
                if m.eq_ignore_ascii_case("error") {
                    true
                } else if m.eq_ignore_ascii_case("timeout") {
                    false
                } else {
                    self.ctx.end_cmd(name_lower, true);
                    replies.push(if resp3 {
                        serialize_to_vec_resp3(&Value::error(
                            "ERR syntax error, CLIENT UNBLOCK mode must be TIMEOUT or ERROR",
                        ))
                    } else {
                        serialize_to_vec_resp2(&Value::error(
                            "ERR syntax error, CLIENT UNBLOCK mode must be TIMEOUT or ERROR",
                        ))
                    });
                    return Some(true);
                }
            }
            None => false,
        };
        if args.args.len() > 3 {
            self.ctx.end_cmd(name_lower, true);
            replies.push(if resp3 {
                serialize_to_vec_resp3(&Value::error("ERR syntax error"))
            } else {
                serialize_to_vec_resp2(&Value::error("ERR syntax error"))
            });
            return Some(true);
        }

        let token = self
            .connection_registry
            .token_for_client_id(client_id as u64);
        let exists = token.is_some_and(|token| self.ctx.client_info(token.0).is_some());
        let connection_token = token.filter(|token| self.connection_registry.contains(*token));
        let paused_clients = self.ctx.paused_clients();
        let in_paused_set = token.is_some_and(|token| paused_clients.contains(&token));

        // Resolve to the real blocking command:
        // * If the client is NOT in paused_clients, use conn.blocked.cmd
        //   directly (it can still be Paused if a command has been postponed
        //   before being dispatched, e.g. EXEC mid-pause — but that is the
        //   literal outer block the C-side btype check would see).
        // * If the client IS in paused_clients, the outer state is the C-side
        //   BLOCKED_POSTPONE wrapper: C blocks on the postpone-list separately
        //   from the real block. On Rust we unwrap orig_wrapped to see the
        //   underlying BLPOP/BRPOP/XREAD state if it exists.
        let effective_cmd = connection_token.and_then(|token| {
            self.connection_registry
                .get(token)
                .and_then(|c| c.blocked.as_ref())
                .map(|b| {
                    if in_paused_set {
                        b.orig_wrapped.as_ref().map(|w| &w.cmd).unwrap_or(&b.cmd)
                    } else {
                        &b.cmd
                    }
                })
        });

        let is_timeout_able_blocked = effective_cmd.map(cmd_may_timeout).unwrap_or(false);
        let ok = exists && is_timeout_able_blocked;

        if ok {
            if let Some(token) = token {
                pending_unblock.push((token.0, is_error));
            }
            replies.push(if resp3 {
                serialize_to_vec_resp3(&Value::integer(1))
            } else {
                serialize_to_vec_resp2(&Value::integer(1))
            });
        } else {
            replies.push(if resp3 {
                serialize_to_vec_resp3(&Value::integer(0))
            } else {
                serialize_to_vec_resp2(&Value::integer(0))
            });
        }
        self.ctx.end_cmd(name_lower, false);
        Some(false)
    }
}
