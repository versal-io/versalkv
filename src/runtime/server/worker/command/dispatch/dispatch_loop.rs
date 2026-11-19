use super::super::super::ServerWorkerState;
use super::side_effects::{CommandEffects, ReadBatchEffects};
use super::types::{CmdPreconditions, LoopAction};
use crate::engine::command::CommandName;
use crate::engine::command::{CommandArgs, CommandError, DispatchGroup};
use crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{CommandEffectDrain, ConnectionState};
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::command::invocation::{CommandInvocation, CommandTarget};
use crate::runtime::server::worker::command::outcome::{CommandDisposition, SpecialCommandResult};
use bytes::Bytes;

struct ParsedCommandInput {
    token: Token,
    args: CommandArgs,
    name: Bytes,
    resp3: bool,
}

impl ParsedCommandInput {
    /// Returns the command name as a UTF-8 string slice (original case).
    fn name_str(&self) -> &str {
        std::str::from_utf8(&self.name).unwrap_or("")
    }
}

enum SpecialCommandRoute {
    Transport,
    ClientAdmin,
    PubSub,
    Exec,
    MultiQueue,
    RejectSubscribeMode,
    Ordinary,
}

#[derive(Clone, Copy)]
pub(crate) struct SpecialCommandCompletion {
    pub(crate) disposition: CommandDisposition,
    pub(crate) reply_start: usize,
    pub(crate) reply_end: usize,
}

/// Saved priority-context state for restore after a command completes.
/// Only populated when at least one priority client is connected; otherwise
/// the per-command priority bookkeeping is skipped on the hot path.
struct PriorityState {
    previous_token: Option<Token>,
    previous_nested: bool,
    previous_priority: bool,
    owns_priority_cycle: bool,
}

impl ServerWorkerState {
    pub(crate) fn check_no_multi_pure(
        meta: &CmdPreconditions,
        name_lower: &str,
        command_name: &str,
    ) -> Option<Value> {
        let is_tx_control = matches!(
            name_lower,
            "exec" | "discard" | "multi" | "reset" | "watch" | "unwatch"
        );
        if is_tx_control || (meta.flags & crate::framework::CMD_NO_MULTI) == 0 {
            return None;
        }
        Some(Value::error(format!(
            "ERR Command '{}' not allowed inside a transaction",
            command_name
        )))
    }

    pub(crate) fn process_parsed_command(
        &mut self,
        token: Token,
        args: CommandArgs,
        name: impl Into<Bytes>,
        resp3: bool,
        batch: &mut ReadBatchEffects,
    ) -> LoopAction {
        let name = name.into();
        if matches!(
            self.connection_registry[token].slot_migration_role,
            Some(SlotMigrationConnectionRole::Import(_))
        ) && (!batch.replication_only.is_empty() || !batch.aof_only.is_empty())
        {
            // Import commands propagate immediately. Publish earlier commands
            // before that path changes either stream's selected database.
            self.queue_command_propagation(batch);
        }
        self.refresh_connection_priorities();
        // With no priority clients connected, no token can resolve to a
        // priority connection, so the per-command priority save/restore
        // (Cell swaps, stats write, priority-cycle bookkeeping) is dead
        // work. Skip it entirely on the common standalone PING hot path.
        // `refresh_connection_priorities` still runs to detect subnet
        // generation changes; its own fast path returns before touching
        // `priority_command_token` when the count is zero.
        let priority_state = if self.ctx.stats().connected_priority_clients != 0 {
            let is_priority = self.is_priority_connection(token);
            let previous_token = self.priority_command_token.replace(token);
            let previous_nested = std::mem::replace(
                &mut self.priority_command_is_nested,
                self.command_execution_depth != 0,
            );
            let previous_priority = self
                .ctx
                .stats_mut()
                .set_priority_command(is_priority && !self.priority_command_is_nested);
            let owns_priority_cycle = is_priority && self.begin_priority_cycle();
            Some(PriorityState {
                previous_token,
                previous_nested,
                previous_priority,
                owns_priority_cycle,
            })
        } else {
            None
        };
        let input = ParsedCommandInput {
            token,
            args,
            name,
            resp3,
        };
        // Reuse the worker-level effect scratch instead of default-constructing
        // a twelve-`Vec` `CommandEffects` on every command. `Option::take` is
        // a pointer-swap; the first call falls back to `Default`, and every
        // subsequent call reuses the struct whose `Vec` capacities survived
        // the prior `commit`. After dispatch, `commit` drains every field, so
        // the scratch is empty when stored back.
        let mut effects = self.command_effects_scratch.take().unwrap_or_default();
        let token = input.token;
        let action = self.dispatch_parsed_command(input, &batch.replies, &mut effects);
        self.refresh_connection_priorities();
        self.account_command_reply_effects(token, &mut effects);
        batch.commit(&mut effects);
        self.command_effects_scratch = Some(effects);
        if let Some(state) = priority_state {
            self.finish_priority_cycle(state.owns_priority_cycle);
            self.priority_command_token = state.previous_token;
            self.priority_command_is_nested = state.previous_nested;
            self.ctx
                .stats_mut()
                .set_priority_command(state.previous_priority);
        }
        action
    }

    fn dispatch_parsed_command(
        &mut self,
        input: ParsedCommandInput,
        pending_replies: &[Vec<u8>],
        fx: &mut CommandEffects,
    ) -> LoopAction {
        // Resolve spec-derived routing values once. All are owned or Copy, so
        // the immutable borrow of self.registry ends before any mutable
        // access below. canonical_name feeds both the panic diagnostic
        // (matching C's c->cmd->name) and stats accounting; cloning it is a
        // &'static str copy or an Arc refcount bump, never a heap alloc.
        //
        // The resolved executor and preconditions are also extracted here so
        // that `execute_invocation` does not need to look up the registry a
        // second time for known commands, eliminating one HashMap lookup on
        // the hot path (e.g. every PING). Unknown commands leave both as
        // `None` and fall back to the lookup in `execute_invocation`.
        let (
            canonical_name,
            dispatch_group,
            command_flags,
            resolved_executor,
            resolved_preconditions,
        ) = {
            let spec = self.registry.lookup(&input.args.name);
            let canonical_name = spec.map(|s| s.name.clone());
            let dispatch_group = spec
                .map(|s| s.dispatch_group)
                .unwrap_or(DispatchGroup::Default);
            let command_flags = spec
                .map(|s| {
                    super::subcmd_flags::effective_command_flags_for_call(
                        s.name.as_str(),
                        s.flags,
                        &input.args,
                        &self.ctx,
                    )
                })
                .unwrap_or(0);
            let (resolved_executor, resolved_preconditions) = match spec {
                Some(spec) => {
                    let acl_categories = super::subcmd_flags::effective_acl_categories(
                        spec.name.as_str(),
                        spec.acl_categories,
                        input.args.args.first().map(|arg| arg.as_ref()),
                    );
                    let preconditions = super::CmdPreconditions {
                        name: spec.name.clone(),
                        arity: spec.arity,
                        flags: command_flags,
                        first_key: spec.first_key,
                        last_key: spec.last_key,
                        key_step: spec.key_step,
                        key_flags: spec.key_flags,
                        acl_categories,
                        get_keys: spec.get_keys,
                    };
                    (Some(spec.executor), Some(preconditions))
                }
                None => (None, None),
            };
            (
                canonical_name,
                dispatch_group,
                command_flags,
                resolved_executor,
                resolved_preconditions,
            )
        };
        let diagnostic_command = canonical_name
            .as_ref()
            .map(|n| n.as_str())
            .unwrap_or_else(|| input.name_str());
        let diagnostic = {
            let conn = &mut self.connection_registry[input.token];
            crate::foundation::io::sys::log::CommandDiagnostic {
                client_id: conn.client_id(),
                command_seq: conn.record_received_command(),
                peer: conn.peer_addr,
                db: conn.selected_db,
                command: diagnostic_command,
                argument_count: input.args.args.len(),
                request_bytes: conn.parser.last_cmd_len(),
            }
        };
        let _diagnostic_guard = crate::foundation::io::sys::log::enter_active_command(&diagnostic);
        if let Some(action) = self.try_apply_slot_import(&input, fx) {
            return action;
        }
        if let Some(action) = self.reject_unauthenticated_command(&input, fx) {
            return action;
        }
        self.apply_client_test_tcp_buffer_policy(input.token, &input.name, &input.args);
        self.ctx.begin_cmd();
        self.connection_registry[input.token].begin_admitted_command(diagnostic_command);
        self.dispatch_admitted_command(
            input,
            pending_replies,
            fx,
            dispatch_group,
            command_flags,
            canonical_name.clone(),
            resolved_executor,
            resolved_preconditions,
        )
    }

    /// Slot-import admission guard. Returns an action when the command is
    /// consumed by the import path, otherwise `None` to continue admission.
    fn try_apply_slot_import(
        &mut self,
        input: &ParsedCommandInput,
        fx: &mut CommandEffects,
    ) -> Option<LoopAction> {
        let token_value = input.token.0;
        if self.should_defer_slot_import_write(token_value, &input.args)
            || !self.apply_slot_import_command(token_value, input.args.clone())
        {
            return None;
        }
        Some(
            if self
                .connection_registry
                .get(input.token)
                .is_none_or(|connection| connection.state == ConnectionState::Closing)
            {
                fx.mark_closing = true;
                LoopAction::Break
            } else {
                LoopAction::Continue
            },
        )
    }

    /// Default admission path once slot-import and authentication guards have
    /// passed: route special dispatch groups, then fall back to ordinary
    /// command execution. Dispatch group and flags are resolved by the caller
    /// so admission bookkeeping stays in `dispatch_parsed_command`.
    fn dispatch_admitted_command(
        &mut self,
        input: ParsedCommandInput,
        pending_replies: &[Vec<u8>],
        fx: &mut CommandEffects,
        dispatch_group: DispatchGroup,
        command_flags: u64,
        canonical_name: Option<CommandName>,
        resolved_executor: Option<crate::framework::CommandExecutor>,
        resolved_preconditions: Option<super::CmdPreconditions>,
    ) -> LoopAction {
        let token_value = input.token.0;
        let reply_start = fx.replies.len();
        let disposition = match self.decide_special_command_route(&input, dispatch_group) {
            SpecialCommandRoute::Transport => self
                .try_handle_transport_cmd(
                    token_value,
                    token_value,
                    &input.args,
                    input.name_str(),
                    input.resp3,
                    &mut fx.0.replies,
                    &mut fx.0.unblock,
                )
                .map(|_| CommandDisposition::Completed),
            SpecialCommandRoute::ClientAdmin => self
                .try_handle_client_admin(
                    token_value,
                    token_value,
                    &input.args,
                    input.name_str(),
                    input.resp3,
                    &mut fx.0.replies,
                    &mut fx.0.unblock,
                )
                .map(|_| CommandDisposition::Completed),
            SpecialCommandRoute::PubSub => self
                .try_handle_pubsub_cmd(
                    token_value,
                    &input.args,
                    input.name_str(),
                    input.resp3,
                    &mut fx.0.replies,
                    &mut fx.0.publish,
                )
                .map(|_| CommandDisposition::Completed),
            SpecialCommandRoute::Exec => {
                self.connection_registry[input.token]
                    .transaction
                    .set_exec_arguments(input.args.args.clone());
                let disposition = self.handle_exec(
                    CommandTarget::new(input.token),
                    std::str::from_utf8(&input.args.name).unwrap_or(input.name_str()),
                    input.resp3,
                    fx,
                );
                if disposition != CommandDisposition::ConnectionGone {
                    let internal_monitor =
                        std::mem::take(&mut self.ctx.script_mut().pending_internal_monitor);
                    self.deliver_monitor(internal_monitor);
                }
                Some(disposition)
            }
            SpecialCommandRoute::MultiQueue => {
                self.handle_multi_queue(
                    input.token,
                    &input.args,
                    input.name_str(),
                    input.resp3,
                    &mut fx.replies,
                );
                Some(CommandDisposition::Completed)
            }
            SpecialCommandRoute::RejectSubscribeMode => {
                let reply = Value::error(format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    input.name_str()
                ));
                // Slow path: subscribe-mode rejection. Allocate the lowercased
                // name for stats accounting; this is not on the hot path.
                self.ctx
                    .end_cmd(&input.name_str().to_ascii_lowercase(), true);
                super::record_error_reply(&mut self.ctx, &reply, None);
                fx.replies.push(serialize_to_vec_resp2(&reply));
                Some(CommandDisposition::Completed)
            }
            SpecialCommandRoute::Ordinary => None,
        };
        let result = disposition.map_or(
            SpecialCommandResult::NotHandled,
            SpecialCommandResult::Handled,
        );
        let reply_end = self.attribute_special_command_replies(
            dispatch_group,
            input.name_str(),
            reply_start,
            result,
            fx,
        );
        match result {
            SpecialCommandResult::NotHandled => {}
            SpecialCommandResult::Handled(disposition) => {
                let disposition = self.complete_special_command(
                    input.token,
                    SpecialCommandCompletion {
                        disposition,
                        reply_start,
                        reply_end,
                    },
                    fx,
                );
                if disposition == CommandDisposition::ConnectionGone {
                    return LoopAction::Break;
                }
                let Some(connection) = self.connection_registry.get(input.token) else {
                    return LoopAction::Break;
                };
                if disposition.stops_read_loop() || connection.blocked.is_some() {
                    return LoopAction::Break;
                } else {
                    return LoopAction::Continue;
                }
            }
        }

        // Use the canonical lowercased name from the resolved spec for stats
        // accounting. Fall back to the original-case name only for unknown
        // commands (rare); record_cmd_completion normalizes to lowercase on
        // its slow path.
        let stats_name = canonical_name
            .as_ref()
            .map(|name| name.as_str())
            .unwrap_or_else(|| input.name_str());
        let invocation = CommandInvocation::direct(
            CommandTarget::new(input.token),
            &input.args,
            stats_name,
            command_flags,
            dispatch_group,
            pending_replies,
        );
        let disposition = self.execute_command_and_handle_result(
            invocation,
            resolved_executor,
            resolved_preconditions,
            fx,
        );
        if disposition.stops_read_loop() {
            LoopAction::Break
        } else {
            LoopAction::Continue
        }
    }

    fn apply_client_test_tcp_buffer_policy(
        &mut self,
        token: Token,
        name: &[u8],
        args: &CommandArgs,
    ) {
        // Replicas negotiate on an unclassified accepted socket. Keep their
        // kernel defaults, and apply harness backpressure only once a command
        // identifies an ordinary client. Migration has its own explicit policy.
        //
        // Cheap command-name checks run before the `replication_runtime.role()`
        // HashMap lookup so that common commands (PING, AUTH, HELLO, ...) skip
        // the map entirely. The role lookup is only needed for commands that
        // could legitimately trigger buffer-limit application.
        if self.connection_registry[token].is_test_tcp_buffer_policy_applied
            || (name.eq_ignore_ascii_case(b"ping") && args.args.is_empty())
            || name.eq_ignore_ascii_case(b"auth")
            || name.eq_ignore_ascii_case(b"hello")
            || name.eq_ignore_ascii_case(b"replconf")
            || name.eq_ignore_ascii_case(b"psync")
            || name.eq_ignore_ascii_case(b"sync")
            || self
                .replication_runtime
                .role(crate::framework::distributed::replication::ConnectionId(
                    token.0,
                ))
                .is_some()
        {
            return;
        }
        match self.transport.apply_test_tcp_buffer_limits(token) {
            Ok(()) => self.connection_registry[token].is_test_tcp_buffer_policy_applied = true,
            Err(error) => crate::server_log!(
                warning,
                "Could not apply client test TCP buffer policy: {}",
                error
            ),
        }
    }

    fn reject_unauthenticated_command(
        &mut self,
        input: &ParsedCommandInput,
        fx: &mut CommandEffects,
    ) -> Option<LoopAction> {
        let conn = &self.connection_registry[input.token];
        if conn.authenticated || Self::is_auth_allowed(&input.args.name) {
            return None;
        }
        let close = !conn.ever_authenticated && self.ctx.config().server.enforce_reply_list;
        self.ctx.begin_cmd();
        // Slow path: authentication rejection. Allocate the lowercased name
        // for stats accounting; this is not on the hot path.
        self.ctx
            .end_cmd(&input.name_str().to_ascii_lowercase(), true);
        let reply = CommandError::noauth();
        super::record_error_reply(&mut self.ctx, &reply, None);
        if close {
            self.connection_registry[input.token].mark_closing();
            fx.mark_closing = true;
            return Some(LoopAction::Break);
        }
        fx.replies.push(if input.resp3 {
            serialize_to_vec_resp3(&reply)
        } else {
            serialize_to_vec_resp2(&reply)
        });
        Some(LoopAction::Continue)
    }

    fn decide_special_command_route(
        &self,
        input: &ParsedCommandInput,
        group: DispatchGroup,
    ) -> SpecialCommandRoute {
        if group == DispatchGroup::Transport {
            return SpecialCommandRoute::Transport;
        }
        if group != DispatchGroup::ClientAdmin {
            if self.should_reject_subscribe_mode(input) {
                return SpecialCommandRoute::RejectSubscribeMode;
            }
            // Byte-wise ASCII case folding avoids a per-command UTF-8
            // validation (`from_utf8`); command names only need to match ASCII
            // literals here, and `[u8]::eq_ignore_ascii_case` is equivalent.
            if input.name.eq_ignore_ascii_case(b"exec") {
                return SpecialCommandRoute::Exec;
            }
        }
        if self.should_queue_in_transaction(input.token, &input.name) {
            return SpecialCommandRoute::MultiQueue;
        }
        match group {
            DispatchGroup::Transport => SpecialCommandRoute::Transport,
            DispatchGroup::ClientAdmin => SpecialCommandRoute::ClientAdmin,
            DispatchGroup::PubSubControl => SpecialCommandRoute::PubSub,
            DispatchGroup::Default
            | DispatchGroup::Transaction
            | DispatchGroup::Auth
            | DispatchGroup::Shutdown => {
                if input.name.eq_ignore_ascii_case(b"publish")
                    || input.name.eq_ignore_ascii_case(b"spublish")
                {
                    SpecialCommandRoute::PubSub
                } else {
                    SpecialCommandRoute::Ordinary
                }
            }
        }
    }

    fn attribute_special_command_replies(
        &self,
        group: DispatchGroup,
        name: &str,
        reply_start: usize,
        result: SpecialCommandResult,
        effects: &mut CommandEffects,
    ) -> usize {
        if !matches!(result, SpecialCommandResult::Handled(_)) {
            return reply_start;
        }
        let reply_end = match group {
            DispatchGroup::Transport if name.eq_ignore_ascii_case("replconf") => {
                reply_start.saturating_add(1)
            }
            DispatchGroup::Transport => {
                if effects
                    .replies
                    .get(reply_start)
                    .is_some_and(|frame| frame.starts_with(b"-"))
                {
                    reply_start.saturating_add(1)
                } else {
                    reply_start
                }
            }
            _ => effects.replies.len(),
        };
        effects.attribute_replies(reply_start, reply_end);
        reply_end
    }

    pub(crate) fn complete_special_command(
        &mut self,
        token: Token,
        completion: SpecialCommandCompletion,
        effects: &mut CommandEffects,
    ) -> CommandDisposition {
        if completion.disposition != CommandDisposition::Completed {
            return completion.disposition;
        }
        let Some(connection) = self.connection_registry.get(token) else {
            return CommandDisposition::ConnectionGone;
        };
        if connection.blocked.is_some() {
            return CommandDisposition::Blocked;
        }
        self.account_command_reply_effects(token, effects);
        let (suppress_reply, visible_reply_bytes, lazyfree_objects) = {
            let connection = &mut self.connection_registry[token];
            let mut connection_effects = connection
                .take_command_effects(CommandEffectDrain::Completed)
                .expect("a completed special command drains its connection effects exactly once");
            let suppress_reply = connection_effects.should_suppress_reply();
            let visible_reply_bytes = if suppress_reply {
                0
            } else {
                connection_effects.reply_bytes()
            };
            let lazyfree_objects = connection_effects.take_lazyfree_objects();
            connection.finish_reply_cycle();
            (suppress_reply, visible_reply_bytes, lazyfree_objects)
        };
        self.dispose_data_lazyfree_objects(lazyfree_objects);
        if let Some(metric) = effects.take_deferred_slot_metric() {
            self.ctx.slot_metrics_mut().record(
                &[metric.slot],
                metric.cpu_usec,
                metric.network_bytes_in,
                visible_reply_bytes as u64,
            );
        }
        if suppress_reply && completion.reply_start < completion.reply_end {
            let reply_end = completion.reply_end.min(effects.replies.len());
            effects.replies.drain(completion.reply_start..reply_end);
        }
        if self.connection_registry[token].state == ConnectionState::Closing {
            effects.mark_closing = true;
            CommandDisposition::Closing
        } else {
            CommandDisposition::Completed
        }
    }

    pub(crate) fn account_command_reply_effects(
        &mut self,
        token: Token,
        effects: &mut CommandEffects,
    ) {
        let bytes = effects.take_command_reply_bytes();
        if bytes == 0 {
            return;
        }
        if let Some(connection) = self.connection_registry.get_mut(token) {
            connection.record_command_reply_bytes(bytes);
        }
    }

    fn should_reject_subscribe_mode(&self, input: &ParsedCommandInput) -> bool {
        let conn = &self.connection_registry[input.token];
        let is_subscribed = conn.sub_channel_count > 0
            || conn.psub_channel_count > 0
            || conn.ssub_channel_count > 0;
        is_subscribed && !input.resp3 && !Self::is_subscribe_mode_allowed(&input.name)
    }

    pub(crate) fn process_dirty_keys_in_loop(
        &mut self,
        token_value: usize,
        fx: &mut ReadBatchEffects,
    ) {
        let effects = &mut fx.0;
        if effects.watch_dirty.is_empty() {
            return;
        }
        self.collect_tracking_invalidations(
            &effects.watch_dirty,
            Some(Token(token_value)),
            &mut effects.tracking,
        );
        // Fast path: when no connection holds WATCHed keys, the per-connection
        // scan below is pure overhead. The `watched_key_clients` counter is
        // maintained by `record_completion_metadata`, the EXEC accounting
        // path, and connection teardown, so it stays accurate without a scan.
        if self.watched_key_clients > 0 {
            for (db_id, key) in &effects.watch_dirty {
                let exists_after = self
                    .ctx
                    .db(*db_id)
                    .is_some_and(|db| db.exists_no_expire(key));
                for (_, conn) in self.connection_registry.iter_mut() {
                    if conn.transaction.watched_keys().is_empty() {
                        continue;
                    }
                    conn.transaction
                        .touch_watched_key(*db_id, key, exists_after);
                }
            }
        }
        let dirty_keys = std::mem::take(&mut effects.watch_dirty);
        self.wakeup_blocked_on_keys_with_effect_queue(&dirty_keys, None, None, Some(effects));
    }
}
