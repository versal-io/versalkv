use super::super::super::ServerWorkerState;
use super::side_effects::{CommandEffects, ReadBatchEffects};
use super::types::{CmdPreconditions, LoopAction};
use crate::engine::command::{CommandArgs, CommandError, DispatchGroup};
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{CommandEffectDrain, ConnectionState};
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::flows::request::invocation::{
    CommandInvocation, CommandTarget,
};
use crate::runtime::server::worker::flows::request::outcome::{
    CommandDisposition, SpecialCommandResult,
};

struct ParsedCommandInput {
    token: Token,
    args: CommandArgs,
    name: String,
    resp3: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct SpecialCommandCompletion {
    pub(crate) disposition: CommandDisposition,
    pub(crate) reply_start: usize,
    pub(crate) reply_end: usize,
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
        name_lower: String,
        resp3: bool,
        batch: &mut ReadBatchEffects,
    ) -> LoopAction {
        self.refresh_connection_priorities();
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
        let input = ParsedCommandInput {
            token,
            args,
            name: name_lower,
            resp3,
        };
        let mut effects = CommandEffects::default();
        let token = input.token;
        let action = self.dispatch_parsed_command(input, &batch.replies, &mut effects);
        self.refresh_connection_priorities();
        self.account_command_reply_effects(token, &mut effects);
        batch.commit(effects);
        self.finish_priority_cycle(owns_priority_cycle);
        self.priority_command_token = previous_token;
        self.priority_command_is_nested = previous_nested;
        self.ctx.stats_mut().set_priority_command(previous_priority);
        action
    }

    fn dispatch_parsed_command(
        &mut self,
        input: ParsedCommandInput,
        pending_replies: &[Vec<u8>],
        fx: &mut CommandEffects,
    ) -> LoopAction {
        let token_value = input.token.0;
        let diagnostic = self.record_command_diagnostic(&input);
        let _diagnostic_guard = crate::foundation::io::sys::log::enter_active_command(diagnostic);
        if !self.should_defer_slot_import_write(token_value, &input.args)
            && self.apply_slot_import_command(token_value, input.args.clone())
        {
            return if self.connection_registry[input.token].state == ConnectionState::Closing {
                fx.mark_closing = true;
                LoopAction::Break
            } else {
                LoopAction::Continue
            };
        }
        if let Some(action) = self.reject_unauthenticated_command(&input, fx) {
            return action;
        }
        self.apply_client_test_tcp_buffer_policy(input.token, &input.name, &input.args);

        self.ctx.begin_cmd();
        let (dispatch_group, command_flags) = {
            let spec = self.registry.lookup(&input.args.name);
            self.connection_registry[input.token]
                .begin_admitted_command(spec.map(|s| s.name.as_str()).unwrap_or(&input.name));
            (
                spec.map(|s| s.dispatch_group)
                    .unwrap_or(DispatchGroup::Default),
                spec.map(|s| {
                    super::subcmd_flags::effective_command_flags_for_call(
                        s.name.as_str(),
                        s.flags,
                        &input.args,
                        &self.ctx,
                    )
                })
                .unwrap_or(0),
            )
        };
        if let Some(action) = self.dispatch_special_group(&input, dispatch_group, fx) {
            return action;
        }

        let invocation = CommandInvocation::direct(
            CommandTarget::new(input.token),
            &input.args,
            &input.name,
            command_flags,
            dispatch_group,
            pending_replies,
        );
        let outcome = self.execute_command_and_handle_result(invocation);
        let (disposition, effects) = outcome.into_parts();
        fx.commit(effects);
        if disposition.stops_read_loop() {
            LoopAction::Break
        } else {
            LoopAction::Continue
        }
    }

    fn apply_client_test_tcp_buffer_policy(
        &mut self,
        token: Token,
        name: &str,
        args: &CommandArgs,
    ) {
        // Replicas negotiate on an unclassified accepted socket. Keep their
        // kernel defaults, and apply harness backpressure only once a command
        // identifies an ordinary client. Migration has its own explicit policy.
        if self.connection_registry[token].is_test_tcp_buffer_policy_applied
            || self
                .replication_runtime
                .role(crate::framework::distributed::replication::ConnectionId(
                    token.0,
                ))
                .is_some()
            || (name == "ping" && args.args.is_empty())
            || matches!(name, "auth" | "hello" | "replconf" | "psync" | "sync")
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

    fn record_command_diagnostic(
        &mut self,
        input: &ParsedCommandInput,
    ) -> crate::foundation::io::sys::log::CommandDiagnostic {
        let conn = &mut self.connection_registry[input.token];
        crate::foundation::io::sys::log::CommandDiagnostic {
            client_id: conn.client_id(),
            command_seq: conn.record_received_command(),
            peer: conn.peer_addr.to_string(),
            db: conn.selected_db,
            command: input.name.clone(),
            argument_count: input.args.args.len(),
            request_bytes: conn.parser.last_cmd_len(),
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
        self.ctx.end_cmd(&input.name, true);
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

    fn dispatch_special_group(
        &mut self,
        input: &ParsedCommandInput,
        group: DispatchGroup,
        fx: &mut CommandEffects,
    ) -> Option<LoopAction> {
        let token_value = input.token.0;
        let reply_start = fx.replies.len();
        let result = match group {
            DispatchGroup::Transport => {
                if self
                    .try_handle_transport_cmd(
                        token_value,
                        token_value,
                        &input.args,
                        &input.name,
                        input.resp3,
                        &mut fx.0.replies,
                        &mut fx.0.unblock,
                    )
                    .is_some()
                {
                    SpecialCommandResult::Handled(CommandDisposition::Completed)
                } else {
                    SpecialCommandResult::NotHandled
                }
            }
            DispatchGroup::ClientAdmin => {
                if self.dispatch_client_admin_group(input, fx) {
                    SpecialCommandResult::Handled(CommandDisposition::Completed)
                } else {
                    SpecialCommandResult::NotHandled
                }
            }
            _ => self.dispatch_default_group(input, fx),
        };
        let reply_end =
            self.attribute_special_command_replies(group, &input.name, reply_start, result, fx);
        match result {
            SpecialCommandResult::NotHandled => None,
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
                    return Some(LoopAction::Break);
                }
                let Some(connection) = self.connection_registry.get(input.token) else {
                    return Some(LoopAction::Break);
                };
                if disposition.stops_read_loop() || connection.blocked.is_some() {
                    Some(LoopAction::Break)
                } else {
                    Some(LoopAction::Continue)
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
            DispatchGroup::Transport if name == "replconf" => reply_start.saturating_add(1),
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
        let (suppress_reply, visible_reply_bytes, lazyfree_objects) =
            self.take_special_reply_effects(token);
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

    fn take_special_reply_effects(
        &mut self,
        token: Token,
    ) -> (bool, usize, Vec<crate::foundation::io::store::Robj>) {
        let connection = &mut self.connection_registry[token];
        let mut effects = connection
            .take_command_effects(CommandEffectDrain::Completed)
            .expect("a completed special command drains its connection effects exactly once");
        let suppress_reply = effects.should_suppress_reply();
        let visible_reply_bytes = if suppress_reply {
            0
        } else {
            effects.reply_bytes()
        };
        let lazyfree_objects = effects.take_lazyfree_objects();
        connection.finish_reply_cycle();
        (suppress_reply, visible_reply_bytes, lazyfree_objects)
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

    fn dispatch_default_group(
        &mut self,
        input: &ParsedCommandInput,
        fx: &mut CommandEffects,
    ) -> SpecialCommandResult {
        let connection_token = input.token.0;
        let token_value = input.token.0;
        let args = &input.args;
        let name_lower = input.name.as_str();
        let resp3 = input.resp3;
        if self.reject_subscribe_mode(input, fx) {
            return SpecialCommandResult::Handled(CommandDisposition::Completed);
        }

        if name_lower.eq_ignore_ascii_case("exec") {
            self.connection_registry[input.token]
                .transaction
                .set_exec_arguments(args.args.clone());
        }
        let exec_result = self.try_handle_exec(
            CommandTarget::new(Token(token_value)),
            std::str::from_utf8(&args.name).unwrap_or(name_lower),
            resp3,
            fx,
        );
        if let SpecialCommandResult::Handled(disposition) = exec_result {
            if disposition == CommandDisposition::ConnectionGone {
                return exec_result;
            }
            let internal_monitor =
                std::mem::take(&mut self.ctx.script_mut().pending_internal_monitor);
            self.deliver_monitor(internal_monitor);
            return SpecialCommandResult::Handled(disposition);
        }

        if self
            .try_handle_multi_queue(input.token, args, name_lower, resp3, &mut fx.replies)
            .is_some()
        {
            return SpecialCommandResult::Handled(CommandDisposition::Completed);
        }

        let pubsub_handled = {
            let effects = &mut fx.0;
            self.try_handle_pubsub_cmd(
                connection_token,
                args,
                name_lower,
                resp3,
                &mut effects.replies,
                &mut effects.publish,
            )
            .is_some()
        };
        if pubsub_handled {
            return SpecialCommandResult::Handled(CommandDisposition::Completed);
        }

        SpecialCommandResult::NotHandled
    }

    fn reject_subscribe_mode(
        &mut self,
        input: &ParsedCommandInput,
        fx: &mut CommandEffects,
    ) -> bool {
        let conn = &self.connection_registry[input.token];
        let subscribed = conn.sub_channel_count > 0
            || conn.psub_channel_count > 0
            || conn.ssub_channel_count > 0;
        if !subscribed || input.resp3 || Self::is_subscribe_mode_allowed(&input.name) {
            return false;
        }
        let reply = Value::error(format!(
            "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            input.name
        ));
        self.ctx.end_cmd(&input.name, true);
        super::record_error_reply(&mut self.ctx, &reply, None);
        fx.replies.push(serialize_to_vec_resp2(&reply));
        true
    }

    fn dispatch_client_admin_group(
        &mut self,
        input: &ParsedCommandInput,
        fx: &mut CommandEffects,
    ) -> bool {
        let connection_token = input.token.0;
        let token_value = input.token.0;
        let args = &input.args;
        let name_lower = input.name.as_str();
        let resp3 = input.resp3;
        if self
            .try_handle_multi_queue(input.token, args, name_lower, resp3, &mut fx.replies)
            .is_some()
        {
            return true;
        }
        self.try_handle_client_admin(
            connection_token,
            token_value,
            args,
            name_lower,
            resp3,
            &mut fx.0.replies,
            &mut fx.0.unblock,
        )
        .is_some()
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
        let dirty_keys = std::mem::take(&mut effects.watch_dirty);
        self.wakeup_blocked_on_keys_with_effect_queue(&dirty_keys, None, None, Some(effects));
    }
}
