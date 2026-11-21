//! Linear orchestration for one already parsed command.

use bytes::Bytes;

use super::command_exec::CommandPropagationInput;
use super::side_effects::CommandEffects;
use super::{CmdPreconditions, DispatchAction, PostponeReason, ServerWorkerState};
use crate::engine::command::CommandError;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{BlockedCmd, CommandEffectDrain, ConnectionState};
use crate::runtime::server::worker::command::invocation::CommandInvocation;
use crate::runtime::server::worker::command::outcome::{
    CommandCompletion, CommandDisposition, CommandExecution, CommandExecutionStatus,
};

impl ServerWorkerState {
    pub(super) fn execute_command_and_handle_result(
        &mut self,
        invocation: CommandInvocation<'_>,
        resolved_executor: Option<crate::framework::CommandExecutor>,
        resolved_preconditions: Option<super::CmdPreconditions>,
        fx: &mut CommandEffects,
    ) -> CommandDisposition {
        let Ok((connection_token, client_eviction_was_enabled)) =
            self.prepare_command_resources(invocation, fx)
        else {
            return CommandDisposition::ConnectionGone;
        };
        let was_blocked = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_some();
        self.register_active_persistence_child();
        let Ok(execution) = self.execute_invocation(
            invocation,
            connection_token,
            resolved_executor,
            resolved_preconditions,
        ) else {
            return CommandDisposition::ConnectionGone;
        };
        self.finish_invocation(
            invocation,
            execution,
            was_blocked,
            client_eviction_was_enabled,
            fx,
        )
    }

    fn prepare_command_resources(
        &mut self,
        invocation: CommandInvocation<'_>,
        effects: &mut CommandEffects,
    ) -> Result<(usize, bool), CommandDisposition> {
        let token = invocation
            .target
            .resolve(self)
            .ok_or(CommandDisposition::ConnectionGone)?;
        let connection_token = token.0;
        // Resolve the client memory limit once. Both `refresh_client_memory_usage`
        // and `evict_clients` independently call `effective_client_memory_limit`,
        // which parses the `maxmemory-clients` config string on every call. On
        // the common no-limit hot path (PING, GET, SET, ...) the limit is `None`
        // and both helpers return immediately, so skipping them avoids two
        // redundant config-string parses per command.
        let client_memory_limit = self.effective_client_memory_limit();
        let client_eviction_was_enabled = client_memory_limit.is_some();
        if client_memory_limit.is_some() {
            self.refresh_client_memory_usage_with_pending_replies(
                invocation.target.token(),
                invocation.pending_replies,
            );
            self.evict_clients();
        }
        if invocation.target.resolve(self).is_none() {
            return Err(CommandDisposition::ConnectionGone);
        }
        if invocation.flags & crate::engine::command::CMD_WRITE != 0 {
            self.ctx.set_suppress_eviction_until_write(false);
        }
        let eviction = self.perform_evictions();
        if !eviction.evicted.is_empty() {
            self.command_eviction_ran_this_turn = true;
        }
        self.apply_command_evictions(invocation.target.token_value(), eviction.evicted, effects);
        self.ctx.refresh_dict_resize_budget();
        Ok((connection_token, client_eviction_was_enabled))
    }

    fn execute_invocation(
        &mut self,
        invocation: CommandInvocation<'_>,
        connection_token: usize,
        resolved_executor: Option<crate::framework::CommandExecutor>,
        resolved_preconditions: Option<super::CmdPreconditions>,
    ) -> Result<CommandExecution, CommandDisposition> {
        // Use the executor and preconditions resolved during admission when
        // available, avoiding a second `registry.lookup` HashMap lookup on the
        // hot path. Unknown commands (resolved values are `None`) fall back to
        // the lookup below to produce the standard rejection reply.
        let (executor, preconditions) = match (resolved_executor, resolved_preconditions) {
            (Some(executor), Some(preconditions)) => (executor, preconditions),
            _ => {
                let Some(spec) = self.registry.lookup(&invocation.args.name) else {
                    return Ok(CommandExecution::rejected(unknown_command_reply(
                        invocation.args,
                        self.connection_registry[invocation.target.token()].session_id(),
                        &self.ctx,
                    )));
                };
                let acl_categories = super::subcmd_flags::effective_acl_categories(
                    spec.name.as_str(),
                    spec.acl_categories,
                    invocation.args.args.first().map(|arg| arg.as_ref()),
                );
                let preconditions = CmdPreconditions {
                    name: spec.name.clone(),
                    arity: spec.arity,
                    // `invocation.flags` already carries the effective per-call
                    // flags resolved during admission; reuse them.
                    flags: invocation.flags,
                    first_key: spec.first_key,
                    last_key: spec.last_key,
                    key_step: spec.key_step,
                    key_flags: spec.key_flags,
                    acl_categories,
                    get_keys: spec.get_keys,
                };
                (spec.executor, preconditions)
            }
        };
        let decision = self.pre_dispatch_exec(connection_token, preconditions, invocation.args);
        match decision.action {
            DispatchAction::Execute => {
                let is_script_debug = invocation.args.name.eq_ignore_ascii_case(b"script")
                    && invocation
                        .args
                        .args
                        .first()
                        .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"debug"));
                let has_pending_replies = !invocation.pending_replies.is_empty()
                    || self.connection_registry[invocation.target.token()].has_pending_output()
                    || self.io_threads.is_write_inflight(invocation.target.token());
                if is_script_debug && has_pending_replies {
                    return Ok(CommandExecution::rejected(Value::error(
                        "ERR SCRIPT DEBUG must be called outside a pipeline",
                    )));
                }
                let call_flags = decision.call_flags;
                if call_flags.client_asking {
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .asking = true;
                }
                if super::monitor::is_explicit_script_monitor_command(invocation.name) {
                    let connection = &self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)];
                    let mut argv = Vec::with_capacity(invocation.args.args.len() + 1);
                    argv.push(invocation.args.name.clone());
                    argv.extend(invocation.args.args.iter().cloned());
                    if let Some(event) = self.ctx.monitor_event(
                        connection.selected_db,
                        connection.peer_addr.to_string(),
                        argv,
                        invocation.flags,
                        true,
                    ) {
                        self.ctx.script_mut().pending_internal_monitor.push(event);
                    }
                }
                let reply = self.execute_registered_handler(
                    invocation.dispatch_group,
                    connection_token,
                    executor,
                    invocation.args,
                );
                let connection_token = invocation
                    .target
                    .resolve(self)
                    .ok_or(CommandDisposition::ConnectionGone)?;
                if call_flags.client_asking && super::consumes_asking(invocation.name) {
                    self.connection_registry[connection_token].asking = false;
                }
                // Duration is measured by end_cmd() from begin_cmd()'s
                // timestamp, matching C's full-command accounting and
                // avoiding a second clock read on the hot path.
                Ok(CommandExecution::executed(reply))
            }
            DispatchAction::Rejected => Ok(CommandExecution::rejected(
                decision.reject_reply.unwrap_or(Value::null()),
            )),
            DispatchAction::Postpone(reason) => {
                let is_throttled = reason == PostponeReason::ReplicationThrottle;
                let is_forkless = reason == PostponeReason::ForklessKey;
                let command = match reason {
                    PostponeReason::ClientPause => BlockedCmd::Paused,
                    PostponeReason::ForklessKey => BlockedCmd::Forkless,
                    PostponeReason::ReplicationThrottle => BlockedCmd::Throttled,
                    PostponeReason::SlotMigration(job_id) => BlockedCmd::SlotMigration(job_id),
                };
                let connection = &mut self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)];
                connection.postpone_command(
                    command,
                    Vec::new(),
                    0,
                    invocation.args,
                    connection.request_bytes_current_command(),
                );
                if is_throttled {
                    self.ctx
                        .throttled_clients_mut()
                        .insert(invocation.target.token());
                    self.schedule_replication_throttle();
                } else {
                    self.ctx
                        .paused_clients_mut()
                        .insert(invocation.target.token());
                    if is_forkless {
                        self.ctx
                            .forkless_clients_mut()
                            .insert(invocation.target.token());
                    }
                }
                Ok(CommandExecution::executed(Value::Null))
            }
        }
    }

    fn finish_invocation(
        &mut self,
        invocation: CommandInvocation<'_>,
        execution: CommandExecution,
        was_blocked: bool,
        client_eviction_was_enabled: bool,
        fx: &mut CommandEffects,
    ) -> CommandDisposition {
        let Some(token) = invocation.target.resolve(self) else {
            return CommandDisposition::ConnectionGone;
        };
        let notification_effects =
            crate::framework::capabilities::module::dispatch_module_keyspace_notifications(
                &mut self.connection_registry[token],
                &mut self.ctx,
            );
        self.connection_registry[token].record_module_notification_effects(notification_effects);
        let Some(token) = invocation.target.resolve(self) else {
            return CommandDisposition::ConnectionGone;
        };
        let connection_token = token.0;
        let is_blocked = self.connection_registry[token].blocked.is_some();
        let mut connection_effects = if is_blocked {
            None
        } else {
            self.connection_registry[token].take_command_effects(CommandEffectDrain::Completed)
        };
        self.apply_client_memory_limit_change(client_eviction_was_enabled);
        let mut reply = self.apply_post_command_intents(
            invocation.origin,
            connection_token,
            connection_effects.as_mut(),
            execution.reply,
        );
        self.register_active_persistence_child();
        if execution.status == CommandExecutionStatus::Executed
            && invocation.name == "flushall"
            && !matches!(reply, Value::Error(_))
        {
            self.abort_diskless_relay_for_dataset_flush();
        }
        let propagation_rejected =
            execution.status.is_rejected() || matches!(&reply, Value::Error(_));
        let stats_name = super::command_stats_name(invocation.name, invocation.args);
        self.refresh_connection_priorities();
        let rejected = execution.status.is_rejected();
        // Measure full-command duration from begin_cmd()'s timestamp, matching
        // C. Only executed commands keep the priority-command flag so that
        // record_priority_command fires for them; rejected/postponed commands
        // must not skew priority accounting.
        let duration_us = if execution.status == CommandExecutionStatus::Executed {
            self.ctx.end_cmd(&stats_name, rejected)
        } else {
            let was_priority = self.ctx.stats_mut().set_priority_command(false);
            let duration_us = self.ctx.end_cmd(&stats_name, rejected);
            self.ctx.stats_mut().set_priority_command(was_priority);
            duration_us
        };
        let disposition = self.complete_command(
            CommandCompletion {
                invocation,
                reply: std::mem::replace(&mut reply, Value::Null),
                status: execution.status,
                duration_us,
                was_blocked_before: was_blocked,
            },
            connection_effects.as_mut(),
            fx,
        );
        if disposition.stops_read_loop() {
            return disposition;
        }
        self.capture_direct_propagation(
            invocation,
            propagation_rejected,
            connection_effects.expect("completed command effects were drained once"),
            fx,
        )
    }

    fn capture_direct_propagation(
        &mut self,
        invocation: CommandInvocation<'_>,
        rejected: bool,
        connection_effects: crate::framework::capabilities::connection::ConnectionCommandEffects,
        fx: &mut CommandEffects,
    ) -> CommandDisposition {
        let Some(token) = invocation.target.resolve(self) else {
            return CommandDisposition::ConnectionGone;
        };
        let connection_token = token.0;
        let suppress_reply = connection_effects.should_suppress_reply();
        fx.swapdb = self.build_command_propagation(
            CommandPropagationInput {
                connection_token,
                token_value: invocation.target.token_value(),
                args: invocation.args,
                name: invocation.name,
                command_flags: invocation.flags,
                rejected,
            },
            connection_effects,
            fx,
        );
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        if suppress_reply {
            fx.clear_replies();
        }
        connection.finish_reply_cycle();
        if connection.state == ConnectionState::Closing {
            fx.mark_closing = true;
            CommandDisposition::Closing
        } else {
            CommandDisposition::Completed
        }
    }
}

fn unknown_command_reply(
    args: &crate::engine::command::CommandArgs,
    session: crate::engine::command::SessionId,
    ctx: &crate::framework::context::ServerContext,
) -> Value {
    if ctx.script().debug_modes.contains_key(&session) {
        if (args.name.eq_ignore_ascii_case(b"print") || args.name.eq_ignore_ascii_case(b"p"))
            && args.args.len() == 1
        {
            let value = (ctx.script().debug_session == Some(session))
                .then(|| std::str::from_utf8(&args.args[0]).ok())
                .flatten()
                .and_then(crate::engine::extension::lua::runtime::debug_global);
            return Value::SimpleString(Bytes::from(match value {
                Some(value) => format!("<value> {value:?}"),
                None => "No such variable.".to_string(),
            }));
        }
        if args.name.eq_ignore_ascii_case(b"server")
            && args
                .args
                .first()
                .is_some_and(|arg| arg.eq_ignore_ascii_case(b"ping"))
        {
            return Value::SimpleString(Bytes::from_static(b"PONG"));
        }
        if args.name.eq_ignore_ascii_case(b"redis") {
            return Value::SimpleString(Bytes::from_static(b"Unknown command called from script"));
        }
        return Value::error("ERR Unknown debugger command");
    }
    CommandError::unknown(&args.name)
}
