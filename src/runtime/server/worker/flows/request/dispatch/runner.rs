//! Linear orchestration for one already parsed command.

use bytes::Bytes;

use super::command_exec::CommandPropagationInput;
use super::side_effects::CommandEffects;
use super::{CmdCallFlags, CmdPreconditions, DispatchAction, PostponeReason, ServerWorkerState};
use crate::engine::command::CommandError;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{BlockedCmd, CommandEffectDrain, ConnectionState};
use crate::runtime::server::worker::flows::request::invocation::CommandInvocation;
use crate::runtime::server::worker::flows::request::outcome::{
    CommandCompletion, CommandDisposition, CommandExecution, CommandExecutionStatus, CommandOutcome,
};

impl ServerWorkerState {
    pub(super) fn execute_command_and_handle_result(
        &mut self,
        invocation: CommandInvocation<'_>,
    ) -> CommandOutcome {
        let mut effects = CommandEffects::default();
        let Ok((connection_token, client_eviction_was_enabled)) =
            self.prepare_command_resources(invocation, &mut effects)
        else {
            return CommandOutcome::new(CommandDisposition::ConnectionGone, effects);
        };
        let was_blocked = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .blocked
        .is_some();
        self.register_active_persistence_child();
        let Ok(execution) = self.execute_invocation(invocation, connection_token) else {
            return CommandOutcome::new(CommandDisposition::ConnectionGone, effects);
        };
        self.finish_invocation(
            invocation,
            execution,
            was_blocked,
            client_eviction_was_enabled,
            effects,
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
        let client_eviction_was_enabled = self.effective_client_memory_limit().is_some();
        self.refresh_client_memory_usage_with_pending_replies(
            invocation.target.token(),
            invocation.pending_replies,
        );
        self.evict_clients();
        if invocation.target.resolve(self).is_none() {
            return Err(CommandDisposition::ConnectionGone);
        }
        let eviction = self.perform_evictions();
        self.apply_command_evictions(invocation.target.token_value(), eviction.evicted, effects);
        self.ctx.refresh_dict_resize_budget();
        Ok((connection_token, client_eviction_was_enabled))
    }

    fn execute_invocation(
        &mut self,
        invocation: CommandInvocation<'_>,
        connection_token: usize,
    ) -> Result<CommandExecution, CommandDisposition> {
        let Some(spec) = self.registry.lookup(&invocation.args.name) else {
            return Ok(CommandExecution::rejected(unknown_command_reply(
                invocation.args,
            )));
        };
        let executor = spec.executor;
        let preconditions = CmdPreconditions {
            name: spec.name.clone(),
            arity: spec.arity,
            flags: super::subcmd_flags::effective_command_flags_for_call(
                spec.name.as_str(),
                spec.flags,
                invocation.args,
                &self.ctx,
            ),
            first_key: spec.first_key,
            last_key: spec.last_key,
            key_step: spec.key_step,
            key_flags: spec.key_flags,
            acl_categories: super::subcmd_flags::effective_acl_categories(
                spec.name.as_str(),
                spec.acl_categories,
                invocation.args.args.first().map(|arg| arg.as_ref()),
            ),
            get_keys: spec.get_keys,
        };
        let decision = self.pre_dispatch_exec(connection_token, preconditions, invocation.args);
        match decision.action {
            DispatchAction::Execute => self.execute_admitted_invocation(
                invocation,
                connection_token,
                executor,
                decision.call_flags,
            ),
            DispatchAction::Rejected => Ok(CommandExecution::rejected(
                decision.reject_reply.unwrap_or(Value::null()),
            )),
            DispatchAction::Postpone(reason) => {
                self.block_postponed_invocation(invocation, connection_token, reason);
                Ok(CommandExecution::executed(Value::Null))
            }
        }
    }

    fn execute_admitted_invocation(
        &mut self,
        invocation: CommandInvocation<'_>,
        connection_token: usize,
        executor: crate::framework::spec::CommandExecutor,
        call_flags: CmdCallFlags,
    ) -> Result<CommandExecution, CommandDisposition> {
        if call_flags.client_asking {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .asking = true;
        }
        self.capture_outer_script_monitor(invocation, connection_token);
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
        Ok(CommandExecution::executed(reply))
    }

    fn capture_outer_script_monitor(
        &mut self,
        invocation: CommandInvocation<'_>,
        connection_token: usize,
    ) {
        if !super::monitor::is_explicit_script_monitor_command(invocation.name) {
            return;
        }
        let connection =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
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

    fn block_postponed_invocation(
        &mut self,
        invocation: CommandInvocation<'_>,
        connection_token: usize,
        reason: PostponeReason,
    ) {
        let command = match reason {
            PostponeReason::ClientPause => BlockedCmd::Paused,
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
        self.ctx
            .paused_clients_mut()
            .insert(invocation.target.token());
    }

    fn finish_invocation(
        &mut self,
        invocation: CommandInvocation<'_>,
        execution: CommandExecution,
        was_blocked: bool,
        client_eviction_was_enabled: bool,
        mut effects: CommandEffects,
    ) -> CommandOutcome {
        let Some(token) = invocation.target.resolve(self) else {
            return CommandOutcome::new(CommandDisposition::ConnectionGone, effects);
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
        let duration_us = self
            .ctx
            .end_cmd(&stats_name, execution.status.is_rejected());
        let disposition = self.complete_command(
            CommandCompletion {
                invocation,
                reply: std::mem::replace(&mut reply, Value::Null),
                status: execution.status,
                duration_us,
                was_blocked_before: was_blocked,
            },
            connection_effects.as_mut(),
            &mut effects,
        );
        if disposition.stops_read_loop() {
            return CommandOutcome::new(disposition, effects);
        }
        self.capture_direct_propagation(
            invocation,
            propagation_rejected,
            connection_effects.expect("completed command effects were drained once"),
            effects,
        )
    }

    fn capture_direct_propagation(
        &mut self,
        invocation: CommandInvocation<'_>,
        rejected: bool,
        connection_effects: crate::framework::capabilities::connection::ConnectionCommandEffects,
        mut effects: CommandEffects,
    ) -> CommandOutcome {
        let Some(token) = invocation.target.resolve(self) else {
            return CommandOutcome::new(CommandDisposition::ConnectionGone, effects);
        };
        let connection_token = token.0;
        let suppress_reply = connection_effects.should_suppress_reply();
        effects.swapdb = self.build_command_propagation(
            CommandPropagationInput {
                connection_token,
                token_value: invocation.target.token_value(),
                args: invocation.args,
                name: invocation.name,
                command_flags: invocation.flags,
                rejected,
            },
            connection_effects,
            &mut effects,
        );
        let connection = &mut self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)];
        if suppress_reply {
            effects.clear_replies();
        }
        connection.finish_reply_cycle();
        if connection.state == ConnectionState::Closing {
            effects.mark_closing = true;
            CommandOutcome::new(CommandDisposition::Closing, effects)
        } else {
            CommandOutcome::new(CommandDisposition::Completed, effects)
        }
    }
}

fn unknown_command_reply(args: &crate::engine::command::CommandArgs) -> Value {
    if crate::framework::commands::scripting::script_cmd::script_debug_enabled() {
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
