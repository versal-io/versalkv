//! EXEC admission, execution, and completion as an explicit linear pipeline.

use super::{ExecPrechecks, ServerWorkerState};
use crate::engine::command::context::CommandConn;
use crate::engine::distributed::cluster::ClusterRouteDecision;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::TransactionEnd;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::flows::request::dispatch::{
    CmdCallFlags, CommandEffects, DispatchAction,
};
use crate::runtime::server::worker::flows::request::invocation::CommandTarget;
use crate::runtime::server::worker::flows::request::outcome::{
    CommandDisposition, CommandExecutionStatus, SpecialCommandResult,
};
use bytes::Bytes;
use std::time::Instant;

enum ExecDecision {
    Execute,
    Postpone,
    WithoutTransaction,
    RouteRejected(Value),
    WatchedKeyChanged,
    Abort(Value),
}

enum StandaloneExecDecision {
    Allow,
    Reject(Value),
    Postpone,
}

struct ExecDecisionInputs {
    cluster_route_error: Option<ClusterRouteDecision>,
    standalone: StandaloneExecDecision,
    prechecks: ExecPrechecks,
    should_pause: bool,
}

struct ExecExecution {
    reply: Value,
    status: CommandExecutionStatus,
    disposition: CommandDisposition,
}

impl ExecExecution {
    fn completed(reply: Value, status: CommandExecutionStatus) -> Self {
        Self {
            reply,
            status,
            disposition: CommandDisposition::Completed,
        }
    }

    fn blocked() -> Self {
        Self {
            reply: Value::Null,
            status: CommandExecutionStatus::Executed,
            disposition: CommandDisposition::Blocked,
        }
    }
}

struct ExecAccounting {
    duration_us: u64,
    transaction_input_bytes: u64,
}

#[derive(Clone, Copy)]
struct ExecContext<'a> {
    started: Instant,
    target: CommandTarget,
    name: &'a str,
    log_name: &'a str,
    resp3: bool,
    cluster_metric_slot: Option<u16>,
    transaction_input_bytes: u64,
}

impl ServerWorkerState {
    pub(crate) fn try_handle_exec(
        &mut self,
        target: CommandTarget,
        name_lower: &str,
        resp3: bool,
        effects: &mut CommandEffects,
    ) -> SpecialCommandResult {
        if !name_lower.eq_ignore_ascii_case("exec") {
            return SpecialCommandResult::NotHandled;
        }
        let Some(token) = target.resolve(self) else {
            return SpecialCommandResult::Handled(CommandDisposition::ConnectionGone);
        };
        let context = ExecContext {
            started: Instant::now(),
            target,
            name: "exec",
            log_name: name_lower,
            resp3,
            cluster_metric_slot: self.exec_cluster_metric_slot(token.0),
            transaction_input_bytes: self.connection_registry[token]
                .transaction
                .input_bytes_with_exec(
                    self.connection_registry[token].request_bytes_current_command() as u64,
                )
                .unwrap_or(0),
        };
        let decision = self.prepare_exec_decision(token, effects);
        let execution = self.apply_exec_decision(context, decision, effects);
        let disposition = self.finish_exec(context, execution, effects);
        SpecialCommandResult::Handled(disposition)
    }

    fn prepare_exec_decision(
        &mut self,
        token: Token,
        effects: &mut CommandEffects,
    ) -> ExecDecision {
        let cluster_route_error = self.exec_cluster_route_decision(token.0);
        let standalone = self.exec_standalone_replica_decision(token.0);
        if self.connection_registry[token].transaction.is_active() && cluster_route_error.is_none()
        {
            let sinks = effects.exec_sinks();
            self.exec_expire_watched_keys(
                token.0,
                sinks.watch_dirty,
                sinks.replication_only,
                sinks.aof_only,
            );
        }
        let prechecks = self.exec_compute_prechecks(token.0);
        let should_pause = self.exec_should_pause(token.0);
        self.decide_exec(
            token,
            ExecDecisionInputs {
                cluster_route_error,
                standalone,
                prechecks,
                should_pause,
            },
        )
    }

    fn decide_exec(&self, token: Token, inputs: ExecDecisionInputs) -> ExecDecision {
        let transaction = &self.connection_registry[token].transaction;
        if !transaction.is_active() {
            return ExecDecision::WithoutTransaction;
        }
        if let Some(decision) = inputs.cluster_route_error {
            return ExecDecision::RouteRejected(super::super::dispatch::route_error_value(
                decision,
            ));
        }
        match inputs.standalone {
            StandaloneExecDecision::Reject(reply) => {
                return ExecDecision::RouteRejected(reply);
            }
            StandaloneExecDecision::Postpone => return ExecDecision::Postpone,
            StandaloneExecDecision::Allow => {}
        }
        if transaction.is_watch_dirty() {
            return ExecDecision::WatchedKeyChanged;
        }
        if let Some(reply) = self.exec_precheck_abort_reply(inputs.prechecks) {
            return ExecDecision::Abort(reply);
        }
        if transaction.has_queue_error() {
            return ExecDecision::Abort(Value::error(
                "EXECABORT Transaction discarded because of previous errors.",
            ));
        }
        if inputs.should_pause {
            return ExecDecision::Postpone;
        }
        ExecDecision::Execute
    }

    fn exec_precheck_abort_reply(&self, prechecks: ExecPrechecks) -> Option<Value> {
        if prechecks.oom_reject {
            return Some(Value::error(
                "EXECABORT Transaction discarded because of: OOM command not allowed when used memory > 'maxmemory'.",
            ));
        }
        if self.ctx.is_script_busy() {
            return Some(Value::error("EXECABORT Transaction discarded because of: BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."));
        }
        if prechecks.noreplicas_reject {
            return Some(Value::error(
                "EXECABORT Transaction discarded because of: NOREPLICAS Not enough good replicas to write.",
            ));
        }
        if prechecks.readonly_reject {
            return Some(Value::error(
                "EXECABORT Transaction discarded because of: READONLY You can't write against a read only replica.",
            ));
        }
        prechecks.stale_reject.then(|| {
            Value::error(
                "EXECABORT Transaction discarded because of: MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
            )
        })
    }

    fn apply_exec_decision(
        &mut self,
        context: ExecContext<'_>,
        decision: ExecDecision,
        effects: &mut CommandEffects,
    ) -> ExecExecution {
        match decision {
            ExecDecision::Execute => self.execute_queued_transaction(context, effects),
            ExecDecision::Postpone => {
                self.block_exec(context.target.token());
                ExecExecution::blocked()
            }
            ExecDecision::WithoutTransaction => ExecExecution::completed(
                Value::error("ERR EXEC without MULTI"),
                CommandExecutionStatus::Executed,
            ),
            ExecDecision::RouteRejected(reply) => {
                self.end_exec(context.target.token(), TransactionEnd::AdmissionRejected);
                ExecExecution::completed(reply, CommandExecutionStatus::Rejected)
            }
            ExecDecision::WatchedKeyChanged => {
                self.end_exec(context.target.token(), TransactionEnd::WatchInvalidated);
                ExecExecution::completed(Value::null_array(), CommandExecutionStatus::Executed)
            }
            ExecDecision::Abort(reply) => {
                self.end_exec(context.target.token(), TransactionEnd::AdmissionRejected);
                ExecExecution::completed(reply, CommandExecutionStatus::Executed)
            }
        }
    }

    fn end_exec(&mut self, token: Token, end: TransactionEnd) {
        self.connection_registry[token].transaction.end(end);
    }

    fn block_exec(&mut self, token: Token) {
        let connection = &mut self.connection_registry[token];
        connection.postpone_transaction_command(&crate::engine::command::CommandArgs {
            name: Bytes::from_static(b"EXEC"),
            args: Vec::new(),
        });
        self.ctx.paused_clients_mut().insert(token);
    }

    fn execute_queued_transaction(
        &mut self,
        context: ExecContext<'_>,
        effects: &mut CommandEffects,
    ) -> ExecExecution {
        self.connection_registry[context.target.token()].discard_command_effects();
        let offset_before = self.ctx.replication().primary_repl_offset();
        let (replies, publish, publish_shard) = {
            let sinks = effects.exec_sinks();
            let output = self.exec_run_commands(context.target, sinks.wakeup_only);
            self._append_exec_propagation(
                &output.propagation_ops,
                sinks.replication_only,
                sinks.aof_only,
            );
            sinks.watch_dirty.extend(output.dirty_keys);
            (output.results, output.publish, output.publish_shard)
        };
        let offset_after = self.ctx.replication().primary_repl_offset();
        if offset_after != offset_before {
            self.connection_registry[context.target.token()].set_last_write_offset(offset_after);
        }
        effects.publish.extend(publish);
        effects.publish_shard.extend(publish_shard);

        let Some(token) = context.target.resolve(self) else {
            return ExecExecution::completed(Value::Null, CommandExecutionStatus::Executed);
        };
        self.connection_registry[token]
            .transaction
            .end(TransactionEnd::ExecutionCompleted);
        ExecExecution::completed(Value::Array(replies), CommandExecutionStatus::Executed)
    }

    fn finish_exec(
        &mut self,
        context: ExecContext<'_>,
        execution: ExecExecution,
        effects: &mut CommandEffects,
    ) -> CommandDisposition {
        let Some(token) = context.target.resolve(self) else {
            return CommandDisposition::ConnectionGone;
        };
        self.capture_exec_monitor(token);
        let accounting = self.record_exec_completion(context, &execution);
        if execution.disposition == CommandDisposition::Blocked {
            self.finish_blocked_exec(token);
            return CommandDisposition::Blocked;
        }
        self.queue_exec_reply(context, execution, accounting, effects);
        CommandDisposition::Completed
    }

    fn capture_exec_monitor(&mut self, token: Token) {
        let connection = &self.connection_registry[token];
        if let Some(event) = self.ctx.monitor_event(
            connection.selected_db,
            connection.peer_addr.to_string(),
            vec![Bytes::from_static(b"exec")],
            crate::framework::generated::commands::EXEC_META.flags,
            false,
        ) {
            self.ctx.script_mut().pending_internal_monitor.push(event);
        }
    }

    fn record_exec_completion(
        &mut self,
        context: ExecContext<'_>,
        execution: &ExecExecution,
    ) -> ExecAccounting {
        let rejected = execution.status.is_rejected();
        let duration_us = self.ctx.end_cmd_with_duration(
            context.name,
            rejected,
            context.started.elapsed().as_micros() as u64,
        );
        super::super::dispatch::record_error_reply(
            &mut self.ctx,
            &execution.reply,
            (!rejected).then_some(context.name),
        );
        let Some(token) = context.target.resolve(self) else {
            return ExecAccounting {
                duration_us,
                transaction_input_bytes: 0,
            };
        };
        let connection = &mut self.connection_registry[token];
        let selected_db = connection.selected_db;
        let watched_keys = connection.transaction.watched_key_count();
        if let Some(info) = self.ctx.client_info_mut(token.0) {
            info.last_cmd = context.name.to_string();
            info.db = selected_db;
            info.watched_keys = watched_keys;
        }
        ExecAccounting {
            duration_us,
            transaction_input_bytes: context.transaction_input_bytes,
        }
    }

    fn finish_blocked_exec(&mut self, token: Token) {
        if let Some(info) = self.ctx.client_info_mut(token.0) {
            info.flags.retain(|flag| flag != 'N');
            if !info.flags.contains('b') {
                info.flags.push('b');
            }
        }
        let connection = &mut self.connection_registry[token];
        connection.cmd_count -= 1;
        connection.set_custom_reply_enabled(false);
    }

    fn queue_exec_reply(
        &mut self,
        context: ExecContext<'_>,
        execution: ExecExecution,
        accounting: ExecAccounting,
        effects: &mut CommandEffects,
    ) {
        let Some(token) = context.target.resolve(self) else {
            return;
        };
        let connection = &self.connection_registry[token];
        let should_encode = !connection.has_custom_reply();
        let encoded = should_encode.then(|| {
            if context.resp3 {
                serialize_to_vec_resp3(&execution.reply)
            } else {
                serialize_to_vec_resp2(&execution.reply)
            }
        });
        self.ctx.push_cmd_logs(
            vec![Bytes::copy_from_slice(context.log_name.as_bytes())],
            accounting.duration_us,
            &connection.peer_addr,
            connection.name.clone(),
            connection.request_bytes_current_command(),
            encoded.as_ref().map_or(0, Vec::len),
            crate::framework::generated::commands::EXEC_META.flags,
        );
        if !execution.status.is_rejected() {
            if let Some(slot) = context.cluster_metric_slot {
                effects.defer_slot_metric(
                    crate::runtime::server::worker::flows::request::dispatch::DeferredSlotMetric {
                        slot,
                        cpu_usec: accounting.duration_us,
                        network_bytes_in: accounting.transaction_input_bytes,
                    },
                );
            }
        }
        if let Some(frame) = encoded {
            effects.replies.push(frame);
        }
    }

    fn exec_standalone_replica_decision(&self, connection_token: usize) -> StandaloneExecDecision {
        if !self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .is_active()
        {
            return StandaloneExecDecision::Allow;
        }
        let command_flags = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .transaction
        .queued_commands()
        .iter()
        .fold(0, |flags, command| {
            flags
                | self.registry.lookup(&command.name).map_or(0, |spec| {
                    super::super::dispatch::subcmd_flags::effective_command_flags_for_call(
                        spec.name.as_str(),
                        spec.flags,
                        command,
                        &self.ctx,
                    )
                })
        });
        let outcome = self.standalone_replica_redirect_outcome(
            connection_token,
            command_flags,
            CmdCallFlags::default(),
        );
        match outcome.map(|outcome| (outcome.action, outcome.reject_reply)) {
            Some((DispatchAction::Rejected, Some(reply))) => StandaloneExecDecision::Reject(reply),
            Some((DispatchAction::Rejected, None)) => StandaloneExecDecision::Reject(Value::null()),
            Some((DispatchAction::Postpone(_), _)) => StandaloneExecDecision::Postpone,
            Some((DispatchAction::Execute, _)) | None => StandaloneExecDecision::Allow,
        }
    }
}
