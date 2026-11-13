//! Shared handler invocation and post-handler runtime intents.
//! Direct commands and EXEC items deliberately have different admission
//! policies, but once admitted they use the same hosted/immediate handler
//! boundary and the same ordered intent executor.

use super::super::ServerWorkerState;
use super::invocation::CommandOrigin;
use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::ConnectionCommandEffects;
use crate::framework::capabilities::io::Token;
use crate::framework::spec::CommandExecutor;
use crate::runtime::server::worker::command::runtime_command::execute_runtime_command;

impl ServerWorkerState {
    pub(crate) fn invoke_command_executor(
        &mut self,
        token: Token,
        executor: CommandExecutor,
        args: &CommandArgs,
    ) -> Value {
        let had_migration_role = self.connection_registry[token]
            .slot_migration_role
            .is_some();
        let samples_independently =
            self.command_execution_depth == 0 && self.priority_command_token != Some(token);
        let owns_priority_cycle = samples_independently
            && self.is_priority_connection(token)
            && self.begin_priority_cycle();
        let started = std::time::Instant::now();
        self.command_execution_depth += 1;
        let reply = match executor {
            CommandExecutor::Hosted { prepare, .. } => {
                match prepare(&mut self.connection_registry[token], args, &mut self.ctx) {
                    Ok(task) => self.run_prepared_host_task(token, task),
                    Err(reply) => reply,
                }
            }
            CommandExecutor::Runtime(handler) => execute_runtime_command(
                &mut self.connection_registry[token],
                args,
                &mut self.ctx,
                handler,
            ),
            // CommandExecutor is Copy, so the original execute entry point stays
            // available here, preserving its hotkeys scope wrapper for Immediate.
            CommandExecutor::Immediate(_) => {
                executor.execute(&mut self.connection_registry[token], args, &mut self.ctx)
            }
        };
        self.kill_acl_subscribers();
        self.drain_acl_kills_for_command(Some(token));
        self.command_execution_depth -= 1;
        let duration_us = (started.elapsed().as_micros() as u64).max(1);
        self.refresh_connection_priorities();
        if !had_migration_role
            && self.connection_registry.get(token).is_some_and(|connection| {
                matches!(
                    connection.slot_migration_role,
                    Some(crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(_))
                )
            })
        {
            self.prioritize_connection(token);
        }
        if samples_independently && self.is_priority_connection(token) {
            self.ctx.stats_mut().record_priority_command(duration_us);
        }
        self.finish_priority_cycle(owns_priority_cycle);
        reply
    }

    pub(crate) fn apply_post_command_intents(
        &mut self,
        origin: CommandOrigin,
        connection_token: usize,
        mut connection_effects: Option<&mut ConnectionCommandEffects>,
        mut reply: Value,
    ) -> Value {
        self.apply_pending_cluster_role_actions();
        if origin == CommandOrigin::Direct {
            let io_threads_reply = self.resolve_io_threads_reconfigure_request();
            if let Some(result) = io_threads_reply {
                allow_default_reply(&mut connection_effects);
                let failed = matches!(result, Value::Error(_));
                reply = result;
                if failed {
                    let _ = self.ctx.take_tcp_rebind_request();
                    let _ = self.ctx.take_tls_reconfigure_request();
                    return reply;
                }
            }
        }
        self.apply_connection_and_process_intents(connection_token, connection_effects, reply)
    }

    fn apply_connection_and_process_intents(
        &mut self,
        connection_token: usize,
        mut connection_effects: Option<&mut ConnectionCommandEffects>,
        reply: Value,
    ) -> Value {
        let tcp_reply = self.resolve_tcp_rebind_request(connection_token);
        if matches!(tcp_reply.as_ref(), Some(Value::Error(_))) {
            let _ = self.ctx.take_tls_reconfigure_request();
            allow_default_reply(&mut connection_effects);
            return tcp_reply.expect("checked TCP rebind error");
        }
        if let Some(reply) = self.resolve_tls_reconfigure_request(connection_token) {
            allow_default_reply(&mut connection_effects);
            return reply;
        }
        if let Some(reply) = tcp_reply {
            allow_default_reply(&mut connection_effects);
            return reply;
        }
        let cluster_config_save = connection_effects
            .as_deref_mut()
            .and_then(|effects| effects.take_cluster_config_save());
        if let Some(reply) =
            self.resolve_cluster_config_save_request(connection_token, cluster_config_save)
        {
            allow_default_reply(&mut connection_effects);
            return reply;
        }
        if let Some(reply) = self.resolve_server_restart_request(connection_token) {
            allow_default_reply(&mut connection_effects);
            return reply;
        }
        reply
    }
}

fn allow_default_reply(effects: &mut Option<&mut ConnectionCommandEffects>) {
    if let Some(effects) = effects.as_deref_mut() {
        effects.allow_default_reply();
    }
}
