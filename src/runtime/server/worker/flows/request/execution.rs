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
use crate::runtime::server::worker::flows::request::runtime_command::execute_runtime_command;

impl ServerWorkerState {
    pub(crate) fn invoke_command_executor(
        &mut self,
        token: Token,
        executor: CommandExecutor,
        args: &CommandArgs,
    ) -> Value {
        if let Some(prepare) = executor.hosted() {
            return match prepare(&mut self.connection_registry[token], args, &mut self.ctx) {
                Ok(task) => self.run_prepared_host_task(token, task),
                Err(reply) => reply,
            };
        }
        if let Some(handler) = executor.runtime_handler() {
            return execute_runtime_command(
                &mut self.connection_registry[token],
                args,
                &mut self.ctx,
                handler,
            );
        }
        executor.execute(&mut self.connection_registry[token], args, &mut self.ctx)
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
