use super::super::ServerWorkerState;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::time::Duration;

impl ServerWorkerState {
    pub(crate) fn resolve_server_restart_request(
        &mut self,
        connection_token: usize,
    ) -> Option<Value> {
        let intent = self.ctx.take_server_restart_request()?;
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .set_custom_reply(false);

        if intent.config_rewrite {
            if self.ctx.cluster_state().is_some() {
                if let Err(error) = self.save_cluster_config_now() {
                    return Some(Value::error(format!(
                        "ERR failed to restart the server: {error}"
                    )));
                }
            }
            if self.ctx.config().server.config_file.is_some() {
                if let Value::Error(error) =
                    crate::framework::commands::server::config::config_rewrite(&mut self.ctx)
                {
                    return Some(Value::Error(error));
                }
            }
        }
        if intent.graceful {
            self.register_active_persistence_child();
            let shutdown_result =
                crate::runtime::server::supervisor::shutdown::prepare_for_shutdown(
                    &mut self.ctx,
                    crate::engine::distributed::shutdown::SHUTDOWN_NOW,
                );
            self.register_active_persistence_child();
            if let Err(message) = shutdown_result {
                return Some(Value::error(message));
            }
        }

        let executable = match std::env::current_exe() {
            Ok(path) => path,
            Err(error) => {
                return Some(Value::error(format!(
                    "ERR failed to restart the server: {error}"
                )))
            }
        };
        let args = restart_arguments(
            self.ctx.startup_args().iter().cloned(),
            self.ctx.config().server.config_file.as_deref(),
        );
        self.cancel_disk_sync_batch();
        self.close_listeners_for_shutdown();
        if intent.delay_ms != 0 {
            std::thread::sleep(Duration::from_millis(intent.delay_ms));
        }
        let error = Command::new(executable).args(args).exec();
        crate::server_log!(warning, "Failed to restart server: {}", error);
        std::process::exit(1);
    }
}

fn restart_arguments(
    current: impl IntoIterator<Item = OsString>,
    config_file: Option<&str>,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = current.into_iter().collect();
    if let Some(config_file) = config_file {
        if args
            .first()
            .is_some_and(|argument| !argument.to_string_lossy().starts_with('-'))
        {
            args[0] = config_file.into();
        } else {
            args.insert(0, config_file.into());
        }
    }
    args
}

#[cfg(test)]
mod tests {
    use super::restart_arguments;
    use std::ffi::OsString;

    #[test]
    fn restart_uses_the_normalized_config_path_and_preserves_overrides() {
        assert_eq!(
            restart_arguments(
                [
                    OsString::from("relative.conf"),
                    OsString::from("--port"),
                    OsString::from("7000"),
                ],
                Some("/tmp/server/valkey.conf"),
            ),
            vec![
                OsString::from("/tmp/server/valkey.conf"),
                OsString::from("--port"),
                OsString::from("7000"),
            ]
        );
    }
}
