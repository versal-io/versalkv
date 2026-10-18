use crate::engine::command::context::CommandConn;
use crate::engine::distributed::cluster::ClusterState;
use crate::foundation::io::protocol::Value;
use crate::runtime::server::worker::ServerWorkerState;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const BEST_EFFORT_WARNING_INTERVAL_MS: i64 = 30_000;

pub(crate) fn persist_cluster_config(path: &Path, content: &str) -> io::Result<()> {
    persist_cluster_config_with_fsync(path, content, true)
}

fn persist_cluster_config_with_fsync(path: &Path, content: &str, do_fsync: bool) -> io::Result<()> {
    let temp_path = cluster_config_temp_path(path)?;
    let result = persist_cluster_config_inner(path, &temp_path, content, do_fsync);
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

fn persist_cluster_config_inner(
    path: &Path,
    temp_path: &Path,
    content: &str,
    do_fsync: bool,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temp_path)
        .map_err(|error| io_stage_error("Could not open temp cluster config file", error))?;
    file.write_all(content.as_bytes())
        .map_err(|error| io_stage_error("Failed writing tmp cluster config file", error))?;
    if do_fsync {
        file.sync_all()
            .map_err(|error| io_stage_error("Could not sync tmp cluster config file", error))?;
    }
    drop(file);
    std::fs::rename(temp_path, path)
        .map_err(|error| io_stage_error("Could not rename tmp cluster config file", error))?;
    if do_fsync {
        sync_parent_directory(path)
            .map_err(|error| io_stage_error("Could not sync cluster config file dir", error))?;
    }
    Ok(())
}

fn io_stage_error(stage: &str, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{stage}: {error}"))
}

fn cluster_config_temp_path(path: &Path) -> io::Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster config has no file name",
        )
    })?;
    let temp_name = format!(
        "{}.tmp-{}-{}",
        file_name.to_string_lossy(),
        std::process::id(),
        crate::foundation::io::sys::time::mstime()
    );
    Ok(path.with_file_name(temp_name))
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

impl ServerWorkerState {
    pub(crate) fn resolve_cluster_config_save_request(
        &mut self,
        connection_token: usize,
        success_reply: Option<Value>,
    ) -> Option<Value> {
        let success_reply = success_reply?;
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .set_custom_reply(false);
        Some(match self.save_cluster_config_now() {
            Ok(()) => success_reply,
            Err(error) => {
                Value::error(format!("ERR error saving the cluster node config: {error}"))
            }
        })
    }

    pub(crate) fn save_cluster_config_now(&mut self) -> io::Result<()> {
        let path = self.cluster_config.path.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "cluster config path is not configured",
            )
        })?;
        let state = self.ctx.cluster_state().as_deref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "cluster state is not initialized")
        })?;
        let content = crate::engine::distributed::cluster::config::render_cluster_config(state);
        persist_cluster_config(path, &content)?;
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            state.take_config_save_request();
        }
        Ok(())
    }

    pub(crate) fn persist_cluster_config_if_changed(&mut self, state: &mut ClusterState) {
        if !state.take_config_save_request() {
            return;
        }
        // C only enters clusterSaveConfig after a topology mutation schedules
        // CLUSTER_TODO_SAVE_CONFIG. Rendering the whole node bitmap on every
        // cron tick turns a large cluster into an O(nodes * slots) idle path.
        let content = crate::engine::distributed::cluster::config::render_cluster_config(state);
        let do_fsync = state.take_config_fsync_request();
        let Some(path) = self.cluster_config.path.as_deref() else {
            if do_fsync {
                state.request_config_fsync();
            } else {
                state.request_config_save();
            }
            return;
        };
        match persist_cluster_config_with_fsync(path, &content, do_fsync) {
            Ok(()) => {}
            Err(error) => {
                if do_fsync {
                    state.request_config_fsync();
                } else {
                    state.request_config_save();
                }
                crate::server_log!(
                    warning,
                    "Could not persist cluster config file {}: {}",
                    path.display(),
                    error
                );
                if self.ctx.config().server.cluster_config_save_behavior == "sync" {
                    crate::server_log!(warning, "Fatal: can't update cluster config file.");
                    std::process::exit(1);
                }
                let now = crate::foundation::io::sys::time::mstime();
                if now.saturating_sub(self.cluster_config.last_best_effort_warning_ms)
                    >= BEST_EFFORT_WARNING_INTERVAL_MS
                {
                    crate::server_log!(
                        warning,
                        "Cluster config updated even though writing the cluster config file to disk failed."
                    );
                    self.cluster_config.last_best_effort_warning_ms = now;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::persist_cluster_config;
    use crate::engine::command::context::CommandConn;
    use crate::engine::distributed::cluster::ClusterState;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::{CommandEffectDrain, Connection};
    use crate::runtime::server::worker::ServerWorkerState;
    use bytes::Bytes;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "valkey-cluster-config-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before Unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn atomic_persist_replaces_content_without_leaving_a_temp_file() {
        let dir = test_dir("success");
        std::fs::create_dir(&dir).expect("create test directory");
        let path = dir.join("nodes.conf");
        std::fs::write(&path, b"old\n").expect("write old config");

        persist_cluster_config(&path, "new\n").expect("persist cluster config");

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new\n");
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "successful atomic replace must remove its sibling temp file"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_atomic_persist_removes_its_temp_file() {
        let dir = test_dir("failure");
        std::fs::create_dir(&dir).expect("create test directory");
        let path = dir.join("nodes.conf");
        std::fs::create_dir(&path).expect("make final path a directory");

        assert!(persist_cluster_config(&path, "new\n").is_err());
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "failed atomic replace must remove its sibling temp file"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn save_request_returns_its_success_reply_after_atomic_persist() {
        let dir = test_dir("custom-reply");
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("nodes.conf");
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.cluster_config.path = Some(path.clone());
        *runtime.ctx.cluster_state_mut() =
            Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16_379)));
        let mut connection = Connection::new_fake();
        connection.request_cluster_config_save_with_reply(Value::SimpleString(Bytes::from_static(
            b"BUMPED 7",
        )));
        let connection_token = runtime.connection_registry.insert(connection);
        let success_reply = runtime.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .take_command_effects(CommandEffectDrain::Completed)
        .and_then(|mut effects| effects.take_cluster_config_save());

        assert_eq!(
            runtime.resolve_cluster_config_save_request(connection_token, success_reply),
            Some(Value::SimpleString(Bytes::from_static(b"BUMPED 7")))
        );
        assert!(path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
