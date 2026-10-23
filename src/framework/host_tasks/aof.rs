use bytes::Bytes;

use crate::engine::host::{
    dispatch_command, yield_aof, HostAbort, HostCommandOrigin, HostControl, HostDispatcher,
    HostError,
};
use crate::foundation::io::persistence::RdbDecodedMetadata;
use crate::foundation::io::persistence::{AofDecoder, AofLoadError, AofLoadStats, AofStep};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) enum AofTaskError {
    Load(AofLoadError),
    Host(HostError),
    Aborted(HostAbort),
    /// A replayed command returned an error while `propagation-error-behavior`
    /// is `panic`. Mirrors C `afterErrorReply`'s `panic_in_aof` branch: during
    /// startup load this is fatal and must terminate the process. We propagate
    /// instead of `panic!` so termination does not depend on the global panic
    /// hook (which may block while flushing the crash report).
    PropagationError {
        command: String,
    },
}

#[derive(Debug)]
pub(crate) enum AofFileTaskError {
    Aof(AofTaskError),
    Rdb(super::RdbTaskError),
    InvalidRdbMetadata,
    Truncated,
    TruncatedBeforeFinal,
    Truncate(std::io::Error),
}

pub(crate) struct AofFileResult {
    pub(crate) stats: AofLoadStats,
    pub(crate) tail_start: usize,
    pub(crate) tail_len: usize,
}

pub(crate) fn apply_aof_truncation(
    path: &Path,
    result: &AofFileResult,
    allow_truncated: bool,
    is_last: bool,
) -> Result<bool, AofFileTaskError> {
    if result.stats.valid_up_to == result.tail_len {
        return Ok(false);
    }
    if !allow_truncated {
        return Err(AofFileTaskError::Truncated);
    }
    if !is_last {
        return Err(AofFileTaskError::TruncatedBeforeFinal);
    }
    let truncate_at = result.tail_start.saturating_add(result.stats.valid_up_to);
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.set_len(truncate_at as u64))
        .map_err(AofFileTaskError::Truncate)?;
    Ok(true)
}

pub(crate) struct DebugAofTask {
    inputs: Vec<(PathBuf, Vec<u8>)>,
    total_size: u64,
}

impl DebugAofTask {
    pub(crate) fn new(inputs: Vec<(PathBuf, Vec<u8>)>) -> Self {
        let total_size = inputs.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        Self { inputs, total_size }
    }

    pub(crate) fn take_inputs(&mut self) -> Vec<(PathBuf, Vec<u8>)> {
        std::mem::take(&mut self.inputs)
    }

    pub(crate) fn finish(
        self,
        result: Result<(), AofFileTaskError>,
        ctx: &mut ServerContext,
    ) -> Value {
        ctx.set_host_task_loading(false);
        ctx.config_mut().server.import_mode = false;
        match result {
            Ok(()) => {
                ctx.persistence_mut().record_rdb_save(
                    (crate::foundation::io::store::kv_engine::now_ms() / 1000) as i64,
                );
                ctx.persistence_mut().record_aof_size(self.total_size);
                ctx.mem_stats().update_startup_snapshot();
                ctx.finish_slot_migration_load();
                Value::ok()
            }
            Err(error) => Value::error(format!(
                "ERR Error trying to load the AOF files, check server logs: {:?}",
                error
            )),
        }
    }
}

pub(crate) fn install_rdb_preamble(
    ctx: &mut ServerContext,
    dataset: crate::foundation::io::store::kv_engine::DbSet,
    metadata: &RdbDecodedMetadata,
) -> Result<(), AofFileTaskError> {
    *ctx.dbs_mut() = dataset;
    super::replace_function_libraries(ctx, &metadata.function_codes)
        .map_err(|()| AofFileTaskError::InvalidRdbMetadata)?;
    let aux_fields = metadata
        .replication
        .as_ref()
        .map(|info| info.aux_fields.as_slice())
        .unwrap_or_default();
    ctx.replace_slot_imports_from_rdb_aux(aux_fields)
        .map_err(|_| AofFileTaskError::InvalidRdbMetadata)
}

impl From<AofLoadError> for AofTaskError {
    fn from(error: AofLoadError) -> Self {
        Self::Load(error)
    }
}

impl From<HostError> for AofTaskError {
    fn from(error: HostError) -> Self {
        Self::Host(error)
    }
}

pub(crate) struct AofReplayTask {
    bytes: Vec<u8>,
    decoder: AofDecoder,
    key_load_delay_us: i64,
    panic_on_propagation_error: bool,
    loops: u64,
    open_transaction_offset: Option<usize>,
}

impl AofReplayTask {
    pub(crate) fn with_key_load_delay_and_panic_on_error(
        bytes: Vec<u8>,
        key_load_delay_us: i64,
        panic_on_propagation_error: bool,
    ) -> Self {
        Self {
            bytes,
            decoder: AofDecoder::new(),
            key_load_delay_us,
            panic_on_propagation_error,
            loops: 0,
            open_transaction_offset: None,
        }
    }

    pub(crate) fn has_open_transaction(&self) -> bool {
        self.open_transaction_offset.is_some()
    }

    pub(crate) fn run(
        &mut self,
        dispatcher: &mut HostDispatcher<'_>,
    ) -> Result<AofLoadStats, AofTaskError> {
        loop {
            if self.loops.is_multiple_of(1024) {
                match yield_aof(dispatcher, self.decoder.position() as u64, self.loops)? {
                    HostControl::Continue => {}
                    HostControl::Abort(reason) => return Err(AofTaskError::Aborted(reason)),
                }
            }
            let command_start = self.decoder.position();
            match self.decoder.step(&self.bytes)? {
                AofStep::Command(parts) => {
                    self.loops = self.loops.saturating_add(1);
                    if parts.is_empty() {
                        continue;
                    }
                    let starts_transaction = parts[0].eq_ignore_ascii_case(b"multi");
                    let ends_transaction = parts[0].eq_ignore_ascii_case(b"exec");
                    let args = CommandArgs::from_parts(
                        parts.into_iter().map(Bytes::from).collect::<Vec<_>>(),
                    );
                    let reply = dispatch_command(dispatcher, &args, HostCommandOrigin::AofReplay)?;
                    if let crate::foundation::io::protocol::Value::Error(error) = reply {
                        crate::server_log!(
                            warning,
                            "== CRITICAL == This server is sending an error to its AOF-loading-client: '{}' after processing the command '{}'",
                            String::from_utf8_lossy(&error),
                            String::from_utf8_lossy(&args.name)
                        );
                        if self.panic_on_propagation_error {
                            return Err(AofTaskError::PropagationError {
                                command: String::from_utf8_lossy(&args.name).into_owned(),
                            });
                        }
                    }
                    crate::foundation::io::sys::time::sleep_debug_delay_interruptible(
                        self.key_load_delay_us,
                        || false,
                    );
                    if starts_transaction {
                        self.open_transaction_offset = Some(command_start);
                    } else if ends_transaction {
                        self.open_transaction_offset = None;
                    }
                }
                AofStep::Finished(mut stats) | AofStep::Truncated(mut stats) => {
                    if let Some(offset) = self.open_transaction_offset {
                        stats.valid_up_to = offset;
                    }
                    return Ok(stats);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::host::{HostAbort, HostReply, HostRequest};
    use crate::foundation::io::protocol::Value;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    static YIELDS: AtomicUsize = AtomicUsize::new(0);
    static COMMANDS: AtomicUsize = AtomicUsize::new(0);
    static RECORDING_DISPATCHER_LOCK: Mutex<()> = Mutex::new(());

    fn recording_dispatcher(request: HostRequest<'_>) -> Result<HostReply, HostError> {
        match request {
            HostRequest::Yield(crate::engine::host::HostYield::AofProgress { .. }) => {
                YIELDS.fetch_add(1, Ordering::Relaxed);
                Ok(HostReply::Continue)
            }
            HostRequest::ExecuteCommand {
                origin: HostCommandOrigin::AofReplay,
                ..
            } => {
                COMMANDS.fetch_add(1, Ordering::Relaxed);
                Ok(HostReply::Command(Value::ok()))
            }
            _ => Err(HostError::Invariant("unexpected AOF request")),
        }
    }

    #[test]
    fn yields_before_first_command_and_after_each_1024_loops() {
        let _guard = RECORDING_DISPATCHER_LOCK
            .lock()
            .expect("lock recording dispatcher");
        let mut bytes = Vec::new();
        for _ in 0..1025 {
            bytes.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        }
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(bytes, 0, false);
        let mut dispatcher = recording_dispatcher;
        YIELDS.store(0, Ordering::Relaxed);
        COMMANDS.store(0, Ordering::Relaxed);

        let stats = task.run(&mut dispatcher).expect("replay AOF");

        assert_eq!(stats.cmd_count, 1025);
        assert_eq!(COMMANDS.load(Ordering::Relaxed), 1025);
        assert_eq!(YIELDS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn replay_applies_key_load_delay_after_each_aof_command() {
        let _guard = RECORDING_DISPATCHER_LOCK
            .lock()
            .expect("lock recording dispatcher");
        let bytes = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n".to_vec();
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(bytes, 20_000, false);
        let mut dispatcher = recording_dispatcher;
        let started = std::time::Instant::now();

        task.run(&mut dispatcher).expect("replay AOF");

        assert!(
            started.elapsed() >= std::time::Duration::from_millis(30),
            "two replayed commands must each apply the configured 20ms delay"
        );
    }

    #[test]
    fn replay_continues_after_command_error_without_panic_policy() {
        let bytes = b"*1\r\n$7\r\nUNKNOWN\r\n".to_vec();
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(bytes, 0, false);
        let mut dispatcher = |request: HostRequest<'_>| match request {
            HostRequest::Yield(crate::engine::host::HostYield::AofProgress { .. }) => {
                Ok(HostReply::Continue)
            }
            HostRequest::ExecuteCommand {
                origin: HostCommandOrigin::AofReplay,
                ..
            } => Ok(HostReply::Command(Value::error("ERR unknown command"))),
            _ => Err(HostError::Invariant("unexpected AOF request")),
        };

        let stats = task
            .run(&mut dispatcher)
            .expect("ignore policy should finish AOF replay after command errors");

        assert_eq!(stats.cmd_count, 1);
    }

    #[test]
    fn replay_reports_propagation_error_with_panic_policy() {
        let bytes = b"*1\r\n$7\r\nUNKNOWN\r\n".to_vec();
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(bytes, 0, true);
        let mut dispatcher = |request: HostRequest<'_>| match request {
            HostRequest::Yield(crate::engine::host::HostYield::AofProgress { .. }) => {
                Ok(HostReply::Continue)
            }
            HostRequest::ExecuteCommand {
                origin: HostCommandOrigin::AofReplay,
                ..
            } => Ok(HostReply::Command(Value::error("ERR unknown command"))),
            _ => Err(HostError::Invariant("unexpected AOF request")),
        };

        // The panic policy must surface as a fatal error the startup layer can
        // turn into a deterministic process exit, not a `panic!` that depends on
        // the global panic hook terminating the process.
        assert!(matches!(
            task.run(&mut dispatcher),
            Err(AofTaskError::PropagationError { command }) if command.eq_ignore_ascii_case("UNKNOWN")
        ));
    }

    #[test]
    fn preserves_shutdown_abort_reason() {
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(
            b"*1\r\n$4\r\nPING\r\n".to_vec(),
            0,
            false,
        );
        let mut dispatcher = |request: HostRequest<'_>| match request {
            HostRequest::Yield(crate::engine::host::HostYield::AofProgress { .. }) => {
                Ok(HostReply::Abort(HostAbort::Shutdown))
            }
            _ => Err(HostError::Invariant("unexpected AOF request after abort")),
        };

        assert!(matches!(
            task.run(&mut dispatcher),
            Err(AofTaskError::Aborted(HostAbort::Shutdown))
        ));
    }

    #[test]
    fn unfinished_multi_reports_valid_offset_before_transaction() {
        let first = crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET", b"foo", b"hello",
        ]);
        let mut bytes = first.clone();
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[b"MULTI"]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET", b"bar", b"world",
            ]),
        );
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(bytes, 0, false);
        let mut dispatcher = |request: HostRequest<'_>| match request {
            HostRequest::Yield(crate::engine::host::HostYield::AofProgress { .. }) => {
                Ok(HostReply::Continue)
            }
            HostRequest::ExecuteCommand {
                origin: HostCommandOrigin::AofReplay,
                ..
            } => Ok(HostReply::Command(Value::ok())),
            _ => Err(HostError::Invariant("unexpected AOF request")),
        };

        let stats = task.run(&mut dispatcher).expect("parse valid AOF frames");

        assert_eq!(stats.valid_up_to, first.len());
    }

    #[test]
    fn completed_multi_keeps_the_full_valid_offset() {
        let mut bytes =
            crate::engine::distributed::replication::protocol::encode_command(&[b"MULTI"]);
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET", b"bar", b"world",
            ]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[b"EXEC"]),
        );
        let expected = bytes.len();
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(bytes, 0, false);
        let mut dispatcher = |request: HostRequest<'_>| match request {
            HostRequest::Yield(crate::engine::host::HostYield::AofProgress { .. }) => {
                Ok(HostReply::Continue)
            }
            HostRequest::ExecuteCommand {
                origin: HostCommandOrigin::AofReplay,
                ..
            } => Ok(HostReply::Command(Value::ok())),
            _ => Err(HostError::Invariant("unexpected AOF request")),
        };

        let stats = task
            .run(&mut dispatcher)
            .expect("replay complete transaction");

        assert_eq!(stats.valid_up_to, expected);
        assert!(!task.has_open_transaction());
    }

    #[test]
    fn discard_inside_aof_multi_does_not_close_the_replay_transaction() {
        let mut bytes =
            crate::engine::distributed::replication::protocol::encode_command(&[b"MULTI"]);
        let transaction_offset = 0;
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET", b"bar", b"world",
            ]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[b"DISCARD"]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET", b"after", b"discard",
            ]),
        );
        let mut task = AofReplayTask::with_key_load_delay_and_panic_on_error(bytes, 0, false);
        let mut dispatcher = |request: HostRequest<'_>| match request {
            HostRequest::Yield(crate::engine::host::HostYield::AofProgress { .. }) => {
                Ok(HostReply::Continue)
            }
            HostRequest::ExecuteCommand {
                origin: HostCommandOrigin::AofReplay,
                ..
            } => Ok(HostReply::Command(Value::ok())),
            _ => Err(HostError::Invariant("unexpected AOF request")),
        };

        let stats = task
            .run(&mut dispatcher)
            .expect("parse transaction containing DISCARD");

        assert_eq!(stats.valid_up_to, transaction_offset);
        assert!(task.has_open_transaction());
    }

    #[test]
    fn truncation_is_allowed_only_for_the_last_aof_file() {
        let result = AofFileResult {
            stats: AofLoadStats {
                cmd_count: 1,
                valid_up_to: 4,
            },
            tail_start: 3,
            tail_len: 8,
        };

        assert!(matches!(
            apply_aof_truncation(Path::new("unused"), &result, false, true),
            Err(AofFileTaskError::Truncated)
        ));
        assert!(matches!(
            apply_aof_truncation(Path::new("unused"), &result, true, false),
            Err(AofFileTaskError::TruncatedBeforeFinal)
        ));
    }

    #[test]
    fn aof_truncation_failure_is_returned() {
        let result = AofFileResult {
            stats: AofLoadStats {
                cmd_count: 1,
                valid_up_to: 0,
            },
            tail_start: 0,
            tail_len: 1,
        };

        assert!(matches!(
            apply_aof_truncation(&std::env::temp_dir(), &result, true, true),
            Err(AofFileTaskError::Truncate(_))
        ));
    }
}
