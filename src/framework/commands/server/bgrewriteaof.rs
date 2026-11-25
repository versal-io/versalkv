//!   - String  → SET key value
//!   - List    → RPUSH key v1 v2 ...
//!   - Set     → SADD key m1 m2 ...
//!   - Hash    → HSET key f1 v1 f2 v2 ...
//!   - ZSet    → ZADD key s1 m1 s2 m2 ...
//!
//! rewriteListObject/rewriteSetObject/rewriteHashObject/rewriteSortedSetObject

use crate::engine::command::context::CommandConn;
use crate::foundation::io::persistence::aof_rewrite::AofRewriteFileOptions;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::now_ms;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use command_macros::command;
use std::path::Path;

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_REWRITE_AOF_OPEN: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static PAUSE_REWRITE_CHILD_AFTER_AFFINITY: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

fn _open_rewrite_aof(
    options: &AofRewriteFileOptions,
    fsync: crate::foundation::io::persistence::AofFsync,
) -> std::io::Result<crate::foundation::io::persistence::AofAppender> {
    #[cfg(test)]
    if FAIL_NEXT_REWRITE_AOF_OPEN.with(|fail| fail.replace(false)) {
        return Err(std::io::Error::other("injected rewrite AOF open failure"));
    }
    crate::foundation::io::persistence::AofAppender::open(
        options.dir.to_string_lossy().as_ref(),
        &options.filename,
        &options.appenddirname,
        crate::foundation::io::persistence::AofOpenMode::AppendExisting,
        fsync,
    )
}

#[command("BGREWRITEAOF")]
pub fn bgrewriteaof(
    conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if ctx.persistence().aof_rewrite_child_pid().is_some() {
        return Value::error("ERR Background append only file rewriting already in progress");
    }
    if ctx.persistence().rdb_child_pid().is_some() || conn.in_transaction() {
        ctx.persistence_mut().set_aof_rewrite_scheduled(true);
        crate::server_log!(notice, "Background append only file rewriting scheduled.");
        return Value::SimpleString("Background append only file rewriting scheduled".into());
    }

    match start_aof_rewrite(ctx) {
        Ok(()) => {
            ctx.update_dict_resize_policy();
            Value::SimpleString("Background append only file rewriting started".into())
        }
        Err(e) => Value::error(format!("ERR {}", e)),
    }
}

pub(crate) fn start_aof_rewrite(ctx: &mut ServerContext) -> Result<(), String> {
    if ctx.persistence().has_active_child() {
        return Err("background persistence operation already in progress".to_string());
    }
    let options = rewrite_file_options(ctx);
    let mut function_codes: Vec<Vec<u8>> = ctx
        .script()
        .function_registry
        .list_libraries()
        .into_iter()
        .map(|lib| lib.code.as_bytes().to_vec())
        .collect();
    function_codes.sort();
    let migration_snapshot = ctx
        .slot_import_aof_snapshot()
        .map_err(|error| error.to_string())?;
    let rewrite_affinity = crate::foundation::io::sys::process::prepare_cpu_affinity(
        &ctx.config().server.aof_rewrite_cpulist,
    )
    .map_err(|error| format!("invalid aof-rewrite-cpulist: {error}"))?;
    rotate_increment_for_rewrite(ctx, &options)?;
    let baseline_epoch = ctx.aof().baseline_epoch;
    let fork_started = std::time::Instant::now();
    let pid = match crate::framework::capabilities::process::fork_process()
        .map_err(|error| format!("fork failed: {error}"))?
    {
        crate::framework::capabilities::process::ForkBranch::Parent(child) => child.into_pid(),
        crate::framework::capabilities::process::ForkBranch::Child => {
            crate::framework::commands::server::save::child_after_fork(
                ctx.config(),
                ctx.listener_fds(),
            );
            if let Some(affinity) = rewrite_affinity.as_ref() {
                let _ = crate::foundation::io::sys::process::apply_prepared_cpu_affinity(affinity);
            }
            let child_pid = std::process::id() as i32;
            #[cfg(test)]
            if PAUSE_REWRITE_CHILD_AFTER_AFFINITY.with(|pause| pause.get()) {
                let _ = crate::foundation::io::sys::process::send_signal(
                    child_pid,
                    crate::foundation::io::sys::process::ProcessSignal::Stop,
                );
            }
            let code = match crate::foundation::io::persistence::aof_rewrite::write_rewrite_temp(
                ctx.dbs(),
                &function_codes,
                &migration_snapshot,
                &options,
                child_pid,
            ) {
                Ok(()) => 0,
                Err(_) => 1,
            };
            crate::foundation::io::sys::process::exit_immediately(code);
        }
    };
    ctx.stats_mut()
        .record_fork(fork_started.elapsed().as_micros() as u64);
    ctx.persistence_mut()
        .begin_aof_rewrite_child(pid, now_ms() as i64)
        .map_err(|_| "background persistence operation already in progress".to_string())?;
    ctx.aof_mut().rewrite.baseline_epoch = baseline_epoch;
    Ok(())
}

fn rotate_increment_for_rewrite(
    ctx: &mut ServerContext,
    options: &AofRewriteFileOptions,
) -> Result<(), String> {
    if options.appenddirname.is_empty()
        || !ctx.config().persistence.aof_enabled
        || ctx.aof().journal.is_none()
    {
        return Ok(());
    }
    let manifest_path = options
        .aof_dir()
        .join(format!("{}.manifest", options.filename));
    if !manifest_path.exists() {
        return Ok(());
    }
    ctx.persistence_mut().reset_aof_selected_db();
    if let Some(journal) = ctx.aof_mut().journal.as_mut() {
        journal
            .appender
            .shutdown()
            .map_err(|error| error.to_string())?;
    }
    let incr_path = crate::foundation::io::persistence::rotate_aof_increment(
        options.dir.to_string_lossy().as_ref(),
        &options.filename,
        &options.appenddirname,
    )
    .map_err(|error| error.to_string())?;
    crate::server_log!(notice, "Creating AOF increment {}", incr_path.display());
    let mut appender = crate::foundation::io::persistence::AofAppender::open(
        options.dir.to_string_lossy().as_ref(),
        &options.filename,
        &options.appenddirname,
        crate::foundation::io::persistence::AofOpenMode::AppendExisting,
        ctx.config().persistence.aof_fsync,
    )
    .map_err(|error| error.to_string())?;
    appender.flush(true).map_err(|error| error.to_string())?;
    ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
    Ok(())
}

/// Start an automatic rewrite when the configured size and growth thresholds are met.
pub fn check_auto_aof_rewrite(ctx: &mut ServerContext) {
    let policy =
        crate::engine::persistence::AofRewritePolicy::from_config(&ctx.config().persistence);
    if !crate::engine::persistence::aof::should_auto_rewrite(ctx.persistence(), policy) {
        return;
    }
    if ctx.persistence().auto_aof_rewrite_limited() {
        if ctx.persistence_mut().mark_auto_aof_rewrite_limit_logged() {
            crate::server_log!(
                warning,
                "Automatic AOF rewrite triggered the limit after repeated failures"
            );
        }
        return;
    }

    crate::server_log!(notice, "Starting automatic rewriting of AOF");
    match start_aof_rewrite(ctx) {
        Ok(()) => {
            crate::server_log!(notice, "Background AOF rewrite started");
        }
        Err(e) => {
            crate::server_log!(warning, "Failed to start auto AOF rewrite: {}", e);
        }
    }
}

pub fn check_scheduled_aof_rewrite(ctx: &mut ServerContext) {
    if !ctx.persistence().aof_rewrite_scheduled() || ctx.persistence().has_active_child() {
        return;
    }
    match start_aof_rewrite(ctx) {
        Ok(()) => {
            crate::server_log!(notice, "Background AOF rewrite started");
        }
        Err(e) => {
            crate::server_log!(warning, "Failed to start scheduled AOF rewrite: {}", e);
        }
    }
}

#[cfg(test)]
pub fn reap_aof_child_if_any(ctx: &mut ServerContext) -> Option<ReapResult> {
    let pid = ctx.persistence().aof_rewrite_child_pid()?;
    let exited_ok = match crate::foundation::io::sys::process::wait_nonblocking(pid) {
        Ok(crate::foundation::io::sys::process::WaitOutcome::Running) => return None,
        Ok(crate::foundation::io::sys::process::WaitOutcome::Exited(0)) => true,
        Ok(_) => false,
        Err(error) => {
            if error.kind() == std::io::ErrorKind::Interrupted {
                return None;
            }
            let options = rewrite_file_options(ctx);
            let _ = std::fs::remove_file(options.temp_path(pid));
            ctx.persistence_mut().abort_child();
            ctx.persistence_mut().record_aof_rewrite_commit_failure();
            return Some(ReapResult::Error);
        }
    };
    apply_aof_rewrite_child_completion(ctx, pid, exited_ok)
}

pub(crate) fn apply_aof_rewrite_child_completion(
    ctx: &mut ServerContext,
    pid: i32,
    exited_ok: bool,
) -> Option<ReapResult> {
    if ctx.persistence().aof_rewrite_child_pid() != Some(pid) {
        return None;
    }
    let completed_ms = now_ms() as i64;
    let buf = ctx
        .persistence_mut()
        .complete_aof_rewrite_child(exited_ok, completed_ms)
        .unwrap_or_default();
    let elapsed = ctx.aof().rewrite.last_duration_sec;
    let options = rewrite_file_options(ctx);

    if !exited_ok {
        let _ = std::fs::remove_file(options.temp_path(pid));
        crate::server_log!(warning, "AOF rewrite child {} failed", pid);
        if ctx.config().persistence.aof_enabled && ctx.aof().journal.is_none() {
            crate::server_log!(notice, "Removing the temp incr aof file");
            ctx.persistence_mut().set_aof_rewrite_scheduled(true);
        }
        return Some(ReapResult::Error);
    }

    if ctx.aof().rewrite.baseline_epoch != ctx.aof().baseline_epoch {
        let _ = std::fs::remove_file(options.temp_path(pid));
        ctx.persistence_mut().record_aof_rewrite_commit_failure();
        ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Unavailable;
        if ctx.config().persistence.aof_enabled {
            ctx.persistence_mut().set_aof_rewrite_scheduled(true);
        }
        crate::server_log!(warning, "AOF rewrite snapshot was invalidated by unrecorded writes");
        return Some(ReapResult::Error);
    }

    let increment_rotated = ctx.aof().journal.is_some();
    if let Some(aof) = ctx.aof_mut().journal.as_mut() {
        if let Err(error) = aof.appender.shutdown() {
            ctx.persistence_mut().record_aof_rewrite_commit_failure();
            ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Err;
            let _ = std::fs::remove_file(options.temp_path(pid));
            crate::server_log!(
                warning,
                "AOF rewrite old journal synchronization failed: {error}"
            );
            return Some(ReapResult::Error);
        }
    }
    ctx.aof_mut().journal.take();

    let append_enabled = ctx.config().persistence.aof_enabled;
    let disable_auto_gc = ctx.config().persistence.aof_disable_auto_gc;
    let finalize_result = crate::foundation::io::persistence::aof_rewrite::finalize_rewrite(
        &options,
        pid,
        &buf,
        append_enabled,
        disable_auto_gc,
        increment_rotated,
    );

    if let Err(e) = finalize_result {
        ctx.persistence_mut().record_aof_rewrite_commit_failure();
        ctx.aof_mut().invalidate_replication_baseline();
        crate::server_log!(warning, "AOF rewrite finalize failed: {}", e);
        let new_aof = _open_rewrite_aof(&options, ctx.config_mut().persistence.aof_fsync);
        match new_aof {
            Ok(mut app) => {
                if let Err(error) = app.flush(true) {
                    ctx.aof_mut().last_write_status =
                        crate::engine::persistence::OperationStatus::Err;
                    record_rewrite_journal_failure(ctx);
                    crate::server_log!(
                        warning,
                        "AOF rewrite: can't sync reopened old AOF: {error}"
                    );
                    return Some(ReapResult::Error);
                }
                ctx.persistence_mut().record_aof_size(app.current_size());
                ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Ok;
                ctx.aof_mut().journal =
                    Some(crate::foundation::io::persistence::journal::AofJournal::new(app));
                if ctx.config().replication.repl_after_aof_fsync
                    && ctx.aof().replication_baseline
                        == crate::engine::persistence::AofBaseline::Unavailable
                {
                    ctx.aof_mut().last_write_status =
                        crate::engine::persistence::OperationStatus::Err;
                    ctx.persistence_mut().set_aof_rewrite_scheduled(true);
                }
            }
            Err(e2) => {
                record_rewrite_journal_failure(ctx);
                ctx.persistence_mut().reset_aof_selected_db();
                crate::server_log!(warning, "AOF rewrite: can't reopen old AOF: {}", e2);
            }
        }
        return Some(ReapResult::Error);
    }

    if !append_enabled {
        ctx.persistence_mut().record_aof_rewrite_commit_success();
        crate::server_log!(notice, "AOF rewrite complete ({}s)", elapsed);
        return Some(ReapResult::Success);
    }

    // Finalization has installed and synced the complete baseline. A failed journal
    // reopen must block writes, but does not invalidate those installed files.
    ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
    let new_aof = _open_rewrite_aof(&options, ctx.config_mut().persistence.aof_fsync);
    match new_aof {
        Ok(mut app) => {
            if let Err(error) = app.flush(true) {
                ctx.persistence_mut().record_aof_rewrite_commit_failure();
                ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Err;
                record_rewrite_journal_failure(ctx);
                ctx.persistence_mut().reset_aof_selected_db();
                crate::server_log!(warning, "AOF rewrite: can't sync new AOF: {error}");
                return Some(ReapResult::Error);
            }
            let current_size = app.current_size();
            ctx.persistence_mut().record_aof_size(current_size);
            ctx.persistence_mut()
                .record_aof_rewrite_base_size(current_size);
            ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Ok;
            ctx.persistence_mut().record_aof_rewrite_commit_success();
            ctx.aof_mut().started_this_session = true;
            ctx.aof_mut().journal =
                Some(crate::foundation::io::persistence::journal::AofJournal::new(app));
            ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Ready;
            crate::server_log!(
                notice,
                "AOF rewrite complete ({}s, {} bytes final)",
                elapsed,
                current_size
            );
            Some(ReapResult::Success)
        }
        Err(e) => {
            ctx.persistence_mut().record_aof_rewrite_commit_failure();
            record_rewrite_journal_failure(ctx);
            ctx.persistence_mut().reset_aof_selected_db();
            crate::server_log!(warning, "AOF rewrite: can't reopen new AOF: {}", e);
            Some(ReapResult::Error)
        }
    }
}

fn record_rewrite_journal_failure(ctx: &mut ServerContext) {
    ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Err;
    if !ctx.config().replication.repl_after_aof_fsync {
        ctx.config_mut().persistence.aof_enabled = false;
        ctx.aof_mut().replication_baseline = crate::engine::persistence::AofBaseline::Unavailable;
    } else if ctx.aof().replication_baseline == crate::engine::persistence::AofBaseline::Unavailable
    {
        ctx.persistence_mut().set_aof_rewrite_scheduled(true);
    }
}

fn rewrite_file_options(ctx: &ServerContext) -> AofRewriteFileOptions {
    let config = &ctx.config().persistence;
    AofRewriteFileOptions {
        dir: Path::new(&config.dir).to_path_buf(),
        filename: config.aof_filename.clone(),
        appenddirname: config.aof_appenddirname.clone(),
        use_rdb_preamble: config.aof_use_rdb_preamble,
        timestamp_enabled: config.aof_timestamp_enabled,
        key_delay_us: config.rdb_key_save_delay,
        rdb_compression: config.rdbcompression,
        incremental_fsync: config.aof_rewrite_incremental_fsync,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReapResult {
    Success,
    Error,
}

#[cfg(test)]
#[path = "bgrewriteaof_tests.rs"]
mod tests;
