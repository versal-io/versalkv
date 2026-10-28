//!   - String  → SET key value
//!   - List    → RPUSH key v1 v2 ...
//!   - Set     → SADD key m1 m2 ...
//!   - Hash    → HSET key f1 v1 f2 v2 ...
//!   - ZSet    → ZADD key s1 m1 s2 m2 ...
//!
//! rewriteListObject/rewriteSetObject/rewriteHashObject/rewriteSortedSetObject

use crate::engine::command::context::CommandConn;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use crate::foundation::io::store::kv_engine::now_ms;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::persistence::aof_rewrite::AofRewriteFileOptions;
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
    if ctx.persistence().has_active_child() || conn.in_transaction() {
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
    // During the initial rewrite that enables AOF (C's AOF_WAIT_REWRITE) the
    // confirmed fsynced offset is pinned to 0 while the optimistic pending
    // offset captures the current replication offset. The confirmed value is
    // promoted from pending only once the rewrite finishes.
    if ctx.aof().aof_wait_rewrite {
        let repl_offset = ctx.replication().primary_repl_offset().get();
        ctx.aof_mut().set_fsynced_pending(repl_offset);
        ctx.aof_mut().fsynced_repl_offset = Some(0);
    }
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
            .flush(true)
            .map_err(|error| error.to_string())?;
    }
    let incr_path = crate::foundation::io::persistence::rotate_aof_increment(
        options.dir.to_string_lossy().as_ref(),
        &options.filename,
        &options.appenddirname,
    )
    .map_err(|error| error.to_string())?;
    crate::server_log!(
        notice,
        "Creating AOF increment {}",
        incr_path.display()
    );
    let appender = crate::foundation::io::persistence::AofAppender::open(
        options.dir.to_string_lossy().as_ref(),
        &options.filename,
        &options.appenddirname,
        crate::foundation::io::persistence::AofOpenMode::AppendExisting,
        ctx.config().persistence.aof_fsync,
    )
    .map_err(|error| error.to_string())?;
    let shutdown_result = ctx
        .aof_mut()
        .journal
        .take()
        .map(|mut old_journal| old_journal.appender.shutdown());
    ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(appender),
    );
    shutdown_result
        .transpose()
        .map(|_| ())
        .map_err(|error| error.to_string())
}

///   4. aof_current_size > aof_rewrite_min_size；
pub fn check_auto_aof_rewrite(ctx: &mut ServerContext) {
    let policy =
        crate::engine::persistence::AofRewritePolicy::from_config(&ctx.config().persistence);
    if !crate::engine::persistence::aof::should_auto_rewrite(ctx.persistence(), policy) {
        return;
    }
    if ctx.persistence().auto_aof_rewrite_limited() {
        if ctx
            .persistence_mut()
            .mark_auto_aof_rewrite_limit_logged()
        {
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
            ctx.persistence_mut()
                .record_aof_rewrite_commit_failure();
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

    let increment_rotated = ctx.aof().journal.is_some();
    if let Some(mut aof) = ctx.aof_mut().journal.take() {
        aof.appender.shutdown().ok();
    }

    let append_enabled = ctx.config().persistence.aof_enabled;
    let disable_auto_gc = ctx.config().persistence.aof_disable_auto_gc;
    let finalize_result =
        crate::foundation::io::persistence::aof_rewrite::finalize_rewrite(
            &options,
            pid,
            &buf,
            append_enabled,
            disable_auto_gc,
            increment_rotated,
        );

    if let Err(e) = finalize_result {
        ctx.persistence_mut()
            .record_aof_rewrite_commit_failure();
        crate::server_log!(warning, "AOF rewrite finalize failed: {}", e);
        let new_aof = _open_rewrite_aof(&options, ctx.config_mut().persistence.aof_fsync);
        match new_aof {
            Ok(app) => {
                ctx.persistence_mut().record_aof_size(app.current_size());
                ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Ok;
                ctx.aof_mut().journal = Some(crate::foundation::io::persistence::journal::AofJournal::new(app));
            }
            Err(e2) => {
                ctx.config_mut().persistence.aof_enabled = false;
                ctx.persistence_mut().reset_aof_selected_db();
                crate::server_log!(warning, "AOF rewrite: can't reopen old AOF: {}", e2);
            }
        }
        return Some(ReapResult::Error);
    }

    if !append_enabled {
        ctx.persistence_mut()
            .record_aof_rewrite_commit_success();
        crate::server_log!(notice, "AOF rewrite complete ({}s)", elapsed);
        return Some(ReapResult::Success);
    }

    let new_aof = _open_rewrite_aof(&options, ctx.config_mut().persistence.aof_fsync);
    match new_aof {
        Ok(app) => {
            let current_size = app.current_size();
            ctx.persistence_mut().record_aof_size(current_size);
            ctx.persistence_mut()
                .record_aof_rewrite_base_size(current_size);
            ctx.aof_mut().last_write_status = crate::engine::persistence::OperationStatus::Ok;
            ctx.persistence_mut()
                .record_aof_rewrite_commit_success();
            ctx.aof_mut().started_this_session = true;
            // C's backgroundRewriteDoneHandler leaves AOF_WAIT_REWRITE for
            // AOF_ON and promotes the confirmed fsynced offset to whatever the
            // optimistic pending value now holds (set in startAppendOnly or
            // advanced by a bio fsync in the meantime).
            if ctx.aof().aof_wait_rewrite {
                ctx.aof_mut().aof_wait_rewrite = false;
                let pending = ctx.aof().fsynced_repl_offset_pending;
                ctx.aof_mut().fsynced_repl_offset = Some(pending);
            }
            ctx.aof_mut().journal = Some(crate::foundation::io::persistence::journal::AofJournal::new(app));
            crate::server_log!(
                notice,
                "AOF rewrite complete ({}s, {} bytes final)",
                elapsed,
                current_size
            );
            Some(ReapResult::Success)
        }
        Err(e) => {
            ctx.persistence_mut()
                .record_aof_rewrite_commit_failure();
            ctx.config_mut().persistence.aof_enabled = false;
            ctx.persistence_mut().reset_aof_selected_db();
            crate::server_log!(warning, "AOF rewrite: can't reopen new AOF: {}", e);
            Some(ReapResult::Error)
        }
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
