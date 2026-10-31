
use crate::engine::command::context::CommandConn;
use crate::engine::persistence::RdbSaveOptions;
use crate::framework::context::ServerContext;
use crate::framework::context::RuntimeConfig;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::kv_engine::now_ms;
use crate::foundation::io::protocol::Value;
use command_macros::command;

mod fullsync_fork;
pub use fullsync_fork::{
    fork_direct_diskless_rdb_child, fork_disk_rdb_file_child, fork_diskless_rdb_child,
    pause_parent_after_replication_fork, DiskSyncChildExit,
};
#[cfg(test)]
pub use fullsync_fork::reap_disk_sync_file_child;
pub(crate) use fullsync_fork::{
    drain_direct_dual_rdb_phase_pipe, DirectDualRdbChildPhase,
};

#[cfg(test)]
static DISKLESS_FORK_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Unit tests share one process and may otherwise fork overlapping diskless
/// children that inherit each other's safe-to-exit pipe descriptors. Production
/// enforces a single persistence child, so serialize those test lifetimes too.
#[cfg(test)]
pub(crate) fn diskless_fork_test_guard() -> std::sync::MutexGuard<'static, ()> {
    DISKLESS_FORK_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn install_child_signal_handlers() {
    crate::framework::capabilities::process::install_persistence_child_signal_handlers();
}

#[command("SAVE")]
pub fn save(_conn: &mut dyn CommandConn, _args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if ctx.persistence().rdb_bgsave_in_progress() {
        return Value::error("ERR Background save already in progress");
    }
    match do_save(ctx) {
        Ok(()) => Value::ok(),
        Err(_) => Value::error("ERR"),
    }
}

#[command("BGSAVE")]
pub fn bgsave(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    // BGSAVE [SCHEDULE | CANCEL]
    if !args.args.is_empty() {
        if args.args.len() == 1 {
            let opt = String::from_utf8_lossy(&args.args[0]).to_ascii_lowercase();
            match opt.as_str() {
                "schedule" => {} //  fork
                "cancel" => {
                    if ctx.persistence().forkless_save_in_progress() {
                        ctx.persistence().cancel_forkless_save();
                        return Value::SimpleString("Background saving cancelled".into());
                    }
                    if let Some(pid) = ctx.persistence().rdb_child_pid() {
                        let _ = crate::foundation::io::sys::process::send_signal(
                            pid,
                            crate::foundation::io::sys::process::ProcessSignal::Kill,
                        );
                        let _ = crate::foundation::io::sys::process::wait_blocking(pid);
                        let temp_path = std::path::Path::new(&ctx.config().persistence.dir)
                            .join(format!("temp-{}.rdb", pid));
                        let _ = std::fs::remove_file(temp_path);
                        ctx.persistence_mut().abort_child();
                        return Value::SimpleString("Background saving cancelled".into());
                    }
                    if ctx.persistence().bgsave_scheduled() {
                        ctx.persistence_mut().set_bgsave_scheduled(false);
                        return Value::SimpleString("Scheduled background saving cancelled".into());
                    }
                    return Value::error(
                        "ERR Background saving is currently not in progress or scheduled",
                    );
                }
                _ => return CommandError::syntax_error(),
            }
        } else {
            return CommandError::syntax_error();
        }
    }

    if ctx.persistence().rdb_child_pid().is_some()
        || ctx.persistence().forkless_save_in_progress()
    {
        return Value::error("ERR Background save already in progress");
    }
    if ctx.persistence().has_active_child() || conn.in_transaction() {
        if conn.in_transaction()
            || args.args.first().is_some_and(|arg| arg.eq_ignore_ascii_case(b"SCHEDULE"))
        {
            let method = resolve_bgsave_method(ctx, ctx.config().persistence.bgsave_default_method);
            ctx.persistence_mut().schedule_bgsave(method);
            return Value::SimpleString("Background saving scheduled".into());
        }
        return Value::error(
            "ERR Another child process is active (AOF?): can't BGSAVE right now. Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible."
        );
    }

    match start_bgsave(ctx) {
        Ok(()) => {
            ctx.update_dict_resize_policy();
            Value::SimpleString("Background saving started".into())
        }
        Err(_) => Value::error("ERR"),
    }
}

#[command("LASTSAVE")]
pub fn lastsave(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    Value::Integer(ctx.persistence().rdb().last_save_time_sec)
}

pub fn do_save(ctx: &mut ServerContext) -> Result<(), ()> {
    let options = RdbSaveOptions::from_config(&ctx.config().persistence);
    let preserve_expired = !ctx.config().server.active_expire_enabled;
    ctx.dbs_mut().set_pause_skip_expire_all(preserve_expired);
    let function_codes: Vec<Vec<u8>> = ctx
        .script()
        .function_registry
        .list_libraries()
        .into_iter()
        .map(|lib| lib.code.as_bytes().to_vec())
        .collect();
    let replication = ctx.rdb_replication_info();
    let slot_imports = ctx.slot_import_dbs();
    let result = crate::engine::persistence::rdb::save_with_functions_and_replication(
        ctx.dbs(),
        &slot_imports,
        &options,
        &function_codes,
        Some(&replication),
    );
    ctx.dbs_mut().set_pause_skip_expire_all(false);
    result?;
    ctx.persistence_mut()
        .record_rdb_save((now_ms() / 1000) as i64);
    Ok(())
}

pub fn start_bgsave(ctx: &mut ServerContext) -> Result<(), ()> {
    start_bgsave_with_method(ctx, ctx.config().persistence.bgsave_default_method)
}

fn resolve_bgsave_method(
    ctx: &ServerContext,
    requested: crate::engine::extension::config::persistence::BgsaveMethod,
) -> crate::engine::extension::config::persistence::BgsaveMethod {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    if requested == BgsaveMethod::Forkless
        && !ctx.module_registry().list().all(|module| module.options.get() & (1 << 6) != 0)
    {
        crate::server_log!(warning,
            "Falling back to fork-based save: a loaded module has not declared VALKEYMODULE_OPTIONS_HANDLE_FORKLESS"
        );
        BgsaveMethod::Fork
    } else {
        requested
    }
}

fn start_bgsave_with_method(
    ctx: &mut ServerContext,
    method: crate::engine::extension::config::persistence::BgsaveMethod,
) -> Result<(), ()> {
    if ctx.persistence().has_active_child() {
        return Err(());
    }
    if resolve_bgsave_method(ctx, method)
        == crate::engine::extension::config::persistence::BgsaveMethod::Forkless
    {
        return start_forkless_bgsave(ctx);
    }
    let options = RdbSaveOptions::from_config(&ctx.config().persistence);
    let total_keys = (0..ctx.num_dbs())
        .map(|index| ctx.db_size(index as u32))
        .sum();
    ctx.persistence_mut().begin_rdb_metrics(total_keys);
    let preserve_expired = !ctx.config().server.active_expire_enabled;
    ctx.dbs_mut().set_pause_skip_expire_all(preserve_expired);
    let function_codes: Vec<Vec<u8>> = ctx
        .script()
        .function_registry
        .list_libraries()
        .into_iter()
        .map(|lib| lib.code.as_bytes().to_vec())
        .collect();
    let replication = ctx.rdb_replication_info();
    let slot_imports = ctx.slot_import_dbs();
    let bgsave_affinity = prepare_bgsave_cpu_affinity(
        &ctx.config().server.bgsave_cpulist,
    )
    .map_err(|_| ())?;
    let fork_started = std::time::Instant::now();
    let pid = match crate::framework::capabilities::process::fork_process() {
        Ok(crate::framework::capabilities::process::ForkBranch::Parent(child)) => child.into_pid(),
        Ok(crate::framework::capabilities::process::ForkBranch::Child) => {
            child_after_fork(ctx.config(), ctx.listener_fds());
            if let Some(affinity) = bgsave_affinity.as_ref() {
                let _ = crate::foundation::io::sys::process::apply_prepared_cpu_affinity(affinity);
            }
            let code = match crate::engine::persistence::rdb::save_with_functions_and_replication(
                ctx.dbs(),
                &slot_imports,
                &options,
                &function_codes,
                Some(&replication),
            ) {
                Ok(()) => 0,
                Err(()) => 1,
            };
            crate::foundation::io::sys::process::exit_immediately(code);
        }
        Err(_) => {
            crate::server_log!(warning, "fork() failed, falling back to synchronous save");
            return do_save(ctx);
        }
    };
    ctx.dbs_mut().set_pause_skip_expire_all(false);
    ctx.stats_mut()
        .record_fork(fork_started.elapsed().as_micros() as u64);
    ctx.persistence_mut()
        .begin_rdb_child(pid, (now_ms() / 1000) as i64)
        .map_err(|_| ())
}

fn start_forkless_bgsave(ctx: &mut ServerContext) -> Result<(), ()> {
    use crate::foundation::io::persistence::rdb::{
        compression::{RdbCompression, RdbSnapshotOptions},
        frozen::FrozenRdb,
        RdbSerializeOptions,
    };
    use crate::foundation::io::persistence::snapshot::background::BackgroundRdbSave;

    let options = RdbSaveOptions::from_config(&ctx.config().persistence);
    let function_codes: Vec<Vec<u8>> = ctx.script().function_registry.list_libraries()
        .into_iter().map(|lib| lib.code.as_bytes().to_vec()).collect();
    let replication = ctx.rdb_replication_info();
    let capture_ms = now_ms();
    let preserve_expired = !ctx.config().server.active_expire_enabled
        || (ctx.replication().is_replica() && ctx.config().replication.replica_read_only);
    let serialize_options = RdbSerializeOptions {
        compression: options.compression.uses_lzf(),
        checksum: options.checksum && options.compression != RdbCompression::Lz4,
    };
    let save_options = RdbSnapshotOptions {
        compression: options.compression,
        checksum: options.checksum,
    };
    let frozen = {
        let slot_imports = ctx.slot_import_dbs_with_ids();
        FrozenRdb::capture_header(
            ctx.dbs(),
            &slot_imports,
            &function_codes,
            Some(&replication),
            serialize_options,
        )
    };
    let total_keys = frozen.total_keys();
    let (task, feeder) = BackgroundRdbSave::start_incremental(
        total_keys,
        options.dir,
        options.filename,
        ctx.persistence().rdb().saves.saturating_add(1),
        options.key_delay_us,
        options.incremental_fsync,
        save_options,
    )
    .map_err(|error| {
        crate::server_log!(warning, "Failed to start forkless background saving: {}", error);
    })?;
    let feed = frozen.into_incremental_feed(feeder, capture_ms, preserve_expired);
    ctx.set_forkless_capture_expiration_paused(true);
    if ctx
        .persistence_mut()
        .begin_forkless_save(task, (now_ms() / 1000) as i64, Vec::new(), Some(feed))
        .is_err()
    {
        ctx.set_forkless_capture_expiration_paused(false);
        return Err(());
    }
    let _ = ctx.drive_forkless_feed();
    Ok(())
}

pub(crate) fn prepare_bgsave_cpu_affinity(
    cpulist: &str,
) -> std::io::Result<Option<crate::foundation::io::sys::process::PreparedCpuAffinity>> {
    crate::foundation::io::sys::process::prepare_cpu_affinity(cpulist)
}

pub fn close_listener_fds(fds: &[i32]) -> usize {
    let mut closed = 0;
    for &fd in fds {
        if fd < 0 {
            continue;
        }
        if crate::framework::capabilities::process::close_fd(fd) {
            closed += 1;
        }
    }
    closed
}

pub fn child_after_fork(rcfg: &RuntimeConfig, listener_fds: &[i32]) {
    close_listener_fds(listener_fds);
    crate::foundation::io::sys::process::monitor_persistence_child_parent();
    crate::foundation::io::persistence::serialize::reset_key_save_delay_interrupt();
    install_child_signal_handlers();
    if let Err(msg) = crate::framework::commands::server::config::set_oom_score_adj_child(rcfg) {
        let line = format!("Warning: BGSAVE child {}: {}\n", std::process::id(), msg);
        let bytes = line.as_bytes();
        crate::framework::capabilities::process::write_all_fd(libc::STDERR_FILENO, bytes);
    }
}

pub fn apply_bgsave_child_completion(
    ctx: &mut ServerContext,
    pid: i32,
    success: bool,
    completed_sec: i64,
) -> Result<(), ()> {
    if ctx.persistence().rdb_child_pid() != Some(pid) {
        return Ok(());
    }
    ctx.persistence_mut()
        .complete_rdb_child(success, completed_sec)
        .map_err(|_| ())
}

///   2. dirty >= saveparam.changes；
pub fn check_auto_bgsave(ctx: &mut ServerContext) {
    if let Some(method) = ctx.persistence().scheduled_bgsave_method() {
        let now_sec = (now_ms() / 1000) as i64;
        let should_retry = ctx.persistence().rdb().last_bgsave_status
            == crate::engine::persistence::OperationStatus::Ok
            || now_sec - ctx.persistence().rdb().last_bgsave_try_sec
                > crate::engine::extension::config::persistence::BGSAVE_RETRY_DELAY_SEC;
        if should_retry && !ctx.persistence().has_active_child()
            && start_bgsave_with_method(ctx, method).is_ok()
        {
            ctx.persistence_mut().set_bgsave_scheduled(false);
            ctx.update_dict_resize_policy();
        }
    }
    let saveparams = ctx.config().persistence.saveparams.clone();
    if saveparams.is_empty() {
        return;
    }
    let now = (now_ms() / 1000) as i64;
    if crate::engine::persistence::rdb::should_auto_bgsave(ctx.persistence(), &saveparams, now) {
        crate::server_log!(notice, "Persistence thresholds reached. Saving...",);
        match start_bgsave(ctx) {
            Ok(()) => {
                crate::server_log!(notice, "Background saving started");
            }
            Err(()) => {
                crate::server_log!(warning, "Failed to start auto BGSAVE");
            }
        }
    }
}

#[cfg(test)]
fn write_rdb<W: std::io::Write>(
    dbs: &crate::foundation::io::store::kv_engine::DbSet,
    writer: &mut W,
    key_delay_us: i64,
) -> std::io::Result<()> {
    writer.write_all(&crate::foundation::io::persistence::serialize_all(dbs, key_delay_us))
}


#[cfg(test)]
#[path = "save/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "save/tests_rdb.rs"]
mod tests_rdb;
