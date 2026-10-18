use super::{
    bgrewriteaof, reap_aof_child_if_any, rewrite_file_options, rotate_increment_for_rewrite,
    ReapResult, FAIL_NEXT_REWRITE_AOF_OPEN,
};
#[cfg(target_os = "linux")]
use super::{start_aof_rewrite, PAUSE_REWRITE_CHILD_AFTER_AFFINITY};
use crate::engine::persistence::OperationStatus;
use crate::foundation::io::persistence::journal::AofJournal;
use crate::foundation::io::persistence::{AofAppender, AofOpenMode};
use crate::framework::capabilities::connection::Connection;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use bytes::Bytes;
use std::time::{Duration, Instant};

fn _temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "valkey-bgrewriteaof-cursor-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ))
}

fn _prepare_successful_rewrite_child(ctx: &mut ServerContext) -> libc::pid_t {
    let pid = crate::framework::capabilities::process::fork_owned_child(|| 0)
        .expect("fork rewrite child")
        .into_pid();
    ctx.persistence_mut()
        .begin_aof_rewrite_child(pid, 1_000)
        .unwrap();
    let options = rewrite_file_options(ctx);
    crate::foundation::io::persistence::aof_rewrite::write_rewrite_temp(
        ctx.dbs(),
        &[],
        &[],
        &options,
        pid,
    )
    .unwrap();
    pid
}

fn _reap_rewrite_child(ctx: &mut ServerContext) -> ReapResult {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(result) = reap_aof_child_if_any(ctx) {
            return result;
        }
        assert!(Instant::now() < deadline, "AOF rewrite child did not exit");
        std::thread::yield_now();
    }
}

#[test]
fn aof_rewrite_child_failure_completion_is_idempotent() {
    let dir = _temp_dir().join("idempotent-failure");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create rewrite test directory");
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.aof_enabled = true;
    ctx.persistence_mut()
        .begin_aof_rewrite_child(1234, 1_000)
        .unwrap();

    assert_eq!(
        super::apply_aof_rewrite_child_completion(&mut ctx, 1234, false),
        Some(ReapResult::Error)
    );
    assert_eq!(
        super::apply_aof_rewrite_child_completion(&mut ctx, 1234, false),
        None
    );

    assert_eq!(ctx.persistence().aof_rewrite_child_pid(), None);
    assert!(ctx.persistence().aof_rewrite_scheduled());
    std::fs::remove_dir_all(dir).expect("remove rewrite test directory");
}

#[test]
fn aof_rewrite_child_completion_ignores_stale_pid() {
    let dir = _temp_dir().join("stale-pid");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create rewrite test directory");
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.persistence_mut()
        .begin_aof_rewrite_child(1234, 1_000)
        .unwrap();

    assert_eq!(
        super::apply_aof_rewrite_child_completion(&mut ctx, 4321, false),
        None
    );

    assert_eq!(ctx.persistence().aof_rewrite_child_pid(), Some(1234));
    ctx.persistence_mut().abort_child();
    std::fs::remove_dir_all(dir).expect("remove rewrite test directory");
}

#[cfg(target_os = "linux")]
fn first_allowed_cpu() -> String {
    let status = std::fs::read_to_string("/proc/self/status").expect("read current process status");
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .expect("current process CPU allowance")
        .split(',')
        .next()
        .expect("non-empty CPU allowance")
        .split('-')
        .next()
        .expect("CPU range start")
        .to_string()
}

#[cfg(target_os = "linux")]
#[test]
fn aof_rewrite_child_applies_configured_cpu_affinity() {
    use crate::foundation::io::sys::process::{send_signal, wait_blocking, ProcessSignal, WaitOutcome};

    let dir = _temp_dir().join("affinity");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create rewrite test directory");
    let cpu = first_allowed_cpu();
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().server.aof_rewrite_cpulist = cpu.clone();
    PAUSE_REWRITE_CHILD_AFTER_AFFINITY.with(|pause| pause.set(true));

    start_aof_rewrite(&mut ctx).expect("start AOF rewrite child");
    PAUSE_REWRITE_CHILD_AFTER_AFFINITY.with(|pause| pause.set(false));
    let pid = ctx
        .persistence()
        .aof_rewrite_child_pid()
        .expect("AOF rewrite child pid");
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
            .expect("read AOF rewrite child status");
        let affinity = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"));
        let stopped = status
            .lines()
            .any(|line| line.starts_with("State:\tT"));
        if affinity == Some(cpu.as_str()) && stopped {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "AOF rewrite child affinity remained {affinity:?}, expected {cpu:?}"
        );
        std::thread::yield_now();
    }
    send_signal(pid, ProcessSignal::Continue).expect("continue AOF rewrite child");
    assert_eq!(wait_blocking(pid).unwrap(), WaitOutcome::Exited(0));
    ctx.persistence_mut().abort_child();
    std::fs::remove_dir_all(dir).expect("remove rewrite test directory");
}

#[test]
fn bgrewriteaof_inside_exec_is_scheduled_without_forking() {
    let mut ctx = ServerContext::new(1);
    let mut conn = Connection::new_fake();
    assert!(conn.transaction.begin(0, 0));
    let args = CommandArgs::from_parts(vec![Bytes::from_static(b"BGREWRITEAOF")]);

    assert_eq!(
        bgrewriteaof(&mut conn, &args, &mut ctx),
        crate::foundation::io::protocol::Value::SimpleString(
            "Background append only file rewriting scheduled".into()
        )
    );
    assert!(ctx.persistence().aof_rewrite_scheduled());
    assert!(!ctx.persistence().has_active_child());
}

#[test]
fn rewrite_file_options_carry_incremental_fsync_policy() {
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.aof_rewrite_incremental_fsync = false;

    assert!(!rewrite_file_options(&ctx).incremental_fsync);
}

#[test]
fn rewrite_journal_rotation_resets_only_aof_cursor() {
    let dir = _temp_dir();
    let _ = std::fs::remove_dir_all(&dir);
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.aof_enabled = true;
    let config = &ctx.config().persistence;
    let appender = AofAppender::open(
        &config.dir,
        &config.aof_filename,
        &config.aof_appenddirname,
        AofOpenMode::CreateNew,
        config.aof_fsync,
    )
    .unwrap();
    ctx.aof_mut().journal = Some(AofJournal::new(appender));
    ctx.set_repl_seldb(9);
    ctx.persistence_mut().set_aof_selected_db(7);

    let options = rewrite_file_options(&ctx);
    rotate_increment_for_rewrite(&mut ctx, &options).unwrap();

    assert_eq!(ctx.repl_seldb(), 9);
    assert_eq!(ctx.persistence().aof_selected_db(), -1);

    if let Some(mut journal) = ctx.aof_mut().journal.take() {
        journal.appender.shutdown().unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn rewrite_journal_open_failure_resets_only_aof_cursor_before_replacement() {
    let dir = _temp_dir().join("open-failure");
    let _ = std::fs::remove_dir_all(&dir);
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.aof_enabled = true;
    let config = &ctx.config().persistence;
    let appender = AofAppender::open(
        &config.dir,
        &config.aof_filename,
        &config.aof_appenddirname,
        AofOpenMode::CreateNew,
        config.aof_fsync,
    )
    .unwrap();
    let app_dir = dir.join(&config.aof_appenddirname);
    let first_increment = app_dir.join(format!("{}.1.incr.aof", config.aof_filename));
    std::fs::remove_file(&first_increment).unwrap();
    std::fs::create_dir(&first_increment).unwrap();
    std::fs::write(
        app_dir.join(format!("{}.manifest", config.aof_filename)),
        format!(
            "file missing.base.rdb seq 1 type b\nfile {}.1.incr.aof seq 1 type i\n",
            config.aof_filename
        ),
    )
    .unwrap();
    ctx.aof_mut().journal = Some(AofJournal::new(appender));
    ctx.set_repl_seldb(9);
    ctx.persistence_mut().set_aof_selected_db(7);

    let options = rewrite_file_options(&ctx);
    assert!(rotate_increment_for_rewrite(&mut ctx, &options).is_err());

    assert_eq!(ctx.repl_seldb(), 9);
    assert_eq!(ctx.persistence().aof_selected_db(), -1);
    assert!(ctx.aof().journal.is_some());

    if let Some(mut journal) = ctx.aof_mut().journal.take() {
        journal.appender.shutdown().unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn failed_finalize_and_old_aof_reopen_disable_aof_and_reset_only_aof_cursor() {
    let dir = _temp_dir().join("old-aof-reopen-failure");
    let _ = std::fs::remove_dir_all(&dir);
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.aof_enabled = true;
    ctx.set_repl_seldb(9);
    let pid = _prepare_successful_rewrite_child(&mut ctx);
    ctx.persistence_mut().set_aof_selected_db(7);
    let options = rewrite_file_options(&ctx);
    std::fs::remove_file(options.temp_path(pid)).unwrap();
    FAIL_NEXT_REWRITE_AOF_OPEN.with(|fail| fail.set(true));

    assert_eq!(_reap_rewrite_child(&mut ctx), ReapResult::Error);

    assert!(!ctx.config().persistence.aof_enabled);
    assert_eq!(ctx.aof().rewrite.last_status, OperationStatus::Err);
    assert_eq!(ctx.persistence().aof_selected_db(), -1);
    assert_eq!(ctx.repl_seldb(), 9);
    assert!(ctx.aof().journal.is_none());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn replacement_aof_open_failure_disables_aof_and_resets_only_aof_cursor() {
    let dir = _temp_dir().join("replacement-aof-open-failure");
    let _ = std::fs::remove_dir_all(&dir);
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.aof_enabled = true;
    ctx.set_repl_seldb(9);
    _prepare_successful_rewrite_child(&mut ctx);
    ctx.persistence_mut().set_aof_selected_db(7);
    FAIL_NEXT_REWRITE_AOF_OPEN.with(|fail| fail.set(true));

    assert_eq!(_reap_rewrite_child(&mut ctx), ReapResult::Error);

    assert!(!ctx.config().persistence.aof_enabled);
    assert_eq!(ctx.aof().rewrite.last_status, OperationStatus::Err);
    assert_eq!(ctx.persistence().aof_selected_db(), -1);
    assert_eq!(ctx.repl_seldb(), 9);
    assert!(ctx.aof().journal.is_none());
    std::fs::remove_dir_all(dir).unwrap();
}
