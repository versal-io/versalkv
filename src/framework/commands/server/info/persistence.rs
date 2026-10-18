use crate::engine::distributed::shutdown::state::ShutdownState;
use crate::engine::persistence::PersistenceState;
use crate::framework::context::ServerContext;
use crate::framework::context::RuntimeConfig;

use super::util::emit_line;

fn shutdown_in_milliseconds(state: &ShutdownState) -> i64 {
    if !state.asap && !state.initiated {
        return 0;
    }
    let now = crate::foundation::io::sys::time::mstime();
    if state.mstime_deadline > 0 {
        (state.mstime_deadline - now).max(0)
    } else {
        0
    }
}

pub(super) fn gen_persistence_section(
    rcfg: &RuntimeConfig,
    state: &PersistenceState,
    shutdown_state: &ShutdownState,
    is_loading: bool,
    is_async_loading: bool,
) -> String {
    let mut s = String::new();
    s.push_str("# Persistence\r\n");
    emit_line(&mut s, "loading", i64::from(is_loading));
    emit_line(&mut s, "async_loading", i64::from(is_async_loading));
    let rdb_in_progress = if state.rdb_bgsave_in_progress() { 1 } else { 0 };
    emit_line(&mut s, "rdb_changes_since_last_save", state.dirty());
    emit_line(&mut s, "rdb_bgsave_in_progress", rdb_in_progress);
    emit_line(&mut s, "rdb_last_save_time", state.rdb().last_save_time_sec);
    emit_line(
        &mut s,
        "rdb_last_bgsave_status",
        state.rdb().last_bgsave_status.as_str(),
    );
    emit_line(&mut s, "rdb_last_bgsave_time_sec", -1);
    emit_line(&mut s, "rdb_current_bgsave_time_sec", -1);
    emit_line(&mut s, "rdb_saves", 0);
    emit_line(&mut s, "rdb_last_cow_size", state.rdb().last_cow_size);
    emit_line(
        &mut s,
        "rdb_last_load_keys_expired",
        state.rdb().last_load_keys_expired,
    );
    emit_line(
        &mut s,
        "rdb_last_load_keys_loaded",
        state.rdb().last_load_keys_loaded,
    );
    emit_line(&mut s, "current_cow_peak", 0);
    emit_line(&mut s, "current_cow_size", state.rdb().current_cow_size);
    emit_line(&mut s, "current_cow_size_age", 0);
    emit_line(&mut s, "current_fork_perc", format!("{:.2}", state.rdb().current_fork_perc));
    emit_line(
        &mut s,
        "current_save_keys_processed",
        state.rdb().current_save_keys_processed,
    );
    emit_line(&mut s, "current_save_keys_total", state.rdb().current_save_keys_total);
    let aof_enabled = if rcfg.persistence.aof_enabled { 1 } else { 0 };
    let aof_rewrite_in_progress = if state.aof_rewrite_child_pid().is_some() {
        1
    } else {
        0
    };
    emit_line(&mut s, "aof_enabled", aof_enabled);
    emit_line(&mut s, "aof_rewrite_in_progress", aof_rewrite_in_progress);
    emit_line(
        &mut s,
        "aof_rewrite_scheduled",
        i64::from(state.aof_rewrite_scheduled()),
    );
    emit_line(
        &mut s,
        "aof_last_rewrite_time_sec",
        state.aof().rewrite.last_duration_sec,
    );
    emit_line(&mut s, "aof_current_rewrite_time_sec", -1);
    emit_line(
        &mut s,
        "aof_last_bgrewrite_status",
        state.aof().rewrite.last_status.as_str(),
    );
    emit_line(
        &mut s,
        "aof_last_write_status",
        state.aof().last_write_status.as_str(),
    );
    emit_line(&mut s, "aof_last_cow_size", 0);
    emit_line(&mut s, "aof_current_size", state.aof().current_size);
    emit_line(&mut s, "aof_pending_bio_fsync", 0);
    emit_line(&mut s, "aof_delayed_fsync", state.aof().delayed_fsync);
    emit_line(&mut s, "aof_rewrites", state.aof().rewrite.total_rewrites);
    emit_line(
        &mut s,
        "aof_rewrites_consecutive_failures",
        state.aof().rewrite.consecutive_failures,
    );
    emit_line(&mut s, "module_fork_in_progress", 0);
    emit_line(&mut s, "module_fork_last_cow_size", 0);
    emit_line(&mut s, "slot_migration_fork_in_progress", 0);
    emit_line(
        &mut s,
        "shutdown_in_milliseconds",
        shutdown_in_milliseconds(shutdown_state),
    );
    s
}

pub(super) fn gen_cpu_section(ctx: &ServerContext) -> String {
    let (u_sec, u_usec) = crate::foundation::io::sys::time::used_cpu_user_sec_frac();
    let (s_sec, s_usec) = crate::foundation::io::sys::time::used_cpu_sys_sec_frac();
    let stats = ctx.stats();
    let mut s = String::new();
    s.push_str("# CPU\r\n");
    emit_line(&mut s, "used_cpu_sys", format!("{}.{:06}", s_sec, s_usec));
    emit_line(&mut s, "used_cpu_user", format!("{}.{:06}", u_sec, u_usec));
    emit_line(&mut s, "used_cpu_sys_children", "0.000000");
    emit_line(&mut s, "used_cpu_user_children", "0.000000");
    emit_line(
        &mut s,
        "used_cpu_sys_main_thread",
        format!("{}.{:06}", s_sec, s_usec),
    );
    emit_line(
        &mut s,
        "used_cpu_user_main_thread",
        format!("{}.{:06}", u_sec, u_usec),
    );
    emit_line(
        &mut s,
        "used_active_time_main_thread",
        format!(
            "{}.{:06}",
            stats.active_time_usec / 1_000_000,
            stats.active_time_usec % 1_000_000
        ),
    );
    for id in 1..ctx.io_thread_info().configured_threads {
        let active_us = ctx
            .io_thread_info()
            .active_time_us
            .get(id)
            .copied()
            .unwrap_or(0);
        emit_line(
            &mut s,
            &format!("used_active_time_io_thread_{id}"),
            format!("{}.{:06}", active_us / 1_000_000, active_us % 1_000_000),
        );
    }
    s
}

#[cfg(test)]
mod tests {
    use super::{gen_cpu_section, gen_persistence_section};
    use crate::engine::distributed::shutdown::state::ShutdownState;
    use crate::engine::extension::config::RuntimeConfig;
    use crate::engine::persistence::PersistenceState;

    #[test]
    fn async_replication_load_is_reported_without_blocking_loading() {
        let info = gen_persistence_section(
            &RuntimeConfig::new(),
            &PersistenceState::new(),
            &ShutdownState::default(),
            false,
            true,
        );

        assert!(info.contains("loading:0\r\n"), "{info}");
        assert!(info.contains("async_loading:1\r\n"), "{info}");
    }

    #[test]
    fn diskless_child_sets_rdb_bgsave_in_progress_line() {
        let mut state = PersistenceState::new();
        state.begin_diskless_child(999).unwrap();
        let out = gen_persistence_section(
            &RuntimeConfig::new(),
            &state,
            &ShutdownState::default(),
            false,
            false,
        );
        assert!(out.contains("rdb_bgsave_in_progress:1\r\n"), "got:\n{out}");
    }

    #[test]
    fn cpu_section_reports_only_configured_io_worker_times() {
        let mut ctx = crate::framework::context::ServerContext::new(1);
        ctx.set_io_thread_info(crate::framework::context::IoThreadInfo {
            configured_threads: 3,
            active_time_us: vec![0, 1_500_000, 25],
            ..crate::framework::context::IoThreadInfo::default()
        });

        let section = gen_cpu_section(&ctx);

        assert!(section.contains("used_active_time_io_thread_1:1.500000\r\n"));
        assert!(section.contains("used_active_time_io_thread_2:0.000025\r\n"));
        assert!(!section.contains("used_active_time_io_thread_3:"));
    }
}
