//! Shutdown pipeline: prepare/finish/abort core + kill_children helper.
//! `prepare_for_shutdown` is the single entry from both signal-driven cron and
//! the SHUTDOWN command handler. It records flags on `ShutdownState`, rejects
//! invalid contexts (e.g. initial AOFRW without NOW/FORCE), and delegates the
//! work to `finish_shutdown` which kills persistence children, optionally
//! writes a final RDB, and flushes the AOF before flipping
//! `ctx.shutdown_requested`.

use crate::engine::distributed::shutdown::messages;
use crate::engine::distributed::shutdown::{
    replicas_are_ready, select_shutdown_handoff, ShutdownHandoffDecision, ShutdownReplicaFacts,
    SHUTDOWN_FAILOVER, SHUTDOWN_FORCE, SHUTDOWN_NOSAVE, SHUTDOWN_NOW, SHUTDOWN_SAFE, SHUTDOWN_SAVE,
};
use crate::engine::persistence::{PersistenceChild, RdbSaveOptions};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;

pub fn execute(ctx: &mut ServerContext, args: &CommandArgs) -> Value {
    let (flags, should_abort) = match crate::framework::commands::server::shutdown::parse_argv(args)
    {
        Ok(parsed) => parsed,
        Err(reply) => return reply,
    };
    if should_abort {
        return if abort_shutdown(ctx) {
            Value::ok()
        } else {
            Value::error("ERR No shutdown in progress")
        };
    }
    if flags & SHUTDOWN_FAILOVER != 0 && !ctx.config().server.cluster_enabled {
        return Value::error("ERR SHUTDOWN FAILOVER is only supported in cluster mode.");
    }
    if ctx.is_script_busy() {
        if flags & SHUTDOWN_NOSAVE == 0 {
            return Value::error(
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
            );
        }
        let has_initial_aof_rewrite = ctx.persistence().aof_rewrite_child_pid().is_some();
        return match crate::engine::distributed::shutdown::request_shutdown(
            ctx.shutdown_state_mut(),
            flags,
            has_initial_aof_rewrite,
        ) {
            Ok(()) => {
                // Final persistence shutdown must run only after the Lua hook
                // unwinds and the outer EVAL/FCALL commits its propagation.
                ctx.interrupt_running_script_for_shutdown();
                Value::ok()
            }
            Err(message) => {
                crate::server_log!(warning, "{}", message);
                Value::error(message)
            }
        };
    }
    match prepare_for_shutdown(ctx, flags) {
        Ok(()) => {
            ctx.interrupt_running_script_for_shutdown();
            Value::ok()
        }
        Err(message) => shutdown_error_reply(message),
    }
}

fn shutdown_error_reply(message: &str) -> Value {
    Value::error(format!("ERR {message}"))
}

fn abort_shutdown(ctx: &mut ServerContext) -> bool {
    let aborted = crate::engine::distributed::shutdown::abort_shutdown(ctx.shutdown_state_mut());
    crate::foundation::io::sys::signal::take_pending_signal();
    let _ = crate::foundation::io::sys::signal::take_second_sigint();
    if aborted {
        crate::server_log!(notice, "{}", messages::MANUALLY_ABORTED);
    }
    aborted
}

/// Enter the shutdown pipeline. Returns Ok on success (event loop will exit
/// next tick because `ctx.shutdown_requested` was set) or Err(message) with a
/// human-readable rejection reason (already logged). Callers translate Err
/// into an RESP error reply.
pub fn prepare_for_shutdown(ctx: &mut ServerContext, flags: u32) -> Result<(), &'static str> {
    let flags = if ctx.is_loading() || ctx.sentinel_state().sentinel_mode {
        (flags & !SHUTDOWN_SAVE) | SHUTDOWN_NOSAVE
    } else {
        flags
    };
    // Validate first: rejecting shutdown must leave `ShutdownState` untouched
    // so a later retry (with NOW/FORCE) starts from a clean slate.
    if ctx.persistence().aof_rewrite_child_pid().is_some()
        && flags & (SHUTDOWN_NOW | SHUTDOWN_FORCE) == 0
    {
        crate::server_log!(warning, "{}", messages::ERR_AOF_INIT_CANT_EXIT);
        // Clear the signal-scheduled shutdown so cron doesn't keep retrying
        // once the AOF child completes. Aligns with C's cancelShutdown() on
        // finishShutdown() error path.
        ctx.shutdown_state_mut().asap = false;
        ctx.shutdown_state_mut().pending_flags = 0;
        crate::foundation::io::sys::signal::take_pending_signal();
        return Err(messages::ERR_AOF_INIT_CANT_EXIT);
    }

    if ctx.shutdown_state().initiated {
        return Ok(());
    }

    let now = crate::foundation::io::sys::time::mstime();
    let shutdown_timeout_ms = (ctx.config().server.shutdown_timeout as i64).saturating_mul(1_000);
    let should_wait = flags & SHUTDOWN_NOW == 0
        && ctx.config().server.shutdown_timeout != 0
        && !replicas_are_ready_for_shutdown(ctx);

    {
        let state = ctx.shutdown_state_mut();
        state.flags = flags;
        state.asap = false;
        state.initiated = true;
        state.replica_getack_sent = false;
        state.mstime_deadline = if should_wait {
            now.saturating_add(shutdown_timeout_ms)
        } else {
            0
        };
        if state.start_time_ms == 0 {
            state.start_time_ms = now;
        }
    }

    if flags & SHUTDOWN_NOSAVE != 0 {
        crate::server_log!(notice, "{}", messages::USER_REQUESTED);
    }

    if should_wait {
        ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
        ctx.client_pause_mut().end_time_ms = i64::MAX;
        ctx.client_pause_mut().reason = "shutdown";
        crate::server_log!(notice, "Waiting for replicas before shutting down.");
        return Ok(());
    }

    finish_shutdown(ctx, flags)
}

pub(crate) fn finish_shutdown(ctx: &mut ServerContext, flags: u32) -> Result<(), &'static str> {
    log_replica_shutdown_status(ctx);
    let is_voting_cluster_primary = ctx.cluster_state().as_ref().is_some_and(|state| {
        crate::engine::distributed::cluster::ClusterState::is_voting_primary(&state.myself)
    });
    if flags & SHUTDOWN_SAFE != 0 && is_voting_cluster_primary {
        if flags & SHUTDOWN_FORCE != 0 {
            crate::server_log!(
                warning,
                "This is a voting primary. Shutting down may cause the cluster to go down. Exit anyway."
            );
        } else {
            crate::server_log!(
                warning,
                "This is a voting primary. Shutting down may cause the cluster to go down. Can't exit."
            );
            cancel_failed_shutdown(ctx);
            return Err(messages::ERR_SHUTDOWN);
        }
    }
    if ctx.config().persistence.aof_enabled
        && ctx.aof().journal.is_none()
        && ctx.persistence().aof_rewrite_child_pid().is_some()
        && flags & SHUTDOWN_FORCE == 0
    {
        crate::server_log!(warning, "{}", messages::ERR_AOF_INIT_CANT_EXIT);
        crate::server_log!(
            warning,
            "Errors trying to shut down the server. Check the logs for more information."
        );
        crate::server_log!(warning, "{}", messages::ERR_SHUTDOWN);
        cancel_failed_shutdown(ctx);
        return Err(messages::ERR_SHUTDOWN);
    }
    kill_children(ctx, flags & SHUTDOWN_NOW != 0, flags & SHUTDOWN_FORCE != 0);

    let should_save = if flags & SHUTDOWN_SAVE != 0 {
        true
    } else if flags & SHUTDOWN_NOSAVE != 0 {
        false
    } else {
        !ctx.config().persistence.aof_enabled && !ctx.config().persistence.saveparams.is_empty()
    };

    if should_save {
        crate::server_log!(notice, "Saving the final RDB snapshot before exiting.");
        let options = RdbSaveOptions::from_config(&ctx.config().persistence);
        let replication = ctx.rdb_replication_info();
        let preserve_expired = !ctx.config().server.active_expire_enabled;
        ctx.dbs_mut().set_pause_skip_expire_all(preserve_expired);
        let slot_imports = ctx.slot_import_dbs();
        let save_result = crate::engine::persistence::rdb::save_with_replication(
            ctx.dbs(),
            &slot_imports,
            &options,
            Some(&replication),
        );
        ctx.dbs_mut().set_pause_skip_expire_all(false);
        if save_result.is_ok() {
            ctx.persistence_mut()
                .record_rdb_save(crate::foundation::io::sys::time::mstime() / 1_000);
        }
        if save_result.is_err() {
            if flags & SHUTDOWN_FORCE == 0 {
                crate::server_log!(warning, "{}", messages::ERR_SAVE_CANT_EXIT);
                crate::server_log!(
                    warning,
                    "Errors trying to shut down the server. Check the logs for more information."
                );
                crate::server_log!(warning, "{}", messages::ERR_SHUTDOWN);
                cancel_failed_shutdown(ctx);
                crate::foundation::io::sys::signal::take_pending_signal();
                return Err(messages::ERR_SHUTDOWN);
            }
            crate::server_log!(
                warning,
                "{} Exit anyway (FORCE).",
                messages::ERR_SAVE_CANT_EXIT
            );
        }
    }

    if let Some(ref mut aof) = ctx.aof_mut().journal {
        crate::server_log!(notice, "Flushing and closing AOF file.");
        let _ = aof.appender.flush(true);
        let _ = aof.appender.shutdown();
    }

    crate::server_log!(notice, "{}", messages::READY_TO_EXIT);
    ctx.set_shutdown_requested(true);
    Ok(())
}

fn cancel_failed_shutdown(ctx: &mut ServerContext) {
    ctx.shutdown_state_mut().initiated = false;
    ctx.shutdown_state_mut().asap = false;
    ctx.shutdown_state_mut().pending_flags = 0;
    ctx.shutdown_state_mut().mstime_deadline = 0;
    ctx.shutdown_state_mut().replica_getack_sent = false;
    clear_shutdown_pause(ctx);
    crate::foundation::io::sys::signal::take_pending_signal();
}

fn replicas_are_ready_for_shutdown(ctx: &ServerContext) -> bool {
    let primary_offset = ctx.replication().backlog.reported_offset();
    let replicas: Vec<_> = ctx
        .replication()
        .view
        .replica_info
        .values()
        .map(|info| (info.state == "online", info.offset))
        .collect();
    replicas_are_ready(primary_offset, &replicas)
}

fn log_replica_shutdown_status(ctx: &ServerContext) {
    let primary_offset = ctx.replication().backlog.reported_offset();
    let mut total = 0usize;
    let mut in_sync = 0usize;
    for info in ctx.replication().view.replica_info.values() {
        total += 1;
        if info.state == "online" && info.offset.is_some_and(|offset| offset >= primary_offset) {
            in_sync += 1;
        } else {
            crate::server_log!(
                notice,
                "Lagging replica {} reported offset {} behind master, lag=0, state={}.",
                format!("{}:{}", info.ip, info.port),
                info.offset.map_or(primary_offset.get(), |offset| {
                    primary_offset.get().saturating_sub(offset.get())
                }),
                info.state
            );
        }
    }
    if total > 0 {
        crate::server_log!(
            notice,
            "{} of {} replicas are in sync when shutting down.",
            in_sync,
            total
        );
    }
}

pub(crate) fn clear_shutdown_pause(ctx: &mut ServerContext) {
    ctx.client_pause_mut().action = crate::framework::context::PauseAction::None;
    ctx.client_pause_mut().end_time_ms = 0;
    ctx.client_pause_mut().reason = "none";
    ctx.set_pending_unpause(true);
}

impl crate::runtime::server::worker::ServerWorkerState {
    pub(crate) fn perform_shutdown_handoff_if_needed(&mut self) {
        if !self.ctx.is_shutdown_requested()
            || self.ctx.shutdown_state().cluster_handoff_done
            || self.ctx.shutdown_state().flags & SHUTDOWN_FAILOVER == 0
        {
            return;
        }
        self.ctx.shutdown_state_mut().cluster_handoff_done = true;
        let local_is_primary = self
            .ctx
            .cluster_state()
            .as_ref()
            .is_some_and(|state| state.myself.is_primary());
        if !local_is_primary {
            return;
        }

        let primary_offset = self.ctx.replication().backlog.reported_offset();
        let replicas: Vec<_> = self
            .replication_runtime
            .primary_links()
            .values()
            .map(|link| ShutdownReplicaFacts {
                connection_id: link.connection_id().0,
                node_id: self
                    .replication_runtime
                    .replica_node_id(link.connection_id()),
                version: link.replica_version(),
                is_online: matches!(
                    link.state(),
                    crate::framework::distributed::replication::PrimaryLinkState::Streaming
                ),
                ack_offset: link.last_ack_position(),
            })
            .collect();
        let selected = match select_shutdown_handoff(primary_offset, &replicas) {
            ShutdownHandoffDecision::Selected(node_id) => node_id,
            ShutdownHandoffDecision::LegacyReplica => {
                crate::server_log!(
                    notice,
                    "Unable to perform auto failover on shutdown since there are legacy replicas."
                );
                return;
            }
            ShutdownHandoffDecision::NoCandidate => {
                crate::server_log!(
                    notice,
                    "Unable to find a replica to perform the auto failover on shutdown."
                );
                return;
            }
        };

        let frame = crate::engine::distributed::replication::protocol::encode_command(&[
            b"CLUSTER",
            b"FAILOVER",
            b"FORCE",
            b"REPLICAID",
            selected.as_str().as_bytes(),
        ]);
        self.propagate_to_replicas(&frame);
        crate::server_log!(
            notice,
            "Perform auto failover to replica {} on shutdown.",
            selected
        );

        // Match C's best-effort flushReplicasOutputBuffers(): the event loop is
        // about to stop, so give each replica socket one synchronous drain.
        let replica_connections: Vec<_> = self
            .replication_runtime
            .primary_links()
            .values()
            .map(|link| link.connection_id().0)
            .collect();
        for connection_id in replica_connections {
            if let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id))
                .then_some(connection_id)
            {
                let _ = self.flush_connection_output(connection_token);
            }
        }
    }

    pub(crate) fn send_shutdown_getack_if_needed(&mut self) {
        if self.ctx.shutdown_state().mstime_deadline == 0
            || self.ctx.shutdown_state().replica_getack_sent
        {
            return;
        }
        self.propagate_to_replicas_with_ack(&[], true);
        self.ctx.shutdown_state_mut().replica_getack_sent = true;
    }

    pub(crate) fn drive_shutdown_progress(&mut self) {
        if !self.ctx.shutdown_state().initiated || self.ctx.shutdown_state().mstime_deadline == 0 {
            return;
        }
        self.refresh_replication_view();
        let now = crate::foundation::io::sys::time::mstime();
        let primary_offset = self.ctx.replication().backlog.reported_offset();
        let replicas: Vec<_> = self
            .ctx
            .replication()
            .view
            .replica_info
            .values()
            .map(|info| (info.state == "online", info.offset))
            .collect();
        let ready =
            crate::engine::distributed::shutdown::replicas_are_ready(primary_offset, &replicas);
        if !ready && now < self.ctx.shutdown_state().mstime_deadline {
            return;
        }
        let flags = self.ctx.shutdown_state().flags;
        self.register_active_persistence_child();
        let shutdown_result = finish_shutdown(&mut self.ctx, flags);
        self.register_active_persistence_child();
        if shutdown_result.is_err() {
            clear_shutdown_pause(&mut self.ctx);
            self.reply_to_shutdown_clients();
        }
    }

    pub(crate) fn reply_to_shutdown_clients(&mut self) {
        let tokens: Vec<_> = self
            .connection_registry
            .iter()
            .filter_map(|(_, conn)| {
                matches!(
                    conn.blocked.as_ref().map(|blocked| &blocked.cmd),
                    Some(crate::framework::capabilities::connection::BlockedCmd::Shutdown)
                )
                .then_some(conn.token())
            })
            .collect();
        for token in tokens {
            let reply = shutdown_error_reply(messages::ERR_SHUTDOWN);
            self.complete_blocked_command_reply(token, reply, true);
        }
    }
}

fn kill_children(ctx: &mut ServerContext, now: bool, force: bool) {
    let dir = ctx.config().persistence.dir.clone();
    match ctx.persistence().child() {
        Some(PersistenceChild::Rdb { pid, .. }) => {
            // Unlink before waiting so a second SIGINT cannot leave a temp file.
            let _ =
                std::fs::remove_file(std::path::Path::new(&dir).join(format!("temp-{}.rdb", pid)));
            kill_one_child(pid, "saving an .rdb", now, force);
            ctx.persistence_mut().abort_child();
        }
        Some(PersistenceChild::AofRewrite { pid, .. }) => {
            let _ = std::fs::remove_file(
                std::path::Path::new(&dir).join(format!("appendonly.aof.{}.tmp", pid)),
            );
            kill_one_child(pid, "rewriting the AOF", now, force);
            ctx.persistence_mut().abort_child();
        }
        // Diskless full-sync serializes to a pipe rather than a temp file, so
        // there is nothing to unlink; just kill the child and release it.
        Some(PersistenceChild::DisklessSync { pid }) => {
            kill_one_child(pid, "performing a diskless full sync", now, force);
            ctx.persistence_mut().abort_child();
        }
        // Disk-based full-sync is either the legacy pipe relay or a batch file.
        // The event-loop exit path removes the batch's exact snapshot path;
        // this context-only phase owns only the persistence child lifecycle.
        Some(PersistenceChild::DiskSync { pid }) => {
            kill_one_child(pid, "performing a disk full sync", now, force);
            ctx.persistence_mut().abort_child();
        }
        Some(PersistenceChild::SlotMigration { pid }) => {
            kill_one_child(pid, "performing a slot migration snapshot", now, force);
            ctx.persistence_mut().abort_child();
        }
        None => {}
    }
}

fn kill_one_child(pid: i32, desc: &str, now: bool, force: bool) {
    let deadline_ms: i64 = 200;
    if now {
        crate::server_log!(warning, "There is a child {}. Killing it (NOW)!", desc);
        let _ = crate::foundation::io::sys::process::send_signal(
            pid,
            crate::foundation::io::sys::process::ProcessSignal::Kill,
        );
    } else {
        crate::server_log!(notice, "There is a child {}. Sending SIGUSR1...", desc);
        let _ = crate::foundation::io::sys::process::send_signal(
            pid,
            crate::foundation::io::sys::process::ProcessSignal::User1,
        );
    }
    let start = crate::foundation::io::sys::time::mstime();
    loop {
        // A completed child is reaped here. An error means it was already
        // reaped elsewhere (for example by another lifecycle boundary).
        if !matches!(
            crate::foundation::io::sys::process::wait_nonblocking(pid),
            Ok(crate::foundation::io::sys::process::WaitOutcome::Running)
        ) {
            return;
        }
        if crate::foundation::io::sys::time::mstime() - start >= deadline_ms {
            if force {
                crate::server_log!(warning, "Child {} still running, killing it (FORCE)!", desc);
                let _ = crate::foundation::io::sys::process::send_signal(
                    pid,
                    crate::foundation::io::sys::process::ProcessSignal::Kill,
                );
            } else {
                crate::server_log!(warning, "Child {} still running, can't exit.", desc);
            }
            return;
        }
        // Event loop is already stopped at this point; sleeping instead of
        // spin_loop avoids burning ~200ms of CPU while the child exits.
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::engine::command::context::{BlockedCmd, BlockedState, CommandConn};
    use crate::engine::distributed::cluster::ClusterState;
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::engine::distributed::shutdown::{
        messages, SHUTDOWN_FORCE, SHUTDOWN_NOSAVE, SHUTDOWN_SAFE,
    };
    use crate::engine::extension::script::{KillSource, ScriptRunCtx, SCRIPT_EVAL_MODE};
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn runtime_with_blocked_shutdown(
        token: Token,
    ) -> crate::runtime::server::worker::ServerWorkerState {
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        let args = args(&["SAVE"]);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        connection.set_custom_reply_enabled(false);
        connection.set_skip_propagation(false);
        connection.begin_nested_command_effects();
        connection.set_request_bytes_for_test(24);
        connection.blocked = Some(BlockedState {
            cmd: BlockedCmd::Shutdown,
            keys: Vec::new(),
            timeout_ms: 0,
            start_time_ms: crate::foundation::io::sys::time::mstime().saturating_sub(2),
            orig_args: args,
            req_bytes: 24,
            orig_wrapped: None,
        });
        runtime.connection_registry.insert(connection);
        runtime.ctx.shutdown_state_mut().blocked_clients = 1;
        runtime
    }

    fn queue_next_ping(
        runtime: &mut crate::runtime::server::worker::ServerWorkerState,
        token: Token,
    ) {
        runtime.connection_registry[token].begin_nested_command_effects();
        runtime.connection_registry[token].queue_command_reply(b"+PONG\r\n".to_vec());
    }

    #[test]
    fn failed_deferred_shutdown_reply_honors_reply_off() {
        let token = Token(91);
        let mut runtime = runtime_with_blocked_shutdown(token);
        runtime.connection_registry[token].set_reply_off(true);

        runtime.reply_to_shutdown_clients();

        assert!(runtime.connection_registry[token].output.queue.is_empty());
        assert!(runtime.connection_registry[token].reply_off());
        assert_eq!(runtime.connection_registry[token].cmd_count, 1);
        queue_next_ping(&mut runtime, token);
        assert!(runtime.connection_registry[token].output.queue.is_empty());
    }

    #[test]
    fn failed_deferred_shutdown_reply_consumes_reply_skip_once() {
        let token = Token(92);
        let mut runtime = runtime_with_blocked_shutdown(token);
        runtime.connection_registry[token].set_reply_skip(true);

        runtime.reply_to_shutdown_clients();

        assert!(runtime.connection_registry[token].output.queue.is_empty());
        assert!(!runtime.connection_registry[token].reply_skip());
        assert_eq!(runtime.connection_registry[token].cmd_count, 1);
        queue_next_ping(&mut runtime, token);
        assert_eq!(
            runtime.connection_registry[token].output.queue.back(),
            Some(&b"+PONG\r\n".to_vec())
        );
    }

    #[test]
    fn shutdown_timeout_controls_replica_catchup_deadline() {
        let mut ctx = ServerContext::new(1);
        ctx.replication_mut().backlog.append(b"x").unwrap();
        ctx.replication_mut().view.replica_info.insert(
            1,
            crate::engine::distributed::replication::ReplicaStatus {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                offset: None,
                state: "online".to_string(),
                lag_ms: 0,
            },
        );
        ctx.config_mut().server.shutdown_timeout = 2;

        super::prepare_for_shutdown(&mut ctx, SHUTDOWN_NOSAVE).expect("stage shutdown");

        let remaining =
            ctx.shutdown_state().mstime_deadline - crate::foundation::io::sys::time::mstime();
        assert!(
            (1_500..=2_000).contains(&remaining),
            "remaining={remaining}"
        );
        assert_eq!(ctx.client_pause().reason, "shutdown");

        let mut immediate = ServerContext::new(1);
        immediate.replication_mut().backlog.append(b"x").unwrap();
        immediate.replication_mut().view.replica_info.insert(
            1,
            crate::engine::distributed::replication::ReplicaStatus {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                offset: None,
                state: "online".to_string(),
                lag_ms: 0,
            },
        );
        immediate.config_mut().server.shutdown_timeout = 0;
        super::prepare_for_shutdown(&mut immediate, SHUTDOWN_NOSAVE)
            .expect("finish immediate shutdown");
        assert_eq!(immediate.shutdown_state().mstime_deadline, 0);
        assert!(immediate.is_shutdown_requested());
    }

    fn args(values: &[&str]) -> CommandArgs {
        CommandArgs {
            name: Bytes::from_static(b"SHUTDOWN"),
            args: values
                .iter()
                .map(|value| Bytes::copy_from_slice(value.as_bytes()))
                .collect(),
        }
    }

    fn busy_context() -> (ServerContext, Box<ScriptRunCtx>) {
        let mut ctx = ServerContext::new(1);
        let run = Box::new(ScriptRunCtx::new_bare_for_test(SCRIPT_EVAL_MODE));
        run.control.mark_timed_out();
        run.control.mark_write_dirty();
        ctx.script_mut().running_script = Some(run.control.clone());
        ctx.set_script_busy(false, true);
        (ctx, run)
    }

    #[test]
    fn accepted_busy_nosave_stages_shutdown_until_script_unwinds() {
        let (mut ctx, run) = busy_context();

        let reply = execute(&mut ctx, &args(&["NOSAVE"]));

        assert_eq!(reply, Value::ok());
        assert!(!ctx.is_shutdown_requested());
        assert!(ctx.shutdown_state().asap);
        assert!(!ctx.shutdown_state().initiated);
        assert_eq!(ctx.shutdown_state().pending_flags, SHUTDOWN_NOSAVE);
        assert!(run.is_killed());
        assert_eq!(run.control.kill_source(), KillSource::Shutdown);
        assert!(ctx.is_script_busy());
    }

    #[test]
    fn busy_shutdown_without_nosave_is_rejected_without_state_changes() {
        for values in [Vec::<&str>::new(), vec!["SAVE"], vec!["NOW"], vec!["FORCE"]] {
            let (mut ctx, run) = busy_context();

            let reply = execute(&mut ctx, &args(&values));

            assert_eq!(
                reply,
                Value::error(
                    "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."
                ),
                "args: {values:?}"
            );
            assert!(!ctx.is_shutdown_requested(), "args: {values:?}");
            assert!(!ctx.shutdown_state().asap, "args: {values:?}");
            assert!(!ctx.shutdown_state().initiated, "args: {values:?}");
            assert!(!run.is_killed(), "args: {values:?}");
            assert_eq!(
                run.control.kill_source(),
                KillSource::None,
                "args: {values:?}"
            );
            assert!(ctx.is_script_busy(), "args: {values:?}");
        }
    }

    #[test]
    fn rejected_shutdown_does_not_interrupt_busy_script() {
        let (mut ctx, run) = busy_context();

        let reply = execute(&mut ctx, &args(&["SAVE", "NOSAVE"]));

        assert_eq!(reply, Value::error("ERR syntax error"));
        assert!(!ctx.is_shutdown_requested());
        assert!(!run.is_killed());
        assert_eq!(run.control.kill_source(), KillSource::None);
        assert!(ctx.is_script_busy());
    }

    #[test]
    fn shutdown_failover_is_rejected_outside_cluster_mode() {
        let mut ctx = ServerContext::new(1);

        assert_eq!(
            execute(&mut ctx, &args(&["NOSAVE", "FAILOVER"])),
            Value::error("ERR SHUTDOWN FAILOVER is only supported in cluster mode.")
        );
        assert!(!ctx.is_shutdown_requested());
        assert!(!ctx.shutdown_state().initiated);
    }

    #[test]
    fn safe_shutdown_rejects_a_voting_cluster_primary_without_force() {
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.cluster_enabled = true;
        let mut cluster = ClusterState::new("127.0.0.1", 6379, 16_379);
        cluster.myself.slots[0] = true;
        cluster.myself.numslots = 1;
        *ctx.cluster_state_mut() = Some(Box::new(cluster));

        assert_eq!(
            super::prepare_for_shutdown(&mut ctx, SHUTDOWN_NOSAVE | SHUTDOWN_SAFE),
            Err(messages::ERR_SHUTDOWN)
        );
        assert!(!ctx.is_shutdown_requested());
        assert!(!ctx.shutdown_state().initiated);
    }

    #[test]
    fn force_overrides_safe_for_a_voting_cluster_primary() {
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.cluster_enabled = true;
        let mut cluster = ClusterState::new("127.0.0.1", 6379, 16_379);
        cluster.myself.slots[0] = true;
        cluster.myself.numslots = 1;
        *ctx.cluster_state_mut() = Some(Box::new(cluster));

        assert_eq!(
            super::prepare_for_shutdown(&mut ctx, SHUTDOWN_NOSAVE | SHUTDOWN_SAFE | SHUTDOWN_FORCE,),
            Ok(())
        );
        assert!(ctx.is_shutdown_requested());
    }

    #[test]
    fn shutdown_failover_queues_targeted_force_command_on_replication_stream() {
        use crate::framework::distributed::replication::{
            ConnectionId, PrimaryLinkState, RdbFilterMode, ReplicaConnectionInfo,
        };

        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().backlog.activate();
        runtime.ctx.config_mut().server.cluster_enabled = true;
        let cluster = ClusterState::new("127.0.0.1", 6379, 16_379);
        *runtime.ctx.cluster_state_mut() = Some(Box::new(cluster));
        runtime.ctx.shutdown_state_mut().flags =
            SHUTDOWN_NOSAVE | crate::engine::distributed::shutdown::SHUTDOWN_FAILOVER;
        runtime.ctx.set_shutdown_requested(true);
        let connection_id = ConnectionId(77);
        let replica_id = runtime.replication_runtime.register_primary(
            connection_id,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: RdbFilterMode::All,
            },
        );
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        runtime
            .replication_runtime
            .set_replica_node_id(connection_id, node_id.to_string());
        let link = runtime
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap();
        link.set_replica_version(0x09_00_00);
        link.set_state(PrimaryLinkState::Streaming);
        link.acknowledge(offset(0), 1);

        runtime.perform_shutdown_handoff_if_needed();

        let expected = crate::engine::distributed::replication::protocol::encode_command(&[
            b"CLUSTER",
            b"FAILOVER",
            b"FORCE",
            b"REPLICAID",
            node_id.as_bytes(),
        ]);
        assert_eq!(
            runtime.ctx.replication().backlog.read_from(
                crate::engine::distributed::replication::BacklogCursor::try_from(0).unwrap(),
            ),
            Some(expected)
        );
    }
}
