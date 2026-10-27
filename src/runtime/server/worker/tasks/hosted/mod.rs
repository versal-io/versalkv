//! Native hosted work remains synchronously scoped.
//!
//! The ownership audit for the driver boundary is deliberate: command and ACL
//! requests borrow `CommandArgs`; script checkpoints borrow `ScriptRunCtx`; RDB
//! progress may borrow the visible `DbSet`; and all yield variants depend on a
//! precise protected-resource scope. None can safely cross a dispatch boundary.
//! Consequently this adapter keeps using `HostDispatcher<'_>`. The owned host
//! request/completion protocol is exercised by hosts that create genuinely
//! owned external operations, and native dispatch rejects an unexpected owned
//! completion instead of lifetime-erasing one of these requests.

use crate::engine::host::{
    HostAbort, HostCommandOrigin, HostDispatcher, HostError, HostReply, HostRequest, HostYield,
};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{CommandEffectDrain, Connection, TransactionEnd};
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::blocked_events::BlockedOperation;

use crate::runtime::server::worker::ServerWorkerState;

#[derive(Clone, Copy, Default)]
pub(crate) struct HostTaskScope {
    protected_client: Option<Token>,
    blocked_operation: Option<BlockedOperation>,
}

impl HostTaskScope {
    pub(crate) fn background() -> Self {
        Self::default()
    }

    pub(crate) fn client(protected_client: Token) -> Self {
        Self {
            protected_client: Some(protected_client),
            blocked_operation: None,
        }
    }

    pub(crate) fn blocked(operation: BlockedOperation) -> Self {
        Self {
            protected_client: None,
            blocked_operation: Some(operation),
        }
    }
}

impl ServerWorkerState {
    pub(crate) fn run_prepared_host_task(
        &mut self,
        token: Token,
        task: crate::framework::host_tasks::PreparedHostTask,
    ) -> crate::foundation::io::protocol::Value {
        match task {
            crate::framework::host_tasks::PreparedHostTask::Lua(mut task) => {
                self.ctx.enter_script(task.control());
                let result = self
                    .with_host_dispatcher(HostTaskScope::client(token), |dispatcher| {
                        task.run(dispatcher)
                    });
                self.ctx.leave_script();
                let Some(connection) = self.connection_registry.get_mut(token) else {
                    return Value::Null;
                };
                task.finish(result, connection, &mut self.ctx)
            }
            crate::framework::host_tasks::PreparedHostTask::DebugRdb(mut task) => {
                let result = self
                    .with_host_dispatcher(HostTaskScope::client(token), |dispatcher| {
                        task.run(dispatcher)
                    });
                task.finish(result, &mut self.ctx)
            }
            crate::framework::host_tasks::PreparedHostTask::DebugAof(mut task) => {
                let mut replay_conn = Connection::new_fake();
                replay_conn.import_source = true;
                let inputs = task.take_inputs();
                let last_input_index = inputs.len().saturating_sub(1);
                let allow_truncated = self.ctx.config().persistence.aof_load_truncated;
                let result =
                    inputs
                        .into_iter()
                        .enumerate()
                        .try_for_each(|(input_index, (path, bytes))| {
                            let result =
                                self.run_aof_file_task(bytes, &mut replay_conn, Some(token))?;
                            match crate::framework::host_tasks::apply_aof_truncation(
                                &path,
                                &result,
                                allow_truncated,
                                input_index == last_input_index,
                            ) {
                                Ok(false) => Ok(()),
                                Ok(true) => {
                                    Err(crate::framework::host_tasks::AofFileTaskError::Truncated)
                                }
                                Err(error) => Err(error),
                            }
                        });
                task.finish(result, &mut self.ctx)
            }
        }
    }

    pub(crate) fn run_background_rdb_task(
        &mut self,
        task: &mut crate::framework::host_tasks::RdbLoadTask,
    ) -> Result<
        crate::foundation::io::persistence::RdbDecodedMetadata,
        crate::framework::host_tasks::RdbTaskError,
    > {
        self.with_host_dispatcher(HostTaskScope::background(), |dispatcher| {
            task.run(dispatcher)
        })
    }

    pub(crate) fn run_aof_replay_task(
        &mut self,
        task: &mut crate::framework::host_tasks::AofReplayTask,
        replay_conn: &mut Connection,
        protected_client: Option<Token>,
    ) -> Result<
        crate::foundation::io::persistence::AofLoadStats,
        crate::framework::host_tasks::AofTaskError,
    > {
        let scope = protected_client.map_or_else(HostTaskScope::background, HostTaskScope::client);
        let mut dispatcher =
            |request: HostRequest<'_>| self.dispatch_aof_host_request(scope, replay_conn, request);
        task.run(&mut dispatcher)
    }

    pub(crate) fn run_aof_file_task(
        &mut self,
        bytes: Vec<u8>,
        replay_conn: &mut Connection,
        protected_client: Option<Token>,
    ) -> Result<
        crate::framework::host_tasks::AofFileResult,
        crate::framework::host_tasks::AofFileTaskError,
    > {
        let scope = protected_client.map_or_else(HostTaskScope::background, HostTaskScope::client);
        let tail_start = if crate::foundation::io::persistence::is_rdb_magic(&bytes) {
            let strict = !self.ctx.config().persistence.rdb_version_check_relaxed;
            let mut rdb_task = crate::framework::host_tasks::RdbLoadTask::new(
                bytes.clone(),
                self.ctx.dbs().empty_like(),
                crate::foundation::io::persistence::RdbDecodePolicy::persisted(
                    strict,
                    self.ctx.config().persistence.key_load_delay,
                ),
                crate::framework::host_tasks::DatasetVisibility::Visible,
                self.ctx
                    .config()
                    .persistence
                    .loading_process_events_interval_bytes,
            );
            let metadata = self
                .with_host_dispatcher(scope, |dispatcher| rdb_task.run(dispatcher))
                .map_err(crate::framework::host_tasks::AofFileTaskError::Rdb)?;
            let consumed = metadata.consumed;
            crate::framework::host_tasks::install_rdb_preamble(
                &mut self.ctx,
                rdb_task.into_dataset(),
                &metadata,
            )?;
            consumed
        } else {
            0
        };
        let tail = bytes[tail_start..].to_vec();
        let tail_len = tail.len();
        let mut task =
            crate::framework::host_tasks::AofReplayTask::with_key_load_delay_and_panic_on_error(
                tail,
                self.ctx.config().persistence.key_load_delay,
                self.ctx
                    .config()
                    .replication
                    .should_panic_on_propagation_error(true),
            );
        let stats = self
            .run_aof_replay_task(&mut task, replay_conn, protected_client)
            .map_err(crate::framework::host_tasks::AofFileTaskError::Aof)?;
        if task.has_open_transaction() {
            replay_conn.transaction.end(TransactionEnd::Discard);
        }
        Ok(crate::framework::host_tasks::AofFileResult {
            stats,
            tail_start,
            tail_len,
        })
    }

    fn dispatch_aof_host_request(
        &mut self,
        scope: HostTaskScope,
        replay_conn: &mut Connection,
        request: HostRequest<'_>,
    ) -> Result<HostReply, HostError> {
        match request {
            HostRequest::ExecuteCommand {
                args,
                origin: HostCommandOrigin::AofReplay,
            } => self
                .apply_aof_command(replay_conn, args.clone(), scope)
                .map(|()| HostReply::Command(Value::ok()))
                .or_else(|error| Ok(HostReply::Command(Value::error(error)))),
            HostRequest::ExecuteCommand {
                args,
                origin: HostCommandOrigin::Script { run, mode },
            } => Ok(HostReply::Command(
                crate::engine::extension::lua::redis_call::execute_script_command(
                    run,
                    mode,
                    args,
                    replay_conn,
                    &mut self.ctx,
                ),
            )),
            HostRequest::CheckAcl { args } => {
                crate::engine::extension::lua::redis_call::check_acl_command(
                    replay_conn,
                    &self.ctx,
                    args,
                )
                .map(HostReply::Allowed)
                .ok_or_else(|| {
                    HostError::Runtime(
                        "Invalid command passed to server.acl_check_cmd()".to_string(),
                    )
                })
            }
            HostRequest::Yield(HostYield::ScriptCheckpoint { run }) => {
                self.dispatch_script_checkpoint(scope, run)
            }
            HostRequest::Yield(HostYield::RdbProgress {
                progress,
                visible_dataset,
            }) => {
                let _ = progress;
                if let Some(dataset) = visible_dataset {
                    std::mem::swap(self.ctx.dbs_mut(), dataset);
                    self.process_host_yield(scope);
                    std::mem::swap(self.ctx.dbs_mut(), dataset);
                } else {
                    self.process_host_yield(scope);
                }
                self.host_control_reply()
            }
            HostRequest::Yield(HostYield::AofProgress { .. })
            | HostRequest::Yield(HostYield::ReplicationReplay { .. }) => {
                self.process_host_yield(scope);
                self.host_control_reply()
            }
        }
    }

    fn apply_aof_command(
        &mut self,
        conn: &mut Connection,
        args: crate::framework::CommandArgs,
        scope: HostTaskScope,
    ) -> Result<(), String> {
        let name = args.name.to_ascii_lowercase();
        let tracking_only = conn.import_source
            && conn.slot_migration_role.is_none()
            && !name.eq_ignore_ascii_case(b"cluster")
            && crate::framework::distributed::migration::apply_tracking_import_command(
                &args,
                &self.registry,
                &mut self.ctx,
            )
            .map_err(|error| format!("{error:?}"))?;
        // C's AOF loader queues every command except EXEC while its fake
        // client is inside MULTI. In particular, DISCARD is data in the
        // transaction rather than an instruction to clear the replay queue.
        if conn.transaction.is_active() && name != b"exec" {
            if !tracking_only {
                conn.transaction.queue(self.ctx.num_dbs(), args);
            }
            return Ok(());
        }
        if name == b"multi" {
            if !conn.transaction.begin(i64::from(conn.selected_db), 0) {
                return Err("ERR MULTI calls can not be nested".to_string());
            }
            return Ok(());
        }
        if name == b"discard" {
            return Err("ERR DISCARD without MULTI".to_string());
        }
        if name == b"exec" {
            if !conn.transaction.is_active() {
                return Err("ERR EXEC without MULTI".to_string());
            }
            let queued_commands = conn.transaction.take_queued_commands();
            conn.transaction.end(TransactionEnd::ExecutionCompleted);
            let mut first_error = None;
            for queued in queued_commands {
                // C's EXEC invokes every queued command even when an earlier
                // one returns an error. Retain the first error for the AOF
                // replay policy after the transaction has drained.
                if let Err(error) = self.execute_one_aof_command(conn, &queued, scope) {
                    first_error.get_or_insert(error);
                }
            }
            self.clear_aof_replay_side_effects(conn);
            return first_error.map_or(Ok(()), Err);
        }
        if tracking_only {
            self.clear_aof_replay_side_effects(conn);
            return Ok(());
        }
        let result = self.execute_one_aof_command(conn, &args, scope);
        self.clear_aof_replay_side_effects(conn);
        result
    }

    fn execute_one_aof_command(
        &mut self,
        conn: &mut Connection,
        args: &crate::framework::CommandArgs,
        scope: HostTaskScope,
    ) -> Result<(), String> {
        let executor = self
            .registry
            .lookup(&args.name)
            .map(|spec| spec.executor)
            .ok_or_else(|| format!("unknown command '{}'", String::from_utf8_lossy(&args.name)))?;
        conn.begin_nested_command_effects();
        let result = if let Some(prepare) = executor.hosted() {
            match prepare(conn, args, &mut self.ctx) {
                Ok(task) => Ok(self.run_nested_aof_host_task(conn, task, scope)),
                Err(reply) => Err(value_error_text(reply)),
            }
        } else {
            Ok(executor.execute(conn, args, &mut self.ctx))
        };
        let mut effects = conn
            .take_command_effects(CommandEffectDrain::Nested)
            .expect("AOF command effects are drained exactly once");
        conn.finish_nested_reply_state(&mut effects);
        let value = result?;
        match value {
            Value::Error(error) => Err(String::from_utf8_lossy(&error).into_owned()),
            _ => Ok(()),
        }
    }

    fn run_nested_aof_host_task(
        &mut self,
        conn: &mut Connection,
        task: crate::framework::host_tasks::PreparedHostTask,
        scope: HostTaskScope,
    ) -> Value {
        match task {
            crate::framework::host_tasks::PreparedHostTask::Lua(mut task) => {
                self.ctx.enter_script(task.control());
                let mut dispatcher =
                    |request: HostRequest<'_>| self.dispatch_aof_host_request(scope, conn, request);
                let result = task.run(&mut dispatcher);
                self.ctx.leave_script();
                task.finish(result, conn, &mut self.ctx)
            }
            crate::framework::host_tasks::PreparedHostTask::DebugRdb(_) => {
                Value::error("ERR DEBUG is not valid while loading AOF")
            }
            crate::framework::host_tasks::PreparedHostTask::DebugAof(_) => {
                Value::error("ERR DEBUG is not valid while loading AOF")
            }
        }
    }

    fn clear_aof_replay_side_effects(&mut self, conn: &mut Connection) {
        conn.discard_command_cycle();
        conn.clear_output_queue();
        self.ctx.clear_expired_deletions();
        let _ = self.ctx.drain_lazy_expired_hash_fields();
        let _ = self.ctx.drain_pending_hexpired();
        self.ctx.script_mut().pending_internal_monitor.clear();
    }

    pub(crate) fn with_host_dispatcher<R>(
        &mut self,
        scope: HostTaskScope,
        invoke: impl FnOnce(&mut HostDispatcher<'_>) -> R,
    ) -> R {
        let mut dispatcher = |request: HostRequest<'_>| self.dispatch_host_request(scope, request);
        invoke(&mut dispatcher)
    }

    fn dispatch_host_request(
        &mut self,
        scope: HostTaskScope,
        request: HostRequest<'_>,
    ) -> Result<HostReply, HostError> {
        match request {
            HostRequest::ExecuteCommand { args, origin } => match origin {
                HostCommandOrigin::Script { run, mode } => {
                    let token = scope.protected_client.ok_or_else(|| {
                        HostError::Runtime("script host has no protected client".to_string())
                    })?;
                    if args.name.eq_ignore_ascii_case(b"client")
                        && args.args.len() == 1
                        && args.args[0].eq_ignore_ascii_case(b"list")
                    {
                        return Ok(HostReply::Command(self.client_list_value(token.0, args).0));
                    }
                    let connection = self.connection_registry.get_mut(token).ok_or_else(|| {
                        HostError::Runtime("protected script client no longer exists".to_string())
                    })?;
                    let value = crate::engine::extension::lua::redis_call::execute_script_command(
                        run,
                        mode,
                        args,
                        connection,
                        &mut self.ctx,
                    );
                    Ok(HostReply::Command(value))
                }
                HostCommandOrigin::AofReplay => Err(HostError::Runtime(
                    "AOF replay host is not installed yet".to_string(),
                )),
            },
            HostRequest::CheckAcl { args } => {
                let token = scope.protected_client.ok_or_else(|| {
                    HostError::Runtime("script host has no protected client".to_string())
                })?;
                let connection = self.connection_registry.get(token).ok_or_else(|| {
                    HostError::Runtime("protected script client no longer exists".to_string())
                })?;
                let allowed = crate::engine::extension::lua::redis_call::check_acl_command(
                    connection, &self.ctx, args,
                )
                .ok_or_else(|| {
                    HostError::Runtime(
                        "Invalid command passed to server.acl_check_cmd()".to_string(),
                    )
                })?;
                Ok(HostReply::Allowed(allowed))
            }
            HostRequest::Yield(HostYield::ScriptCheckpoint { run }) => {
                self.dispatch_script_checkpoint(scope, run)
            }
            HostRequest::Yield(HostYield::RdbProgress {
                progress,
                visible_dataset,
            }) => {
                let _ = (
                    progress.loaded_bytes,
                    progress.total_bytes,
                    progress.loaded_keys,
                );
                if let Some(dataset) = visible_dataset {
                    std::mem::swap(self.ctx.dbs_mut(), dataset);
                    self.process_host_yield(scope);
                    std::mem::swap(self.ctx.dbs_mut(), dataset);
                } else {
                    self.process_host_yield(scope);
                }
                self.host_control_reply()
            }
            HostRequest::Yield(HostYield::AofProgress {
                loaded_bytes,
                command_count,
            }) => {
                let _ = (loaded_bytes, command_count);
                self.process_host_yield(scope);
                self.host_control_reply()
            }
            HostRequest::Yield(HostYield::ReplicationReplay { processed_bytes }) => {
                let _ = processed_bytes;
                self.process_host_yield(scope);
                self.host_control_reply()
            }
        }
    }

    fn process_host_yield(&mut self, scope: HostTaskScope) {
        if let Some(operation) = scope.blocked_operation {
            self.process_events_while_blocked(operation);
        } else if self.ctx.is_host_task_loading() {
            self.process_events_while_loading_host_task(
                scope.protected_client.unwrap_or(Token(usize::MAX)),
            );
        } else {
            self.process_events_while_command_blocked(
                scope.protected_client.unwrap_or(Token(usize::MAX)),
            );
        }
        // C processEventsWhileBlocked() runs whileBlockedCron() after polling.
        // Only its loading path consumes signal-driven shutdown here; ordinary
        // busy scripts keep their existing signal semantics.
        if self.ctx.is_loading() {
            let _ = self.exit_if_shutdown_requested();
        }
    }

    fn dispatch_script_checkpoint(
        &mut self,
        scope: HostTaskScope,
        run: &mut crate::engine::extension::script::ScriptRunCtx,
    ) -> Result<HostReply, HostError> {
        run.check_timeout();
        if run.is_timed_out() {
            if !run.slow_log_emitted {
                crate::server_log!(warning, "Slow script detected");
                run.slow_log_emitted = true;
            }
            self.ctx.set_script_busy(
                run.control.kind() == crate::engine::extension::script::ScriptKind::Function,
                run.is_write_dirty(),
            );
            let monitor_events =
                std::mem::take(&mut self.ctx.script_mut().pending_internal_monitor);
            self.deliver_monitor(monitor_events);
            self.process_host_yield(scope);
        }
        self.host_control_reply()
    }

    fn host_control_reply(&self) -> Result<HostReply, HostError> {
        if self.ctx.is_shutdown_requested() {
            Ok(HostReply::Abort(HostAbort::Shutdown))
        } else {
            Ok(HostReply::Continue)
        }
    }
}

fn value_error_text(value: Value) -> String {
    match value {
        Value::Error(error) => String::from_utf8_lossy(&error).into_owned(),
        other => format!("AOF hosted command preparation returned {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::command::context::CommandConn;
    use crate::engine::command::{CommandArgs, DispatchGroup};
    use crate::engine::distributed::shutdown::{
        SHUTDOWN_FORCE, SHUTDOWN_NOSAVE, SHUTDOWN_SAFE, SHUTDOWN_SAVE,
    };
    use crate::engine::host::{yield_aof, HostAbort, HostControl};
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::{CommandExecutor, CommandSpec};
    use crate::runtime::server::worker::ServerWorkerState;
    use bytes::Bytes;

    use super::HostTaskScope;

    fn rejected_hosted_fallback(
        _conn: &mut dyn CommandConn,
        _args: &CommandArgs,
        _ctx: &mut crate::framework::context::ServerContext,
    ) -> Value {
        unreachable!("the hosted prepare path is selected")
    }

    fn reject_hosted_after_effect(
        conn: &mut dyn CommandConn,
        _args: &CommandArgs,
        _ctx: &mut crate::framework::context::ServerContext,
    ) -> Result<crate::framework::host_tasks::PreparedHostTask, Value> {
        conn.record_dirty_key((0, Bytes::from_static(b"prepare-error")));
        Err(Value::error("ERR hosted prepare failed"))
    }

    #[test]
    fn background_host_task_yields_through_the_runtime_dispatcher() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        runtime.transport.queue_test_transport_event_batch(&[]);

        let control = runtime.with_host_dispatcher(HostTaskScope::background(), |dispatcher| {
            yield_aof(dispatcher, 0, 0)
        });

        assert_eq!(control.unwrap(), HostControl::Continue);
    }

    #[test]
    fn loading_host_yield_drives_staged_shutdown() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        runtime.ctx.set_host_task_loading(true);
        runtime.ctx.shutdown_state_mut().asap = true;
        runtime.ctx.shutdown_state_mut().pending_flags =
            SHUTDOWN_SAVE | SHUTDOWN_SAFE | SHUTDOWN_FORCE;
        runtime.transport.queue_test_transport_event_batch(&[]);

        let control = runtime.with_host_dispatcher(HostTaskScope::background(), |dispatcher| {
            yield_aof(dispatcher, 0, 0)
        });

        assert_eq!(control.unwrap(), HostControl::Abort(HostAbort::Shutdown));
        assert!(runtime.ctx.is_shutdown_requested());
        assert_eq!(runtime.ctx.shutdown_state().flags, SHUTDOWN_NOSAVE);
    }

    #[test]
    fn aof_replay_runs_nested_eval_through_the_same_dispatcher() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        runtime.ctx.set_loading(true);
        runtime.ctx.config_mut().server.import_mode = true;
        let frame = crate::engine::distributed::replication::protocol::encode_command(&[
            b"EVAL",
            b"redis.call('set','aof-eval','1')",
            b"0",
        ]);
        let mut task =
            crate::framework::host_tasks::AofReplayTask::with_key_load_delay_and_panic_on_error(
                frame, 0, false,
            );
        let mut conn = crate::framework::capabilities::connection::Connection::new_fake();
        conn.import_source = true;

        let stats = runtime
            .run_aof_replay_task(&mut task, &mut conn, None)
            .expect("replay nested EVAL");

        assert_eq!(stats.cmd_count, 1);
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .expect("DB 0")
                .get_no_expire(b"aof-eval")
                .expect("script key")
                .as_bytes(),
            b"1"
        );
    }

    #[test]
    fn aof_replay_continues_after_command_error_for_panic_on_replicas() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        runtime
            .ctx
            .config_mut()
            .replication
            .propagation_error_behavior = "panic-on-replicas".to_string();
        let bytes = crate::engine::distributed::replication::protocol::encode_command(&[
            b"UNKNOWN-AOF-COMMAND",
        ]);
        let mut replay_conn = Connection::new_fake();

        let result = runtime
            .run_aof_file_task(bytes, &mut replay_conn, None)
            .expect("panic-on-replicas must not abort AOF replay");

        assert_eq!(result.stats.cmd_count, 1);
    }

    #[test]
    fn aof_replay_reports_propagation_error_for_panic_policy() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        runtime
            .ctx
            .config_mut()
            .replication
            .propagation_error_behavior = "panic".to_string();
        let bytes = crate::engine::distributed::replication::protocol::encode_command(&[
            b"UNKNOWN-AOF-COMMAND",
        ]);
        let mut replay_conn = Connection::new_fake();

        // Under the panic policy a replay error is fatal, but it must surface as
        // an error the startup layer turns into a deterministic process exit,
        // rather than a `panic!` that relies on the global panic hook.
        let result = runtime.run_aof_file_task(bytes, &mut replay_conn, None);
        assert!(matches!(
            result,
            Err(crate::framework::host_tasks::AofFileTaskError::Aof(
                crate::framework::host_tasks::AofTaskError::PropagationError { .. }
            ))
        ));
    }

    #[test]
    fn aof_exec_continues_after_a_queued_command_error() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let mut bytes =
            crate::engine::distributed::replication::protocol::encode_command(&[b"MULTI"]);
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"UNKNOWN-AOF-COMMAND",
            ]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET",
                b"after-error",
                b"applied",
            ]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[b"EXEC"]),
        );
        let mut replay_conn = Connection::new_fake();

        runtime
            .run_aof_file_task(bytes, &mut replay_conn, None)
            .expect("ignore policy should complete transaction replay");

        assert_eq!(
            runtime
                .ctx
                .db(0)
                .expect("DB 0")
                .get_no_expire(b"after-error")
                .expect("command after replay error")
                .as_bytes(),
            b"applied"
        );
    }

    #[test]
    fn aof_exec_drains_failed_hosted_prepare_before_the_next_item() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.registry.register(CommandSpec {
            name: crate::engine::command::CommandName::Static("rejecthosted"),
            arity: 1,
            flags: 0,
            dispatch_group: DispatchGroup::Default,
            first_key: 0,
            last_key: 0,
            key_step: 0,
            container: None,
            key_flags: &[],
            acl_categories: &[],
            executor: CommandExecutor::Hosted {
                fallback: rejected_hosted_fallback,
                prepare: reject_hosted_after_effect,
            },
            get_keys: None,
        });
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let mut bytes =
            crate::engine::distributed::replication::protocol::encode_command(&[b"MULTI"]);
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[b"REJECTHOSTED"]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET",
                b"after-error",
                b"applied",
            ]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[b"EXEC"]),
        );
        let mut replay_conn = Connection::new_fake();

        runtime
            .run_aof_file_task(bytes, &mut replay_conn, None)
            .expect("ignore policy continues after a hosted prepare error");

        assert_eq!(
            runtime
                .ctx
                .db(0)
                .expect("DB 0")
                .get_no_expire(b"after-error")
                .expect("following command ran")
                .as_bytes(),
            b"applied"
        );
    }

    #[test]
    fn aof_rdb_preamble_uses_the_configured_key_load_delay() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        let mut source = crate::foundation::io::store::kv_engine::DbSet::new(1);
        source.get_mut(0).expect("DB 0").set(
            b"first".to_vec(),
            crate::foundation::io::store::Robj::string("one"),
        );
        source.get_mut(0).expect("DB 0").set(
            b"second".to_vec(),
            crate::foundation::io::store::Robj::string("two"),
        );
        let bytes = crate::foundation::io::persistence::serialize_all(&source, 0);
        runtime.ctx.config_mut().persistence.key_load_delay = 20_000;
        let mut replay_conn = Connection::new_fake();
        replay_conn.import_source = true;
        let started = std::time::Instant::now();

        runtime
            .run_aof_file_task(bytes, &mut replay_conn, None)
            .expect("load RDB preamble");

        assert!(
            started.elapsed() >= std::time::Duration::from_millis(30),
            "each RDB preamble key must observe key-load-delay"
        );
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .expect("DB 0")
                .get_no_expire(b"first")
                .expect("loaded key")
                .as_bytes(),
            b"one"
        );
    }

    #[test]
    fn unfinished_aof_transaction_is_discarded_at_file_boundary() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        runtime.ctx.set_host_task_loading(true);
        runtime.ctx.config_mut().server.import_mode = true;
        runtime.transport.queue_test_transport_event_batch(&[]);
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
        let mut replay_conn = Connection::new_fake();
        replay_conn.import_source = true;

        let result = runtime
            .run_aof_file_task(bytes, &mut replay_conn, None)
            .expect("replay truncated transaction");

        assert_eq!(result.stats.valid_up_to, first.len());
        assert!(!replay_conn.transaction.is_active());
        assert!(replay_conn.transaction.queued_commands().is_empty());
    }

    #[test]
    fn discard_inside_unfinished_aof_transaction_is_not_executed() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        runtime.ctx.set_host_task_loading(true);
        runtime.ctx.config_mut().server.import_mode = true;
        runtime.transport.queue_test_transport_event_batch(&[]);
        let mut bytes =
            crate::engine::distributed::replication::protocol::encode_command(&[b"MULTI"]);
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET",
                b"queued",
                b"before-discard",
            ]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[b"DISCARD"]),
        );
        bytes.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_command(&[
                b"SET",
                b"queued",
                b"after-discard",
            ]),
        );
        let mut replay_conn = Connection::new_fake();
        replay_conn.import_source = true;

        let result = runtime
            .run_aof_file_task(bytes, &mut replay_conn, None)
            .expect("replay truncated transaction");

        assert_eq!(result.stats.valid_up_to, 0);
        assert!(runtime
            .ctx
            .db_mut(0)
            .expect("DB 0")
            .get(b"queued")
            .is_none());
        assert!(!replay_conn.transaction.is_active());
        assert!(replay_conn.transaction.queued_commands().is_empty());
    }
}
