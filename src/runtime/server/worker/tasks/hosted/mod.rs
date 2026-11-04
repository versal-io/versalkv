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

mod debugger;
mod module;

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
            crate::framework::host_tasks::PreparedHostTask::Module(mut task) => {
                let session = task.session_id();
                // Protect the real owner, not the boxed guest or a Slab address.
                let previous_protected = self
                    .connection_registry
                    .get_mut(token)
                    .filter(|connection| connection.session_id() == session)
                    .map(|connection| std::mem::replace(&mut connection.protected, true));
                let result = self
                    .with_host_dispatcher(HostTaskScope::client(token), |dispatcher| {
                        task.run(dispatcher)
                    });
                if let Some(connection) = self
                    .connection_registry
                    .get_mut(token)
                    .filter(|connection| connection.session_id() == session)
                {
                    let result = task.finish(result, connection, &mut self.ctx);
                    connection.protected = previous_protected.unwrap_or(false);
                    result
                } else {
                    let mut guest = task.finish_disconnected(&mut self.ctx);
                    self.commit_disconnected_module_effects(&mut guest);
                    result
                }
            }
            crate::framework::host_tasks::PreparedHostTask::DrainClusterConfig => {
                match self.drain_cluster_config_saves() {
                    Ok(()) => Value::ok(),
                    Err(error) => Value::error(format!("ERR {error}")),
                }
            }
            crate::framework::host_tasks::PreparedHostTask::Lua(mut task) => {
                if task.is_forked_debug() {
                    return self.run_forked_lua_debugger(token, task);
                }
                if task.is_synchronous_debug() {
                    return self.run_synchronous_lua_debugger(token, task);
                }
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

    fn commit_disconnected_module_effects(&mut self, guest: &mut Connection) {
        use crate::engine::extension::module::api::call::pending::ModuleCallEffects;
        use crate::engine::extension::script::PropagationOp;
        let Some(mut effects) = guest.take_command_effects(CommandEffectDrain::Nested) else {
            return;
        };
        self.dispose_data_lazyfree_objects(effects.take_lazyfree_objects());
        let swapdb = effects.take_swapdb();
        let dirty_keys = effects.take_dirty_keys();
        let mut propagation = effects.take_script_propagation();
        if propagation.is_empty() {
            propagation.extend(
                effects
                    .take_custom_propagation()
                    .into_iter()
                    .map(|frame| PropagationOp::both(guest.selected_db, frame)),
            );
        }
        // These writes already happened. Losing the reply owner cannot discard
        // their ordered propagation or keyspace effects.
        let committed_keys = self.commit_pending_module_effects(
            vec![ModuleCallEffects {
                dirty_delta: effects.take_dirty_delta(),
                dirty_keys: if swapdb.is_none() {
                    dirty_keys.clone()
                } else {
                    Vec::new()
                },
                propagation,
                notifications: effects.take_notifications(),
                shard_notifications: effects.take_shard_notifications(),
                cluster_notifications: effects.take_cluster_notifications(),
                cluster_shard_notifications: effects.take_cluster_shard_notifications(),
            }],
            None,
            None,
            None,
        );
        if let Some((db1, db2)) = swapdb {
            self.apply_swapdb_watch_state(db1, db2);
            self.wakeup_blocked_on_keys(&dirty_keys, None, None);
        } else if !committed_keys.is_empty() {
            self.wakeup_blocked_on_keys(&committed_keys, None, None);
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
                )
                .with_zset_limits(
                    self.ctx.config().encoding.zset_max_ziplist_entries as usize,
                    self.ctx.config().encoding.zset_max_ziplist_value as usize,
                ),
                crate::framework::host_tasks::DatasetVisibility::Visible,
                self.ctx
                    .config()
                    .persistence
                    .loading_process_events_interval_bytes,
            )
            .with_skip_codec_checksums(
                !self.ctx.config().persistence.rdbchecksum
                    || self.ctx.config().server.skip_checksum_validation,
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
            HostRequest::Yield(HostYield::Module {
                flags,
                busy_reply,
                elapsed_ms,
            }) => self.dispatch_module_yield(scope, flags, busy_reply, elapsed_ms),
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
        let _suppression = crate::foundation::memory::key_access::KeyAccessSuppression::enter();
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
            crate::framework::host_tasks::PreparedHostTask::Module(mut task) => {
                let mut dispatcher =
                    |request: HostRequest<'_>| self.dispatch_aof_host_request(scope, conn, request);
                let result = task.run(&mut dispatcher);
                task.finish(result, conn, &mut self.ctx)
            }
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
            crate::framework::host_tasks::PreparedHostTask::DebugAof(_)
            | crate::framework::host_tasks::PreparedHostTask::DrainClusterConfig => {
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
            HostRequest::Yield(HostYield::Module {
                flags,
                busy_reply,
                elapsed_ms,
            }) => self.dispatch_module_yield(scope, flags, busy_reply, elapsed_ms),
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

    fn dispatch_module_yield(
        &mut self,
        scope: HostTaskScope,
        flags: i32,
        busy_reply: Option<&str>,
        elapsed_ms: u64,
    ) -> Result<HostReply, HostError> {
        let threshold_ms = self.ctx.config().server.lua_time_limit;
        let registry = self.ctx.module_registry_mut();
        registry.busy_reply = Some(busy_reply.unwrap_or("Running a module command").to_owned());
        registry.yield_clients = flags & (1 << 1) != 0 && elapsed_ms >= threshold_ms;
        self.process_host_yield(scope);
        self.host_control_reply()
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

    thread_local! {
        static DISCONNECT_SOCKET: std::cell::RefCell<Option<(
            std::net::TcpStream,
            crate::foundation::io::transport::ThreadIoHandle,
        )>> = const { std::cell::RefCell::new(None) };
        static DISCONNECT_ERROR: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
    }

    #[test]
    fn lua_debug_task_without_connected_client_clears_debug_state() {
        use crate::framework::capabilities::io::Token;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let token = Token(42);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        runtime.connection_registry.insert(connection);
        let session = runtime.connection_registry[token].session_id();
        let debug = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SCRIPT"),
            Bytes::from_static(b"DEBUG"),
            Bytes::from_static(b"YES"),
        ]);
        assert_eq!(
            crate::framework::commands::scripting::script_cmd::script_cmd(
                &mut runtime.connection_registry[token],
                &debug,
                &mut runtime.ctx,
            ),
            Value::ok()
        );
        assert!(runtime.ctx.script().debug_modes.contains_key(&session));
        let eval = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"return 'hello'"),
            Bytes::from_static(b"0"),
        ]);
        let task = crate::framework::host_tasks::prepare_lua_task(
            &mut runtime.connection_registry[token],
            &eval,
            &mut runtime.ctx,
        )
        .unwrap();
        assert_eq!(runtime.ctx.script().debug_session, Some(session));

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(
            result,
            Value::error("ERR debugger requires a connected client")
        );
        assert!(!runtime.ctx.script().debug_modes.contains_key(&session));
        assert_eq!(runtime.ctx.script().debug_session, None);
    }

    #[test]
    fn synchronous_lua_debugger_runs_against_live_dataset_and_returns_final_reply() {
        use crate::framework::capabilities::connection::ConnectionState;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let debug = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SCRIPT"),
            Bytes::from_static(b"DEBUG"),
            Bytes::from_static(b"SYNC"),
        ]);
        assert_eq!(
            crate::framework::commands::scripting::script_cmd::script_cmd(
                &mut runtime.connection_registry[token],
                &debug,
                &mut runtime.ctx,
            ),
            Value::ok()
        );
        let eval = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"redis.call('SET','sync-debug-key','value'); return 'hello'"),
            Bytes::from_static(b"0"),
        ]);
        let task = crate::framework::host_tasks::prepare_lua_task(
            &mut runtime.connection_registry[token],
            &eval,
            &mut runtime.ctx,
        )
        .unwrap();
        peer.write_all(b"*1\r\n$8\r\ncontinue\r\n").unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::bulk(Bytes::from_static(b"hello")));
        assert_eq!(
            runtime.connection_registry[token].state,
            ConnectionState::CloseAfterReply
        );
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"sync-debug-key")
                .map(|value| value.as_bytes()),
            Some(b"value".as_slice())
        );
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(
                        b"-> 1 redis.call('SET','sync-debug-key','value'); return 'hello'"
                    ),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    fn prepare_synchronous_lua_debug_task(
        runtime: &mut ServerWorkerState,
        token: crate::framework::capabilities::io::Token,
        source: &'static [u8],
    ) -> crate::framework::host_tasks::PreparedHostTask {
        prepare_synchronous_lua_debug_task_with_args(
            runtime,
            token,
            Bytes::from_static(source),
            &[],
            &[],
        )
    }

    fn prepare_synchronous_lua_debug_task_with_owned_source(
        runtime: &mut ServerWorkerState,
        token: crate::framework::capabilities::io::Token,
        source: impl Into<Bytes>,
    ) -> crate::framework::host_tasks::PreparedHostTask {
        prepare_synchronous_lua_debug_task_with_args(runtime, token, source.into(), &[], &[])
    }

    fn prepare_synchronous_lua_debug_task_with_args(
        runtime: &mut ServerWorkerState,
        token: crate::framework::capabilities::io::Token,
        source: Bytes,
        keys: &[&'static [u8]],
        args: &[&'static [u8]],
    ) -> crate::framework::host_tasks::PreparedHostTask {
        let debug = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SCRIPT"),
            Bytes::from_static(b"DEBUG"),
            Bytes::from_static(b"SYNC"),
        ]);
        assert_eq!(
            crate::framework::commands::scripting::script_cmd::script_cmd(
                &mut runtime.connection_registry[token],
                &debug,
                &mut runtime.ctx,
            ),
            Value::ok()
        );
        let mut eval_parts = vec![
            Bytes::from_static(b"EVAL"),
            source,
            Bytes::from(keys.len().to_string()),
        ];
        eval_parts.extend(keys.iter().map(|key| Bytes::from_static(key)));
        eval_parts.extend(args.iter().map(|arg| Bytes::from_static(arg)));
        let eval = CommandArgs::from_parts(eval_parts);
        crate::framework::host_tasks::prepare_lua_task(
            &mut runtime.connection_registry[token],
            &eval,
            &mut runtime.ctx,
        )
        .unwrap()
    }

    fn read_debugger_output(peer: &mut std::net::TcpStream) -> Vec<u8> {
        use std::io::Read;
        use std::time::Duration;

        peer.set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let mut output = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            match peer.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => output.extend_from_slice(&buffer[..count]),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("failed to read debugger output: {error}"),
            }
        }
        output
    }

    fn read_debugger_output_until_end_session(peer: &mut std::net::TcpStream) -> Vec<u8> {
        use std::io::Read;
        use std::time::Duration;

        const END_SESSION_FRAME: &[u8] = b"*1\r\n+<endsession>\r\n";

        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut output = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            match peer.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    output.extend_from_slice(&buffer[..count]);
                    if output.ends_with(END_SESSION_FRAME) {
                        return output;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    panic!("timed out before debugger end-session frame")
                }
                Err(error) => panic!("failed to read debugger output: {error}"),
            }
        }
        panic!(
            "debugger output ended before end-session frame after {} bytes",
            output.len()
        )
    }

    #[derive(Debug, PartialEq, Eq)]
    struct DebuggerFrame<'a>(Vec<DebuggerFrameElement<'a>>);

    #[derive(Debug, PartialEq, Eq)]
    enum DebuggerFrameElement<'a> {
        SimpleString(&'a [u8]),
        Error(&'a [u8]),
    }

    fn parse_debugger_frames(mut input: &[u8]) -> Vec<DebuggerFrame<'_>> {
        let mut frames = Vec::new();
        while !input.is_empty() {
            frames.push(parse_debugger_frame(&mut input));
        }
        frames
    }

    fn parse_debugger_frame<'a>(input: &mut &'a [u8]) -> DebuggerFrame<'a> {
        let header = take_debugger_resp_line(input);
        let length = header
            .strip_prefix(b"*")
            .expect("debugger output must contain only top-level RESP arrays");
        assert!(
            !length.is_empty() && length.iter().all(u8::is_ascii_digit),
            "invalid debugger RESP array length"
        );
        let length = std::str::from_utf8(length)
            .expect("debugger RESP array length must be ASCII")
            .parse::<usize>()
            .expect("debugger RESP array length must fit in usize");
        let mut elements = Vec::with_capacity(length);
        for _ in 0..length {
            let element = take_debugger_resp_line(input);
            let (kind, value) = element
                .split_first()
                .expect("debugger RESP array element must have a type prefix");
            elements.push(match kind {
                b'+' => DebuggerFrameElement::SimpleString(value),
                b'-' => DebuggerFrameElement::Error(value),
                _ => panic!("unsupported debugger RESP array element type"),
            });
        }
        DebuggerFrame(elements)
    }

    fn take_debugger_resp_line<'a>(input: &mut &'a [u8]) -> &'a [u8] {
        let line_end = input
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("incomplete debugger RESP frame");
        let (line, remaining) = input.split_at(line_end);
        assert!(
            !line.contains(&b'\r') && !line.contains(&b'\n'),
            "debugger RESP line contains an invalid line break"
        );
        *input = &remaining[2..];
        line
    }

    #[test]
    fn synchronous_lua_debugger_eval_reports_fragment_result() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'eval-complete'");
        peer.write_all(
            b"*4\r\n$4\r\neval\r\n$1\r\n1\r\n$1\r\n+\r\n$1\r\n2\r\n\
              *1\r\n$8\r\ncontinue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::bulk(Bytes::from_static(b"eval-complete")));
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'eval-complete'"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<retval> 3")]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_sanitizes_line_breaks_in_logs() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'log-complete'");
        peer.write_all(
            b"*2\r\n$4\r\neval\r\n$24\r\nstring.char(65,13,10,66)\r\n\
              *1\r\n$8\r\ncontinue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::bulk(Bytes::from_static(b"log-complete")));
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'log-complete'"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<retval> A  B")]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_rejects_oversized_argument_and_ends_session() {
        use std::io::Write;

        const MAX_DEBUGGER_ARGUMENT_LEN: usize = 1024 * 1024;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task(
            &mut runtime,
            token,
            b"redis.call('SET','oversized-debugger-argument','executed'); return 'executed'",
        );
        let command = format!("*2\r\n$4\r\neval\r\n${}\r\n", MAX_DEBUGGER_ARGUMENT_LEN + 1);
        peer.write_all(command.as_bytes()).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::Null);
        assert!(runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"oversized-debugger-argument")
            .is_none());
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(
                        b"-> 1 redis.call('SET','oversized-debugger-argument','executed'); return 'executed'"
                    ),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> unauthenticated bulk length",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_maxlen_controls_log_truncation() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'maxlen-complete'");
        peer.write_all(
            b"*1\r\n$6\r\nmaxlen\r\n\
              *2\r\n$4\r\neval\r\n$19\r\nstring.rep('x',300)\r\n\
              *2\r\n$6\r\nmaxlen\r\n$2\r\n32\r\n\
              *2\r\n$4\r\neval\r\n$19\r\nstring.rep('x',300)\r\n\
              *2\r\n$6\r\nmaxlen\r\n$1\r\n0\r\n\
              *2\r\n$4\r\neval\r\n$19\r\nstring.rep('x',300)\r\n\
              *1\r\n$5\r\nabort\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        let output = read_debugger_output(&mut peer);
        let full_log = format!("<retval> {}", "x".repeat(300));
        let default_truncated = format!("{} ...", &full_log[..256]);
        let minimum_truncated = format!("{} ...", &full_log[..60]);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'maxlen-complete'"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"Current maxlen is 256",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    default_truncated.as_bytes(),
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<hint> The above reply was trimmed. Use 'maxlen 0' to disable trimming.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<value> replies are truncated at 60 bytes.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    minimum_truncated.as_bytes(),
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<value> configured reply truncation is disabled.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    full_log.as_bytes(),
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_maxlen_zero_reports_hard_cap_truncation() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        const DEBUGGER_HARD_LOG_LIMIT: usize = 1024 * 1024;
        const HARD_LIMIT_SUFFIX: &[u8] = b" ...";
        const HARD_LIMIT_HINT: &[u8] = b"<hint> Reply reached debugger hard safety limit.";

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'hard-cap-session'");
        let mut commands = encode_command(&[b"maxlen", b"0"]);
        commands.extend(encode_command(&[b"eval", b"string.rep('x', 1048576+1024)"]));
        commands.extend(encode_command(&[b"eval", b"string.rep('y', 1048576+1024)"]));
        commands.extend(encode_command(&[b"abort"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        let reader = std::thread::spawn(move || read_debugger_output_until_end_session(&mut peer));

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        let output = reader.join().expect("debugger output reader must finish");
        let frames = parse_debugger_frames(&output);
        assert_eq!(
            frames.len(),
            6,
            "unexpected debugger frame count: {}",
            frames.len()
        );
        assert_eq!(
            frames[0],
            DebuggerFrame(vec![
                DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                DebuggerFrameElement::SimpleString(b"-> 1 return 'hard-cap-session'"),
            ])
        );
        assert_eq!(
            frames[1],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                b"<value> configured reply truncation is disabled.",
            )])
        );
        let [DebuggerFrameElement::SimpleString(log)] = frames[2].0.as_slice() else {
            panic!("expected one simple-string debugger hard-cap log frame");
        };
        assert_eq!(log.len(), DEBUGGER_HARD_LOG_LIMIT + HARD_LIMIT_SUFFIX.len());
        assert!(log.starts_with(b"<retval> "));
        assert_eq!(&log[DEBUGGER_HARD_LOG_LIMIT..], HARD_LIMIT_SUFFIX);
        assert_eq!(
            frames[3],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(HARD_LIMIT_HINT,)])
        );
        assert_eq!(
            frames[5],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")])
        );
        let [DebuggerFrameElement::SimpleString(second_log)] = frames[4].0.as_slice() else {
            panic!("expected a second simple-string debugger hard-cap log frame");
        };
        assert_eq!(
            second_log.len(),
            DEBUGGER_HARD_LOG_LIMIT + HARD_LIMIT_SUFFIX.len()
        );
        assert!(second_log.starts_with(b"<retval> "));
        assert_eq!(&second_log[DEBUGGER_HARD_LOG_LIMIT..], HARD_LIMIT_SUFFIX);
    }

    #[test]
    fn synchronous_lua_debugger_bounds_source_output() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        const DEBUGGER_SOURCE_OUTPUT_LIMIT: usize = 1024 * 1024;
        const SOURCE_TRIM_SUFFIX: &[u8] = b" ...";
        const SOURCE_TRUNCATION_MARKER: &[u8] = b"<source output truncated>";

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let mut source = b"--".to_vec();
        source.resize(DEBUGGER_SOURCE_OUTPUT_LIMIT + 128, b'x');
        let source_line_len = source.len();
        source.extend_from_slice(b"\nreturn 'done'");
        let task =
            prepare_synchronous_lua_debug_task_with_owned_source(&mut runtime, token, source);
        let mut commands = encode_command(&[b"list"]);
        commands.extend(encode_command(&[b"abort"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        let reader = std::thread::spawn(move || read_debugger_output_until_end_session(&mut peer));

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        let output = reader.join().expect("debugger output reader must finish");
        let frames = parse_debugger_frames(&output);
        assert_eq!(
            frames.len(),
            3,
            "unexpected debugger frame count: {}",
            frames.len()
        );
        let [DebuggerFrameElement::SimpleString(stopped), DebuggerFrameElement::SimpleString(initial_source)] =
            frames[0].0.as_slice()
        else {
            panic!("expected the stopped frame and its bounded source line");
        };
        assert_eq!(
            *stopped,
            b"* Stopped at 1, stop reason = step over".as_slice()
        );
        let assert_bounded_source =
            |line: &[u8], expected_prefix: &[u8], complete_line_len: usize| {
                assert!(
                    line.len() <= DEBUGGER_SOURCE_OUTPUT_LIMIT + SOURCE_TRIM_SUFFIX.len(),
                    "bounded source element was {} bytes",
                    line.len()
                );
                assert!(
                    line.starts_with(expected_prefix),
                    "bounded source element did not retain its prefix"
                );
                assert!(
                    line.ends_with(SOURCE_TRIM_SUFFIX),
                    "bounded source element did not retain its trim suffix"
                );
                assert!(
                    line.len() < complete_line_len,
                    "bounded source element retained the complete source line"
                );
            };
        assert_bounded_source(initial_source, b"-> 1 --", b"-> 1 ".len() + source_line_len);
        assert_eq!(
            frames[1],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                SOURCE_TRUNCATION_MARKER
            )])
        );
        assert_eq!(
            frames[2],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")])
        );
    }

    #[test]
    fn synchronous_lua_debugger_list_has_total_output_budget() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        const DEBUGGER_SOURCE_OUTPUT_BUDGET: usize = 1024 * 1024;
        const MAX_LISTED_SOURCE_LINES: usize = 4096;
        const SOURCE_COMMENT_LINES: usize = 5000;
        const SOURCE_COMMENT_PAYLOAD_LEN: usize = 240;
        const SOURCE_TRUNCATION_MARKER: &[u8] = b"<source output truncated>";

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let comment_payload = vec![b'x'; SOURCE_COMMENT_PAYLOAD_LEN];
        let mut source =
            Vec::with_capacity(SOURCE_COMMENT_LINES * (SOURCE_COMMENT_PAYLOAD_LEN + 32));
        for line_index in 0..SOURCE_COMMENT_LINES {
            write!(&mut source, "-- source line {line_index:04} ").unwrap();
            source.extend_from_slice(&comment_payload);
            source.push(b'\n');
        }
        source.extend_from_slice(b"return 'done'");
        assert!(source.len() > DEBUGGER_SOURCE_OUTPUT_BUDGET);
        let complete_source_line_count = SOURCE_COMMENT_LINES + 1;
        let task =
            prepare_synchronous_lua_debug_task_with_owned_source(&mut runtime, token, source);
        let mut commands = encode_command(&[b"list"]);
        commands.extend(encode_command(&[b"abort"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        let reader = std::thread::spawn(move || read_debugger_output_until_end_session(&mut peer));

        let result = runtime.run_prepared_host_task(token, task);
        let output = reader.join().expect("debugger output reader must finish");

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        let frames = parse_debugger_frames(&output);
        assert_eq!(
            frames.len(),
            3,
            "unexpected debugger frame count: {}",
            frames.len()
        );
        let [DebuggerFrameElement::SimpleString(stopped), DebuggerFrameElement::SimpleString(initial_source)] =
            frames[0].0.as_slice()
        else {
            panic!("expected a bounded two-element stopped frame");
        };
        assert_eq!(
            *stopped,
            b"* Stopped at 1, stop reason = step over".as_slice()
        );
        assert!(initial_source.starts_with(b"-> 1 -- source line 0000 "));
        assert!(
            initial_source.len() <= DEBUGGER_SOURCE_OUTPUT_BUDGET + SOURCE_TRUNCATION_MARKER.len(),
            "stopped source output was {} bytes",
            initial_source.len()
        );

        let listed_source = &frames[1].0;
        assert!(
            listed_source.len() <= MAX_LISTED_SOURCE_LINES + 1,
            "list frame contained {} elements",
            listed_source.len()
        );
        assert!(
            listed_source.len() < complete_source_line_count,
            "list frame enumerated the complete source"
        );
        let listed_source_bytes = listed_source
            .iter()
            .map(|element| match element {
                DebuggerFrameElement::SimpleString(line) => line.len(),
                DebuggerFrameElement::Error(error) => {
                    panic!(
                        "list frame contained an error element: {}",
                        String::from_utf8_lossy(error)
                    )
                }
            })
            .sum::<usize>();
        assert!(
            listed_source_bytes <= DEBUGGER_SOURCE_OUTPUT_BUDGET + SOURCE_TRUNCATION_MARKER.len(),
            "list frame contained {listed_source_bytes} source bytes"
        );
        assert_eq!(
            listed_source.last(),
            Some(&DebuggerFrameElement::SimpleString(
                SOURCE_TRUNCATION_MARKER
            ))
        );
        assert_eq!(
            frames[2],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")])
        );
    }

    #[test]
    fn synchronous_lua_debugger_maxlen_reports_upstream_argument_errors() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'maxlen-errors'");
        let mut commands = encode_command(&[b"max"]);
        commands.extend(encode_command(&[b"maxlen", b"-1"]));
        commands.extend(encode_command(&[b"maxlen", b"1", b"2"]));
        commands.extend(encode_command(&[b"abort"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'maxlen-errors'"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"Current maxlen is 256",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Invalid maxlen value.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Wrong number of arguments for 'maxlen'.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_maxlen_truncates_binary_logs_by_bytes() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'binary-maxlen'");
        let mut commands = encode_command(&[b"maxlen", b"60"]);
        commands.extend(encode_command(&[
            b"eval",
            b"string.rep(string.char(195,169),40)",
        ]));
        commands.extend(encode_command(&[b"abort"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        let output = read_debugger_output(&mut peer);
        let mut truncated = b"<retval> ".to_vec();
        truncated.extend([0xc3, 0xa9].into_iter().cycle().take(51));
        truncated.extend_from_slice(b" ...");
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'binary-maxlen'"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<value> replies are truncated at 60 bytes.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(&truncated)]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_preserves_modified_key_globals_on_continue() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task_with_args(
            &mut runtime,
            token,
            Bytes::from_static(b"return KEYS[1]"),
            &[b"original"],
            &[],
        );
        peer.write_all(
            b"*2\r\n$4\r\neval\r\n$19\r\nKEYS[1] = 'changed'\r\n\
              *1\r\n$8\r\ncontinue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::bulk(Bytes::from_static(b"changed")));
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return KEYS[1]"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<retval> nil")]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_rejects_extra_fixed_command_arguments() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task(
            &mut runtime,
            token,
            b"redis.call('SET','continue-extra-key','executed'); return 'executed'",
        );
        let mut commands = encode_command(&[b"continue", b"extra"]);
        commands.extend(encode_command(&[b"abort", b"extra"]));
        commands.extend(encode_command(&[b"help", b"extra"]));
        commands.extend(encode_command(&[b"list", b"extra"]));
        commands.extend(encode_command(&[b"print", b"KEYS", b"extra"]));
        commands.extend(encode_command(&[b"abort"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        assert!(runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"continue-extra-key")
            .is_none());
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(
                        b"-> 1 redis.call('SET','continue-extra-key','executed'); return 'executed'"
                    ),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Wrong number of arguments for 'continue'.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Wrong number of arguments for 'abort'.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Wrong number of arguments for 'help'.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Wrong number of arguments for 'list'.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Wrong number of arguments for 'print'.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_rejects_extra_restart_arguments() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'restart-arity'");
        let mut commands = encode_command(&[b"SCRIPT", b"DEBUG", b"SYNC", b"unknown"]);
        commands.extend(encode_command(&[
            b"SCRIPT", b"DEBUG", b"SYNC", b"lua", b"extra",
        ]));
        commands.extend(encode_command(&[b"abort"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        let output = read_debugger_output_until_end_session(&mut peer);
        assert!(
            !output
                .windows(b"+OK\r\n".len())
                .any(|window| window == b"+OK\r\n"),
            "unexpected bare OK debugger reply in {} bytes",
            output.len()
        );
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'restart-arity'"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> No scripting engine found with name 'unknown' to enable debug",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                    b"<error> Wrong number of arguments for 'script debug sync'.",
                )]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_parse_error_ends_session() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'unreachable'");
        peer.write_all(b"*1\r\n+invalid\r\n").unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::Null);
        let output = read_debugger_output(&mut peer);
        let frames = parse_debugger_frames(&output);
        assert_eq!(frames.len(), 3);
        assert_eq!(
            frames[0],
            DebuggerFrame(vec![
                DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                DebuggerFrameElement::SimpleString(b"-> 1 return 'unreachable'"),
            ])
        );
        assert_eq!(
            frames[1],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(
                b"<error> Protocol error: expected '$', got '+'",
            )])
        );
        assert_eq!(
            frames[2],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")])
        );
    }

    #[test]
    fn synchronous_lua_debugger_initialization_failure_ends_without_stopping() {
        use crate::engine::extension::lua::{reset_lua_runtime, with_lua_runtime};
        use crate::engine::extension::script::ScriptError;

        reset_lua_runtime();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'unreachable'");
        with_lua_runtime(false, |lua| {
            lua.set_memory_limit(lua.used_memory().saturating_add(1))
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            Ok(())
        })
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        with_lua_runtime(false, |lua| {
            lua.set_memory_limit(0)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            Ok(())
        })
        .unwrap();
        reset_lua_runtime();
        assert!(matches!(result, Value::Error(_)));
        let output = read_debugger_output(&mut peer);
        let frames = parse_debugger_frames(&output);
        assert_eq!(frames.len(), 2);
        assert!(
            matches!(
                &frames[0].0[..],
                [DebuggerFrameElement::SimpleString(message)]
                    if message.starts_with(b"<error> ")
            ),
            "{frames:?}"
        );
        assert_eq!(
            frames[1],
            DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")])
        );
    }

    #[test]
    fn synchronous_lua_debugger_failed_restart_keeps_old_task() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let source = b"return 'old-task-complete'";
        let task = prepare_synchronous_lua_debug_task(&mut runtime, token, source);
        let mut commands = encode_command(&[b"SCRIPT", b"DEBUG", b"SYNC"]);
        commands.extend(encode_command(&[b"EVAL", b"local =", b"0"]));
        commands.extend(encode_command(&[b"continue"]));
        peer.write_all(&commands).unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(
            result,
            Value::bulk(Bytes::from_static(b"old-task-complete"))
        );
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            output
                .windows(b"* Stopped at 1, stop reason = step over".len())
                .filter(|part| *part == b"* Stopped at 1, stop reason = step over")
                .count(),
            1,
            "unexpected stop-frame count in {} bytes",
            output.len()
        );
        assert!(
            output.windows(b"-ERR ".len()).any(|part| part == b"-ERR "),
            "missing debugger error frame in {} bytes",
            output.len()
        );
        assert!(
            output.ends_with(b"*1\r\n+<endsession>\r\n"),
            "missing end-session frame in {} bytes",
            output.len()
        );
    }

    #[test]
    fn synchronous_lua_debugger_redis_executes_command_against_live_dataset() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'redis-complete'");
        peer.write_all(
            b"*4\r\n$5\r\nredis\r\n$3\r\nSET\r\n$20\r\ndebugger-command-key\r\n\
              $5\r\nvalue\r\n*1\r\n$8\r\ncontinue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::bulk(Bytes::from_static(b"redis-complete")));
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"debugger-command-key")
                .map(|value| value.as_bytes()),
            Some(b"value".as_slice())
        );
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'redis-complete'"),
                ]),
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"<command> SET debugger-command-key value"),
                    DebuggerFrameElement::SimpleString(b"<reply> OK"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_eval_falls_back_to_statement() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'statement-complete'");
        peer.write_all(
            b"*2\r\n$4\r\neval\r\n$25\r\nlocal statement_value = 1\r\n\
              *1\r\n$8\r\ncontinue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(
            result,
            Value::bulk(Bytes::from_static(b"statement-complete"))
        );
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'statement-complete'"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<retval> nil")]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_command_aliases_use_real_dispatch() {
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task =
            prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'alias-complete'");
        peer.write_all(
            b"*2\r\n$6\r\nserver\r\n$4\r\nPING\r\n\
              *3\r\n$6\r\nvalkey\r\n$4\r\nECHO\r\n$11\r\nalias-value\r\n\
              *1\r\n$8\r\ncontinue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::bulk(Bytes::from_static(b"alias-complete")));
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'alias-complete'"),
                ]),
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"<command> PING"),
                    DebuggerFrameElement::SimpleString(b"<reply> PONG"),
                ]),
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"<command> ECHO alias-value"),
                    DebuggerFrameElement::SimpleString(b"<reply> alias-value"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn synchronous_lua_debugger_abort_preserves_command_effects() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use crate::engine::extension::script::PropagationOp;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'unreachable'");
        peer.write_all(
            b"*4\r\n$5\r\nredis\r\n$3\r\nSET\r\n$18\r\ndebugger-abort-key\r\n\
              $5\r\nvalue\r\n*1\r\n$5\r\nabort\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::error("ERR script aborted for user request"));
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"debugger-abort-key")
                .map(|value| value.as_bytes()),
            Some(b"value".as_slice())
        );
        assert_eq!(
            runtime.connection_registry[token].script_propagation(),
            &[PropagationOp::both(
                0,
                encode_command(&[b"SET", b"debugger-abort-key", b"value"]),
            )]
        );
        let output = read_debugger_output(&mut peer);
        assert_eq!(
            parse_debugger_frames(&output),
            vec![
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"* Stopped at 1, stop reason = step over"),
                    DebuggerFrameElement::SimpleString(b"-> 1 return 'unreachable'"),
                ]),
                DebuggerFrame(vec![
                    DebuggerFrameElement::SimpleString(b"<command> SET debugger-abort-key value"),
                    DebuggerFrameElement::SimpleString(b"<reply> OK"),
                ]),
                DebuggerFrame(vec![DebuggerFrameElement::SimpleString(b"<endsession>")]),
            ]
        );
    }

    #[test]
    fn lua_debugger_synchronous_restart_preserves_command_effects() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use crate::engine::extension::script::PropagationOp;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'replaced'");
        peer.write_all(
            b"*4\r\n$5\r\nredis\r\n$3\r\nSET\r\n$20\r\ndebugger-restart-key\r\n\
              $5\r\nvalue\r\n\
              *3\r\n$6\r\nSCRIPT\r\n$5\r\nDEBUG\r\n$4\r\nSYNC\r\n\
              *3\r\n$4\r\nEVAL\r\n$25\r\nreturn 'restart-complete'\r\n$1\r\n0\r\n\
              *1\r\n$8\r\ncontinue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::bulk(Bytes::from_static(b"restart-complete")));
        assert_eq!(
            runtime.connection_registry[token].script_propagation(),
            &[PropagationOp::both(
                0,
                encode_command(&[b"SET", b"debugger-restart-key", b"value"]),
            )]
        );
    }

    #[test]
    fn lua_debugger_synchronous_disconnect_preserves_command_effects() {
        use crate::engine::distributed::replication::protocol::encode_command;
        use crate::engine::extension::script::PropagationOp;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let task = prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'unreachable'");
        peer.write_all(
            b"*4\r\n$5\r\nredis\r\n$3\r\nSET\r\n$23\r\ndebugger-disconnect-key\r\n\
              $5\r\nvalue\r\n",
        )
        .unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();

        let result = runtime.run_prepared_host_task(token, task);

        assert_eq!(result, Value::Null);
        assert_eq!(
            runtime.connection_registry[token].script_propagation(),
            &[PropagationOp::both(
                0,
                encode_command(&[b"SET", b"debugger-disconnect-key", b"value"]),
            )]
        );
    }

    extern "C" fn yield_disconnect_command(
        _ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
        _argv: *mut *mut crate::engine::extension::module::ValkeyModuleString,
        _argc: libc::c_int,
    ) -> libc::c_int {
        if let Some((peer, mut stream)) = DISCONNECT_SOCKET.with(|slot| slot.borrow_mut().take()) {
            use std::io::Read;
            let _ = socket2::SockRef::from(&peer).set_linger(Some(std::time::Duration::ZERO));
            drop(peer);
            let end = std::time::Instant::now() + std::time::Duration::from_secs(1);
            while std::time::Instant::now() < end {
                match stream.read(&mut [0_u8; 1]) {
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(error) => {
                        DISCONNECT_ERROR.with(|slot| slot.set(error.raw_os_error()));
                        break;
                    }
                    Ok(_) => break,
                }
            }
        }
        crate::engine::extension::module::capability::yield_module_to_host(
            1 << 1,
            Some("finite disconnect test"),
        );
        crate::engine::extension::module::VALKEYMODULE_OK
    }

    extern "C" fn write_disconnect_command(
        ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
        argv: *mut *mut crate::engine::extension::module::ValkeyModuleString,
        argc: libc::c_int,
    ) -> libc::c_int {
        use crate::engine::extension::module::api::call::pending::ModuleCallEffects;
        use crate::engine::extension::module::capability::with_active_runtime;
        use crate::engine::extension::script::PropagationOp;
        let key = Bytes::from_static(b"disconnect:effect");
        with_active_runtime(|runtime| {
            runtime.db_mut(0).unwrap().set(
                key.to_vec(),
                crate::foundation::io::store::Robj::string(Bytes::from_static(b"written")),
            );
            runtime.apply_module_call_effects(ModuleCallEffects {
                dirty_delta: Some(1),
                dirty_keys: vec![(0, key)],
                propagation: vec![PropagationOp::both(0, disconnect_effect_frame())],
                ..ModuleCallEffects::default()
            });
            let client_id = runtime.module_client_id();
            runtime.set_module_client_name(client_id, Bytes::from_static(b"guest-name"));
        });
        yield_disconnect_command(ctx, argv, argc)
    }

    fn disconnect_effect_frame() -> Vec<u8> {
        crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET",
            b"disconnect:effect",
            b"written",
        ])
    }

    fn prepare_disconnect_task(
        runtime: &mut ServerWorkerState,
        token: crate::framework::capabilities::io::Token,
    ) -> crate::framework::host_tasks::PreparedHostTask {
        register_disconnect_module(runtime);
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"disconnect-test.run")]);
        crate::framework::host_tasks::prepare_module_task(
            &mut runtime.connection_registry[token],
            &args,
            &mut runtime.ctx,
        )
        .unwrap()
    }

    fn register_disconnect_module(runtime: &mut ServerWorkerState) {
        use crate::engine::extension::module::registry::ModuleRef;
        runtime
            .ctx
            .module_registry_mut()
            .register(ModuleRef::new(
                "disconnect-test",
                std::ptr::null_mut(),
                1,
                true,
            ))
            .unwrap();
        runtime
            .ctx
            .module_registry_mut()
            .register_command(
                "disconnect-test",
                "disconnect-test.run",
                Some(yield_disconnect_command),
            )
            .unwrap();
    }

    fn register_disconnect_runtime_command(runtime: &mut ServerWorkerState) {
        register_disconnect_module(runtime);
        runtime.registry.register(CommandSpec {
            name: crate::engine::command::CommandName::Shared(std::sync::Arc::from(
                "disconnect-test.run",
            )),
            arity: 1,
            flags: crate::engine::command::CMD_MODULE,
            dispatch_group: DispatchGroup::Default,
            first_key: 0,
            last_key: 0,
            key_step: 0,
            container: None,
            key_flags: &[],
            acl_categories: &[],
            executor: CommandExecutor::Hosted {
                fallback: crate::framework::capabilities::module::module_cmd_dispatcher,
                prepare: crate::framework::host_tasks::prepare_module_task,
            },
            get_keys: None,
        });
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
    }

    fn disconnect_owner(
        runtime: &mut ServerWorkerState,
    ) -> (
        crate::framework::capabilities::io::Token,
        std::net::TcpStream,
    ) {
        use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = runtime.transport.connect_tcp(addr).unwrap();
        let (stream, _) = listener.accept().unwrap();
        runtime.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: addr,
        });
        (peer.token, stream)
    }

    #[test]
    fn synchronous_lua_debugger_protects_owner_during_nested_module_disconnect() {
        use crate::engine::extension::lua::runtime::reset_lua_runtime;
        use crate::framework::capabilities::connection::ConnectionState;
        use std::io::Write;

        reset_lua_runtime();
        DISCONNECT_SOCKET.with(|slot| {
            drop(slot.borrow_mut().take());
        });
        DISCONNECT_ERROR.with(|slot| slot.set(None));

        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        register_disconnect_runtime_command(&mut runtime);
        let (token, mut peer) = disconnect_owner(&mut runtime);
        let session = runtime.connection_registry[token].session_id();
        let task = prepare_synchronous_lua_debug_task(&mut runtime, token, b"return 'unreachable'");
        peer.write_all(b"*2\r\n$5\r\nredis\r\n$19\r\ndisconnect-test.run\r\n")
            .unwrap();
        let stream = runtime.transport.duplicate_stream(token).unwrap();
        DISCONNECT_SOCKET.with(|slot| *slot.borrow_mut() = Some((peer, stream)));
        runtime
            .transport
            .queue_test_transport_event_batch(&[(token, false, false, false, true)]);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.run_prepared_host_task(token, task)
        }));

        let disconnect_error = DISCONNECT_ERROR.with(|slot| slot.replace(None));
        DISCONNECT_SOCKET.with(|slot| {
            drop(slot.borrow_mut().take());
        });
        let debug_mode_was_cleared = !runtime.ctx.script().debug_modes.contains_key(&session);
        let debug_session_was_cleared = runtime.ctx.script().debug_session.is_none();
        runtime.ctx.script_mut().debug_modes.remove(&session);
        runtime.ctx.script_mut().debug_session = None;
        reset_lua_runtime();

        assert_eq!(disconnect_error, Some(libc::ECONNRESET));
        assert!(
            outcome.is_ok(),
            "nested disconnect must not panic after removing or deferring the debugger owner"
        );
        let connection = runtime
            .connection_registry
            .get(token)
            .expect("synchronous debugger must not remove its owner during nested disconnect");
        assert_eq!(connection.session_id(), session);
        assert!(!connection.protected);
        assert!(connection.is_async_close_pending);
        assert!(
            matches!(
                connection.state,
                ConnectionState::Closing | ConnectionState::CloseAfterReply
            ),
            "deferred debugger owner must be closing"
        );
        assert!(debug_mode_was_cleared);
        assert!(debug_session_was_cleared);
    }

    #[test]
    fn module_hosted_terminal_error_retains_owner_until_finish() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let (token, peer) = disconnect_owner(&mut runtime);
        let stream = runtime.transport.duplicate_stream(token).unwrap();
        DISCONNECT_SOCKET.with(|slot| *slot.borrow_mut() = Some((peer, stream)));
        DISCONNECT_ERROR.with(|slot| slot.set(None));
        let session = runtime.connection_registry[token].session_id();
        let task = prepare_disconnect_task(&mut runtime, token);
        runtime
            .transport
            .queue_test_transport_event_batch(&[(token, false, false, false, true)]);
        runtime.run_prepared_host_task(token, task);
        assert_eq!(
            DISCONNECT_ERROR.with(|slot| slot.get()),
            Some(libc::ECONNRESET)
        );
        assert_eq!(runtime.ctx.module_registry().busy_reply, None);
        assert!(!runtime.ctx.module_registry().yield_clients);
        let connection = runtime
            .connection_registry
            .get(token)
            .expect("terminal error must defer removal until the command completes");
        assert_eq!(connection.session_id(), session);
        assert!(!connection.protected);
        assert!(connection.is_async_close_pending);
    }

    #[test]
    fn module_hosted_missing_session_commits_occurred_effects_once() {
        use crate::engine::command::context::WatchedKey;
        use crate::framework::capabilities::io::Token;
        for (replace_session, is_backlog_active) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let mut runtime = ServerWorkerState::new(1).unwrap();
            if is_backlog_active {
                runtime.ctx.replication_mut().backlog.activate();
            }
            runtime.ctx.config_mut().persistence.aof_enabled = true;
            runtime.ctx.aof_mut().fsynced_repl_offset = Some(0);
            runtime.ctx.set_repl_seldb(0);
            runtime
                .ctx
                .persistence_mut()
                .begin_aof_rewrite_child(123, 1000)
                .unwrap();
            runtime.ctx.persistence_mut().set_aof_selected_db(0);
            let mut owner = Connection::new_fake();
            owner.set_token_for_test(Token(42));
            let token = runtime.connection_registry.register_for_test(owner);
            let task = prepare_disconnect_task(&mut runtime, token);
            drop(task);
            runtime
                .ctx
                .module_registry_mut()
                .register_command(
                    "disconnect-test",
                    "disconnect-test.write",
                    Some(write_disconnect_command),
                )
                .unwrap();
            let args = CommandArgs::from_parts(vec![Bytes::from_static(b"disconnect-test.write")]);
            let task = crate::framework::host_tasks::prepare_module_task(
                &mut runtime.connection_registry[token],
                &args,
                &mut runtime.ctx,
            )
            .unwrap();
            runtime.connection_registry.remove(token).unwrap();
            if replace_session {
                let mut replacement = Connection::new_fake();
                replacement.set_token_for_test(token);
                replacement.set_client_id_for_test(900);
                replacement.set_name(Some(Bytes::from_static(b"replacement")));
                replacement.protected = true;
                runtime.connection_registry.register_for_test(replacement);
            }
            let mut watcher = Connection::new_fake();
            watcher.set_token_for_test(Token(43));
            watcher.set_resp_version(3);
            watcher.set_tracking(true);
            watcher.transaction.watch_many(vec![WatchedKey::new(
                0,
                Bytes::from_static(b"disconnect:effect"),
                false,
            )]);
            let watcher_token = runtime.connection_registry.register_for_test(watcher);
            runtime
                .ctx
                .tracking_mut()
                .remember_key(watcher_token.0, &Bytes::from_static(b"disconnect:effect"));
            let before = runtime.ctx.replication().backlog.next_stream_cursor();
            let dirty = runtime.ctx.persistence().dirty();
            let mut expected_aof =
                crate::engine::distributed::replication::protocol::encode_command(&[
                    b"SELECT", b"0",
                ]);
            expected_aof.extend(disconnect_effect_frame());
            runtime.transport.queue_test_transport_event_batch(&[]);
            runtime.run_prepared_host_task(token, task);
            assert_eq!(runtime.ctx.module_registry().busy_reply, None);
            assert_eq!(runtime.ctx.persistence().dirty(), dirty + 1);
            let expected_position = if is_backlog_active {
                disconnect_effect_frame().len() as i64
            } else {
                1
            };
            assert_eq!(
                runtime.ctx.replication().primary_repl_offset().get(),
                expected_position
            );
            assert_eq!(runtime.ctx.aof().fsynced_repl_offset, Some(0));
            if is_backlog_active {
                assert_eq!(
                    runtime.ctx.replication().backlog.read_from(before),
                    Some(disconnect_effect_frame())
                );
            }
            assert_eq!(runtime.ctx.aof().rewrite.buffer, expected_aof);
            assert!(runtime.connection_registry[watcher_token]
                .transaction
                .is_watch_dirty());
            assert_eq!(runtime.ctx.tracking().total_keys(), 0);
            let output = runtime.connection_registry[watcher_token].output_buffer_bytes();
            assert!(output > 0);
            if replace_session {
                assert_eq!(
                    runtime.connection_registry[token].name.as_deref(),
                    Some(b"replacement".as_slice())
                );
                assert!(runtime.connection_registry[token].protected);
                assert!(runtime.connection_registry[token]
                    .script_propagation()
                    .is_empty());
            }
            runtime.apply_pending_module_effects();
            assert_eq!(runtime.ctx.persistence().dirty(), dirty + 1);
            assert_eq!(runtime.ctx.aof().rewrite.buffer, expected_aof);
            assert_eq!(
                runtime.connection_registry[watcher_token].output_buffer_bytes(),
                output
            );
            runtime.ctx.persistence_mut().abort_child();
        }
    }

    #[test]
    fn module_hosted_restores_existing_owner_protection() {
        use crate::framework::capabilities::io::Token;
        for was_protected in [false, true] {
            let mut runtime = ServerWorkerState::new(1).unwrap();
            let mut owner = Connection::new_fake();
            owner.set_token_for_test(Token(43));
            owner.protected = was_protected;
            let token = runtime.connection_registry.register_for_test(owner);
            runtime.ctx.module_registry_mut().busy_reply = Some("outer invocation".to_owned());
            runtime.ctx.module_registry_mut().yield_clients = true;
            let task = prepare_disconnect_task(&mut runtime, token);
            runtime.transport.queue_test_transport_event_batch(&[]);
            runtime.run_prepared_host_task(token, task);
            assert_eq!(runtime.connection_registry[token].protected, was_protected);
            assert_eq!(
                runtime.ctx.module_registry().busy_reply.as_deref(),
                Some("outer invocation")
            );
            assert!(runtime.ctx.module_registry().yield_clients);
        }
    }

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
