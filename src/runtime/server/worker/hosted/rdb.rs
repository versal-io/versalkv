use super::{HostTaskScope, ServerWorkerState};
use crate::engine::command::{CommandArgs, CMD_MAY_REPLICATE, CMD_WRITE};
use crate::engine::host::{FunctionLoadPolicy, HostError, HostReply};
use crate::foundation::io::store::kv_engine::DbSet;
use crate::framework::capabilities::connection::Connection;

impl ServerWorkerState {
    pub(super) fn dispatch_rdb_inline_command(
        &mut self,
        scope: HostTaskScope,
        db: u32,
        args: &CommandArgs,
        dataset: &mut DbSet,
        function_codes: Option<&[Vec<u8>]>,
        function_policy: FunctionLoadPolicy,
    ) -> Result<HostReply, HostError> {
        if let Some(codes) = function_codes {
            crate::framework::host_tasks::RdbLoadTask::install_function_libraries(
                &mut self.ctx,
                codes,
                function_policy,
            )
            .map_err(|()| {
                HostError::Runtime("Invalid function library in RDB stream".to_string())
            })?;
        }
        std::mem::swap(self.ctx.dbs_mut(), dataset);
        let result = self.apply_rdb_inline_command(scope, db, args);
        std::mem::swap(self.ctx.dbs_mut(), dataset);
        result.map(|()| HostReply::Command(crate::foundation::io::protocol::Value::ok()))
    }

    pub(crate) fn apply_rdb_inline_command(
        &mut self,
        scope: HostTaskScope,
        db: u32,
        args: &CommandArgs,
    ) -> Result<(), HostError> {
        let spec = self.registry.lookup(&args.name).ok_or_else(|| {
            HostError::Runtime("Unknown inline command in RDB stream".to_string())
        })?;
        let argc = args.args.len() + 1;
        let expected = spec.arity.unsigned_abs() as usize;
        if (spec.arity > 0 && argc != expected) || (spec.arity < 0 && argc < expected) {
            return Err(HostError::Runtime(
                "Wrong number of arguments for inline command".to_string(),
            ));
        }
        if args.name.eq_ignore_ascii_case(b"multi") || args.name.eq_ignore_ascii_case(b"exec") {
            return Ok(());
        }
        let flags = crate::framework::spec::effective_command_flags_for_call(
            spec.name.as_str(),
            spec.flags,
            args,
            &self.ctx,
        );
        if flags & (CMD_WRITE | CMD_MAY_REPLICATE) == 0 {
            return Err(HostError::Runtime(
                "RDB inline command is not a replicable write".to_string(),
            ));
        }
        let mut connection = Connection::new_fake();
        connection.selected_db = db;
        self.apply_aof_command(&mut connection, args.clone(), scope)
            .map_err(HostError::Runtime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::lua::{reset_lua_runtime, with_lua_runtime};
    use crate::engine::extension::script::{ScriptError, ScriptRunCtx};
    use crate::engine::host::{HostAbort, HostRequest, HostYield};
    use crate::foundation::io::persistence::{crc64, LoadError};
    use crate::foundation::io::persistence::{RdbDecodePolicy, RdbSerializeOptions};
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::capabilities::io::Token;
    use crate::framework::host_tasks::{DatasetVisibility, DebugRdbTask, RdbLoadTask};

    const STATEFUL_LIBRARY: &[u8] = b"#!lua name=libold\nlocal counter = 0\nserver.register_function('counter', function() counter = counter + 1 return counter end)";
    const REPLACEMENT_LIBRARY: &[u8] =
        b"#!lua name=libold\nserver.register_function('counter', function() return 100 end)";
    const HEADER_LIBRARY: &[u8] = b"#!lua name=libnew\nserver.register_function('write_header', function() return server.call('SET', 'from-header', 'loaded') end)";

    fn with_function_runtime(test: impl FnOnce(&mut ServerWorkerState)) {
        let guard = crate::foundation::memory::metrics::memory_stats::lock_global_for_test();
        reset_lua_runtime();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.set_command_registry_snapshot(&runtime.registry);
        runtime
            .ctx
            .script_mut()
            .function_registry
            .load_library("libold", std::str::from_utf8(STATEFUL_LIBRARY).unwrap())
            .unwrap();
        runtime
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"old-key".to_vec(), Robj::string_raw("old-value"));

        // A deliberate RED assertion must not poison the shared memory lock.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| test(&mut runtime)));
        runtime.ctx.script_mut().function_registry.flush();
        drop(runtime);
        reset_lua_runtime();
        drop(guard);
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    fn call_counter(runtime: &ServerWorkerState) -> Value {
        let registry = &runtime.ctx.script().function_registry;
        let compiled = registry
            .find_function("counter")
            .expect("counter metadata must survive")
            .clone();
        let engine = registry.function_engine(&compiled).unwrap();
        let mut run = ScriptRunCtx::new_bare_for_test(compiled.flags);
        let mut host = |_: crate::engine::host::HostRequest<'_>| {
            Err(HostError::Invariant("counter must not request host work"))
        };
        engine
            .call(&compiled, &mut run, &[], &[], &mut host)
            .expect("counter callback must survive")
    }

    fn assert_no_restore_transaction() {
        let is_empty = with_lua_runtime(false, |lua| {
            lua.named_registry_value::<mlua::Value>("__valkey_function_restore_backup_v1")
                .map(|value| matches!(value, mlua::Value::Nil))
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        })
        .unwrap();
        assert!(
            is_empty,
            "completed reload must release every restore frame"
        );
    }

    fn load_replacement_after_reload(runtime: &mut ServerWorkerState) {
        assert_no_restore_transaction();
        let args = CommandArgs::from_parts(
            [
                b"FUNCTION".as_slice(),
                b"LOAD",
                b"REPLACE",
                REPLACEMENT_LIBRARY,
            ]
            .into_iter()
            .map(bytes::Bytes::copy_from_slice)
            .collect(),
        );
        runtime
            .apply_rdb_inline_command(HostTaskScope::background(), 0, &args)
            .expect("later FUNCTION LOAD REPLACE must start and commit its own transaction");
        assert_eq!(call_counter(runtime), Value::Integer(100));
        assert_no_restore_transaction();
    }

    fn rdb_with_updates(codes: &[&[u8]], commands: &[&[&[u8]]]) -> Vec<u8> {
        let codes = codes.iter().map(|code| code.to_vec()).collect::<Vec<_>>();
        let mut bytes = crate::foundation::io::persistence::rdb::serialize::serialize_all_with_functions_and_replication_with_options(
            &DbSet::new(1),
            0,
            &codes,
            false,
            None,
            &[],
            RdbSerializeOptions { compression: false, checksum: true },
        );
        bytes.truncate(bytes.len() - 9);
        for command in commands {
            bytes.push(crate::foundation::io::persistence::types::RDB_OPCODE_UPDATE);
            bytes
                .extend(crate::engine::distributed::replication::protocol::encode_command(command));
        }
        bytes.push(crate::foundation::io::persistence::RDB_OPCODE_EOF);
        let checksum = crc64(0, &bytes);
        assert_ne!(checksum, 0, "fixture must exercise checksum validation");
        bytes.extend(checksum.to_le_bytes());
        bytes
    }

    fn corrupt_rdb_checksum(bytes: &mut [u8]) {
        let crc_start = bytes.len() - 8;
        let checksum = u64::from_le_bytes(bytes[crc_start..].try_into().unwrap());
        let invalid = if checksum == 1 { 2 } else { checksum ^ 1 };
        assert_ne!(invalid, 0, "zero would disable checksum validation");
        bytes[crc_start..].copy_from_slice(&invalid.to_le_bytes());
    }

    struct ReloadFile(std::path::PathBuf);

    impl ReloadFile {
        fn new(runtime: &mut ServerWorkerState, bytes: &[u8]) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "versal-hosted-function-policy-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir(&path).unwrap();
            let file = Self(path);
            std::fs::write(file.0.join("dump.rdb"), bytes).unwrap();
            runtime.ctx.config_mut().persistence.dir = file.0.to_string_lossy().into_owned();
            runtime.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
            runtime
                .ctx
                .config_mut()
                .persistence
                .loading_process_events_interval_bytes = 0;
            runtime.ctx.config_mut().persistence.rdbchecksum = true;
            runtime.ctx.config_mut().server.skip_checksum_validation = false;
            runtime.ctx.config_mut().server.enable_debug_command =
                crate::engine::extension::config::server::ProtectedAction::Yes;
            file
        }
    }

    impl Drop for ReloadFile {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn prepare_reload(
        runtime: &mut ServerWorkerState,
        options: &[&[u8]],
    ) -> crate::framework::host_tasks::PreparedHostTask {
        let mut parts = vec![
            bytes::Bytes::from_static(b"DEBUG"),
            bytes::Bytes::from_static(b"RELOAD"),
        ];
        parts.extend(
            options
                .iter()
                .map(|option| bytes::Bytes::copy_from_slice(option)),
        );
        let args = CommandArgs::from_parts(parts);
        crate::framework::commands::server::debug::prepare_debug_task(
            &mut Connection::new_fake(),
            &args,
            &mut runtime.ctx,
        )
        .expect("DEBUG RELOAD must prepare")
    }

    fn assert_rollback_state(runtime: &ServerWorkerState) {
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"old-key")
                .unwrap()
                .as_bytes(),
            b"old-value"
        );
        assert!(runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(b"new-key")
            .is_none());
        assert_eq!(runtime.ctx.script().function_registry.library_count(), 1);
        assert!(runtime
            .ctx
            .script()
            .function_registry
            .get_library("libold")
            .is_some());
        assert!(runtime
            .ctx
            .script()
            .function_registry
            .get_library("libnew")
            .is_none());
        assert_eq!(
            call_counter(runtime),
            Value::Integer(2),
            "rollback must preserve the original closure, not recompile its source"
        );
    }

    #[test]
    fn hosted_function_policy_noflush_plain_update_preserves_old_library_and_callback_state() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let bytes = rdb_with_updates(&[], &[&[b"SET", b"new-key", b"value"]]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"MERGE", b"NOFLUSH", b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::ok()
            );
            assert_eq!(
                runtime
                    .ctx
                    .db(0)
                    .unwrap()
                    .get_no_expire(b"new-key")
                    .unwrap()
                    .as_bytes(),
                b"value"
            );
            assert!(runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"old-key")
                .is_some());
            assert_eq!(call_counter(runtime), Value::Integer(2));
        });
    }

    #[test]
    fn hosted_function_policy_noflush_installs_header_before_fcall_without_clearing_old_library() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let bytes = rdb_with_updates(&[HEADER_LIBRARY], &[&[b"FCALL", b"write_header", b"0"]]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"MERGE", b"NOFLUSH", b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::ok()
            );
            assert_eq!(
                runtime
                    .ctx
                    .db(0)
                    .unwrap()
                    .get_no_expire(b"from-header")
                    .unwrap()
                    .as_bytes(),
                b"loaded"
            );
            assert_eq!(runtime.ctx.script().function_registry.library_count(), 2);
            assert_eq!(call_counter(runtime), Value::Integer(2));
        });
    }

    #[test]
    fn hosted_function_policy_noflush_empty_header_without_update_preserves_callback_state() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let bytes = rdb_with_updates(&[], &[]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"MERGE", b"NOFLUSH", b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::ok()
            );
            assert!(runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"old-key")
                .is_some());
            assert_eq!(call_counter(runtime), Value::Integer(2));
        });
    }

    #[test]
    fn hosted_function_policy_noflush_header_without_update_merges_at_eof() {
        with_function_runtime(|runtime| {
            let bytes = rdb_with_updates(&[REPLACEMENT_LIBRARY, HEADER_LIBRARY], &[]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"MERGE", b"NOFLUSH", b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::ok()
            );
            assert_eq!(runtime.ctx.script().function_registry.library_count(), 2);
            assert!(runtime
                .ctx
                .script()
                .function_registry
                .find_function("write_header")
                .is_some());
            assert_eq!(call_counter(runtime), Value::Integer(100));
        });
    }

    fn assert_crc_rollback(codes: &[&[u8]], commands: &[&[&[u8]]]) {
        assert_crc_rollback_with_options(codes, commands, &[b"NOSAVE"]);
    }

    fn assert_crc_rollback_with_options(codes: &[&[u8]], commands: &[&[&[u8]]], options: &[&[u8]]) {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let mut bytes = rdb_with_updates(codes, commands);
            corrupt_rdb_checksum(&mut bytes);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, options);
            let crate::framework::host_tasks::PreparedHostTask::DebugRdb(mut task) = task else {
                panic!("expected DEBUG RDB task");
            };

            task.begin_function_rollback(&runtime.ctx).unwrap();
            let result = runtime.with_host_dispatcher(HostTaskScope::background(), |dispatcher| {
                task.run(dispatcher)
            });
            assert!(
                matches!(
                    result.as_ref(),
                    Err(crate::framework::host_tasks::RdbTaskError::Load(
                        LoadError::CrcError
                    ))
                ),
                "replay must reach CRC failure after successful UPDATE execution: {result:?}"
            );
            assert_eq!(
                task.finish(result, &mut runtime.ctx),
                Value::error("ERR Error trying to load the RDB dump, check server logs.")
            );
            assert_rollback_state(runtime);
            load_replacement_after_reload(runtime);
        });
    }

    #[test]
    fn hosted_function_policy_crc_failure_without_header_restores_original_callback() {
        assert_crc_rollback(&[], &[&[b"SET", b"new-key", b"value"]]);
    }

    #[test]
    fn hosted_function_policy_crc_failure_after_header_replacement_preserves_closure_state() {
        assert_crc_rollback(
            &[REPLACEMENT_LIBRARY, HEADER_LIBRARY],
            &[&[b"SET", b"new-key", b"value"]],
        );
    }

    #[test]
    fn hosted_function_policy_crc_failure_after_function_load_replace_and_flush_restores_state() {
        assert_crc_rollback(
            &[REPLACEMENT_LIBRARY],
            &[
                &[b"FUNCTION", b"LOAD", b"REPLACE", REPLACEMENT_LIBRARY],
                &[b"FUNCTION", b"FLUSH"],
                &[b"SET", b"new-key", b"value"],
            ],
        );
    }

    #[test]
    fn hosted_function_policy_crc_failure_after_function_restore_flush_restores_state() {
        let payload = format!(
            "libold\n{}\n{}\n",
            REPLACEMENT_LIBRARY.len(),
            std::str::from_utf8(REPLACEMENT_LIBRARY).unwrap()
        );
        assert_crc_rollback(
            &[STATEFUL_LIBRARY],
            &[
                &[b"FUNCTION", b"RESTORE", payload.as_bytes(), b"FLUSH"],
                &[b"SET", b"new-key", b"value"],
            ],
        );
    }

    #[test]
    fn hosted_function_policy_crc_failure_after_function_delete_restores_state() {
        assert_crc_rollback(
            &[STATEFUL_LIBRARY],
            &[
                &[b"FUNCTION", b"DELETE", b"libold"],
                &[b"SET", b"new-key", b"value"],
            ],
        );
    }

    #[test]
    fn hosted_function_policy_noflush_crc_failure_after_flush_restores_state() {
        assert_crc_rollback_with_options(
            &[REPLACEMENT_LIBRARY, HEADER_LIBRARY],
            &[&[b"FUNCTION", b"FLUSH"]],
            &[b"MERGE", b"NOFLUSH", b"NOSAVE"],
        );
    }

    #[test]
    fn hosted_function_policy_successful_reload_commits_before_later_load_replace() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let bytes = rdb_with_updates(&[STATEFUL_LIBRARY], &[]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::ok()
            );
            assert_eq!(call_counter(runtime), Value::Integer(1));
            load_replacement_after_reload(runtime);
        });
    }

    #[test]
    fn hosted_function_policy_failed_nested_begin_cleanup_preserves_outer_frame() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            runtime
                .ctx
                .script()
                .function_registry
                .begin_restore_transaction()
                .unwrap();
            with_lua_runtime(false, |lua| {
                let frame = lua
                    .named_registry_value::<mlua::Table>("__valkey_function_restore_backup_v1")
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                frame
                    .raw_set("outer_marker", true)
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                lua.globals()
                    .raw_set("__valkey_functions_v1", true)
                    .map_err(|error| ScriptError::Runtime(error.to_string()))
            })
            .unwrap();

            assert!(runtime
                .ctx
                .script()
                .function_registry
                .begin_restore_transaction()
                .is_err());
            let has_outer_survived = with_lua_runtime(false, |lua| {
                let frame = lua
                    .named_registry_value::<mlua::Table>("__valkey_function_restore_backup_v1")
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                frame
                    .raw_get::<bool>("outer_marker")
                    .map_err(|error| ScriptError::Runtime(error.to_string()))
            })
            .unwrap();
            assert!(
                has_outer_survived,
                "failed begin cleanup must not pop the outer frame"
            );
            runtime
                .ctx
                .script()
                .function_registry
                .rollback_restore_transaction()
                .unwrap();
            assert_eq!(call_counter(runtime), Value::Integer(2));
            load_replacement_after_reload(runtime);
        });
    }

    #[test]
    fn hosted_function_policy_host_error_rolls_back_outer_callback_state() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let invalid = b"#!lua name=libold\nerror('expected compile failure')";
            let bytes = rdb_with_updates(
                &[STATEFUL_LIBRARY],
                &[&[b"FUNCTION", b"LOAD", b"REPLACE", invalid]],
            );
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"NOSAVE"]);
            let crate::framework::host_tasks::PreparedHostTask::DebugRdb(mut task) = task else {
                panic!("expected DEBUG RDB task");
            };
            task.begin_function_rollback(&runtime.ctx).unwrap();
            let result = runtime.with_host_dispatcher(HostTaskScope::background(), |dispatcher| {
                task.run(dispatcher)
            });
            assert!(matches!(
                result.as_ref(),
                Err(crate::framework::host_tasks::RdbTaskError::Host(_))
            ));
            assert_eq!(
                task.finish(result, &mut runtime.ctx),
                Value::error("ERR Error trying to load the RDB dump, check server logs.")
            );
            assert_rollback_state(runtime);
            load_replacement_after_reload(runtime);
        });
    }

    #[test]
    fn hosted_function_policy_eof_compile_error_rolls_back_outer_callback_state() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let invalid = b"#!lua name=invalid\nerror('expected compile failure')";
            let bytes = rdb_with_updates(&[REPLACEMENT_LIBRARY, invalid], &[]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::error("ERR Error trying to load the RDB dump, check server logs.")
            );
            assert_rollback_state(runtime);
            load_replacement_after_reload(runtime);
        });
    }

    #[test]
    fn hosted_function_policy_abort_after_update_restores_outer_callback_state() {
        with_function_runtime(|runtime| {
            assert_eq!(call_counter(runtime), Value::Integer(1));
            let bytes = rdb_with_updates(
                &[REPLACEMENT_LIBRARY],
                &[&[b"SET", b"new-key", b"value"], &[b"FUNCTION", b"FLUSH"]],
            );
            let _file = ReloadFile::new(runtime, &bytes);
            runtime
                .ctx
                .config_mut()
                .persistence
                .loading_process_events_interval_bytes = 1;
            let task = prepare_reload(runtime, &[b"NOSAVE"]);
            let crate::framework::host_tasks::PreparedHostTask::DebugRdb(mut task) = task else {
                panic!("expected DEBUG RDB task");
            };
            task.begin_function_rollback(&runtime.ctx).unwrap();
            let mut dispatcher = |request: HostRequest<'_>| {
                if matches!(&request, HostRequest::Yield(HostYield::RdbProgress { .. }))
                    && runtime.ctx.script().function_registry.library_count() == 0
                {
                    return Ok(HostReply::Abort(HostAbort::Shutdown));
                }
                runtime.dispatch_host_request(HostTaskScope::background(), request)
            };
            let result = task.run(&mut dispatcher);
            assert!(matches!(
                result.as_ref(),
                Err(crate::framework::host_tasks::RdbTaskError::Aborted(
                    HostAbort::Shutdown
                ))
            ));
            assert_eq!(
                task.finish(result, &mut runtime.ctx),
                Value::error("ERR Error trying to load the RDB dump, check server logs.")
            );
            assert_rollback_state(runtime);
            load_replacement_after_reload(runtime);
        });
    }

    #[test]
    fn hosted_function_policy_default_debug_replaces_empty_header_before_function_load() {
        with_function_runtime(|runtime| {
            let bytes = rdb_with_updates(&[], &[&[b"FUNCTION", b"LOAD", REPLACEMENT_LIBRARY]]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::ok()
            );
            assert!(runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"old-key")
                .is_none());
            assert_eq!(call_counter(runtime), Value::Integer(100));
        });
    }

    #[test]
    fn hosted_function_policy_default_rdb_task_replaces_empty_header_before_function_load() {
        with_function_runtime(|runtime| {
            let bytes = rdb_with_updates(&[], &[&[b"FUNCTION", b"LOAD", REPLACEMENT_LIBRARY]]);
            let mut task = RdbLoadTask::new(
                bytes,
                DbSet::new(1),
                RdbDecodePolicy::persisted(true, 0),
                DatasetVisibility::Visible,
                0,
            );

            let metadata = runtime.run_background_rdb_task(&mut task).unwrap();

            assert!(metadata.functions_applied);
            assert_eq!(call_counter(runtime), Value::Integer(100));
        });
    }

    #[test]
    fn hosted_function_policy_noflush_explicit_function_flush_is_not_undone_at_eof() {
        with_function_runtime(|runtime| {
            let bytes = rdb_with_updates(&[HEADER_LIBRARY], &[&[b"FUNCTION", b"FLUSH"]]);
            let _file = ReloadFile::new(runtime, &bytes);
            let task = prepare_reload(runtime, &[b"MERGE", b"NOFLUSH", b"NOSAVE"]);

            assert_eq!(
                runtime.run_prepared_host_task(Token(usize::MAX), task),
                Value::ok()
            );
            assert_eq!(runtime.ctx.script().function_registry.library_count(), 0);
            assert!(runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"old-key")
                .is_some());
        });
    }

    #[test]
    fn hosted_function_policy_aof_empty_header_replaces_old_library_before_update() {
        with_function_runtime(|runtime| {
            let bytes = rdb_with_updates(&[], &[&[b"FUNCTION", b"LOAD", REPLACEMENT_LIBRARY]]);
            let mut connection = Connection::new_fake();
            connection.import_source = true;

            runtime
                .run_aof_file_task(bytes, &mut connection, None)
                .unwrap();

            assert!(runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"old-key")
                .is_none());
            assert_eq!(call_counter(runtime), Value::Integer(100));
        });
    }

    #[test]
    fn hosted_function_policy_aof_function_restore_update_survives_eof_and_tail() {
        with_function_runtime(|runtime| {
            let payload = format!(
                "libold\n{}\n{}\n",
                REPLACEMENT_LIBRARY.len(),
                std::str::from_utf8(REPLACEMENT_LIBRARY).unwrap()
            );
            let mut bytes = rdb_with_updates(
                &[STATEFUL_LIBRARY],
                &[&[b"FUNCTION", b"RESTORE", payload.as_bytes(), b"FLUSH"]],
            );
            bytes.extend(
                crate::engine::distributed::replication::protocol::encode_command(&[
                    b"FCALL", b"counter", b"0",
                ]),
            );
            let mut connection = Connection::new_fake();
            connection.import_source = true;

            let result = runtime
                .run_aof_file_task(bytes, &mut connection, None)
                .unwrap();

            assert_eq!(result.stats.cmd_count, 1);
            assert_eq!(call_counter(runtime), Value::Integer(100));
        });
    }

    #[test]
    fn hosted_rdb_installs_function_header_before_update_and_does_not_restore_it_at_eof() {
        let _guard = crate::foundation::memory::metrics::memory_stats::lock_global_for_test();
        for command in [
            vec![
                b"FUNCTION".as_slice(),
                b"DELETE".as_slice(),
                b"liba".as_slice(),
            ],
            vec![b"FUNCTION".as_slice(), b"FLUSH".as_slice()],
        ] {
            crate::engine::extension::lua::reset_lua_runtime();
            let mut runtime = ServerWorkerState::new(1).unwrap();
            crate::framework::commands::register_all(&mut runtime.registry);
            runtime.ctx.set_command_registry_snapshot(&runtime.registry);
            let code =
                b"#!lua name=liba\nserver.register_function('fa', function() return 'a' end)";
            let mut bytes = crate::foundation::io::persistence::rdb::serialize::serialize_all_with_functions_and_replication_with_options(
                &DbSet::new(1), 0, &[code.to_vec()], false, None, &[],
                RdbSerializeOptions { compression: false, checksum: true },
            );
            bytes.truncate(bytes.len() - 9);
            bytes.push(crate::foundation::io::persistence::types::RDB_OPCODE_UPDATE);
            bytes.extend(
                crate::engine::distributed::replication::protocol::encode_command(&command),
            );
            bytes.push(crate::foundation::io::persistence::RDB_OPCODE_EOF);
            bytes.extend([0; 8]);
            let mut load = RdbLoadTask::new(
                bytes,
                DbSet::new(1),
                RdbDecodePolicy::persisted(true, 0),
                DatasetVisibility::Visible,
                0,
            );
            runtime.ctx.set_host_task_loading(true);
            let result = runtime.run_background_rdb_task(&mut load);
            assert!(result.as_ref().unwrap().functions_applied);
            let task = DebugRdbTask::new(load, true, None);
            assert_eq!(
                task.finish(result, &mut runtime.ctx),
                crate::foundation::io::protocol::Value::ok()
            );
            assert!(runtime
                .ctx
                .script()
                .function_registry
                .list_libraries()
                .is_empty());
            runtime.ctx.script_mut().function_registry.flush();
        }
        crate::engine::extension::lua::reset_lua_runtime();
    }
}
