use crate::engine::extension::function::parse_engine_name;
use crate::engine::extension::script::ScriptError;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

use super::is_valid_lib_name;

/// FUNCTION LOAD [REPLACE] <library_code>
pub(super) fn function_load(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if ctx.maxmemory_state().is_over_limit() {
        return Value::error("OOM command not allowed when used memory > 'maxmemory'.");
    }

    let argc = args.args.len();
    if argc < 2 {
        return CommandError::wrong_arity("function|load");
    }

    let replace = std::str::from_utf8(&args.args[1])
        .map(|s| s.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false);

    let code_idx = if replace { 2 } else { 1 };
    if code_idx >= argc {
        return CommandError::wrong_arity("function|load");
    }

    if replace && argc > 3 {
        return Value::error("ERR Unknown option given".to_string());
    }
    if !replace && argc > 2 {
        return Value::error("ERR Unknown option given".to_string());
    }

    let code = match std::str::from_utf8(&args.args[code_idx]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Library code must be a valid string"),
    };

    let lib_name = match crate::engine::extension::lua::shebang::extract_shebang_name(code) {
        Ok(name) => name,
        Err(e) => return Value::error(e),
    };

    if !is_valid_lib_name(&lib_name) {
        return Value::error(format!(
            "ERR Library names can only contain letters, numbers, or underscores(_), got '{}'",
            lib_name
        ));
    }

    let engine_name = parse_engine_name(code);
    if ctx
        .script_mut()
        .function_registry
        .engine_mgr()
        .get(&engine_name)
        .is_none()
    {
        return Value::error(format!("ERR Engine '{}' not found", engine_name));
    }

    if !replace
        && ctx
            .script_mut()
            .function_registry
            .get_library(&lib_name)
            .is_some()
    {
        return Value::error(format!(
            "ERR Library '{}' already exists. Use REPLACE to overwrite",
            lib_name
        ));
    }

    if !replace {
        // Fast path: a non-replacing LOAD never removes an existing library or
        // function. Engine compilation uses an isolated staging table and
        // `commit_compile` merges it atomically, so the restore transaction
        // (whose begin step clones the whole active Lua function table) is
        // unnecessary here. Skipping it keeps bulk FUNCTION LOAD at O(1) per
        // command instead of re-copying every previously loaded function.
        if let Err(error) = ctx
            .script_mut()
            .function_registry
            .load_library(&lib_name, code)
        {
            // Remove metadata inserted before the engine commit failed, then
            // drop any staged Lua callbacks. The active table is untouched.
            ctx.script_mut().function_registry.take_library(&lib_name);
            ctx.script_mut().function_registry.abort_compile();
            return function_load_error(error);
        }
    } else {
        if let Err(error) = ctx.script().function_registry.begin_restore_transaction() {
            return error.into_value();
        }

        let old_lib = ctx
            .script_mut()
            .function_registry
            .take_library(&lib_name);

        if let Err(error) = ctx
            .script_mut()
            .function_registry
            .load_library(&lib_name, code)
        {
            return rollback_function_load(ctx, &lib_name, old_lib, error);
        }

        if let Some(old) = &old_lib {
            if let Err(error) = ctx
                .script()
                .function_registry
                .finish_library_replacement(old)
            {
                return rollback_function_load(ctx, &lib_name, old_lib, error);
            }
        }

        if let Err(error) = ctx.script().function_registry.commit_restore_transaction() {
            return rollback_function_load(ctx, &lib_name, old_lib, error);
        }

        drop(old_lib);
    }

    let mut parts: Vec<&[u8]> = Vec::with_capacity(args.args.len() + 1);
    parts.push(b"FUNCTION");
    for a in &args.args {
        parts.push(a.as_ref());
    }
    conn.record_custom_propagation(
        crate::engine::distributed::replication::protocol::encode_command(&parts),
    );
    Value::Bulk(bytes::Bytes::copy_from_slice(lib_name.as_bytes()))
}

fn rollback_function_load(
    ctx: &mut ServerContext,
    lib_name: &str,
    old_lib: Option<crate::engine::extension::function::FunctionLib>,
    error: ScriptError,
) -> Value {
    ctx.script_mut().function_registry.take_library(lib_name);
    match ctx
        .script()
        .function_registry
        .rollback_restore_transaction()
    {
        Ok(()) => {
            if let Some(old) = old_lib {
                ctx.script_mut().function_registry.restore_library(old);
            }
            function_load_error(error)
        }
        Err(rollback_error) => Value::error(format!(
            "ERR Function load failed: {error}; rollback failed: {rollback_error}"
        )),
    }
}

fn function_load_error(error: ScriptError) -> Value {
    match error {
        ScriptError::Compile(msg) if msg.starts_with("Unknown engine: ") => {
            let engine = msg.trim_start_matches("Unknown engine: ");
            Value::error(format!("ERR Engine '{}' not found", engine))
        }
        ScriptError::Compile(msg) => {
            Value::error(format!("ERR Error compiling function: {}", msg))
        }
        other => other.into_value(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::function_load;
    use crate::engine::command::CommandArgs;
    use crate::engine::extension::function::FunctionRegistry;
    use crate::engine::extension::lua::{
        registered_function_count, reset_lua_runtime,
    };
    use crate::engine::extension::script::{
        CompiledFunction, ScriptEngine, ScriptError, ScriptRunCtx,
    };
    use crate::engine::host::HostDispatcher;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    struct CommitFailEngine {
        should_fail: AtomicBool,
    }

    static COMMIT_FAIL_ENGINE: CommitFailEngine = CommitFailEngine {
        should_fail: AtomicBool::new(false),
    };

    impl ScriptEngine for CommitFailEngine {
        fn name(&self) -> &'static str {
            "LUA"
        }

        fn eval(
            &self,
            _code: &str,
            _ctx: &mut ScriptRunCtx,
            _dispatcher: &mut HostDispatcher<'_>,
        ) -> Result<Value, ScriptError> {
            unreachable!("load test engine does not evaluate scripts")
        }

        fn commit_compile(&self, _code: &str, _flags: u64) -> Result<(), ScriptError> {
            if self.should_fail.load(Ordering::Relaxed) {
                Err(ScriptError::Runtime("commit failure".to_string()))
            } else {
                Ok(())
            }
        }

        fn call(
            &self,
            _func: &CompiledFunction,
            _ctx: &mut ScriptRunCtx,
            _keys: &[Bytes],
            _args: &[Bytes],
            _dispatcher: &mut HostDispatcher<'_>,
        ) -> Result<Value, ScriptError> {
            unreachable!("load test engine does not call functions")
        }
    }

    fn load_args(parts: &[&'static [u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            std::iter::once(Bytes::from_static(b"FUNCTION"))
                .chain(parts.iter().copied().map(Bytes::from_static))
                .collect(),
        )
    }

    fn no_command_host(
        request: crate::engine::host::HostRequest<'_>,
    ) -> Result<crate::engine::host::HostReply, crate::engine::host::HostError> {
        match request {
            crate::engine::host::HostRequest::Yield(_) => {
                Ok(crate::engine::host::HostReply::Continue)
            }
            _ => Err(crate::engine::host::HostError::Runtime(
                "unexpected host request".to_string(),
            )),
        }
    }

    fn call_function(ctx: &ServerContext, name: &str) -> crate::foundation::io::protocol::Value {
        let compiled = ctx
            .script()
            .function_registry
            .find_function(name)
            .unwrap()
            .clone();
        let engine = ctx
            .script()
            .function_registry
            .function_engine(&compiled)
            .unwrap();
        let mut run = ScriptRunCtx::new_bare_for_test(compiled.flags);
        let mut host = no_command_host;
        engine
            .call(&compiled, &mut run, &[], &[], &mut host)
            .unwrap()
    }

    #[test]
    fn function_load_replace_removes_callbacks_absent_from_new_library() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old = b"#!lua name=replace_lib\nserver.register_function('same', function() return 'old' end)\nserver.register_function('removed', function() return 1 end)";
        let new = b"#!lua name=replace_lib\nserver.register_function('same', function() return 'new' end)";

        let load_reply = function_load(&mut conn, &load_args(&[b"LOAD", old]), &mut ctx);
        assert!(
            matches!(
                load_reply,
                crate::foundation::io::protocol::Value::Bulk(_)
            ),
            "unexpected load reply: {load_reply:?}"
        );
        assert_eq!(registered_function_count(), 2);

        assert!(matches!(
            function_load(
                &mut conn,
                &load_args(&[b"LOAD", b"REPLACE", new]),
                &mut ctx,
            ),
            crate::foundation::io::protocol::Value::Bulk(_)
        ));

        assert_eq!(registered_function_count(), 1);
        assert!(ctx
            .script()
            .function_registry
            .find_function("same")
            .is_some());
        assert!(ctx
            .script()
            .function_registry
            .find_function("removed")
            .is_none());
        reset_lua_runtime();
    }

    #[test]
    fn function_load_replace_removes_callbacks_with_stale_name_casing() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old = b"#!lua name=case_lib\nserver.register_function('Foo', function() return 'old' end)";
        let new = b"#!lua name=case_lib\nserver.register_function('foo', function() return 'new' end)";

        let _ = function_load(&mut conn, &load_args(&[b"LOAD", old]), &mut ctx);
        let _ = function_load(
            &mut conn,
            &load_args(&[b"LOAD", b"REPLACE", new]),
            &mut ctx,
        );

        assert_eq!(registered_function_count(), 1);
        reset_lua_runtime();
    }

    #[test]
    fn function_load_replace_compile_failure_preserves_stateful_callback() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old = b"#!lua name=stateful\nlocal counter = 0\nserver.register_function('counter', function() counter = counter + 1 return counter end)";
        let invalid = b"#!lua name=stateful\nserver.register_function('counter', function() return 100 end)\nerror('boom')";

        let load_reply = function_load(&mut conn, &load_args(&[b"LOAD", old]), &mut ctx);
        assert!(
            matches!(
                load_reply,
                crate::foundation::io::protocol::Value::Bulk(_)
            ),
            "unexpected load reply: {load_reply:?}"
        );
        assert_eq!(
            call_function(&ctx, "counter"),
            crate::foundation::io::protocol::Value::Integer(1)
        );

        let reply = function_load(
            &mut conn,
            &load_args(&[b"LOAD", b"REPLACE", invalid]),
            &mut ctx,
        );

        assert!(matches!(
            reply,
            crate::foundation::io::protocol::Value::Error(_)
        ));
        assert_eq!(
            call_function(&ctx, "counter"),
            crate::foundation::io::protocol::Value::Integer(2)
        );
        ctx.script_mut().function_registry.flush();
        reset_lua_runtime();
    }

    #[test]
    fn function_load_replace_commit_failure_restores_old_metadata() {
        COMMIT_FAIL_ENGINE
            .should_fail
            .store(false, Ordering::Relaxed);
        let mut ctx = ServerContext::new(1);
        ctx.script_mut().function_registry = FunctionRegistry::new(&COMMIT_FAIL_ENGINE);
        let old = "#!lua name=replace_lib\nreturn 'old'";
        ctx.script_mut()
            .function_registry
            .load_library("replace_lib", old)
            .unwrap();
        COMMIT_FAIL_ENGINE
            .should_fail
            .store(true, Ordering::Relaxed);
        let mut conn = Connection::new_fake();
        let replacement = b"#!lua name=replace_lib\nreturn 'new'";

        let reply = function_load(
            &mut conn,
            &load_args(&[b"LOAD", b"REPLACE", replacement]),
            &mut ctx,
        );

        assert!(matches!(reply, Value::Error(_)));
        let restored = ctx
            .script()
            .function_registry
            .get_library("replace_lib")
            .unwrap();
        assert_eq!(restored.code, old);
        assert_eq!(ctx.script().function_registry.library_count(), 1);
        assert_eq!(ctx.script().function_registry.function_count(), 1);
        COMMIT_FAIL_ENGINE
            .should_fail
            .store(false, Ordering::Relaxed);
    }
}
