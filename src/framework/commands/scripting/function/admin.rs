use crate::engine::command::context::CommandConn;
use crate::engine::extension::script::ScriptError;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

/// FUNCTION DELETE <library_name>
pub(super) fn function_delete(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("function|delete");
    }

    let lib_name = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Library name must be a valid string"),
    };

    match ctx.script_mut().function_registry.delete_library(lib_name) {
        Ok(true) => {
            propagate_verbatim(conn, args);
            Value::ok()
        }
        Ok(false) => Value::error("ERR Library not found".to_string()),
        Err(error) => error.into_value(),
    }
}

/// FUNCTION FLUSH [ASYNC | SYNC]
pub(super) fn function_flush(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() > 1 {
        let opt = match std::str::from_utf8(&args.args[1]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return Value::error("ERR Unknown option"),
        };
        if opt != "ASYNC" && opt != "SYNC" {
            return Value::error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option");
        }
        if args.args.len() > 2 {
            return Value::error(
                "ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.",
            );
        }
    }
    ctx.script_mut().function_registry.flush();
    propagate_verbatim(conn, args);
    Value::ok()
}

fn propagate_verbatim(conn: &mut dyn CommandConn, args: &CommandArgs) {
    let mut parts: Vec<&[u8]> = Vec::with_capacity(args.args.len() + 1);
    parts.push(b"FUNCTION");
    for a in &args.args {
        parts.push(a.as_ref());
    }
    conn.record_custom_propagation(
        crate::engine::distributed::replication::protocol::encode_command(&parts),
    );
}

/// FUNCTION KILL
pub(super) fn function_kill(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    super::super::script_cmd::request_script_kill(
        ctx,
        super::super::script_cmd::KillCommand::Function,
    )
}

/// FUNCTION RESTORE <payload> [FLUSH | APPEND | REPLACE]
pub(super) fn function_restore(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if ctx.maxmemory_state().is_over_limit() {
        return Value::error("OOM command not allowed when used memory > 'maxmemory'.");
    }

    if args.args.len() < 2 || args.args.len() > 3 {
        return Value::error(
            "ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
        );
    }

    let payload = &args.args[1];

    let mut flush_first = false;
    let mut replace_mode = false;
    if args.args.len() > 2 {
        let opt = match std::str::from_utf8(&args.args[2]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return Value::error("ERR Unknown option"),
        };
        if opt == "FLUSH" {
            flush_first = true;
        } else if opt == "REPLACE" {
            replace_mode = true;
        } else if opt != "APPEND" {
            return Value::error("ERR Unknown option");
        }
    }

    let payload_names = restore_payload_library_names(payload);
    let preexisting_names = payload_names
        .iter()
        .filter(|name| ctx.script().function_registry.get_library(name).is_some())
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    if let Err(error) = ctx.script().function_registry.begin_restore_transaction() {
        return error.into_value();
    }
    let mut replaced_libs = if flush_first {
        ctx.script_mut().function_registry.take_all_libraries()
    } else {
        Vec::new()
    };
    if replace_mode {
        for lib_name in &payload_names {
            if let Some(lib) = ctx.script_mut().function_registry.take_library(lib_name) {
                replaced_libs.push(lib);
            }
        }
    }

    let result = ctx.script_mut().function_registry.restore(payload);
    match result {
        Ok(_count) => {
            for lib in &replaced_libs {
                if let Err(error) = ctx
                    .script()
                    .function_registry
                    .finish_library_replacement(lib)
                {
                    take_partial_restore_libraries(
                        ctx,
                        &payload_names,
                        &preexisting_names,
                        flush_first,
                        replace_mode,
                    );
                    return match ctx
                        .script()
                        .function_registry
                        .rollback_restore_transaction()
                    {
                        Ok(()) => {
                            for lib in replaced_libs {
                                ctx.script_mut().function_registry.restore_library(lib);
                            }
                            function_restore_error(error)
                        }
                        Err(rollback_error) => Value::error(format!(
                            "ERR Function restore failed: {error}; rollback failed: {rollback_error}"
                        )),
                    };
                }
            }
            if let Err(commit_error) =
                ctx.script().function_registry.commit_restore_transaction()
            {
                take_partial_restore_libraries(
                    ctx,
                    &payload_names,
                    &preexisting_names,
                    flush_first,
                    replace_mode,
                );
                return match ctx
                    .script()
                    .function_registry
                    .rollback_restore_transaction()
                {
                    Ok(()) => {
                        for lib in replaced_libs {
                            ctx.script_mut().function_registry.restore_library(lib);
                        }
                        Value::error(format!(
                            "ERR Function restore transaction commit failed: {commit_error}"
                        ))
                    }
                    Err(rollback_error) => Value::error(format!(
                        "ERR Function restore transaction commit failed: {commit_error}; rollback failed: {rollback_error}"
                    )),
                };
            }
            drop(replaced_libs);
            Value::ok()
        }
        Err(e) => {
            take_partial_restore_libraries(
                ctx,
                &payload_names,
                &preexisting_names,
                flush_first,
                replace_mode,
            );
            match ctx
                .script()
                .function_registry
                .rollback_restore_transaction()
            {
                Ok(()) => {
                    for lib in replaced_libs {
                        ctx.script_mut().function_registry.restore_library(lib);
                    }
                    function_restore_error(e)
                }
                Err(rollback_error) => Value::error(format!(
                    "ERR Function restore failed: {e}; rollback failed: {rollback_error}"
                )),
            }
        }
    }
}

fn take_partial_restore_libraries(
    ctx: &mut ServerContext,
    payload_names: &[String],
    preexisting_names: &std::collections::HashSet<String>,
    flush_first: bool,
    replace_mode: bool,
) {
    for lib_name in payload_names {
        if flush_first || replace_mode || !preexisting_names.contains(lib_name) {
            ctx.script_mut().function_registry.take_library(lib_name);
        }
    }
}

fn function_restore_error(error: ScriptError) -> Value {
    match error {
        ScriptError::Compile(msg) => Value::error(format!("ERR Error compiling function: {}", msg)),
        ScriptError::Restore(_) => {
            Value::error("ERR DUMP payload version or checksum are wrong".to_string())
        }
        other => other.into_value(),
    }
}

fn restore_payload_library_names(payload: &[u8]) -> Vec<String> {
    let mut names = Vec::new();
    let mut pos = 0;
    while pos < payload.len() {
        if payload[pos] == b'\n' {
            pos += 1;
            continue;
        }
        let name_end = payload[pos..]
            .iter()
            .position(|&byte| byte == b'\n')
            .unwrap_or(payload.len() - pos);
        if name_end == 0 {
            break;
        }
        names.push(String::from_utf8_lossy(&payload[pos..pos + name_end]).into_owned());
        pos += name_end + 1;
        if pos >= payload.len() {
            break;
        }
        let len_end = payload[pos..]
            .iter()
            .position(|&byte| byte == b'\n')
            .unwrap_or(payload.len() - pos);
        if len_end == 0 {
            break;
        }
        let code_len = std::str::from_utf8(&payload[pos..pos + len_end])
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        pos = pos
            .saturating_add(len_end + 1)
            .saturating_add(code_len)
            .min(payload.len());
        if pos < payload.len() && payload[pos] == b'\n' {
            pos += 1;
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::{function_delete, function_restore};
    use crate::engine::command::CommandArgs;
    use crate::engine::extension::function::FunctionRegistry;
    use crate::engine::extension::lua::{registered_function_count, reset_lua_runtime};
    use crate::engine::extension::script::{
        CompiledFunction, ScriptEngine, ScriptError, ScriptRunCtx,
    };
    use crate::engine::host::HostDispatcher;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    struct RemoveFailEngine;

    static REMOVE_FAIL_ENGINE: RemoveFailEngine = RemoveFailEngine;

    impl ScriptEngine for RemoveFailEngine {
        fn name(&self) -> &'static str {
            "LUA"
        }

        fn eval(
            &self,
            _code: &str,
            _ctx: &mut ScriptRunCtx,
            _dispatcher: &mut HostDispatcher<'_>,
        ) -> Result<Value, ScriptError> {
            unreachable!("delete test engine does not evaluate scripts")
        }

        fn remove_functions(
            &self,
            _functions: &[CompiledFunction],
        ) -> Result<(), ScriptError> {
            Err(ScriptError::Runtime(
                "callback removal failed".to_string(),
            ))
        }

        fn call(
            &self,
            _func: &CompiledFunction,
            _ctx: &mut ScriptRunCtx,
            _keys: &[Bytes],
            _args: &[Bytes],
            _dispatcher: &mut HostDispatcher<'_>,
        ) -> Result<Value, ScriptError> {
            unreachable!("delete test engine does not call functions")
        }
    }

    fn restore_payload(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut payload = Vec::new();
        for (name, code) in entries {
            payload.extend_from_slice(name.as_bytes());
            payload.push(b'\n');
            payload.extend_from_slice(code.len().to_string().as_bytes());
            payload.push(b'\n');
            payload.extend_from_slice(code.as_bytes());
            payload.push(b'\n');
        }
        payload
    }

    fn restore_args(payload: Vec<u8>) -> CommandArgs {
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"FUNCTION"),
            Bytes::from_static(b"RESTORE"),
            Bytes::from(payload),
            Bytes::from_static(b"REPLACE"),
        ])
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
    fn function_delete_propagates_callback_removal_error() {
        let mut ctx = ServerContext::new(1);
        ctx.script_mut().function_registry = FunctionRegistry::new(&REMOVE_FAIL_ENGINE);
        ctx.script_mut()
            .function_registry
            .load_library("library", "")
            .unwrap();
        let mut conn = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FUNCTION"),
            Bytes::from_static(b"DELETE"),
            Bytes::from_static(b"library"),
        ]);

        let reply = function_delete(&mut conn, &args, &mut ctx);

        let Value::Error(error) = reply else {
            panic!("expected delete error");
        };
        assert!(String::from_utf8_lossy(&error).contains("callback removal failed"));
        assert!(ctx
            .script()
            .function_registry
            .get_library("library")
            .is_some());
    }

    #[test]
    fn function_restore_replace_removes_callbacks_absent_from_new_library() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old = "#!lua name=restore_lib\nserver.register_function('same', function() return 'old' end)\nserver.register_function('removed', function() return 1 end)";
        let new =
            "#!lua name=restore_lib\nserver.register_function('same', function() return 'new' end)";
        ctx.script_mut()
            .function_registry
            .load_library("restore_lib", old)
            .unwrap();

        let reply = function_restore(
            &mut conn,
            &restore_args(restore_payload(&[("restore_lib", new)])),
            &mut ctx,
        );

        assert_eq!(reply, crate::foundation::io::protocol::Value::ok());
        assert_eq!(registered_function_count(), 1);
        assert_eq!(
            call_function(&ctx, "same"),
            crate::foundation::io::protocol::Value::bulk(Bytes::from_static(b"new"))
        );
        reset_lua_runtime();
    }

    #[test]
    fn function_restore_replace_failure_restores_previous_callbacks() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old_one =
            "#!lua name=lib_one\nserver.register_function('one', function() return 'old-one' end)";
        let old_two =
            "#!lua name=lib_two\nserver.register_function('two', function() return 'old-two' end)";
        let new_one =
            "#!lua name=lib_one\nserver.register_function('one', function() return 'new-one' end)";
        let invalid_two = "#!lua name=lib_two\nserver.register_function('two', function() return 'new-two' end)\nerror('boom')";
        ctx.script_mut()
            .function_registry
            .load_library("lib_one", old_one)
            .unwrap();
        ctx.script_mut()
            .function_registry
            .load_library("lib_two", old_two)
            .unwrap();

        let reply = function_restore(
            &mut conn,
            &restore_args(restore_payload(&[
                ("lib_one", new_one),
                ("lib_two", invalid_two),
            ])),
            &mut ctx,
        );

        assert!(matches!(
            reply,
            crate::foundation::io::protocol::Value::Error(_)
        ));
        assert_eq!(
            call_function(&ctx, "one"),
            crate::foundation::io::protocol::Value::bulk(Bytes::from_static(b"old-one"))
        );
        assert_eq!(
            call_function(&ctx, "two"),
            crate::foundation::io::protocol::Value::bulk(Bytes::from_static(b"old-two"))
        );
        reset_lua_runtime();
    }

    #[test]
    fn function_restore_replace_failure_preserves_previous_callback_state() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old = "#!lua name=stateful\nlocal counter = 0\nserver.register_function('counter', function() counter = counter + 1 return counter end)";
        let replacement =
            "#!lua name=stateful\nserver.register_function('counter', function() return 100 end)";
        let invalid = "#!lua name=bad\nerror('boom')";
        ctx.script_mut()
            .function_registry
            .load_library("stateful", old)
            .unwrap();

        assert_eq!(
            call_function(&ctx, "counter"),
            crate::foundation::io::protocol::Value::Integer(1)
        );

        let reply = function_restore(
            &mut conn,
            &restore_args(restore_payload(&[
                ("stateful", replacement),
                ("bad", invalid),
            ])),
            &mut ctx,
        );

        let error = match reply {
            crate::foundation::io::protocol::Value::Error(error) => error,
            other => panic!("expected restore error, got {other:?}"),
        };
        let error_text = String::from_utf8_lossy(&error);
        assert!(
            error_text.contains("boom"),
            "unexpected restore error: {error_text}"
        );
        assert_eq!(
            call_function(&ctx, "counter"),
            crate::foundation::io::protocol::Value::Integer(2)
        );
        ctx.script_mut().function_registry.flush();
        reset_lua_runtime();
    }

    #[test]
    fn function_restore_flush_failure_restores_previous_libraries() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old = "#!lua name=old_lib\nserver.register_function('old', function() return 'old-value' end)";
        let new = "#!lua name=new_lib\nserver.register_function('new', function() return 'new-value' end)";
        let invalid = "#!lua name=bad_lib\nerror('boom')";
        ctx.script_mut()
            .function_registry
            .load_library("old_lib", old)
            .unwrap();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FUNCTION"),
            Bytes::from_static(b"RESTORE"),
            Bytes::from(restore_payload(&[("new_lib", new), ("bad_lib", invalid)])),
            Bytes::from_static(b"FLUSH"),
        ]);

        let reply = function_restore(&mut conn, &args, &mut ctx);

        assert!(matches!(
            reply,
            crate::foundation::io::protocol::Value::Error(_)
        ));
        assert_eq!(ctx.script().function_registry.library_count(), 1);
        assert!(ctx
            .script()
            .function_registry
            .get_library("old_lib")
            .is_some());
        assert_eq!(
            call_function(&ctx, "old"),
            crate::foundation::io::protocol::Value::bulk(Bytes::from_static(b"old-value"))
        );
        reset_lua_runtime();
    }

    #[test]
    fn function_restore_flush_failure_removes_partial_same_name_library() {
        reset_lua_runtime();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let old =
            "#!lua name=shared\nserver.register_function('old', function() return 'old-value' end)";
        let replacement = "#!lua name=shared\nserver.register_function('old', function() return 'new-value' end)\nserver.register_function('new_only', function() return 1 end)";
        let invalid = "#!lua name=bad\nerror('boom')";
        ctx.script_mut()
            .function_registry
            .load_library("shared", old)
            .unwrap();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FUNCTION"),
            Bytes::from_static(b"RESTORE"),
            Bytes::from(restore_payload(&[
                ("shared", replacement),
                ("bad", invalid),
            ])),
            Bytes::from_static(b"FLUSH"),
        ]);

        let reply = function_restore(&mut conn, &args, &mut ctx);

        assert!(matches!(
            reply,
            crate::foundation::io::protocol::Value::Error(_)
        ));
        assert!(ctx
            .script()
            .function_registry
            .find_function("new_only")
            .is_none());
        assert_eq!(registered_function_count(), 1);
        assert_eq!(
            call_function(&ctx, "old"),
            crate::foundation::io::protocol::Value::bulk(Bytes::from_static(b"old-value"))
        );
        reset_lua_runtime();
    }
}
