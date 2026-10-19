use super::*;
use crate::engine::extension::lua::shebang::{
    extract_function_name, is_blocked_subcmd, is_noscript_subcmd,
};
use crate::engine::extension::script::{
    PropagationOp, PropagationTargets, ScriptControlHandle, ScriptError, ScriptKind,
    SCRIPT_EVAL_MODE,
};
use crate::foundation::io::protocol::Value;
use bytes::Bytes;
use mlua::prelude::LuaValue;

fn echo_host(
    request: crate::engine::host::HostRequest<'_>,
) -> Result<crate::engine::host::HostReply, crate::engine::host::HostError> {
    match request {
        crate::engine::host::HostRequest::ExecuteCommand { args, .. } => {
            Ok(crate::engine::host::HostReply::Command(
                args.args
                    .first()
                    .cloned()
                    .map(Value::Bulk)
                    .unwrap_or(Value::Null),
            ))
        }
        crate::engine::host::HostRequest::CheckAcl { .. } => {
            Ok(crate::engine::host::HostReply::Allowed(true))
        }
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
    }
}

fn test_ctx() -> ScriptRunCtx {
    ScriptRunCtx {
        flags: SCRIPT_EVAL_MODE,
        timeout_ms: 0,
        start_time: None,
        control: ScriptControlHandle::new(ScriptKind::Eval),
        slow_log_emitted: false,
        propagate_dirty: false,
        needs_propagation: true,
        deferred_error_replies: Vec::new(),
        argv_scratch: Vec::new(),
        lua_argument_cache: Vec::new(),
        propagation_ops: Vec::new(),
        dirty_keys_seen: Default::default(),
        propagation_targets: PropagationTargets::Both,
        has_enough_replicas_to_write: true,
        lua_enable_insecure_api: false,
        skip_propagate: false,
        resp: 2,
        func_name: String::new(),
        caller_args: Vec::new(),
        script_slot: None,
        env: None,
    }
}

fn command_args(parts: &[&'static [u8]]) -> crate::engine::command::CommandArgs {
    crate::engine::command::CommandArgs::from_parts(
        parts.iter().map(|part| Bytes::from_static(part)).collect(),
    )
}

fn eval_without_commands(
    engine: &LuaEngine,
    code: &str,
    ctx: &mut ScriptRunCtx,
) -> Result<Value, ScriptError> {
    let mut host = echo_host;
    engine.eval_with_host(code, ctx, &mut host)
}

#[test]
fn test_lua_engine_eval_simple_expression() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(&engine, "return 42", &mut ctx).unwrap();
    assert_eq!(result, Value::Integer(42));
}

#[test]
fn lua_redis_call_uses_the_scoped_host_dispatcher() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = echo_host;

    let result = engine
        .eval_with_host("return redis.call('echo', 'scoped')", &mut ctx, &mut host)
        .unwrap();

    assert_eq!(result, Value::Bulk(Bytes::from_static(b"scoped")));
}

#[test]
fn lua_redis_call_uses_direct_scoped_binding() {
    reset_lua_runtime();
    let mut ctx = test_ctx();
    let mut host = echo_host;

    let result = runtime::with_lua_runtime(false, |lua_runtime| {
        let lua = &lua_runtime.lua;
        runtime::with_lua_host(lua, &mut ctx, &mut host, || {
            lua.unset_named_registry_value(runtime::HOST_CALLBACK_REGISTRY_KEY)?;
            lua.load("return redis.call('echo', 'direct')")
                .eval::<LuaValue>()
        })
        .map_err(|error| ScriptError::Runtime(error.to_string()))
    })
    .unwrap();

    let LuaValue::String(result) = result else {
        panic!("expected string reply");
    };
    assert_eq!(result.as_bytes().as_ref(), b"direct");
}

#[test]
fn direct_scoped_binding_keeps_redis_table_readonly() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = echo_host;

    let error = engine
        .eval_with_host(
            "redis.call = function() return 1 end; return 1",
            &mut ctx,
            &mut host,
        )
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("Attempt to modify a readonly table"));
}

#[test]
fn rawset_cannot_override_runtime_redis_bindings() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = echo_host;

    let error = engine
        .eval_with_host(
            "rawset(redis, 'call', function() return 'forged' end); return 1",
            &mut ctx,
            &mut host,
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Attempt to modify a readonly table"));

    let error = engine
        .eval_with_host(
            "local saved = rawget(_G, '__vky_saved'); rawset(saved, 'redis', {}); return 1",
            &mut ctx,
            &mut host,
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Attempt to modify a readonly table"));

    let result = engine
        .eval_with_host("return redis.call('echo', 'scoped')", &mut ctx, &mut host)
        .unwrap();
    assert_eq!(result, Value::Bulk(Bytes::from_static(b"scoped")));
}

#[test]
fn scoped_pcall_returns_plain_error_table_without_raise_marker() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = |request: crate::engine::host::HostRequest<'_>| match request {
        crate::engine::host::HostRequest::ExecuteCommand { .. } => Ok(
            crate::engine::host::HostReply::Command(Value::error("ERR simulated failure")),
        ),
        crate::engine::host::HostRequest::CheckAcl { .. } => {
            Ok(crate::engine::host::HostReply::Allowed(true))
        }
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
    };

    let result = engine
        .eval_with_host(
            "local reply = redis.pcall('boom') \
             if type(reply) ~= 'table' then return 'reply-not-table' end \
             if reply.__err_raise ~= nil then return 'raise-marker-leaked' end \
             return reply.err",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert_eq!(
        result,
        Value::Bulk(Bytes::from_static(b"ERR simulated failure"))
    );
}

#[test]
fn scoped_call_raises_a_plain_error_table_and_pcall_receives_message() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = |request: crate::engine::host::HostRequest<'_>| match request {
        crate::engine::host::HostRequest::ExecuteCommand { .. } => Ok(
            crate::engine::host::HostReply::Command(Value::error("ERR simulated failure")),
        ),
        crate::engine::host::HostRequest::CheckAcl { .. } => {
            Ok(crate::engine::host::HostReply::Allowed(true))
        }
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
    };

    let result = engine
        .eval_with_host(
            "local ok, err = pcall(function() return redis.call('boom') end) \
             if ok then return 'unexpected-success' end \
             if type(err) ~= 'string' then return 'error-not-string' end \
             return err",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert_eq!(
        result,
        Value::Bulk(Bytes::from_static(b"ERR simulated failure"))
    );
}

#[test]
fn scoped_binding_forwards_exactly_one_return_value() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = echo_host;

    let result = engine
        .eval_with_host(
            "local first, second = redis.call('echo', 'one') \
             if second ~= nil then return 'extra-return-leaked' end \
             return first",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert_eq!(result, Value::Bulk(Bytes::from_static(b"one")));
}

#[test]
fn scoped_setresp_rejects_invalid_version_by_raising() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();

    let error = eval_without_commands(&engine, "redis.setresp(9); return 1", &mut ctx).unwrap_err();

    assert!(error.to_string().contains("RESP version must be 2 or 3"));
}

#[test]
fn lua_redis_call_does_not_checkpoint_each_command() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut command_count = 0;
    let mut checkpoint_count = 0;
    let mut host = |request: crate::engine::host::HostRequest<'_>| match request {
        crate::engine::host::HostRequest::ExecuteCommand { .. } => {
            command_count += 1;
            Ok(crate::engine::host::HostReply::Command(Value::Null))
        }
        crate::engine::host::HostRequest::Yield(
            crate::engine::host::HostYield::ScriptCheckpoint { .. },
        ) => {
            checkpoint_count += 1;
            Ok(crate::engine::host::HostReply::Continue)
        }
        crate::engine::host::HostRequest::CheckAcl { .. } => {
            Ok(crate::engine::host::HostReply::Allowed(true))
        }
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
    };

    engine
        .eval_with_host(
            "redis.call('ping'); redis.call('ping'); return 1",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert_eq!(command_count, 2);
    assert_eq!(checkpoint_count, 0);
}

#[test]
fn lua_instruction_hook_uses_valkey_checkpoint_interval() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut short_script_checkpoints = 0;
    let mut host = |request: crate::engine::host::HostRequest<'_>| match request {
        crate::engine::host::HostRequest::Yield(
            crate::engine::host::HostYield::ScriptCheckpoint { .. },
        ) => {
            short_script_checkpoints += 1;
            Ok(crate::engine::host::HostReply::Continue)
        }
        crate::engine::host::HostRequest::ExecuteCommand { .. } => {
            Ok(crate::engine::host::HostReply::Command(Value::Null))
        }
        crate::engine::host::HostRequest::CheckAcl { .. } => {
            Ok(crate::engine::host::HostReply::Allowed(true))
        }
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
    };

    engine
        .eval_with_host(
            "local total = 0; for i = 1, 10000 do total = total + i end; return total",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert_eq!(short_script_checkpoints, 0);

    let mut long_script_checkpoints = 0;
    let mut host = |request: crate::engine::host::HostRequest<'_>| match request {
        crate::engine::host::HostRequest::Yield(
            crate::engine::host::HostYield::ScriptCheckpoint { .. },
        ) => {
            long_script_checkpoints += 1;
            Ok(crate::engine::host::HostReply::Continue)
        }
        crate::engine::host::HostRequest::ExecuteCommand { .. } => {
            Ok(crate::engine::host::HostReply::Command(Value::Null))
        }
        crate::engine::host::HostRequest::CheckAcl { .. } => {
            Ok(crate::engine::host::HostReply::Allowed(true))
        }
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
    };

    engine
        .eval_with_host(
            "local total = 0; for i = 1, 100000 do total = total + i end; return total",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert!(long_script_checkpoints > 0);
}

#[test]
fn lua_control_helpers_use_the_scoped_invocation_state() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = echo_host;

    let result = engine
        .eval_with_host(
            "redis.setresp(3); redis.set_repl(redis.REPL_NONE); return 1",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert_eq!(result, Value::Integer(1));
    assert_eq!(ctx.resp, 3);
    assert!(ctx.skip_propagate);
    assert_eq!(ctx.propagation_targets, PropagationTargets::None);
}

#[test]
fn lua_acl_check_uses_the_scoped_host_dispatcher() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let mut host = echo_host;

    let result = engine
        .eval_with_host(
            "return redis.acl_check_cmd('get', 'key')",
            &mut ctx,
            &mut host,
        )
        .unwrap();

    assert_eq!(result, Value::Integer(1));
}

#[test]
fn nested_effects_capture_selected_db_at_command_completion() {
    let mut ctx = test_ctx();
    let first_args = command_args(&[b"SET", b"a", b"1"]);
    let select_args = command_args(&[b"SELECT", b"10"]);
    let second_args = command_args(&[b"SET", b"b", b"2"]);

    redis_call::_capture_propagation_effect(
        &mut ctx,
        redis_call::CompletedCommandEffect {
            db_id: 0,
            args: &first_args,
            result: &Value::SimpleString(Bytes::from_static(b"OK")),
            may_write_or_replicate: true,
            skip_propagate: false,
            custom_frames: Vec::new(),
        },
    );
    redis_call::_capture_propagation_effect(
        &mut ctx,
        redis_call::CompletedCommandEffect {
            db_id: 10,
            args: &select_args,
            result: &Value::SimpleString(Bytes::from_static(b"OK")),
            may_write_or_replicate: false,
            skip_propagate: true,
            custom_frames: Vec::new(),
        },
    );
    redis_call::_capture_propagation_effect(
        &mut ctx,
        redis_call::CompletedCommandEffect {
            db_id: 10,
            args: &second_args,
            result: &Value::SimpleString(Bytes::from_static(b"OK")),
            may_write_or_replicate: true,
            skip_propagate: false,
            custom_frames: Vec::new(),
        },
    );

    let first =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"a", b"1"]);
    let second =
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"b", b"2"]);
    assert_eq!(
        ctx.propagation_ops,
        vec![
            PropagationOp::both(0, first),
            PropagationOp::both(10, second)
        ]
    );
}

#[test]
fn consecutive_nested_effects_share_one_propagation_batch() {
    let mut ctx = test_ctx();
    let first = command_args(&[b"SET", b"a", b"1"]);
    let second = command_args(&[b"INCR", b"a"]);

    for args in [&first, &second] {
        redis_call::_capture_propagation_effect(
            &mut ctx,
            redis_call::CompletedCommandEffect {
                db_id: 0,
                args,
                result: &Value::SimpleString(Bytes::from_static(b"OK")),
                may_write_or_replicate: true,
                skip_propagate: false,
                custom_frames: Vec::new(),
            },
        );
    }

    let expected = [
        crate::engine::distributed::replication::protocol::encode_command(&[b"SET", b"a", b"1"]),
        crate::engine::distributed::replication::protocol::encode_command(&[b"INCR", b"a"]),
    ]
    .concat();
    assert_eq!(ctx.propagation_ops.len(), 1);
    assert_eq!(ctx.propagation_ops[0].bytes, expected);
    assert_eq!(ctx.propagation_ops[0].command_count(), 2);
}

#[test]
fn failed_nested_effect_does_not_discard_prior_operations() {
    let mut ctx = test_ctx();
    ctx.propagation_ops
        .push(PropagationOp::both(0, b"first".to_vec()));
    let args = command_args(&[b"SET", b"k"]);

    redis_call::_capture_propagation_effect(
        &mut ctx,
        redis_call::CompletedCommandEffect {
            db_id: 10,
            args: &args,
            result: &Value::error("ERR failed"),
            may_write_or_replicate: true,
            skip_propagate: false,
            custom_frames: vec![b"ignored".to_vec()],
        },
    );

    assert_eq!(
        ctx.propagation_ops,
        vec![PropagationOp::both(0, b"first".to_vec())]
    );
}

#[test]
fn repl_none_retains_captured_operations() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let operation = PropagationOp::new(0, PropagationTargets::Both, b"frame".to_vec());
    ctx.propagation_ops.push(operation.clone());

    let result = eval_without_commands(
        &engine,
        "redis.set_repl(redis.REPL_NONE); return 1",
        &mut ctx,
    )
    .unwrap();

    assert_eq!(result, Value::Integer(1));
    assert!(ctx.skip_propagate);
    assert_eq!(ctx.propagation_ops, vec![operation]);
}

#[test]
fn set_repl_selects_a_sink_without_rejecting_it() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();

    eval_without_commands(
        &engine,
        "redis.set_repl(redis.REPL_AOF); return 1",
        &mut ctx,
    )
    .unwrap();
    assert_eq!(ctx.propagation_targets, PropagationTargets::Aof);

    eval_without_commands(
        &engine,
        "redis.set_repl(redis.REPL_REPLICA); return 1",
        &mut ctx,
    )
    .unwrap();
    assert_eq!(ctx.propagation_targets, PropagationTargets::Replication);

    eval_without_commands(
        &engine,
        "redis.set_repl(redis.REPL_NONE); return 1",
        &mut ctx,
    )
    .unwrap();
    assert_eq!(ctx.propagation_targets, PropagationTargets::None);
}

#[test]
fn set_repl_rejects_unknown_numeric_flags_as_invalid() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();

    let error =
        eval_without_commands(&engine, "redis.set_repl(12345); return 1", &mut ctx).unwrap_err();

    assert!(format!("{error:?}").contains("Invalid replication flags"));
}

#[test]
fn replicate_commands_is_available_to_lua_scripts() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();

    let result =
        eval_without_commands(&engine, "redis.replicate_commands(); return 1", &mut ctx).unwrap();

    assert_eq!(result, Value::Integer(1));
    assert!(!ctx.skip_propagate);
}

#[test]
fn test_lua_engine_eval_string() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(&engine, "return 'hello'", &mut ctx).unwrap();
    assert_eq!(result, Value::Bulk(bytes::Bytes::from_static(b"hello")));
}

#[test]
fn test_lua_engine_eval_table() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(&engine, "return {1, 2, 3}", &mut ctx).unwrap();
    match result {
        Value::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Integer(1));
            assert_eq!(items[1], Value::Integer(2));
            assert_eq!(items[2], Value::Integer(3));
        }
        _ => panic!("expected array"),
    }
}

#[test]
fn test_lua_engine_eval_nil() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(&engine, "return nil", &mut ctx).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn test_lua_engine_eval_boolean() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(&engine, "return true", &mut ctx).unwrap();
    assert_eq!(result, Value::Integer(1));
}

#[test]
fn test_lua_engine_eval_math() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(&engine, "return 1 + 2", &mut ctx).unwrap();
    assert_eq!(result, Value::Integer(3));
}

#[test]
fn test_lua_engine_eval_compile_error() {
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(&engine, "syntax error!", &mut ctx);
    assert!(result.is_err());
    match result.unwrap_err() {
        ScriptError::Compile(_) => {}
        other => panic!("expected CompileError, got: {:?}", other),
    }
}

#[test]
fn test_lua_pcall_does_not_catch_script_kill_error() {
    reset_lua_runtime();
    let engine = LuaEngine;
    let mut ctx = test_ctx();
    let result = eval_without_commands(
        &engine,
        "local ok = pcall(function() error('Script killed by user with SCRIPT KILL.') end); return ok",
        &mut ctx,
    );

    match result {
        Err(ScriptError::Runtime(msg)) => {
            assert!(msg.contains("Script killed by user with SCRIPT KILL."));
        }
        other => panic!(
            "expected script kill error to escape pcall, got: {:?}",
            other
        ),
    }
}

#[test]
fn test_noscript_subcmd_binary_search() {
    assert!(is_noscript_subcmd("CONFIG", "GET"));
    assert!(is_noscript_subcmd("client", "id"));
    assert!(!is_noscript_subcmd("client", "list"));
    assert!(is_noscript_subcmd("debug", "object"));
    assert!(!is_noscript_subcmd("config", "set"));
    assert!(!is_noscript_subcmd("cluster", "reset"));
    assert!(!is_noscript_subcmd("client", "kill"));
    assert!(!is_noscript_subcmd("set", ""));
}

#[test]
fn hard_noscript_block_survives_debug_override() {
    assert!(redis_call::is_noscript_denied(
        redis_call::ScriptCommandRestrictions {
            parent_noscript: false,
            subcommand_noscript: false,
            forced_block: true,
            debug_override: true,
            safe_subcommand: false,
        }
    ));
    assert!(!redis_call::is_noscript_denied(
        redis_call::ScriptCommandRestrictions {
            parent_noscript: true,
            subcommand_noscript: false,
            forced_block: false,
            debug_override: true,
            safe_subcommand: false,
        }
    ));
}

#[test]
fn test_parse_shebang_no_shebang() {
    let code = "return 1";
    let (body, flags, has_shebang) = parse_shebang(code, SCRIPT_EVAL_MODE).unwrap();
    assert_eq!(body, "return 1");
    assert_eq!(flags, SCRIPT_EVAL_MODE);
    assert!(!has_shebang);
}

#[test]
fn test_parse_shebang_with_flags() {
    let code = "#! lua flags=no-writes,allow-oom\nreturn 1";
    let (body, flags, has_shebang) = parse_shebang(code, 0).unwrap();
    assert_eq!(body, "return 1");
    assert!(flags & crate::engine::extension::script::SCRIPT_READ_ONLY != 0);
    assert!(flags & crate::engine::extension::script::SCRIPT_ALLOW_OOM != 0);
    assert!(has_shebang);
}

#[test]
fn test_parse_shebang_allow_cross_slot() {
    let code = "#! lua flags=allow-cross-slot-keys\nreturn 1";
    let (body, flags, has_shebang) = parse_shebang(code, 0).unwrap();
    assert_eq!(body, "return 1");
    assert!(flags & crate::engine::extension::script::SCRIPT_ALLOW_CROSS_SLOT != 0);
    assert!(has_shebang);
}

#[test]
fn test_parse_shebang_no_cluster() {
    let code = "#! lua flags=no-cluster\nreturn 1";
    let (_, flags, has_shebang) = parse_shebang(code, 0).unwrap();
    assert!(flags & crate::engine::extension::script::SCRIPT_NO_CLUSTER != 0);
    assert!(has_shebang);
}

#[test]
fn test_parse_shebang_unknown_flag_errors() {
    let code = "#! lua flags=unknown-flag\nreturn 1";
    let result = parse_shebang(code, 0);
    assert!(result.is_err());
}

#[test]
fn test_parse_shebang_unknown_engine_errors() {
    let code = "#! python flags=no-writes\nreturn 1";
    let result = parse_shebang(code, 0);
    assert!(result.is_err());
}

#[test]
fn writable_shebang_requires_enough_replicas_before_execution() {
    let result = super::engine::check_eval_preconditions(super::engine::EvalPreconditions {
        flags: 0,
        has_shebang: true,
        has_maxmemory_limit: false,
        has_enough_replicas_to_write: false,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        readonly_error_style: super::engine::ReadonlyErrorStyle::Valkey,
    });

    match result {
        Err(ScriptError::Runtime(message)) => {
            assert_eq!(message, "NOREPLICAS Not enough good replicas to write.");
        }
        other => panic!("expected NOREPLICAS error, got: {:?}", other),
    }
}

#[test]
fn legacy_and_no_writes_scripts_defer_or_skip_replica_precheck() {
    assert!(
        super::engine::check_eval_preconditions(super::engine::EvalPreconditions {
            flags: 0,
            has_shebang: false,
            has_maxmemory_limit: false,
            has_enough_replicas_to_write: false,
            is_readonly_replica: true,
            is_stale_replica: false,
            has_persistence_write_error: true,
            readonly_error_style: super::engine::ReadonlyErrorStyle::Legacy,
        })
        .is_ok()
    );
    assert!(
        super::engine::check_eval_preconditions(super::engine::EvalPreconditions {
            flags: crate::engine::extension::script::SCRIPT_READ_ONLY,
            has_shebang: true,
            has_maxmemory_limit: false,
            has_enough_replicas_to_write: false,
            is_readonly_replica: true,
            is_stale_replica: false,
            has_persistence_write_error: true,
            readonly_error_style: super::engine::ReadonlyErrorStyle::Legacy,
        })
        .is_ok()
    );
}

#[test]
fn writable_shebang_on_readonly_replica_uses_valkey_error() {
    let result = super::engine::check_eval_preconditions(super::engine::EvalPreconditions {
        flags: 0,
        has_shebang: true,
        has_maxmemory_limit: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: true,
        is_stale_replica: false,
        has_persistence_write_error: false,
        readonly_error_style: super::engine::ReadonlyErrorStyle::Valkey,
    });

    match result {
        Err(ScriptError::Runtime(message)) => assert_eq!(
            message,
            "READONLY Can not run script with write flag on readonly replica."
        ),
        other => panic!("expected readonly script error, got: {:?}", other),
    }
}

#[test]
fn test_extract_function_name_single_quote() {
    let code = "redis.register_function('my_func', function() return 1 end)";
    assert_eq!(extract_function_name(code), "my_func");
}

#[test]
fn test_extract_function_name_double_quote() {
    let code = "redis.register_function(\"my_func2\", function() end)";
    assert_eq!(extract_function_name(code), "my_func2");
}

#[test]
fn test_extract_function_name_fallback() {
    let code = "abc123xyz";
    assert_eq!(extract_function_name(code), "f_abc123");
}

#[test]
fn test_extract_function_name_anon() {
    let code = "!@#$";
    assert_eq!(extract_function_name(code), "f_anon");
}

#[test]
fn test_has_err_code_prefix() {
    assert!(has_err_code_prefix("ERR something wrong"));
    assert!(has_err_code_prefix("WRONGTYPE wrong type"));
    assert!(has_err_code_prefix("MOVED 123 127.0.0.1:6379"));
    assert!(has_err_code_prefix("OOM command not allowed"));
    assert!(has_err_code_prefix("NOREPLICAS not enough good replicas"));
    assert!(has_err_code_prefix("MISCONF persistence is unavailable"));
    assert!(!has_err_code_prefix("something else"));
    assert!(!has_err_code_prefix(""));
}

#[test]
fn test_is_blocked_subcmd() {
    assert!(is_blocked_subcmd("cluster", "reset"));
    assert!(is_blocked_subcmd("CLUSTER", "RESET"));
    assert!(!is_blocked_subcmd("client", "id"));
    assert!(!is_blocked_subcmd("set", ""));
}
