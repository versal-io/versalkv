use super::*;

use crate::engine::extension::script::CompiledFunction;

fn no_command_host(
    request: crate::engine::host::HostRequest<'_>,
) -> Result<crate::engine::host::HostReply, crate::engine::host::HostError> {
    match request {
        crate::engine::host::HostRequest::Yield(_) => Ok(crate::engine::host::HostReply::Continue),
        _ => Err(crate::engine::host::HostError::Runtime(
            "unexpected host request".to_string(),
        )),
    }
}
use crate::engine::extension::lua::LUA_ENGINE;

#[test]
fn test_parse_engine_name_lua() {
    assert_eq!(parse_engine_name("#!lua flags=no-writes\nreturn 42"), "lua");
    assert_eq!(parse_engine_name("#!lua\nreturn 42"), "lua");
    assert_eq!(parse_engine_name("return 42"), "LUA");
}

#[test]
fn test_engine_manager_default() {
    let mgr = EngineManager::new(&LUA_ENGINE);
    assert!(mgr.get("LUA").is_some());
    assert!(mgr.get("lua").is_some());
    assert!(mgr.get("JS").is_none());
}

#[test]
fn test_function_registry_load_and_find() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('test', function(keys, args) return #keys end)";
    reg.load_library("testlib", code).unwrap();
    assert_eq!(reg.library_count(), 1);
    assert!(reg.function_count() > 0);
    assert!(reg.find_function("test").is_some());
    assert!(reg.find_function("TEST").is_some());
}

#[test]
fn test_function_registry_delete() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('test', function(keys, args) return #keys end)";
    reg.load_library("testlib", code).unwrap();
    assert_eq!(reg.library_count(), 1);
    assert!(reg.delete_library("testlib").unwrap());
    assert_eq!(reg.library_count(), 0);
    assert!(!reg.delete_library("nonexistent").unwrap());
}

#[test]
fn deleting_function_library_releases_compiled_chunk() {
    use crate::engine::extension::lua::{compiled_script_count, reset_lua_runtime};

    reset_lua_runtime();
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('cached', function(keys, args) return 1 end)";
    reg.load_library("cached-lib", code).unwrap();
    assert!(reg.delete_library("cached-lib").unwrap());

    assert_eq!(compiled_script_count(), 0);
    reset_lua_runtime();
}

#[test]
fn test_function_registry_flush() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code1 = "#!lua flags=no-writes\nserver.register_function('test1', function(keys, args) return #keys end)";
    let code2 = "#!lua flags=no-writes\nserver.register_function('test2', function(keys, args) return #keys end)";
    reg.load_library("lib1", code1).unwrap();
    reg.load_library("lib2", code2).unwrap();
    assert_eq!(reg.library_count(), 2);
    reg.flush();
    assert_eq!(reg.library_count(), 0);
    assert_eq!(reg.function_count(), 0);
}

#[test]
fn test_flush_clears_lua_vm_function_table() {
    use crate::engine::extension::lua::registered_function_count;
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('vmtest', function(keys, args) return #keys end)";
    reg.load_library("vmlib", code).unwrap();
    assert!(
        registered_function_count() >= 1,
        "VM should hold the registered callback after load"
    );
    reg.flush();
    assert_eq!(
        registered_function_count(),
        0,
        "flush() must clear the Lua VM function table"
    );
}

#[test]
fn test_fcall_self_heals_after_lua_runtime_reset() {
    use crate::engine::extension::lua::reset_lua_runtime;
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua name=test\nserver.register_function('healme', function(keys, args) return 'hello1' end)\nserver.register_function('healme_sibling', function(keys, args) return 'hello2' end)";
    reg.load_library("healme_lib", code).unwrap();

    reset_lua_runtime();

    let compiled = reg
        .find_function("healme")
        .expect("function still in registry")
        .clone();
    let engine = reg.function_engine(&compiled).expect("engine present");
    let mut ctx = crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(compiled.flags);
    let mut host = no_command_host;
    let result = engine.call(&compiled, &mut ctx, &[], &[], &mut host);
    assert_eq!(
        result.expect("FCALL after VM reset must self-heal, not error nil-to-table"),
        crate::foundation::io::protocol::Value::Bulk(bytes::Bytes::from_static(b"hello1")),
    );

    let sibling = reg
        .find_function("healme_sibling")
        .expect("sibling function still in registry")
        .clone();
    let sibling_engine = reg.function_engine(&sibling).expect("engine present");
    let mut sibling_ctx =
        crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(sibling.flags);
    assert_eq!(
        sibling_engine
            .call(&sibling, &mut sibling_ctx, &[], &[], &mut host)
            .expect("self-healing one callback must restore the whole library"),
        crate::foundation::io::protocol::Value::Bulk(bytes::Bytes::from_static(b"hello2")),
    );
}

#[test]
fn test_fcall_self_heal_propagates_exit_compile_mode_error() {
    use crate::engine::extension::lua::{
        registered_function_count, reset_lua_runtime, with_lua_runtime,
    };
    use crate::engine::extension::script::ScriptError;

    reset_lua_runtime();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut reg = FunctionRegistry::new(&LUA_ENGINE);
        let code = "#!lua name=test\nserver.register_function('healme_error', function(keys, args) return 'hello1' end)";
        reg.load_library("healme_error_lib", code).unwrap();

        reset_lua_runtime();
        with_lua_runtime(false, |lua| {
            lua.globals()
                .raw_set("__vky_saved_runtime", true)
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        })
        .unwrap();

        let compiled = reg
            .find_function("healme_error")
            .expect("function metadata must remain in the registry")
            .clone();
        let engine = reg.function_engine(&compiled).expect("engine present");
        let mut ctx =
            crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(compiled.flags);
        let mut host = no_command_host;
        let error = match engine.call(&compiled, &mut ctx, &[], &[], &mut host) {
            Ok(value) => panic!("self-heal must not return the stale value: {value:?}"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("Failed to get runtime saved"),
            "unexpected error: {error}"
        );
        assert_eq!(
            registered_function_count(),
            0,
            "failed self-healing must not publish callbacks to the active table"
        );
    }));
    reset_lua_runtime();

    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn test_fcall_self_heal_execution_failure_preserves_active_callbacks() {
    use crate::engine::extension::lua::{registered_function_count, reset_lua_runtime};

    reset_lua_runtime();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut reg = FunctionRegistry::new(&LUA_ENGINE);
        let stable_code = "#!lua name=stable\nlocal counter = 0\nserver.register_function('stable_callback', function() counter = counter + 1 return counter end)";
        reg.load_library("stable", stable_code).unwrap();
        let failed = CompiledFunction {
            engine: "LUA",
            name: "failed_callback".to_string(),
            code: "#!lua name=failed\nserver.register_function('failed_callback', function() return 'failed' end)\nerror('boom')".to_string(),
            description: String::new(),
            flags: 0,
        };
        let engine = reg.function_engine(&failed).expect("engine present");
        let mut failed_ctx =
            crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(failed.flags);
        let mut host = no_command_host;

        let error = engine
            .call(&failed, &mut failed_ctx, &[], &[], &mut host)
            .expect_err("self-heal library execution must fail");

        assert!(
            error.to_string().contains("boom"),
            "unexpected error: {error}"
        );
        assert_eq!(
            registered_function_count(),
            1,
            "failed self-healing must leave only the original callback active"
        );
        let stable = reg
            .find_function("stable_callback")
            .expect("stable function remains registered")
            .clone();
        let stable_engine = reg.function_engine(&stable).expect("engine present");
        let mut stable_ctx =
            crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(stable.flags);
        assert_eq!(
            stable_engine
                .call(&stable, &mut stable_ctx, &[], &[], &mut host)
                .expect("original callback remains callable"),
            crate::foundation::io::protocol::Value::Integer(1)
        );
    }));
    reset_lua_runtime();

    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn test_fcall_self_heal_propagates_enter_compile_mode_error() {
    use crate::engine::extension::lua::{reset_lua_runtime, with_lua_runtime};
    use crate::engine::extension::script::ScriptError;

    reset_lua_runtime();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut reg = FunctionRegistry::new(&LUA_ENGINE);
        let code = "#!lua name=test\nserver.register_function('healme_enter_error', function(keys, args) return 'hello1' end)";
        reg.load_library("healme_enter_error_lib", code).unwrap();

        reset_lua_runtime();
        with_lua_runtime(false, |lua| {
            lua.globals()
                .raw_set("__vky_lib_api", true)
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        })
        .unwrap();

        let compiled = reg
            .find_function("healme_enter_error")
            .expect("function metadata must remain in the registry")
            .clone();
        let engine = reg.function_engine(&compiled).expect("engine present");
        let mut ctx =
            crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(compiled.flags);
        let mut host = no_command_host;
        let error = engine
            .call(&compiled, &mut ctx, &[], &[], &mut host)
            .expect_err("self-heal must propagate the compile mode entry error");

        assert!(
            error.to_string().contains("Failed to get lib API"),
            "unexpected error: {error}"
        );
    }));
    reset_lua_runtime();

    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn test_fcall_self_heal_combines_execution_and_exit_compile_mode_errors() {
    use crate::engine::extension::lua::{reset_lua_runtime, with_lua_runtime};
    use crate::engine::extension::script::ScriptError;

    reset_lua_runtime();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut reg = FunctionRegistry::new(&LUA_ENGINE);
        let code = "#!lua name=test\nserver.register_function('healme_combined_error', function(keys, args) return 'hello1' end)";
        reg.load_library("healme_combined_error_lib", code).unwrap();

        reset_lua_runtime();
        with_lua_runtime(false, |lua| {
            lua.unset_named_registry_value("__valkey_redis_runtime_table_v1")
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            lua.globals()
                .raw_set("__vky_saved_runtime", true)
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        })
        .unwrap();

        let compiled = reg
            .find_function("healme_combined_error")
            .expect("function metadata must remain in the registry")
            .clone();
        let engine = reg.function_engine(&compiled).expect("engine present");
        let mut ctx =
            crate::engine::extension::script::ScriptRunCtx::new_bare_for_test(compiled.flags);
        let mut host = no_command_host;
        let error = engine
            .call(&compiled, &mut ctx, &[], &[], &mut host)
            .expect_err("self-heal must report execution and compile mode exit errors");
        let message = error.to_string();

        assert!(
            message.contains("Failed to execute function body"),
            "missing execution error: {message}"
        );
        assert!(
            message.contains("Failed to get runtime saved"),
            "missing compile mode exit error: {message}"
        );
    }));
    reset_lua_runtime();

    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn test_function_registry_dump_restore() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let code = "#!lua flags=no-writes\nserver.register_function('test', function(keys, args) return #keys end)";
    reg.load_library("testlib", code).unwrap();
    let dumped = reg.dump();
    assert!(!dumped.is_empty());

    let mut reg2 = FunctionRegistry::new(&LUA_ENGINE);
    let count = reg2.restore(&dumped).unwrap();
    assert_eq!(count, 1);
    assert_eq!(reg2.library_count(), 1);
}

#[test]
fn function_registry_restore_rejects_truncated_records() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);

    assert!(matches!(
        reg.restore(b"new\n"),
        Err(crate::engine::extension::script::ScriptError::Restore(_))
    ));
    assert!(matches!(
        reg.restore(b"new\n100\nshort"),
        Err(crate::engine::extension::script::ScriptError::Restore(_))
    ));
}

#[test]
fn function_registry_restore_rejects_overflowing_code_length() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let dump = format!("name\n{}\n", usize::MAX);

    let result = reg.restore(dump.as_bytes());

    assert!(
        matches!(
            &result,
            Err(crate::engine::extension::script::ScriptError::Restore(message))
                if message == "Invalid dump format: truncated code"
        ),
        "unexpected restore result: {result:?}"
    );
}

#[test]
fn function_registry_restore_rejects_invalid_utf8_code() {
    let mut reg = FunctionRegistry::new(&LUA_ENGINE);
    let mut code =
        b"#!lua flags=no-writes\nserver.register_function('bad', function() return '".to_vec();
    code.push(0xff);
    code.extend_from_slice(b"' end)");
    let mut dump = format!("bad\n{}\n", code.len()).into_bytes();
    dump.extend_from_slice(&code);

    let result = reg.restore(&dump);

    assert!(
        matches!(
            &result,
            Err(crate::engine::extension::script::ScriptError::Restore(message))
                if message == "Invalid dump format: code is not UTF-8"
        ),
        "unexpected restore result: {result:?}"
    );
    assert_eq!(reg.library_count(), 0);
}

#[test]
fn dropping_function_registry_releases_tracked_memory() {
    use crate::engine::extension::lua::reset_lua_runtime;
    use crate::foundation::memory::metrics::memory_stats::{self, MemoryCategory};

    let guard = memory_stats::lock_global_for_test();
    let tracker = memory_stats::get_tracker();
    let baseline = tracker.get(MemoryCategory::FunctionsVm);

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        reset_lua_runtime();
        {
            let mut registry = FunctionRegistry::new(&LUA_ENGINE);
            let code =
                "#!lua name=tracked\nserver.register_function('tracked', function() return 1 end)";
            registry.load_library("tracked", code).unwrap();

            assert!(tracker.get(MemoryCategory::FunctionsVm) > baseline);
        }

        tracker.get(MemoryCategory::FunctionsVm)
    }));

    let current = tracker.get(MemoryCategory::FunctionsVm);
    if current > baseline {
        memory_stats::track_sub(MemoryCategory::FunctionsVm, current - baseline);
    } else if current < baseline {
        memory_stats::track_add(MemoryCategory::FunctionsVm, baseline - current);
    }
    reset_lua_runtime();
    drop(guard);

    match outcome {
        Ok(after_drop) => assert_eq!(after_drop, baseline),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
