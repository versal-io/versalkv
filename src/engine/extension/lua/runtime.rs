use mlua::prelude::*;
use mlua::RegistryKey;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::engine::extension::script::{ScriptError, ScriptRunCtx};
use crate::engine::host::HostDispatcher;

use super::sandbox;

pub(super) const HOST_CALLBACK_REGISTRY_KEY: &str = "__valkey_host_callback_v1";
pub(super) const REDIS_RUNTIME_TABLE_REGISTRY_KEY: &str = "__valkey_redis_runtime_table_v1";

// Bindings split into two shapes:
// * `raises` bindings forward a single reply; a reply table carrying
//   `__err_raise` is converted into `error({err = ...})`, matching the C
//   `redis.call`/control-helper error contract.
// * protected bindings forward the reply unchanged; error replies are plain
//   `{err = ...}` tables for `redis.pcall`.
// The hot success path allocates no Lua table and performs no `unpack`: it is
// one direct callback call plus a single type tag check.
const HOST_BINDING_FACTORY: &str = r#"
    return function(callback, operation, raises)
        if raises then
            return function(...)
                local reply = callback(operation, ...)
                if type(reply) == 'table' and reply.__err_raise then
                    error({err = reply.err})
                end
                return reply
            end
        end
        return function(...)
            return callback(operation, ...)
        end
    end
"#;

pub(super) struct LuaHostSlotGuard<'lua> {
    lua: &'lua Lua,
}

impl<'lua> LuaHostSlotGuard<'lua> {
    pub(super) fn install(lua: &'lua Lua, callback: LuaFunction) -> LuaResult<Self> {
        lua.set_named_registry_value(HOST_CALLBACK_REGISTRY_KEY, callback)?;
        Ok(Self { lua })
    }
}

impl Drop for LuaHostSlotGuard<'_> {
    fn drop(&mut self) {
        let _ = self
            .lua
            .unset_named_registry_value(HOST_CALLBACK_REGISTRY_KEY);
    }
}

struct LuaHostBindingsGuard {
    redis: LuaTable,
    previous: Vec<(&'static str, LuaValue)>,
}

impl LuaHostBindingsGuard {
    fn install(lua: &Lua, callback: &LuaFunction) -> LuaResult<Self> {
        let redis: LuaTable = lua.named_registry_value(REDIS_RUNTIME_TABLE_REGISTRY_KEY)?;
        let factory: LuaFunction = lua.load(HOST_BINDING_FACTORY).eval()?;
        let mut guard = Self {
            redis,
            previous: Vec::with_capacity(5),
        };
        for (name, operation, raises) in [
            ("call", 0_i64, true),
            ("pcall", 1, false),
            ("setresp", 2, true),
            ("set_repl", 3, true),
            ("acl_check_cmd", 4, true),
        ] {
            let previous = guard.redis.raw_get::<LuaValue>(name)?;
            let binding: LuaFunction = factory.call((callback.clone(), operation, raises))?;
            guard.redis.raw_set(name, binding)?;
            guard.previous.push((name, previous));
        }
        Ok(guard)
    }
}

impl Drop for LuaHostBindingsGuard {
    fn drop(&mut self) {
        for (name, value) in self.previous.drain(..) {
            let _ = self.redis.raw_set(name, value);
        }
    }
}

pub(super) fn with_lua_host<R>(
    lua: &Lua,
    run: &mut ScriptRunCtx,
    dispatcher: &mut HostDispatcher<'_>,
    invoke: impl FnOnce() -> LuaResult<R>,
) -> LuaResult<R> {
    lua.scope(|scope| {
        let callback = scope.create_function_mut(
            move |lua, mut values: LuaMultiValue| -> LuaResult<LuaValue> {
                let operation = match values.pop_front() {
                    Some(LuaValue::Integer(operation)) => operation,
                    _ => return Err(mlua::Error::external("Invalid script host operation")),
                };
                match operation {
                    0 => super::redis_call::redis_call_impl(lua, values, false, run, dispatcher),
                    1 => super::redis_call::redis_call_impl(lua, values, true, run, dispatcher),
                    2 => set_resp(lua, values, run),
                    3 => set_repl(lua, values, run),
                    4 => super::redis_call::acl_check_impl(lua, values, dispatcher),
                    5 => {
                        crate::engine::host::yield_script(dispatcher, run)
                            .map_err(|error| mlua::Error::external(error.to_string()))?;
                        if run.is_killed() {
                            Err(mlua::Error::external(run.kill_message()))
                        } else {
                            Ok(LuaValue::Boolean(true))
                        }
                    }
                    _ => Err(mlua::Error::external("Unknown script host operation")),
                }
            },
        )?;
        let _slot = LuaHostSlotGuard::install(lua, callback.clone())?;
        let _bindings = LuaHostBindingsGuard::install(lua, &callback)?;
        invoke()
    })
}

fn set_resp(lua: &Lua, args: LuaMultiValue, run: &mut ScriptRunCtx) -> LuaResult<LuaValue> {
    let mut iter = args.iter();
    let version = match iter.next() {
        Some(LuaValue::Integer(version)) if *version == 2 || *version == 3 => *version as u8,
        _ => return super::make_raise_err(lua, "RESP version must be 2 or 3."),
    };
    run.resp = version;
    Ok(LuaValue::Boolean(true))
}

fn set_repl(lua: &Lua, args: LuaMultiValue, run: &mut ScriptRunCtx) -> LuaResult<LuaValue> {
    use crate::engine::extension::script::PropagationTargets;

    let mut iter = args.iter();
    let mode = match iter.next() {
        Some(LuaValue::String(mode)) => mode
            .to_str()
            .map(|mode| mode.to_ascii_lowercase())
            .unwrap_or_default(),
        None => return super::make_raise_err(lua, "server.set_repl() requires one argument."),
        _ => {
            return super::make_raise_err(
                lua,
                "Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.",
            )
        }
    };
    match mode.as_str() {
        "all" | "repl_all" => {
            run.skip_propagate = false;
            run.propagation_targets = PropagationTargets::Both;
        }
        "aof" | "repl_aof" => {
            run.skip_propagate = false;
            run.propagation_targets = PropagationTargets::Aof;
        }
        "replica" | "slave" | "repl_replica" | "repl_slave" => {
            run.skip_propagate = false;
            run.propagation_targets = PropagationTargets::Replication;
        }
        "none" | "repl_none" => {
            run.skip_propagate = true;
            run.propagation_targets = PropagationTargets::None;
        }
        _ => {
            return super::make_raise_err(
                lua,
                "REPL_* mode required: REPL_ALL, REPL_AOF, REPL_REPLICA, REPL_SLAVE, REPL_NONE",
            )
        }
    }
    Ok(LuaValue::Boolean(true))
}

pub(super) fn bind_eval_arguments(
    lua: &Lua,
    keys: &[bytes::Bytes],
    args: &[bytes::Bytes],
) -> LuaResult<()> {
    let keys_table = create_argument_table(lua, keys)?;
    let args_table = create_argument_table(lua, args)?;
    let globals = lua.globals();
    let old_keys = globals.raw_get::<LuaValue>("KEYS")?;
    let _old_args = globals.raw_get::<LuaValue>("ARGV")?;

    globals.raw_set("KEYS", keys_table)?;
    if let Err(publish_error) = globals.raw_set("ARGV", args_table) {
        return match globals.raw_set("KEYS", old_keys) {
            Ok(()) => Err(publish_error),
            Err(restore_error) => Err(LuaError::RuntimeError(format!(
                "failed to publish ARGV: {publish_error}; failed to restore KEYS: {restore_error}"
            ))),
        };
    }
    Ok(())
}

fn create_argument_table(lua: &Lua, values: &[bytes::Bytes]) -> LuaResult<LuaTable> {
    let table = lua.create_table_with_capacity(values.len(), 0)?;
    for (index, value) in values.iter().enumerate() {
        table.raw_set(index + 1, lua.create_string(value.as_ref())?)?;
    }
    Ok(table)
}

thread_local! {
    static LUA_RUNTIME: RefCell<Option<LuaRuntime>> = const { RefCell::new(None) };
}

pub(super) struct LuaRuntime {
    pub(super) lua: Lua,
    pub(super) compiled_scripts: HashMap<String, RegistryKey>,
    pub(super) compiled_eval_scripts: HashMap<String, RegistryKey>,
}

impl LuaRuntime {
    fn new(enable_insecure_api: bool) -> Result<Self, ScriptError> {
        let lua = Lua::new();
        super::timeout::install_script_hook(&lua)?;
        sandbox::setup_sandbox(&lua, enable_insecure_api)?;
        sandbox::setup_redis_table(&lua)?;
        super::libs::setup_cjson_table(&lua)?;
        super::libs::setup_cmsgpack_table(&lua)?;
        super::libs::setup_bit_table(&lua)?;
        sandbox::finalize_sandbox(&lua)?;
        let globals = lua.globals();
        let keys = lua
            .create_table()
            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
        let args = lua
            .create_table()
            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
        globals
            .raw_set("KEYS", keys)
            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
        globals
            .raw_set("ARGV", args)
            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
        Ok(Self {
            lua,
            compiled_scripts: HashMap::new(),
            compiled_eval_scripts: HashMap::new(),
        })
    }
}

pub(crate) fn release_eval_script(digest: &str) {
    LUA_RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        let Some(runtime) = runtime.as_mut() else {
            return;
        };
        if let Some(key) = runtime.compiled_eval_scripts.remove(digest) {
            if let Err(error) = runtime.lua.remove_registry_value(key) {
                log::warn!("Failed to release compiled EVAL script {digest}: {error}");
            }
            if let Err(error) = runtime.lua.gc_step() {
                log::warn!("Failed to advance Lua GC after EVAL eviction: {error}");
            }
        }
    });
}

pub(crate) fn clear_eval_scripts() {
    LUA_RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        let Some(runtime) = runtime.as_mut() else {
            return;
        };
        for (_, key) in runtime.compiled_eval_scripts.drain() {
            if let Err(error) = runtime.lua.remove_registry_value(key) {
                log::warn!("Failed to release compiled EVAL script: {error}");
            }
        }
        if let Err(error) = runtime.lua.gc_collect() {
            log::warn!("Failed to collect Lua GC after SCRIPT FLUSH: {error}");
        }
    });
}

pub(crate) fn reset_lua_runtime() {
    LUA_RUNTIME.with(|runtime| {
        *runtime.borrow_mut() = None;
    });
}

/// Copy a selected debugger global into an owned protocol value while its Lua
/// handle is alive. Never recreate a runtime to service a stale debug session.
pub(crate) fn debug_global(name: &str) -> Option<crate::foundation::io::protocol::Value> {
    if !matches!(name, "ARGV" | "KEYS") {
        return None;
    }
    LUA_RUNTIME.with(|runtime| {
        let runtime = runtime.try_borrow().ok()?;
        let runtime = runtime.as_ref()?;
        let value = runtime.lua.globals().raw_get::<LuaValue>(name).ok()?;
        Some(super::convert::lua_value_to_value_resp(&value, 2))
    })
}

pub(crate) fn clear_function_tables() {
    LUA_RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        if let Some(ref mut runtime) = *runtime {
            let globals = runtime.lua.globals();
            let _ = globals.raw_set(
                super::REGISTERED_FUNCS_TABLE,
                runtime.lua.create_table().ok(),
            );
            let _ = globals.raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil);
        }
    });
}

pub(super) fn with_lua_runtime<R>(
    enable_insecure_api: bool,
    f: impl FnOnce(&mut LuaRuntime) -> Result<R, ScriptError>,
) -> Result<R, ScriptError> {
    LUA_RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        if runtime.is_none() {
            *runtime = Some(LuaRuntime::new(enable_insecure_api)?);
        }
        f(runtime.as_mut().unwrap())
    })
}

#[cfg(test)]
pub(crate) fn registered_function_count() -> usize {
    LUA_RUNTIME.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        if runtime.is_none() {
            *runtime = Some(LuaRuntime::new(false).expect("lua runtime init"));
        }
        let runtime = runtime.as_ref().unwrap();
        let globals = runtime.lua.globals();
        match globals.raw_get::<LuaTable>(super::REGISTERED_FUNCS_TABLE) {
            Ok(t) => t.pairs::<LuaString, LuaTable>().count(),
            Err(_) => 0,
        }
    })
}

#[cfg(test)]
pub(crate) fn compiled_eval_count() -> usize {
    with_lua_runtime(false, |runtime| Ok(runtime.compiled_eval_scripts.len())).unwrap()
}

#[cfg(test)]
pub(crate) fn compiled_script_count() -> usize {
    with_lua_runtime(false, |runtime| Ok(runtime.compiled_scripts.len())).unwrap()
}

#[cfg(test)]
pub(crate) fn eval_argument_tables() -> Result<(LuaTable, LuaTable), ScriptError> {
    with_lua_runtime(false, |runtime| {
        let globals = runtime.lua.globals();
        Ok((
            globals.raw_get::<LuaTable>("KEYS").unwrap(),
            globals.raw_get::<LuaTable>("ARGV").unwrap(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::lua::LuaEngine;
    use bytes::Bytes;

    fn compiled_eval_function(runtime: &LuaRuntime, digest: &str) -> LuaFunction {
        let key = &runtime.compiled_eval_scripts[digest];
        runtime.lua.registry_value(key).unwrap()
    }

    #[test]
    fn bind_eval_arguments_publishes_globals_atomically_on_allocation_failure() {
        const OLD_KEYS: &str = "old-keys-sentinel";
        const OLD_ARGS: &str = "old-args-sentinel";

        reset_lua_runtime();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let keys = (0..32)
                .map(|index| Bytes::from(format!("key-{index:03}-{}", "k".repeat(256))))
                .collect::<Vec<_>>();
            let args = (0..256)
                .map(|index| Bytes::from(format!("arg-{index:03}-{}", "a".repeat(512))))
                .collect::<Vec<_>>();
            let mut observed_error = false;

            for headroom in [
                64usize,
                512,
                2 * 1024,
                8 * 1024,
                16 * 1024,
                32 * 1024,
                64 * 1024,
                128 * 1024,
                256 * 1024,
            ] {
                reset_lua_runtime();
                with_lua_runtime(false, |runtime| {
                    let old_keys = runtime
                        .lua
                        .create_table()
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    old_keys
                        .raw_set(1, OLD_KEYS)
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    let old_args = runtime
                        .lua
                        .create_table()
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    old_args
                        .raw_set(1, OLD_ARGS)
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    let globals = runtime.lua.globals();
                    globals
                        .raw_set("KEYS", old_keys)
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    globals
                        .raw_set("ARGV", old_args)
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    runtime
                        .lua
                        .set_memory_limit(runtime.lua.used_memory().saturating_add(headroom))
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    Ok(())
                })
                .unwrap();

                let bind_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    with_lua_runtime(false, |runtime| {
                        bind_eval_arguments(&runtime.lua, &keys, &args)
                            .map_err(|error| ScriptError::Runtime(error.to_string()))
                    })
                }));
                let limit_cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    with_lua_runtime(false, |runtime| {
                        runtime
                            .lua
                            .set_memory_limit(0)
                            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                        Ok(())
                    })
                }));
                let bind_result = match bind_outcome {
                    Ok(result) => {
                        match limit_cleanup {
                            Ok(result) => result.unwrap(),
                            Err(payload) => std::panic::resume_unwind(payload),
                        }
                        result
                    }
                    Err(payload) => {
                        let _ = limit_cleanup;
                        std::panic::resume_unwind(payload);
                    }
                };

                if bind_result.is_err() {
                    observed_error = true;
                    with_lua_runtime(false, |runtime| {
                        let globals = runtime.lua.globals();
                        let current_keys = globals
                            .raw_get::<LuaTable>("KEYS")
                            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                        let current_args = globals
                            .raw_get::<LuaTable>("ARGV")
                            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                        assert_eq!(
                            current_keys
                                .raw_get::<String>(1)
                                .map_err(|error| ScriptError::Runtime(error.to_string()))?,
                            OLD_KEYS
                        );
                        assert_eq!(
                            current_args
                                .raw_get::<String>(1)
                                .map_err(|error| ScriptError::Runtime(error.to_string()))?,
                            OLD_ARGS
                        );
                        Ok(())
                    })
                    .unwrap();
                }
                reset_lua_runtime();
            }

            assert!(
                observed_error,
                "at least one constrained-memory argument bind must fail"
            );
        }));

        let limit_cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            LUA_RUNTIME.with(|runtime| {
                if let Some(runtime) = runtime.borrow().as_ref() {
                    runtime.lua.set_memory_limit(0).unwrap();
                }
            });
        }));
        let reset_cleanup =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(reset_lua_runtime));

        match outcome {
            Ok(()) => {
                if let Err(payload) = limit_cleanup {
                    std::panic::resume_unwind(payload);
                }
                if let Err(payload) = reset_cleanup {
                    std::panic::resume_unwind(payload);
                }
            }
            Err(payload) => {
                let _ = limit_cleanup;
                let _ = reset_cleanup;
                std::panic::resume_unwind(payload);
            }
        }
    }

    #[test]
    fn compiled_eval_scripts_release_by_original_digest() {
        reset_lua_runtime();
        LuaEngine
            .prepare_cached_eval("original", "return ARGV[1]", false)
            .unwrap();
        LuaEngine
            .prepare_cached_eval("original", "return ARGV[1]", false)
            .unwrap();
        LuaEngine
            .prepare_cached_eval("pinned", "return 3", false)
            .unwrap();
        assert!(LuaEngine
            .prepare_cached_eval("invalid", "this is not Lua", false)
            .is_err());
        let weak_functions = with_lua_runtime(false, |runtime| {
            assert_eq!(runtime.compiled_eval_scripts.len(), 2);
            assert!(!runtime.compiled_eval_scripts.contains_key("invalid"));
            let function: LuaFunction = runtime
                .lua
                .load("return function() return 42 end")
                .eval()
                .unwrap();
            let key = runtime.lua.create_registry_value(function.clone()).unwrap();
            runtime.compiled_scripts.insert("library".to_string(), key);
            let table = runtime.lua.create_table().unwrap();
            table.raw_set("library_function", function).unwrap();
            runtime
                .lua
                .globals()
                .raw_set(super::super::REGISTERED_FUNCS_TABLE, table)
                .unwrap();
            let weak = runtime.lua.create_table().unwrap();
            let meta = runtime.lua.create_table().unwrap();
            meta.raw_set("__mode", "v").unwrap();
            weak.set_metatable(Some(meta)).unwrap();
            weak.raw_set("original", compiled_eval_function(runtime, "original"))
                .unwrap();
            weak.raw_set("pinned", compiled_eval_function(runtime, "pinned"))
                .unwrap();
            Ok(weak)
        })
        .unwrap();

        release_eval_script("original");
        release_eval_script("missing");
        with_lua_runtime(false, |runtime| {
            runtime.lua.gc_collect().unwrap();
            assert!(matches!(
                weak_functions.raw_get::<LuaValue>("original").unwrap(),
                LuaValue::Nil
            ));
            assert!(matches!(
                weak_functions.raw_get::<LuaValue>("pinned").unwrap(),
                LuaValue::Function(_)
            ));
            assert!(!runtime.compiled_eval_scripts.contains_key("original"));
            assert!(runtime.compiled_eval_scripts.contains_key("pinned"));
            let function: LuaFunction = runtime
                .lua
                .registry_value(&runtime.compiled_scripts["library"])
                .unwrap();
            assert_eq!(function.call::<i64>(()).unwrap(), 42);
            Ok(())
        })
        .unwrap();
        clear_eval_scripts();
        with_lua_runtime(false, |runtime| {
            assert!(matches!(
                weak_functions.raw_get::<LuaValue>("pinned").unwrap(),
                LuaValue::Nil
            ));
            assert!(runtime.compiled_eval_scripts.is_empty());
            assert!(runtime.compiled_scripts.contains_key("library"));
            Ok(())
        })
        .unwrap();
        assert_eq!(registered_function_count(), 1);
        reset_lua_runtime();
    }
}
