use mlua::prelude::*;
use mlua::RegistryKey;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::engine::extension::script::{ScriptError, ScriptRunCtx};
use crate::engine::host::HostDispatcher;

use super::sandbox;

pub(super) const HOST_CALLBACK_REGISTRY_KEY: &str = "__valkey_host_callback_v1";

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
        let _slot = LuaHostSlotGuard::install(lua, callback)?;
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

thread_local! {
    static LUA_RUNTIME: RefCell<Option<LuaRuntime>> = const { RefCell::new(None) };
}

pub(super) struct LuaRuntime {
    pub(super) lua: Lua,
    pub(super) compiled_scripts: HashMap<String, RegistryKey>,
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
        Ok(Self {
            lua,
            compiled_scripts: HashMap::new(),
        })
    }
}

pub(crate) fn reset_lua_runtime() {
    LUA_RUNTIME.with(|runtime| {
        *runtime.borrow_mut() = None;
    });
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
