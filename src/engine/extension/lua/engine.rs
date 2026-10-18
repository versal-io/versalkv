use mlua::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::extension::script::{
    CompiledFunction, ScriptEngine, ScriptError, ScriptFlags, ScriptRunCtx,
    SCRIPT_ALLOW_CROSS_SLOT, SCRIPT_ALLOW_OOM, SCRIPT_ALLOW_STALE, SCRIPT_NO_CLUSTER,
    SCRIPT_NO_WRITES, SCRIPT_READ_ONLY,
};
use crate::foundation::io::protocol::Value;

use super::convert;
use super::runtime::with_lua_runtime;
use super::shebang::parse_shebang;

pub struct LuaEngine;

pub static LUA_ENGINE: LuaEngine = LuaEngine;

impl LuaEngine {
    pub fn eval_with_host(
        &self,
        code: &str,
        ctx: &mut ScriptRunCtx,
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        let _time_guard = crate::foundation::io::sys::time::enter_command_time_snapshot(
            crate::foundation::io::sys::time::mstime(),
        );
        let (body, flags, _has_shebang) = parse_shebang(code, ctx.flags)?;
        ctx.flags = flags;

        let wrapped = format!("return function(...)\n{}\nend", body);
        let result = with_lua_runtime(ctx.lua_enable_insecure_api, |runtime| {
            let lua = &runtime.lua;
            let pcall_fn: LuaFunction = lua
                .globals()
                .get("pcall")
                .map_err(|e| ScriptError::Runtime(format!("Failed to get pcall: {}", e)))?;

            if !runtime.compiled_scripts.contains_key(&wrapped) {
                let chunk_fn: LuaFunction = match lua.load(&wrapped).set_name("@user_script").eval()
                {
                    Ok(function) => function,
                    Err(error) => return Err(ScriptError::Compile(error.to_string())),
                };
                let key = lua
                    .create_registry_value(chunk_fn)
                    .map_err(|e| ScriptError::Runtime(e.to_string()))?;
                runtime.compiled_scripts.insert(wrapped.clone(), key);
            }

            let key = runtime.compiled_scripts.get(&wrapped).unwrap();
            let chunk_fn: LuaFunction = lua
                .registry_value(key)
                .map_err(|e| ScriptError::Runtime(e.to_string()))?;

            let pcall_result: LuaMultiValue =
                super::runtime::with_lua_host(lua, ctx, dispatcher, || pcall_fn.call(chunk_fn))
                    .map_err(|error| {
                        if ctx.is_killed() {
                            ScriptError::KillError(ctx.kill_message())
                        } else {
                            ScriptError::Runtime(error.to_string())
                        }
                    })?;

            let mut iter = pcall_result.iter();
            let success = matches!(iter.next(), Some(LuaValue::Boolean(true)));
            if success {
                if ctx.is_killed() {
                    return Err(ScriptError::KillError(ctx.kill_message()));
                }
                let result = iter.next().cloned().unwrap_or(LuaValue::Nil);
                Ok(convert::lua_value_to_value_resp(&result, ctx.resp))
            } else {
                if ctx.is_killed() {
                    return Err(ScriptError::KillError(ctx.kill_message()));
                }
                let err_obj = iter.next().cloned().unwrap_or(LuaValue::Nil);
                let err_msg = super::lua_value_to_err_string(&err_obj);
                Err(ScriptError::Runtime(super::format_top_level_lua_error(
                    &err_msg, &body,
                )))
            }
        });
        if matches!(result, Err(ScriptError::KillError(_))) {
            super::runtime::reset_lua_runtime();
        }
        result
    }

    pub fn call_with_host(
        &self,
        func: &CompiledFunction,
        ctx: &mut ScriptRunCtx,
        keys: &[bytes::Bytes],
        args: &[bytes::Bytes],
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        let _time_guard = crate::foundation::io::sys::time::enter_command_time_snapshot(
            crate::foundation::io::sys::time::mstime(),
        );
        let (body, _, _) = parse_shebang(&func.code, func.flags)?;
        let result = with_lua_runtime(ctx.lua_enable_insecure_api, |runtime| {
            let lua = &runtime.lua;
            let pcall_fn: LuaFunction = lua
                .globals()
                .get("pcall")
                .map_err(|e| ScriptError::Runtime(format!("Failed to get pcall: {}", e)))?;

            let globals = lua.globals();
            let reg_table = match globals.raw_get::<LuaTable>(super::REGISTERED_FUNCS_TABLE) {
                Ok(table) => table,
                Err(_) => {
                    let table = lua
                        .create_table()
                        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
                    globals
                        .raw_set(super::REGISTERED_FUNCS_TABLE, table.clone())
                        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
                    table
                }
            };
            let has_callback = reg_table
                .get::<LuaTable>(func.name.as_str())
                .and_then(|entry| entry.get::<LuaFunction>("callback"))
                .is_ok()
                || reg_table.get::<LuaFunction>(func.name.as_str()).is_ok();

            if !has_callback {
                let load_wrapped = format!("return function(...)\n{}\nend", body);
                let load_chunk: LuaFunction = lua
                    .load(&load_wrapped)
                    .set_name("@user_script")
                    .eval()
                    .map_err(|error| {
                        ScriptError::Runtime(format!("Failed to load function body: {error}"))
                    })?;
                let _ = crate::engine::extension::lua::sandbox::enter_compile_mode(lua);
                let load_result: LuaMultiValue =
                    super::runtime::with_lua_host(lua, ctx, dispatcher, || {
                        pcall_fn.call(load_chunk)
                    })
                    .map_err(|error| {
                        if ctx.is_killed() {
                            ScriptError::KillError(ctx.kill_message())
                        } else {
                            ScriptError::Runtime(format!(
                                "Failed to execute function body: {error}"
                            ))
                        }
                    })?;
                let _ = crate::engine::extension::lua::sandbox::exit_compile_mode(lua);
                let mut load_iter = load_result.iter();
                if !matches!(load_iter.next(), Some(LuaValue::Boolean(true))) {
                    let err_obj = load_iter.next().cloned().unwrap_or(LuaValue::Nil);
                    return Err(ScriptError::Runtime(super::lua_value_to_err_string(
                        &err_obj,
                    )));
                }
            }

            let lua_func: LuaFunction =
                if let Ok(entry) = reg_table.get::<LuaTable>(func.name.as_str()) {
                    entry.get::<LuaFunction>("callback").map_err(|error| {
                        ScriptError::Runtime(format!(
                            "function '{}' entry missing callback: {error}",
                            func.name
                        ))
                    })?
                } else {
                    reg_table
                        .get::<LuaFunction>(func.name.as_str())
                        .map_err(|error| {
                            ScriptError::Runtime(format!(
                                "function '{}' not registered: {error}",
                                func.name
                            ))
                        })?
                };

            let keys_table = lua
                .create_table()
                .map_err(|e| ScriptError::Runtime(e.to_string()))?;
            for (index, key) in keys.iter().enumerate() {
                keys_table
                    .set(index + 1, String::from_utf8_lossy(key.as_ref()).as_ref())
                    .map_err(|e| ScriptError::Runtime(e.to_string()))?;
            }
            let args_table = lua
                .create_table()
                .map_err(|e| ScriptError::Runtime(e.to_string()))?;
            for (index, arg) in args.iter().enumerate() {
                args_table
                    .set(index + 1, String::from_utf8_lossy(arg.as_ref()).as_ref())
                    .map_err(|e| ScriptError::Runtime(e.to_string()))?;
            }
            lua.globals()
                .raw_set("KEYS", keys_table.clone())
                .map_err(|e| ScriptError::Runtime(e.to_string()))?;
            lua.globals()
                .raw_set("ARGV", args_table.clone())
                .map_err(|e| ScriptError::Runtime(e.to_string()))?;

            let call_result: LuaMultiValue =
                super::runtime::with_lua_host(lua, ctx, dispatcher, || {
                    pcall_fn.call((lua_func, keys_table.clone(), args_table.clone()))
                })
                .map_err(|error| {
                    if ctx.is_killed() {
                        ScriptError::KillError(ctx.kill_message())
                    } else {
                        ScriptError::Runtime(error.to_string())
                    }
                })?;
            let mut iter = call_result.iter();
            if matches!(iter.next(), Some(LuaValue::Boolean(true))) {
                if ctx.is_killed() {
                    return Err(ScriptError::KillError(ctx.kill_message()));
                }
                let result = iter.next().cloned().unwrap_or(LuaValue::Nil);
                Ok(convert::lua_value_to_value_resp(&result, ctx.resp))
            } else {
                if ctx.is_killed() {
                    return Err(ScriptError::KillError(ctx.kill_message()));
                }
                let err_obj = iter.next().cloned().unwrap_or(LuaValue::Nil);
                Err(ScriptError::Runtime(super::lua_value_to_err_string(
                    &err_obj,
                )))
            }
        });
        if matches!(result, Err(ScriptError::KillError(_))) {
            super::runtime::reset_lua_runtime();
        }
        result
    }
}

pub(crate) struct EvalPreconditions {
    pub flags: ScriptFlags,
    pub has_shebang: bool,
    pub has_maxmemory_limit: bool,
    pub has_enough_replicas_to_write: bool,
    pub is_readonly_replica: bool,
    pub is_stale_replica: bool,
    pub has_persistence_write_error: bool,
    pub readonly_error_style: ReadonlyErrorStyle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadonlyErrorStyle {
    Legacy,
    Valkey,
}

pub(crate) fn check_eval_preconditions(
    preconditions: EvalPreconditions,
) -> Result<(), ScriptError> {
    if preconditions.is_stale_replica
        && preconditions.flags & crate::engine::extension::script::SCRIPT_ALLOW_STALE == 0
    {
        return Err(ScriptError::Runtime(
            "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
                .to_string(),
        ));
    }
    if !preconditions.has_shebang {
        return Ok(());
    }
    if preconditions.flags & SCRIPT_READ_ONLY != 0 {
        return Ok(());
    }
    if preconditions.is_readonly_replica {
        let message = match preconditions.readonly_error_style {
            ReadonlyErrorStyle::Legacy => "READONLY You can't write against a read only replica.",
            ReadonlyErrorStyle::Valkey => {
                "READONLY Can not run script with write flag on readonly replica."
            }
        };
        return Err(ScriptError::Runtime(message.to_string()));
    }
    if preconditions.has_persistence_write_error {
        return Err(ScriptError::Runtime(
            "MISCONF Valkey is configured to save RDB snapshots, but it's currently unable to persist to disk. Writable scripts are blocked. Use 'no-writes' flag for read only scripts.".to_string(),
        ));
    }
    if !preconditions.has_enough_replicas_to_write {
        return Err(ScriptError::Runtime(
            "NOREPLICAS Not enough good replicas to write.".to_string(),
        ));
    }
    if preconditions.flags & SCRIPT_ALLOW_OOM == 0 && preconditions.has_maxmemory_limit {
        return Err(ScriptError::Runtime(
            "OOM command not allowed when used memory > 'maxmemory'.".to_string(),
        ));
    }
    Ok(())
}

impl ScriptEngine for LuaEngine {
    fn name(&self) -> &'static str {
        "LUA"
    }

    fn eval(
        &self,
        code: &str,
        ctx: &mut ScriptRunCtx,
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        LuaEngine::eval_with_host(self, code, ctx, dispatcher)
    }

    fn compile(
        &self,
        code: &str,
        flags: ScriptFlags,
    ) -> Result<Vec<CompiledFunction>, ScriptError> {
        let (body, func_flags, _) = parse_shebang(code, flags)?;
        let result = with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            let pcall_fn: LuaFunction = lua
                .globals()
                .get("pcall")
                .map_err(|e| ScriptError::Runtime(format!("Failed to get pcall: {}", e)))?;

            let wrapped = format!("return function(...)\n{}\nend", body);

            let old_table = lua
                .globals()
                .raw_get::<LuaTable>(super::REGISTERED_FUNCS_TABLE)
                .ok();
            let new_table = lua.create_table().ok();
            if let Some(ref nt) = new_table {
                let _ = lua
                    .globals()
                    .raw_set(super::REGISTERED_FUNCS_TABLE, nt.clone());
            }

            crate::engine::extension::lua::sandbox::enter_compile_mode(lua)?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            crate::engine::extension::lua::timeout::COMPILE_START_MS.with(|c| c.set(now));
            crate::engine::extension::lua::timeout::COMPILE_TIMEOUT_MS.with(|c| c.set(500));

            let compile_result = (|| -> Result<Vec<CompiledFunction>, ScriptError> {
                if !runtime.compiled_scripts.contains_key(&wrapped) {
                    let chunk_fn: LuaFunction =
                        match lua.load(&wrapped).set_name("@user_script").eval() {
                            Ok(f) => f,
                            Err(e) => {
                                return Err(ScriptError::Compile(e.to_string()));
                            }
                        };
                    let key = lua
                        .create_registry_value(chunk_fn)
                        .map_err(|e| ScriptError::Compile(e.to_string()))?;
                    runtime.compiled_scripts.insert(wrapped.clone(), key);
                }
                let key = runtime.compiled_scripts.get(&wrapped).unwrap();
                let chunk_fn: LuaFunction = lua
                    .registry_value(key)
                    .map_err(|e| ScriptError::Compile(e.to_string()))?;
                let pcall_result: LuaMultiValue = pcall_fn
                    .call(chunk_fn)
                    .map_err(|e| ScriptError::Compile(e.to_string()))?;
                let mut iter = pcall_result.iter();
                let success: bool = match iter.next() {
                    Some(LuaValue::Boolean(b)) => *b,
                    _ => false,
                };
                if !success {
                    let err_obj = iter.next().cloned().unwrap_or(LuaValue::Nil);
                    let err_msg = super::lua_value_to_err_string(&err_obj);
                    return Err(ScriptError::Compile(err_msg));
                }

                let globals = lua.globals();
                let funcs = globals.raw_get::<LuaTable>(super::REGISTERED_FUNCS_TABLE);
                if let Ok(table) = funcs {
                    let mut result = Vec::new();
                    for pair in table.pairs::<LuaString, LuaTable>() {
                        let (name_val, entry) =
                            pair.map_err(|e| ScriptError::Compile(e.to_string()))?;
                        let func_name = name_val
                            .to_str()
                            .map_err(|e| ScriptError::Compile(e.to_string()))?
                            .to_string();
                        let _callback: LuaFunction = entry.get("callback").map_err(|e| {
                            ScriptError::Compile(format!(
                                "missing callback for '{}': {}",
                                func_name, e
                            ))
                        })?;
                        let per_func_flags_str: String = entry.get("flags").unwrap_or_default();
                        let desc_str: String = entry.get("description").unwrap_or_default();
                        let mut per_func_flags: ScriptFlags = 0;
                        for flag in per_func_flags_str.split(',') {
                            let flag = flag.trim();
                            if flag == "no-writes" {
                                per_func_flags |= SCRIPT_NO_WRITES;
                            } else if flag == "allow-oom" {
                                per_func_flags |= SCRIPT_ALLOW_OOM;
                            } else if flag == "allow-stale" {
                                per_func_flags |= SCRIPT_ALLOW_STALE;
                            } else if flag == "allow-cross-slot-keys" {
                                per_func_flags |= SCRIPT_ALLOW_CROSS_SLOT;
                            } else if flag == "no-cluster" {
                                per_func_flags |= SCRIPT_NO_CLUSTER;
                            }
                        }
                        let combined_flags = func_flags | per_func_flags;
                        result.push(CompiledFunction {
                            engine: self.name(),
                            name: func_name,
                            code: code.to_string(),
                            description: desc_str,
                            flags: combined_flags,
                        });
                    }
                    if !result.is_empty() {
                        return Ok(result);
                    }
                }

                Err(ScriptError::Compile("No functions registered".to_string()))
            })();

            let _ = crate::engine::extension::lua::sandbox::exit_compile_mode(lua);
            crate::engine::extension::lua::timeout::COMPILE_TIMEOUT_MS.with(|c| c.set(0));

            let new_entries: Vec<(String, LuaTable)> = new_table
                .as_ref()
                .map(|nt| {
                    nt.pairs::<LuaString, LuaTable>()
                        .filter_map(|r| r.ok())
                        .map(|(k, v)| (k.to_str().map(|s| s.to_string()).unwrap_or_default(), v))
                        .collect()
                })
                .unwrap_or_default();

            if let Some(ref old) = old_table {
                let _ = lua
                    .globals()
                    .raw_set(super::REGISTERED_FUNCS_TABLE, old.clone());
            }

            if let Ok(compiled) = &compile_result {
                if !compiled.is_empty() {
                    if lua
                        .globals()
                        .raw_get::<LuaTable>(super::REGISTERED_FUNCS_PENDING_TABLE)
                        .is_ok()
                    {
                        let _ = lua
                            .globals()
                            .raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil);
                    }
                    if let Ok(new_pending) = lua.create_table() {
                        for (name, entry) in &new_entries {
                            let _ = new_pending.raw_set(name.as_str(), entry.clone());
                        }
                        let _ = lua
                            .globals()
                            .raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, new_pending);
                    }
                }
            }

            compile_result
        });
        result
    }

    fn commit_compile(&self, _code: &str, _flags: ScriptFlags) -> Result<(), ScriptError> {
        with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            let pending: Option<LuaTable> = lua
                .globals()
                .raw_get(super::REGISTERED_FUNCS_PENDING_TABLE)
                .ok();
            if let Some(pending) = pending {
                if let Ok(table) = lua
                    .globals()
                    .raw_get::<LuaTable>(super::REGISTERED_FUNCS_TABLE)
                {
                    for (name, entry) in pending.pairs::<LuaString, LuaTable>().flatten() {
                        let _ = table.raw_set(name, entry);
                    }
                }
                let _ = lua
                    .globals()
                    .raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil);
            }
            Ok(())
        })
    }

    fn clear_functions(&self) {
        super::runtime::clear_function_tables();
    }

    fn call(
        &self,
        func: &CompiledFunction,
        ctx: &mut ScriptRunCtx,
        keys: &[bytes::Bytes],
        args: &[bytes::Bytes],
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        LuaEngine::call_with_host(self, func, ctx, keys, args, dispatcher)
    }
}
