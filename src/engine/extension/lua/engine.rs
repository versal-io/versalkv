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

const FUNCTION_RESTORE_BACKUP_REGISTRY_KEY: &str = "__valkey_function_restore_backup_v1";

fn collect_function_garbage(lua: &Lua) {
    // mlua can require a second cycle when deletion occurs during an active GC cycle.
    for _ in 0..2 {
        if let Err(error) = lua.gc_collect() {
            log::warn!("Failed to collect Lua function garbage: {error}");
        }
    }
}

fn combine_compile_mode_result<T>(
    operation_result: Result<T, ScriptError>,
    exit_result: Result<(), ScriptError>,
) -> Result<T, ScriptError> {
    match (operation_result, exit_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(operation_error), Err(exit_error)) => Err(ScriptError::Runtime(format!(
            "{operation_error}; failed to exit compile mode: {exit_error}"
        ))),
    }
}

impl LuaEngine {
    pub(crate) fn prepare_cached_eval(
        &self,
        digest: &str,
        code: &str,
        enable_insecure_api: bool,
    ) -> Result<(), ScriptError> {
        with_lua_runtime(enable_insecure_api, |runtime| {
            if runtime.compiled_eval_scripts.contains_key(digest) {
                return Ok(());
            }
            let (mut body, _, has_shebang) = parse_shebang(code, 0)?;
            if has_shebang {
                // Valkey removes the shebang text but retains its newline.
                body.insert(0, '\n');
            }
            let function: LuaFunction = runtime
                .lua
                .load(&body)
                .set_name("@user_script")
                .into_function()
                .map_err(|error| ScriptError::Compile(error.to_string()))?;
            let key = runtime
                .lua
                .create_registry_value(function)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            runtime
                .compiled_eval_scripts
                .insert(digest.to_string(), key);
            Ok(())
        })
    }

    pub fn eval_with_host(
        &self,
        code: &str,
        ctx: &mut ScriptRunCtx,
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        self.eval_with_identity(None, code, ctx, None, dispatcher)
    }

    pub(crate) fn eval_cached_with_host(
        &self,
        code: &str,
        ctx: &mut ScriptRunCtx,
        keys: &[bytes::Bytes],
        args: &[bytes::Bytes],
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        if ctx.func_name.is_empty() {
            with_lua_runtime(ctx.lua_enable_insecure_api, |runtime| {
                super::runtime::bind_eval_arguments(&runtime.lua, keys, args)
                    .map_err(|error| ScriptError::Runtime(error.to_string()))
            })?;
            return self.eval(code, ctx, dispatcher);
        }
        let digest = ctx.func_name.clone();
        self.eval_with_identity(Some(&digest), code, ctx, Some((keys, args)), dispatcher)
    }

    pub(crate) fn eval_cached_with_bound_globals(
        &self,
        code: &str,
        ctx: &mut ScriptRunCtx,
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        if ctx.func_name.is_empty() {
            return self.eval(code, ctx, dispatcher);
        }
        let digest = ctx.func_name.clone();
        self.eval_with_identity(Some(&digest), code, ctx, None, dispatcher)
    }

    fn eval_with_identity(
        &self,
        digest: Option<&str>,
        code: &str,
        ctx: &mut ScriptRunCtx,
        arguments: Option<(&[bytes::Bytes], &[bytes::Bytes])>,
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError> {
        let _time_guard = crate::foundation::io::sys::time::enter_command_time_snapshot(
            crate::foundation::io::sys::time::mstime(),
        );
        let (body, flags, _has_shebang) = parse_shebang(code, ctx.flags)?;
        ctx.flags = flags;

        let wrapped = if digest.is_none() {
            format!("return function(...)\n{}\nend", body)
        } else {
            String::new()
        };
        if let Some(digest) = digest {
            self.prepare_cached_eval(digest, code, ctx.lua_enable_insecure_api)?;
        }
        let result = with_lua_runtime(ctx.lua_enable_insecure_api, |runtime| {
            let lua = &runtime.lua;
            let pcall_fn: LuaFunction = lua
                .globals()
                .get("pcall")
                .map_err(|e| ScriptError::Runtime(format!("Failed to get pcall: {}", e)))?;

            if digest.is_none() && !runtime.compiled_scripts.contains_key(&wrapped) {
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

            let key = match digest {
                Some(digest) => runtime.compiled_eval_scripts.get(digest),
                None => runtime.compiled_scripts.get(&wrapped),
            }
            .ok_or_else(|| ScriptError::Runtime("Compiled script is unavailable".to_string()))?;
            let chunk_fn: LuaFunction = lua
                .registry_value(key)
                .map_err(|e| ScriptError::Runtime(e.to_string()))?;
            if let Some((keys, args)) = arguments {
                super::runtime::bind_eval_arguments(lua, keys, args)
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            }

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
                let mut err_msg = super::lua_value_to_err_string(&err_obj);
                if digest.is_some() && err_msg.trim().starts_with("user_script:") {
                    // Raw EVAL chunks need no legacy wrapper-line adjustment.
                    err_msg = format!("ERR {}", err_msg.trim());
                }
                Err(ScriptError::Runtime(super::format_top_level_lua_error(
                    &err_msg,
                    if digest.is_some() { code } else { &body },
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
            let mut reg_table = match globals
                .raw_get::<Option<LuaTable>>(super::REGISTERED_FUNCS_TABLE)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?
            {
                Some(table) => table,
                None => {
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

            let lua_func: LuaFunction = if has_callback {
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
                }
            } else {
                let load_wrapped = format!("return function(...)\n{}\nend", body);
                let load_chunk: LuaFunction = lua
                    .load(&load_wrapped)
                    .set_name("@user_script")
                    .eval()
                    .map_err(|error| {
                        ScriptError::Runtime(format!("Failed to load function body: {error}"))
                    })?;
                let temporary = lua
                    .create_table()
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                if let Err(switch_error) =
                    globals.raw_set(super::REGISTERED_FUNCS_TABLE, temporary.clone())
                {
                    return match globals.raw_set(super::REGISTERED_FUNCS_TABLE, reg_table.clone()) {
                        Ok(()) => Err(ScriptError::Runtime(format!(
                            "Failed to activate the temporary function table: {switch_error}"
                        ))),
                        Err(restore_error) => Err(ScriptError::Runtime(format!(
                            "Failed to activate the temporary function table: {switch_error}; \
                             failed to restore the active function table: {restore_error}"
                        ))),
                    };
                }

                let load_result =
                    match crate::engine::extension::lua::sandbox::enter_compile_mode(lua) {
                        Ok(()) => {
                            let run_result =
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
                                });
                            let exit_result =
                                crate::engine::extension::lua::sandbox::exit_compile_mode(lua);
                            combine_compile_mode_result(run_result, exit_result)
                        }
                        Err(error) => Err(error),
                    };
                let restore_result =
                    globals.raw_set(super::REGISTERED_FUNCS_TABLE, reg_table.clone());
                let load_result: LuaMultiValue = match (load_result, restore_result) {
                    (Ok(result), Ok(())) => result,
                    (Err(error), Ok(())) => return Err(error),
                    (Ok(_), Err(restore_error)) => {
                        return Err(ScriptError::Runtime(format!(
                            "Failed to restore the active function table: {restore_error}"
                        )));
                    }
                    (Err(error), Err(restore_error)) => {
                        return Err(ScriptError::Runtime(format!(
                            "{error}; failed to restore the active function table: {restore_error}"
                        )));
                    }
                };
                let mut load_iter = load_result.iter();
                if !matches!(load_iter.next(), Some(LuaValue::Boolean(true))) {
                    let err_obj = load_iter.next().cloned().unwrap_or(LuaValue::Nil);
                    return Err(ScriptError::Runtime(super::lua_value_to_err_string(
                        &err_obj,
                    )));
                }

                let working = lua
                    .create_table()
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                for pair in reg_table.clone().pairs::<LuaValue, LuaValue>() {
                    let (key, value) =
                        pair.map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    working
                        .raw_set(key, value)
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                }
                for pair in temporary.pairs::<LuaString, LuaTable>() {
                    let (name, entry) =
                        pair.map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    let name = name
                        .to_str()
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    working
                        .raw_set(name.as_ref(), entry)
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                }
                let active = reg_table;
                reg_table = working;
                let callback = if let Ok(entry) = reg_table.get::<LuaTable>(func.name.as_str()) {
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
                if let Err(publish_error) =
                    globals.raw_set(super::REGISTERED_FUNCS_TABLE, reg_table.clone())
                {
                    return match globals.raw_set(super::REGISTERED_FUNCS_TABLE, active) {
                        Ok(()) => Err(ScriptError::Runtime(publish_error.to_string())),
                        Err(restore_error) => Err(ScriptError::Runtime(format!(
                            "Failed to publish self-healed functions: {publish_error}; \
                             failed to restore the active function table: {restore_error}"
                        ))),
                    };
                }
                callback
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
        with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            let globals = lua.globals();
            globals
                .raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil)
                .map_err(|error| ScriptError::Compile(error.to_string()))?;
            let pcall_fn: LuaFunction = globals
                .get("pcall")
                .map_err(|e| ScriptError::Runtime(format!("Failed to get pcall: {}", e)))?;

            let wrapped = format!("return function(...)\n{}\nend", body);

            let old_table = globals
                .raw_get::<Option<LuaTable>>(super::REGISTERED_FUNCS_TABLE)
                .map_err(|error| ScriptError::Compile(error.to_string()))?;
            let old_table = match old_table {
                Some(old_table) => old_table,
                None => {
                    let old_table = lua
                        .create_table()
                        .map_err(|error| ScriptError::Compile(error.to_string()))?;
                    globals
                        .raw_set(super::REGISTERED_FUNCS_TABLE, old_table.clone())
                        .map_err(|error| ScriptError::Compile(error.to_string()))?;
                    old_table
                }
            };
            let new_table = lua
                .create_table()
                .map_err(|error| ScriptError::Compile(error.to_string()))?;
            if let Err(switch_error) =
                globals.raw_set(super::REGISTERED_FUNCS_TABLE, new_table.clone())
            {
                let restore_result = globals.raw_set(super::REGISTERED_FUNCS_TABLE, old_table);
                return match restore_result {
                    Ok(()) => Err(ScriptError::Compile(switch_error.to_string())),
                    Err(restore_error) => Err(ScriptError::Runtime(format!(
                        "Failed to activate the temporary function table: {switch_error}; \
                         failed to restore the active function table: {restore_error}"
                    ))),
                };
            }

            let compile_result =
                match crate::engine::extension::lua::sandbox::enter_compile_mode(lua) {
                    Ok(()) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        crate::engine::extension::lua::timeout::COMPILE_START_MS
                            .with(|c| c.set(now));
                        crate::engine::extension::lua::timeout::COMPILE_TIMEOUT_MS
                            .with(|c| c.set(500));

                        let result = (|| -> Result<
                            (Vec<CompiledFunction>, Vec<(String, LuaTable)>),
                            ScriptError,
                        > {
                            let chunk_fn: LuaFunction = lua
                                .load(&wrapped)
                                .set_name("@user_script")
                                .eval()
                                .map_err(|error| ScriptError::Compile(error.to_string()))?;
                            let pcall_result: LuaMultiValue = pcall_fn
                                .call(chunk_fn)
                                .map_err(|error| ScriptError::Compile(error.to_string()))?;
                            let mut iter = pcall_result.iter();
                            let success =
                                matches!(iter.next(), Some(LuaValue::Boolean(true)));
                            if !success {
                                let error = iter.next().cloned().unwrap_or(LuaValue::Nil);
                                return Err(ScriptError::Compile(
                                    super::lua_value_to_err_string(&error),
                                ));
                            }

                            let mut result = Vec::new();
                            let mut entries = Vec::new();
                            for pair in new_table.clone().pairs::<LuaString, LuaTable>() {
                                let (name_val, entry) =
                                    pair.map_err(|e| ScriptError::Compile(e.to_string()))?;
                                let func_name = name_val
                                    .to_str()
                                    .map_err(|e| ScriptError::Compile(e.to_string()))?
                                    .to_string();
                                let _callback: LuaFunction =
                                    entry.get("callback").map_err(|e| {
                                        ScriptError::Compile(format!(
                                            "missing callback for '{}': {}",
                                            func_name, e
                                        ))
                                    })?;
                                let per_func_flags_str = entry
                                    .get::<Option<String>>("flags")
                                    .map_err(|error| ScriptError::Compile(error.to_string()))?
                                    .unwrap_or_default();
                                let desc_str = entry
                                    .get::<Option<String>>("description")
                                    .map_err(|error| ScriptError::Compile(error.to_string()))?
                                    .unwrap_or_default();
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
                                    name: func_name.clone(),
                                    code: code.to_string(),
                                    description: desc_str,
                                    flags: combined_flags,
                                });
                                entries.push((func_name, entry));
                            }
                            if !result.is_empty() {
                                return Ok((result, entries));
                            }

                            Err(ScriptError::Compile(
                                "No functions registered".to_string(),
                            ))
                        })();
                        let exit_result =
                            crate::engine::extension::lua::sandbox::exit_compile_mode(lua);
                        combine_compile_mode_result(result, exit_result)
                    }
                    Err(error) => Err(error),
                };
            crate::engine::extension::lua::timeout::COMPILE_TIMEOUT_MS.with(|c| c.set(0));

            let staged = compile_result.and_then(|(compiled, entries)| {
                let pending = lua
                    .create_table()
                    .map_err(|error| ScriptError::Compile(error.to_string()))?;
                for (name, entry) in entries {
                    pending
                        .raw_set(name, entry)
                        .map_err(|error| ScriptError::Compile(error.to_string()))?;
                }
                Ok((compiled, pending))
            });

            let restore_result = globals.raw_set(super::REGISTERED_FUNCS_TABLE, old_table);
            if let Err(restore_error) = restore_result {
                return match staged {
                    Ok(_) => Err(ScriptError::Runtime(format!(
                        "Failed to restore the active function table: {restore_error}"
                    ))),
                    Err(error) => Err(ScriptError::Runtime(format!(
                        "{error}; failed to restore the active function table: {restore_error}"
                    ))),
                };
            }

            let (compiled, pending) = staged?;
            if let Err(publish_error) =
                globals.raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, pending)
            {
                return match globals.raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil) {
                    Ok(()) => Err(ScriptError::Compile(publish_error.to_string())),
                    Err(cleanup_error) => Err(ScriptError::Runtime(format!(
                        "Failed to publish pending functions: {publish_error}; \
                         failed to clear pending functions: {cleanup_error}"
                    ))),
                };
            }

            Ok(compiled)
        })
    }

    fn commit_compile(&self, _code: &str, _flags: ScriptFlags) -> Result<(), ScriptError> {
        with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            let globals = lua.globals();
            let pending = match globals
                .raw_get::<Option<LuaTable>>(super::REGISTERED_FUNCS_PENDING_TABLE)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?
            {
                Some(pending) => pending,
                None => return Ok(()),
            };
            let active = globals
                .raw_get::<LuaTable>(super::REGISTERED_FUNCS_TABLE)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;

            // Merge the staged entries directly into the active table in
            // O(pending) instead of cloning the whole active table on every
            // FUNCTION LOAD. Remember each overwritten value so the merge can
            // be rolled back, preserving the atomicity contract that the active
            // table is left untouched on failure.
            let mut applied: Vec<(String, LuaValue)> = Vec::new();
            let merge: LuaResult<()> = (|| {
                for pair in pending.clone().pairs::<LuaString, LuaTable>() {
                    let (name, entry) = pair?;
                    let name = name.to_str()?.to_string();
                    let previous = active.raw_get::<LuaValue>(name.as_str())?;
                    active.raw_set(name.as_str(), entry)?;
                    applied.push((name, previous));
                }
                Ok(())
            })();
            if let Err(error) = merge {
                for (name, previous) in applied.drain(..) {
                    let _ = active.raw_set(name.as_str(), previous);
                }
                return Err(ScriptError::Runtime(error.to_string()));
            }

            if let Err(clear_error) =
                globals.raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil)
            {
                // The merge succeeded but clearing the pending table failed.
                // Restore the active table and keep pending intact for retry.
                for (name, previous) in applied.drain(..) {
                    let _ = active.raw_set(name.as_str(), previous);
                }
                let _ = globals.raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, pending);
                return Err(ScriptError::Runtime(clear_error.to_string()));
            }
            Ok(())
        })
    }

    fn begin_function_restore_transaction(&self) -> Result<(), ScriptError> {
        with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            let transaction_state = lua
                .named_registry_value::<LuaValue>(FUNCTION_RESTORE_BACKUP_REGISTRY_KEY)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            if !matches!(transaction_state, LuaValue::Nil) {
                return Err(ScriptError::Runtime(
                    "Function restore transaction is already active".to_string(),
                ));
            }

            let globals = lua.globals();
            let active = globals
                .raw_get::<Option<LuaTable>>(super::REGISTERED_FUNCS_TABLE)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            let active = match active {
                Some(active) => active,
                None => {
                    let active = lua
                        .create_table()
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    globals
                        .raw_set(super::REGISTERED_FUNCS_TABLE, active.clone())
                        .map_err(|error| ScriptError::Runtime(error.to_string()))?;
                    active
                }
            };
            let working = lua
                .create_table()
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            for pair in active.clone().pairs::<LuaValue, LuaValue>() {
                let (key, value) = pair.map_err(|error| ScriptError::Runtime(error.to_string()))?;
                working
                    .raw_set(key, value)
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            }

            lua.set_named_registry_value(FUNCTION_RESTORE_BACKUP_REGISTRY_KEY, active.clone())
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            if let Err(activate_error) = globals.raw_set(super::REGISTERED_FUNCS_TABLE, working) {
                return match lua.unset_named_registry_value(FUNCTION_RESTORE_BACKUP_REGISTRY_KEY) {
                    Ok(()) => Err(ScriptError::Runtime(activate_error.to_string())),
                    Err(cleanup_error) => Err(ScriptError::Runtime(format!(
                        "Failed to activate the function restore working table: {activate_error}; \
                         failed to clear the function restore backup: {cleanup_error}"
                    ))),
                };
            }
            Ok(())
        })
    }

    fn commit_function_restore_transaction(&self) -> Result<(), ScriptError> {
        with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            lua.globals()
                .raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            lua.unset_named_registry_value(FUNCTION_RESTORE_BACKUP_REGISTRY_KEY)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            // Do not force a full-heap garbage collection on every committed
            // FUNCTION LOAD/RESTORE. The Lua runtime runs incremental GC by
            // itself, and a full collection per command turned bulk loading into
            // repeated full-heap scans. Rollback/removal paths still collect.
            Ok(())
        })
    }

    fn rollback_function_restore_transaction(&self) -> Result<(), ScriptError> {
        with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            let backup = lua
                .named_registry_value::<Option<LuaTable>>(FUNCTION_RESTORE_BACKUP_REGISTRY_KEY)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            let Some(backup) = backup else {
                return Ok(());
            };
            let globals = lua.globals();
            globals
                .raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            lua.unset_named_registry_value(FUNCTION_RESTORE_BACKUP_REGISTRY_KEY)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            if let Err(restore_error) =
                globals.raw_set(super::REGISTERED_FUNCS_TABLE, backup.clone())
            {
                return match lua
                    .set_named_registry_value(FUNCTION_RESTORE_BACKUP_REGISTRY_KEY, backup)
                {
                    Ok(()) => Err(ScriptError::Runtime(format!(
                        "Failed to restore the function callback table: {restore_error}"
                    ))),
                    Err(recovery_error) => Err(ScriptError::Runtime(format!(
                        "Failed to restore the function callback table: {restore_error}; \
                         failed to preserve the function restore backup: {recovery_error}"
                    ))),
                };
            }
            collect_function_garbage(lua);
            Ok(())
        })
    }

    fn clear_functions(&self) {
        super::runtime::clear_function_tables();
    }

    fn discard_pending_compile(&self) {
        let result = with_lua_runtime(false, |runtime| {
            runtime
                .lua
                .globals()
                .raw_set(super::REGISTERED_FUNCS_PENDING_TABLE, LuaValue::Nil)
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        });
        if let Err(error) = result {
            log::warn!("Failed to discard pending Lua functions: {error}");
        }
    }

    fn remove_functions(&self, functions: &[CompiledFunction]) -> Result<(), ScriptError> {
        with_lua_runtime(false, |runtime| {
            let lua = &runtime.lua;
            let table = lua
                .globals()
                .raw_get::<LuaTable>(super::REGISTERED_FUNCS_TABLE)
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            for function in functions {
                table
                    .raw_set(function.name.as_str(), LuaValue::Nil)
                    .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            }
            collect_function_garbage(lua);
            Ok(())
        })
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
