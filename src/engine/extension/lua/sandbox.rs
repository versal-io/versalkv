use mlua::prelude::*;

use crate::engine::extension::script::ScriptError;

mod finalize;
mod setup;

pub(super) use finalize::finalize_sandbox;
pub(super) use setup::setup_sandbox;

pub fn enter_compile_mode(lua: &Lua) -> Result<(), ScriptError> {
    let globals = lua.globals();
    let lib_api = globals
        .raw_get::<LuaTable>("__vky_lib_api")
        .map_err(|e| ScriptError::Runtime(format!("Failed to get lib API: {}", e)))?;
    globals
        .raw_set(super::VKY_SAVED_KEY, lib_api)
        .map_err(|e| ScriptError::Runtime(format!("Failed to enter compile mode: {}", e)))?;
    // Set compile mode flag
    let _ = globals.raw_set(super::COMPILE_MODE_FLAG, true);
    Ok(())
}

pub fn exit_compile_mode(lua: &Lua) -> Result<(), ScriptError> {
    let globals = lua.globals();
    // Clear compile mode flag
    let _ = globals.raw_set(super::COMPILE_MODE_FLAG, LuaValue::Nil);
    let saved = globals
        .raw_get::<LuaTable>("__vky_saved_runtime")
        .map_err(|e| ScriptError::Runtime(format!("Failed to get runtime saved: {}", e)))?;
    globals
        .raw_set(super::VKY_SAVED_KEY, saved)
        .map_err(|e| ScriptError::Runtime(format!("Failed to exit compile mode: {}", e)))?;
    Ok(())
}

pub(super) fn setup_redis_table(lua: &Lua) -> Result<(), ScriptError> {
    let redis_table = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let call_fn = lua
        .create_function(move |lua, args: LuaMultiValue| dispatch_scoped_host(lua, 0, args))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let pcall_fn = lua
        .create_function(move |lua, args: LuaMultiValue| dispatch_scoped_host(lua, 1, args))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let log_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut iter = real_args!(args);
            let level: u8 = match iter.next() {
                Some(LuaValue::Integer(n)) => *n as u8,
                Some(LuaValue::Number(n)) => *n as u8,
                None => {
                    return super::make_raise_err(
                        lua,
                        "server.log() requires two arguments or more.",
                    )
                }
                _ => {
                    return super::make_raise_err(
                        lua,
                        "First argument must be a number (log level).",
                    )
                }
            };
            let msg: String = match iter.next() {
                Some(LuaValue::String(s)) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                _ => {
                    return super::make_raise_err(
                        lua,
                        "server.log() requires two arguments or more.",
                    )
                }
            };
            match level {
                0..=3 => {}
                _ => return super::make_raise_err(lua, "Invalid log level."),
            }
            let level_str = match level {
                0 => "debug",
                1 => "verbose",
                2 => "notice",
                _ => "warning",
            };
            eprintln!("[LUA {}] {}", level_str, msg);
            Ok(LuaValue::Boolean(true))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let status_reply_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut iter = real_args!(args);
            let msg: String = match iter.next() {
                Some(LuaValue::String(s)) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                _ => return super::make_raise_err(lua, "status_reply requires a string argument"),
            };
            let table = lua.create_table()?;
            table.set("ok", msg.as_str())?;
            Ok(LuaValue::Table(table))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let error_reply_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut iter = real_args!(args);
            let mut msg: String = match iter.next() {
                Some(LuaValue::String(s)) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                _ => return super::make_raise_err(lua, "error_reply requires a string argument"),
            };
            if msg.is_empty() {
                msg = "ERR".to_string();
            }
            let table = lua.create_table()?;
            table.set("err", msg.as_str())?;
            Ok(LuaValue::Table(table))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let sha1hex_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            use sha1::{Digest, Sha1};
            let mut iter = real_args!(args);
            let data: String = match iter.next() {
                Some(LuaValue::String(s)) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                _ => return super::make_raise_err(lua, "wrong number of arguments"),
            };
            let mut hasher = Sha1::new();
            hasher.update(data.as_bytes());
            let result = hasher.finalize();
            let hex_str = hex::encode(result);
            Ok(LuaValue::String(lua.create_string(&hex_str)?))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let setresp_fn = lua
        .create_function(|lua, args: LuaMultiValue| dispatch_scoped_host(lua, 2, args))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let set_repl_fn = lua
        .create_function(|lua, args: LuaMultiValue| dispatch_scoped_host(lua, 3, args))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    // Nested command effects are already captured individually, so this C-compatible
    // API is an explicit no-op in the Rust propagation model.
    let replicate_commands_fn = lua
        .create_function(|_lua, _args: LuaMultiValue| Ok(LuaValue::Boolean(true)))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let acl_check_cmd_fn = lua
        .create_function(|lua, args: LuaMultiValue| dispatch_scoped_host(lua, 4, args))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let register_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let globals = lua.globals();

            // Check if we're in compile mode via the dedicated flag.
            // During compile mode (FUNCTION LOAD), __vky_compile_mode is set to true.
            // During runtime (function execution), it is nil.
            let in_compile_mode = globals
                .raw_get::<LuaValue>(super::COMPILE_MODE_FLAG)
                .map(|v| matches!(v, LuaValue::Boolean(true)))
                .unwrap_or(false);
            if !in_compile_mode {
                return Err(LuaError::RuntimeError(
                    "server.register_function can only be called on FUNCTION LOAD command".into(),
                ));
            }

            let reg_table: LuaTable = match globals.raw_get(super::REGISTERED_FUNCS_TABLE) {
                Ok(t) => t,
                Err(_) => {
                    let t = lua.create_table()?;
                    globals.raw_set(super::REGISTERED_FUNCS_TABLE, t.clone())?;
                    t
                }
            };

            if args.is_empty() {
                return Err(LuaError::RuntimeError(
                    "wrong number of arguments to server.register_function".into(),
                ));
            }

            if args.len() > 3 {
                return Err(LuaError::RuntimeError(
                    "wrong number of arguments to server.register_function".into(),
                ));
            }

            let (name, func, flags_str, description): (String, LuaFunction, String, String) = if args.len() == 1 {
                let arg = args.front().unwrap();
                let table = match arg {
                    LuaValue::Table(t) => t,
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "calling server.register_function with a single argument is only applicable to Lua table".into(),
                        ));
                    }
                };

                let known_keys = ["function_name", "callback", "description", "flags"];
                for (k, _) in table.pairs::<LuaValue, LuaValue>().flatten() {
                    if let LuaValue::String(key) = k {
                        if let Ok(key_str) = key.to_str() {
                            if known_keys.iter().any(|known| key_str == *known) {
                                continue;
                            }
                            return Err(LuaError::RuntimeError(
                                "unknown argument given to server.register_function".into(),
                            ));
                        }
                    }
                }

                let name_val: LuaValue = table.get("function_name")?;
                let name = match name_val {
                    LuaValue::Nil => {
                        return Err(LuaError::RuntimeError(
                            "server.register_function must get a function name argument".into(),
                        ));
                    }
                    LuaValue::String(s) => s.to_str()?.to_string(),
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "function_name argument given to server.register_function must be a string".into(),
                        ));
                    }
                };

                let callback_val: LuaValue = table.get("callback")?;
                let func = match callback_val {
                    LuaValue::Nil => {
                        return Err(LuaError::RuntimeError(
                            "server.register_function must get a callback argument".into(),
                        ));
                    }
                    LuaValue::Function(f) => f,
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "callback argument given to server.register_function must be a function".into(),
                        ));
                    }
                };

                let desc_str: String = match table.get::<LuaValue>("description") {
                    Ok(LuaValue::String(s)) => s.to_str()?.to_string(),
                    Ok(LuaValue::Nil) => String::new(),
                    Ok(_) => {
                        return Err(LuaError::RuntimeError(
                            "description argument given to server.register_function must be a string".into(),
                        ));
                    }
                    _ => String::new(),
                };

                let flags_str = match table.get::<LuaValue>("flags") {
                    Ok(LuaValue::Table(ft)) => {
                        let mut parts: Vec<String> = Vec::new();
                        let mut valid_flags = true;
                        for v in ft.sequence_values::<LuaValue>().flatten() {
                            match v {
                                LuaValue::String(s) => {
                                    parts.push(s.to_str()?.to_string());
                                }
                                _ => {
                                    valid_flags = false;
                                }
                            }
                        }
                        if !valid_flags {
                            return Err(LuaError::RuntimeError(
                                "unknown flag given".into(),
                            ));
                        }
                        for flag in &parts {
                            let lower = flag.to_lowercase();
                            if lower != "no-writes"
                                && lower != "allow-oom"
                                && lower != "allow-stale"
                                && lower != "allow-cross-slot-keys"
                                && lower != "no-cluster"
                            {
                                return Err(LuaError::RuntimeError(
                                    "unknown flag given".into(),
                                ));
                            }
                        }
                        parts.join(",")
                    }
                    Ok(LuaValue::Nil) => String::new(),
                    Ok(_) => {
                        return Err(LuaError::RuntimeError(
                            "flags argument to server.register_function must be a table representing function flags".into(),
                        ));
                    }
                    _ => String::new(),
                };
                (name, func, flags_str, desc_str)
            } else if args.len() >= 2 {
                let name = match args.front().unwrap() {
                    LuaValue::String(s) => s.to_str()?.to_string(),
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "first argument to server.register_function must be a string".into(),
                        ));
                    }
                };
                if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    return Err(LuaError::RuntimeError(
                        "Function names can only contain letters, numbers, or underscores(_) and must be at least one character long".into(),
                    ));
                }
                let func = match args.get(1).unwrap() {
                    LuaValue::Function(f) => f.clone(),
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "second argument to server.register_function must be a function".into(),
                        ));
                    }
                };
                (name, func, String::new(), String::new())
            } else {
                return Err(LuaError::RuntimeError(
                    "wrong number of arguments to server.register_function".into(),
                ));
            };

            let entry = lua.create_table()?;
            entry.set("callback", func)?;
            entry.set("flags", flags_str.as_str())?;
            if !description.is_empty() {
                let _ = entry.set("description", description.as_str());
            }
            let lower_name = name.to_lowercase();
            for (k, _) in reg_table.pairs::<LuaString, LuaValue>().flatten() {
                if let Ok(k_str) = k.to_str() {
                    if k_str.to_lowercase() == lower_name {
                        return Err(LuaError::RuntimeError(
                            "Function already exists in the library".to_string(),
                        ));
                    }
                }
            }
            reg_table.set(name.as_str(), entry)?;
            Ok(())
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let _ = redis_table.set("LOG_DEBUG", 0u8);
    let _ = redis_table.set("LOG_VERBOSE", 1u8);
    let _ = redis_table.set("LOG_NOTICE", 2u8);
    let _ = redis_table.set("LOG_WARNING", 3u8);

    let _ = redis_table.set("REPL_ALL", "all");
    let _ = redis_table.set("REPL_AOF", "aof");
    let _ = redis_table.set("REPL_REPLICA", "replica");
    let _ = redis_table.set("REPL_SLAVE", "replica");
    let _ = redis_table.set("REPL_NONE", "none");

    let _ = redis_table.set("call", call_fn);
    let _ = redis_table.set("pcall", pcall_fn);
    let _ = redis_table.set("log", log_fn);
    let _ = redis_table.set("status_reply", status_reply_fn);
    let _ = redis_table.set("error_reply", error_reply_fn);
    let _ = redis_table.set("sha1hex", sha1hex_fn);
    let _ = redis_table.set("setresp", setresp_fn);
    let _ = redis_table.set("set_repl", set_repl_fn);
    let _ = redis_table.set("replicate_commands", replicate_commands_fn);
    let _ = redis_table.set("acl_check_cmd", acl_check_cmd_fn);

    let version_num = (7_i64 << 16) | (2 << 8); // 7.2.0
    let _ = redis_table.set("REDIS_VERSION_NUM", version_num);
    let _ = redis_table.set("REDIS_VERSION", "7.2.0");

    let globals = lua.globals();
    globals
        .raw_set("__vky_reg_fn", register_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    lua.globals()
        .set("redis", redis_table)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    Ok(())
}

fn dispatch_scoped_host(lua: &Lua, operation: i64, mut args: LuaMultiValue) -> LuaResult<LuaValue> {
    let callback: LuaFunction = lua
        .named_registry_value(super::runtime::HOST_CALLBACK_REGISTRY_KEY)
        .map_err(|_| mlua::Error::external("Script host dispatcher is not active"))?;
    args.push_front(LuaValue::Integer(operation));
    callback.call(args)
}
