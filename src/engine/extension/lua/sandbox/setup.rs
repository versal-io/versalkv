use mlua::prelude::*;

use crate::engine::extension::script::ScriptError;

pub(in crate::engine::extension::lua) fn setup_sandbox(
    lua: &Lua,
    enable_insecure_api: bool,
) -> Result<(), ScriptError> {
    lua.load("string = require('string'); table = require('table'); math = require('math'); coroutine = require('coroutine')")
        .exec()
        .map_err(|e| ScriptError::Runtime(format!("Failed to setup sandbox: {}", e)))?;

    let sandbox_code = r#"
        local _orig_load = load
        local _orig_loadstring = loadstring
        rawset(_G, 'dofile', nil)
        rawset(_G, 'loadfile', nil)
        rawset(_G, 'require', nil)
        rawset(_G, 'package', nil)
        rawset(_G, 'collectgarbage', nil)
        rawset(_G, 'module', nil)
        rawset(_G, 'io', nil)
        rawset(_G, 'debug', nil)

        do
            local os_clock = os.clock
            local safe_os = {}
            local os_meta = {
                __index = function(t, k)
                    error("attempt to call field '" .. tostring(k) .. "' (a nil value)")
                end,
                __newindex = function(t, k, v)
                    error("Attempt to modify a readonly table")
                end,
                __metatable = "readonly table",
            }
            safe_os.clock = os_clock
            setmetatable(safe_os, os_meta)
            rawset(_G, 'os', safe_os)
        end


        -- Lua 5.1: load(func, chunkname) / loadstring(string, chunkname)
        -- Lua 5.2+: load(chunk, chunkname, mode, env)
        local _string_sub = string.sub
        local _string_char = string.char
        local _binary_sig = _string_char(0x1b) .. 'Lua'
        local function _is_binary(chunk)
            if type(chunk) == 'string' then
                return _string_sub(chunk, 1, 4) == _binary_sig
            end
            return false
        end
        load = function(chunk, chunkname, mode, env)
            if mode ~= nil and string.find(mode, "b") then
                error("attempt to load a binary chunk (mode is 'b')")
            end
            if type(chunk) == 'string' and _is_binary(chunk) then
                return nil, "attempt to load a binary chunk"
            end
            if _VERSION == "Lua 5.1" then
                if type(chunk) == "string" then
                    return _orig_loadstring(chunk, chunkname)
                else
                    return _orig_load(chunk, chunkname)
                end
            end
            return _orig_load(chunk, chunkname, mode, env)
        end
        rawset(_G, 'load', load)
        if _orig_loadstring ~= nil then
            loadstring = load
        end
        rawset(_G, 'loadstring', load)
    "#;
    lua.load(sandbox_code)
        .exec()
        .map_err(|e| ScriptError::Runtime(format!("Failed to configure sandbox: {}", e)))?;
    if enable_insecure_api {
        lua.load("getfenv = function() return {} end")
            .exec()
            .map_err(|e| ScriptError::Runtime(format!("Failed to enable insecure API: {}", e)))?;
    } else {
        lua.load("getfenv = nil; setfenv = nil")
            .exec()
            .map_err(|e| ScriptError::Runtime(format!("Failed to disable insecure API: {}", e)))?;
    }

    let unpack_fn = lua
        .create_function(|_, args: LuaMultiValue| {
            let table = match args.front() {
                Some(LuaValue::Table(t)) => t.clone(),
                _ => {
                    return Err(mlua::Error::external(
                        "bad argument #1 to 'unpack' (table expected)",
                    ))
                }
            };
            let i = match args.get(1) {
                Some(LuaValue::Integer(n)) => *n,
                Some(LuaValue::Number(n)) => *n as i64,
                Some(LuaValue::Nil) | None => 1,
                _ => {
                    return Err(mlua::Error::external(
                        "bad argument #2 to 'unpack' (number expected)",
                    ))
                }
            };
            let j = match args.get(2) {
                Some(LuaValue::Integer(n)) => *n,
                Some(LuaValue::Number(n)) => *n as i64,
                Some(LuaValue::Nil) | None => table.raw_len() as i64,
                _ => {
                    return Err(mlua::Error::external(
                        "bad argument #3 to 'unpack' (number expected)",
                    ))
                }
            };
            if j < i {
                return Ok(LuaMultiValue::new());
            }
            let count = j
                .checked_sub(i)
                .and_then(|n| n.checked_add(1))
                .ok_or_else(|| mlua::Error::external("too many results to unpack"))?;
            if count > 100000 {
                return Err(mlua::Error::external("too many results to unpack"));
            }
            if count > 7000 && args.len() == 1 {
                table.raw_set("__vky_unpack_args", true)?;
                return Ok(LuaMultiValue::from_vec(vec![LuaValue::Table(table)]));
            }
            let mut values = Vec::with_capacity(count as usize);
            for idx in i..=j {
                values.push(table.raw_get::<LuaValue>(idx)?);
            }
            Ok(LuaMultiValue::from_vec(values))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let globals = lua.globals();
    globals
        .raw_set("unpack", unpack_fn.clone())
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let table_lib: LuaTable = globals
        .get("table")
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    table_lib
        .raw_set("unpack", unpack_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    Ok(())
}
