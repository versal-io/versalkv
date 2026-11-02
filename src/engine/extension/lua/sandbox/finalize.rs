use mlua::prelude::*;

use crate::engine::extension::script::ScriptError;

fn wrap_redis_functions(lua: &Lua) -> Result<(), ScriptError> {
    let wrap_code = r#"
        local _unpack = table.unpack or unpack
        local redis = rawget(_G, 'redis')
        if type(redis) == 'table' then
            for k, v in pairs(redis) do
                if type(v) == 'function' then
                    redis[k] = function(...)
                        local results = {v(...)}
                        if type(results[1]) == 'table' and results[1].__err_raise then
                            error({err = results[1].err})
                        end
                        return _unpack(results)
                    end
                end
            end
        end
        local function _is_script_kill_error(err)
            local err_type = type(err)
            if err_type == 'table' then
                err = err.err
                err_type = type(err)
            end
            if err_type ~= 'string' then
                err = tostring(err)
            end
            return string.find(err, 'Script killed by user with SCRIPT KILL%.', 1) ~= nil
                or string.find(err, 'Script killed by user with FUNCTION KILL%.', 1) ~= nil
        end

        local _orig_pcall = pcall
        pcall = function(fn, ...)
            local results = {_orig_pcall(fn, ...)}
            if not results[1] and _is_script_kill_error(results[2]) then
                error(results[2])
            elseif not results[1] and type(results[2]) == 'table' then
                local err_field = results[2].err
                if type(err_field) == 'string' then
                    results[2] = err_field
                end
            end
            return _unpack(results)
        end
    "#;
    lua.load(wrap_code)
        .exec()
        .map_err(|e| ScriptError::Runtime(format!("Failed to wrap redis functions: {}", e)))?;
    Ok(())
}

fn make_readonly_proxy(lua: &Lua, real_table: LuaTable) -> Result<LuaTable, ScriptError> {
    let proxy = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let meta = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let real_clone = real_table.clone();
    let idx = lua
        .create_function(move |_lua, (_, key): (LuaTable, LuaValue)| {
            real_clone.get::<LuaValue>(key)
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    meta.set("__index", idx)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let newidx = lua
        .create_function(|_lua, (_t, _k, _v): (LuaTable, LuaValue, LuaValue)| {
            Err::<(), _>(mlua::Error::external("Attempt to modify a readonly table"))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    meta.set("__newindex", newidx)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    meta.set("__metatable", "readonly table")
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    proxy
        .set_metatable(Some(meta))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    Ok(proxy)
}

pub(in crate::engine::extension::lua) fn finalize_sandbox(lua: &Lua) -> Result<(), ScriptError> {
    let globals = lua.globals();
    if globals
        .get::<LuaValue>(super::super::REGISTERED_FUNCS_TABLE)
        .is_err()
    {
        let empty = lua
            .create_table()
            .map_err(|e| ScriptError::Runtime(e.to_string()))?;
        globals
            .raw_set(super::super::REGISTERED_FUNCS_TABLE, empty)
            .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    }

    wrap_redis_functions(lua)?;

    let redis_table: LuaTable = globals
        .get("redis")
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    lua.set_named_registry_value(
        super::super::runtime::REDIS_RUNTIME_TABLE_REGISTRY_KEY,
        redis_table.clone(),
    )
    .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let redis_proxy = make_readonly_proxy(lua, redis_table)?;
    globals
        .set("redis", redis_proxy.clone())
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    globals
        .set("server", redis_proxy)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    if let Ok(cjson) = globals.get::<LuaTable>("cjson") {
        let proxy = make_readonly_proxy(lua, cjson)?;
        globals
            .set("cjson", proxy)
            .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    }
    if let Ok(cmsgpack) = globals.get::<LuaTable>("cmsgpack") {
        let proxy = make_readonly_proxy(lua, cmsgpack)?;
        globals
            .set("cmsgpack", proxy)
            .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    }
    if let Ok(bit) = globals.get::<LuaTable>("bit") {
        let proxy = make_readonly_proxy(lua, bit)?;
        globals
            .set("bit", proxy)
            .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    }

    let save_code = r#"
        local _G = _G
        local rawget, rawset, pairs, ipairs, error, tostring, type =
            rawget, rawset, pairs, ipairs, error, tostring, type
        local _orig_setmetatable = setmetatable
        local _orig_getmetatable = getmetatable
        local _orig_pairs = pairs

        rawset(_G, 'print', nil)

        local saved = {}
        for k, v in _orig_pairs(_G) do
            saved[k] = v
        end

        local function readonly_err()
            error("Attempt to modify a readonly table")
        end

        local function make_proxy(real)
            local proxy = {}
            _orig_setmetatable(proxy, {
                __index = real,
                __newindex = readonly_err,
                __metatable = false,
            })
            return proxy
        end

        local protected_tables = {}
        local function protect(t)
            if t ~= nil then protected_tables[t] = true end
        end

        local meta = {}
        meta.__newindex = function(t, k, v)
            if k == 'KEYS' or k == 'ARGV' then
                rawset(t, k, v)
                return
            end
            readonly_err()
        end
        meta.__index = function(t, k)
            if k == '_G' then return t end
            local v = rawget(t, k)
            if v ~= nil then return v end
            local saved = rawget(_G, '__vky_saved')
            if saved == nil then return nil end
            v = saved[k]
            if v ~= nil then return v end
            error("Script attempted to access nonexistent global variable '" .. tostring(k) .. "'")
        end

        for k, _ in _orig_pairs(_G) do
            rawset(_G, k, nil)
        end

        _orig_setmetatable(_G, meta)
        protect(_G)
        protect(saved)
        protect(saved.redis)
        protect(saved.server)
        protect(saved.cjson)
        protect(saved.cmsgpack)
        protect(saved.bit)

        rawset(_G, '__vky_saved', saved)

        local lib_api = {}
        local lib_api_meta = {
            __index = function(t, k)
                if k == 'server' then
                    return rawget(t, 'server')
                end
                if k == 'redis' then
                    return rawget(t, 'redis')
                end
                local safe = {
                    type=true, error=true, tostring=true, tonumber=true,
                    pairs=true, ipairs=true, next=true, select=true, unpack=true,
                    pcall=true, xpcall=true, print=true, _VERSION=true,
                    assert=true, rawget=true, rawset=true, rawequal=true,
                    load=true, loadstring=true,
                    string=true, table=true, coroutine=true, _G=true,
                }
                if safe[k] then
                    return saved[k]
                end
                error("Script attempted to access nonexistent global variable '" .. tostring(k) .. "'")
            end,
            __newindex = function(t, k, v)
                if k == 'server' then
                    rawset(t, k, v)
                elseif k == 'redis' then
                    rawset(t, k, v)
                else
                    error("Attempt to modify a readonly table")
                end
            end,
            __metatable = false,
        }
        _orig_setmetatable(lib_api, lib_api_meta)
        lib_api.server = {}
        lib_api.server.register_function = saved['__vky_reg_fn']
        local lib_redis_saved = {}
        lib_redis_saved.register_function = saved['__vky_reg_fn']
        lib_redis_saved.REDIS_VERSION_NUM = saved['redis'].REDIS_VERSION_NUM
        lib_redis_saved.REDIS_VERSION = saved['redis'].REDIS_VERSION
        for _, name in _orig_pairs({'log', 'LOG_DEBUG', 'LOG_VERBOSE', 'LOG_NOTICE', 'LOG_WARNING'}) do
            lib_api.server[name] = saved.server[name]
            lib_redis_saved[name] = saved.redis[name]
        end
        local lib_redis_proxy = {}
        local lib_redis_proxy_meta = {
            __index = function(t, k)
                if lib_redis_saved[k] ~= nil then
                    return lib_redis_saved[k]
                end
                error("Script attempted to access nonexistent global variable '" .. tostring(k) .. "'")
            end,
            __newindex = function(t, k, v)
                error("Attempt to modify a readonly table")
            end,
        }
        _orig_setmetatable(lib_redis_proxy, lib_redis_proxy_meta)
        lib_api.redis = lib_redis_proxy
        rawset(_G, '__vky_lib_api', lib_api)
        rawset(_G, '__vky_saved_runtime', saved)
        rawset(_G, '__vky_saved_runtime', saved)

        saved.rawset = function(t, k, v)
            if protected_tables[t] then readonly_err() end
            return rawset(t, k, v)
        end

        saved.setmetatable = function(t, mt)
            if protected_tables[t] then readonly_err() end
            return _orig_setmetatable(t, mt)
        end

        saved.getmetatable = function(t)
            if protected_tables[t] then
                local mt = _orig_getmetatable(t)
                if mt ~= nil then
                    return make_proxy(mt)
                end
                return nil
            end
            if type(t) ~= 'table' then
                local mt = _orig_getmetatable(t)
                if type(mt) == 'table' then
                    return make_proxy(mt)
                end
                return mt
            end
            return _orig_getmetatable(t)
        end
    "#;
    lua.load(save_code)
        .exec()
        .map_err(|e| ScriptError::Runtime(format!("Failed to finalize sandbox: {}", e)))?;

    Ok(())
}
