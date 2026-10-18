use mlua::prelude::*;

use crate::engine::extension::script::ScriptError;

macro_rules! real_args {
    ($args:expr) => {{
        let mut _iter = $args.iter();
        let _first = _iter.next();
        match _first {
            Some(LuaValue::Table(_)) => _iter,
            _ => $args.iter(),
        }
    }};
}

pub fn setup_bit_table(lua: &Lua) -> Result<(), ScriptError> {
    let bit = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    macro_rules! bit_fn_1 {
        ($lua:expr, $f:expr) => {{
            $lua.create_function(|_lua, args: LuaMultiValue| {
                let mut iter = real_args!(args);
                let n = match iter.next() {
                    Some(LuaValue::Integer(n)) => *n,
                    Some(LuaValue::Number(n)) => *n as i64,
                    _ => 0,
                };
                Ok(LuaValue::Integer($f(n) & 0xffffffff))
            })
            .map_err(|e| ScriptError::Runtime(e.to_string()))?
        }};
    }

    macro_rules! bit_fn_2 {
        ($lua:expr, $f:expr) => {{
            $lua.create_function(|_lua, args: LuaMultiValue| {
                let mut iter = real_args!(args);
                let a = match iter.next() {
                    Some(LuaValue::Integer(n)) => *n,
                    Some(LuaValue::Number(n)) => *n as i64,
                    _ => 0,
                };
                let b = match iter.next() {
                    Some(LuaValue::Integer(n)) => *n,
                    Some(LuaValue::Number(n)) => *n as i64,
                    _ => 0,
                };
                Ok(LuaValue::Integer($f(a, b) & 0xffffffff))
            })
            .map_err(|e| ScriptError::Runtime(e.to_string()))?
        }};
    }

    macro_rules! bit_fn_var {
        ($lua:expr, $init:expr, $f:expr) => {{
            $lua.create_function(|_lua, args: LuaMultiValue| {
                let iter = real_args!(args);
                let mut result: i64 = $init;
                let mut first = true;
                for arg in iter {
                    let v = match arg {
                        LuaValue::Integer(n) => *n,
                        LuaValue::Number(n) => *n as i64,
                        _ => 0,
                    };
                    if first {
                        result = v;
                        first = false;
                    } else {
                        result = $f(result, v);
                    }
                }
                Ok(LuaValue::Integer(result & 0xffffffff))
            })
            .map_err(|e| ScriptError::Runtime(e.to_string()))?
        }};
    }

    let tobit_fn = bit_fn_1!(lua, |n: i64| n);
    let bnot_fn = bit_fn_1!(lua, |n: i64| !n);
    let band_fn = bit_fn_var!(lua, 0, |a: i64, b: i64| a & b);
    let bor_fn = bit_fn_var!(lua, 0, |a: i64, b: i64| a | b);
    let bxor_fn = bit_fn_var!(lua, 0, |a: i64, b: i64| a ^ b);
    let lshift_fn = bit_fn_2!(lua, |a: i64, b: i64| a << (b & 31));
    let rshift_fn = bit_fn_2!(lua, |a: i64, b: i64| (a as u32 >> (b & 31)) as i64);
    let arshift_fn = bit_fn_2!(lua, |a: i64, b: i64| (a as i32 >> (b & 31)) as u32 as i64);

    let tohex_fn = lua
        .create_function(|_lua, args: LuaMultiValue| {
            let mut iter = real_args!(args);
            let n = match iter.next() {
                Some(LuaValue::Integer(n)) => *n,
                Some(LuaValue::Number(n)) => *n as i64,
                _ => 0,
            };
            let n = n & 0xffffffff;
            let hex = format!("{:08X}", n as u32);
            Ok(LuaValue::String(_lua.create_string(&hex)?))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let bswap_fn = bit_fn_1!(lua, |n: i64| (n as u32).swap_bytes() as i64);

    let rol_fn = bit_fn_2!(lua, |a: i64, b: i64| a.rotate_left((b & 31) as u32));
    let ror_fn = bit_fn_2!(lua, |a: i64, b: i64| a.rotate_right((b & 31) as u32));

    bit.set("tobit", tobit_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("tohex", tohex_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("bnot", bnot_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("band", band_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("bor", bor_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("bxor", bxor_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("lshift", lshift_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("rshift", rshift_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("arshift", arshift_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("bswap", bswap_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("rol", rol_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    bit.set("ror", ror_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    lua.globals()
        .set("bit", bit)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    Ok(())
}
