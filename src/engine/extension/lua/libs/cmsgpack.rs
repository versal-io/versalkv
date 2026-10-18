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

pub fn setup_cmsgpack_table(lua: &Lua) -> Result<(), ScriptError> {
    let cmsgpack = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let pack_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut buf = Vec::new();
            for arg in args.iter() {
                pack_msgpack_depth(lua, arg, &mut buf, 0, 16)?;
            }
            Ok(LuaValue::String(lua.create_string(&buf)?))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let unpack_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut iter = real_args!(args);
            let data = match iter.next() {
                Some(LuaValue::String(s)) => s.as_bytes().to_vec(),
                _ => {
                    return Err(mlua::Error::external(
                        "cmsgpack.unpack requires a string argument",
                    ))
                }
            };
            let start_offset: usize = match iter.next() {
                Some(LuaValue::Integer(n)) => (*n).try_into().unwrap_or(0),
                _ => 0,
            };
            let mut pos = start_offset;
            unpack_msgpack(lua, &data, &mut pos)
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let unpack_one_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut iter = real_args!(args);
            let data = match iter.next() {
                Some(LuaValue::String(s)) => s.as_bytes().to_vec(),
                _ => {
                    return Err(mlua::Error::external(
                        "cmsgpack.unpack_one requires a string argument",
                    ))
                }
            };
            let start_offset: usize = match iter.next() {
                Some(LuaValue::Integer(n)) => (*n).try_into().unwrap_or(0),
                _ => 0,
            };
            let mut pos = start_offset;
            if pos > data.len() {
                return Err(mlua::Error::external("Offset out of bounds"));
            }
            let val = if pos == data.len() {
                LuaValue::Nil
            } else {
                unpack_msgpack(lua, &data, &mut pos)?
            };
            let next_offset = if pos >= data.len() {
                LuaValue::Integer(-1)
            } else {
                LuaValue::Integer(pos as i64)
            };
            let multi = LuaMultiValue::from_vec(vec![next_offset, val]);
            Ok(multi)
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let unpack_limit_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let mut iter = real_args!(args);
            let data = match iter.next() {
                Some(LuaValue::String(s)) => s.as_bytes().to_vec(),
                _ => {
                    return Err(mlua::Error::external(
                        "cmsgpack.unpack_limit requires a string argument",
                    ))
                }
            };
            let limit: i64 = match iter.next() {
                Some(LuaValue::Integer(n)) => *n,
                _ => {
                    return Err(mlua::Error::external(
                        "cmsgpack.unpack_limit requires a limit",
                    ))
                }
            };
            let start_offset: usize = match iter.next() {
                Some(LuaValue::Integer(n)) => (*n).try_into().unwrap_or(0),
                _ => 0,
            };
            let mut pos = start_offset;
            let mut results: Vec<LuaValue> = Vec::new();
            if limit != 0 {
                while pos < data.len() && (limit < 0 || (results.len() as i64) < limit) {
                    let val = unpack_msgpack(lua, &data, &mut pos)?;
                    results.push(val);
                    if limit > 0 && (results.len() as i64) >= limit {
                        break;
                    }
                }
            }
            let next_offset = if pos >= data.len() {
                LuaValue::Integer(-1)
            } else {
                LuaValue::Integer(pos as i64)
            };
            let mut ret_vals = vec![next_offset];
            ret_vals.append(&mut results);
            Ok(LuaMultiValue::from_vec(ret_vals))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    cmsgpack
        .set("pack", pack_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cmsgpack
        .set("unpack", unpack_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cmsgpack
        .set("unpack_one", unpack_one_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cmsgpack
        .set("unpack_limit", unpack_limit_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    lua.globals()
        .set("cmsgpack", cmsgpack)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    Ok(())
}

fn pack_msgpack_depth(
    lua: &Lua,
    val: &LuaValue,
    buf: &mut Vec<u8>,
    depth: i32,
    max_depth: i32,
) -> Result<(), mlua::Error> {
    if depth >= max_depth && matches!(val, LuaValue::Table(_)) {
        buf.push(0xc0);
        return Ok(());
    }
    match val {
        LuaValue::Nil => buf.push(0xc0),
        LuaValue::Boolean(b) => buf.push(if *b { 0xc3 } else { 0xc2 }),
        LuaValue::Integer(n) => {
            if *n >= 0 {
                if *n <= 0x7f {
                    buf.push(*n as u8);
                } else if *n <= 0xff {
                    buf.extend_from_slice(&[0xcc, *n as u8]);
                } else if *n <= 0xffff {
                    buf.extend_from_slice(&[0xcd, (*n >> 8) as u8, *n as u8]);
                } else if *n <= 0xffff_ffff {
                    let n = *n as u32;
                    buf.extend_from_slice(&[
                        0xce,
                        (n >> 24) as u8,
                        (n >> 16) as u8,
                        (n >> 8) as u8,
                        n as u8,
                    ]);
                } else {
                    buf.extend_from_slice(&[
                        0xcf,
                        (*n as u64 >> 56) as u8,
                        (*n as u64 >> 48) as u8,
                        (*n as u64 >> 40) as u8,
                        (*n as u64 >> 32) as u8,
                        (*n as u64 >> 24) as u8,
                        (*n as u64 >> 16) as u8,
                        (*n as u64 >> 8) as u8,
                        *n as u8,
                    ]);
                }
            } else {
                if *n >= -32 {
                    buf.push((*n as i8) as u8);
                } else if *n >= -128 {
                    buf.extend_from_slice(&[0xd0, *n as u8]);
                } else if *n >= -32768 {
                    let n = *n as i16;
                    buf.extend_from_slice(&[0xd1, (n >> 8) as u8, n as u8]);
                } else if *n >= -2147483648 {
                    let n = *n as i32;
                    buf.extend_from_slice(&[
                        0xd2,
                        (n >> 24) as u8,
                        (n >> 16) as u8,
                        (n >> 8) as u8,
                        n as u8,
                    ]);
                } else {
                    let n = *n;
                    buf.extend_from_slice(&[
                        0xd3,
                        (n >> 56) as u8,
                        (n >> 48) as u8,
                        (n >> 40) as u8,
                        (n >> 32) as u8,
                        (n >> 24) as u8,
                        (n >> 16) as u8,
                        (n >> 8) as u8,
                        n as u8,
                    ]);
                }
            }
        }
        LuaValue::Number(n) => {
            buf.push(0xcb);
            buf.extend_from_slice(&n.to_be_bytes());
        }
        LuaValue::String(s) => {
            let bytes = s.as_bytes();
            let len = bytes.len();
            if len <= 31 {
                buf.push(0xa0 | len as u8);
            } else if len <= 0xff {
                buf.extend_from_slice(&[0xd9, len as u8]);
            } else if len <= 0xffff {
                buf.extend_from_slice(&[0xda, (len >> 8) as u8, len as u8]);
            } else {
                buf.extend_from_slice(&[
                    0xdb,
                    (len >> 24) as u8,
                    (len >> 16) as u8,
                    (len >> 8) as u8,
                    len as u8,
                ]);
            }
            buf.extend_from_slice(bytes.as_ref());
        }
        LuaValue::Table(t) => {
            let mut arr_len = 0i64;
            for i in 1.. {
                match t.get::<LuaValue>(i) {
                    Ok(LuaValue::Nil) | Err(_) => break,
                    Ok(_) => arr_len = i,
                }
            }
            let mut map_keys: Vec<(String, LuaValue)> = Vec::new();
            for (k, v) in t.pairs::<String, LuaValue>().flatten() {
                let is_arr_key = k
                    .parse::<i64>()
                    .map(|i| i >= 1 && i <= arr_len)
                    .unwrap_or(false);
                if !is_arr_key {
                    map_keys.push((k, v));
                }
            }
            if arr_len > 0 && map_keys.is_empty() {
                let len = arr_len as usize;
                if len <= 15 {
                    buf.push(0x90 | len as u8);
                } else if len <= 0xffff {
                    buf.extend_from_slice(&[0xdc, (len >> 8) as u8, len as u8]);
                } else {
                    buf.extend_from_slice(&[
                        0xdd,
                        (len >> 24) as u8,
                        (len >> 16) as u8,
                        (len >> 8) as u8,
                        len as u8,
                    ]);
                }
                for i in 1..=arr_len {
                    if let Ok(v) = t.get::<LuaValue>(i) {
                        pack_msgpack_depth(lua, &v, buf, depth + 1, max_depth)?;
                    }
                }
            } else {
                let total = arr_len as usize + map_keys.len();
                if total <= 15 {
                    buf.push(0x80 | total as u8);
                } else if total <= 0xffff {
                    buf.extend_from_slice(&[0xde, (total >> 8) as u8, total as u8]);
                } else {
                    buf.extend_from_slice(&[
                        0xdf,
                        (total >> 24) as u8,
                        (total >> 16) as u8,
                        (total >> 8) as u8,
                        total as u8,
                    ]);
                }
                for i in 1..=arr_len {
                    if let Ok(v) = t.get::<LuaValue>(i) {
                        let key_val = LuaValue::Integer(i);
                        pack_msgpack_depth(lua, &key_val, buf, depth + 1, max_depth)?;
                        pack_msgpack_depth(lua, &v, buf, depth + 1, max_depth)?;
                    }
                }
                for (k, v) in &map_keys {
                    let key_str = LuaValue::String(lua.create_string(k.as_str())?);
                    pack_msgpack_depth(lua, &key_str, buf, depth + 1, max_depth)?;
                    pack_msgpack_depth(lua, v, buf, depth + 1, max_depth)?;
                }
            }
        }
        _ => return Err(mlua::Error::external("Cannot encode value to msgpack")),
    }
    Ok(())
}

fn unpack_msgpack(lua: &Lua, data: &[u8], pos: &mut usize) -> Result<LuaValue, mlua::Error> {
    if *pos >= data.len() {
        return Err(mlua::Error::external("Unexpected end of msgpack data"));
    }
    let b = data[*pos];
    *pos += 1;
    match b {
        0xc0 => Ok(LuaValue::Nil),
        0xc2 => Ok(LuaValue::Boolean(false)),
        0xc3 => Ok(LuaValue::Boolean(true)),
        0xcc => {
            if *pos + 1 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let v = data[*pos];
            *pos += 1;
            Ok(LuaValue::Integer(v as i64))
        }
        0xcd => {
            if *pos + 2 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let v = ((data[*pos] as u16) << 8) | data[*pos + 1] as u16;
            *pos += 2;
            Ok(LuaValue::Integer(v as i64))
        }
        0xce => {
            if *pos + 4 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let v = ((data[*pos] as u32) << 24)
                | ((data[*pos + 1] as u32) << 16)
                | ((data[*pos + 2] as u32) << 8)
                | data[*pos + 3] as u32;
            *pos += 4;
            Ok(LuaValue::Integer(v as i64))
        }
        0xcf => {
            if *pos + 8 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[*pos..*pos + 8]);
            *pos += 8;
            let v = u64::from_be_bytes(bytes) as i64;
            Ok(LuaValue::Integer(v))
        }
        0xd0 => {
            if *pos + 1 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let v = data[*pos] as i8;
            *pos += 1;
            Ok(LuaValue::Integer(v as i64))
        }
        0xd1 => {
            if *pos + 2 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let v = i16::from_be_bytes([data[*pos], data[*pos + 1]]);
            *pos += 2;
            Ok(LuaValue::Integer(v as i64))
        }
        0xd2 => {
            if *pos + 4 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let v =
                i32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(LuaValue::Integer(v as i64))
        }
        0xd3 => {
            if *pos + 8 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[*pos..*pos + 8]);
            *pos += 8;
            let v = i64::from_be_bytes(bytes);
            Ok(LuaValue::Integer(v))
        }
        0xca => {
            if *pos + 4 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&data[*pos..*pos + 4]);
            *pos += 4;
            Ok(LuaValue::Number(f32::from_be_bytes(bytes) as f64))
        }
        0xcb => {
            if *pos + 8 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack"));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[*pos..*pos + 8]);
            *pos += 8;
            Ok(LuaValue::Number(f64::from_be_bytes(bytes)))
        }
        0xd9 | 0xda | 0xdb | 0xc4 | 0xc5 | 0xc6 => {
            let len = match b {
                0xd9 | 0xc4 => {
                    let v = data[*pos] as usize;
                    *pos += 1;
                    v
                }
                0xda | 0xc5 => {
                    let v = ((data[*pos] as usize) << 8) | data[*pos + 1] as usize;
                    *pos += 2;
                    v
                }
                _ => {
                    let v = ((data[*pos] as usize) << 24)
                        | ((data[*pos + 1] as usize) << 16)
                        | ((data[*pos + 2] as usize) << 8)
                        | data[*pos + 3] as usize;
                    *pos += 4;
                    v
                }
            };
            if *pos + len > data.len() {
                return Err(mlua::Error::external("Truncated msgpack string"));
            }
            let s = lua.create_string(&data[*pos..*pos + len])?;
            *pos += len;
            Ok(LuaValue::String(s))
        }
        0x90..=0x9f => {
            let len = (b & 0x0f) as usize;
            let t = lua.create_table()?;
            for i in 0..len {
                t.set(i + 1, unpack_msgpack(lua, data, pos)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        0xdc => {
            if *pos + 2 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack array16"));
            }
            let len = ((data[*pos] as usize) << 8) | data[*pos + 1] as usize;
            *pos += 2;
            let t = lua.create_table()?;
            for i in 0..len {
                t.set(i + 1, unpack_msgpack(lua, data, pos)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        0xdd => {
            if *pos + 4 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack array32"));
            }
            let len = ((data[*pos] as usize) << 24)
                | ((data[*pos + 1] as usize) << 16)
                | ((data[*pos + 2] as usize) << 8)
                | data[*pos + 3] as usize;
            *pos += 4;
            let t = lua.create_table()?;
            for i in 0..len {
                t.set(i + 1, unpack_msgpack(lua, data, pos)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        0x80..=0x8f => {
            let len = (b & 0x0f) as usize;
            let t = lua.create_table()?;
            for _ in 0..len {
                let k = unpack_msgpack(lua, data, pos)?;
                let v = unpack_msgpack(lua, data, pos)?;
                match k {
                    LuaValue::String(s) => {
                        let key_str = s.to_str().map(|s| s.to_string()).unwrap_or_default();
                        t.set(key_str, v)?;
                    }
                    LuaValue::Integer(n) => t.set(n, v)?,
                    _ => {}
                }
            }
            Ok(LuaValue::Table(t))
        }
        0xde => {
            if *pos + 2 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack map16"));
            }
            let len = ((data[*pos] as usize) << 8) | data[*pos + 1] as usize;
            *pos += 2;
            let t = lua.create_table()?;
            for _ in 0..len {
                let k = unpack_msgpack(lua, data, pos)?;
                let v = unpack_msgpack(lua, data, pos)?;
                match k {
                    LuaValue::String(s) => {
                        let key_str = s.to_str().map(|s| s.to_string()).unwrap_or_default();
                        t.set(key_str, v)?;
                    }
                    LuaValue::Integer(n) => t.set(n, v)?,
                    _ => {}
                }
            }
            Ok(LuaValue::Table(t))
        }
        0xdf => {
            if *pos + 4 > data.len() {
                return Err(mlua::Error::external("Truncated msgpack map32"));
            }
            let len = ((data[*pos] as usize) << 24)
                | ((data[*pos + 1] as usize) << 16)
                | ((data[*pos + 2] as usize) << 8)
                | data[*pos + 3] as usize;
            *pos += 4;
            let t = lua.create_table()?;
            for _ in 0..len {
                let k = unpack_msgpack(lua, data, pos)?;
                let v = unpack_msgpack(lua, data, pos)?;
                match k {
                    LuaValue::String(s) => {
                        let key_str = s.to_str().map(|s| s.to_string()).unwrap_or_default();
                        t.set(key_str, v)?;
                    }
                    LuaValue::Integer(n) => t.set(n, v)?,
                    _ => {}
                }
            }
            Ok(LuaValue::Table(t))
        }
        0xa0..=0xbf => {
            let len = (b & 0x1f) as usize;
            if *pos + len > data.len() {
                return Err(mlua::Error::external("Truncated msgpack fixstr"));
            }
            let s = lua.create_string(&data[*pos..*pos + len])?;
            *pos += len;
            Ok(LuaValue::String(s))
        }
        _ => {
            if b <= 0x7f {
                Ok(LuaValue::Integer(b as i64))
            } else if b >= 0xe0 {
                Ok(LuaValue::Integer((b as i8) as i64))
            } else {
                Err(mlua::Error::external(format!(
                    "Unsupported msgpack type: 0x{:02x}",
                    b
                )))
            }
        }
    }
}
