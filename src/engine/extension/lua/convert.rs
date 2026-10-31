use crate::foundation::io::protocol::serializer::format_double;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;
use mlua::prelude::*;
use std::cell::RefCell;
use std::collections::HashSet;

const MAX_LUA_REPLY_DEPTH: i32 = 16;

thread_local! {
    static VISITED_TABLES: RefCell<HashSet<usize>> = RefCell::new(HashSet::new());
}

/// - SimpleString (+OK) → table `{ok = "str"}`
/// - Error (-ERR) → table `{err = "msg"}`
/// - Integer (:N) → Lua number (integer)
/// - Bulk ($N str) → Lua string
/// - Null ($-1) → Lua boolean false
/// - NullArray (*-1) → Lua boolean false
/// - Boolean → Lua boolean
/// - Double → table `{double = f}`
/// - BigNumber → table `{big_number = s}`
/// - Verbatim → table `{verbatim_string = {format=..., string=...}}`
/// - Map → table `{map = {k1=v1, ...}}`
/// - Set → table `{set = {k1=true, ...}}`
#[cfg(test)]
pub fn value_to_lua(lua: &Lua, val: &Value) -> LuaResult<LuaValue> {
    value_to_lua_resp(lua, val, 2)
}

pub fn value_to_lua_resp(lua: &Lua, val: &Value, resp: u8) -> LuaResult<LuaValue> {
    match val {
        Value::SimpleString(s) => {
            let table = lua.create_table()?;
            table.set("ok", lua.create_string(s.as_ref())?)?;
            Ok(LuaValue::Table(table))
        }
        Value::Error(s) => {
            let table = lua.create_table()?;
            table.set("err", lua.create_string(s.as_ref())?)?;
            Ok(LuaValue::Table(table))
        }
        Value::Integer(n) => Ok(LuaValue::Integer(*n)),
        Value::Bulk(b) => {
            if b.is_empty() {
                Ok(LuaValue::String(lua.create_string("")?))
            } else {
                Ok(LuaValue::String(lua.create_string(b)?))
            }
        }
        Value::Null | Value::NullArray if resp >= 3 => Ok(LuaValue::Nil),
        Value::Null | Value::NullArray => Ok(LuaValue::Boolean(false)),
        Value::Array(items) => {
            let table = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                let lua_val = value_to_lua_resp(lua, item, resp)?;
                table.set(i + 1, lua_val)?;
            }
            Ok(LuaValue::Table(table))
        }
        Value::Boolean(b) => Ok(LuaValue::Boolean(*b)),
        Value::Double(f) => {
            let table = lua.create_table()?;
            table.set("double", *f)?;
            Ok(LuaValue::Table(table))
        }
        Value::DecimalDouble(text) if resp < 3 => {
            Ok(LuaValue::String(lua.create_string(text.as_bytes())?))
        }
        Value::DecimalDouble(text) => {
            // Lua numbers are binary64; only this capability boundary narrows the reply.
            let number = std::str::from_utf8(text.as_bytes())
                .map_err(LuaError::external)?
                .parse::<f64>()
                .map_err(LuaError::external)?;
            let table = lua.create_table()?;
            table.set("double", number)?;
            Ok(LuaValue::Table(table))
        }
        Value::BigNumber(s) => {
            let table = lua.create_table()?;
            table.set("big_number", lua.create_string(s)?)?;
            Ok(LuaValue::Table(table))
        }
        Value::Verbatim { format, data } => {
            let vb_table = lua.create_table()?;
            vb_table.set("format", lua.create_string(format.as_slice())?)?;
            vb_table.set("string", lua.create_string(data)?)?;
            let table = lua.create_table()?;
            table.set("verbatim_string", vb_table)?;
            Ok(LuaValue::Table(table))
        }
        Value::Push(items) => {
            let table = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                let lua_val = value_to_lua_resp(lua, item, resp)?;
                table.set(i + 1, lua_val)?;
            }
            Ok(LuaValue::Table(table))
        }
        Value::Attribute { inner, .. } => value_to_lua_resp(lua, inner, resp),
        Value::Map(entries) => {
            let map_table = lua.create_table()?;
            for (k, v) in entries {
                let lua_k = value_to_lua_resp(lua, k, resp)?;
                let lua_v = value_to_lua_resp(lua, v, resp)?;
                map_table.set(lua_k, lua_v)?;
            }
            let table = lua.create_table()?;
            table.set("map", map_table)?;
            Ok(LuaValue::Table(table))
        }
        Value::Set(items) => {
            let set_table = lua.create_table()?;
            for item in items {
                let lua_val = value_to_lua_resp(lua, item, resp)?;
                set_table.set(lua_val, true)?;
            }
            let table = lua.create_table()?;
            table.set("set", set_table)?;
            Ok(LuaValue::Table(table))
        }
    }
}

/// Convert a command reply to the shape a Lua script sees for its selected
/// `redis.setresp()` version.
pub fn value_for_script_resp(val: &Value, resp: u8) -> Value {
    if resp >= 3 {
        return val.clone();
    }

    match val {
        Value::Double(f) => Value::Bulk(Bytes::from(format_double(*f).into_bytes())),
        Value::DecimalDouble(text) => Value::Bulk(text.bytes()),
        Value::Boolean(b) => Value::Integer(if *b { 1 } else { 0 }),
        Value::BigNumber(s) => Value::Bulk(s.clone()),
        Value::Verbatim { data, .. } => Value::Bulk(data.clone()),
        Value::Map(entries) => {
            let mut items = Vec::with_capacity(entries.len() * 2);
            for (k, v) in entries {
                items.push(value_for_script_resp(k, resp));
                items.push(value_for_script_resp(v, resp));
            }
            Value::Array(items)
        }
        Value::Set(items) => Value::Array(
            items
                .iter()
                .map(|item| value_for_script_resp(item, resp))
                .collect(),
        ),
        Value::Attribute { inner, .. } => value_for_script_resp(inner, resp),
        Value::Push(items) | Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| value_for_script_resp(item, resp))
                .collect(),
        ),
        _ => val.clone(),
    }
}

#[cfg(test)]
pub fn lua_value_to_value(val: &LuaValue) -> Value {
    lua_value_to_value_resp(val, 2)
}

pub fn lua_value_to_value_resp(val: &LuaValue, resp: u8) -> Value {
    VISITED_TABLES.with(|v| v.borrow_mut().clear());
    lua_value_to_value_depth(val, 0, resp)
}

fn lua_value_to_value_depth(val: &LuaValue, depth: i32, resp: u8) -> Value {
    if depth > MAX_LUA_REPLY_DEPTH {
        return Value::Null;
    }
    match val {
        LuaValue::Nil => Value::Null,
        LuaValue::Boolean(b) => {
            if resp >= 3 {
                return Value::Boolean(*b);
            }
            if *b {
                Value::Integer(1)
            } else {
                Value::Null
            }
        }
        LuaValue::Integer(n) => Value::Integer(*n),
        LuaValue::Number(f) => Value::Integer(*f as i64),
        LuaValue::String(s) => Value::Bulk(Bytes::copy_from_slice(s.as_bytes().as_ref())),
        LuaValue::Table(table) => {
            let table_ptr = table.to_pointer() as usize;
            let is_cycle = VISITED_TABLES.with(|v| {
                let mut visited = v.borrow_mut();
                if visited.contains(&table_ptr) {
                    true
                } else {
                    visited.insert(table_ptr);
                    false
                }
            });
            if is_cycle {
                return Value::Error(Bytes::from_static(b"ERR reached lua stack limit"));
            }

            let result = convert_table(table, depth, resp);

            VISITED_TABLES.with(|v| {
                v.borrow_mut().remove(&table_ptr);
            });
            result
        }
        LuaValue::Error(e) => {
            let msg = format!("{}", e);
            Value::Error(Bytes::from(msg.into_bytes()))
        }
        _ => Value::Null,
    }
}

fn get_table_string_field(table: &LuaTable, key: &str) -> Option<String> {
    match table.raw_get::<LuaValue>(key) {
        Ok(LuaValue::String(s)) => s.to_str().ok().map(|s| s.to_string()),
        _ => None,
    }
}

fn get_table_number_field(table: &LuaTable, key: &str) -> Option<f64> {
    match table.raw_get::<LuaValue>(key) {
        Ok(LuaValue::Number(n)) => Some(n),
        Ok(LuaValue::Integer(n)) => Some(n as f64),
        _ => None,
    }
}

fn sanitize_resp_line(s: &str) -> Bytes {
    Bytes::from(
        s.bytes()
            .map(|b| if b == b'\r' || b == b'\n' { b' ' } else { b })
            .collect::<Vec<u8>>(),
    )
}

fn convert_table(table: &LuaTable, depth: i32, resp: u8) -> Value {
    // Check for error reply: {err = "msg"}
    if let Some(err_msg) = get_table_string_field(table, "err") {
        return Value::Error(err_msg.into());
    }

    // Check for status reply: {ok = "msg"}
    if let Some(ok_msg) = get_table_string_field(table, "ok") {
        return Value::SimpleString(ok_msg.into());
    }

    // Check for double reply: {double = f}
    if let Some(d) = get_table_number_field(table, "double") {
        return Value::Double(d);
    }

    // Check for big number reply: {big_number = s}
    if let Some(bn) = get_table_string_field(table, "big_number") {
        return Value::BigNumber(sanitize_resp_line(&bn));
    }

    // Check for verbatim reply: {verbatim_string = {format=..., string=...}}
    if let Ok(LuaValue::Table(vb)) = table.raw_get::<LuaValue>("verbatim_string") {
        let format_str = match vb.raw_get::<LuaValue>("format") {
            Ok(LuaValue::String(s)) => s
                .to_str()
                .ok()
                .map(|b| b.to_string())
                .unwrap_or_else(|| "txt".to_string()),
            _ => "txt".to_string(),
        };
        let data_str = match vb.raw_get::<LuaValue>("string") {
            Ok(LuaValue::String(s)) => s.to_str().ok().map(|b| b.to_string()).unwrap_or_default(),
            _ => String::new(),
        };
        let mut fmt = *b"txt";
        let fb = format_str.as_bytes();
        let len = fb.len().min(3);
        fmt[..len].copy_from_slice(&fb[..len]);
        return Value::Verbatim {
            format: fmt,
            data: Bytes::from(data_str.into_bytes()),
        };
    }

    // Check for map reply: {map = {k1=v1, k2=v2, ...}}
    if let Ok(LuaValue::Table(map_table)) = table.raw_get::<LuaValue>("map") {
        let mut entries = Vec::new();
        if let Ok(pairs) = map_table
            .pairs::<LuaValue, LuaValue>()
            .collect::<LuaResult<Vec<_>>>()
        {
            for (k, v) in pairs {
                entries.push((
                    lua_value_to_value_depth(&k, depth + 1, resp),
                    lua_value_to_value_depth(&v, depth + 1, resp),
                ));
            }
        }
        return Value::Map(entries);
    }

    // Check for set reply: {set = {k1=true, k2=true, ...}}
    if let Ok(LuaValue::Table(set_table)) = table.raw_get::<LuaValue>("set") {
        let mut items = Vec::new();
        if let Ok(pairs) = set_table
            .pairs::<LuaValue, LuaValue>()
            .collect::<LuaResult<Vec<_>>>()
        {
            for (k, _) in pairs {
                items.push(lua_value_to_value_depth(&k, depth + 1, resp));
            }
        }
        return Value::Set(items);
    }

    // Default: array reply (integer keys 1..n, stop at nil)
    let mut items = Vec::new();
    let mut i: i64 = 1;
    loop {
        match table.raw_get::<LuaValue>(i) {
            Ok(v) if !v.is_nil() => {
                items.push(lua_value_to_value_depth(&v, depth + 1, resp));
                i += 1;
            }
            _ => break,
        }
    }
    Value::Array(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_double_lua_resp2_preserves_text_and_resp3_narrows_at_bridge() {
        use crate::foundation::io::protocol::DecimalDoubleText;
        let lua = Lua::new();
        for (text, number) in [
            ("9007199254740993", 9007199254740992.0),
            ("1e4932", f64::INFINITY),
            ("1e-4932", 0.0),
            ("-inf", f64::NEG_INFINITY),
            ("0", 0.0),
        ] {
            let value = Value::DecimalDouble(DecimalDoubleText::new(text.to_owned()).unwrap());
            assert_eq!(
                value_for_script_resp(&value, 2),
                Value::bulk(text.to_owned())
            );
            assert_eq!(value_for_script_resp(&value, 3), value);
            let direct = value_to_lua_resp(&lua, &value, 2).unwrap();
            let LuaValue::String(string) = direct else {
                panic!("expected exact Lua string")
            };
            assert_eq!(string.as_bytes().as_ref(), text.as_bytes());
            let bridged = value_to_lua_resp(&lua, &value, 3).unwrap();
            let LuaValue::Table(table) = bridged else {
                panic!("expected Lua double table")
            };
            assert_eq!(table.get::<f64>("double").unwrap(), number);
            assert!(
                matches!(lua_value_to_value_resp(&LuaValue::Table(table), 3),
                Value::Double(value) if value == number)
            );
        }
        let scalar = Value::DecimalDouble(DecimalDoubleText::new("9007199254740993").unwrap());
        let nested = Value::attribute(
            vec![(Value::bulk("meta"), scalar.clone())],
            Value::Map(vec![(Value::bulk("key"), Value::Set(vec![scalar]))]),
        );
        assert_eq!(
            value_for_script_resp(&nested, 2),
            Value::Array(vec![
                Value::bulk("key"),
                Value::Array(vec![Value::bulk("9007199254740993")]),
            ])
        );
    }

    #[test]
    fn test_value_to_lua_nil() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::Null).unwrap();
        assert_eq!(result, LuaValue::Boolean(false));
    }

    #[test]
    fn test_value_to_lua_resp3_null() {
        let lua = Lua::new();
        let result = value_to_lua_resp(&lua, &Value::Null, 3).unwrap();
        assert_eq!(result, LuaValue::Nil);
    }

    #[test]
    fn test_value_to_lua_integer() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::Integer(42)).unwrap();
        assert_eq!(result, LuaValue::Integer(42));
    }

    #[test]
    fn test_value_to_lua_bulk_string() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::Bulk(Bytes::from_static(b"hello"))).unwrap();
        match result {
            LuaValue::String(s) => assert_eq!(s.to_str().unwrap(), "hello"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_lua_value_to_value_nil() {
        assert_eq!(lua_value_to_value(&LuaValue::Nil), Value::Null);
    }

    #[test]
    fn test_lua_value_to_value_integer() {
        assert_eq!(
            lua_value_to_value(&LuaValue::Integer(42)),
            Value::Integer(42)
        );
    }

    #[test]
    fn test_lua_value_to_value_string() {
        let lua = Lua::new();
        let s = lua.create_string("hello").unwrap();
        let result = lua_value_to_value(&LuaValue::String(s));
        match result {
            Value::Bulk(b) => assert_eq!(b.as_ref(), b"hello"),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_lua_value_to_value_boolean_true() {
        assert_eq!(
            lua_value_to_value(&LuaValue::Boolean(true)),
            Value::Integer(1)
        );
    }

    #[test]
    fn test_lua_value_to_value_boolean_false() {
        assert_eq!(lua_value_to_value(&LuaValue::Boolean(false)), Value::Null);
    }

    #[test]
    fn test_lua_value_to_value_resp3_boolean() {
        assert_eq!(
            lua_value_to_value_resp(&LuaValue::Boolean(true), 3),
            Value::Boolean(true)
        );
        assert_eq!(
            lua_value_to_value_resp(&LuaValue::Boolean(false), 3),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_lua_value_to_value_function_returns_null() {
        let lua = Lua::new();
        let f: LuaFunction = lua.create_function(|_, ()| Ok(())).unwrap();
        assert_eq!(lua_value_to_value(&LuaValue::Function(f)), Value::Null);
    }

    #[test]
    fn test_roundtrip_integer() {
        let lua = Lua::new();
        let val = Value::Integer(100);
        let lua_val = value_to_lua(&lua, &val).unwrap();
        let back = lua_value_to_value(&lua_val);
        assert_eq!(back, val);
    }

    #[test]
    fn test_roundtrip_array() {
        let lua = Lua::new();
        let val = Value::Array(vec![
            Value::Bulk(Bytes::from_static(b"a")),
            Value::Bulk(Bytes::from_static(b"b")),
            Value::Bulk(Bytes::from_static(b"c")),
        ]);
        let lua_val = value_to_lua(&lua, &val).unwrap();
        let back = lua_value_to_value(&lua_val);
        assert_eq!(back, val);
    }

    #[test]
    fn test_lua_error_table() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.set("err", "something wrong").unwrap();
        let result = lua_value_to_value(&LuaValue::Table(table));
        match result {
            Value::Error(s) => assert_eq!(s.as_ref(), b"something wrong"),
            _ => panic!("expected error, got {:?}", result),
        }
    }

    #[test]
    fn test_lua_status_table() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.set("ok", "OK").unwrap();
        let result = lua_value_to_value(&LuaValue::Table(table));
        match result {
            Value::SimpleString(s) => assert_eq!(s.as_ref(), b"OK"),
            _ => panic!("expected simple string, got {:?}", result),
        }
    }

    #[test]
    fn test_lua_empty_table_returns_empty_array() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        let result = lua_value_to_value(&LuaValue::Table(table));
        assert_eq!(result, Value::Array(vec![]));
    }

    #[test]
    fn test_circular_table_no_overflow() {
        let lua = Lua::new();
        let code = r#"
            _G.test_self = _G
            return _G
        "#;
        let result: LuaValue = lua.load(code).eval().unwrap();
        let val = lua_value_to_value(&result);
        match val {
            Value::Array(items) => assert!(items.is_empty(), "_G should return empty array"),
            _ => panic!(
                "expected empty array for _G, got {:?}",
                std::mem::discriminant(&val)
            ),
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_lua_number_returns_integer() {
        let result = lua_value_to_value(&LuaValue::Number(3.14));
        assert_eq!(result, Value::Integer(3));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_value_for_script_resp2_downgrades_resp3_scalars() {
        assert_eq!(
            value_for_script_resp(&Value::BigNumber(Bytes::from_static(b"123")), 2),
            Value::Bulk(Bytes::from_static(b"123"))
        );
        assert_eq!(
            value_for_script_resp(&Value::verbatim(*b"txt", "hello"), 2),
            Value::Bulk(Bytes::from_static(b"hello"))
        );
        assert_eq!(
            value_for_script_resp(&Value::Double(3.141), 2),
            Value::Bulk(Bytes::from_static(b"3.141"))
        );
        assert_eq!(
            value_for_script_resp(&Value::Boolean(false), 2),
            Value::Integer(0)
        );
    }

    #[test]
    fn test_value_for_script_resp2_downgrades_collections() {
        let value = Value::map(vec![
            (Value::integer(0), Value::boolean(false)),
            (Value::integer(1), Value::boolean(true)),
        ]);
        assert_eq!(
            value_for_script_resp(&value, 2),
            Value::Array(vec![
                Value::integer(0),
                Value::integer(0),
                Value::integer(1),
                Value::integer(1),
            ])
        );

        let value = Value::set(vec![Value::bignum("123"), Value::boolean(true)]);
        assert_eq!(
            value_for_script_resp(&value, 2),
            Value::Array(vec![
                Value::Bulk(Bytes::from_static(b"123")),
                Value::integer(1)
            ])
        );
    }

    #[test]
    fn test_value_for_script_resp3_keeps_resp3_types() {
        let value = Value::map(vec![(Value::integer(1), Value::boolean(true))]);
        assert_eq!(value_for_script_resp(&value, 3), value);
    }

    #[test]
    fn test_value_to_lua_simple_string() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::SimpleString(Bytes::from_static(b"OK"))).unwrap();
        match result {
            LuaValue::Table(t) => {
                let ok: LuaValue = t.get("ok").unwrap();
                match ok {
                    LuaValue::String(s) => assert_eq!(s.to_str().unwrap(), "OK"),
                    _ => panic!("expected string"),
                }
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    fn test_value_to_lua_error() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::Error(Bytes::from_static(b"ERR boom"))).unwrap();
        match result {
            LuaValue::Table(t) => {
                let err: LuaValue = t.get("err").unwrap();
                match err {
                    LuaValue::String(s) => assert_eq!(s.to_str().unwrap(), "ERR boom"),
                    _ => panic!("expected string"),
                }
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    fn test_value_to_lua_empty_bulk() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::Bulk(Bytes::new())).unwrap();
        match result {
            LuaValue::String(s) => assert_eq!(s.to_str().unwrap(), ""),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_value_to_lua_null_array_resp2() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::NullArray).unwrap();
        assert_eq!(result, LuaValue::Boolean(false));
    }

    #[test]
    fn test_value_to_lua_null_array_resp3() {
        let lua = Lua::new();
        let result = value_to_lua_resp(&lua, &Value::NullArray, 3).unwrap();
        assert_eq!(result, LuaValue::Nil);
    }

    #[test]
    fn test_value_to_lua_boolean() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::Boolean(true)).unwrap();
        assert_eq!(result, LuaValue::Boolean(true));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_value_to_lua_double() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::Double(3.14)).unwrap();
        match result {
            LuaValue::Table(t) => {
                let d: f64 = t.get("double").unwrap();
                assert!((d - 3.14).abs() < 1e-9);
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    fn test_value_to_lua_big_number() {
        let lua = Lua::new();
        let result = value_to_lua(
            &lua,
            &Value::BigNumber(Bytes::from_static(b"12345678901234567890")),
        )
        .unwrap();
        match result {
            LuaValue::Table(t) => {
                let bn: LuaValue = t.get("big_number").unwrap();
                match bn {
                    LuaValue::String(s) => assert_eq!(s.to_str().unwrap(), "12345678901234567890"),
                    _ => panic!("expected string"),
                }
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    fn test_value_to_lua_verbatim() {
        let lua = Lua::new();
        let result = value_to_lua(&lua, &Value::verbatim(*b"txt", "hello world")).unwrap();
        match result {
            LuaValue::Table(t) => {
                let vb: LuaTable = t.get("verbatim_string").unwrap();
                let fmt: LuaValue = vb.get("format").unwrap();
                let s: LuaValue = vb.get("string").unwrap();
                match (fmt, s) {
                    (LuaValue::String(f), LuaValue::String(s)) => {
                        assert_eq!(f.to_str().unwrap(), "txt");
                        assert_eq!(s.to_str().unwrap(), "hello world");
                    }
                    _ => panic!("expected strings"),
                }
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    fn test_value_to_lua_push() {
        let lua = Lua::new();
        let val = Value::Push(vec![Value::Integer(1), Value::Integer(2)]);
        let result = value_to_lua(&lua, &val).unwrap();
        match result {
            LuaValue::Table(t) => {
                assert_eq!(t.len().unwrap(), 2);
                let first: i64 = t.get(1).unwrap();
                let second: i64 = t.get(2).unwrap();
                assert_eq!(first, 1);
                assert_eq!(second, 2);
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    fn test_value_to_lua_attribute() {
        let lua = Lua::new();
        let inner = Value::Integer(42);
        let val = Value::Attribute {
            inner: Box::new(inner.clone()),
            attrs: vec![],
        };
        let result = value_to_lua(&lua, &val).unwrap();
        assert_eq!(result, LuaValue::Integer(42));
    }

    #[test]
    fn test_value_to_lua_map() {
        let lua = Lua::new();
        let val = Value::map(vec![(
            Value::Bulk(Bytes::from_static(b"key1")),
            Value::Bulk(Bytes::from_static(b"val1")),
        )]);
        let result = value_to_lua(&lua, &val).unwrap();
        match result {
            LuaValue::Table(t) => {
                let map_table: LuaTable = t.get("map").unwrap();
                let v: LuaValue = map_table.get("key1").unwrap();
                match v {
                    LuaValue::String(s) => assert_eq!(s.to_str().unwrap(), "val1"),
                    _ => panic!("expected string"),
                }
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    fn test_value_to_lua_set() {
        let lua = Lua::new();
        let val = Value::set(vec![Value::Bulk(Bytes::from_static(b"a"))]);
        let result = value_to_lua(&lua, &val).unwrap();
        match result {
            LuaValue::Table(t) => {
                let set_table: LuaTable = t.get("set").unwrap();
                let has: bool = set_table.get("a").unwrap();
                assert!(has);
            }
            _ => panic!("expected table"),
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_lua_value_to_value_double_table() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.set("double", 2.718).unwrap();
        let result = lua_value_to_value(&LuaValue::Table(table));
        match result {
            Value::Double(f) => assert!((f - 2.718).abs() < 1e-9),
            _ => panic!("expected double, got {:?}", result),
        }
    }

    #[test]
    fn test_lua_value_to_value_big_number_table() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.set("big_number", "12345678901234567890").unwrap();
        let result = lua_value_to_value(&LuaValue::Table(table));
        match result {
            Value::BigNumber(s) => assert_eq!(s.as_ref(), b"12345678901234567890"),
            _ => panic!("expected big number, got {:?}", result),
        }
    }
}

#[cfg(test)]
#[path = "convert/tests.rs"]
mod conversion_tail_tests;
