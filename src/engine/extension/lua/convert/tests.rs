use super::*;

#[test]
fn test_lua_value_to_value_big_number_sanitizes_newlines() {
    let lua = Lua::new();
    let table = lua.create_table().unwrap();
    table.set("big_number", "123\r\n456").unwrap();
    let result = lua_value_to_value(&LuaValue::Table(table));
    match result {
        Value::BigNumber(s) => assert_eq!(s.as_ref(), b"123  456"),
        _ => panic!("expected big number, got {:?}", result),
    }
}

#[test]
fn test_lua_value_to_value_verbatim_table() {
    let lua = Lua::new();
    let vb = lua.create_table().unwrap();
    vb.set("format", "mkd").unwrap();
    vb.set("string", "hello").unwrap();
    let table = lua.create_table().unwrap();
    table.set("verbatim_string", vb).unwrap();
    let result = lua_value_to_value(&LuaValue::Table(table));
    match result {
        Value::Verbatim { format, data } => {
            assert_eq!(format, *b"mkd");
            assert_eq!(data.as_ref(), b"hello");
        }
        _ => panic!("expected verbatim, got {:?}", result),
    }
}

#[test]
fn test_lua_value_to_value_verbatim_default_format() {
    let lua = Lua::new();
    let vb = lua.create_table().unwrap();
    vb.set("string", "data").unwrap();
    let table = lua.create_table().unwrap();
    table.set("verbatim_string", vb).unwrap();
    let result = lua_value_to_value(&LuaValue::Table(table));
    match result {
        Value::Verbatim { format, data } => {
            assert_eq!(format, *b"txt");
            assert_eq!(data.as_ref(), b"data");
        }
        _ => panic!("expected verbatim, got {:?}", result),
    }
}

#[test]
fn test_lua_value_to_value_map_table() {
    let lua = Lua::new();
    let map = lua.create_table().unwrap();
    map.set("a", 1).unwrap();
    map.set("b", 2).unwrap();
    let table = lua.create_table().unwrap();
    table.set("map", map).unwrap();
    let result = lua_value_to_value(&LuaValue::Table(table));
    match result {
        Value::Map(entries) => {
            assert_eq!(entries.len(), 2);
        }
        _ => panic!("expected map, got {:?}", result),
    }
}

#[test]
fn test_lua_value_to_value_set_table() {
    let lua = Lua::new();
    let set = lua.create_table().unwrap();
    set.set("x", true).unwrap();
    set.set("y", true).unwrap();
    let table = lua.create_table().unwrap();
    table.set("set", set).unwrap();
    let result = lua_value_to_value(&LuaValue::Table(table));
    match result {
        Value::Set(items) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected set, got {:?}", result),
    }
}

#[test]
fn test_lua_error_value() {
    let err = LuaError::runtime("test error");
    let result = lua_value_to_value(&LuaValue::Error(Box::new(err)));
    match result {
        Value::Error(_) => {}
        _ => panic!("expected error, got {:?}", result),
    }
}

#[test]
fn test_deeply_nested_table_hits_depth_limit() {
    let lua = Lua::new();
    let code = r#"
        local t = {}
        local cur = t
        for i = 1, 20 do
            cur[1] = {}
            cur = cur[1]
        end
        return t
    "#;
    let result: LuaValue = lua.load(code).eval().unwrap();
    let val = lua_value_to_value(&result);
    let mut cur = &val;
    let mut depth = 0;
    while let Value::Array(items) = cur {
        if items.is_empty() {
            break;
        }
        cur = &items[0];
        depth += 1;
    }
    assert!(depth <= MAX_LUA_REPLY_DEPTH + 1);
    assert_eq!(*cur, Value::Null);
}

#[test]
fn test_value_for_script_resp2_attribute() {
    let inner = Value::Double(1.5);
    let val = Value::Attribute {
        inner: Box::new(inner),
        attrs: vec![],
    };
    let result = value_for_script_resp(&val, 2);
    match result {
        Value::Bulk(b) => assert_eq!(b.as_ref(), b"1.5"),
        _ => panic!("expected bulk, got {:?}", result),
    }
}

#[test]
fn test_value_for_script_resp2_array() {
    let val = Value::Array(vec![Value::Boolean(true), Value::Boolean(false)]);
    let result = value_for_script_resp(&val, 2);
    assert_eq!(
        result,
        Value::Array(vec![Value::Integer(1), Value::Integer(0)])
    );
}
