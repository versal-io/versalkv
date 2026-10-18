use mlua::prelude::*;

use crate::engine::extension::script::ScriptError;

pub fn setup_cjson_table(lua: &Lua) -> Result<(), ScriptError> {
    let cjson = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let encode_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let val = args
                .front()
                .ok_or_else(|| mlua::Error::external("cjson.encode requires an argument"))?;
            let cjson_t: LuaTable = lua
                .globals()
                .get("cjson")
                .map_err(|e| mlua::Error::external(e.to_string()))?;
            let allow_invalid: bool = cjson_t.get("_encode_invalid_numbers").unwrap_or(false);
            let max_depth: i64 = cjson_t.get("_encode_max_depth").unwrap_or(1000);
            let json = lua_value_to_json_depth(val, allow_invalid, 0, max_depth)?;
            Ok(LuaValue::String(lua.create_string(&json)?))
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let decode_fn = lua
        .create_function(|lua, args: LuaMultiValue| {
            let s = match args.front() {
                Some(LuaValue::String(s)) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                _ => {
                    return Err(mlua::Error::external(
                        "cjson.decode requires a string argument",
                    ))
                }
            };
            let cjson_t: LuaTable = lua
                .globals()
                .get("cjson")
                .map_err(|e| mlua::Error::external(e.to_string()))?;
            let max_depth: i64 = cjson_t.get("_decode_max_depth").unwrap_or(1000);
            json_to_lua_value(lua, &s, max_depth)
        })
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let empty_arr = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let arr_mt = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    let empty_arr_mt = lua
        .create_table()
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("encode", encode_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("decode", decode_fn)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("encode_empty_table_as_object", true)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("empty_array", LuaValue::Table(empty_arr))
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("array_mt", arr_mt)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("empty_array_mt", empty_arr_mt)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("_encode_invalid_numbers", false)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("_encode_max_depth", 1000)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("_decode_max_depth", 1000)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;
    cjson
        .set("_encode_keep_buffer", false)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    lua.globals()
        .set("cjson", cjson)
        .map_err(|e| ScriptError::Runtime(e.to_string()))?;

    let config_code = r#"
        local cjson = cjson
        cjson.encode_keep_buffer = function(v)
            if v == nil then return cjson._encode_keep_buffer end
            cjson._encode_keep_buffer = v and true or false
            return cjson._encode_keep_buffer
        end
        cjson.encode_max_depth = function(v)
            if v == nil then return cjson._encode_max_depth end
            cjson._encode_max_depth = v
            return cjson._encode_max_depth
        end
        cjson.decode_max_depth = function(v)
            if v == nil then return cjson._decode_max_depth end
            cjson._decode_max_depth = v
            return cjson._decode_max_depth
        end
        cjson.encode_invalid_numbers = function(v)
            if v == nil then return cjson._encode_invalid_numbers end
            cjson._encode_invalid_numbers = v and true or false
            return cjson._encode_invalid_numbers
        end
    "#;
    lua.load(config_code)
        .exec()
        .map_err(|e| ScriptError::Runtime(format!("Failed to setup cjson config: {}", e)))?;

    Ok(())
}

fn lua_value_to_json_depth(
    val: &LuaValue,
    allow_invalid: bool,
    depth: i64,
    max_depth: i64,
) -> Result<String, mlua::Error> {
    match val {
        LuaValue::Nil => Ok("null".to_string()),
        LuaValue::Boolean(b) => Ok(if *b {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        LuaValue::Integer(n) => Ok(n.to_string()),
        LuaValue::Number(n) => {
            if n.is_finite() {
                Ok(n.to_string())
            } else if allow_invalid {
                if n.is_nan() {
                    Ok("null".to_string())
                } else if n.is_infinite() {
                    if n.is_sign_positive() {
                        Ok("1e+9999".to_string())
                    } else {
                        Ok("-1e+9999".to_string())
                    }
                } else {
                    Ok("null".to_string())
                }
            } else {
                Err(mlua::Error::external("Cannot encode infinity/NaN to JSON"))
            }
        }
        LuaValue::String(s) => {
            let s = s
                .to_str()
                .map_err(|e| mlua::Error::external(e.to_string()))?;
            Ok(json_escape(&s))
        }
        LuaValue::Table(t) => {
            if depth >= max_depth {
                return Err(mlua::Error::external("Cannot serialise, excessive nesting"));
            }
            let next_depth = depth + 1;
            let mut is_array = true;
            let mut max_idx = 0i64;
            for pair in t.sequence_values::<LuaValue>() {
                if pair.is_ok() {
                    max_idx += 1;
                } else {
                    is_array = false;
                    break;
                }
            }
            if is_array {
                for (k, _) in t.pairs::<LuaValue, LuaValue>().flatten() {
                    match k {
                        LuaValue::Integer(_) => {}
                        _ => {
                            is_array = false;
                            break;
                        }
                    }
                }
            }
            if is_array && max_idx > 0 {
                let mut items = Vec::new();
                for i in 1..=max_idx {
                    match t.get::<LuaValue>(i) {
                        Ok(v) => items.push(lua_value_to_json_depth(
                            &v,
                            allow_invalid,
                            next_depth,
                            max_depth,
                        )?),
                        Err(_) => items.push("null".to_string()),
                    }
                }
                Ok(format!("[{}]", items.join(",")))
            } else {
                let mut items = Vec::new();
                for (k, v) in t.pairs::<LuaValue, LuaValue>().flatten() {
                    let key_str = match &k {
                        LuaValue::String(s) => {
                            s.to_str().map(|s| s.to_string()).unwrap_or_default()
                        }
                        LuaValue::Integer(n) => n.to_string(),
                        LuaValue::Number(n) => n.to_string(),
                        LuaValue::Boolean(_) => {
                            return Err(mlua::Error::external(
                                "Cannot encode table with boolean keys to JSON",
                            ))
                        }
                        LuaValue::Nil => continue,
                        _ => {
                            return Err(mlua::Error::external(
                                "Cannot encode table with non-string keys to JSON",
                            ))
                        }
                    };
                    items.push(format!(
                        "{}:{}",
                        json_escape(&key_str),
                        lua_value_to_json_depth(&v, allow_invalid, next_depth, max_depth)?
                    ));
                }
                Ok(format!("{{{}}}", items.join(",")))
            }
        }
        _ => Err(mlua::Error::external("Cannot encode value to JSON")),
    }
}

pub(crate) fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_to_lua_value(lua: &Lua, s: &str, max_depth: i64) -> Result<LuaValue, mlua::Error> {
    let mut parser = JsonParser::new(s, max_depth);
    let val = parser.parse_value()?;
    convert_json_to_lua(lua, &val)
}

fn convert_json_to_lua(lua: &Lua, val: &JsonValue) -> Result<LuaValue, mlua::Error> {
    match val {
        JsonValue::Null => Ok(LuaValue::Nil),
        JsonValue::Bool(b) => Ok(LuaValue::Boolean(*b)),
        JsonValue::Number(n) => {
            if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                Ok(LuaValue::Integer(*n as i64))
            } else {
                Ok(LuaValue::Number(*n))
            }
        }
        JsonValue::String(s) => Ok(LuaValue::String(lua.create_string(s)?)),
        JsonValue::Array(arr) => {
            let t = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                t.set(i + 1, convert_json_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        JsonValue::Object(obj) => {
            let t = lua.create_table()?;
            for (k, v) in obj {
                t.set(k.as_str(), convert_json_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(t))
        }
    }
}

enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

struct JsonParser<'a> {
    s: &'a str,
    pos: usize,
    depth: i64,
    max_depth: i64,
}

impl<'a> JsonParser<'a> {
    fn new(s: &'a str, max_depth: i64) -> Self {
        JsonParser {
            s,
            pos: 0,
            depth: 0,
            max_depth,
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.s.len() {
            match self.s.as_bytes()[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, mlua::Error> {
        self.skip_ws();
        if self.pos >= self.s.len() {
            return Err(mlua::Error::external("Unexpected end of JSON"));
        }
        match self.s.as_bytes()[self.pos] {
            b'{' => {
                self.depth += 1;
                if self.depth > self.max_depth {
                    return Err(mlua::Error::external(
                        "Cannot deserialise, excessive nesting",
                    ));
                }
                let v = self.parse_object();
                self.depth -= 1;
                v
            }
            b'[' => {
                self.depth += 1;
                if self.depth > self.max_depth {
                    return Err(mlua::Error::external(
                        "Cannot deserialise, excessive nesting",
                    ));
                }
                let v = self.parse_array();
                self.depth -= 1;
                v
            }
            b'"' => self.parse_string().map(JsonValue::String),
            b't' => self.parse_literal("true", JsonValue::Bool(true)),
            b'f' => self.parse_literal("false", JsonValue::Bool(false)),
            b'n' => self.parse_literal("null", JsonValue::Null),
            b'-' | b'0'..=b'9' => self.parse_number(),
            c => Err(mlua::Error::external(format!(
                "Unexpected character '{}' in JSON",
                c as char
            ))),
        }
    }

    fn parse_literal(&mut self, lit: &str, val: JsonValue) -> Result<JsonValue, mlua::Error> {
        if self.s[self.pos..].starts_with(lit) {
            self.pos += lit.len();
            Ok(val)
        } else {
            Err(mlua::Error::external(format!(
                "Invalid JSON literal at position {}",
                self.pos
            )))
        }
    }

    fn parse_number(&mut self) -> Result<JsonValue, mlua::Error> {
        let start = self.pos;
        if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b'-' {
            self.pos += 1;
        }
        while self.pos < self.s.len() && self.s.as_bytes()[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b'.' {
            self.pos += 1;
            while self.pos < self.s.len() && self.s.as_bytes()[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        if self.pos < self.s.len()
            && (self.s.as_bytes()[self.pos] == b'e' || self.s.as_bytes()[self.pos] == b'E')
        {
            self.pos += 1;
            if self.pos < self.s.len()
                && (self.s.as_bytes()[self.pos] == b'+' || self.s.as_bytes()[self.pos] == b'-')
            {
                self.pos += 1;
            }
            while self.pos < self.s.len() && self.s.as_bytes()[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        let num_str = &self.s[start..self.pos];
        num_str
            .parse::<f64>()
            .map(JsonValue::Number)
            .map_err(|e| mlua::Error::external(format!("Invalid number '{}': {}", num_str, e)))
    }

    fn parse_string(&mut self) -> Result<String, mlua::Error> {
        if self.pos >= self.s.len() || self.s.as_bytes()[self.pos] != b'"' {
            return Err(mlua::Error::external("Expected string"));
        }
        self.pos += 1;
        let mut out = String::new();
        while self.pos < self.s.len() {
            let c = self.s.as_bytes()[self.pos];
            if c == b'"' {
                self.pos += 1;
                return Ok(out);
            }
            if c == b'\\' {
                self.pos += 1;
                if self.pos >= self.s.len() {
                    return Err(mlua::Error::external("Unterminated escape in JSON string"));
                }
                match self.s.as_bytes()[self.pos] {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000c}'),
                    b'u' => {
                        self.pos += 1;
                        if self.pos + 4 > self.s.len() {
                            return Err(mlua::Error::external("Invalid unicode escape"));
                        }
                        let hex = &self.s[self.pos..self.pos + 4];
                        let code = u32::from_str_radix(hex, 16)
                            .map_err(|e| mlua::Error::external(e.to_string()))?;
                        if let Some(ch) = char::from_u32(code) {
                            out.push(ch);
                        }
                        self.pos += 3;
                    }
                    _ => out.push(self.s.as_bytes()[self.pos] as char),
                }
            } else {
                out.push(c as char);
            }
            self.pos += 1;
        }
        Err(mlua::Error::external("Unterminated JSON string"))
    }

    fn parse_array(&mut self) -> Result<JsonValue, mlua::Error> {
        self.pos += 1;
        let mut arr = Vec::new();
        self.skip_ws();
        if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b']' {
            self.pos += 1;
            return Ok(JsonValue::Array(arr));
        }
        loop {
            arr.push(self.parse_value()?);
            self.skip_ws();
            if self.pos >= self.s.len() {
                return Err(mlua::Error::external("Unterminated JSON array"));
            }
            match self.s.as_bytes()[self.pos] {
                b',' => {
                    self.pos += 1;
                    self.skip_ws();
                }
                b']' => {
                    self.pos += 1;
                    return Ok(JsonValue::Array(arr));
                }
                c => {
                    return Err(mlua::Error::external(format!(
                        "Expected ',' or ']' in array, got '{}'",
                        c as char
                    )))
                }
            }
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, mlua::Error> {
        self.pos += 1;
        let mut obj = Vec::new();
        self.skip_ws();
        if self.pos < self.s.len() && self.s.as_bytes()[self.pos] == b'}' {
            self.pos += 1;
            return Ok(JsonValue::Object(obj));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            if self.pos >= self.s.len() || self.s.as_bytes()[self.pos] != b':' {
                return Err(mlua::Error::external("Expected ':' in object"));
            }
            self.pos += 1;
            let val = self.parse_value()?;
            obj.push((key, val));
            self.skip_ws();
            if self.pos >= self.s.len() {
                return Err(mlua::Error::external("Unterminated JSON object"));
            }
            match self.s.as_bytes()[self.pos] {
                b',' => {
                    self.pos += 1;
                }
                b'}' => {
                    self.pos += 1;
                    return Ok(JsonValue::Object(obj));
                }
                c => {
                    return Err(mlua::Error::external(format!(
                        "Expected ',' or '}}' in object, got '{}'",
                        c as char
                    )))
                }
            }
        }
    }
}
