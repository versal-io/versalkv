use bytes::Bytes;

use crate::foundation::io::protocol::Value;

#[derive(Clone)]
pub struct CommandArgs {
    pub name: Bytes,
    pub args: Vec<Bytes>,
}

impl CommandArgs {
    pub fn from_parts(parts: Vec<Bytes>) -> Self {
        let mut iter = parts.into_iter();
        let name = iter.next().unwrap_or_else(|| Bytes::from_static(b""));
        Self {
            name,
            args: iter.collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.args.len()
    }

    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    pub fn parse<T: std::str::FromStr>(&self, i: usize) -> Result<T, Value> {
        let s = std::str::from_utf8(&self.args[i])
            .map_err(|_| Value::error("ERR value is not an integer or out of range"))?;
        s.parse::<T>()
            .map_err(|_| Value::error("ERR value is not an integer or out of range"))
    }

    pub fn parse_positive<T: std::str::FromStr + PartialOrd + From<u8>>(
        &self,
        i: usize,
        name: &str,
    ) -> Result<T, Value> {
        let s = std::str::from_utf8(&self.args[i])
            .map_err(|_| Value::error(format!("ERR {} is not an integer or out of range", name)))?;
        let val: T = s
            .parse::<T>()
            .map_err(|_| Value::error(format!("ERR {} is not an integer or out of range", name)))?;
        if val <= T::from(0u8) {
            return Err(Value::error(format!(
                "ERR {} should be greater than 0",
                name
            )));
        }
        Ok(val)
    }

    #[cfg(not(feature = "wasm"))]
    pub fn parse_timeout_ms(&self, i: usize) -> Result<u64, Value> {
        let s = std::str::from_utf8(&self.args[i])
            .map_err(|_| Value::error("ERR timeout is not a float or out of range"))?;
        let secs: f64 = parse_hex_or_float(s)
            .ok_or(Value::error("ERR timeout is not a float or out of range"))?;
        if secs < 0.0 {
            return Err(Value::error("ERR timeout is not a float or out of range"));
        }
        let ms = (secs * 1000.0) as u64;
        if ms > i32::MAX as u64 {
            return Err(Value::error("ERR timeout is out of range"));
        }
        Ok(ms)
    }
}

#[cfg(not(feature = "wasm"))]
pub fn parse_hex_or_float(s: &str) -> Option<f64> {
    if let Ok(v) = s.parse::<f64>() {
        return Some(v);
    }
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        if let Ok(v) = i64::from_str_radix(hex, 16) {
            return Some(v as f64);
        }
    }
    None
}

pub struct CommandError;

impl CommandError {
    pub fn wrong_arity(cmd: &str) -> Value {
        Value::error(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd
        ))
    }
    pub fn unknown(cmd: &[u8]) -> Value {
        Value::error(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(cmd)
        ))
    }
    pub fn syntax_error() -> Value {
        Value::error("ERR syntax error")
    }
    pub fn db_index_out_of_range() -> Value {
        Value::error("ERR DB index is out of range")
    }
    pub fn wrong_type() -> Value {
        Value::error("WRONGTYPE Operation against a key holding the wrong kind of value")
    }
}

cfg_if::cfg_if! {
    if #[cfg(not(feature = "wasm"))] {
        impl CommandArgs {
            pub fn from_parts_reusing(mut parts: Vec<Bytes>) -> Self {
                if parts.is_empty() {
                    return Self::from_parts(parts);
                }
                let name = parts.remove(0);
                Self { name, args: parts }
            }

            pub fn into_parts_reusing(self) -> Vec<Bytes> {
                let Self { name, mut args } = self;
                args.insert(0, name);
                args
            }

        }

        impl CommandError {
            pub fn noauth() -> Value {
                Value::error("NOAUTH Authentication required.")
            }

            pub fn no_password_set() -> Value {
                Value::error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
            }

            pub fn wrongpass() -> Value {
                Value::error("WRONGPASS invalid username-password pair or user is disabled.")
            }
        }
    }
}
