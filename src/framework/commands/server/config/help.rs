use crate::foundation::io::protocol::Value;
use bytes::Bytes;

pub fn config_help() -> Value {
    Value::bulk(Bytes::from(
        "CONFIG <subcommand> [<arg> [value] [opt] ...]. See https://valkey.io for details.",
    ))
}