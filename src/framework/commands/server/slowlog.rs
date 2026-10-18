use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;
use std::str;

#[command("SLOWLOG")]
pub fn slowlog_cmd(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("slowlog");
    }
    let sub = String::from_utf8_lossy(&args.args[0]).to_ascii_lowercase();
    match sub.as_str() {
        "get" => {
            let count = match parse_count(args.args.get(1)) {
                Ok(n) => n,
                Err(e) => return e,
            };
            let entries = ctx.slowlog().get(count);
            entries_to_value(entries)
        }
        "len" => Value::Integer(ctx.slowlog().len() as i64),
        "reset" => {
            ctx.slowlog_mut().reset();
            Value::ok()
        }
        "help" => slowlog_help(),
        _ => Value::error("ERR unknown subcommand. Try SLOWLOG HELP."),
    }
}

pub(crate) fn parse_count(arg: Option<&Bytes>) -> Result<i64, Value> {
    match arg {
        Some(v) => match str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) if n < -1 => Err(Value::error(
                "ERR count should be greater than or equal to -1",
            )),
            Some(n) => Ok(n),
            None => Err(Value::error("ERR value is not an integer or out of range")),
        },
        None => Ok(10),
    }
}

pub(crate) fn entries_to_value(entries: Vec<crate::framework::observability::slowlog::SlowLogEntry>) -> Value {
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let mut argv_vals = Vec::with_capacity(e.argv.len());
        for a in &e.argv {
            argv_vals.push(Value::bulk(a.clone()));
        }
        let client_name = match &e.client_name {
            Some(n) => Value::bulk(n.clone()),
            None => Value::SimpleString(Bytes::from_static(b"")),
        };
        out.push(Value::Array(vec![
            Value::Integer(e.id as i64),
            Value::Integer(e.ts_sec as i64),
            Value::Integer(e.duration),
            Value::Array(argv_vals),
            Value::bulk(Bytes::from(e.client_addr)),
            client_name,
        ]));
    }
    Value::Array(out)
}

fn slowlog_help() -> Value {
    let lines = vec![
        "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET [<count>]",
        "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
        "LEN",
        "    Return the length of the slowlog.",
        "RESET",
        "    Reset the slowlog.",
        "HELP",
        "    Prints this help.",
    ];
    let arr: Vec<Value> = lines
        .into_iter()
        .map(|s| Value::bulk(Bytes::from(s.as_bytes().to_vec())))
        .collect();
    Value::Array(arr)
}
