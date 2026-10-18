use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::framework::observability::commandlog::{CAT_LARGE_REPLY, CAT_LARGE_REQUEST, CAT_SLOW};
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;
use std::str;

#[command("COMMANDLOG")]
pub fn commandlog_cmd(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("commandlog");
    }
    let sub = String::from_utf8_lossy(&args.args[0]).to_ascii_lowercase();
    match sub.as_str() {
        "get" => cmdlog_get(conn, args, ctx),
        "len" => cmdlog_len(args, ctx),
        "reset" => cmdlog_reset(args, ctx),
        "help" => cmdlog_help(),
        _ => Value::error("ERR unknown subcommand. Try COMMANDLOG HELP."),
    }
}

fn parse_category(arg: &Bytes) -> Result<&'static str, Value> {
    let cat = String::from_utf8_lossy(arg).to_ascii_lowercase();
    match cat.as_str() {
        "slow" => Ok(CAT_SLOW),
        "large-request" => Ok(CAT_LARGE_REQUEST),
        "large-reply" => Ok(CAT_LARGE_REPLY),
        _ => Err(Value::error(
            "ERR type should be one of the following: slow, large-request, large-reply",
        )),
    }
}

fn parse_count(arg: Option<&Bytes>) -> Result<i64, Value> {
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

fn cmdlog_get(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return Value::error("ERR syntax error");
    }
    let (count_arg_idx, cat_arg_idx) = if args.args.len() == 2 {
        (None, 1)
    } else {
        (Some(1), 2)
    };
    let count = match count_arg_idx {
        Some(i) => match parse_count(Some(&args.args[i])) {
            Ok(n) => n,
            Err(e) => return e,
        },
        None => 10,
    };
    let cat_key = match parse_category(&args.args[cat_arg_idx]) {
        Ok(k) => k,
        Err(e) => return e,
    };
    let sl = ctx.commandlog_mut().category(cat_key).unwrap();
    let entries = sl.get(count);
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
            Value::bulk(Bytes::from(e.client_addr.clone())),
            client_name,
        ]));
    }
    Value::Array(out)
}

fn cmdlog_reset(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return Value::error("ERR syntax error");
    }
    let cat_key = match parse_category(&args.args[1]) {
        Ok(k) => k,
        Err(e) => return e,
    };
    let sl = ctx.commandlog_mut().category(cat_key).unwrap();
    sl.reset();
    Value::ok()
}

fn cmdlog_len(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return Value::error("ERR syntax error");
    }
    let cat_key = match parse_category(&args.args[1]) {
        Ok(k) => k,
        Err(e) => return e,
    };
    let n = ctx.commandlog_mut().category(cat_key).unwrap().len();
    Value::Integer(n as i64)
}

fn cmdlog_help() -> Value {
    let lines = vec![
        "COMMANDLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET <type> [<count>]",
        "    Return top <count> entries from the specified command log.",
        "LEN <type>",
        "    Return the length of the specified command log.",
        "RESET",
        "    Reset all command logs.",
        "HELP",
        "    Prints this help.",
    ];
    let arr: Vec<Value> = lines
        .into_iter()
        .map(|s| Value::bulk(Bytes::from(s.as_bytes().to_vec())))
        .collect();
    Value::Array(arr)
}
