use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;

#[command("PUBSUB")]
pub fn pubsub(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("pubsub");
    }
    let subcmd = String::from_utf8_lossy(&args.args[0]).to_lowercase();
    match subcmd.as_str() {
        "channels" => pubsub_channels(conn, args, ctx),
        "numpat" => pubsub_numpat(conn, args, ctx),
        "numsub" => pubsub_numsub(conn, args, ctx),
        "shardchannels" => pubsub_shardchannels(conn, args, ctx),
        "shardnumsub" => pubsub_shardnumsub(conn, args, ctx),
        "help" => pubsub_help(),
        _ => Value::error(format!("ERR Unknown subcommand '{}'", subcmd)),
    }
}

fn pubsub_channels(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let channels = ctx.pubsub_mut().active_channels();
    let pattern = if args.args.len() > 1 {
        Some(&args.args[1][..])
    } else {
        None
    };
    let mut result = Vec::new();
    for ch in channels {
        match pattern {
            Some(pat) => {
                if crate::engine::distributed::pubsub::pattern_match(pat, &ch) {
                    result.push(Value::Bulk(ch));
                }
            }
            None => result.push(Value::Bulk(ch)),
        }
    }
    Value::Array(result)
}

fn pubsub_numpat(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    Value::integer(ctx.pubsub_mut().pattern_count() as i64)
}

fn pubsub_numsub(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() <= 1 {
        return Value::Array(vec![]);
    }
    let mut result = Vec::with_capacity((args.args.len() - 1) * 2);
    for ch in &args.args[1..] {
        result.push(Value::Bulk(ch.clone()));
        result.push(Value::integer(ctx.pubsub_mut().subscriber_count(ch) as i64));
    }
    Value::Array(result)
}

fn pubsub_shardchannels(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let channels = ctx.pubsub_mut().active_shard_channels();
    if args.args.len() <= 1 {
        let mut result = Vec::with_capacity(channels.len());
        for ch in channels {
            result.push(Value::Bulk(ch));
        }
        return Value::Array(result);
    }
    let pat = &args.args[1];
    let mut result = Vec::new();
    for ch in &channels {
        if crate::engine::distributed::pubsub::pattern_match(pat, ch) {
            result.push(Value::Bulk(ch.clone()));
        }
    }
    Value::Array(result)
}

fn pubsub_shardnumsub(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() <= 1 {
        return Value::Array(vec![]);
    }
    let mut result = Vec::with_capacity((args.args.len() - 1) * 2);
    for ch in &args.args[1..] {
        result.push(Value::Bulk(ch.clone()));
        result.push(Value::integer(ctx.pubsub_mut().shard_subscriber_count(ch) as i64));
    }
    Value::Array(result)
}

fn pubsub_help() -> Value {
    let help = vec![
        Value::Bulk(Bytes::from_static(b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands:")),
        Value::Bulk(Bytes::from_static(b"CHANNELS [pattern]      -- Return the list of active channels")),
        Value::Bulk(Bytes::from_static(b"NUMPAT                   -- Return the number of pattern subscriptions")),
        Value::Bulk(Bytes::from_static(b"NUMSUB [channel ...]     -- Return the number of subscribers for channels")),
        Value::Bulk(Bytes::from_static(b"SHARDCHANNELS [pattern]  -- Return the list of active shard channels")),
        Value::Bulk(Bytes::from_static(b"SHARDNUMSUB [shardchannel ...] -- Return the number of subscribers for shard channels")),
        Value::Bulk(Bytes::from_static(b"HELP                     -- Print this help message")),
    ];
    Value::Array(help)
}
