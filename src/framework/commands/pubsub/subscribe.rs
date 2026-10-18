use crate::engine::distributed::pubsub::encode_confirm;
use crate::engine::distributed::sentinel::hello::{HelloMessage, SENTINEL_HELLO_CHANNEL};
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::ServerContext;
use crate::framework::distributed::sentinel::apply_hello_and_flush_config;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

fn confirm_value(kind: &[u8], channel: Option<Bytes>, count: i64) -> Value {
    let mut items = Vec::with_capacity(3);
    items.push(Value::Bulk(Bytes::copy_from_slice(kind)));
    match channel {
        Some(ch) => items.push(Value::Bulk(ch)),
        None => items.push(Value::Null),
    }
    items.push(Value::Integer(count));
    Value::Push(items)
}

#[command("SUBSCRIBE")]
pub fn subscribe(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("subscribe");
    }
    let resp3 = conn.resp_version() >= 3;
    let mut total = 0i64;
    for (i, ch) in args.args.iter().enumerate() {
        let count = ctx.pubsub_mut().subscribe(conn.token().0, ch.clone());
        conn.set_sub_channel_count(count);
        total = (count + conn.psub_channel_count()) as i64;
        if i > 0 {
            let frame = encode_confirm(b"subscribe", Some(ch), total, resp3);
            conn.queue_command_push_reply(frame);
        }
    }
    confirm_value(b"subscribe", Some(args.args[0].clone()), total)
}

#[command("UNSUBSCRIBE")]
pub fn unsubscribe(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let resp3 = conn.resp_version() >= 3;
    if args.args.is_empty() {
        let removed = ctx.pubsub_mut().unsubscribe_all(conn.token().0);
        conn.set_sub_channel_count(0);
        let total = (conn.psub_channel_count()) as i64;
        if removed.is_empty() {
            return confirm_value(b"unsubscribe", None, total);
        }
        let mut iter = removed.into_iter();
        let first = iter.next().unwrap();
        let total = (conn.sub_channel_count() + conn.psub_channel_count()) as i64;
        for ch in iter {
            let frame = encode_confirm(b"unsubscribe", Some(&ch), total, resp3);
            conn.queue_command_push_reply(frame);
        }
        return confirm_value(b"unsubscribe", Some(first), total);
    }
    let mut total = 0i64;
    for (i, ch) in args.args.iter().enumerate() {
        let count = ctx.pubsub_mut().unsubscribe(conn.token().0, ch);
        conn.set_sub_channel_count(count);
        total = (count + conn.psub_channel_count()) as i64;
        if i > 0 {
            let frame = encode_confirm(b"unsubscribe", Some(ch), total, resp3);
            conn.queue_command_push_reply(frame);
        }
    }
    confirm_value(b"unsubscribe", Some(args.args[0].clone()), total)
}

#[command("PSUBSCRIBE")]
pub fn psubscribe(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.is_empty() {
        return Value::error("ERR wrong number of arguments for 'psubscribe' command");
    }
    let resp3 = conn.resp_version() >= 3;
    for pattern in &args.args {
        let count = ctx.pubsub_mut().psubscribe(conn.token().0, pattern.clone());
        conn.set_psub_channel_count(count);
        let frame = encode_confirm(b"psubscribe", Some(pattern), count as i64, resp3);
        conn.queue_command_push_reply(frame);
    }
    conn.set_custom_reply(true);
    Value::null()
}

#[command("PUNSUBSCRIBE")]
pub fn punsubscribe(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let resp3 = conn.resp_version() >= 3;
    if args.args.is_empty() {
        let removed = ctx.pubsub_mut().punsubscribe_all(conn.token().0);
        conn.set_psub_channel_count(0);
        if removed.is_empty() {
            let frame = encode_confirm(b"punsubscribe", None, 0, resp3);
            conn.queue_command_push_reply(frame);
        } else {
            for pat in removed {
                let frame = encode_confirm(b"punsubscribe", Some(&pat), 0, resp3);
                conn.queue_command_push_reply(frame);
            }
        }
    } else {
        for pattern in &args.args {
            let count = ctx.pubsub_mut().punsubscribe(conn.token().0, pattern);
            conn.set_psub_channel_count(count);
            let frame = encode_confirm(b"punsubscribe", Some(pattern), count as i64, resp3);
            conn.queue_command_push_reply(frame);
        }
    }
    conn.set_custom_reply(true);
    Value::null()
}

#[command("PUBLISH")]
pub fn publish(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("publish");
    }
    let channel = args.args[0].clone();
    let message = args.args[1].clone();
    if ctx.sentinel_state_mut().sentinel_mode
        && channel.as_ref() == SENTINEL_HELLO_CHANNEL.as_bytes()
    {
        if let Ok(payload) = std::str::from_utf8(&message) {
            if let Some(hello) = HelloMessage::parse(payload) {
                apply_hello_and_flush_config(ctx, &hello, mstime());
            }
        }
    }
    let receivers =
        ctx.pubsub_mut().subscribers(&channel).len() + ctx.pubsub_mut().pattern_subscribers(&channel).len();
    conn.record_notification((channel.clone(), message.clone()));
    conn.record_cluster_notification((channel, message));
    Value::integer(receivers as i64)
}
