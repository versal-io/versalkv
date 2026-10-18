use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::engine::distributed::pubsub::encode_confirm;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;

#[command("SUNSUBSCRIBE")]
pub fn sunsubscribe(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let resp3 = conn.resp_version() >= 3;
    if args.args.is_empty() {
        let removed = ctx.pubsub_mut().sunsubscribe_all(conn.token().0);
        conn.set_ssub_channel_count(0);
        let total = 0;
        if removed.is_empty() {
            let items = vec![
                Value::Bulk(Bytes::from_static(b"sunsubscribe")),
                Value::Null,
                Value::Integer(total),
            ];
            return Value::Push(items);
        }
        let mut iter = removed.into_iter();
        let first = iter.next().unwrap();
        let total = 0;
        for ch in iter {
            let frame = encode_confirm(b"sunsubscribe", Some(&ch), total, resp3);
            conn.queue_command_push_reply(frame);
        }
        let items = vec![
            Value::Bulk(Bytes::from_static(b"sunsubscribe")),
            Value::Bulk(first),
            Value::Integer(total),
        ];
        return Value::Push(items);
    }
    let mut total = 0i64;
    for (i, ch) in args.args.iter().enumerate() {
        let count = ctx.pubsub_mut().sunsubscribe(conn.token().0, ch);
        conn.set_ssub_channel_count(count);
        total = count as i64;
        if i > 0 {
            let frame = encode_confirm(b"sunsubscribe", Some(ch), total, resp3);
            conn.queue_command_push_reply(frame);
        }
    }
    let items = vec![
            Value::Bulk(Bytes::from_static(b"sunsubscribe")),
            Value::Bulk(args.args[0].clone()),
            Value::Integer(total),
        ];
        Value::Push(items)
}
