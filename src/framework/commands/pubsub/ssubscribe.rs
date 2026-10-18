use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::engine::distributed::pubsub::encode_confirm;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;

pub(crate) fn route_channels(args: &CommandArgs) -> Vec<Vec<u8>> {
    args.args.iter().map(|channel| channel.to_vec()).collect()
}

#[command("SSUBSCRIBE")]
pub fn ssubscribe(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("ssubscribe");
    }
    let resp3 = conn.resp_version() >= 3;
    let mut total = 0i64;
    for (i, ch) in args.args.iter().enumerate() {
        let count = ctx.pubsub_mut().ssubscribe(conn.token().0, ch.clone());
        conn.set_ssub_channel_count(count);
        total = count as i64;
        if i > 0 {
            let frame = encode_confirm(b"ssubscribe", Some(ch), total, resp3);
            conn.queue_command_push_reply(frame);
        }
    }
    let items = vec![
        Value::Bulk(Bytes::from_static(b"ssubscribe")),
        Value::Bulk(args.args[0].clone()),
        Value::Integer(total),
    ];
    Value::Push(items)
}

#[cfg(test)]
mod tests {
    use super::ssubscribe;
    use crate::engine::command::context::CommandConn;
    use crate::engine::command::CommandArgs;
    use crate::framework::context::ServerContext;
    use crate::framework::capabilities::connection::Connection;
    use bytes::Bytes;

    #[test]
    fn handler_keeps_regular_and_shard_subscription_counts_separate() {
        let mut conn = Connection::new_fake();
        conn.sub_channel_count = 2;
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SSUBSCRIBE"),
            Bytes::from_static(b"shard"),
        ]);
        let mut ctx = ServerContext::new(1);

        let _ = ssubscribe(&mut conn, &args, &mut ctx);

        assert_eq!(conn.sub_channel_count(), 2);
        assert_eq!(conn.ssub_channel_count(), 1);
    }
}
