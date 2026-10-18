use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

pub(crate) fn route_channels(args: &CommandArgs) -> Vec<Vec<u8>> {
    args.args
        .first()
        .map(|channel| vec![channel.to_vec()])
        .unwrap_or_default()
}

#[command("SPUBLISH")]
pub fn spublish(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("spublish");
    }
    let channel = args.args[0].clone();
    let message = args.args[1].clone();
    let receivers = ctx.pubsub_mut().shard_subscribers(&channel).len();
    conn.record_shard_notification((channel.clone(), message.clone()));
    conn.record_cluster_shard_notification((channel, message));
    Value::integer(receivers as i64)
}
