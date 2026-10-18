mod client_admin;
mod client_control;
mod client_info;
mod client_tracking;

use self::client_admin::{
    client_caching, client_capa, client_import_source, client_no_touch, client_reply,
    client_setinfo,
};
use self::client_control::{client_no_evict, client_pause, client_setname};
use self::client_info::client_info;
use self::client_tracking::{client_tracking, client_trackinginfo};
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::{PauseAction, ServerContext};
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("CLIENT")]
pub fn client(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let sub = match args.args.first() {
        Some(s) => String::from_utf8_lossy(s).to_ascii_lowercase(),
        None => return CommandError::wrong_arity("client"),
    };

    match sub.as_str() {
        "setname" => client_setname(conn, args, ctx),
        "getname" => match conn.name().cloned() {
            Some(n) => Value::bulk(n),
            None => Value::Null,
        },
        "id" => Value::integer(conn.client_id() as i64),
        "info" => client_info(conn, ctx),
        "setinfo" => client_setinfo(conn, args, ctx),
        "capa" => client_capa(conn, args),
        "getaddr" => {
            let addr = conn.peer_addr().to_string();
            Value::bulk(Bytes::from(addr))
        }
        "no-touch" => client_no_touch(conn, args),
        "import-source" => client_import_source(conn, args, ctx),
        "reply" => client_reply(conn, args),
        "caching" => client_caching(conn, args),
        "tracking" => client_tracking(conn, args, ctx),
        "trackinginfo" => client_trackinginfo(conn),
        "getredir" => Value::integer(if conn.tracking() {
            conn.tracking_redirect().map(|id| id as i64).unwrap_or(0)
        } else {
            -1
        }),
        "no-evict" => client_no_evict(conn, args),
        "pause" => client_pause(args, ctx),
        "unpause" => {
            ctx.client_pause_mut().action = PauseAction::None;
            ctx.client_pause_mut().end_time_ms = 0;
            ctx.client_pause_mut().reason = "none";
            ctx.dbs_mut().set_pause_skip_expire_all(false);
            ctx.set_pending_unpause(true);
            Value::ok()
        }
        "help" => Value::bulk(Bytes::from(
            "CLIENT <subcommand> [<arg> [value] [opt] ...]. See https://valkey.io for details.",
        )),
        _ => Value::error("ERR unknown subcommand or wrong number of arguments. Try CLIENT HELP"),
    }
}
