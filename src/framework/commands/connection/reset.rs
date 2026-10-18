use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;

#[command("RESET")]
pub fn reset(conn: &mut dyn CommandConn, _args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if conn.monitoring() {
        conn.set_monitoring(false);
        ctx.unregister_monitor_client(conn.client_id());
    }
    conn.set_selected_db(0);
    conn.reset_parser();
    conn.set_authenticated(crate::engine::extension::acl::default_user_has_nopass());
    conn.set_username("default".to_string());
    conn.set_resp_version(2);
    conn.set_blocked(None);
    Value::SimpleString(Bytes::from_static(b"RESET"))
}
