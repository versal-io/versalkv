use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::commands::command_runtime::execute_server_command;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("RESTORE-ASKING")]
pub fn restore_asking(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let was_import_source = conn.import_source();
    conn.set_import_source(true);
    let reply = execute_server_command(
        conn,
        args,
        ctx,
        crate::framework::commands::generic::restore::restore,
    );
    conn.set_import_source(was_import_source);
    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::capabilities::connection::Connection;
    use bytes::Bytes;

    #[test]
    fn delegates_restore_argument_validation() {
        let mut connection = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"RESTORE-ASKING")]);
        let mut context = ServerContext::new(1);

        let reply = restore_asking(&mut connection, &args, &mut context);

        assert!(matches!(reply, Value::Error(message) if message.as_ref().windows(25).any(|part| part == b"wrong number of arguments")));
    }
}
