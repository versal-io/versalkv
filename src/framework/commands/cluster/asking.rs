use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use command_macros::command;

#[command("ASKING")]
pub fn asking(conn: &mut dyn CommandConn, _args: &CommandArgs, _ctx: &mut ServerContext) -> Value {
    conn.set_asking(true);
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::capabilities::connection::Connection;
    use bytes::Bytes;

    #[test]
    fn asking_arms_the_connection_for_the_next_command() {
        let mut connection = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![Bytes::from_static(b"ASKING")]);
        let mut context = ServerContext::new(1);

        assert_eq!(asking(&mut connection, &args, &mut context), Value::ok());
        assert!(connection.asking());
    }
}
