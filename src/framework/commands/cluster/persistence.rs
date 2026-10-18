use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;

pub(super) fn cluster_saveconfig(conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    if ctx.cluster_state().is_none() {
        return Value::error("ERR This instance has cluster support disabled");
    }
    conn.request_cluster_config_save();
    conn.set_custom_reply(true);
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::cluster_saveconfig;
    use crate::engine::distributed::cluster::ClusterState;
    use crate::engine::command::context::CommandConn;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use crate::framework::capabilities::connection::{CommandEffectDrain, Connection};

    #[test]
    fn saveconfig_registers_runtime_io_intent_without_a_handler_reply() {
        let mut connection = Connection::new_fake();
        connection.set_custom_reply(false);
        let mut context = ServerContext::new(1);
        *context.cluster_state_mut() = Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16379)));

        assert_eq!(
            cluster_saveconfig(&mut connection, &mut context),
            Value::Null
        );
        assert!(connection.custom_reply());
        let mut effects = connection
            .take_command_effects(CommandEffectDrain::Completed)
            .expect("saveconfig command effects");
        assert_eq!(effects.take_cluster_config_save(), Some(Value::ok()));
    }
}
