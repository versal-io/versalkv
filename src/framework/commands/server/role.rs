use crate::framework::CommandArgs;
use crate::framework::context::ServerContext;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;
use command_macros::command;

#[command("ROLE")]
pub fn role(_conn: &mut dyn CommandConn, _args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if ctx.sentinel_state_mut().sentinel_mode {
        let primary_names: Vec<Value> = ctx
            .sentinel_state_mut()
            .registry
            .master_names()
            .map(|name| Value::bulk(Bytes::copy_from_slice(name.as_bytes())))
            .collect();
        return Value::Array(vec![
            Value::bulk(Bytes::from_static(b"sentinel")),
            Value::Array(primary_names),
        ]);
    }
    let replication_config = &ctx.config().replication;
    if ctx.replication().is_replica() {
        let master_host = replication_config.master_host.as_deref().unwrap_or("");
        let state = match replication_config.master_link_status.as_str() {
            "up" => "connected",
            other => other,
        };
        return Value::Array(vec![
            Value::bulk(Bytes::from_static(b"slave")),
            Value::bulk(Bytes::copy_from_slice(master_host.as_bytes())),
            Value::integer(replication_config.master_port as i64),
            Value::bulk(Bytes::copy_from_slice(state.as_bytes())),
            Value::integer(ctx.replication().slave_repl_offset.get()),
        ]);
    }
    primary_role(ctx)
}

fn primary_role(ctx: &ServerContext) -> Value {
    // C's roleCommand walks `server.replicas` in PSYNC acceptance order
    // (replication.c:4727), which `replica_order` mirrors. Connection-id
    // sorting would renumber entries when concurrent connects register in a
    // different order than their PSYNC is accepted.
    let view = &ctx.replication().view;
    let replicas = view
        .replica_order
        .iter()
        .filter_map(|id| view.replica_info.get(id))
        .filter(|info| info.state == "online")
        .map(|info| {
            Value::Array(vec![
                Value::bulk(Bytes::copy_from_slice(info.ip.as_bytes())),
                Value::integer(info.port as i64),
                Value::integer(info.offset.map_or(-1, |offset| offset.get())),
            ])
        })
        .collect();
    Value::Array(vec![
        Value::bulk(Bytes::from_static(b"master")),
        Value::integer(ctx.replication().primary_repl_offset().get()),
        Value::Array(replicas),
    ])
}

#[cfg(test)]
mod tests {
    use super::primary_role;
    use crate::engine::distributed::replication::ReplicaStatus;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    fn offset(value: i64) -> crate::engine::distributed::replication::ReplicationOffset {
        crate::engine::distributed::replication::ReplicationOffset::try_from(value)
            .expect("valid test offset")
    }

    #[test]
    fn primary_role_lists_connected_replicas() {
        let mut context = ServerContext::new(1);
        let state = context.replication_mut();
        state.view.replica_order = vec![7];
        state.view.replica_info.insert(
            7,
            ReplicaStatus {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                offset: Some(offset(42)),
                state: "online".to_string(),
                lag_ms: 0,
            },
        );

        assert_eq!(
            primary_role(&context),
            Value::Array(vec![
                Value::bulk(Bytes::from_static(b"master")),
                Value::integer(0),
                Value::Array(vec![Value::Array(vec![
                    Value::bulk(Bytes::from_static(b"127.0.0.1")),
                    Value::integer(6380),
                    Value::integer(42),
                ])]),
            ])
        );
    }

    #[test]
    fn primary_role_follows_psync_acceptance_order_not_connection_id() {
        // C's roleCommand walks server.replicas (PSYNC acceptance order,
        // replication.c:4727), not connection-id order. A replica accepted
        // later can own a smaller connection id, so connection-id sorting
        // would renumber the ROLE array relative to C.
        let mut context = ServerContext::new(1);
        let view = context.replication_mut();
        view.view.replica_order = vec![9, 7];
        view.view.replica_info.insert(
            7,
            ReplicaStatus {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                offset: Some(offset(42)),
                state: "online".to_string(),
                lag_ms: 0,
            },
        );
        view.view.replica_info.insert(
            9,
            ReplicaStatus {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                offset: Some(offset(43)),
                state: "online".to_string(),
                lag_ms: 0,
            },
        );

        let Value::Array(sections) = primary_role(&context) else {
            panic!("primary ROLE must be an array");
        };
        let Value::Array(replicas) = &sections[2] else {
            panic!("ROLE replicas must be an array");
        };
        let ports: Vec<i64> = replicas
            .iter()
            .map(|replica| {
                let Value::Array(fields) = replica else {
                    panic!("replica entry must be an array");
                };
                match &fields[1] {
                    Value::Integer(port) => *port,
                    other => panic!("port must be an integer, got {other:?}"),
                }
            })
            .collect();
        assert_eq!(ports, vec![6381, 6380]);
    }
}
