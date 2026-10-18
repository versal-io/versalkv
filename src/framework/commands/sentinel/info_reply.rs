use std::net::ToSocketAddrs;

use bytes::Bytes;

use crate::engine::distributed::sentinel::{
    split_sentinel_endpoint, DownState, InstanceRole, SentinelInstance, SentinelInstanceId,
};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;

pub(super) fn instance_reply(
    instance: &SentinelInstance,
    now: i64,
    announce_hostnames: bool,
) -> Value {
    let mut fields = Vec::new();
    fields.push((
        Value::bulk("name"),
        Value::bulk(instance.name.as_bytes().to_vec()),
    ));
    let endpoint = split_sentinel_endpoint(&instance.addr);
    let configured_host = endpoint
        .map(|endpoint| endpoint.host)
        .unwrap_or(&instance.addr);
    let rendered_host = if announce_hostnames {
        configured_host.to_string()
    } else {
        (configured_host, 0_u16)
            .to_socket_addrs()
            .ok()
            .and_then(|addrs| {
                let mut fallback = None;
                for addr in addrs {
                    if addr.is_ipv4() {
                        return Some(addr.ip().to_string());
                    }
                    fallback.get_or_insert(addr.ip().to_string());
                }
                fallback
            })
            .unwrap_or_else(|| configured_host.to_string())
    };
    fields.push((
        Value::bulk("ip"),
        Value::bulk(rendered_host.as_bytes().to_vec()),
    ));
    fields.push((
        Value::bulk("port"),
        Value::Integer(endpoint.map_or(0, |endpoint| i64::from(endpoint.port))),
    ));
    fields.push((
        Value::bulk("runid"),
        Value::bulk(instance.runid.as_bytes().to_vec()),
    ));

    let mut flags = String::new();
    if instance.down_state == DownState::SDown {
        flags.push_str("s_down,");
    }
    if instance.down_state == DownState::ODown {
        flags.push_str("o_down,");
    }
    if instance.link_state != 0 {
        flags.push_str("disconnected,");
    }
    match instance.role {
        InstanceRole::Master => flags.push_str("master,"),
        InstanceRole::Replica => flags.push_str("slave,"),
        InstanceRole::Sentinel => flags.push_str("sentinel,"),
    }
    if !flags.is_empty() {
        flags.truncate(flags.len() - 1);
    }
    fields.push((Value::bulk("flags"), Value::bulk(flags.as_bytes().to_vec())));
    fields.push((
        Value::bulk("link-pending-commands"),
        Value::Integer(instance.pending_commands),
    ));
    fields.push((Value::bulk("link-refcount"), Value::Integer(0)));

    if instance.failover_state != crate::engine::distributed::sentinel::FailoverState::None {
        fields.push((
            Value::bulk("failover-state"),
            Value::bulk(failover_state_str(instance.failover_state)),
        ));
    }

    fields.push((
        Value::bulk("last-ping-sent"),
        Value::Integer(elapsed(now, instance.last_ping_sent)),
    ));
    fields.push((
        Value::bulk("last-ok-ping-reply"),
        Value::Integer(elapsed(now, instance.last_ping_ok)),
    ));
    fields.push((
        Value::bulk("last-ping-reply"),
        Value::Integer(elapsed(now, instance.last_ping_time)),
    ));

    if matches!(instance.down_state, DownState::SDown | DownState::ODown) {
        fields.push((
            Value::bulk("s-down-time"),
            Value::Integer(elapsed(now, instance.sdown_since_time)),
        ));
    }
    if instance.down_state == DownState::ODown {
        fields.push((
            Value::bulk("o-down-time"),
            Value::Integer(elapsed(now, instance.odown_since_time)),
        ));
    }
    fields.push((
        Value::bulk("down-after-milliseconds"),
        Value::Integer(instance.down_after_ms),
    ));

    if matches!(instance.role, InstanceRole::Master | InstanceRole::Replica) {
        fields.push((
            Value::bulk("info-refresh"),
            Value::Integer(elapsed(now, instance.info_refresh)),
        ));
        let role_reported = match instance.role_reported {
            InstanceRole::Master => "master",
            InstanceRole::Replica => "slave",
            InstanceRole::Sentinel => "unknown",
        };
        fields.push((
            Value::bulk("role-reported"),
            Value::bulk(role_reported.as_bytes().to_vec()),
        ));
        fields.push((
            Value::bulk("role-reported-time"),
            Value::Integer(instance.role_reported_time),
        ));
    }

    match instance.role {
        InstanceRole::Master => append_master_fields(&mut fields, instance),
        InstanceRole::Replica => append_replica_fields(&mut fields, instance),
        InstanceRole::Sentinel => append_sentinel_fields(&mut fields, instance, now),
    }
    Value::Map(fields)
}

fn elapsed(now: i64, timestamp: i64) -> i64 {
    if timestamp > 0 {
        now - timestamp
    } else {
        0
    }
}

fn failover_state_str(state: crate::engine::distributed::sentinel::FailoverState) -> &'static str {
    match state {
        crate::engine::distributed::sentinel::FailoverState::None => "none",
        crate::engine::distributed::sentinel::FailoverState::WaitStart => "wait_start",
        crate::engine::distributed::sentinel::FailoverState::SelectReplica => "select_replica",
        crate::engine::distributed::sentinel::FailoverState::SendReplicaOfNoOne => "send_replicaof_no_one",
        crate::engine::distributed::sentinel::FailoverState::SendFailover => "send_failover",
        crate::engine::distributed::sentinel::FailoverState::WaitPromotion => "wait_promotion",
        crate::engine::distributed::sentinel::FailoverState::ReconfigureReplicas => "reconfigure_replicas",
        crate::engine::distributed::sentinel::FailoverState::UpdateConfig => "update_config",
    }
}

fn append_master_fields(fields: &mut Vec<(Value, Value)>, instance: &SentinelInstance) {
    fields.push((
        Value::bulk("config-epoch"),
        Value::Integer(instance.config_epoch),
    ));
    fields.push((
        Value::bulk("num-slaves"),
        Value::Integer(instance.replicas.len() as i64),
    ));
    fields.push((
        Value::bulk("num-other-sentinels"),
        Value::Integer(instance.sentinels.len() as i64),
    ));
    fields.push((Value::bulk("quorum"), Value::Integer(instance.quorum)));
    fields.push((
        Value::bulk("failover-timeout"),
        Value::Integer(instance.failover_timeout),
    ));
    fields.push((
        Value::bulk("parallel-syncs"),
        Value::Integer(instance.parallel_syncs),
    ));
    if !instance.notification_script.is_empty() {
        fields.push((
            Value::bulk("notification-script"),
            Value::bulk(instance.notification_script.as_bytes().to_vec()),
        ));
    }
    if !instance.client_reconfig_script.is_empty() {
        fields.push((
            Value::bulk("client-reconfig-script"),
            Value::bulk(instance.client_reconfig_script.as_bytes().to_vec()),
        ));
    }
}

fn append_replica_fields(fields: &mut Vec<(Value, Value)>, instance: &SentinelInstance) {
    fields.push((Value::bulk("master-link-down-time"), Value::Integer(0)));
    fields.push((
        Value::bulk("master-link-status"),
        Value::bulk(b"ok".to_vec()),
    ));
    fields.push((
        Value::bulk("master-host"),
        Value::bulk(Bytes::from_static(b"?")),
    ));
    fields.push((Value::bulk("master-port"), Value::Integer(0)));
    fields.push((
        Value::bulk("slave-priority"),
        Value::Integer(instance.slave_priority),
    ));
    fields.push((
        Value::bulk("slave-repl-offset"),
        Value::Integer(instance.repl_offset),
    ));
    fields.push((
        Value::bulk("replica-announced"),
        Value::Integer(i64::from(instance.replica_announced)),
    ));
}

fn append_sentinel_fields(fields: &mut Vec<(Value, Value)>, instance: &SentinelInstance, now: i64) {
    fields.push((
        Value::bulk("last-hello-message"),
        Value::Integer(elapsed(now, instance.last_hello_sent)),
    ));
    fields.push((
        Value::bulk("voted-leader"),
        Value::bulk(instance.leader.as_bytes().to_vec()),
    ));
    fields.push((
        Value::bulk("voted-leader-epoch"),
        Value::Integer(instance.leader_epoch),
    ));
}

pub(super) fn master_reply_instance(
    master: &SentinelInstance,
    monitored: Option<&SentinelInstance>,
) -> SentinelInstance {
    let mut reply = master.clone();
    if let Some(monitored) = monitored {
        reply.link_state = monitored.link_state;
        reply.pending_commands = monitored.pending_commands;
        reply.last_ping_sent = monitored.last_ping_sent;
        reply.last_ping_ok = monitored.last_ping_ok;
        reply.last_ping_time = monitored.last_ping_time;
        reply.info_refresh = monitored.info_refresh;
        reply.role_reported = monitored.role_reported;
        reply.role_reported_time = monitored.role_reported_time;
    }
    reply
}

pub(super) fn master_ids(
    ctx: &ServerContext,
    name: &str,
    replicas: bool,
) -> Option<Vec<SentinelInstanceId>> {
    let master = ctx.sentinel_state().registry.master(name)?;
    Some(if replicas {
        master.replicas.clone()
    } else {
        master.sentinels.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::instance_reply;
    use crate::engine::distributed::sentinel::SentinelInstance;
    use crate::foundation::io::protocol::Value;

    fn instance_ip(instance: &SentinelInstance, announce_hostnames: bool) -> String {
        let Value::Map(fields) = instance_reply(instance, 1_000, announce_hostnames) else {
            panic!("instance reply should be a map");
        };
        fields
            .iter()
            .find_map(|(key, value)| match (key, value) {
                (Value::Bulk(key), Value::Bulk(value)) if key.as_ref() == b"ip" => {
                    Some(String::from_utf8_lossy(value).to_string())
                }
                _ => None,
            })
            .expect("ip field should exist")
    }

    fn integer_field(instance: &SentinelInstance, name: &[u8]) -> i64 {
        let Value::Map(fields) = instance_reply(instance, 1_000, false) else {
            panic!("instance reply should be a map");
        };
        fields
            .iter()
            .find_map(|(key, value)| match (key, value) {
                (Value::Bulk(key), Value::Integer(value)) if key.as_ref() == name => Some(*value),
                _ => None,
            })
            .expect("integer field should exist")
    }

    #[test]
    fn instance_ip_respects_announce_hostnames_setting() {
        let instance = SentinelInstance::new_master(1, "primary", "localhost:6379", 1);
        assert_eq!(instance_ip(&instance, true), "localhost");
        assert_eq!(instance_ip(&instance, false), "127.0.0.1");
    }

    #[test]
    fn ipv6_instance_reply_uses_bare_ip_and_separate_port() {
        let instance = SentinelInstance::new_master(1, "primary", "[::1]:6379", 1);
        assert_eq!(instance_ip(&instance, true), "::1");
        assert_eq!(instance_ip(&instance, false), "::1");
        assert_eq!(integer_field(&instance, b"port"), 6379);
    }

    #[test]
    fn disconnected_link_is_reported_in_instance_flags() {
        let mut instance = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
        instance.link_state = 1;
        let Value::Map(fields) = instance_reply(&instance, 1_000, false) else {
            panic!("instance reply should be a map");
        };
        let flags = fields
            .iter()
            .find_map(|(key, value)| match (key, value) {
                (Value::Bulk(key), Value::Bulk(value)) if key.as_ref() == b"flags" => {
                    Some(String::from_utf8_lossy(value).to_string())
                }
                _ => None,
            })
            .expect("flags field should exist");
        assert!(flags.contains("disconnected"));
    }
}
