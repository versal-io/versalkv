use crate::engine::distributed::sentinel::{
    format_sentinel_endpoint, InstanceRole, SentinelInstanceId, SentinelState,
};
use crate::framework::context::ServerContext;

pub(super) fn parse_info_response(
    ctx: &mut ServerContext,
    id: SentinelInstanceId,
    info: &str,
    now: i64,
) {
    let mut runid = String::new();
    let mut role = String::new();
    let mut master_host = String::new();
    let mut master_port = String::new();
    let mut master_link_status = String::new();
    let mut repl_offset: i64 = 0;
    let mut slave_priority: i64 = 100;
    let state = ctx.sentinel_state_mut();
    for line in info.lines() {
        if let Some(val) = line.strip_prefix("run_id:") {
            runid = val.to_string();
        } else if let Some(val) = line.strip_prefix("role:") {
            role = val.to_string();
        } else if let Some(val) = line.strip_prefix("master_host:") {
            master_host = val.to_string();
        } else if let Some(val) = line.strip_prefix("master_port:") {
            master_port = val.to_string();
        } else if let Some(val) = line.strip_prefix("master_link_status:") {
            master_link_status = val.trim().to_string();
        } else if line.starts_with("slave_repl_offset:") || line.starts_with("repl_offset:") {
            let val = &line[line.find(':').unwrap_or(0) + 1..];
            repl_offset = val.parse().unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("slave_priority:") {
            slave_priority = val.parse().unwrap_or(100);
        } else if line.starts_with("replica_announced:") || line.starts_with("slave_announced:") {
            let val = &line[line.find(':').unwrap_or(0) + 1..];
            let val = val.trim();
            if let Some(inst) = state.registry.instance_mut(id) {
                inst.replica_announced = val == "yes" || val == "1" || val == "true";
            }
        } else if let Some(val) = line.strip_prefix("master_failover_state:") {
            let val_state = if val.trim() == "no-failover" {
                Some(crate::engine::distributed::sentinel::MONITORED_INSTANCE_NO_FAILOVER)
            } else if val.trim().is_empty() {
                None
            } else {
                Some(crate::engine::distributed::sentinel::MONITORED_INSTANCE_FAILOVER)
            };
            if let (Some(inst), Some(val_state)) = (state.registry.instance_mut(id), val_state) {
                if inst.monitored_instance_failover_state != val_state {
                    inst.monitored_instance_failover_change_time = now;
                }
                inst.monitored_instance_failover_state = val_state;
            }
            if let Some(master) = state.registry.master_by_id_mut(id) {
                if let Some(val_state) = val_state {
                    if master.monitored_instance_failover_state != val_state {
                        master.monitored_instance_failover_change_time = now;
                    }
                    master.monitored_instance_failover_state = val_state;
                }
            }
        }
    }
    if let Some(inst) = state.registry.instance_mut(id) {
        if !runid.is_empty() {
            if !inst.runid.is_empty()
                && inst.runid != runid
                && inst.last_info_time > 0
                && inst.role == InstanceRole::Master
            {
                inst.flags |= crate::engine::distributed::sentinel::SRI_PRIMARY_REBOOT;
                inst.reboot_time = now;
                let event_msg = format!("instance {} {}", inst.name, inst.addr);
                let event_inst = inst.clone();
                crate::engine::distributed::sentinel::event::sentinel_event(
                    &mut state.script_engine,
                    "warning",
                    "+reboot",
                    Some(&event_inst),
                    &event_msg,
                );
            }
            inst.runid = runid;
        }
        inst.repl_offset = repl_offset;
        inst.slave_priority = slave_priority;
        let reported_role = if role == "slave" {
            Some(InstanceRole::Replica)
        } else if role == "master" {
            Some(InstanceRole::Master)
        } else {
            None
        };
        if let Some(reported_role) = reported_role {
            if inst.role_reported_time == 0 || inst.role_reported != reported_role {
                inst.role_reported = reported_role;
                inst.role_reported_time = now;
            }
        }
    }
    if role == "master" {
        discover_replicas_from_info(state, id, info, now);
    }
    if role == "slave" && !master_host.is_empty() {
        if let Ok(port) = master_port.parse::<u16>() {
            let reported_primary = format_sentinel_endpoint(&master_host, port);
            if let Some(inst) = state.registry.instance_mut(id) {
                inst.master_name = reported_primary;
                inst.master_link_status.clone_from(&master_link_status);
                inst.reconfig_sent = false;
                inst.reconfig_sent_time = 0;
            }
        }
    }
}

fn discover_replicas_from_info(
    state: &mut SentinelState,
    master_id: SentinelInstanceId,
    info: &str,
    now: i64,
) {
    let mut in_slave_section = false;
    for line in info.lines() {
        if line.starts_with("# Replication") {
            in_slave_section = true;
            continue;
        }
        if line.starts_with('#') {
            in_slave_section = false;
            continue;
        }
        if in_slave_section && (line.starts_with("slave") || line.starts_with("replica")) {
            if let Some(colon_idx) = line.find(':') {
                let after_colon = &line[colon_idx + 1..];
                let parts: Vec<&str> = after_colon.split(',').collect();
                let mut ip = String::new();
                let mut port = String::new();
                for part in parts {
                    if let Some(val) = part.strip_prefix("ip=") {
                        ip = val.to_string();
                    } else if let Some(val) = part.strip_prefix("port=") {
                        port = val.to_string();
                    }
                }
                if !ip.is_empty() {
                    let Ok(port) = port.parse::<u16>() else {
                        continue;
                    };
                    let addr = format_sentinel_endpoint(&ip, port);
                    let exists = state.registry.instances().values().any(|i| i.addr == addr);
                    if !exists {
                        let master_config = state
                            .registry
                            .masters()
                            .map(|(_, master)| master)
                            .find(|master| master.id == master_id)
                            .map(|master| {
                                (
                                    master.down_after_ms,
                                    master.quorum,
                                    master.parallel_syncs,
                                    master.failover_timeout,
                                    master.auth_user.clone(),
                                    master.auth_pass.clone(),
                                )
                            })
                            .unwrap_or((30_000, 1, 1, 180_000, String::new(), String::new()));
                        let new_id = state.next_instance_id;
                        let mut replica =
                            crate::engine::distributed::sentinel::SentinelInstance::new_master(
                                new_id, &addr, &addr, 1,
                            );
                        replica.role = InstanceRole::Replica;
                        replica.role_reported = InstanceRole::Replica;
                        replica.master_name = String::new();
                        replica.down_after_ms = master_config.0;
                        replica.quorum = master_config.1;
                        replica.parallel_syncs = master_config.2;
                        replica.failover_timeout = master_config.3;
                        replica.auth_user = master_config.4;
                        replica.auth_pass = master_config.5;
                        replica.last_ping_ok = now;
                        replica.last_info_time = now;
                        if state
                            .registry
                            .insert_related_instance_by_master_id(master_id, replica)
                            .is_ok()
                        {
                            state.next_instance_id += 1;
                        }
                    } else if let Some(master) = state.registry.master_by_id(master_id).cloned() {
                        // Existing replica entry on a known master: keep its health
                        // counters but refresh auth credentials and key runtime
                        // params from the current master config. This absorbs
                        // SENTINEL SET auth-pass/auth-user updates even when the
                        // instance record was already discovered earlier.
                        if let Some(existing) = state.registry.values_mut().find(|i| i.addr == addr)
                        {
                            existing.down_after_ms = master.down_after_ms;
                            existing.quorum = master.quorum;
                            existing.parallel_syncs = master.parallel_syncs;
                            existing.failover_timeout = master.failover_timeout;
                            existing.auth_user = master.auth_user;
                            existing.auth_pass = master.auth_pass;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_info_response;
    use crate::engine::distributed::sentinel::{
        FailoverState, InstanceRole, SentinelInstance, SRI_FAILOVER_IN_PROGRESS,
    };
    use crate::framework::context::ServerContext;

    #[test]
    fn info_report_does_not_replace_configured_replica_identity() {
        let mut ctx = ServerContext::new(1);
        let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
        replica.role = InstanceRole::Replica;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        parse_info_response(
            &mut ctx,
            2,
            "# Replication\r\nrole:master\r\nrun_id:reported-master\r\n",
            1_000,
        );

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].role,
            InstanceRole::Replica
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].role_reported,
            InstanceRole::Master
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].role_reported_time,
            1_000
        );
    }

    #[test]
    fn promoted_replica_info_does_not_switch_primary_before_update_config() {
        let mut ctx = ServerContext::new(1);
        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
        master.replicas.push(2);
        master.promoted_replica = Some(2);
        master.failover_state = FailoverState::WaitPromotion;
        master.flags |= SRI_FAILOVER_IN_PROGRESS;
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        let mut promoted = SentinelInstance::new_master(2, "promoted", "127.0.0.1:6380", 1);
        promoted.role = InstanceRole::Replica;
        promoted.role_reported = InstanceRole::Replica;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(promoted)
            .unwrap();

        parse_info_response(
            &mut ctx,
            2,
            "# Replication\r\nrole:master\r\nrun_id:promoted\r\n",
            1_000,
        );

        let master = &ctx.sentinel_state().registry.master("primary").unwrap();
        assert_eq!(master.id, 1);
        assert_eq!(master.addr, "127.0.0.1:6379");
        assert_eq!(master.failover_state, FailoverState::WaitPromotion);
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].role_reported,
            InstanceRole::Master
        );
    }

    #[test]
    fn wrong_upstream_clears_pending_reconfiguration_marker() {
        let mut ctx = ServerContext::new(1);
        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
        master.replicas.push(2);
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
        replica.role = InstanceRole::Replica;
        replica.role_reported = InstanceRole::Replica;
        replica.reconfig_sent = true;
        replica.reconfig_sent_time = 900;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        parse_info_response(
            &mut ctx,
            2,
            "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6381\r\n",
            1_000,
        );

        let replica = &ctx.sentinel_state().registry.instances()[&2];
        assert!(!replica.reconfig_sent);
        assert_eq!(replica.reconfig_sent_time, 0);
    }

    #[test]
    fn replica_info_records_master_link_status() {
        let mut ctx = ServerContext::new(1);
        let mut replica = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 1);
        replica.role = InstanceRole::Replica;
        replica.role_reported = InstanceRole::Replica;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        parse_info_response(
            &mut ctx,
            2,
            "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6379\r\nmaster_link_status:up\r\n",
            1_000,
        );

        let replica = &ctx.sentinel_state().registry.instances()[&2];
        assert_eq!(replica.master_name, "127.0.0.1:6379");
        assert_eq!(replica.master_link_status, "up");
    }

    #[test]
    fn replica_info_normalizes_reported_ipv6_primary_endpoint() {
        let mut ctx = ServerContext::new(1);
        let mut replica = SentinelInstance::new_master(2, "replica", "[::1]:6380", 1);
        replica.role = InstanceRole::Replica;
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(replica)
            .unwrap();

        parse_info_response(
            &mut ctx,
            2,
            "# Replication\r\nrole:slave\r\nmaster_host:::1\r\nmaster_port:6379\r\nmaster_link_status:up\r\n",
            1_000,
        );

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].master_name,
            "[::1]:6379"
        );
    }

    #[test]
    fn discovered_replica_inherits_master_runtime_config() {
        let mut ctx = ServerContext::new(1);
        let mut master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 3);
        master.down_after_ms = 2_000;
        master.parallel_syncs = 4;
        master.failover_timeout = 10_000;
        master.auth_user = "sentinel-user".to_string();
        master.auth_pass = "sentinel-pass".to_string();
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();
        ctx.sentinel_state_mut().next_instance_id = 2;

        parse_info_response(
            &mut ctx,
            1,
            "# Replication\r\nrole:master\r\nslave0:ip=127.0.0.1,port=6380,state=online,offset=0,lag=0\r\n",
            1_000,
        );

        let replica_id = ctx
            .sentinel_state()
            .registry
            .master("primary")
            .unwrap()
            .replicas[0];
        let replica = &ctx.sentinel_state().registry.instances()[&replica_id];
        assert_eq!(replica.down_after_ms, 2_000);
        assert_eq!(replica.quorum, 3);
        assert_eq!(replica.parallel_syncs, 4);
        assert_eq!(replica.failover_timeout, 10_000);
        assert_eq!(replica.auth_user, "sentinel-user");
        assert_eq!(replica.auth_pass, "sentinel-pass");
    }

    #[test]
    fn primary_info_discovers_bracketed_ipv6_replica_endpoint() {
        let mut ctx = ServerContext::new(1);
        let master = SentinelInstance::new_master(1, "primary", "[::1]:6379", 1);
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();
        ctx.sentinel_state_mut().next_instance_id = 2;

        parse_info_response(
            &mut ctx,
            1,
            "# Replication\r\nrole:master\r\nslave0:ip=::1,port=6380,state=online,offset=0,lag=0\r\n",
            1_000,
        );

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&2].addr,
            "[::1]:6380"
        );
    }

    #[test]
    fn primary_info_detects_failover_command_support() {
        let mut ctx = ServerContext::new(1);
        let master = SentinelInstance::new_master(1, "primary", "127.0.0.1:6379", 1);
        ctx.sentinel_state_mut()
            .registry
            .insert_instance(master)
            .unwrap();

        parse_info_response(
            &mut ctx,
            1,
            "# Replication\r\nrole:master\r\nmaster_failover_state:no-failover\r\n",
            1_000,
        );

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].monitored_instance_failover_state,
            crate::engine::distributed::sentinel::MONITORED_INSTANCE_NO_FAILOVER
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].monitored_instance_failover_change_time,
            1_000
        );

        parse_info_response(
            &mut ctx,
            1,
            "# Replication\r\nrole:master\r\nmaster_failover_state:waiting-for-sync\r\n",
            2_000,
        );

        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].monitored_instance_failover_state,
            crate::engine::distributed::sentinel::MONITORED_INSTANCE_FAILOVER
        );
        assert_eq!(
            ctx.sentinel_state().registry.instances()[&1].monitored_instance_failover_change_time,
            2_000
        );
    }
}
