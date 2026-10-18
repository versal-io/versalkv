use std::collections::HashMap;

use crate::engine::distributed::sentinel::{
    DownState, FailoverState, InstanceRole, SentinelInstance, SentinelInstanceId,
    SRI_FAILOVER_IN_PROGRESS,
};

pub fn find_misconfigured_replica(
    master: &SentinelInstance,
    instances: &HashMap<SentinelInstanceId, SentinelInstance>,
    now: i64,
) -> Option<SentinelInstanceId> {
    const ROLE_STABILIZATION_MS: i64 = 8_000;
    if master.down_state != DownState::Up {
        return None;
    }
    master.replicas.iter().copied().find(|id| {
        let Some(replica) = instances.get(id) else {
            return false;
        };
        if replica.down_state != DownState::Up || replica.reconfig_sent {
            return false;
        }
        let stuck_failover = replica.role_reported == InstanceRole::Replica
            && replica.monitored_instance_failover_state
                == crate::engine::distributed::sentinel::MONITORED_INSTANCE_FAILOVER
            && replica.monitored_instance_failover_change_time > 0
            && now - replica.monitored_instance_failover_change_time >= master.failover_timeout;
        let has_adopted_failover_epoch = master.config_epoch > 0;
        stuck_failover
            || (has_adopted_failover_epoch
                && replica.role_reported == InstanceRole::Master
                && replica.role_reported_time > 0
                && now - replica.role_reported_time >= ROLE_STABILIZATION_MS)
            || (has_adopted_failover_epoch
                && replica.role_reported == InstanceRole::Replica
                && !replica.master_name.is_empty()
                && replica.master_name != master.addr)
    })
}

pub fn switch_primary_address(
    master: &mut SentinelInstance,
    instances: &mut HashMap<SentinelInstanceId, SentinelInstance>,
    promoted_id: SentinelInstanceId,
    now: i64,
) -> bool {
    let old_primary_id = master.id;
    let Some(promoted) = instances.get(&promoted_id).cloned() else {
        return false;
    };
    reset_master_after_switch(master, promoted_id, &promoted.addr, now);
    master.replicas.retain(|id| *id != promoted_id);
    if old_primary_id != promoted_id
        && instances.contains_key(&old_primary_id)
        && !master.replicas.contains(&old_primary_id)
    {
        master.replicas.push(old_primary_id);
    }
    for replica_id in &master.replicas {
        if let Some(replica) = instances.get_mut(replica_id) {
            *replica = fresh_replica(replica, master, now);
        }
    }
    if let Some(promoted) = instances.get_mut(&promoted_id) {
        reset_promoted_instance(promoted, master, now);
    }
    true
}

fn reset_master_after_switch(
    master: &mut SentinelInstance,
    promoted_id: SentinelInstanceId,
    promoted_addr: &str,
    now: i64,
) {
    // Match sentinelResetPrimaryAndChangeAddress(): keep configured primary
    // options and peer Sentinels, but discard observations from the old node.
    master.id = promoted_id;
    master.addr = promoted_addr.to_string();
    master.runid.clear();
    master.flags = 0;
    master.role = InstanceRole::Master;
    master.role_reported = InstanceRole::Master;
    master.role_reported_time = now;
    master.down_state = DownState::Up;
    master.sdown_time = 0;
    master.sdown_since_time = 0;
    master.odown_since_time = 0;
    master.leader.clear();
    master.failover_state = FailoverState::None;
    master.failover_state_change_time = 0;
    master.failover_start_time = 0;
    master.promoted_replica = None;
    master.master_name.clear();
    master.act_ping_time = now;
    master.last_ping_time = 0;
    master.last_ping_ok = now;
    master.link_state = 1;
    master.link_down_since_time = now;
    master.pending_commands = 0;
}

fn fresh_replica(
    previous: &SentinelInstance,
    master: &SentinelInstance,
    now: i64,
) -> SentinelInstance {
    // C destroys and recreates every surviving replica after a primary switch.
    let mut replica =
        SentinelInstance::new_master(previous.id, &previous.addr, &previous.addr, master.quorum);
    replica.role = InstanceRole::Replica;
    replica.role_reported = InstanceRole::Replica;
    replica.role_reported_time = now;
    replica.down_after_ms = master.down_after_ms;
    replica.link_state = 1;
    replica.link_down_since_time = now;
    replica.act_ping_time = now;
    replica.last_ping_ok = now;
    replica
}

fn reset_promoted_instance(promoted: &mut SentinelInstance, master: &SentinelInstance, now: i64) {
    promoted.role = InstanceRole::Master;
    promoted.role_reported = InstanceRole::Master;
    promoted.role_reported_time = now;
    promoted.master_name.clear();
    promoted.down_after_ms = master.down_after_ms;
    promoted.quorum = master.quorum;
    promoted.parallel_syncs = master.parallel_syncs;
    promoted.failover_timeout = master.failover_timeout;
    promoted.auth_user.clone_from(&master.auth_user);
    promoted.auth_pass.clone_from(&master.auth_pass);
    promoted.primary_reboot_down_after_period = master.primary_reboot_down_after_period;
}

pub fn finish_failover(
    master: &mut SentinelInstance,
    instances: &mut HashMap<SentinelInstanceId, SentinelInstance>,
    now: i64,
) {
    let Some(promoted_id) = master.promoted_replica else {
        return;
    };
    if !switch_primary_address(master, instances, promoted_id, now) {
        return;
    }
    master.config_epoch = master.failover_epoch;
    master.failover_state = FailoverState::None;
    master.flags &= !SRI_FAILOVER_IN_PROGRESS;
    master.failover_state_change_time = now;
    master.last_hello_sent = 0;
    master.promoted_replica = None;
    instances.insert(master.id, master.clone());
}
