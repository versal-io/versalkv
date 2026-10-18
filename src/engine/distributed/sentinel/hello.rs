use crate::engine::distributed::sentinel::{
    format_sentinel_endpoint, split_sentinel_endpoint, DownState, FailoverState, InstanceRole,
    SentinelInstance, SentinelInstanceId, SentinelRegistry, SentinelState,
};

pub const SENTINEL_HELLO_CHANNEL: &str = "__sentinel__:hello";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloMessage {
    pub sentinel_addr: String,
    pub runid: String,
    pub current_epoch: i64,
    pub master_name: String,
    pub master_addr: String,
    pub master_config_epoch: i64,
}

impl HelloMessage {
    pub fn encode(&self, _use_hostname: bool) -> String {
        let sentinel = split_sentinel_endpoint(&self.sentinel_addr);
        let master = split_sentinel_endpoint(&self.master_addr);
        let (sentinel_host, sentinel_port) = sentinel
            .map(|endpoint| (endpoint.host, endpoint.port.to_string()))
            .unwrap_or(("", String::new()));
        let (master_host, master_port) = master
            .map(|endpoint| (endpoint.host, endpoint.port.to_string()))
            .unwrap_or(("", String::new()));
        format!(
            "{},{},{},{},{},{},{},{}",
            sentinel_host,
            sentinel_port,
            self.runid,
            self.current_epoch,
            self.master_name,
            master_host,
            master_port,
            self.master_config_epoch
        )
    }

    pub fn parse(payload: &str) -> Option<Self> {
        let parts: Vec<&str> = payload.split(',').collect();
        if parts.len() != 8 {
            return None;
        }
        let sentinel_port = parts[1].parse::<u16>().ok()?;
        let current_epoch = parts[3].parse::<i64>().ok()?;
        let master_port = parts[6].parse::<u16>().ok()?;
        let master_config_epoch = parts[7].parse::<i64>().ok()?;
        Some(Self {
            sentinel_addr: format_sentinel_endpoint(parts[0], sentinel_port),
            runid: parts[2].to_string(),
            current_epoch,
            master_name: parts[4].to_string(),
            master_addr: format_sentinel_endpoint(parts[5], master_port),
            master_config_epoch,
        })
    }
}

#[derive(Clone)]
struct HelloDraft {
    registry: SentinelRegistry,
    next_instance_id: SentinelInstanceId,
    current_epoch: i64,
}

impl HelloDraft {
    fn from_state(state: &SentinelState) -> Self {
        Self {
            registry: state.registry.clone(),
            next_instance_id: state.next_instance_id,
            current_epoch: state.current_epoch,
        }
    }

    fn allocate_id(&mut self) -> Option<SentinelInstanceId> {
        let id = self.next_instance_id;
        self.next_instance_id = self.next_instance_id.checked_add(1)?;
        Some(id)
    }

    fn publish(self, state: &mut SentinelState) {
        state.registry = self.registry;
        state.next_instance_id = self.next_instance_id;
        state.current_epoch = self.current_epoch;
    }
}

pub fn apply_hello(state: &mut SentinelState, hello: &HelloMessage, now: i64) -> bool {
    if hello.runid == state.myid || !state.registry.contains_master(&hello.master_name) {
        return false;
    }
    let mut draft = HelloDraft::from_state(state);
    if !apply_hello_to_draft(&mut draft, hello, now) {
        return false;
    }
    draft.publish(state);
    true
}

fn apply_hello_to_draft(draft: &mut HelloDraft, hello: &HelloMessage, now: i64) -> bool {
    draft.current_epoch = draft.current_epoch.max(hello.current_epoch);
    let Some(sentinel_id) = apply_peer_hello(draft, hello) else {
        return false;
    };
    if should_update_primary(&draft.registry, hello) && !apply_announced_primary(draft, hello, now)
    {
        return false;
    }
    draft.registry.instance(sentinel_id).is_some()
}

fn apply_peer_hello(draft: &mut HelloDraft, hello: &HelloMessage) -> Option<SentinelInstanceId> {
    let master = draft.registry.master(&hello.master_name)?;
    let down_after_ms = master.down_after_ms;
    let existing_id = master.sentinels.iter().copied().find(|id| {
        draft
            .registry
            .instance(*id)
            .is_some_and(|instance| instance.runid == hello.runid)
    });
    if existing_id.is_none() {
        remove_obsolete_peer(&mut draft.registry, hello);
    }
    match existing_id {
        Some(id) => update_existing_peer(&mut draft.registry, id, hello, down_after_ms),
        None => insert_new_peer(draft, hello, down_after_ms),
    }
}

fn update_existing_peer(
    registry: &mut SentinelRegistry,
    id: SentinelInstanceId,
    hello: &HelloMessage,
    down_after_ms: i64,
) -> Option<SentinelInstanceId> {
    let instance = registry.instance_mut(id)?;
    instance.addr = hello.sentinel_addr.clone();
    instance.current_epoch = hello.current_epoch;
    instance.down_after_ms = down_after_ms;
    Some(id)
}

fn insert_new_peer(
    draft: &mut HelloDraft,
    hello: &HelloMessage,
    down_after_ms: i64,
) -> Option<SentinelInstanceId> {
    let id = draft.allocate_id()?;
    let mut instance = SentinelInstance::new_master(id, &hello.runid, &hello.sentinel_addr, 1);
    instance.role = InstanceRole::Sentinel;
    instance.role_reported = InstanceRole::Sentinel;
    instance.runid = hello.runid.clone();
    instance.master_name = hello.master_name.clone();
    instance.current_epoch = hello.current_epoch;
    instance.down_after_ms = down_after_ms;
    draft
        .registry
        .insert_related_instance(&hello.master_name, instance)
        .ok()?;
    Some(id)
}

fn remove_obsolete_peer(registry: &mut SentinelRegistry, hello: &HelloMessage) {
    let obsolete_runid = registry.master(&hello.master_name).and_then(|master| {
        master.sentinels.iter().find_map(|id| {
            registry.instance(*id).and_then(|instance| {
                (instance.addr == hello.sentinel_addr && instance.runid != hello.runid)
                    .then(|| instance.runid.clone())
            })
        })
    });
    let Some(runid) = obsolete_runid else { return };
    let ids: Vec<_> = registry
        .instances()
        .iter()
        .filter_map(|(id, instance)| {
            (instance.role == InstanceRole::Sentinel && instance.runid == runid).then_some(*id)
        })
        .collect();
    for id in &ids {
        registry.remove_instance(*id);
    }
    for instance in registry.values_mut() {
        instance.sentinels.retain(|id| !ids.contains(id));
        instance.peer_states.retain(|id, _| !ids.contains(id));
    }
}

fn should_update_primary(registry: &SentinelRegistry, hello: &HelloMessage) -> bool {
    registry.master(&hello.master_name).is_some_and(|master| {
        hello.master_config_epoch > master.config_epoch
            || should_adopt_same_epoch_topology(registry, master, hello)
    })
}

fn apply_announced_primary(draft: &mut HelloDraft, hello: &HelloMessage, now: i64) -> bool {
    let Some(promoted_id) = ensure_announced_primary(draft, hello) else {
        return false;
    };
    let Some(mut master) = draft.registry.take_master_snapshot(&hello.master_name) else {
        return false;
    };
    if !draft
        .registry
        .switch_primary_address(&mut master, promoted_id, now)
    {
        return false;
    }
    finish_hello_primary_update(&mut master, hello, now);
    draft
        .registry
        .replace_master(hello.master_name.clone(), master)
        .is_ok()
}

fn ensure_announced_primary(
    draft: &mut HelloDraft,
    hello: &HelloMessage,
) -> Option<SentinelInstanceId> {
    let master = draft.registry.master(&hello.master_name)?;
    let candidate = std::iter::once(master.id)
        .chain(master.replicas.iter().copied())
        .find(|id| {
            draft
                .registry
                .instance(*id)
                .is_some_and(|instance| instance.addr == hello.master_addr)
        });
    if candidate.is_some() {
        return candidate;
    }
    insert_announced_replica(draft, hello)
}

fn insert_announced_replica(
    draft: &mut HelloDraft,
    hello: &HelloMessage,
) -> Option<SentinelInstanceId> {
    let master = draft.registry.master(&hello.master_name)?;
    let quorum = master.quorum;
    let previous_addr = master.addr.clone();
    let id = draft.allocate_id()?;
    let mut instance =
        SentinelInstance::new_master(id, &hello.master_addr, &hello.master_addr, quorum);
    instance.role = InstanceRole::Replica;
    instance.role_reported = InstanceRole::Replica;
    instance.master_name = previous_addr;
    draft
        .registry
        .insert_related_instance(&hello.master_name, instance)
        .ok()?;
    Some(id)
}

fn finish_hello_primary_update(master: &mut SentinelInstance, hello: &HelloMessage, now: i64) {
    master.config_epoch = hello.master_config_epoch;
    master.failover_state = FailoverState::None;
    master.failover_state_change_time = now;
    master.last_hello_sent = 0;
    master.flags &= !(crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS
        | crate::engine::distributed::sentinel::SRI_FORCE_FAILOVER);
    master.promoted_replica = None;
}

fn should_adopt_same_epoch_topology(
    registry: &SentinelRegistry,
    master: &SentinelInstance,
    hello: &HelloMessage,
) -> bool {
    if hello.master_config_epoch == 0
        || hello.master_config_epoch != master.config_epoch
        || hello.master_addr == master.addr
    {
        return false;
    }
    let local_topology_is_unstable = master.failover_state != FailoverState::None
        || master.flags & crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS != 0
        || master.down_state != DownState::Up;
    if !local_topology_is_unstable {
        return false;
    }
    registry
        .instances()
        .values()
        .find(|instance| instance.addr == hello.master_addr)
        .is_some_and(|instance| master.replicas.contains(&instance.id))
}

#[cfg(test)]
mod tests {
    use super::{apply_hello, HelloMessage};
    use crate::engine::distributed::sentinel::{InstanceRole, SentinelInstance, SentinelState};

    #[test]
    fn hello_payload_round_trips_all_eight_fields() {
        let hello = HelloMessage {
            sentinel_addr: "10.0.0.1:26379".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 4,
            master_name: "cache".to_string(),
            master_addr: "10.0.0.2:6379".to_string(),
            master_config_epoch: 2,
        };

        assert_eq!(HelloMessage::parse(&hello.encode(false)), Some(hello));
    }

    #[test]
    fn hello_ipv6_wire_hosts_are_bare_and_parse_to_internal_endpoints() {
        let hello = HelloMessage {
            sentinel_addr: "[::1]:26379".to_string(),
            runid: "peer-v6".to_string(),
            current_epoch: 4,
            master_name: "cache".to_string(),
            master_addr: "[2001:db8::1]:6379".to_string(),
            master_config_epoch: 2,
        };

        let encoded = hello.encode(false);
        assert_eq!(encoded, "::1,26379,peer-v6,4,cache,2001:db8::1,6379,2");
        assert_eq!(HelloMessage::parse(&encoded), Some(hello));
    }

    #[test]
    fn hello_discovers_sentinel_for_known_master() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let mut master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 2);
        master.down_after_ms = 2_000;
        state
            .registry
            .insert_master("cache".to_string(), master)
            .unwrap();
        state.next_instance_id = 2;

        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 4,
            master_name: "cache".to_string(),
            master_addr: "127.0.0.1:6379".to_string(),
            master_config_epoch: 2,
        };

        assert!(apply_hello(&mut state, &hello, 1_000));
        let master = state.registry.master("cache").unwrap();
        assert_eq!(master.sentinels.len(), 1);
        let peer = state
            .registry
            .instances()
            .get(&master.sentinels[0])
            .unwrap();
        assert_eq!(peer.role, InstanceRole::Sentinel);
        assert_eq!(peer.runid, "peer-a");
        assert_eq!(peer.addr, "127.0.0.1:26380");
        assert_eq!(peer.down_after_ms, 2_000);
        assert_eq!(state.current_epoch, 4);
    }

    #[test]
    fn new_runid_at_existing_address_replaces_old_sentinel() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 2);
        state
            .registry
            .insert_master("cache".to_string(), master)
            .unwrap();
        state.next_instance_id = 2;

        let mut hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 1,
            master_name: "cache".to_string(),
            master_addr: "127.0.0.1:6379".to_string(),
            master_config_epoch: 0,
        };
        assert!(apply_hello(&mut state, &hello, 1_000));

        hello.runid = "peer-b".to_string();
        assert!(apply_hello(&mut state, &hello, 2_000));

        let peer_ids = &state.registry.master("cache").unwrap().sentinels;
        assert_eq!(peer_ids.len(), 1);
        assert_eq!(state.registry.instances()[&peer_ids[0]].runid, "peer-b");
        assert!(!state
            .registry
            .instances()
            .values()
            .any(|instance| instance.runid == "peer-a"));
    }

    #[test]
    fn newer_hello_switches_primary_identity_and_preserves_old_primary_as_replica() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let mut master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 2);
        master.replicas = vec![2, 3];
        master.failover_state =
            crate::engine::distributed::sentinel::FailoverState::ReconfigureReplicas;
        master.flags |= crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS;
        master.promoted_replica = Some(3);
        state
            .registry
            .insert_master("cache".to_string(), master)
            .unwrap();
        let mut promoted = SentinelInstance::new_master(2, "127.0.0.1:6380", "127.0.0.1:6380", 2);
        promoted.role = InstanceRole::Replica;
        promoted.role_reported = InstanceRole::Replica;
        state.registry.insert_instance(promoted).unwrap();
        let mut follower = SentinelInstance::new_master(3, "127.0.0.1:6381", "127.0.0.1:6381", 2);
        follower.role = InstanceRole::Replica;
        follower.role_reported = InstanceRole::Replica;
        state.registry.insert_instance(follower).unwrap();
        state.next_instance_id = 4;

        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 4,
            master_name: "cache".to_string(),
            master_addr: "127.0.0.1:6380".to_string(),
            master_config_epoch: 4,
        };

        assert!(apply_hello(&mut state, &hello, 1_000));
        let master = state.registry.master("cache").unwrap();
        assert_eq!(master.id, 2);
        assert_eq!(master.addr, "127.0.0.1:6380");
        assert_eq!(master.replicas, vec![3, 1]);
        assert_eq!(
            master.failover_state,
            crate::engine::distributed::sentinel::FailoverState::None
        );
        assert_eq!(
            master.flags & crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS,
            0
        );
        assert_eq!(master.promoted_replica, None);
        let mirrored_master = state.registry.instances().get(&2).unwrap();
        assert_eq!(mirrored_master.role, InstanceRole::Master);
        assert_eq!(mirrored_master.addr, master.addr);
        assert_eq!(mirrored_master.config_epoch, master.config_epoch);
        assert_eq!(mirrored_master.replicas, master.replicas);
        assert_eq!(
            state.registry.instances().get(&1).unwrap().role,
            InstanceRole::Replica
        );
    }

    #[test]
    fn newer_hello_switches_to_primary_missing_from_local_instances() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 2);
        state
            .registry
            .insert_master("cache".to_string(), master)
            .unwrap();
        state.next_instance_id = 2;

        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 4,
            master_name: "cache".to_string(),
            master_addr: "127.0.0.1:6380".to_string(),
            master_config_epoch: 4,
        };

        assert!(apply_hello(&mut state, &hello, 1_000));
        let master = state.registry.master("cache").unwrap();
        assert_eq!(master.addr, "127.0.0.1:6380");
        assert_eq!(master.config_epoch, 4);
        assert_eq!(
            state.registry.instances().get(&master.id).unwrap().role,
            InstanceRole::Master
        );
        assert!(master.replicas.contains(&1));
    }

    #[test]
    fn same_epoch_hello_does_not_roll_back_primary_address() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let mut master = SentinelInstance::new_master(2, "cache", "127.0.0.1:6380", 2);
        master.config_epoch = 4;
        master.replicas = vec![1];
        state
            .registry
            .insert_master("cache".to_string(), master)
            .unwrap();
        let mut old_primary =
            SentinelInstance::new_master(1, "127.0.0.1:6379", "127.0.0.1:6379", 2);
        old_primary.role = InstanceRole::Replica;
        old_primary.role_reported = InstanceRole::Replica;
        state.registry.insert_instance(old_primary).unwrap();
        state.next_instance_id = 3;

        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 4,
            master_name: "cache".to_string(),
            master_addr: "127.0.0.1:6379".to_string(),
            master_config_epoch: 4,
        };

        assert!(apply_hello(&mut state, &hello, 1_000));
        let master = state.registry.master("cache").unwrap();
        assert_eq!(master.id, 2);
        assert_eq!(master.addr, "127.0.0.1:6380");
        assert_eq!(master.config_epoch, 4);
    }

    #[test]
    fn same_epoch_hello_repairs_unstable_local_failover_topology() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let mut master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 2);
        master.config_epoch = 4;
        master.failover_state = crate::engine::distributed::sentinel::FailoverState::WaitPromotion;
        master.flags |= crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS;
        master.replicas = vec![2];
        state
            .registry
            .insert_master("cache".to_string(), master)
            .unwrap();
        let mut promoted = SentinelInstance::new_master(2, "replica", "127.0.0.1:6380", 2);
        promoted.role = InstanceRole::Replica;
        promoted.role_reported = InstanceRole::Replica;
        state.registry.insert_instance(promoted).unwrap();
        state.next_instance_id = 3;

        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 4,
            master_name: "cache".to_string(),
            master_addr: "127.0.0.1:6380".to_string(),
            master_config_epoch: 4,
        };

        assert!(apply_hello(&mut state, &hello, 1_000));
        let master = state.registry.master("cache").unwrap();
        assert_eq!(master.id, 2);
        assert_eq!(master.addr, "127.0.0.1:6380");
        assert_eq!(
            master.failover_state,
            crate::engine::distributed::sentinel::FailoverState::None
        );
        assert_eq!(
            master.flags & crate::engine::distributed::sentinel::SRI_FAILOVER_IN_PROGRESS,
            0
        );
        assert!(master.replicas.contains(&1));
    }

    #[test]
    fn hello_from_self_is_ignored() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let master = SentinelInstance::new_master(1, "cache", "127.0.0.1:6379", 1);
        state
            .registry
            .insert_master("cache".to_string(), master)
            .unwrap();
        state.next_instance_id = 2;
        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26379".to_string(),
            runid: "self".to_string(),
            current_epoch: 1,
            master_name: "cache".to_string(),
            master_addr: "127.0.0.1:6379".to_string(),
            master_config_epoch: 0,
        };

        assert!(!apply_hello(&mut state, &hello, 1_000));
        assert!(state.registry.master("cache").unwrap().sentinels.is_empty());
    }

    #[test]
    fn hello_announcing_another_master_endpoint_never_reuses_its_id() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let master_a = SentinelInstance::new_master(1, "a", "127.0.0.1:6379", 1);
        let master_b = SentinelInstance::new_master(2, "b", "127.0.0.1:6380", 1);
        state
            .registry
            .insert_master("a".to_string(), master_a)
            .unwrap();
        state
            .registry
            .insert_master("b".to_string(), master_b)
            .unwrap();
        state.next_instance_id = 3;
        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 2,
            master_name: "a".to_string(),
            master_addr: "127.0.0.1:6380".to_string(),
            master_config_epoch: 2,
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            apply_hello(&mut state, &hello, 1_000)
        }));

        assert!(result.is_ok(), "network HELLO must not panic");
        assert!(result.unwrap());
        let master_a = state.registry.master("a").unwrap();
        let master_b = state.registry.master("b").unwrap();
        assert_ne!(master_a.id, master_b.id);
        assert_eq!(master_b.id, 2);
        assert_eq!(master_b.name, "b");
        assert_eq!(master_b.addr, "127.0.0.1:6380");
        assert_eq!(state.registry.instance(2).unwrap().name, "b");
        assert_eq!(state.registry.master_count(), 2);
    }

    #[test]
    fn failed_announced_primary_stage_rolls_back_peer_and_next_id() {
        let mut state = SentinelState::new();
        state.myid = "self".to_string();
        let master = SentinelInstance::new_master(1, "a", "127.0.0.1:6379", 1);
        state
            .registry
            .insert_master("a".to_string(), master)
            .unwrap();
        let occupied = SentinelInstance::new_master(4, "occupied", "127.0.0.1:6390", 1);
        state.registry.insert_instance(occupied).unwrap();
        state.next_instance_id = 3;
        let original_epoch = state.current_epoch;
        let hello = HelloMessage {
            sentinel_addr: "127.0.0.1:26380".to_string(),
            runid: "peer-a".to_string(),
            current_epoch: 2,
            master_name: "a".to_string(),
            master_addr: "127.0.0.1:6380".to_string(),
            master_config_epoch: 2,
        };

        assert!(!apply_hello(&mut state, &hello, 1_000));
        assert_eq!(state.next_instance_id, 3);
        assert_eq!(state.current_epoch, original_epoch);
        assert_eq!(state.registry.master("a").unwrap().id, 1);
        assert!(state.registry.instance(3).is_none());
        assert_eq!(state.registry.instance(4).unwrap().name, "occupied");
    }
}
