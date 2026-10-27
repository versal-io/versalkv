use std::collections::HashMap;

use super::config::{
    MasterNumberField, MasterTextField, MonitorDirective, PostMonitorDirective,
    PreMonitorDirective, SentinelConfigPlan, SentinelConfigScriptValidator,
};
use super::script;

pub const SRI_COORD_FAILOVER: u64 = 1 << 0;
pub const SRI_PRIMARY_REBOOT: u64 = 1 << 1;
pub const SRI_FORCE_FAILOVER: u64 = 1 << 2;
pub const SRI_FAILOVER_IN_PROGRESS: u64 = 1 << 3;

pub const MONITORED_INSTANCE_FAILOVER_NS: i64 = 0; //
pub const MONITORED_INSTANCE_NO_FAILOVER: i64 = 1;
pub const MONITORED_INSTANCE_FAILOVER: i64 = 2;

/// Timing knobs accepted by `SENTINEL DEBUG`.
/// They are grouped because cron, link health, failover and script scheduling
/// must observe one coherent set of test/runtime overrides.
#[derive(Debug, Clone)]
pub struct SentinelTunables {
    pub info_period_ms: i64,
    pub ping_period_ms: i64,
    pub ask_period_ms: i64,
    pub publish_period_ms: i64,
    pub default_down_after_ms: i64,
    pub tilt_trigger_ms: i64,
    pub tilt_period_ms: i64,
    pub replica_reconf_timeout_ms: i64,
    pub min_link_reconnect_period_ms: i64,
    pub default_failover_timeout_ms: i64,
    pub election_timeout_ms: i64,
    pub script_max_runtime_ms: i64,
    pub script_retry_delay_ms: i64,
}

impl Default for SentinelTunables {
    fn default() -> Self {
        Self {
            info_period_ms: 10_000,
            ping_period_ms: 1_000,
            ask_period_ms: 1_000,
            publish_period_ms: 2_000,
            default_down_after_ms: 30_000,
            tilt_trigger_ms: 2_000,
            tilt_period_ms: 30_000,
            replica_reconf_timeout_ms: 10_000,
            min_link_reconnect_period_ms: 15_000,
            default_failover_timeout_ms: 180_000,
            election_timeout_ms: 10_000,
            script_max_runtime_ms: 60_000,
            script_retry_delay_ms: 30_000,
        }
    }
}

pub type SentinelInstanceId = u64;

/// Owns every Sentinel instance exactly once and indexes configured masters by
/// name. The name index stores only stable IDs, so master mutations are
/// immediately visible to instance-oriented callers without clone-and-sync.
#[derive(Debug, Clone, Default)]
pub(crate) struct SentinelRegistry {
    instances: HashMap<SentinelInstanceId, SentinelInstance>,
    master_by_name: HashMap<String, SentinelInstanceId>,
}

impl SentinelRegistry {
    pub fn instances(&self) -> &HashMap<SentinelInstanceId, SentinelInstance> {
        &self.instances
    }

    pub fn instance(&self, id: SentinelInstanceId) -> Option<&SentinelInstance> {
        self.instances.get(&id)
    }

    pub fn instance_mut(&mut self, id: SentinelInstanceId) -> Option<&mut SentinelInstance> {
        self.instances.get_mut(&id)
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut SentinelInstance> {
        self.instances.values_mut()
    }

    pub fn master(&self, name: &str) -> Option<&SentinelInstance> {
        self.master_by_name
            .get(name)
            .and_then(|id| self.instances.get(id))
    }

    pub fn master_mut(&mut self, name: &str) -> Option<&mut SentinelInstance> {
        let id = *self.master_by_name.get(name)?;
        self.instances.get_mut(&id)
    }

    pub fn contains_master(&self, name: &str) -> bool {
        self.master_by_name.contains_key(name)
    }

    pub fn insert_master(
        &mut self,
        name: String,
        instance: SentinelInstance,
    ) -> Result<(), SentinelRegistryError> {
        if self.master_by_name.contains_key(&name) {
            return Err(SentinelRegistryError::DuplicateMasterName);
        }
        let id = instance.id;
        if self.instances.contains_key(&id) {
            return Err(SentinelRegistryError::DuplicateInstanceId);
        }
        self.instances.insert(id, instance);
        self.master_by_name.insert(name, id);
        Ok(())
    }

    pub fn replace_master(
        &mut self,
        name: String,
        instance: SentinelInstance,
    ) -> Result<(), SentinelRegistryError> {
        if !self.master_by_name.contains_key(&name) {
            return Err(SentinelRegistryError::NoSuchMaster);
        }
        let id = instance.id;
        if self
            .master_by_name
            .iter()
            .any(|(other_name, other_id)| other_name != &name && *other_id == id)
        {
            return Err(SentinelRegistryError::DuplicateInstanceId);
        }
        self.instances.insert(id, instance);
        self.master_by_name.insert(name, id);
        Ok(())
    }

    pub fn insert_instance(
        &mut self,
        instance: SentinelInstance,
    ) -> Result<(), SentinelRegistryError> {
        if self.instances.contains_key(&instance.id) {
            return Err(SentinelRegistryError::DuplicateInstanceId);
        }
        self.instances.insert(instance.id, instance);
        Ok(())
    }

    pub fn insert_related_instance(
        &mut self,
        master_name: &str,
        instance: SentinelInstance,
    ) -> Result<(), SentinelRegistryError> {
        if !self.master_by_name.contains_key(master_name) {
            return Err(SentinelRegistryError::NoSuchMaster);
        }
        let id = instance.id;
        let role = instance.role;
        if role == InstanceRole::Master {
            return Err(SentinelRegistryError::InvalidRelatedRole);
        }
        self.insert_instance(instance)?;
        let master = self
            .master_mut(master_name)
            .expect("master existence validated before related instance insert");
        match role {
            InstanceRole::Replica => master.replicas.push(id),
            InstanceRole::Sentinel => master.sentinels.push(id),
            InstanceRole::Master => unreachable!("master role rejected before insert"),
        }
        Ok(())
    }

    pub fn insert_related_instance_by_master_id(
        &mut self,
        master_id: SentinelInstanceId,
        instance: SentinelInstance,
    ) -> Result<(), SentinelRegistryError> {
        let name = self
            .master_by_name
            .iter()
            .find_map(|(name, id)| (*id == master_id).then(|| name.clone()))
            .ok_or(SentinelRegistryError::NoSuchMaster)?;
        self.insert_related_instance(&name, instance)
    }

    pub fn remove_instance(&mut self, id: SentinelInstanceId) -> Option<SentinelInstance> {
        if self
            .master_by_name
            .values()
            .any(|master_id| *master_id == id)
        {
            return None;
        }
        self.instances.remove(&id)
    }

    pub fn switch_primary_address(
        &mut self,
        master: &mut SentinelInstance,
        promoted_id: SentinelInstanceId,
        now: i64,
    ) -> bool {
        super::failover::switch_primary_address(master, &mut self.instances, promoted_id, now)
    }

    pub fn finish_failover(&mut self, master: &mut SentinelInstance, now: i64) {
        super::failover::finish_failover(master, &mut self.instances, now);
        self.replace_master(master.name.clone(), master.clone())
            .expect("failover replaces an existing configured master");
    }

    pub fn remove_master(&mut self, name: &str) -> Option<SentinelInstance> {
        let id = self.master_by_name.remove(name)?;
        self.instances.remove(&id)
    }

    pub fn take_master_snapshot(&self, name: &str) -> Option<SentinelInstance> {
        self.master(name).cloned()
    }

    pub fn masters(&self) -> impl Iterator<Item = (&str, &SentinelInstance)> {
        self.master_by_name.iter().filter_map(|(name, id)| {
            self.instances
                .get(id)
                .map(|instance| (name.as_str(), instance))
        })
    }

    pub fn master_names(&self) -> impl Iterator<Item = &String> {
        self.master_by_name.keys()
    }

    pub fn master_count(&self) -> usize {
        self.master_by_name.len()
    }

    pub fn master_by_id(&self, id: SentinelInstanceId) -> Option<&SentinelInstance> {
        self.master_by_name
            .values()
            .any(|master_id| *master_id == id)
            .then(|| self.instances.get(&id))
            .flatten()
    }

    pub fn master_by_id_mut(&mut self, id: SentinelInstanceId) -> Option<&mut SentinelInstance> {
        if !self
            .master_by_name
            .values()
            .any(|master_id| *master_id == id)
        {
            return None;
        }
        self.instances.get_mut(&id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SentinelRegistryError {
    DuplicateMasterName,
    DuplicateInstanceId,
    NoSuchMaster,
    InvalidRelatedRole,
}

impl std::fmt::Display for SentinelRegistryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::DuplicateMasterName => "Duplicate master name.",
            Self::DuplicateInstanceId => "Duplicate instance id.",
            Self::NoSuchMaster => "No such master with specified name.",
            Self::InvalidRelatedRole => "Only replica or sentinel instances may be related.",
        };
        formatter.write_str(message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceRole {
    Master,
    Replica,
    Sentinel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownState {
    Up,
    SDown,
    ODown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    None,
    WaitStart,
    SelectReplica,
    SendReplicaOfNoOne,
    SendFailover, //  SENTINEL FAILOVER  REPLICAOF NO ONE
    WaitPromotion,
    ReconfigureReplicas,
    UpdateConfig,
}

#[derive(Debug, Clone)]
pub struct SentinelPeerMasterState {
    pub reported_master_down: bool,
    pub last_master_down_reply: i64,
    pub leader: String,
    pub leader_epoch: i64,
}

#[derive(Debug, Clone)]
pub struct SentinelInstance {
    pub id: SentinelInstanceId,
    pub name: String,
    pub addr: String,
    pub role: InstanceRole,
    pub role_reported: InstanceRole,
    pub role_reported_time: i64,
    pub runid: String,
    pub flags: u64,
    pub reboot_time: i64, //
    pub down_state: DownState,
    pub down_after_ms: i64,
    pub quorum: i64,
    pub parallel_syncs: i64,
    pub failover_timeout: i64,
    pub failover_epoch: i64,
    pub failover_state: FailoverState,
    pub failover_state_change_time: i64,
    pub failover_start_time: i64,
    pub leader: String,
    pub leader_epoch: i64,
    pub promoted_replica: Option<SentinelInstanceId>,
    pub reconfig_sent: bool,
    pub reconfig_sent_time: i64,
    pub master_name: String,
    pub master_link_status: String,
    pub replicas: Vec<SentinelInstanceId>,
    pub sentinels: Vec<SentinelInstanceId>,
    pub peer_states: HashMap<SentinelInstanceId, SentinelPeerMasterState>,
    pub last_ping_time: i64,
    pub last_info_time: i64,
    pub current_epoch: i64,
    pub config_epoch: i64,
    pub sdown_time: i64,
    pub sdown_since_time: i64,
    pub odown_since_time: i64,
    pub slave_priority: i64,
    pub repl_offset: i64,
    pub link_state: i64,
    pub link_down_since_time: i64,
    pub last_ping_sent: i64,
    pub act_ping_time: i64,
    pub last_info_sent: i64,
    pub last_ping_ok: i64,
    pub last_hello_sent: i64,
    pub info_cache: String,
    pub info_refresh: i64,
    pub notification_script: String,
    pub client_reconfig_script: String,
    pub auth_pass: String,
    pub auth_user: String,
    pub primary_reboot_down_after_period: i64,
    pub renamed_commands: HashMap<String, String>,
    pub pending_commands: i64,
    pub replica_announced: bool,
    pub monitored_instance_failover_state: i64,
    pub monitored_instance_failover_change_time: i64,
}

impl SentinelInstance {
    pub fn new_master(id: SentinelInstanceId, name: &str, addr: &str, quorum: i64) -> Self {
        Self {
            id,
            name: name.to_string(),
            addr: addr.to_string(),
            role: InstanceRole::Master,
            role_reported: InstanceRole::Master,
            role_reported_time: 0,
            runid: String::new(),
            flags: 0,
            reboot_time: 0,
            down_state: DownState::Up,
            down_after_ms: 30000,
            quorum,
            parallel_syncs: 1,
            failover_timeout: 180000,
            failover_epoch: 0,
            failover_state: FailoverState::None,
            failover_state_change_time: 0,
            failover_start_time: 0,
            leader: String::new(),
            leader_epoch: 0,
            promoted_replica: None,
            reconfig_sent: false,
            reconfig_sent_time: 0,
            master_name: String::new(),
            master_link_status: String::new(),
            replicas: Vec::new(),
            sentinels: Vec::new(),
            peer_states: HashMap::new(),
            last_ping_time: 0,
            last_info_time: 0,
            current_epoch: 0,
            config_epoch: 0,
            sdown_time: 0,
            sdown_since_time: 0,
            odown_since_time: 0,
            slave_priority: 100,
            repl_offset: 0,
            link_state: 0,
            link_down_since_time: 0,
            last_ping_sent: 0,
            act_ping_time: 0,
            last_info_sent: 0,
            last_ping_ok: 0,
            last_hello_sent: 0,
            info_cache: String::new(),
            info_refresh: 0,
            notification_script: String::new(),
            client_reconfig_script: String::new(),
            auth_pass: String::new(),
            auth_user: String::new(),
            primary_reboot_down_after_period: 0,
            renamed_commands: HashMap::new(),
            pending_commands: 0,
            replica_announced: true,
            monitored_instance_failover_state: MONITORED_INSTANCE_FAILOVER_NS,
            monitored_instance_failover_change_time: 0,
        }
    }
}

#[derive(Debug)]
pub struct SentinelState {
    pub myid: String,
    pub current_epoch: i64,
    pub(crate) registry: SentinelRegistry,
    pub next_instance_id: SentinelInstanceId,
    pub initialized: bool,
    pub config_file: String,
    pub sentinel_mode: bool,
    pub announce_addr: String,
    pub announce_ip: String,
    pub announce_port: i32,
    pub resolve_hostnames: bool,
    pub announce_hostnames: bool,
    pub sentinel_auth_user: String,
    pub sentinel_auth_pass: String,
    pub deny_scripts_reconfig: bool,

    /// Timing overrides used by `SENTINEL DEBUG` and the runtime scheduler.
    pub tunables: SentinelTunables,

    pub simfailure_flags: u32,

    pub tilt: bool,
    pub tilt_start_time: i64,
    pub total_tilt: i64,
    pub previous_time: i64,
    pub script_engine: script::SentinelScriptEngine,
    pub link_rebuild_needed: bool,
    pub pending_link_close: Vec<String>,
}

#[derive(Debug)]
struct SentinelConfigDraft {
    myid: String,
    current_epoch: i64,
    registry: SentinelRegistry,
    next_instance_id: SentinelInstanceId,
    announce_ip: String,
    announce_port: i32,
    resolve_hostnames: bool,
    announce_hostnames: bool,
    sentinel_auth_user: String,
    sentinel_auth_pass: String,
    deny_scripts_reconfig: bool,
}

impl SentinelConfigDraft {
    fn from_state(state: &SentinelState) -> Self {
        Self {
            myid: state.myid.clone(),
            current_epoch: state.current_epoch,
            registry: state.registry.clone(),
            next_instance_id: state.next_instance_id,
            announce_ip: state.announce_ip.clone(),
            announce_port: state.announce_port,
            resolve_hostnames: state.resolve_hostnames,
            announce_hostnames: state.announce_hostnames,
            sentinel_auth_user: state.sentinel_auth_user.clone(),
            sentinel_auth_pass: state.sentinel_auth_pass.clone(),
            deny_scripts_reconfig: state.deny_scripts_reconfig,
        }
    }

    fn apply(&mut self, plan: &SentinelConfigPlan) -> Result<(), String> {
        for directive in &plan.pre {
            self.apply_pre_monitor(directive);
        }
        for directive in &plan.monitors {
            self.apply_monitor(directive)?;
        }
        for directive in &plan.post {
            self.apply_post_monitor(directive)?;
        }
        Ok(())
    }

    fn publish(self, state: &mut SentinelState) {
        state.myid = self.myid;
        state.current_epoch = self.current_epoch;
        state.registry = self.registry;
        state.next_instance_id = self.next_instance_id;
        state.announce_ip = self.announce_ip;
        state.announce_port = self.announce_port;
        state.resolve_hostnames = self.resolve_hostnames;
        state.announce_hostnames = self.announce_hostnames;
        state.sentinel_auth_user = self.sentinel_auth_user;
        state.sentinel_auth_pass = self.sentinel_auth_pass;
        state.deny_scripts_reconfig = self.deny_scripts_reconfig;
    }

    fn apply_pre_monitor(&mut self, directive: &PreMonitorDirective) {
        match directive {
            PreMonitorDirective::MyId(value) => self.myid.clone_from(value),
            PreMonitorDirective::CurrentEpoch(epoch) if *epoch > self.current_epoch => {
                self.current_epoch = *epoch;
            }
            PreMonitorDirective::DenyScriptsReconfig(value) => {
                self.deny_scripts_reconfig = *value;
            }
            PreMonitorDirective::SentinelUser(value) => self.sentinel_auth_user.clone_from(value),
            PreMonitorDirective::SentinelPass(value) => self.sentinel_auth_pass.clone_from(value),
            PreMonitorDirective::AnnounceIp(value) => self.announce_ip.clone_from(value),
            PreMonitorDirective::AnnouncePort(value) => self.announce_port = *value,
            PreMonitorDirective::ResolveHostnames(value) => self.resolve_hostnames = *value,
            PreMonitorDirective::AnnounceHostnames(value) => self.announce_hostnames = *value,
            PreMonitorDirective::CurrentEpoch(_) => {}
        }
    }

    fn apply_monitor(&mut self, directive: &MonitorDirective) -> Result<(), String> {
        let id = self.next_instance_id;
        let instance =
            SentinelInstance::new_master(id, &directive.name, &directive.addr, directive.quorum);
        self.registry
            .insert_master(directive.name.clone(), instance)
            .map_err(|error| error.to_string())?;
        self.advance_instance_id()
    }

    fn apply_post_monitor(&mut self, directive: &PostMonitorDirective) -> Result<(), String> {
        match directive {
            PostMonitorDirective::Number { name, field, value } => {
                self.apply_master_number(name, *field, *value);
            }
            PostMonitorDirective::Text { name, field, value } => {
                self.apply_master_text(name, *field, value);
            }
            PostMonitorDirective::RenameCommand {
                name,
                source,
                target,
            } => {
                self.registry
                    .master_mut(name)
                    .expect("master reference validated before config apply")
                    .renamed_commands
                    .insert(source.clone(), target.clone());
            }
            PostMonitorDirective::KnownReplica { name, addr } => {
                self.apply_known_instance(name, addr, None)?;
            }
            PostMonitorDirective::KnownSentinel { name, addr, runid } => {
                self.apply_known_instance(name, addr, Some(runid))?;
            }
            PostMonitorDirective::Ignore => {}
        }
        Ok(())
    }

    fn apply_master_number(&mut self, name: &str, field: MasterNumberField, value: i64) {
        let master = self
            .registry
            .master_mut(name)
            .expect("master reference validated before config apply");
        match field {
            MasterNumberField::DownAfterMs => master.down_after_ms = value,
            MasterNumberField::FailoverTimeout => master.failover_timeout = value,
            MasterNumberField::ParallelSyncs => master.parallel_syncs = value,
            MasterNumberField::PrimaryRebootDownAfter => {
                master.primary_reboot_down_after_period = value;
            }
            MasterNumberField::ConfigEpoch => {
                master.config_epoch = value;
                self.current_epoch = self.current_epoch.max(value);
            }
            MasterNumberField::LeaderEpoch => master.leader_epoch = value,
        }
    }

    fn apply_master_text(&mut self, name: &str, field: MasterTextField, value: &str) {
        let master = self
            .registry
            .master_mut(name)
            .expect("master reference validated before config apply");
        match field {
            MasterTextField::NotificationScript => master.notification_script = value.to_string(),
            MasterTextField::ClientReconfigScript => {
                master.client_reconfig_script = value.to_string();
            }
            MasterTextField::AuthPass => master.auth_pass = value.to_string(),
            MasterTextField::AuthUser => master.auth_user = value.to_string(),
        }
    }

    fn apply_known_instance(
        &mut self,
        name: &str,
        addr: &str,
        runid: Option<&String>,
    ) -> Result<(), String> {
        let quorum = self
            .registry
            .master(name)
            .expect("master reference validated before config apply")
            .quorum;
        let id = self.next_instance_id;
        let mut instance = SentinelInstance::new_master(id, name, addr, quorum);
        instance.master_name = name.to_string();
        if let Some(runid) = runid {
            instance.role = InstanceRole::Sentinel;
            instance.role_reported = InstanceRole::Sentinel;
            instance.runid.clone_from(runid);
        } else {
            instance.role = InstanceRole::Replica;
            instance.role_reported = InstanceRole::Replica;
        }
        self.registry
            .insert_related_instance(name, instance)
            .map_err(|error| error.to_string())?;
        self.advance_instance_id()
    }

    fn advance_instance_id(&mut self) -> Result<(), String> {
        self.next_instance_id = self
            .next_instance_id
            .checked_add(1)
            .ok_or_else(|| "Sentinel instance id overflow.".to_string())?;
        Ok(())
    }
}

impl SentinelState {
    pub fn new() -> Self {
        Self {
            myid: Self::generate_myid(),
            current_epoch: 0,
            registry: SentinelRegistry::default(),
            next_instance_id: 1,
            initialized: false,
            config_file: String::new(),
            sentinel_mode: false,
            announce_addr: String::new(),
            announce_ip: String::new(),
            announce_port: 0,
            resolve_hostnames: false,
            announce_hostnames: false,
            sentinel_auth_user: String::new(),
            sentinel_auth_pass: String::new(),
            deny_scripts_reconfig: true, //

            tunables: SentinelTunables::default(),
            simfailure_flags: 0,

            tilt: false,
            tilt_start_time: 0,
            total_tilt: 0,
            previous_time: 0,

            script_engine: script::SentinelScriptEngine::new(),
            link_rebuild_needed: false,
            pending_link_close: Vec::new(),
        }
    }

    fn generate_myid() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let ts = now.as_nanos();
        let high = ts >> 64;
        let mid = (ts >> 32) & 0xffffffff;
        let low = ts & 0xffffffff;
        format!(
            "{:016x}{:08x}{:08x}{:08x}",
            high,
            mid,
            low,
            high ^ mid ^ low
        )
    }

    pub fn load_config_directives(
        &mut self,
        directives: &[String],
        script_validator: &dyn SentinelConfigScriptValidator,
    ) -> Result<(), String> {
        let plan = SentinelConfigPlan::build(
            directives,
            &self.registry,
            self.resolve_hostnames,
            script_validator,
        )?;
        let mut draft = SentinelConfigDraft::from_state(self);
        draft.apply(&plan)?;
        draft.publish(self);
        Ok(())
    }

    pub fn check_tilt_condition(&mut self, now: i64) {
        if self.previous_time == 0 {
            self.previous_time = now;
            return;
        }
        let delta = now - self.previous_time;
        self.previous_time = now;

        if (delta < 0 || delta > self.tunables.tilt_trigger_ms) && !self.tilt {
            self.tilt = true;
            self.tilt_start_time = now;
            self.total_tilt += 1; //  C  sentinel.c:5460 sentinel.total_tilt++
            crate::engine::distributed::sentinel::event::sentinel_event(
                &mut self.script_engine,
                "warning",
                "+tilt",
                None,
                &format!("delta={}", delta),
            );
        }

        if self.tilt && (now - self.tilt_start_time > self.tunables.tilt_period_ms) {
            self.tilt = false;
            self.tilt_start_time = 0;
            crate::engine::distributed::sentinel::event::sentinel_event(
                &mut self.script_engine,
                "notice",
                "-tilt",
                None,
                "tilt mode exited",
            );
        }
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod state_tests;
