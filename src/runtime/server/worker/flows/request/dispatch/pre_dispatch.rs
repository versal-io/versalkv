use crate::engine::command::{
    CommandArgs, CommandError, CMD_ALLOW_BUSY, CMD_DENYOOM, CMD_LOADING, CMD_MAY_REPLICATE,
    CMD_NO_ASYNC_LOADING, CMD_SENTINEL, CMD_STALE, CMD_WRITE,
};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::PauseAction;

use super::super::super::ServerWorkerState;
use super::script_helpers::{is_eval_without_shebang, is_ro_script};
use super::{
    CmdCallFlags, CmdPreconditions, CommandClientFacts, DispatchAction, DispatchOutcome,
    PostponeReason,
};

impl ServerWorkerState {
    pub(super) fn stage_sentinel_mode(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        _call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        if self.ctx.sentinel_state().sentinel_mode && (meta.flags & CMD_SENTINEL) == 0 {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags: CmdCallFlags::default(),
                reject_reply: Some(CommandError::unknown(&args.name)),
            });
        }
        None
    }

    pub(crate) fn stage_arity(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        _call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let argc = args.args.len() as i32 + 1;
        let arity_ok = if meta.arity > 0 {
            argc == meta.arity
        } else {
            argc >= -meta.arity
        };
        if !arity_ok {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags: CmdCallFlags::default(),
                reject_reply: Some(CommandError::wrong_arity(meta.name.as_str())),
            });
        }
        None
    }

    pub(super) fn stage_import_mode_oom(
        &self,
        facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        _args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let import_mode = self.ctx.config().server.import_mode || facts.import_source;
        if import_mode
            && (meta.flags & CMD_DENYOOM) != 0
            && self.ctx.maxmemory_state().is_over_limit()
        {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "OOM command not allowed when used memory > 'maxmemory'.",
                )),
            });
        }
        None
    }

    pub(super) fn stage_no_eviction_candidate_oom(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        _args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let config = self.ctx.config();
        if self.ctx.replication().is_replica() && config.replication.replica_ignore_maxmemory {
            return None;
        }
        let policy = &config.server.maxmemory_policy;
        if config.server.maxmemory == 0
            || (meta.flags & CMD_DENYOOM) == 0
            || !policy.starts_with("volatile-")
        {
            return None;
        }
        let has_candidate = (0..self.ctx.num_dbs()).any(|db_idx| {
            self.ctx
                .db(db_idx as u32)
                .is_some_and(|db| db.expires_iter().any(|(k, _)| !db.is_importing_slot_key(k)))
        });
        if self.ctx.maxmemory_state().is_over_limit() && !has_candidate {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "OOM command not allowed when used memory > 'maxmemory'.",
                )),
            });
        }
        None
    }

    pub(super) fn stage_noeviction_oom(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        _args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let config = self.ctx.config();
        if self.ctx.replication().is_replica() && config.replication.replica_ignore_maxmemory {
            return None;
        }
        if config.server.maxmemory == 0
            || (meta.flags & CMD_DENYOOM) == 0
            || config.server.maxmemory_policy != "noeviction"
        {
            return None;
        }
        if self.ctx.maxmemory_state().is_over_limit() {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "OOM command not allowed when used memory > 'maxmemory'.",
                )),
            });
        }
        None
    }

    pub(super) fn stage_script_busy(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        _args: &CommandArgs,
        _call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let is_busy_control = matches!(meta.name.as_str(), "script" | "function");
        if self.ctx.is_loading() {
            return None;
        }
        if self.ctx.is_script_busy() && (meta.flags & CMD_ALLOW_BUSY) == 0 && !is_busy_control {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags: CmdCallFlags::default(),
                reject_reply: Some(Value::error(
                    "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
                )),
            });
        }
        None
    }

    pub(super) fn stage_loading(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        _args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let async_loading = self.ctx.replication().async_loading;
        let internal_loading = self.ctx.is_loading();
        let reject = || {
            Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "LOADING Valkey is loading the dataset in memory",
                )),
            })
        };
        if internal_loading && !async_loading && (meta.flags & CMD_LOADING) == 0 {
            return reject();
        }
        if self.ctx.is_host_task_loading() && matches!(meta.name.as_str(), "replicaof" | "slaveof")
        {
            return reject();
        }
        if async_loading && (meta.flags & CMD_NO_ASYNC_LOADING) != 0 {
            return reject();
        }
        None
    }

    pub(crate) fn stage_cluster_redirect(
        &self,
        facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        if !self.ctx.config().server.cluster_enabled {
            return None;
        }
        // First pass (immutable cluster borrow): extract keys and detect whether
        // the target slot is open (MIGRATING/IMPORTING). Only open slots need a
        // dataset lookup, so the stable-slot path stays lookup-free.
        let (keys, slot_open) = {
            let Some(state) = self.ctx.cluster_state() else {
                return None;
            };
            let keys = Self::cluster_redirect_keys(meta, args);
            let slot_open = !keys.is_empty()
                && state.is_slot_open(crate::engine::distributed::cluster::node::key_hash_slot(
                    &keys[0],
                ) as usize);
            (keys, slot_open)
        };
        // For open slots, compute per-key local existence (NOTOUCH/NOEXPIRE
        // semantics) so ASK vs TRYAGAIN can be decided correctly.
        let route_sharded_pubsub = matches!(meta.name.as_str(), "ssubscribe" | "spublish");
        let existing: Option<Vec<bool>> =
            if slot_open && !route_sharded_pubsub && !facts.transaction {
                let db_id = facts.selected_db;
                let flags: Vec<bool> = self
                    .ctx
                    .db(db_id)
                    .map(|db| keys.iter().map(|k| db.get_no_expire(k).is_some()).collect())
                    .unwrap_or_else(|| vec![false; keys.len()]);
                Some(flags)
            } else {
                None
            };
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let Some(state) = self.ctx.cluster_state() else {
            return None;
        };
        let allow_replica_read = route_sharded_pubsub
            || (facts.readonly && (meta.flags & crate::engine::command::CMD_READONLY) != 0);
        let client_readonly = facts.readonly;
        let redirect_keyless = facts.keyless_redirect;
        let preferred_endpoint = match self
            .ctx
            .config()
            .server
            .cluster_preferred_endpoint_type
            .as_str()
        {
            "hostname" => crate::engine::distributed::cluster::PreferredEndpoint::Hostname,
            "unknown-endpoint" => crate::engine::distributed::cluster::PreferredEndpoint::Unknown,
            _ => crate::engine::distributed::cluster::PreferredEndpoint::Ip,
        };
        let prefer_ipv6 = facts.peer_addr.is_ipv6();
        let decision =
            state
                .projection()
                .decide_route(crate::engine::distributed::cluster::RouteRequest {
                    keys: &key_refs,
                    command_flags: meta.flags,
                    existing_keys: existing.as_deref(),
                    allow_replica_read,
                    preferred_endpoint,
                    transport: crate::engine::distributed::cluster::ClientTransport::Tcp,
                    prefer_ipv6,
                    // C cluster.c permits sharded Pub/Sub through a failed
                    // cluster only when cluster-allow-pubsubshard-when-down
                    // is enabled. Ordinary command health gating remains in
                    // its existing command-class paths.
                    require_cluster_ok: route_sharded_pubsub
                        && !self.ctx.config().server.cluster_allow_pubsubshard_when_down,
                    route_sharded_pubsub,
                    redirect_keyless,
                    client_readonly,
                    client_asking: call_flags.client_asking,
                    open_slot_policy: open_slot_policy(meta.name.as_str(), args, facts.selected_db),
                });
        if decision != crate::engine::distributed::cluster::ClusterRouteDecision::ExecuteLocal {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(route_error_value(decision)),
            });
        }
        None
    }

    /// Redirect read/write commands from a standalone replica when the client
    /// opted into redirect handling. This gate precedes READONLY, matching C
    /// `processCommand`, so capable clients see the primary endpoint instead.
    pub(super) fn stage_standalone_replica_redirect(
        &self,
        facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        _args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let accesses_keyspace = meta.flags
            & (crate::engine::command::CMD_READONLY
                | crate::engine::command::CMD_WRITE
                | crate::engine::command::CMD_MAY_REPLICATE)
            != 0;
        if facts.monitoring && accesses_keyspace {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error("ERR Replica can't interact with the keyspace")),
            });
        }
        self.standalone_replica_redirect_outcome_for_facts(facts, meta.flags, call_flags)
    }

    pub(crate) fn standalone_replica_redirect_outcome(
        &self,
        connection_token: usize,
        command_flags: u64,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let facts = CommandClientFacts::capture(
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)],
        );
        self.standalone_replica_redirect_outcome_for_facts(&facts, command_flags, call_flags)
    }

    fn standalone_replica_redirect_outcome_for_facts(
        &self,
        facts: &CommandClientFacts,
        command_flags: u64,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let is_write = (command_flags & CMD_WRITE) != 0;
        let is_read = (command_flags & crate::engine::command::CMD_READONLY) != 0;
        let should_redirect = is_write || (is_read && !facts.readonly);
        let config = self.ctx.config();
        if config.server.cluster_enabled
            || !self.ctx.replication().is_replica()
            || !facts.keyless_redirect
            || !should_redirect
        {
            return None;
        }
        let host = config.replication.master_host.as_deref()?;
        if self.ctx.replication().failover_state == "failover-in-progress" {
            return Some(DispatchOutcome {
                action: DispatchAction::Postpone(PostponeReason::ClientPause),
                call_flags,
                reject_reply: None,
            });
        }
        Some(DispatchOutcome {
            action: DispatchAction::Rejected,
            call_flags,
            reject_reply: Some(Value::error(format!(
                "REDIRECT {host}:{}",
                config.replication.master_port
            ))),
        })
    }

    pub(crate) fn cluster_redirect_keys(
        meta: &CmdPreconditions,
        args: &CommandArgs,
    ) -> Vec<Vec<u8>> {
        match meta.name.as_str() {
            "ssubscribe" => {
                return crate::framework::commands::pubsub::ssubscribe::route_channels(args);
            }
            "spublish" => {
                return crate::framework::commands::pubsub::spublish::route_channels(args);
            }
            _ => {}
        }
        if let Some(get_keys) = meta.get_keys {
            return get_keys(&args.args)
                .into_iter()
                .map(|(key, _)| key.to_vec())
                .collect();
        }
        if meta.first_key <= 0 || meta.key_step == 0 {
            return Vec::new();
        }
        let argc = args.args.len() as i32;
        let last = if meta.last_key < 0 {
            argc + meta.last_key + 1
        } else {
            meta.last_key
        };
        if last < meta.first_key {
            return Vec::new();
        }
        let mut keys = Vec::new();
        let mut position = meta.first_key;
        while position <= last && position <= argc {
            keys.push(args.args[(position - 1) as usize].to_vec());
            position += meta.key_step;
        }
        keys
    }

    pub(super) fn stage_client_pause(
        &self,
        facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        if facts.migration_control {
            return None;
        }
        let in_transaction = facts.transaction;
        if !in_transaction {
            let now = mstime();
            let pause = self.ctx.client_pause().is_paused(now);
            let has_write_flag = (meta.flags & (CMD_WRITE | CMD_MAY_REPLICATE)) != 0;
            let is_ro = if has_write_flag {
                is_ro_script(meta.name.as_str(), args, &self.ctx)
            } else {
                true
            };
            let should_pause = match pause {
                PauseAction::None => false,
                PauseAction::Write => has_write_flag && !is_ro,
                PauseAction::All => true,
            };
            if should_pause {
                return Some(DispatchOutcome {
                    action: DispatchAction::Postpone(PostponeReason::ClientPause),
                    call_flags,
                    reject_reply: None,
                });
            }
        }
        None
    }

    pub(super) fn stage_slot_migration_pause(
        &self,
        facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        if !call_flags.is_write || facts.transaction {
            return None;
        }
        let keys = Self::cluster_redirect_keys(meta, args);
        let job_id = self.ctx.cluster_state().as_deref().and_then(|state| {
            keys.iter().find_map(|key| {
                state.migrations.paused_export_for_slot(
                    crate::engine::distributed::cluster::node::key_hash_slot(key),
                )
            })
        })?;
        Some(DispatchOutcome {
            action: DispatchAction::Postpone(PostponeReason::SlotMigration(job_id)),
            call_flags,
            reject_reply: None,
        })
    }

    pub(super) fn stage_bgsave_error(
        &mut self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        self.reap_bgsave_child();
        // Gated reap: only retires a lingering diskless child when no relay is in flight (ABA-safe).
        self.collect_idle_diskless_child_exit();
        let has_write_flag = (meta.flags & CMD_WRITE) != 0;
        if (has_write_flag || meta.name.as_str().eq_ignore_ascii_case("ping"))
            && self.ctx.config().persistence.stop_writes_on_bgsave_error
            && !self.ctx.config().persistence.saveparams.is_empty()
            && self.ctx.persistence().rdb().last_bgsave_status
                == crate::engine::persistence::OperationStatus::Err
            && !is_ro_script(meta.name.as_str(), args, &self.ctx)
            && !meta.name.as_str().eq_ignore_ascii_case("eval")
        {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "MISCONF Errors writing to the RDB file, writes are disabled because stop-writes-on-bgsave-error is enabled.",
                )),
            });
        }
        None
    }

    pub(super) fn stage_min_replicas(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let has_write_flag = (meta.flags & CMD_WRITE) != 0;
        let replication = &self.ctx.config().replication;
        if has_write_flag
            && replication.min_replicas_max_lag > 0
            && replication.min_replicas_to_write > self.good_replicas_count()
            && !is_ro_script(meta.name.as_str(), args, &self.ctx)
            && !is_eval_without_shebang(meta.name.as_str(), args)
        {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "NOREPLICAS Not enough good replicas to write.",
                )),
            });
        }
        None
    }

    pub(super) fn stage_readonly_replica(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        let has_write_flag = (meta.flags & CMD_WRITE) != 0;
        if has_write_flag
            && self.ctx.replication().is_replica()
            && self.ctx.config().replication.replica_read_only
            && !is_ro_script(meta.name.as_str(), args, &self.ctx)
            && !is_eval_without_shebang(meta.name.as_str(), args)
        {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "READONLY You can't write against a read only replica.",
                )),
            });
        }
        None
    }

    pub(super) fn stage_stale_replica(
        &self,
        _facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        _args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        if self.ctx.replication().is_replica()
            && self.ctx.config().replication.master_link_status != "up"
            && !self.ctx.config().replication.replica_serve_stale_data
            && (meta.flags & CMD_STALE) == 0
        {
            return Some(DispatchOutcome {
                action: DispatchAction::Rejected,
                call_flags,
                reject_reply: Some(Value::error(
                    "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
                )),
            });
        }
        None
    }
}

fn open_slot_policy(
    command: &str,
    args: &CommandArgs,
    selected_db: u32,
) -> crate::engine::distributed::cluster::OpenSlotPolicy {
    use crate::engine::distributed::cluster::OpenSlotPolicy;
    if command.eq_ignore_ascii_case("migrate") {
        return OpenSlotPolicy::ExecuteLocal;
    }
    if command.eq_ignore_ascii_case("move") {
        return OpenSlotPolicy::RejectUnstable;
    }
    if command.eq_ignore_ascii_case("copy") {
        let cross_db = args.args.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case(b"db")
                && std::str::from_utf8(&pair[1])
                    .ok()
                    .and_then(|value| value.parse::<u32>().ok())
                    != Some(selected_db)
        });
        if cross_db {
            return OpenSlotPolicy::RejectUnstable;
        }
    }
    OpenSlotPolicy::Standard
}

pub(crate) fn route_error_value(
    decision: crate::engine::distributed::cluster::ClusterRouteDecision,
) -> Value {
    Value::error(
        decision
            .error_message()
            .expect("ExecuteLocal is handled before route error conversion"),
    )
}
