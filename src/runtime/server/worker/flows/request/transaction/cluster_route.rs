use crate::engine::command::{CommandArgs, CMD_ADMIN, CMD_WRITE};
use crate::engine::distributed::cluster::{
    ClientTransport, ClusterRouteDecision, ClusterTopologySnapshot, PreferredEndpoint, RouteRequest,
};
use crate::framework::CommandRegistry;
use crate::runtime::server::worker::flows::request::dispatch::CmdPreconditions;
use crate::runtime::server::worker::ServerWorkerState;

pub(super) struct TransactionRouteContext<'a> {
    pub registry: &'a CommandRegistry,
    pub snapshot: &'a ClusterTopologySnapshot,
    pub selected_db: u32,
    pub num_dbs: usize,
    pub client_readonly: bool,
    pub client_asking: bool,
    pub redirect_keyless: bool,
    pub preferred_endpoint: PreferredEndpoint,
    pub transport: ClientTransport,
    pub prefer_ipv6: bool,
    pub key_exists: &'a dyn Fn(u32, &[u8]) -> bool,
}

struct TransactionRouteFacts {
    keys: Vec<(u32, Vec<u8>)>,
    command_flags: u64,
    has_sharded_pubsub: bool,
    open_slot_policy: crate::engine::distributed::cluster::OpenSlotPolicy,
}

pub(super) fn validate_transaction_route(
    queued: &[CommandArgs],
    context: TransactionRouteContext<'_>,
) -> Result<Option<u16>, ClusterRouteDecision> {
    let facts = transaction_route_facts(
        queued,
        context.registry,
        context.selected_db,
        context.num_dbs,
    );
    let key_refs = facts
        .keys
        .iter()
        .map(|(_, key)| key.as_slice())
        .collect::<Vec<_>>();
    let slot = common_slot(&key_refs);
    let existing = transaction_existing_keys(&facts, slot, &context);
    let has_write = (facts.command_flags & CMD_WRITE) != 0;
    let decision = context.snapshot.decide_route(RouteRequest {
        keys: &key_refs,
        command_flags: facts.command_flags,
        existing_keys: existing.as_deref(),
        allow_replica_read: !has_write && (context.client_readonly || facts.has_sharded_pubsub),
        preferred_endpoint: context.preferred_endpoint,
        transport: context.transport,
        prefer_ipv6: context.prefer_ipv6,
        require_cluster_ok: false,
        // EXEC must route the aggregate command even when one queued command
        // carries CMD_PUBSUB; ordinary PUBLISH contributes no key here.
        route_sharded_pubsub: true,
        redirect_keyless: context.redirect_keyless,
        client_readonly: context.client_readonly,
        client_asking: context.client_asking,
        open_slot_policy: facts.open_slot_policy,
    });
    if decision == ClusterRouteDecision::ExecuteLocal {
        Ok(slot)
    } else {
        Err(decision)
    }
}

fn transaction_route_facts(
    queued: &[CommandArgs],
    registry: &CommandRegistry,
    selected_db: u32,
    num_dbs: usize,
) -> TransactionRouteFacts {
    let mut facts = TransactionRouteFacts {
        keys: Vec::new(),
        command_flags: 0,
        has_sharded_pubsub: false,
        open_slot_policy: crate::engine::distributed::cluster::OpenSlotPolicy::Standard,
    };
    let mut db_id = selected_db;
    for command in queued {
        let Some(spec) = registry.lookup(&command.name) else {
            continue;
        };
        // EXEC itself is not administrative, so an administrative subcommand
        // cannot exempt other queued keyed commands from aggregate routing.
        facts.command_flags |= spec.flags & !CMD_ADMIN;
        let is_sharded_pubsub = command.name.eq_ignore_ascii_case(b"spublish")
            || command.name.eq_ignore_ascii_case(b"ssubscribe");
        facts.has_sharded_pubsub |= is_sharded_pubsub;
        if command.name.eq_ignore_ascii_case(b"move") || copy_targets_another_db(command, db_id) {
            facts.open_slot_policy =
                crate::engine::distributed::cluster::OpenSlotPolicy::RejectUnstable;
        } else if command.name.eq_ignore_ascii_case(b"migrate")
            && facts.open_slot_policy
                == crate::engine::distributed::cluster::OpenSlotPolicy::Standard
        {
            facts.open_slot_policy =
                crate::engine::distributed::cluster::OpenSlotPolicy::ExecuteLocal;
        }
        facts.keys.extend(
            transaction_command_keys(command, spec)
                .into_iter()
                .map(|key| (db_id, key)),
        );
        db_id = selected_db_after(command, db_id, num_dbs);
    }
    facts
}

fn copy_targets_another_db(command: &CommandArgs, current_db: u32) -> bool {
    command.name.eq_ignore_ascii_case(b"copy")
        && command.args.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case(b"db")
                && std::str::from_utf8(&pair[1])
                    .ok()
                    .and_then(|value| value.parse::<u32>().ok())
                    != Some(current_db)
        })
}

fn transaction_command_keys(
    command: &CommandArgs,
    spec: &crate::framework::CommandSpec,
) -> Vec<Vec<u8>> {
    if command.name.eq_ignore_ascii_case(b"spublish") {
        return crate::framework::commands::pubsub::spublish::route_channels(command);
    }
    if command.name.eq_ignore_ascii_case(b"ssubscribe") {
        return crate::framework::commands::pubsub::ssubscribe::route_channels(command);
    }
    ServerWorkerState::cluster_redirect_keys(
        &CmdPreconditions {
            name: spec.name.clone(),
            arity: spec.arity,
            flags: spec.flags,
            first_key: spec.first_key,
            last_key: spec.last_key,
            key_step: spec.key_step,
            key_flags: spec.key_flags,
            acl_categories: spec.acl_categories,
            get_keys: spec.get_keys,
        },
        command,
    )
}

fn selected_db_after(command: &CommandArgs, current: u32, num_dbs: usize) -> u32 {
    if !command.name.eq_ignore_ascii_case(b"select") {
        return current;
    }
    command
        .args
        .first()
        .and_then(|arg| std::str::from_utf8(arg).ok())
        .and_then(|arg| arg.parse::<u32>().ok())
        .filter(|db_id| (*db_id as usize) < num_dbs)
        .unwrap_or(current)
}

fn common_slot(keys: &[&[u8]]) -> Option<u16> {
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(keys.first()?);
    keys.iter()
        .all(|key| crate::engine::distributed::cluster::node::key_hash_slot(key) == slot)
        .then_some(slot)
}

fn transaction_existing_keys(
    facts: &TransactionRouteFacts,
    slot: Option<u16>,
    context: &TransactionRouteContext<'_>,
) -> Option<Vec<bool>> {
    if facts.has_sharded_pubsub || !slot.is_some_and(|slot| context.snapshot.is_slot_open(slot)) {
        return None;
    }
    Some(
        facts
            .keys
            .iter()
            .map(|(db_id, key)| (context.key_exists)(*db_id, key))
            .collect(),
    )
}

impl ServerWorkerState {
    pub(super) fn exec_cluster_metric_slot(&self, connection_token: usize) -> Option<u16> {
        if !self.ctx.config().server.cluster_enabled
            || !self.ctx.config().server.cluster_slot_stats_enabled
            || !self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .is_active()
        {
            return None;
        }
        let facts = transaction_route_facts(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .transaction
                .queued_commands(),
            &self.registry,
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .selected_db,
            self.ctx.num_dbs(),
        );
        let key_refs = facts
            .keys
            .iter()
            .map(|(_, key)| key.as_slice())
            .collect::<Vec<_>>();
        common_slot(&key_refs)
    }

    pub(super) fn exec_cluster_route_decision(
        &self,
        connection_token: usize,
    ) -> Option<ClusterRouteDecision> {
        if !self.ctx.config().server.cluster_enabled
            || !self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .transaction
            .is_active()
        {
            return None;
        }
        let snapshot = self.ctx.cluster_state().as_deref()?.topology_snapshot();
        let conn =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        let preferred_endpoint = match self
            .ctx
            .config()
            .server
            .cluster_preferred_endpoint_type
            .as_str()
        {
            "hostname" => PreferredEndpoint::Hostname,
            "unknown-endpoint" => PreferredEndpoint::Unknown,
            _ => PreferredEndpoint::Ip,
        };
        let key_exists = |db_id, key: &[u8]| {
            self.ctx
                .db(db_id)
                .is_some_and(|db| db.get_no_expire(key).is_some())
        };
        validate_transaction_route(
            conn.transaction.queued_commands(),
            TransactionRouteContext {
                registry: &self.registry,
                snapshot: &snapshot,
                selected_db: conn.selected_db,
                num_dbs: self.ctx.num_dbs(),
                client_readonly: conn.readonly,
                client_asking: conn.asking,
                redirect_keyless: conn
                    .capa_flags
                    .iter()
                    .any(|capability| capability.eq_ignore_ascii_case("redirect")),
                preferred_endpoint,
                transport: ClientTransport::Tcp,
                prefer_ipv6: conn.peer_addr.is_ipv6(),
                key_exists: &key_exists,
            },
        )
        .err()
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_transaction_route, TransactionRouteContext};
    use crate::engine::command::CommandArgs;
    use crate::engine::distributed::cluster::node::{
        key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::{
        ClientTransport, ClusterRouteDecision, ClusterState, PreferredEndpoint,
    };
    use crate::framework::{commands, CommandRegistry};
    use bytes::Bytes;

    const REMOTE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    fn registry() -> CommandRegistry {
        let mut registry = CommandRegistry::new();
        commands::register_all(&mut registry);
        registry
    }

    fn state_with_remote_primary() -> ClusterState {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut remote = ClusterNode::new(
            REMOTE.to_string(),
            "192.0.2.1".to_string(),
            6380,
            16380,
            true,
        );
        remote.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(REMOTE.to_string(), remote);
        state.state = crate::engine::distributed::cluster::state::CLUSTER_OK;
        state
    }

    fn context<'a>(
        registry: &'a CommandRegistry,
        snapshot: &'a crate::engine::distributed::cluster::ClusterTopologySnapshot,
        key_exists: &'a dyn Fn(u32, &[u8]) -> bool,
    ) -> TransactionRouteContext<'a> {
        TransactionRouteContext {
            registry,
            snapshot,
            selected_db: 0,
            num_dbs: 16,
            client_readonly: false,
            client_asking: false,
            redirect_keyless: false,
            preferred_endpoint: PreferredEndpoint::Ip,
            transport: ClientTransport::Tcp,
            prefer_ipv6: false,
            key_exists,
        }
    }

    #[test]
    fn invalid_select_does_not_change_database_for_open_slot_key_checks() {
        let registry = registry();
        let key = b"{3560}key";
        let slot = key_hash_slot(key);
        let mut state = state_with_remote_primary();
        state
            .set_slot_node(slot as usize, &state.myself.name.clone())
            .unwrap();
        state.seed_migrating_slot(slot as usize, REMOTE);
        let snapshot = state.topology_snapshot();
        let exists = |db_id, candidate: &[u8]| db_id == 0 && candidate == key;
        let queued = vec![args(&[b"SELECT", b"100"]), args(&[b"EXISTS", key])];

        assert_eq!(
            validate_transaction_route(&queued, context(&registry, &snapshot, &exists)),
            Ok(Some(slot))
        );
    }

    #[test]
    fn exec_recomputes_moved_from_the_current_owner() {
        let registry = registry();
        let queued = vec![args(&[b"SET", b"{tx}key", b"value"])];
        let slot = key_hash_slot(b"{tx}key");
        let mut state = state_with_remote_primary();
        state
            .set_slot_node(slot as usize, &state.myself.name.clone())
            .unwrap();

        state.set_slot_node(slot as usize, REMOTE).unwrap();
        let snapshot = state.topology_snapshot();
        let result =
            validate_transaction_route(&queued, context(&registry, &snapshot, &|_, _| false));

        assert_eq!(
            result,
            Err(ClusterRouteDecision::Moved {
                slot,
                endpoint: crate::engine::distributed::cluster::AdvertisedEndpoint {
                    host: "192.0.2.1".to_string(),
                    port: 6380,
                },
            })
        );
    }

    #[test]
    fn exec_recomputes_keyless_redirect_and_readonly_after_demotion() {
        let registry = registry();
        let mut state = state_with_remote_primary();
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = REMOTE.to_string();
        state.mark_topology_changed();
        let snapshot = state.topology_snapshot();
        let exists = |_, _: &[u8]| false;

        let mut redirect = context(&registry, &snapshot, &exists);
        redirect.redirect_keyless = true;
        assert_eq!(
            validate_transaction_route(&[args(&[b"DBSIZE"])], redirect),
            Err(ClusterRouteDecision::Redirect {
                endpoint: crate::engine::distributed::cluster::AdvertisedEndpoint {
                    host: "192.0.2.1".to_string(),
                    port: 6380,
                },
            })
        );

        assert_eq!(
            validate_transaction_route(
                &[args(&[b"FLUSHDB"])],
                context(&registry, &snapshot, &exists),
            ),
            Err(ClusterRouteDecision::ReadOnly)
        );
    }

    #[test]
    fn exec_allows_same_slot_reads_on_readonly_replica() {
        let registry = registry();
        let queued = vec![args(&[b"GET", b"{same}one"]), args(&[b"GET", b"{same}two"])];
        let slot = key_hash_slot(b"{same}one");
        let mut state = state_with_remote_primary();
        state.nodes.get_mut(REMOTE).unwrap().slots[slot as usize] = true;
        state.nodes.get_mut(REMOTE).unwrap().numslots = 1;
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = REMOTE.to_string();
        state.mark_topology_changed();
        let snapshot = state.topology_snapshot();
        let exists = |_, _: &[u8]| false;
        let mut route = context(&registry, &snapshot, &exists);
        route.client_readonly = true;

        assert_eq!(validate_transaction_route(&queued, route), Ok(Some(slot)));
    }

    #[test]
    fn exec_allows_ssubscribe_on_replica_serving_its_primary_slot() {
        let registry = registry();
        let channel = b"{shard}channel";
        let slot = key_hash_slot(channel);
        let mut state = state_with_remote_primary();
        state.nodes.get_mut(REMOTE).unwrap().slots[slot as usize] = true;
        state.nodes.get_mut(REMOTE).unwrap().numslots = 1;
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = REMOTE.to_string();
        state.mark_topology_changed();
        let snapshot = state.topology_snapshot();

        assert_eq!(
            validate_transaction_route(
                &[args(&[b"SSUBSCRIBE", channel])],
                context(&registry, &snapshot, &|_, _| false),
            ),
            Ok(Some(slot))
        );
    }

    #[test]
    fn exec_rejects_cross_slot_before_any_handler_runs() {
        let registry = registry();
        let state = state_with_remote_primary();
        let snapshot = state.topology_snapshot();
        let queued = vec![
            args(&[b"GET", b"{one}key"]),
            args(&[b"SET", b"{two}key", b"value"]),
        ];

        assert_eq!(
            validate_transaction_route(&queued, context(&registry, &snapshot, &|_, _| false),),
            Err(ClusterRouteDecision::CrossSlot)
        );
    }

    #[test]
    fn keyless_admin_command_does_not_bypass_keyed_transaction_route() {
        let registry = registry();
        let key = b"{tx}key";
        let slot = key_hash_slot(key);
        let mut state = state_with_remote_primary();
        state.nodes.get_mut(REMOTE).unwrap().slots[slot as usize] = true;
        state.nodes.get_mut(REMOTE).unwrap().numslots = 1;
        state.mark_topology_changed();
        let snapshot = state.topology_snapshot();

        assert_eq!(
            validate_transaction_route(
                &[args(&[b"INFO"]), args(&[b"GET", key])],
                context(&registry, &snapshot, &|_, _| false),
            ),
            Err(ClusterRouteDecision::Moved {
                slot,
                endpoint: crate::engine::distributed::cluster::AdvertisedEndpoint {
                    host: "192.0.2.1".to_string(),
                    port: 6380,
                },
            })
        );
    }

    #[test]
    fn exec_routes_spublish_channel_with_other_transaction_keys() {
        let registry = registry();
        let state = state_with_remote_primary();
        let snapshot = state.topology_snapshot();
        let exists = |_, _: &[u8]| false;

        assert_eq!(
            validate_transaction_route(
                &[
                    args(&[b"SPUBLISH", b"foo", b"payload"]),
                    args(&[b"GET", b"foo"]),
                ],
                context(&registry, &snapshot, &exists),
            ),
            Err(ClusterRouteDecision::ClusterDown)
        );
        assert_eq!(
            validate_transaction_route(
                &[
                    args(&[b"SPUBLISH", b"ch1", b"payload"]),
                    args(&[b"GET", b"foo"]),
                ],
                context(&registry, &snapshot, &exists),
            ),
            Err(ClusterRouteDecision::CrossSlot)
        );
    }
}
