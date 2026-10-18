//! Cluster command handlers.
//! These handlers use the engine-level ClusterState via ServerContext.
//! Phase 0: single-node cluster mode.

use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[path = "links.rs"]
mod links;
#[path = "shards.rs"]
mod shards;
#[path = "topology_reply.rs"]
mod topology_reply;

use crate::engine::distributed::cluster::state::CLUSTER_SLOTS;

#[path = "flush_slot.rs"]
mod flush_slot;
#[path = "help.rs"]
mod help;
#[path = "keyslot.rs"]
mod keyslot;
#[path = "migration/mod.rs"]
mod migration;
#[path = "persistence.rs"]
mod persistence;
pub(super) const CLUSTER_SLOTS_I64: i64 = CLUSTER_SLOTS as i64;

/// Helper to get a mutable reference to the cluster state, or return an error if not enabled.
pub(super) fn with_cluster_state(
    ctx: &mut ServerContext,
) -> Result<&mut crate::engine::distributed::cluster::ClusterState, Value> {
    ctx.cluster_state_mut()
        .as_deref_mut()
        .ok_or_else(|| Value::error("ERR This instance has cluster support disabled"))
}

#[command("CLUSTER")]
pub fn cluster(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("cluster");
    }
    let subcmd = match std::str::from_utf8(&args.args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return CommandError::unknown(&args.args[0]),
    };

    match subcmd.as_str() {
        "ADDSLOTS" => super::slots::cluster_addslots(&args.args[1..], ctx),
        "ADDSLOTSRANGE" => super::slots::cluster_addslotsrange(&args.args[1..], ctx),
        "NODES" => cluster_nodes(conn, ctx),
        "INFO" => cluster_info(ctx),
        "MYID" => cluster_myid(ctx),
        "KEYSLOT" => cluster_keyslot(&args.args[1..]),
        "COUNTKEYSINSLOT" => keyslot::cluster_countkeysinslot(&args.args[1..], conn, ctx),
        "GETKEYSINSLOT" => keyslot::cluster_getkeysinslot(&args.args[1..], conn, ctx),
        "SLOTS" => cluster_slots(&args.args[1..], conn, ctx),
        "SET-CONFIG-EPOCH" => cluster_set_config_epoch(&args.args[1..], ctx),
        "MEET" => cluster_meet(&args.args[1..], ctx),
        "RESET" => cluster_reset(&args.args[1..], ctx),
        "REPLICATE" => cluster_replicate(&args.args[1..], ctx),
        "FAILOVER" => super::failover::cluster_failover(&args.args[1..], false, ctx),
        "SAVECONFIG" => persistence::cluster_saveconfig(conn, ctx),
        "FORGET" => cluster_forget(&args.args[1..], ctx),
        "BUMPEPOCH" => cluster_bumpepoch(conn, ctx),
        "DELSLOTS" => super::slots::cluster_delslots(&args.args[1..], ctx),
        "DELSLOTSRANGE" => super::slots::cluster_delslotsrange(&args.args[1..], ctx),
        "HELP" => help::cluster_help(),
        "REPLICAS" | "SLAVES" => super::replicas::cluster_replicas(&args.args[1..], ctx),
        "SHARDS" => cluster_shards(conn, ctx),
        "LINKS" => links::cluster_links(ctx),
        "MYSHARDID" => cluster_myshardid(ctx),
        "COUNT-FAILURE-REPORTS" => Value::integer(0),
        "FLUSHSLOT" => flush_slot::cluster_flushslot(&args.args[1..], conn, ctx),
        "FLUSHSLOTS" => cluster_flushslots(ctx),
        "SETSLOT" => super::slots::cluster_setslot(&args.args[1..], conn.import_source(), ctx),
        "SYNCSLOTS" => migration::cluster_syncslots(&args.args[1..], conn, ctx),
        "GETSLOTMIGRATIONS" => migration::cluster_getslotmigrations(&args.args[1..], ctx),
        "MIGRATESLOTS" => migration::cluster_migrateslots(&args.args[1..], ctx),
        "CANCELSLOTMIGRATIONS" => migration::cluster_cancelslotmigrations(&args.args[1..], ctx),
        "SLOT-STATS" => super::slot_stats::cluster_slot_stats(&args.args[1..], ctx),
        _ => Value::error(format!(
            "ERR unknown subcommand '{}'. Try CLUSTER HELP.",
            subcmd.to_ascii_lowercase()
        )),
    }
}

fn cluster_nodes(conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    let prefer_ipv6 = conn.peer_addr().is_ipv6();
    let replication_views = &ctx.replication().view.cluster_replica_views;
    let state = match ctx.cluster_state().as_deref() {
        Some(state) => state,
        None => return Value::error("ERR This instance has cluster support disabled"),
    };
    let topology = state.topology_snapshot_with_replication(replication_views);
    Value::bulk(Bytes::from(topology_reply::render_nodes(
        &topology,
        crate::engine::distributed::cluster::ClientTransport::Tcp,
        prefer_ipv6,
    )))
}

fn cluster_info(ctx: &mut ServerContext) -> Value {
    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    Value::bulk(Bytes::from(state.info_str()))
}

fn cluster_myid(ctx: &mut ServerContext) -> Value {
    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    Value::bulk(Bytes::from(state.myself.name.clone()))
}

fn cluster_myshardid(ctx: &mut ServerContext) -> Value {
    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    Value::bulk(Bytes::from(state.myself.shard_id.clone()))
}

fn cluster_keyslot(args: &[Bytes]) -> Value {
    if args.is_empty() {
        return CommandError::wrong_arity("cluster");
    }
    let key = &args[0];
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(key) as i64;
    Value::integer(slot)
}

fn cluster_slots(args: &[Bytes], conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    if !args.is_empty() {
        return CommandError::wrong_arity("cluster|slots");
    }
    let preference = preferred_endpoint(ctx);
    let prefer_ipv6 = conn.peer_addr().is_ipv6();
    let replication_views = &ctx.replication().view.cluster_replica_views;
    let state = match ctx.cluster_state().as_deref() {
        Some(state) => state,
        None => return Value::error("ERR This instance has cluster support disabled"),
    };
    let topology = state.topology_snapshot_with_replication(replication_views);
    let ranges = topology_reply::render_slots(
        &topology,
        preference,
        crate::engine::distributed::cluster::ClientTransport::Tcp,
        prefer_ipv6,
    );
    if ranges.is_empty() {
        Value::Array(vec![])
    } else {
        Value::Array(ranges.into_iter().map(Value::Array).collect())
    }
}

fn cluster_shards(conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    let preference = preferred_endpoint(ctx);
    let prefer_ipv6 = conn.peer_addr().is_ipv6();
    let replication = ctx.replication();
    let local_replication_offset = if replication.is_replica() {
        replication.slave_repl_offset.get()
    } else {
        replication.backlog.reported_offset().get()
    };
    let replication_views = &replication.view.cluster_replica_views;
    ctx.cluster_state()
        .as_deref()
        .ok_or_else(|| Value::error("ERR This instance has cluster support disabled"))
        .map(|state| {
            let topology = state.topology_snapshot_with_local_replication_offset(
                replication_views,
                local_replication_offset,
            );
            Value::Array(shards::cluster_shards(
                &topology,
                preference,
                crate::engine::distributed::cluster::ClientTransport::Tcp,
                prefer_ipv6,
            ))
        })
        .unwrap_or_else(|error| error)
}

fn preferred_endpoint(
    ctx: &ServerContext,
) -> crate::engine::distributed::cluster::PreferredEndpoint {
    match ctx.config().server.cluster_preferred_endpoint_type.as_str() {
        "hostname" => crate::engine::distributed::cluster::PreferredEndpoint::Hostname,
        "unknown-endpoint" => crate::engine::distributed::cluster::PreferredEndpoint::Unknown,
        _ => crate::engine::distributed::cluster::PreferredEndpoint::Ip,
    }
}

fn cluster_set_config_epoch(args: &[Bytes], ctx: &mut ServerContext) -> Value {
    if args.is_empty() {
        return CommandError::wrong_arity("cluster");
    }
    let s = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Invalid epoch"),
    };
    let epoch: u64 = match s.parse() {
        Ok(n) => n,
        Err(_) => return Value::error("ERR Invalid epoch"),
    };
    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    state.advance_current_epoch(epoch);
    state.set_local_config_epoch(epoch);
    state.mark_topology_changed();
    Value::ok()
}

fn cluster_reset(args: &[Bytes], ctx: &mut ServerContext) -> Value {
    let hard = args
        .first()
        .map(|a| {
            std::str::from_utf8(a)
                .unwrap_or("")
                .eq_ignore_ascii_case("hard")
        })
        .unwrap_or(false);
    let has_keys = ctx.dbs().iter_dbs().any(|(_, db)| db.dbsize() != 0);

    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    if state.myself.is_primary() && has_keys {
        return Value::error("ERR CLUSTER RESET can't be called with master nodes containing keys");
    }

    let was_replica = state.myself.is_replica();
    if hard {
        // Hard reset: reinitialize from scratch
        let ip = state.myself.ip.clone();
        let port = state.myself.port;
        let cport = state.myself.cport;
        state.reset_hard(&ip, port, cport);
    } else {
        state.reset_soft();
    }
    if was_replica {
        state.queue_cluster_actions([
            crate::engine::distributed::cluster::action::ClusterAction::DetachReplica,
        ]);
    }
    Value::ok()
}

fn cluster_bumpepoch(conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    match state.bump_config_epoch_without_consensus() {
        Ok(was_bumped) => {
            let reply = Value::SimpleString(Bytes::from(format!(
                "{} {}",
                if was_bumped { "BUMPED" } else { "STILL" },
                state.myself.config_epoch
            )));
            if was_bumped {
                conn.request_cluster_config_save_with_reply(reply);
                conn.set_custom_reply(true);
                Value::Null
            } else {
                reply
            }
        }
        Err(error) => Value::error(format!("ERR {error}")),
    }
}

fn cluster_flushslots(ctx: &mut ServerContext) -> Value {
    if ctx
        .cluster_state()
        .as_deref()
        .is_some_and(|state| state.myself.is_replica())
    {
        return Value::error("ERR Please use FLUSHSLOTS only with primaries.");
    }
    if ctx.dbs().iter_dbs().any(|(_, db)| db.dbsize() != 0) {
        return Value::error("ERR DB must be empty to perform CLUSTER FLUSHSLOTS.");
    }
    let state = match with_cluster_state(ctx) {
        Ok(state) => state,
        Err(reply) => return reply,
    };
    let slots: Vec<usize> = state
        .myself
        .slots
        .iter()
        .enumerate()
        .filter_map(|(slot, is_owned)| is_owned.then_some(slot))
        .collect();
    for slot in slots {
        let _ = state.del_slot(slot);
    }
    Value::ok()
}

fn cluster_meet(args: &[Bytes], ctx: &mut ServerContext) -> Value {
    if args.len() < 2 {
        return CommandError::wrong_arity("cluster");
    }

    let ip = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return Value::error("ERR Invalid IP address"),
    };

    let port_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Invalid port"),
    };
    let port: u16 = match port_str.parse() {
        Ok(p) => p,
        Err(_) => return Value::error("ERR Invalid port"),
    };

    let bus_port = if args.len() >= 3 {
        let bus_port_str = match std::str::from_utf8(&args[2]) {
            Ok(s) => s,
            Err(_) => return Value::error("ERR Invalid bus port"),
        };
        match bus_port_str.parse() {
            Ok(p) => p,
            Err(_) => return Value::error("ERR Invalid bus port"),
        }
    } else {
        port + 10000
    };

    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Queue the MEET request for the event loop to process
    state.queue_meet((ip.clone(), port, bus_port));

    crate::server_log!(
        notice,
        "CLUSTER MEET {}:{} (bus port {}) queued",
        ip,
        port,
        bus_port
    );

    Value::ok()
}

fn cluster_forget(args: &[Bytes], ctx: &mut ServerContext) -> Value {
    if args.is_empty() {
        return CommandError::wrong_arity("cluster");
    }
    let node_id = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Invalid node ID"),
    };
    let blacklist_ttl_ms = ctx
        .config()
        .server
        .cluster_blacklist_ttl
        .saturating_mul(1_000)
        .min(i64::MAX as u64) as i64;

    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };

    if node_id == state.myself.name {
        return Value::error("ERR I can't forget myself, press ENTER to exit...");
    }

    if !state.nodes.contains_key(node_id) {
        if state
            .nodes_black_list
            .get(node_id)
            .is_some_and(|expires_at| *expires_at > crate::foundation::io::sys::time::mstime())
        {
            return Value::ok();
        }
        return Value::error("ERR Unknown node");
    }

    let forgotten = state
        .nodes
        .get(node_id)
        .map(|node| (node.ip().to_string(), node.port(), node.cport()))
        .expect("node existence was checked above");
    state.blacklist_node_until(
        node_id.to_string(),
        crate::foundation::io::sys::time::mstime().saturating_add(blacklist_ttl_ms),
    );

    state.remove_node(node_id);
    state.retain_pending_meets(|(ip, port, bus_port)| {
        !(ip == &forgotten.0 && *port == forgotten.1 && *bus_port == forgotten.2)
    });
    state.retain_pending_connects(|(ip, bus_port, _)| {
        !(ip == &forgotten.0 && *bus_port == forgotten.2)
    });
    let stale_handshakes = state
        .nodes
        .values()
        .filter(|node| {
            node.has_flag(crate::engine::distributed::cluster::node::CLUSTER_NODE_HANDSHAKE)
                && node.ip() == forgotten.0
                && node.port() == forgotten.1
                && node.cport() == forgotten.2
        })
        .map(|node| node.name.clone())
        .collect::<Vec<_>>();
    for handshake_id in stale_handshakes {
        state.remove_handshake_node(&handshake_id);
        state.queue_forget(handshake_id);
    }
    state.mark_topology_changed();

    state.observe_link_close(
        node_id,
        crate::engine::distributed::cluster::ClusterLinkDirection::All,
    );

    // Queue the FORGET for the event loop to clean up bus connections
    state.queue_forget(node_id.to_string());

    crate::server_log!(notice, "Cluster FORGET of node {}", node_id);
    Value::ok()
}

fn cluster_replicate(args: &[Bytes], ctx: &mut ServerContext) -> Value {
    if args.is_empty() || args.len() > 2 {
        return CommandError::wrong_arity("cluster");
    }

    let detach = args.len() == 2
        && args[0].eq_ignore_ascii_case(b"NO")
        && args[1].eq_ignore_ascii_case(b"ONE");
    if args.len() == 2 && !detach {
        return CommandError::syntax_error();
    }
    if !detach && std::str::from_utf8(&args[0]).is_err() {
        return Value::error("ERR Invalid node id".to_string());
    }

    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let result = if detach {
        state.detach_replica().map(|action| {
            state.queue_cluster_actions([action]);
        })
    } else {
        // C's CLUSTER REPLICATE always calls clusterSetPrimary(full_sync_required=1):
        // an empty primary or a replica switching master has no usable history
        // for the new shard, so a full sync is mandatory.
        state.request_replication_switch(
            &args[0],
            crate::engine::distributed::cluster::state::ReplicationSwitchMode::ForceFull,
        )
    };
    match result {
        Ok(()) => Value::ok(),
        Err(error) => Value::error(format!("ERR {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cluster_bumpepoch, cluster_forget, cluster_replicate, cluster_reset, cluster_shards,
    };
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::ClusterState;
    use crate::engine::command::context::CommandConn;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use crate::framework::capabilities::connection::{CommandEffectDrain, Connection};
    use bytes::Bytes;

    fn map_field<'a>(fields: &'a [(Value, Value)], key: &[u8]) -> &'a Value {
        fields
            .iter()
            .find_map(|(candidate, value)| {
                matches!(candidate, Value::Bulk(name) if name.as_ref() == key).then_some(value)
            })
            .expect("expected map field")
    }

    #[test]
    fn cluster_shards_uses_live_local_primary_replication_offset() {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(0).unwrap();
        state.myself.repl_offset = 0;
        *context.cluster_state_mut() = Some(Box::new(state));
        context.replication_mut().backlog.append(b"replicated").unwrap();
        let mut connection = Connection::new_fake();

        let Value::Array(shards) = cluster_shards(&mut connection, &mut context) else {
            panic!("expected shard array");
        };
        let Value::Map(shard) = &shards[0] else {
            panic!("expected shard map");
        };
        let Value::Array(nodes) = map_field(shard, b"nodes") else {
            panic!("expected node array");
        };
        let Value::Map(node) = &nodes[0] else {
            panic!("expected node map");
        };

        assert_eq!(map_field(node, b"replication-offset"), &Value::integer(10));
    }

    #[test]
    fn forget_is_idempotent_while_node_is_blacklisted() {
        let mut context = ServerContext::new(1);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.nodes_black_list.insert(
            node_id.to_string(),
            crate::foundation::io::sys::time::mstime() + 60_000,
        );
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_forget(&[Bytes::from_static(node_id.as_bytes())], &mut context),
            Value::ok()
        );
    }

    #[test]
    fn forget_uses_configured_blacklist_ttl() {
        let mut context = ServerContext::new(1);
        context.config_mut().server.cluster_blacklist_ttl = 2;
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        *context.cluster_state_mut() = Some(Box::new(state));
        let before = crate::foundation::io::sys::time::mstime();

        assert_eq!(
            cluster_forget(&[Bytes::from_static(node_id.as_bytes())], &mut context),
            Value::ok()
        );
        let expires_at = context
            .cluster_state()
            .as_ref()
            .unwrap()
            .nodes_black_list
            .get(node_id)
            .copied()
            .expect("forgotten node should be blacklisted");

        assert!(expires_at >= before + 2_000);
        assert!(expires_at <= before + 2_100);
    }

    #[test]
    fn forget_cancels_pending_connections_to_the_removed_endpoint() {
        let mut context = ServerContext::new(1);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        state
            .pending_connects
            .push(("127.0.0.1".to_string(), 16_380, false));
        state
            .pending_meets
            .push(("127.0.0.1".to_string(), 6380, 16_380));
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_forget(&[Bytes::from_static(node_id.as_bytes())], &mut context),
            Value::ok()
        );
        let state = context.cluster_state().as_ref().unwrap();
        assert!(state.pending_connects.is_empty());
        assert!(state.pending_meets.is_empty());
        assert_eq!(state.pending_forgets, vec![node_id.to_string()]);
    }

    #[test]
    fn bumpepoch_reports_bumped_then_still_and_scans_failed_peers() {
        let mut context = ServerContext::new(1);
        let mut connection = Connection::new_fake();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.current_epoch = 1;
        state.myself.config_epoch = 1;
        let mut peer = ClusterNode::new(
            "ffffffffffffffffffffffffffffffffffffffff".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        peer.config_epoch = 99;
        state.nodes.insert(peer.name.clone(), peer);
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_bumpepoch(&mut connection, &mut context),
            Value::Null
        );
        assert!(connection.custom_reply());
        let mut effects = connection
            .take_command_effects(CommandEffectDrain::Completed)
            .expect("bumpepoch command effects");
        assert_eq!(
            effects.take_cluster_config_save(),
            Some(Value::SimpleString(Bytes::from_static(b"BUMPED 100")))
        );
        assert_eq!(
            cluster_bumpepoch(&mut connection, &mut context),
            Value::SimpleString(Bytes::from_static(b"STILL 100"))
        );
    }

    #[test]
    fn cluster_replicate_defers_role_commit_to_runtime() {
        use crate::engine::distributed::cluster::state::{
            PendingReplicationSwitch, ReplicationSwitchMode,
        };
        use crate::engine::distributed::replication::ReplicationRole;

        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut primary = ClusterNode::new(
            primary_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(primary.name.clone(), primary);
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_replicate(&[Bytes::from_static(primary_id.as_bytes())], &mut context),
            Value::ok()
        );
        let state = context.cluster_state().as_ref().unwrap();
        assert!(state.myself.is_primary());
        assert_eq!(context.replication().role, ReplicationRole::Primary);
        assert_eq!(
            state.pending_replication_switch,
            Some(PendingReplicationSwitch {
                node_id: primary_id.to_string(),
                mode: ReplicationSwitchMode::ForceFull,
            })
        );
    }

    #[test]
    fn cluster_replicate_no_one_defers_detach_to_runtime() {
        use crate::engine::distributed::cluster::action::ClusterAction;
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        use crate::engine::distributed::replication::ReplicationRole;

        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        let old_shard = state.myself.shard_id.clone();
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        state.myself.shard_id = old_shard.clone();
        context.replication_mut().role = ReplicationRole::Replica;
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_replicate(
                &[Bytes::from_static(b"NO"), Bytes::from_static(b"ONE")],
                &mut context,
            ),
            Value::ok()
        );
        let state = context.cluster_state_mut().as_mut().unwrap();
        assert!(state.myself.is_replica());
        assert_eq!(state.myself.shard_id, old_shard);
        assert_eq!(
            state.take_manual_failover_actions(),
            vec![ClusterAction::DetachReplica]
        );
        let _ = state;
        assert_eq!(context.replication().role, ReplicationRole::Replica);
    }

    #[test]
    fn cluster_reset_replica_defers_replication_detach_to_runtime() {
        use crate::engine::distributed::cluster::action::ClusterAction;
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };
        use crate::engine::distributed::replication::ReplicationRole;

        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        context.replication_mut().role = ReplicationRole::Replica;
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(cluster_reset(&[], &mut context), Value::ok());

        let state = context.cluster_state_mut().as_mut().unwrap();
        assert!(state.myself.is_primary());
        assert_eq!(
            state.take_manual_failover_actions(),
            vec![ClusterAction::DetachReplica]
        );
        let _ = state;
        assert_eq!(context.replication().role, ReplicationRole::Replica);
    }
}
