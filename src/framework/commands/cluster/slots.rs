//! CLUSTER slot-assignment subcommands: ADDSLOTS / DELSLOTS /
//! ADDSLOTSRANGE / DELSLOTSRANGE / SETSLOT.
//! Semantics mirror C `cluster_legacy.c`: parse every slot first, validate the
//! whole batch against a scratch bitmap, and only mutate state once all
//! assignments are known to be valid (all-or-nothing).

use super::cluster::{with_cluster_state, CLUSTER_SLOTS_I64};
use crate::engine::distributed::cluster::state::CLUSTER_SLOTS;
use crate::engine::distributed::cluster::ClusterState;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandError;
use bytes::Bytes;

const CLUSTER_OPERATION_TIMEOUT_MS: u64 = 2_000;

#[derive(Clone)]
enum SetSlotAction {
    Migrating(String),
    Importing(String),
    Stable,
    Node(String),
}

#[derive(Clone)]
pub(crate) struct PreparedSetSlot {
    slot: usize,
    action: SetSlotAction,
    pub(crate) timeout_ms: u64,
}

pub(crate) fn prepare_cluster_setslot(
    args: &[Bytes],
    is_replication: bool,
    ctx: &ServerContext,
) -> Result<PreparedSetSlot, Value> {
    if args.len() < 2 {
        return Err(CommandError::wrong_arity("cluster"));
    }
    let slot = parse_slot(&args[0])?;
    let action = std::str::from_utf8(&args[1])
        .ok()
        .map(str::to_ascii_uppercase)
        .ok_or_else(|| Value::error("ERR Invalid SETSLOT action"))?;
    let action = match action.as_str() {
        "MIGRATING" if args.len() >= 3 => SetSlotAction::Migrating(
            std::str::from_utf8(&args[2])
                .map_err(|_| Value::error("ERR Invalid node id"))?
                .to_string(),
        ),
        "IMPORTING" if args.len() >= 3 => SetSlotAction::Importing(
            std::str::from_utf8(&args[2])
                .map_err(|_| Value::error("ERR Invalid node id"))?
                .to_string(),
        ),
        "NODE" if args.len() >= 3 => SetSlotAction::Node(
            std::str::from_utf8(&args[2])
                .map_err(|_| Value::error("ERR Invalid node id"))?
                .to_string(),
        ),
        "STABLE" => SetSlotAction::Stable,
        _ => {
            return Err(Value::error(
                "ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP",
            ));
        }
    };
    let option_start = if matches!(action, SetSlotAction::Stable) {
        2
    } else {
        3
    };
    let mut timeout_ms = CLUSTER_OPERATION_TIMEOUT_MS;
    let mut index = option_start;
    while index < args.len() {
        if !args[index].eq_ignore_ascii_case(b"timeout") {
            index += 1;
            continue;
        }
        let Some(value) = args.get(index + 1) else {
            return Err(Value::error("ERR Missing timeout value"));
        };
        let text = std::str::from_utf8(value)
            .map_err(|_| Value::error("ERR timeout is not an integer or out of range"))?;
        let parsed = text
            .parse::<i64>()
            .map_err(|_| Value::error("ERR timeout is not an integer or out of range"))?;
        if parsed < 0 {
            return Err(Value::error("ERR timeout is negative"));
        }
        timeout_ms = parsed as u64;
        index += 2;
    }

    let state = ctx
        .cluster_state()
        .as_deref()
        .ok_or_else(|| Value::error("ERR This instance has cluster support disabled"))?;
    if state.myself.is_replica() && !is_replication {
        return Err(Value::error("ERR Please use SETSLOT only with masters."));
    }
    if state.migrations.has_active_imports() {
        return Err(Value::error("ERR Slot import in progress."));
    }
    if state.migrations.has_active_exports() {
        return Err(Value::error("ERR Slot export in progress."));
    }
    let validate_node = |node_id: &str, reject_self: bool, unknown: &str| -> Result<(), Value> {
        let node = if node_id == state.myself.name {
            if reject_self {
                return Err(Value::error("ERR Target node is myself"));
            }
            &state.myself
        } else {
            state
                .nodes
                .get(node_id)
                .ok_or_else(|| Value::error(format!("ERR {unknown} {node_id}")))?
        };
        if node.is_replica() {
            return Err(Value::error("ERR Target node is not a master"));
        }
        Ok(())
    };
    match &action {
        SetSlotAction::Migrating(node_id) => {
            if !is_replication && !state.myself.slots[slot] {
                return Err(Value::error(format!(
                    "ERR I'm not the owner of hash slot {slot}"
                )));
            }
            validate_node(node_id, true, "I don't know about node")?;
        }
        SetSlotAction::Importing(node_id) => {
            if state.myself.slots[slot] {
                return Err(Value::error(format!(
                    "ERR I'm already the owner of hash slot {slot}"
                )));
            }
            validate_node(node_id, true, "I don't know about node")?;
        }
        SetSlotAction::Node(node_id) => validate_node(node_id, false, "Unknown node")?,
        SetSlotAction::Stable => {}
    }
    Ok(PreparedSetSlot {
        slot,
        action,
        timeout_ms,
    })
}

/// Parse a single slot argument, matching C `getSlotOrReply`:
/// rejects non-numeric, negative, and out-of-range slots with a uniform error.
fn parse_slot(arg: &Bytes) -> Result<usize, Value> {
    let s = std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<i64>().ok());
    match s {
        Some(n) if (0..CLUSTER_SLOTS_I64).contains(&n) => Ok(n as usize),
        _ => Err(Value::error("ERR Invalid or out of range slot")),
    }
}

/// Two-pass slot assignment check, matching C `checkSlotAssignmentsOrReply`.
/// Records each requested slot into a scratch counter so that duplicates and
/// already-busy / already-unassigned slots are rejected before any mutation.
fn check_slot_assignments(
    state: &ClusterState,
    scratch: &mut [u8],
    del: bool,
    start: usize,
    end: usize,
) -> Result<(), Value> {
    for (offset, count) in scratch[start..=end].iter_mut().enumerate() {
        let slot = start + offset;
        // C `checkSlotAssignmentsOrReply` consults the authoritative slot
        // binding. A withdrawn claim remains bound until a later ownership
        // transition, so DELSLOTS must still be allowed to remove it even
        // though live routing deliberately ignores that claim.
        let is_assigned = if del {
            state.bound_node_by_slot(slot).is_some()
        } else {
            state.declared_node_by_slot(slot).is_some()
        };
        if del && !is_assigned {
            return Err(Value::error(format!(
                "ERR Slot {} is already unassigned",
                slot
            )));
        } else if !del && is_assigned {
            return Err(Value::error(format!("ERR Slot {} is already busy", slot)));
        }
        // C increments the scratch counter and errors when it was already 1,
        // i.e. the slot appears a second time in this same command.
        if *count == 1 {
            return Err(Value::error(format!(
                "ERR Slot {} specified multiple times",
                slot
            )));
        }
        *count += 1;
    }
    Ok(())
}

pub(super) fn cluster_addslots(slot_args: &[Bytes], ctx: &mut ServerContext) -> Value {
    apply_slots(slot_args, ctx, false)
}

pub(super) fn cluster_delslots(slot_args: &[Bytes], ctx: &mut ServerContext) -> Value {
    apply_slots(slot_args, ctx, true)
}

/// Shared implementation of CLUSTER ADDSLOTS / DELSLOTS.
/// Mirrors C `cluster_legacy.c`: parse every slot first, then validate all
/// assignments against a scratch bitmap, and only mutate state once the whole
/// batch is known to be valid (all-or-nothing).
fn apply_slots(slot_args: &[Bytes], ctx: &mut ServerContext, del: bool) -> Value {
    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    if state.myself.is_replica() {
        return Value::error("ERR Please use ADDSLOTS/DELSLOTS only with primaries.");
    }
    // C requires `c->argc >= 3`, i.e. at least one slot argument.
    if slot_args.is_empty() {
        return CommandError::wrong_arity("cluster");
    }
    let mut slots = Vec::with_capacity(slot_args.len());
    for arg in slot_args {
        match parse_slot(arg) {
            Ok(s) => slots.push(s),
            Err(e) => return e,
        }
    }
    let mut scratch = vec![0u8; CLUSTER_SLOTS];
    for &slot in &slots {
        if let Err(e) = check_slot_assignments(state, &mut scratch, del, slot, slot) {
            return e;
        }
    }
    if del {
        for &slot in &slots {
            if let Err(error) = state.del_slot(slot) {
                return Value::error(format!("ERR {error}"));
            }
        }
    } else if let Err(error) = state.add_slots_batch(&slots) {
        return Value::error(format!("ERR {error}"));
    }
    state.finalize_slot_changes();
    for slot in slots {
        ctx.slot_metrics_mut().reset_slot(slot);
    }
    Value::ok()
}

pub(super) fn cluster_addslotsrange(slot_args: &[Bytes], ctx: &mut ServerContext) -> Value {
    apply_slots_range(slot_args, ctx, false, "cluster|addslotsrange")
}

pub(super) fn cluster_delslotsrange(slot_args: &[Bytes], ctx: &mut ServerContext) -> Value {
    apply_slots_range(slot_args, ctx, true, "cluster|delslotsrange")
}

/// Shared implementation of CLUSTER ADDSLOTSRANGE / DELSLOTSRANGE.
/// Mirrors C: requires at least one `<start> <end>` pair and an even number of
/// slot arguments (odd argc → arity error), validates every range against a
/// scratch bitmap, and applies the whole batch only if all ranges are valid.
fn apply_slots_range(
    slot_args: &[Bytes],
    ctx: &mut ServerContext,
    del: bool,
    subcmd: &str,
) -> Value {
    let state = match with_cluster_state(ctx) {
        Ok(s) => s,
        Err(e) => return e,
    };
    if state.myself.is_replica() {
        return Value::error("ERR Please use ADDSLOTSRANGE/DELSLOTSRANGE only with primaries.");
    }
    // C requires `c->argc >= 4` (at least one pair) and an even slot-arg count.
    if slot_args.is_empty() || !slot_args.len().is_multiple_of(2) {
        return CommandError::wrong_arity(subcmd);
    }
    let mut ranges = Vec::with_capacity(slot_args.len() / 2);
    let mut i = 0;
    while i < slot_args.len() {
        let start = match parse_slot(&slot_args[i]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let end = match parse_slot(&slot_args[i + 1]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if start > end {
            return Value::error(format!(
                "ERR start slot number {} is greater than end slot number {}",
                start, end
            ));
        }
        ranges.push((start, end));
        i += 2;
    }
    let mut scratch = vec![0u8; CLUSTER_SLOTS];
    for &(start, end) in &ranges {
        if let Err(e) = check_slot_assignments(state, &mut scratch, del, start, end) {
            return e;
        }
    }
    for &(start, end) in &ranges {
        for slot in start..=end {
            let res = if del {
                state.del_slot(slot)
            } else {
                state.add_slot(slot)
            };
            if let Err(e) = res {
                return Value::error(format!("ERR {}", e));
            }
        }
    }
    state.finalize_slot_changes();
    for (start, end) in ranges {
        for slot in start..=end {
            ctx.slot_metrics_mut().reset_slot(slot);
        }
    }
    Value::ok()
}

pub(super) fn cluster_setslot(
    args: &[Bytes],
    is_replication: bool,
    ctx: &mut ServerContext,
) -> Value {
    let allow_replica_migration = ctx.config().server.cluster_allow_replica_migration;
    let prepared = match prepare_cluster_setslot(args, is_replication, ctx) {
        Ok(prepared) => prepared,
        Err(error) => return error,
    };
    let state = ctx.cluster_state_mut().as_deref_mut().unwrap();
    let mut became_empty_primary = false;
    let slot = prepared.slot;

    let result = match &prepared.action {
        SetSlotAction::Migrating(node_id) => {
            if is_replication {
                state.apply_replicated_slot_migrating(slot, node_id)
            } else {
                state.set_slot_migrating(slot, node_id)
            }
        }
        SetSlotAction::Importing(node_id) => {
            if is_replication {
                state.apply_replicated_slot_importing(slot, node_id)
            } else {
                state.set_slot_importing(slot, node_id)
            }
        }
        SetSlotAction::Stable => {
            if is_replication {
                state.apply_replicated_slot_stable(slot)
            } else {
                state.set_slot_stable(slot)
            }
        }
        SetSlotAction::Node(node_id) => {
            let current_primary_id = if state.myself.is_primary() {
                state.myself.name.clone()
            } else {
                state.myself.replicaof.clone()
            };
            let moved_current_primary_slot = node_id.as_str() != current_primary_id
                && if current_primary_id == state.myself.name {
                    state.myself.slots[slot]
                } else {
                    state
                        .nodes
                        .get(&current_primary_id)
                        .is_some_and(|primary| primary.slots[slot])
                };
            let result = if is_replication {
                state.apply_replicated_slot_node(slot, node_id)
            } else {
                state.set_slot_node(slot, node_id)
            };
            let current_primary_is_empty = if current_primary_id == state.myself.name {
                state.myself.numslots == 0
            } else {
                state
                    .nodes
                    .get(&current_primary_id)
                    .is_some_and(|primary| primary.numslots == 0)
            };
            if result.is_ok()
                && moved_current_primary_slot
                && current_primary_is_empty
                && allow_replica_migration
            {
                if let Err(error) = state.request_replication_switch(
                    node_id.as_bytes(),
                    crate::engine::distributed::cluster::state::ReplicationSwitchMode::ForceFull,
                ) {
                    return Value::error(format!("ERR {error}"));
                }
            } else if result.is_ok()
                && moved_current_primary_slot
                && current_primary_is_empty
                && !allow_replica_migration
                && state.myself.is_primary()
            {
                became_empty_primary = true;
            }
            result
        }
    };

    match result {
        Ok(()) => {
            if matches!(prepared.action, SetSlotAction::Node(_)) {
                ctx.slot_metrics_mut().reset_slot(slot);
            }
            if became_empty_primary {
                crate::server_log!(
                    notice,
                    "My last slot was migrated to another node. I am now an empty primary."
                );
            }
            Value::ok()
        }
        Err(e) => Value::error(format!("ERR {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::migration::{
        MigrationJob, MigrationJobId, MigrationOperation, SlotRanges,
    };
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::NodeId;
    use crate::engine::distributed::replication::ReplicationRole;

    const REMOTE_NODE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const MIGRATION_JOB_ID: &[u8; 40] = b"cccccccccccccccccccccccccccccccccccccccc";

    fn add_active_migration(state: &mut ClusterState, operation: MigrationOperation, slot: u16) {
        let local = NodeId::parse(state.myself.name.as_bytes()).expect("local node ID is valid");
        let remote = NodeId::parse(REMOTE_NODE_ID.as_bytes()).expect("remote node ID is valid");
        let (source, target) = match operation {
            MigrationOperation::Import => (remote, local),
            MigrationOperation::Export => (local, remote),
        };
        let ranges =
            SlotRanges::parse_pairs(&[slot.to_string().as_bytes(), slot.to_string().as_bytes()])
                .expect("test slot range is valid");
        let job = MigrationJob::new(
            MigrationJobId::parse(MIGRATION_JOB_ID).expect("test job ID is valid"),
            source,
            target,
            ranges,
            operation,
            1,
        );
        state
            .migrations
            .start(job)
            .expect("test migration can start");
    }

    fn add_remote_primary(state: &mut ClusterState) {
        let mut remote = ClusterNode::new(
            REMOTE_NODE_ID.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        remote.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(remote.name.clone(), remote);
    }

    #[test]
    fn delslots_accepts_a_withdrawn_but_still_bound_owner() {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        add_remote_primary(&mut state);
        state.set_slot_node(0, REMOTE_NODE_ID).unwrap();
        state.set_slot_claim_withdrawn(0, Some(REMOTE_NODE_ID.to_string()));
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_delslots(&[Bytes::from_static(b"0")], &mut context),
            Value::ok()
        );
        let state = context.cluster_state().as_deref().unwrap();
        assert!(!state.nodes[REMOTE_NODE_ID].slots[0]);
        assert!(state.slot_claim_withdrawn_by(0).is_none());
    }

    #[test]
    fn setslot_timeout_parser_matches_cluster_grammar() {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(609).unwrap();
        let mut target = ClusterNode::new(
            "target".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(target.name.clone(), target);
        *context.cluster_state_mut() = Some(Box::new(state));
        let parse = |parts: &[&'static [u8]]| {
            let args = parts
                .iter()
                .map(|part| Bytes::from_static(part))
                .collect::<Vec<_>>();
            prepare_cluster_setslot(&args, false, &context).map(|request| request.timeout_ms)
        };

        assert_eq!(
            parse(&[b"609".as_slice(), b"MIGRATING", b"target"]),
            Ok(2_000)
        );
        assert_eq!(
            parse(&[
                b"609".as_slice(),
                b"MIGRATING",
                b"target",
                b"TIMEOUT",
                b"3000"
            ]),
            Ok(3_000)
        );
        assert_eq!(
            parse(&[b"609".as_slice(), b"MIGRATING", b"target", b"TIMEOUT"]),
            Err(Value::error("ERR Missing timeout value"))
        );
        assert_eq!(
            parse(&[
                b"609".as_slice(),
                b"MIGRATING",
                b"target",
                b"TIMEOUT",
                b"-1"
            ]),
            Err(Value::error("ERR timeout is negative"))
        );
        assert_eq!(
            parse(&[
                b"609".as_slice(),
                b"MIGRATING",
                b"target",
                b"TIMEOUT",
                b"abc"
            ]),
            Err(Value::error(
                "ERR timeout is not an integer or out of range"
            ))
        );
        assert_eq!(
            parse(&[
                b"609".as_slice(),
                b"TIMEOUT",
                b"100",
                b"MIGRATING",
                b"target"
            ]),
            Err(Value::error(
                "ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP"
            ))
        );
    }

    #[test]
    fn setslot_rejects_active_atomic_import_before_action_validation() {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        add_remote_primary(&mut state);
        add_active_migration(&mut state, MigrationOperation::Import, 16_383);
        *context.cluster_state_mut() = Some(Box::new(state));

        let args = [
            Bytes::from_static(b"0"),
            Bytes::from_static(b"IMPORTING"),
            Bytes::from_static(REMOTE_NODE_ID.as_bytes()),
        ];

        assert_eq!(
            prepare_cluster_setslot(&args, false, &context).map(|_| ()),
            Err(Value::error("ERR Slot import in progress."))
        );
    }

    #[test]
    fn setslot_rejects_active_atomic_export_before_action_validation() {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(0).unwrap();
        add_remote_primary(&mut state);
        add_active_migration(&mut state, MigrationOperation::Export, 16_383);
        *context.cluster_state_mut() = Some(Box::new(state));

        let args = [
            Bytes::from_static(b"0"),
            Bytes::from_static(b"MIGRATING"),
            Bytes::from_static(REMOTE_NODE_ID.as_bytes()),
        ];

        assert_eq!(
            prepare_cluster_setslot(&args, false, &context).map(|_| ()),
            Err(Value::error("ERR Slot export in progress."))
        );
    }

    #[test]
    fn addslots_and_delslots_reset_existing_slot_counters() {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.add_slot(7).unwrap();
        *context.cluster_state_mut() = Some(Box::new(state));
        context.slot_metrics_mut().record(&[7], 9, 11, 13);

        assert_eq!(
            cluster_delslots(&[Bytes::from_static(b"7")], &mut context),
            Value::ok()
        );
        assert_eq!(context.slot_metrics().counters(7).unwrap().cpu_usec, 0);

        context.slot_metrics_mut().record(&[7], 17, 19, 23);
        assert_eq!(
            cluster_addslots(&[Bytes::from_static(b"7")], &mut context),
            Value::ok()
        );
        assert_eq!(context.slot_metrics().counters(7).unwrap().cpu_usec, 0);
    }

    #[test]
    fn addslots_records_one_topology_change_for_the_whole_command() {
        let mut context = ServerContext::new(1);
        let state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let generation = state.topology_generation();
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_addslots(
                &[Bytes::from_static(b"1"), Bytes::from_static(b"2")],
                &mut context,
            ),
            Value::ok()
        );

        let state = context.cluster_state().as_deref().unwrap();
        assert_eq!(state.topology_generation(), generation + 1);
        assert_eq!(state.myself.numslots, 2);
    }

    #[test]
    fn replica_rejects_slot_assignment_before_slot_state_validation() {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };

        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(100).unwrap();
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        *context.cluster_state_mut() = Some(Box::new(state));

        assert_eq!(
            cluster_addslots(&[Bytes::from_static(b"100")], &mut context),
            Value::error("ERR Please use ADDSLOTS/DELSLOTS only with primaries.")
        );
        assert_eq!(
            cluster_delslotsrange(
                &[Bytes::from_static(b"100"), Bytes::from_static(b"200")],
                &mut context,
            ),
            Value::error("ERR Please use ADDSLOTSRANGE/DELSLOTSRANGE only with primaries.",)
        );
    }

    #[test]
    fn moving_last_slot_demotes_source_to_target_replica() {
        const TARGET: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.add_slot(0).unwrap();
        let mut target = ClusterNode::new(
            TARGET.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(target.name.clone(), target);
        *context.cluster_state_mut() = Some(Box::new(state));
        context.slot_metrics_mut().record(&[0], 7, 11, 13);

        let reply = cluster_setslot(
            &[
                Bytes::from_static(b"0"),
                Bytes::from_static(b"NODE"),
                Bytes::from_static(TARGET.as_bytes()),
            ],
            false,
            &mut context,
        );

        assert_eq!(reply, Value::ok());
        let state = context.cluster_state().as_ref().unwrap();
        assert!(state.myself.is_primary());
        assert_eq!(
            state.pending_replication_switch,
            Some(
                crate::engine::distributed::cluster::state::PendingReplicationSwitch {
                    node_id: TARGET.to_string(),
                    mode:
                        crate::engine::distributed::cluster::state::ReplicationSwitchMode::ForceFull,
                }
            )
        );
        assert_eq!(context.replication().role, ReplicationRole::Primary);
        assert_eq!(context.config().replication.master_port, 0);
        assert_eq!(context.slot_metrics().counters(0).unwrap().cpu_usec, 0);
    }

    #[test]
    fn replicated_last_slot_move_reparents_source_replica_to_target() {
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
        };

        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let source_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let target_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = source_id.to_string();

        let mut source = ClusterNode::new(
            source_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        source.flags &= !CLUSTER_NODE_MYSELF;
        source.slots[0] = true;
        source.numslots = 1;
        state.insert_node(source);
        let mut target = ClusterNode::new(
            target_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(target_id.to_string(), target);
        *context.cluster_state_mut() = Some(Box::new(state));

        let reply = cluster_setslot(
            &[
                Bytes::from_static(b"0"),
                Bytes::from_static(b"NODE"),
                Bytes::from_static(target_id.as_bytes()),
            ],
            true,
            &mut context,
        );

        assert_eq!(reply, Value::ok());
        let state = context.cluster_state().as_ref().unwrap();
        assert_eq!(
            state.pending_replication_switch,
            Some(
                crate::engine::distributed::cluster::state::PendingReplicationSwitch {
                    node_id: target_id.to_string(),
                    mode:
                        crate::engine::distributed::cluster::state::ReplicationSwitchMode::ForceFull,
                }
            )
        );
    }

    #[test]
    fn moving_last_slot_keeps_source_primary_when_migration_is_disabled() {
        const TARGET: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut context = ServerContext::new(1);
        context.config_mut().server.cluster_allow_replica_migration = false;
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        state.add_slot(0).unwrap();
        let mut target = ClusterNode::new(
            TARGET.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        target.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(target.name.clone(), target);
        *context.cluster_state_mut() = Some(Box::new(state));

        let reply = cluster_setslot(
            &[
                Bytes::from_static(b"0"),
                Bytes::from_static(b"NODE"),
                Bytes::from_static(TARGET.as_bytes()),
            ],
            false,
            &mut context,
        );

        assert_eq!(reply, Value::ok());
        let state = context.cluster_state().as_ref().unwrap();
        assert!(state.myself.is_primary());
        assert_eq!(state.myself.numslots, 0);
        assert_eq!(context.replication().role, ReplicationRole::Primary);
    }

    #[test]
    fn delslots_clears_failed_remote_owner_outside_alive_coverage() {
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
        const FAILED_OWNER: &str = "cccccccccccccccccccccccccccccccccccccccc";

        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut owner = ClusterNode::new(
            FAILED_OWNER.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        owner.flags &= !CLUSTER_NODE_MYSELF;
        owner.flags |= CLUSTER_NODE_FAIL;
        owner.slots[0] = true;
        owner.numslots = 1;
        state.insert_node(owner);
        state.recompute_state();
        assert!(!state.slots[0]);
        assert!(state.declared_node_by_slot(0).is_some());
        *context.cluster_state_mut() = Some(Box::new(state));

        let reply = cluster_delslots(&[Bytes::from_static(b"0")], &mut context);

        assert_eq!(reply, Value::ok());
        assert!(context
            .cluster_state()
            .as_ref()
            .unwrap()
            .declared_node_by_slot(0)
            .is_none());
    }
}
