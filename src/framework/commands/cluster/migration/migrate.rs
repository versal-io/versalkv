use crate::engine::distributed::cluster::migration::{
    MigrationError, MigrationJob, MigrationJobId, MigrationOperation, SlotRanges, SlotRangesError,
};
use crate::engine::distributed::cluster::NodeId;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use bytes::Bytes;

pub(crate) fn cluster_migrateslots(
    args: &[Bytes],
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
) -> Value {
    // Scan the original command, including invalid and incomplete AUTH groups.
    for (index, arg) in args.iter().enumerate() {
        if arg.eq_ignore_ascii_case(b"auth") {
            for position in index + 1..(index + 3).min(args.len()) {
                conn.redact_command_argument(position + 2);
            }
        }
    }
    let now = crate::foundation::io::sys::time::unixtime();
    let jobs = match parse_migrate_slots(args, ctx, now) {
        Ok(jobs) => jobs,
        Err(error) => return error,
    };
    let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
        return Value::error("ERR This instance has cluster support disabled");
    };
    match state.start_migrations(jobs) {
        Ok(actions) => {
            state.queue_migration_actions(actions);
            Value::ok()
        }
        Err(MigrationError::ActiveSlotOverlap { slot }) => {
            Value::error(format!("ERR I am already migrating slot {slot}."))
        }
        Err(_) => Value::error("ERR Unable to start slot migration"),
    }
}

fn parse_migrate_slots(
    args: &[Bytes],
    ctx: &ServerContext,
    now: i64,
) -> Result<Vec<MigrationJob>, Value> {
    if args.is_empty() {
        return Err(crate::framework::CommandError::wrong_arity(
            "cluster|migrateslots",
        ));
    }
    let state = ctx
        .cluster_state()
        .as_deref()
        .ok_or_else(|| Value::error("ERR This instance has cluster support disabled"))?;
    validate_source_state(state)?;
    let source = NodeId::parse(state.myself.name.as_bytes())
        .map_err(|_| Value::error("ERR Invalid local node name"))?;
    let mut jobs = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if !args[index].eq_ignore_ascii_case(b"slotsrange") {
            return Err(crate::framework::CommandError::syntax_error());
        }
        index += 1;
        if index == args.len() {
            return Err(crate::framework::CommandError::wrong_arity(
                "cluster|migrateslots",
            ));
        }
        let range_start = index;
        while index < args.len() && parses_integer(&args[index]) {
            if index + 1 >= args.len() {
                return Err(Value::error("ERR No end slot for final slot range"));
            }
            index += 2;
        }
        let ranges = parse_ranges(&args[range_start..index])?;
        validate_range_owner(state, &ranges)?;
        if let Some(slot) = state.migrations.active_overlap(&ranges) {
            return Err(Value::error(format!(
                "ERR I am already migrating slot {slot}."
            )));
        }
        if jobs
            .iter()
            .any(|job: &MigrationJob| job.ranges.overlaps(&ranges).is_some())
        {
            return Err(Value::error("ERR Slot ranges in migrations overlap"));
        }
        if index >= args.len() || !args[index].eq_ignore_ascii_case(b"node") {
            return Err(crate::framework::CommandError::syntax_error());
        }
        let Some(target_bytes) = args.get(index + 1) else {
            return Err(crate::framework::CommandError::syntax_error());
        };
        if target_bytes.len() != 40 {
            return Err(Value::error(format!(
                "ERR Invalid node name: {}",
                String::from_utf8_lossy(target_bytes)
            )));
        }
        let target = NodeId::parse(target_bytes).map_err(|_| {
            Value::error(format!(
                "ERR Invalid node name: {}",
                String::from_utf8_lossy(target_bytes)
            ))
        })?;
        if target.as_str() == state.myself.name {
            return Err(Value::error("ERR Target node can not be this node."));
        }
        if !state.nodes.contains_key(target.as_str()) {
            return Err(Value::error(format!(
                "ERR Unknown node name: {}",
                target.as_str()
            )));
        }
        let mut job = MigrationJob::new(
            MigrationJobId::generate(),
            source.clone(),
            target,
            ranges,
            MigrationOperation::Export,
            now,
        );
        index += 2;
        if args.get(index).is_some_and(|arg| arg.eq_ignore_ascii_case(b"auth")) {
            if index + 2 >= args.len() {
                return Err(crate::framework::CommandError::syntax_error());
            }
            job.set_auth(&args[index + 1], &args[index + 2]);
            index += 3;
        }
        jobs.push(job);
    }
    Ok(jobs)
}

fn validate_source_state(
    state: &crate::engine::distributed::cluster::ClusterState,
) -> Result<(), Value> {
    if !state.myself.is_primary() {
        return Err(Value::error(
            "ERR Slot migration can only be used on primary nodes.",
        ));
    }
    if state.has_importing_slots() {
        return Err(Value::error("ERR Slots are being manually imported"));
    }
    if state.has_migrating_slots() {
        return Err(Value::error("ERR Slots are being manually migrated"));
    }
    Ok(())
}

fn parses_integer(value: &[u8]) -> bool {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .is_some()
}

fn parse_ranges(args: &[Bytes]) -> Result<SlotRanges, Value> {
    SlotRanges::parse_pairs(args).map_err(|error| match error {
        SlotRangesError::Empty => Value::error("ERR No slot ranges specified"),
        SlotRangesError::MissingEnd => Value::error("ERR No end slot for final slot range"),
        SlotRangesError::InvalidSlot => Value::error("ERR Invalid or out of range slot"),
        SlotRangesError::Reversed { start, end } => Value::error(format!(
            "ERR Start slot number {start} is greater than end slot number {end}."
        )),
        SlotRangesError::Overlap { current, previous } => Value::error(format!(
            "ERR Slot range {current} overlaps with previous range {previous}."
        )),
    })
}

fn validate_range_owner(
    state: &crate::engine::distributed::cluster::ClusterState,
    ranges: &SlotRanges,
) -> Result<(), Value> {
    let mut owner = None::<&str>;
    for range in ranges.iter() {
        for slot in range.start..=range.end {
            let Some(slot_owner) = state.bound_node_by_slot(slot as usize) else {
                return Err(Value::error(format!("ERR Slot {slot} has no node served.")));
            };
            if owner.is_some_and(|owner| owner != slot_owner.name) {
                return Err(Value::error("ERR Requested slots span multiple shards"));
            }
            owner = Some(&slot_owner.name);
        }
    }
    if owner != Some(state.myself.name.as_str()) {
        return Err(Value::error("ERR Slots are not served by this node."));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{cluster_migrateslots, parse_migrate_slots};
    use crate::engine::command::context::CommandConn;
    use crate::framework::capabilities::connection::Connection;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::ClusterState;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    const LOCAL: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TARGET: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const OTHER: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const UNKNOWN: &str = "dddddddddddddddddddddddddddddddddddddddd";

    fn bytes(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect()
    }

    fn error_text(value: Value) -> String {
        let Value::Error(message) = value else {
            panic!("expected error reply");
        };
        String::from_utf8(message.to_vec()).unwrap()
    }

    fn context() -> ServerContext {
        let mut ctx = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.name = LOCAL.to_string();
        state.myself.shard_id = LOCAL.to_string();
        for slot in 0..=10 {
            state.add_slot(slot).unwrap();
        }
        for (id, start, end) in [(TARGET, 11, 20), (OTHER, 21, 30)] {
            let mut node = ClusterNode::new(id.to_string(), "127.0.0.1".into(), 6380, 16_380, true);
            node.flags &= !CLUSTER_NODE_MYSELF;
            for slot in start..=end {
                node.slots[slot] = true;
                node.numslots += 1;
            }
            state.nodes.insert(id.to_string(), node);
        }
        *ctx.cluster_state_mut() = Some(Box::new(state));
        ctx
    }

    #[test]
    fn grammar_errors_match_upstream_contract() {
        let ctx = context();
        let cases = [
            (vec![], "wrong number of arguments"),
            (vec!["INVALID", "0", "1"], "syntax error"),
            (vec!["SLOTSRANGE"], "wrong number of arguments"),
            (vec!["SLOTSRANGE", "0"], "No end slot for final slot range"),
            (
                vec!["SLOTSRANGE", "16385", "16388"],
                "Invalid or out of range slot",
            ),
            (
                vec!["SLOTSRANGE", "16380", "16388"],
                "Invalid or out of range slot",
            ),
            (vec!["SLOTSRANGE", "a", "0"], "No slot ranges specified"),
            (vec!["SLOTSRANGE", "0", "a"], "Invalid or out of range slot"),
            (
                vec!["SLOTSRANGE", "1", "0"],
                "Start slot number 1 is greater than end slot number 0",
            ),
            (
                vec!["SLOTSRANGE", "0", "20"],
                "Requested slots span multiple shards",
            ),
            (
                vec!["SLOTSRANGE", "0", "5", "3", "6"],
                "Slot range 3-6 overlaps with previous range 0-5",
            ),
            (
                vec!["SLOTSRANGE", "3", "6", "0", "5"],
                "Slot range 0-5 overlaps with previous range 3-6",
            ),
            (vec!["SLOTSRANGE", "0", "0"], "syntax error"),
            (vec!["SLOTSRANGE", "0", "0", "NODE"], "syntax error"),
            (
                vec!["SLOTSRANGE", "0", "0", "NODE", "blah"],
                "Invalid node name",
            ),
            (
                vec!["SLOTSRANGE", "0", "0", "NODE", UNKNOWN],
                "Unknown node name",
            ),
        ];

        for (args, expected) in cases {
            let actual = parse_migrate_slots(&bytes(&args), &ctx, 100).unwrap_err();
            assert!(error_text(actual).contains(expected), "args={args:?}");
        }
    }

    #[test]
    fn rejects_cross_request_overlap_without_creating_jobs() {
        let ctx = context();
        for args in [
            vec![
                "SLOTSRANGE",
                "1",
                "1",
                "NODE",
                TARGET,
                "SLOTSRANGE",
                "0",
                "2",
                "NODE",
                OTHER,
            ],
            vec![
                "SLOTSRANGE",
                "0",
                "0",
                "2",
                "2",
                "NODE",
                TARGET,
                "SLOTSRANGE",
                "2",
                "2",
                "NODE",
                OTHER,
            ],
        ] {
            let actual = parse_migrate_slots(&bytes(&args), &ctx, 100).unwrap_err();
            assert!(error_text(actual).contains("Slot ranges in migrations overlap"));
        }
        assert!(ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrations
            .views()
            .is_empty());
    }

    #[test]
    fn validates_role_owner_target_and_manual_state() {
        let mut ctx = context();
        ctx.cluster_state_mut().as_mut().unwrap().myself.flags =
            CLUSTER_NODE_MYSELF | CLUSTER_NODE_REPLICA;
        let error =
            parse_migrate_slots(&bytes(&["SLOTSRANGE", "0", "0", "NODE", TARGET]), &ctx, 100)
                .unwrap_err();
        assert!(error_text(error).contains("Slot migration can only be used on primary nodes"));

        ctx.cluster_state_mut().as_mut().unwrap().myself.flags =
            CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY;
        let error = parse_migrate_slots(
            &bytes(&["SLOTSRANGE", "11", "11", "NODE", TARGET]),
            &ctx,
            100,
        )
        .unwrap_err();
        assert!(error_text(error).contains("Slots are not served by this node"));

        let error =
            parse_migrate_slots(&bytes(&["SLOTSRANGE", "0", "0", "NODE", LOCAL]), &ctx, 100)
                .unwrap_err();
        assert!(error_text(error).contains("Target node can not be this node"));

        ctx.cluster_state_mut()
            .as_mut()
            .unwrap()
            .seed_migrating_slot(0, TARGET);
        let error =
            parse_migrate_slots(&bytes(&["SLOTSRANGE", "1", "1", "NODE", TARGET]), &ctx, 100)
                .unwrap_err();
        assert!(error_text(error).contains("Slots are being manually migrated"));
    }

    #[test]
    fn withdrawn_remote_slot_is_still_bound_for_migration_source_validation() {
        let mut ctx = context();
        let state = ctx.cluster_state_mut().as_deref_mut().unwrap();
        state.set_slot_node(0, TARGET).unwrap();
        state.set_slot_claim_withdrawn(0, Some(TARGET.to_string()));

        let error =
            parse_migrate_slots(&bytes(&["SLOTSRANGE", "0", "0", "NODE", OTHER]), &ctx, 100)
                .unwrap_err();

        assert!(error_text(error).contains("Slots are not served by this node"));
    }

    #[test]
    fn accepts_multiple_disjoint_jobs_without_mutating_registry() {
        let ctx = context();
        let jobs = parse_migrate_slots(
            &bytes(&[
                "SLOTSRANGE",
                "0",
                "1",
                "NODE",
                TARGET,
                "SLOTSRANGE",
                "2",
                "3",
                "NODE",
                OTHER,
            ]),
            &ctx,
            100,
        )
        .unwrap();

        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].ranges.to_string(), "0-1");
        assert_eq!(jobs[1].ranges.to_string(), "2-3");
        assert!(ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .migrations
            .views()
            .is_empty());
    }

    #[test]
    fn accepts_per_target_auth_with_binary_credentials() {
        let ctx = context();
        let mut args = bytes(&["SLOTSRANGE", "0", "1", "NODE", TARGET, "aUtH"]);
        args.push(Bytes::from_static(b"user\0\xff"));
        args.push(Bytes::from_static(b"password\r\n\0\xfe"));
        args.extend(bytes(&["SLOTSRANGE", "2", "3", "NODE", OTHER]));
        let mut jobs = parse_migrate_slots(&args, &ctx, 100).unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].target.as_str(), TARGET);
        assert_eq!(jobs[1].target.as_str(), OTHER);
        let auth = jobs[0].take_auth().unwrap();
        assert_eq!(auth.username(), b"user\0\xff");
        assert_eq!(auth.password(), b"password\r\n\0\xfe");
        assert!(!jobs[1].has_auth());
        assert!(ctx.cluster_state().as_ref().unwrap().migrations.views().is_empty());
    }

    #[test]
    fn auth_syntax_errors_do_not_publish_any_jobs() {
        let ctx = context();
        for suffix in [
            vec!["AUTH"],
            vec!["AUTH", "onlyuser"],
            vec!["AUTH", "user", "password", "AUTH", "user", "password"],
            vec!["REPLACE"],
        ] {
            let mut args = bytes(&[
                "SLOTSRANGE", "0", "0", "NODE", TARGET,
                "SLOTSRANGE", "1", "1", "NODE", OTHER,
            ]);
            args.extend(bytes(&suffix));
            assert_eq!(
                parse_migrate_slots(&args, &ctx, 100).unwrap_err(),
                crate::framework::CommandError::syntax_error()
            );
            assert!(ctx.cluster_state().as_ref().unwrap().migrations.views().is_empty());
        }
    }

    #[test]
    fn handler_redacts_all_auth_arguments_even_before_source_validation() {
        for should_disable_cluster in [false, true] {
            let mut ctx = context();
            if should_disable_cluster {
                *ctx.cluster_state_mut() = None;
            }
            let mut conn = Connection::new_fake();
            let args = bytes(&[
                "SLOTSRANGE", "0", "0", "NODE", "invalid",
                "AUTH", "user", "password", "AUTH", "onlyuser",
            ]);
            let reply = cluster_migrateslots(&args, &mut conn, &mut ctx);
            assert_eq!(
                reply,
                if should_disable_cluster {
                    Value::error("ERR This instance has cluster support disabled")
                } else {
                    Value::error("ERR Invalid node name: invalid")
                }
            );
            for position in [8, 9, 11] {
                assert!(conn.is_redacted_command_argument(position));
            }
            assert!(!conn.is_redacted_command_argument(7));
            assert_eq!(args[6].as_ref(), b"user");
            assert_eq!(args[7].as_ref(), b"password");
        }
    }

    #[test]
    fn handler_validates_every_group_before_publishing_or_connecting() {
        let mut ctx = context();
        let mut conn = Connection::new_fake();
        let invalid = bytes(&[
            "SLOTSRANGE", "0", "0", "NODE", TARGET, "AUTH", "user", "password",
            "SLOTSRANGE", "1", "1", "NODE", OTHER, "AUTH", "onlyuser",
        ]);
        assert_eq!(
            cluster_migrateslots(&invalid, &mut conn, &mut ctx),
            crate::framework::CommandError::syntax_error()
        );
        let state = ctx.cluster_state_mut().as_deref_mut().unwrap();
        assert!(state.migrations.views().is_empty());
        assert!(state.take_migration_actions().is_empty());
        let valid = bytes(&[
            "SLOTSRANGE", "0", "0", "NODE", TARGET, "AUTH", "user", "password",
            "SLOTSRANGE", "1", "1", "NODE", OTHER, "AUTH", "", "",
        ]);
        assert_eq!(cluster_migrateslots(&valid, &mut conn, &mut ctx), Value::ok());
        let state = ctx.cluster_state_mut().as_deref_mut().unwrap();
        assert_eq!(state.migrations.views().len(), 2);
        assert_eq!(state.take_migration_actions().len(), 2);
    }
}
