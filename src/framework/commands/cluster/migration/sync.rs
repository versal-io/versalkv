use crate::engine::command::context::CommandConn;
use crate::engine::distributed::cluster::migration::{
    MigrationEvent, MigrationJob, MigrationJobId, MigrationOperation, SlotMigrationConnectionRole,
    SlotRanges, SlotRangesError, TerminalPhase,
};
use crate::engine::distributed::cluster::NodeId;
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use bytes::Bytes;

const CLUSTER_OPERATION_TIMEOUT_MS: i64 = 2_000;

pub(crate) fn cluster_syncslots(
    args: &[Bytes],
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
) -> Value {
    conn.set_skip_propagate(true);
    let Some(subcommand) = args.first() else {
        return crate::framework::CommandError::syntax_error();
    };
    if subcommand.eq_ignore_ascii_case(b"establish") {
        let reply = establish(&args[1..], conn, ctx);
        if !matches!(reply, Value::Error(_)) {
            conn.set_skip_propagate(false);
        } else if !conn.import_source() {
            crate::server_log!(warning, "Rejected slot migration ESTABLISH: {:?}", reply);
        }
        return reply;
    }
    if subcommand.eq_ignore_ascii_case(b"finish") {
        if !conn.import_source()
            && !matches!(
                conn.slot_migration_role(),
                Some(SlotMigrationConnectionRole::Import(_))
            )
        {
            return Value::error(
                "ERR CLUSTER SYNCSLOTS FINISH should only be used by slot migration clients",
            );
        }
        let reply = finish(&args[1..], conn, ctx);
        if !matches!(reply, Value::Error(_)) {
            conn.set_skip_propagate(false);
        }
        return reply;
    }
    if subcommand.eq_ignore_ascii_case(b"capa") {
        conn.suppress_reply();
        return Value::Null;
    }
    let Some(role) = conn.slot_migration_role().cloned() else {
        let name = String::from_utf8_lossy(subcommand).to_ascii_uppercase();
        return match name.as_str() {
            "REQUEST-PAUSE" | "REQUEST-FAILOVER" | "SNAPSHOT-EOF" | "PAUSED"
            | "FAILOVER-GRANTED" | "ACK" => Value::error(format!(
                "ERR CLUSTER SYNCSLOTS {name} should only be used by slot migration clients"
            )),
            _ => crate::framework::CommandError::syntax_error(),
        };
    };
    if args.len() != 1 {
        return fail_protocol_connection(
            conn,
            ctx,
            role.job_id(),
            "Unexpected state machine transition",
        );
    }
    let now = crate::foundation::io::sys::time::unixtime();
    let is_import = matches!(
        role,
        crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(_)
    );
    let event = if subcommand.eq_ignore_ascii_case(b"snapshot-eof") {
        MigrationEvent::SnapshotEof
    } else if subcommand.eq_ignore_ascii_case(b"request-pause") {
        MigrationEvent::PauseRequested
    } else if subcommand.eq_ignore_ascii_case(b"paused") {
        MigrationEvent::Paused
    } else if subcommand.eq_ignore_ascii_case(b"request-failover")
        || subcommand.eq_ignore_ascii_case(b"failover-granted")
    {
        MigrationEvent::FailoverGranted
    } else if subcommand.eq_ignore_ascii_case(b"ack") {
        crate::server_log!(
            debug,
            "Slot migration {} received ACK at {}",
            role.job_id(),
            now
        );
        let result = ctx
            .cluster_state_mut()
            .as_deref_mut()
            .ok_or_else(|| Value::error("ERR This instance has cluster support disabled"))
            .and_then(|state| {
                state
                    .migrations
                    .record_ack(role.job_id(), now)
                    .map(|actions| state.queue_migration_actions(actions))
                    .map_err(|_| Value::error("ERR Unexpected state machine transition"))
            });
        if result.is_err() {
            return fail_protocol_connection(
                conn,
                ctx,
                role.job_id(),
                "Unexpected state machine transition",
            );
        }
        // Reply with an ACK so the export's `last_ack_time` advances. C sends
        // `sendSyncSlotsMessage(job, "ACK")` from `clusterSlotMigrationCron`;
        // this connection has `reply_off` set, so queue the ACK directly.
        if is_import {
            queue_syncslots_ack(conn);
        }
        conn.suppress_reply();
        return Value::Null;
    } else {
        return fail_protocol_connection(
            conn,
            ctx,
            role.job_id(),
            "Unknown SYNCSLOTS subcommand used",
        );
    };
    let transition = ctx
        .cluster_state_mut()
        .as_deref_mut()
        .ok_or_else(|| Value::error("ERR This instance has cluster support disabled"))
        .and_then(|state| {
            state
                .transition_migration(role.job_id(), event, now)
                .map(|actions| state.queue_migration_actions(actions))
                .map_err(|_| Value::error("ERR Unexpected state machine transition"))
        });
    if transition.is_err() {
        return fail_protocol_connection(
            conn,
            ctx,
            role.job_id(),
            "Unexpected state machine transition",
        );
    }
    if !is_import && subcommand.eq_ignore_ascii_case(b"request-failover") {
        // C renews the source's migration pause before granting the target's
        // final failover request, so topology ownership has one operation
        // timeout to propagate before writes can resume. Preserve a longer
        // existing pause instead of shortening it.
        let propagation_deadline =
            crate::foundation::io::sys::time::mstime().saturating_add(CLUSTER_OPERATION_TIMEOUT_MS);
        let pause = ctx.client_pause_mut();
        pause.end_time_ms = pause.end_time_ms.max(propagation_deadline);
    }
    // Reply with an ACK so the export's `last_ack_time` advances. C sends
    // `sendSyncSlotsMessage(job, "ACK")` from `clusterSlotMigrationCron` for
    // every received frame; since this connection has `reply_off` set, the
    // function return value is suppressed, so we queue the ACK frame directly
    // onto the socket's output buffer. We only do this after a successful
    // transition so an invalid transition drops the connection cleanly.
    if is_import {
        queue_syncslots_ack(conn);
    }
    conn.suppress_reply();
    Value::Null
}

fn queue_syncslots_ack(conn: &mut dyn CommandConn) {
    let reply_off = conn.reply_off();
    conn.set_reply_off(false);
    conn.queue_reply(encode_command(&[b"CLUSTER", b"SYNCSLOTS", b"ACK"]));
    conn.set_reply_off(reply_off);
}

fn establish(args: &[Bytes], conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    if conn.slot_migration_role().is_some() {
        let id = conn
            .slot_migration_role()
            .expect("role was checked above")
            .job_id()
            .clone();
        return fail_protocol_connection(conn, ctx, &id, "Unexpected state machine transition");
    }
    let state = match ctx.cluster_state().as_deref() {
        Some(state) => state,
        None => return Value::error("ERR This instance has cluster support disabled"),
    };
    let tracking_only = conn.import_source();
    if !tracking_only && !state.myself.is_primary() {
        return Value::error("ERR Slot migration can only be used on primary nodes.");
    }
    if !tracking_only && state.has_importing_slots() {
        return Value::error("ERR Slots are being manually imported");
    }
    if state.has_migrating_slots() {
        return Value::error("ERR Slots are being manually migrated");
    }
    let fields = match parse_establish(args) {
        Ok(fields) => fields,
        Err(reply) => return reply,
    };
    let source_node = state.nodes.get(fields.source.as_str());
    if !tracking_only && source_node.is_none() {
        return Value::error("ERR Target node does not know the source node");
    }
    if source_node.is_some_and(|source_node| source_node.name == state.myself.name) {
        return Value::error("ERR Source node is target node itself");
    }
    if !tracking_only
        && fields.ranges.iter().any(|range| {
            (range.start..=range.end).any(|slot| {
                state
                    .declared_node_by_slot(slot as usize)
                    .is_none_or(|owner| {
                        Some(owner.name.as_str()) != source_node.map(|node| node.name.as_str())
                    })
            })
        })
    {
        return Value::error("ERR Target node does not agree about current slot ownership");
    }
    if state
        .migrations
        .active_import_overlap(&fields.ranges)
        .is_some()
    {
        return Value::error(
            "ERR Slot is already being imported on the target by a different migration",
        );
    }
    let target = NodeId::parse(state.myself.name.as_bytes())
        .expect("ClusterState local node ID is validated at ingress");
    let now = crate::foundation::io::sys::time::unixtime();
    let tracking_ranges = tracking_only.then(|| fields.ranges.clone());
    let job = if tracking_only {
        MigrationJob::new_tracking_import(
            fields.name.clone(),
            fields.source,
            target,
            fields.ranges.clone(),
            now,
        )
    } else {
        MigrationJob::new(
            fields.name.clone(),
            fields.source,
            target,
            fields.ranges.clone(),
            MigrationOperation::Import,
            now,
        )
    };
    let is_loading = ctx.is_loading();
    let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
        return Value::error("ERR This instance has cluster support disabled");
    };
    let actions = match state.start_migration(job) {
        Ok(actions) => actions,
        Err(_) => return Value::error("ERR Slot migration job already exists"),
    };
    if !is_loading {
        state.queue_migration_actions(actions);
    }
    ctx.create_slot_import(fields.name.clone(), &fields.ranges);
    if let Some(ranges) = tracking_ranges.as_ref() {
        ctx.absorb_serving_slots_into_import(&fields.name, ranges);
    }
    if !tracking_only {
        conn.set_slot_migration_role(Some(SlotMigrationConnectionRole::Import(fields.name)));
        conn.queue_reply(
            crate::foundation::io::protocol::serializer::serialize_to_vec_resp2(&Value::ok()),
        );
        conn.set_custom_reply(true);
        conn.set_reply_off(true);
        return Value::Null;
    }
    Value::ok()
}

struct EstablishFields {
    source: NodeId,
    name: MigrationJobId,
    ranges: SlotRanges,
}

fn parse_establish(args: &[Bytes]) -> Result<EstablishFields, Value> {
    let mut source = None;
    let mut name = None;
    let mut ranges = None;
    let mut index = 0;
    while index < args.len() {
        if args[index].eq_ignore_ascii_case(b"source") {
            if source.is_some() || index + 1 >= args.len() {
                return Err(crate::framework::CommandError::syntax_error());
            }
            source = Some(
                NodeId::parse(&args[index + 1])
                    .map_err(|_| crate::framework::CommandError::syntax_error())?,
            );
            index += 2;
        } else if args[index].eq_ignore_ascii_case(b"name") {
            if name.is_some() || index + 1 >= args.len() {
                return Err(crate::framework::CommandError::syntax_error());
            }
            name = Some(
                MigrationJobId::parse(&args[index + 1])
                    .map_err(|_| crate::framework::CommandError::syntax_error())?,
            );
            index += 2;
        } else if args[index].eq_ignore_ascii_case(b"slotsrange") {
            if ranges.is_some() {
                return Err(crate::framework::CommandError::syntax_error());
            }
            let start = index + 1;
            index = start;
            while index < args.len() && parses_integer(&args[index]) {
                if index + 1 >= args.len() {
                    return Err(Value::error("ERR No end slot for final slot range"));
                }
                index += 2;
            }
            ranges = Some(parse_ranges(&args[start..index])?);
        } else {
            return Err(crate::framework::CommandError::syntax_error());
        }
    }
    match (source, name, ranges) {
        (Some(source), Some(name), Some(ranges)) => Ok(EstablishFields {
            source,
            name,
            ranges,
        }),
        _ => Err(crate::framework::CommandError::syntax_error()),
    }
}

fn finish(args: &[Bytes], _conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    let mut name = None;
    let mut terminal = None;
    let mut message = None;
    let mut index = 0;
    while index + 1 < args.len() {
        if args[index].eq_ignore_ascii_case(b"name") && name.is_none() {
            name = MigrationJobId::parse(&args[index + 1]).ok();
        } else if args[index].eq_ignore_ascii_case(b"state") && terminal.is_none() {
            terminal = if args[index + 1].eq_ignore_ascii_case(b"success") {
                Some(TerminalPhase::Success)
            } else if args[index + 1].eq_ignore_ascii_case(b"failed") {
                Some(TerminalPhase::Failed)
            } else {
                None
            };
        } else if args[index].eq_ignore_ascii_case(b"message") && message.is_none() {
            message = Some(String::from_utf8_lossy(&args[index + 1]).into_owned());
        } else {
            return crate::framework::CommandError::syntax_error();
        }
        index += 2;
    }
    if index != args.len() || name.is_none() || terminal.is_none() {
        return crate::framework::CommandError::syntax_error();
    }
    let is_loading = ctx.is_loading();
    let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
        return Value::error("ERR This instance has cluster support disabled");
    };
    let ranges = state
        .migrations
        .get(name.as_ref().unwrap())
        .map(|job| job.ranges.clone());
    if terminal == Some(TerminalPhase::Success) {
        if let Some(ranges) = ranges.as_ref() {
            if ctx
                .commit_slot_import(name.as_ref().unwrap(), ranges)
                .is_err()
            {
                return Value::error("ERR Failed to commit slot import");
            }
        }
    } else {
        ctx.discard_slot_import(name.as_ref().unwrap());
    }
    let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
        return Value::error("ERR This instance has cluster support disabled");
    };
    match state.finish_migration(
        name.as_ref().unwrap(),
        terminal.unwrap(),
        message,
        crate::foundation::io::sys::time::unixtime(),
    ) {
        Ok(actions) => {
            if !is_loading {
                state.queue_migration_actions(actions);
            }
            Value::ok()
        }
        Err(_) => Value::error("ERR No such slot migration job"),
    }
}

fn fail_protocol_connection(
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
    id: &MigrationJobId,
    message: &str,
) -> Value {
    if let Some(state) = ctx.cluster_state_mut().as_deref_mut() {
        if let Ok(actions) = state.finish_migration(
            id,
            TerminalPhase::Failed,
            Some(message.to_string()),
            crate::foundation::io::sys::time::unixtime(),
        ) {
            state.queue_migration_actions(actions);
        }
    }
    conn.mark_closing();
    conn.suppress_reply();
    Value::Null
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

#[cfg(test)]
mod tests {
    use super::cluster_syncslots;
    use crate::engine::distributed::cluster::migration::{
        ExportPhase, MigrationEvent, MigrationJob, MigrationJobId, MigrationOperation,
        MigrationPhase, SlotMigrationConnectionRole, SlotRanges,
    };
    use crate::engine::distributed::cluster::node::{ClusterNode, CLUSTER_NODE_MYSELF};
    use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    const LOCAL: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SOURCE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const JOB: &str = "cccccccccccccccccccccccccccccccccccccccc";

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect()
    }

    fn context() -> ServerContext {
        let mut ctx = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.name = LOCAL.into();
        state.myself.shard_id = LOCAL.into();
        let mut source = ClusterNode::new(SOURCE.into(), "127.0.0.1".into(), 6380, 16_380, true);
        source.flags &= !CLUSTER_NODE_MYSELF;
        source.slots[100] = true;
        source.slots[101] = true;
        source.numslots = 2;
        state.nodes.insert(SOURCE.into(), source);
        *ctx.cluster_state_mut() = Some(Box::new(state));
        ctx
    }

    #[test]
    fn establish_is_order_agnostic_and_binds_import_role() {
        let mut ctx = context();
        let mut conn = Connection::new_fake();
        let reply = cluster_syncslots(
            &args(&[
                "ESTABLISH",
                "NAME",
                JOB,
                "SLOTSRANGE",
                "100",
                "100",
                "SOURCE",
                SOURCE,
            ]),
            &mut conn,
            &mut ctx,
        );

        assert_eq!(reply, Value::Null);
        assert_eq!(
            conn.output.queue.front().map(Vec::as_slice),
            Some(b"+OK\r\n".as_slice())
        );
        assert_eq!(
            conn.slot_migration_role.as_ref().unwrap().job_id().as_str(),
            JOB
        );
        assert_eq!(
            ctx.cluster_state()
                .as_ref()
                .unwrap()
                .migrations
                .views()
                .len(),
            1
        );
    }

    #[test]
    fn import_transition_ack_bypasses_reply_off_for_protocol_progress() {
        let mut ctx = context();
        let mut conn = Connection::new_fake();
        assert_eq!(
            cluster_syncslots(
                &args(&[
                    "ESTABLISH",
                    "NAME",
                    JOB,
                    "SLOTSRANGE",
                    "100",
                    "100",
                    "SOURCE",
                    SOURCE,
                ]),
                &mut conn,
                &mut ctx,
            ),
            Value::Null
        );
        conn.clear_output_queue();
        assert_eq!(
            cluster_syncslots(&args(&["ACK"]), &mut conn, &mut ctx),
            Value::Null
        );
        conn.clear_output_queue();
        conn.finish_reply_cycle();

        assert_eq!(
            cluster_syncslots(&args(&["SNAPSHOT-EOF"]), &mut conn, &mut ctx),
            Value::Null
        );

        assert_eq!(
            conn.output.queue.front().map(Vec::as_slice),
            Some(
                crate::engine::distributed::replication::protocol::encode_command(&[
                    b"CLUSTER",
                    b"SYNCSLOTS",
                    b"ACK",
                ])
                .as_slice()
            )
        );
    }

    #[test]
    fn tracking_establish_does_not_bind_shared_replication_connection() {
        let mut ctx = context();
        let mut conn = Connection::new_fake();
        conn.import_source = true;

        for (job, slot) in [
            ("cccccccccccccccccccccccccccccccccccccccc", "100"),
            ("dddddddddddddddddddddddddddddddddddddddd", "101"),
        ] {
            assert_eq!(
                cluster_syncslots(
                    &args(&[
                        "ESTABLISH",
                        "SOURCE",
                        SOURCE,
                        "NAME",
                        job,
                        "SLOTSRANGE",
                        slot,
                        slot,
                    ]),
                    &mut conn,
                    &mut ctx,
                ),
                Value::ok()
            );
        }

        assert_eq!(conn.slot_migration_role, None);
        assert_eq!(
            ctx.cluster_state()
                .as_ref()
                .unwrap()
                .migrations
                .views()
                .len(),
            2
        );
    }

    #[test]
    fn tracking_establish_allows_topology_importing_slot_marker() {
        let mut ctx = context();
        ctx.cluster_state_mut()
            .as_deref_mut()
            .unwrap()
            .set_slot_importing(100, SOURCE)
            .unwrap();
        let mut conn = Connection::new_fake();
        conn.import_source = true;

        assert_eq!(
            cluster_syncslots(
                &args(&[
                    "ESTABLISH",
                    "SOURCE",
                    SOURCE,
                    "NAME",
                    JOB,
                    "SLOTSRANGE",
                    "100",
                    "100",
                ]),
                &mut conn,
                &mut ctx,
            ),
            Value::ok()
        );

        assert_eq!(conn.slot_migration_role, None);
        assert!(ctx.has_slot_import(
            &MigrationJobId::parse(JOB.as_bytes()).expect("test job id is valid")
        ));
    }

    #[test]
    fn tracking_establish_absorbs_earlier_replication_writes() {
        let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| {
            crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
        });
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.name = LOCAL.into();
        state.myself.shard_id = LOCAL.into();
        *ctx.cluster_state_mut() = Some(Box::new(state));
        let key = b"{atomic}arrived-before-establish";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        ctx.db_mut(0).unwrap().set(
            key.to_vec(),
            crate::foundation::io::store::Robj::string("value"),
        );
        let mut conn = Connection::new_fake();
        conn.import_source = true;

        assert_eq!(
            cluster_syncslots(
                &args(&[
                    "ESTABLISH",
                    "SOURCE",
                    SOURCE,
                    "NAME",
                    JOB,
                    "SLOTSRANGE",
                    &slot.to_string(),
                    &slot.to_string(),
                ]),
                &mut conn,
                &mut ctx,
            ),
            Value::ok()
        );

        assert_eq!(ctx.dbs().db_size(0), 0);
        assert_eq!(ctx.slot_import_key_count(0, slot as usize), 1);
    }

    #[test]
    fn establish_allows_previous_export_awaiting_terminal_cleanup() {
        let mut ctx = context();
        let mut conn = Connection::new_fake();
        let state = ctx.cluster_state_mut().as_deref_mut().unwrap();
        let export = MigrationJob::new(
            MigrationJobId::parse(b"dddddddddddddddddddddddddddddddddddddddd").unwrap(),
            NodeId::parse(LOCAL.as_bytes()).unwrap(),
            NodeId::parse(SOURCE.as_bytes()).unwrap(),
            SlotRanges::new(vec![SlotRange::new(100, 100).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        state.migrations.start(export).unwrap();
        state.myself.slots[100] = false;
        state.myself.numslots = state.myself.numslots.saturating_sub(1);
        state.nodes.get_mut(SOURCE).unwrap().slots[100] = true;

        assert_eq!(
            cluster_syncslots(
                &args(&[
                    "ESTABLISH",
                    "SOURCE",
                    SOURCE,
                    "NAME",
                    JOB,
                    "SLOTSRANGE",
                    "100",
                    "100",
                ]),
                &mut conn,
                &mut ctx,
            ),
            Value::Null
        );
        assert_eq!(
            conn.slot_migration_role.as_ref().unwrap().job_id().as_str(),
            JOB
        );
    }

    #[test]
    fn normal_clients_cannot_advance_internal_states() {
        for subcommand in [
            "REQUEST-PAUSE",
            "REQUEST-FAILOVER",
            "SNAPSHOT-EOF",
            "PAUSED",
            "FAILOVER-GRANTED",
            "ACK",
        ] {
            let mut ctx = context();
            let mut conn = Connection::new_fake();
            let reply = cluster_syncslots(&args(&[subcommand]), &mut conn, &mut ctx);
            let Value::Error(message) = reply else {
                panic!("expected error");
            };
            assert!(String::from_utf8_lossy(&message)
                .contains("should only be used by slot migration clients"));
        }
    }

    #[test]
    fn request_failover_renews_the_source_pause_for_topology_propagation() {
        let mut ctx = context();
        let id = MigrationJobId::parse(JOB.as_bytes()).unwrap();
        let export = MigrationJob::new(
            id.clone(),
            NodeId::parse(LOCAL.as_bytes()).unwrap(),
            NodeId::parse(SOURCE.as_bytes()).unwrap(),
            SlotRanges::new(vec![SlotRange::new(100, 100).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        let state = ctx.cluster_state_mut().as_deref_mut().unwrap();
        state.migrations.start(export).unwrap();
        for event in [
            MigrationEvent::Connected {
                auth_required: false,
            },
            MigrationEvent::EstablishCommandSent,
            MigrationEvent::Established,
            MigrationEvent::SnapshotStarted,
            MigrationEvent::SnapshotFinished,
            MigrationEvent::PauseRequested,
            MigrationEvent::OutputDrained,
        ] {
            state.migrations.transition(&id, event, 1).unwrap();
        }
        assert_eq!(
            state.migrations.get(&id).unwrap().phase,
            MigrationPhase::Export(ExportPhase::FailoverPaused)
        );
        ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
        ctx.client_pause_mut().end_time_ms = crate::foundation::io::sys::time::mstime() + 1;
        ctx.client_pause_mut().reason = "slot_migration_in_progress";
        let previous_deadline = ctx.client_pause().end_time_ms;
        let mut conn = Connection::new_fake();
        conn.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id));

        assert_eq!(
            cluster_syncslots(&args(&["REQUEST-FAILOVER"]), &mut conn, &mut ctx),
            Value::Null
        );

        assert!(
            ctx.client_pause().end_time_ms >= previous_deadline + 1_900,
            "source pause must cover the C-compatible propagation window"
        );
    }

    #[test]
    fn establish_rejects_missing_duplicate_and_invalid_fields() {
        let cases = [
            vec!["ESTABLISH"],
            vec!["ESTABLISH", "NAME", JOB, "SLOTSRANGE", "100", "100"],
            vec!["ESTABLISH", "SOURCE", SOURCE, "SLOTSRANGE", "100", "100"],
            vec!["ESTABLISH", "SOURCE", SOURCE, "NAME", JOB],
            vec![
                "ESTABLISH",
                "SOURCE",
                "invalid",
                "NAME",
                JOB,
                "SLOTSRANGE",
                "100",
                "100",
            ],
            vec![
                "ESTABLISH",
                "SOURCE",
                SOURCE,
                "NAME",
                "invalid",
                "SLOTSRANGE",
                "100",
                "100",
            ],
            vec![
                "ESTABLISH",
                "SOURCE",
                SOURCE,
                "NAME",
                JOB,
                "SLOTSRANGE",
                "100",
                "100",
                "NAME",
                JOB,
            ],
            vec![
                "ESTABLISH",
                "SOURCE",
                SOURCE,
                "SOURCE",
                SOURCE,
                "NAME",
                JOB,
                "SLOTSRANGE",
                "100",
                "100",
            ],
            vec![
                "ESTABLISH",
                "SOURCE",
                SOURCE,
                "NAME",
                JOB,
                "SLOTSRANGE",
                "100",
                "100",
                "SLOTSRANGE",
                "100",
                "100",
            ],
        ];
        for values in cases {
            let mut ctx = context();
            let mut conn = Connection::new_fake();
            assert_eq!(
                cluster_syncslots(&args(&values), &mut conn, &mut ctx),
                crate::framework::CommandError::syntax_error(),
                "args={values:?}"
            );
        }
    }

    #[test]
    fn duplicate_snapshot_eof_is_fatal_to_bound_connection() {
        let mut ctx = context();
        let mut conn = Connection::new_fake();
        assert_eq!(
            cluster_syncslots(
                &args(&[
                    "ESTABLISH",
                    "SOURCE",
                    SOURCE,
                    "NAME",
                    JOB,
                    "SLOTSRANGE",
                    "100",
                    "100"
                ]),
                &mut conn,
                &mut ctx,
            ),
            Value::Null
        );
        assert_eq!(
            cluster_syncslots(&args(&["ACK"]), &mut conn, &mut ctx),
            Value::Null
        );
        assert_eq!(
            cluster_syncslots(&args(&["SNAPSHOT-EOF"]), &mut conn, &mut ctx),
            Value::Null
        );
        assert_eq!(
            cluster_syncslots(&args(&["SNAPSHOT-EOF"]), &mut conn, &mut ctx),
            Value::Null
        );
        assert!(matches!(
            conn.state,
            crate::engine::command::context::ConnectionState::Closing
        ));
    }
}
