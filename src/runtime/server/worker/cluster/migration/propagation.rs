use crate::engine::command::CommandArgs;
use crate::engine::distributed::cluster::migration::{
    MigrationJobId, MigrationJobView, MigrationOperation, MigrationPhase,
};
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::protocol::parser::Parser;
use crate::framework::CommandRegistry;

use super::super::super::ServerWorkerState;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MigrationMutation {
    pub job_id: MigrationJobId,
    pub db_id: u32,
    pub frame: Vec<u8>,
    pub defer_until_snapshot: bool,
}

pub(crate) fn classify_migration_mutations(
    ops: &[PropagationOp],
    jobs: &[MigrationJobView],
    registry: &CommandRegistry,
) -> Vec<MigrationMutation> {
    if !jobs
        .iter()
        .any(|job| job.operation == MigrationOperation::Export)
    {
        return Vec::new();
    }

    let mut mutations = Vec::new();
    for op in ops {
        let Some(commands) = commands_with_slots(&op.bytes, registry) else {
            continue;
        };
        for (frame, slots) in commands {
            for job in jobs
                .iter()
                .filter(|job| job.operation == MigrationOperation::Export)
            {
                if slots.iter().any(|slot| job.ranges.contains(*slot)) {
                    mutations.push(MigrationMutation {
                        job_id: job.id.clone(),
                        db_id: op.db_id,
                        frame: frame.clone(),
                        defer_until_snapshot: matches!(
                            job.phase,
                            MigrationPhase::Export(crate::engine::distributed::cluster::migration::ExportPhase::Snapshotting)
                        ),
                    });
                }
            }
        }
    }
    mutations
}

fn commands_with_slots(
    bytes: &[u8],
    registry: &CommandRegistry,
) -> Option<Vec<(Vec<u8>, Vec<u16>)>> {
    let mut parser = Parser::new();
    parser.feed(bytes);
    let mut commands = Vec::new();
    while parser.has_pending_data() {
        let parts = parser.try_parse().ok()??;
        let args = CommandArgs::from_parts_reusing(parts);
        let Some(spec) = registry.lookup(&args.name) else {
            continue;
        };
        let Some(keys) = crate::runtime::server::worker::command::extract_key_args(spec, &args)
        else {
            continue;
        };
        let slots = keys
            .iter()
            .map(|key| crate::engine::distributed::cluster::node::key_hash_slot(key))
            .collect();
        commands.push((
            crate::runtime::server::worker::cluster::propagation::encode_command_args(&args),
            slots,
        ));
    }
    Some(commands)
}

impl ServerWorkerState {
    pub(crate) fn queue_slot_migration_mutations(&mut self, ops: &[PropagationOp]) {
        let jobs = self
            .ctx
            .cluster_state()
            .as_deref()
            .map(|state| state.migrations.active_export_views())
            .unwrap_or_default();
        let mutations = classify_migration_mutations(ops, &jobs, &self.registry);
        for mutation in mutations {
            let connection_id = self
                .slot_migration_runtime
                .export_connection(&mutation.job_id);
            let frames = self.queue_slot_export_mutation(
                &mutation.job_id,
                mutation.db_id,
                mutation.frame,
                mutation.defer_until_snapshot,
            );
            let Some(connection_id) = connection_id else {
                continue;
            };
            if !self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id))
            {
                continue;
            }
            for frame in frames {
                if !self.queue_slot_migration_output(&mutation.job_id, connection_id, frame) {
                    break;
                }
            }
            // Deferred snapshot mutations are retained outside the connection
            // queue to preserve snapshot-before-tail ordering, but they still
            // belong to the same replica-class output budget.
            if self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id))
            {
                self.enforce_client_output_buffer_limit_for(connection_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::classify_migration_mutations;
    use crate::engine::distributed::cluster::migration::{
        MigrationJob, MigrationJobId, MigrationOperation, MigrationRegistry, SlotRanges,
    };
    use crate::engine::distributed::cluster::{NodeId, SlotRange};
    use crate::engine::distributed::replication::protocol::encode_command;
    use crate::engine::extension::script::PropagationOp;
    use crate::framework::CommandRegistry;

    fn runtime_with_export_phase(
        hard_limit: usize,
        phase: crate::engine::distributed::cluster::migration::ExportPhase,
    ) -> (
        crate::runtime::server::worker::ServerWorkerState,
        MigrationJobId,
        crate::framework::capabilities::io::Token,
    ) {
        use crate::engine::distributed::cluster::migration::{
            MigrationPhase, SlotMigrationConnectionRole,
        };

        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        runtime
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_hard_limit = hard_limit;
        runtime.ctx.config_mut().server.repl_backlog_size = 0;
        let mut state =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        job.phase = MigrationPhase::Export(phase);
        state.migrations.start(job).unwrap();
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
        let token = crate::framework::capabilities::io::Token(79);
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.set_token_for_test(token);
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
        let _connection_token = runtime.connection_registry.insert(connection);
        runtime.slot_migration_runtime.register_export(
            crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
        );
        (runtime, id, token)
    }

    fn runtime_with_established_export(
        hard_limit: usize,
    ) -> (
        crate::runtime::server::worker::ServerWorkerState,
        MigrationJobId,
        crate::framework::capabilities::io::Token,
    ) {
        runtime_with_export_phase(
            hard_limit,
            crate::engine::distributed::cluster::migration::ExportPhase::Streaming,
        )
    }

    #[test]
    fn classifier_routes_exact_frames_only_to_overlapping_exports() {
        let key = b"{atomic}key";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let mut migrations = MigrationRegistry::default();
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        migrations
            .start(MigrationJob::new(
                id.clone(),
                NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
                NodeId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap(),
                SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
                MigrationOperation::Export,
                1,
            ))
            .unwrap();
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let matching = encode_command(&[b"SET", key, b"value"]);
        let ignored = encode_command(&[b"SET", b"other", b"value"]);

        let result = classify_migration_mutations(
            &[
                PropagationOp::both(3, matching.clone()),
                PropagationOp::both(3, ignored),
            ],
            &migrations.active_export_views(),
            &registry,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].job_id, id);
        assert_eq!(result[0].db_id, 3);
        assert_eq!(result[0].frame, matching);
    }

    #[test]
    fn classifier_preserves_matching_commands_from_a_concatenated_batch() {
        let key = b"{atomic}key";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let mut migrations = MigrationRegistry::default();
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        migrations
            .start(MigrationJob::new(
                id,
                NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
                NodeId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap(),
                SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
                MigrationOperation::Export,
                1,
            ))
            .unwrap();
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let first = encode_command(&[b"SET", key, b"one"]);
        let second = encode_command(&[b"SET", key, b"two"]);
        let mut batch = PropagationOp::both(4, first.clone());
        batch.append_frame(second.clone());

        let result =
            classify_migration_mutations(&[batch], &migrations.active_export_views(), &registry);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].frame, first);
        assert_eq!(result[1].frame, second);
        assert!(result.iter().all(|mutation| mutation.db_id == 4));
    }

    #[test]
    fn classifier_skips_unknown_frames_without_discarding_known_neighbors() {
        let key = b"{atomic}key";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let mut migrations = MigrationRegistry::default();
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        migrations
            .start(MigrationJob::new(
                id,
                NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
                NodeId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap(),
                SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
                MigrationOperation::Export,
                1,
            ))
            .unwrap();
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        registry.rename("set", "setalias");
        let first = encode_command(&[b"SETALIAS", key, b"one"]);
        let rewritten = encode_command(&[b"SET", key, b"two", b"KEEPTTL"]);
        let last = encode_command(&[b"SETALIAS", key, b"three"]);
        let mut batch = PropagationOp::both(4, first.clone());
        batch.append_frame(rewritten);
        batch.append_frame(last.clone());

        let result =
            classify_migration_mutations(&[batch], &migrations.active_export_views(), &registry);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].frame, first);
        assert_eq!(result[1].frame, last);
    }

    #[test]
    fn queued_export_mutations_are_counted_as_replication_bytes() {
        let key = b"{atomic}key";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        let mut state =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        state.migrations.start(job).unwrap();
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let peer = runtime.transport.connect_tcp(peer_addr).unwrap();
        let (_target, _) = listener.accept().unwrap();
        runtime.add_connection(
            crate::runtime::server::worker::event_loop::transport::RuntimeConnectedPeer {
                token: peer.token,
                peer_addr,
            },
        );
        let token = peer.token;
        let connection_token = token.0;
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .slot_migration_role = Some(
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                id.clone(),
            ),
        );
        runtime.slot_migration_runtime.register_export(
            crate::framework::distributed::migration::SlotExportLink::new(id, token.0),
        );
        let frame = encode_command(&[b"SET", key, b"value"]);

        runtime.queue_slot_migration_mutations(&[PropagationOp::both(0, frame.clone())]);

        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .replication_bytes,
            encode_command(&[b"SELECT", b"0"]).len() + frame.len()
        );
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .ordinary_bytes,
            0
        );
    }

    #[test]
    fn queued_export_mutation_immediately_enforces_replica_hard_limit() {
        use crate::engine::distributed::cluster::migration::{
            MigrationPhase, SlotMigrationConnectionRole, TerminalPhase,
        };

        let key = b"{atomic}key";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_hard_limit = 32;
        runtime.ctx.config_mut().server.repl_backlog_size = 0;
        let mut state =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        state
            .migrations
            .start(MigrationJob::new(
                id.clone(),
                NodeId::parse(state.myself.name.as_bytes()).unwrap(),
                NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
                SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
                MigrationOperation::Export,
                1,
            ))
            .unwrap();
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
        let token = crate::framework::capabilities::io::Token(77);
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.set_token_for_test(token);
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
        let _connection_token = runtime.connection_registry.insert(connection);
        runtime.slot_migration_runtime.register_export(
            crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
        );
        let frame = encode_command(&[b"SET", key, &[b'x'; 64]]);

        runtime.queue_slot_migration_mutations(&[PropagationOp::both(0, frame)]);

        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(TerminalPhase::Failed)
        );
        assert_eq!(
            runtime
                .ctx
                .stats()
                .client_output_buffer_limit_disconnections,
            1
        );
    }

    #[test]
    fn slot_export_hard_limit_uses_pending_output_memory_not_logical_len() {
        use crate::engine::distributed::cluster::migration::{MigrationPhase, TerminalPhase};

        let (mut runtime, id, token) = runtime_with_established_export(1024);
        let mut frame = Vec::with_capacity(4096);
        frame.extend_from_slice(b"tiny");

        assert!(!runtime.queue_slot_migration_output(&id, token.0, frame));

        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(TerminalPhase::Failed)
        );
        assert_eq!(
            runtime
                .ctx
                .stats()
                .client_output_buffer_limit_disconnections,
            1
        );
    }

    #[test]
    fn slot_export_generic_output_participates_in_client_output_limit() {
        use crate::engine::distributed::cluster::migration::{MigrationPhase, TerminalPhase};

        let (mut runtime, id, token) = runtime_with_established_export(1024);
        let mut frame = Vec::with_capacity(4096);
        frame.extend_from_slice(b"control");

        assert!(!runtime.queue_client_reply(token, frame));

        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(TerminalPhase::Failed)
        );
        assert_eq!(
            runtime
                .ctx
                .stats()
                .client_output_buffer_limit_disconnections,
            1
        );
    }

    #[test]
    fn slot_export_control_output_enforces_limit_before_periodic_ack_flush() {
        use crate::engine::distributed::cluster::migration::{MigrationPhase, TerminalPhase};

        let (mut runtime, id, token) = runtime_with_established_export(1);

        runtime.send_slot_migration_periodic_acks();

        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(TerminalPhase::Failed)
        );
        assert_eq!(
            runtime
                .ctx
                .stats()
                .client_output_buffer_limit_disconnections,
            1
        );
    }

    #[test]
    fn disabled_replica_limits_remain_disabled_for_slot_export_with_nonzero_backlog() {
        let (mut runtime, _id, token) = runtime_with_established_export(0);
        runtime
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_soft_limit = 0;
        runtime
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_soft_limit_seconds = 0;
        runtime.ctx.config_mut().server.repl_backlog_size = 1024;
        let connection_token = token.0;
        runtime.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(vec![b'x'; 2048]);

        assert!(!runtime.enforce_client_output_buffer_limit_for(token.0));

        let connection_token = token.0;
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .soft_limit_reached_at_ms,
            None
        );
        assert_eq!(
            runtime
                .ctx
                .stats()
                .client_output_buffer_limit_disconnections,
            0
        );
    }

    #[test]
    fn pre_client_auth_is_excluded_from_export_reply_and_cob_projections() {
        let (mut runtime, id, token) = runtime_with_export_phase(
            1024,
            crate::engine::distributed::cluster::migration::ExportPhase::SendingAuthCommand,
        );
        runtime.ctx.config_mut().security.primaryauth = Some(vec![b'p'; 4096]);
        runtime
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .unwrap()
            .queue_migration_actions([
                crate::engine::distributed::cluster::migration::MigrationAction::SendAuth(
                    id.clone(),
                ),
            ]);

        runtime.drive_slot_migration_actions();

        let connection_token = token.0;
        assert_eq!(runtime.slot_export_reply_bytes(&id, token.0), 0);
        assert_eq!(runtime.slot_export_output_memory_usage(&id, token.0), 0);
        assert!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output_buffer_allocation_bytes()
                > 1024,
            "the physical allocator projection must retain pre-client AUTH ownership"
        );
        assert!(!runtime.enforce_client_output_buffer_limit_for(token.0));
        assert!(runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            runtime.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .output
            .soft_limit_reached_at_ms,
            None
        );
    }

    #[test]
    fn deferred_snapshot_mutation_enforces_replica_hard_limit() {
        use crate::engine::distributed::cluster::migration::{
            ExportPhase, MigrationPhase, SlotMigrationConnectionRole, TerminalPhase,
        };

        let key = b"{atomic}key";
        let slot = crate::engine::distributed::cluster::node::key_hash_slot(key);
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime
            .ctx
            .config_mut()
            .replication
            .replica_output_buffer_hard_limit = 32;
        runtime.ctx.config_mut().server.repl_backlog_size = 0;
        let mut state =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        job.phase = MigrationPhase::Export(ExportPhase::Snapshotting);
        state.migrations.start(job).unwrap();
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));
        let token = crate::framework::capabilities::io::Token(78);
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.set_token_for_test(token);
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
        let _connection_token = runtime.connection_registry.insert(connection);
        runtime.slot_migration_runtime.register_export(
            crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
        );
        let frame = encode_command(&[b"SET", key, &[b'x'; 64]]);

        runtime.queue_slot_migration_mutations(&[PropagationOp::both(0, frame)]);

        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .phase,
            MigrationPhase::Terminal(TerminalPhase::Failed)
        );
        assert_eq!(
            runtime
                .ctx
                .stats()
                .client_output_buffer_limit_disconnections,
            1
        );
    }
}
