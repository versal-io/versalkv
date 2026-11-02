use super::super::super::ServerWorkerState;
use crate::engine::command::CommandArgs;
use crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole;
use crate::engine::extension::script::PropagationOp;
use crate::runtime::server::worker::distributed::propagation::sequence_propagation;

impl ServerWorkerState {
    pub(crate) fn should_defer_slot_import_write(
        &self,
        connection_token: usize,
        args: &CommandArgs,
    ) -> bool {
        if !self.ctx.persistence().forkless_save_in_progress()
            || !matches!(
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .slot_migration_role,
                Some(SlotMigrationConnectionRole::Import(_))
            )
            || is_syncslots_ack(args)
        {
            return false;
        }
        self.registry
            .lookup(&args.name)
            .is_some_and(|spec| {
                (crate::runtime::server::worker::flows::request::dispatch::subcmd_flags::effective_command_flags_for_call(
                    spec.name.as_str(),
                    spec.flags,
                    args,
                    &self.ctx,
                ) & crate::engine::command::CMD_WRITE)
                    != 0
            })
    }

    pub(crate) fn apply_slot_import_command(
        &mut self,
        connection_token: usize,
        args: CommandArgs,
    ) -> bool {
        let Some(SlotMigrationConnectionRole::Import(id)) = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role
        .clone() else {
            return false;
        };
        if args.name.eq_ignore_ascii_case(b"cluster")
            && args
                .args
                .first()
                .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"syncslots"))
        {
            return false;
        }
        if args.name.eq_ignore_ascii_case(b"reset") {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .mark_closing();
            self.fail_slot_migration(&id, "RESET is not allowed on a slot migration connection");
            return true;
        }
        let command_flags = self.registry.lookup(&args.name).map_or(0, |spec| {
            crate::runtime::server::worker::flows::request::dispatch::subcmd_flags::effective_command_flags_for_call(
                spec.name.as_str(),
                spec.flags,
                &args,
                &self.ctx,
            )
        });
        if command_flags & crate::engine::command::CMD_WRITE != 0 {
            self.ctx.set_suppress_eviction_until_write(false);
        }
        self.propagate_slot_import_evictions();
        let rejects_for_oom = command_flags & crate::engine::command::CMD_DENYOOM != 0
            && self.ctx.maxmemory_state().is_over_limit();
        if rejects_for_oom {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .mark_closing();
            self.fail_slot_migration(&id, "Ran out of memory (OOM) during slot import");
            return true;
        }
        let frame =
            crate::runtime::server::worker::distributed::propagation::encode_command_args(&args);
        let command_name = args.name.clone();
        let is_outer_execution = self.command_execution_depth == 0;
        self.command_execution_depth += 1;
        let (result, execution, selected_db) =
            self.ctx
                .apply_slot_import_with_execution(&id, args, &self.registry);
        self.command_execution_depth -= 1;
        if is_outer_execution
            && self
                .is_priority_connection(crate::framework::capabilities::io::Token(connection_token))
        {
            if let Some(duration_us) = execution.duration_us() {
                self.ctx.stats_mut().record_priority_command(duration_us);
            }
        }
        if let Err(error) = result {
            crate::server_log!(
                warning,
                "Slot import {} failed to apply {}: {:?}",
                id,
                String::from_utf8_lossy(&command_name),
                error
            );
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .mark_closing();
            self.fail_slot_migration(
                &id,
                "Failed to process command during slot migration. Check logs for more information",
            );
        } else {
            if !command_name.eq_ignore_ascii_case(b"select") {
                let sequenced =
                    sequence_propagation(&[PropagationOp::both(selected_db, frame)], -1, -1);
                self.ctx.set_repl_seldb(sequenced.replication_selected_db);
                self.ctx
                    .persistence_mut()
                    .set_aof_selected_db(sequenced.aof_selected_db);
                for frame in &sequenced.replication_frames {
                    self.propagate_to_replicas(frame);
                }
                if self.ctx.config().persistence.aof_enabled {
                    for frame in &sequenced.aof_frames {
                        self.feed_aof(frame);
                    }
                }
            }
            self.propagate_slot_import_evictions();
        }
        true
    }

    fn propagate_slot_import_evictions(&mut self) {
        let evicted = self.perform_evictions().evicted;
        self.apply_background_evictions(evicted);
    }
}

pub(crate) fn is_syncslots_ack(args: &CommandArgs) -> bool {
    is_syncslots_command(args)
        && args
            .args
            .get(1)
            .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"ack"))
}

pub(crate) fn is_syncslots_command(args: &CommandArgs) -> bool {
    args.name.eq_ignore_ascii_case(b"cluster")
        && args
            .args
            .first()
            .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"syncslots"))
}
