use super::super::super::ServerWorkerState;
use crate::engine::command::CommandArgs;
use crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole;

impl ServerWorkerState {
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
        let rejects_for_oom = self.registry.lookup(&args.name).is_some_and(|spec| {
            (spec.flags & crate::engine::command::CMD_DENYOOM) != 0
                && self.ctx.maxmemory_state().is_over_limit()
        });
        if rejects_for_oom {
            self.fail_slot_migration(&id, "Ran out of memory (OOM) during slot import");
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .mark_closing();
            return true;
        }
        let frame =
            crate::runtime::server::worker::distributed::propagation::encode_command_args(&args);
        let command_name = args.name.clone();
        if let Err(error) = self.ctx.apply_slot_import(&id, args, &self.registry) {
            crate::server_log!(
                warning,
                "Slot import {} failed to apply {}: {:?}",
                id,
                String::from_utf8_lossy(&command_name),
                error
            );
            self.fail_slot_migration(
                &id,
                "Failed to process command during slot migration. Check logs for more information",
            );
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .mark_closing();
        } else {
            self.propagate_to_replicas(&frame);
            if self.ctx.config().persistence.aof_enabled {
                self.feed_aof(&frame);
            }
        }
        true
    }
}
