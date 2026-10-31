use super::super::super::ServerWorkerState;
use crate::engine::command::CommandArgs;
use crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole;

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
