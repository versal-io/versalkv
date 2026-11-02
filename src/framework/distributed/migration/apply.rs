use crate::framework::capabilities::connection::Connection;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandRegistry};

#[derive(Debug, PartialEq, Eq)]
pub enum MigrationApplyError {
    Command(String),
}

pub struct MigrationCommandPlayer {
    conn: Connection,
}

/// Apply a command from a shared replication/AOF stream to every matching
/// tracking-only import. Unlike a live slot-import socket, this stream is not
/// owned by one job, so routing is derived from the command's keys.
pub(crate) fn apply_tracking_import_command(
    args: &CommandArgs,
    registry: &CommandRegistry,
    ctx: &mut ServerContext,
) -> Result<bool, MigrationApplyError> {
    let is_stream_control = [b"SELECT".as_slice(), b"MULTI", b"EXEC", b"DISCARD"]
        .iter()
        .any(|name| args.name.eq_ignore_ascii_case(name));
    let slots = (!is_stream_control)
        .then(|| command_slots(args, registry))
        .flatten();
    let ids = ctx
        .cluster_state()
        .as_deref()
        .map(|state| {
            state
                .migrations
                .views()
                .into_iter()
                .filter(|job| {
                    job.operation
                        == crate::engine::distributed::cluster::migration::MigrationOperation::Import
                        && job.phase
                            == crate::engine::distributed::cluster::migration::MigrationPhase::Import(
                                crate::engine::distributed::cluster::migration::ImportPhase::OccurringOnPrimary,
                            )
                        && (is_stream_control
                            || slots.as_ref().is_some_and(|slots| {
                                slots.iter().any(|slot| job.ranges.contains(*slot))
                            }))
                })
                .map(|job| job.id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if ids.is_empty() {
        return Ok(false);
    }
    for id in ids {
        ctx.apply_slot_import(&id, args.clone(), registry)?;
    }
    Ok(!is_stream_control)
}

fn command_slots(args: &CommandArgs, registry: &CommandRegistry) -> Option<Vec<u16>> {
    let spec = registry.lookup(&args.name)?;
    let keys = if let Some(get_keys) = spec.get_keys {
        get_keys(&args.args)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>()
    } else {
        let first = usize::try_from(spec.first_key).ok()?.checked_sub(1)?;
        let step = spec.key_step.unsigned_abs() as usize;
        if step == 0 || first >= args.args.len() {
            return None;
        }
        let last = if spec.last_key < 0 {
            args.args.len().checked_sub(1)?
        } else {
            usize::try_from(spec.last_key).ok()?.checked_sub(1)?
        };
        (first..=last)
            .step_by(step)
            .filter_map(|index| args.args.get(index).map(AsRef::as_ref))
            .collect::<Vec<_>>()
    };
    Some(
        keys.into_iter()
            .map(crate::engine::distributed::cluster::node::key_hash_slot)
            .collect(),
    )
}

impl MigrationCommandPlayer {
    pub fn new() -> Self {
        let mut conn = Connection::new_fake();
        conn.import_source = true;
        Self { conn }
    }

    pub(crate) fn selected_db(&self) -> u32 {
        self.conn
            .transaction
            .selected_db()
            .and_then(|db| u32::try_from(db).ok())
            .unwrap_or(self.conn.selected_db)
    }

    pub fn apply(
        &mut self,
        args: CommandArgs,
        registry: &CommandRegistry,
        ctx: &mut ServerContext,
    ) -> Result<(), MigrationApplyError> {
        ctx.set_command_registry_snapshot(registry);
        crate::framework::aof_replay::apply_replay_command(&mut self.conn, args, registry, ctx)
            .map_err(MigrationApplyError::Command)
    }

    pub(crate) fn apply_with_execution(
        &mut self,
        args: CommandArgs,
        registry: &CommandRegistry,
        ctx: &mut ServerContext,
    ) -> (
        Result<(), MigrationApplyError>,
        crate::framework::aof_replay::ReplayExecution,
    ) {
        ctx.set_command_registry_snapshot(registry);
        let (result, execution) = crate::framework::aof_replay::apply_replay_command_with_execution(
            &mut self.conn,
            args,
            registry,
            ctx,
        );
        (result.map_err(MigrationApplyError::Command), execution)
    }
}

#[cfg(test)]
mod tests {
    use super::MigrationCommandPlayer;
    use crate::framework::context::ServerContext;
    use crate::framework::{CommandArgs, CommandRegistry};
    use bytes::Bytes;

    #[test]
    fn transaction_selected_db_tracks_queued_select_before_exec() {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let mut ctx = ServerContext::new(2);
        let mut player = MigrationCommandPlayer::new();

        player
            .apply(
                CommandArgs::from_parts(vec![Bytes::from_static(b"MULTI")]),
                &registry,
                &mut ctx,
            )
            .unwrap();
        player
            .apply(
                CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SELECT"),
                    Bytes::from_static(b"1"),
                ]),
                &registry,
                &mut ctx,
            )
            .unwrap();

        assert_eq!(player.selected_db(), 1);
    }
}
