//! Framework adapter for replaying persisted AOF commands through handlers.
//! Persistence owns the file format; this adapter supplies the command registry,
//! connection, and server context required to apply decoded commands.

#[cfg(test)]
use bytes::Bytes;

#[cfg(test)]
use crate::foundation::io::persistence::AofCommandPlayer;
use crate::framework::capabilities::connection::{CommandEffectDrain, Connection, TransactionEnd};
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandRegistry};

#[cfg(test)]
pub(crate) fn load_aof_rdb_preamble(
    bytes: &[u8],
    ctx: &mut ServerContext,
) -> Result<usize, crate::foundation::io::persistence::LoadError> {
    if !crate::foundation::io::persistence::is_rdb_magic(bytes) {
        return Ok(0);
    }
    let strict = !ctx.config().persistence.rdb_version_check_relaxed;
    let policy = crate::foundation::io::persistence::RdbDecodePolicy::replication(strict, 0)
        .with_hash_limits(
            ctx.config().encoding.hash_max_listpack_entries as usize,
            ctx.config().encoding.hash_max_listpack_value as usize,
        )
        .with_zset_limits(
            ctx.config().encoding.zset_max_ziplist_entries as usize,
            ctx.config().encoding.zset_max_ziplist_value as usize,
        );
    let metadata = crate::foundation::io::persistence::load_rdb_bytes_with_policy(
        bytes,
        ctx.dbs_mut(),
        policy,
    )?;
    let consumed = metadata.consumed;
    let replication = metadata.replication;
    let aux_fields = replication
        .as_ref()
        .map(|info| info.aux_fields.as_slice())
        .unwrap_or_default();
    ctx.replace_slot_imports_from_rdb_aux(aux_fields)
        .map_err(|error| {
            log::warn!("Invalid slot migration metadata in AOF RDB preamble: {error}");
            crate::foundation::io::persistence::LoadError::Corrupt
        })?;
    Ok(consumed)
}

/// A fake connection used while replaying persisted commands synchronously.
#[cfg(test)]
pub(crate) struct AofReplayPlayer<'a> {
    pub(crate) conn: Connection,
    pub(crate) registry: &'a CommandRegistry,
    pub(crate) ctx: &'a mut ServerContext,
}

#[cfg(test)]
impl<'a> AofReplayPlayer<'a> {
    pub(crate) fn new(registry: &'a CommandRegistry, ctx: &'a mut ServerContext) -> Self {
        let mut conn = Connection::new_fake();
        conn.import_source = true;
        Self {
            conn,
            registry,
            ctx,
        }
    }

    fn _clear_replay_side_effects(&mut self) {
        self.conn.discard_command_cycle();
        self.conn.clear_output_queue();
        self.ctx.clear_expired_deletions();
        let _ = self.ctx.drain_lazy_expired_hash_fields();
        let _ = self.ctx.drain_pending_hexpired();
        self.ctx.script_mut().pending_internal_monitor.clear();
    }
}

pub(crate) fn apply_replay_command(
    conn: &mut Connection,
    args: CommandArgs,
    registry: &CommandRegistry,
    ctx: &mut ServerContext,
) -> Result<(), String> {
    apply_replay_command_with_execution(conn, args, registry, ctx).0
}

/// Replay transaction controls run directly rather than through a command spec.
pub(crate) enum ReplayExecution {
    NotInvoked,
    Handler { duration_us: u64 },
    TransactionControl { duration_us: u64 },
}

impl ReplayExecution {
    pub(crate) fn duration_us(self) -> Option<u64> {
        match self {
            Self::NotInvoked => None,
            Self::Handler { duration_us } | Self::TransactionControl { duration_us } => {
                Some(duration_us)
            }
        }
    }
}

pub(crate) fn apply_replay_command_with_execution(
    conn: &mut Connection,
    args: CommandArgs,
    registry: &CommandRegistry,
    ctx: &mut ServerContext,
) -> (Result<(), String>, ReplayExecution) {
    let mut execution = ReplayExecution::NotInvoked;
    let result = apply_replay_command_inner(conn, args, registry, ctx, &mut execution);
    (result, execution)
}

fn apply_replay_command_inner(
    conn: &mut Connection,
    args: CommandArgs,
    registry: &CommandRegistry,
    ctx: &mut ServerContext,
    execution: &mut ReplayExecution,
) -> Result<(), String> {
    let name_lower = args.name.to_ascii_lowercase();
    let tracking_only = conn.import_source
        && conn.slot_migration_role.is_none()
        && !name_lower.eq_ignore_ascii_case(b"cluster")
        && crate::framework::distributed::migration::apply_tracking_import_command(
            &args, registry, ctx,
        )
        .map_err(|error| format!("{error:?}"))?;
    if name_lower == b"multi" {
        let started = std::time::Instant::now();
        if !conn.transaction.begin(i64::from(conn.selected_db), 0) {
            return Err("ERR MULTI calls can not be nested".to_string());
        }
        *execution = ReplayExecution::TransactionControl {
            duration_us: (started.elapsed().as_micros() as u64).max(1),
        };
        return Ok(());
    }
    if name_lower == b"discard" {
        let started = std::time::Instant::now();
        if conn.transaction.is_active() {
            conn.transaction.end(TransactionEnd::Discard);
        }
        *execution = ReplayExecution::TransactionControl {
            duration_us: (started.elapsed().as_micros() as u64).max(1),
        };
        return Ok(());
    }
    if name_lower == b"exec" {
        if !conn.transaction.is_active() {
            let started = std::time::Instant::now();
            let error = "ERR EXEC without MULTI".to_string();
            *execution = ReplayExecution::TransactionControl {
                duration_us: (started.elapsed().as_micros() as u64).max(1),
            };
            return Err(error);
        }
        let started = std::time::Instant::now();
        let commands = conn.transaction.take_queued_commands();
        conn.transaction.end(TransactionEnd::ExecutionCompleted);
        let result = commands.iter().try_for_each(|queued| {
            execute_one_replay_command(
                conn,
                queued,
                registry,
                ctx,
                &mut ReplayExecution::NotInvoked,
            )
        });
        *execution = ReplayExecution::TransactionControl {
            duration_us: (started.elapsed().as_micros() as u64).max(1),
        };
        if result.is_ok() {
            clear_replay_side_effects(conn, ctx);
        }
        return result;
    }
    if conn.transaction.is_active() {
        if tracking_only {
            return Ok(());
        }
        conn.transaction.queue(ctx.num_dbs(), args);
        return Ok(());
    }
    if tracking_only {
        clear_replay_side_effects(conn, ctx);
        return Ok(());
    }
    let result = execute_one_replay_command(conn, &args, registry, ctx, execution);
    clear_replay_side_effects(conn, ctx);
    result
}

fn execute_one_replay_command(
    conn: &mut Connection,
    args: &CommandArgs,
    registry: &CommandRegistry,
    ctx: &mut ServerContext,
    execution: &mut ReplayExecution,
) -> Result<(), String> {
    let spec = registry
        .lookup(&args.name)
        .ok_or_else(|| format!("unknown command '{}'", String::from_utf8_lossy(&args.name)))?;
    conn.begin_nested_command_effects();
    let started = std::time::Instant::now();
    let result = spec.execute(conn, args, ctx);
    *execution = ReplayExecution::Handler {
        duration_us: (started.elapsed().as_micros() as u64).max(1),
    };
    let mut effects = conn
        .take_command_effects(CommandEffectDrain::Nested)
        .expect("AOF replay command effects are drained exactly once");
    conn.finish_nested_reply_state(&mut effects);
    match result {
        crate::foundation::io::protocol::Value::Error(error) => {
            Err(String::from_utf8_lossy(&error).into_owned())
        }
        _ => Ok(()),
    }
}

fn clear_replay_side_effects(conn: &mut Connection, ctx: &mut ServerContext) {
    conn.discard_command_cycle();
    conn.clear_output_queue();
    ctx.clear_expired_deletions();
    let _ = ctx.drain_lazy_expired_hash_fields();
    let _ = ctx.drain_pending_hexpired();
    ctx.script_mut().pending_internal_monitor.clear();
}

#[cfg(test)]
impl AofCommandPlayer for AofReplayPlayer<'_> {
    fn select_db(&mut self, dbid: u32) {
        self.conn.selected_db = dbid;
    }

    fn execute(&mut self, args: &[&[u8]]) -> Result<(), String> {
        if args.is_empty() {
            return Ok(());
        }
        let parts: Vec<Bytes> = args.iter().map(|arg| Bytes::copy_from_slice(arg)).collect();
        let cmd_args = CommandArgs::from_parts(parts);
        apply_replay_command(&mut self.conn, cmd_args, self.registry, self.ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::{load_aof_rdb_preamble, AofReplayPlayer};
    use crate::foundation::io::persistence::AofCommandPlayer;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandRegistry;
    use bytes::Bytes;

    fn _seed_replay_expiry_side_effects(ctx: &mut ServerContext, key: &[u8]) {
        let db = ctx.db_mut(0).unwrap();
        db.get_or_create_hash(key)
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
        db.set_hash_field_expire(key, b"field", 0);
        assert!(db.check_hash_field_expired(key, b"field"));
    }

    fn _assert_replay_effects_cleared(player: &AofReplayPlayer<'_>) {
        assert!(player.conn.script_propagation().is_empty());
        assert!(player.conn.custom_propagation().is_empty());
        assert!(player.conn.dirty_keys().is_empty());
        assert!(player.conn.pending_notifications().is_empty());
        assert!(player.conn.pending_shard_notifications().is_empty());
    }

    fn _drain_replay_expiry_side_effects(
        ctx: &mut ServerContext,
    ) -> (u64, Vec<(u32, Vec<u8>, Vec<u8>)>) {
        (
            ctx.drain_lazy_expired_hash_fields(),
            ctx.drain_pending_hexpired(),
        )
    }

    #[test]
    fn replayed_slot_import_lifecycle_leaves_no_runtime_actions() {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let mut ctx = ServerContext::new(1);
        let cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        *ctx.cluster_state_mut() = Some(Box::new(cluster));
        ctx.set_loading(true);
        let mut player = AofReplayPlayer::new(&registry, &mut ctx);
        let source = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let name = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        player
            .execute(&[
                b"CLUSTER",
                b"SYNCSLOTS",
                b"ESTABLISH",
                b"SOURCE",
                source,
                b"NAME",
                name,
                b"SLOTSRANGE",
                b"1",
                b"1",
            ])
            .unwrap();
        player
            .execute(&[
                b"CLUSTER",
                b"SYNCSLOTS",
                b"FINISH",
                b"STATE",
                b"FAILED",
                b"NAME",
                name,
                b"MESSAGE",
                b"persisted terminal state",
            ])
            .unwrap();

        let cluster = ctx.cluster_state_mut().as_deref_mut().unwrap();
        assert!(cluster.take_migration_actions().is_empty());
        let id =
            crate::engine::distributed::cluster::migration::MigrationJobId::parse(name).unwrap();
        let job = cluster.migrations.get(&id).unwrap();
        assert_eq!(
            job.phase,
            crate::engine::distributed::cluster::migration::MigrationPhase::Terminal(
                crate::engine::distributed::cluster::migration::TerminalPhase::Failed,
            )
        );
        assert_eq!(job.message.as_deref(), Some("persisted terminal state"));
    }

    #[test]
    fn rdb_preamble_restores_slot_import_before_incremental_finish() {
        use crate::engine::distributed::cluster::migration::{MigrationJobId, SlotRanges};
        use crate::engine::distributed::cluster::{NodeId, SlotRange};
        use crate::foundation::io::persistence::RdbReplicationInfo;
        use crate::framework::distributed::migration::{SlotImportMetadata, SLOT_IMPORT_AUX_KEY};

        let source = NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let target = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
        let ranges = SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap();
        let metadata = SlotImportMetadata {
            id: id.clone(),
            source,
            target,
            ranges,
        };
        let replication = RdbReplicationInfo {
            repl_stream_db: 0,
            repl_id: "dddddddddddddddddddddddddddddddddddddddd".to_string(),
            repl_offset: 1,
            aux_fields: vec![(SLOT_IMPORT_AUX_KEY.to_vec(), metadata.encode())],
        };
        let source_ctx = ServerContext::new(1);
        let preamble =
            crate::foundation::io::persistence::serialize_all_with_functions_and_replication(
                source_ctx.dbs(),
                0,
                &[],
                true,
                Some(&replication),
                &[],
            );

        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let mut ctx = ServerContext::new(1);
        let cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6379, 16_379);
        *ctx.cluster_state_mut() = Some(Box::new(cluster));
        ctx.prepare_slot_migration_load();
        ctx.set_loading(true);

        assert_eq!(
            load_aof_rdb_preamble(&preamble, &mut ctx).unwrap(),
            preamble.len()
        );
        let mut player = AofReplayPlayer::new(&registry, &mut ctx);
        player
            .execute(&[
                b"CLUSTER",
                b"SYNCSLOTS",
                b"FINISH",
                b"STATE",
                b"FAILED",
                b"NAME",
                id.as_str().as_bytes(),
                b"MESSAGE",
                b"persisted terminal state",
            ])
            .unwrap();

        let job = ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .migrations
            .get(&id)
            .unwrap();
        assert!(job.phase.is_terminal());
        assert_eq!(job.message.as_deref(), Some("persisted terminal state"));
    }

    #[test]
    fn replay_clears_side_effects_after_commands_and_exec() {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let mut ctx = ServerContext::new(16);
        ctx.set_command_registry_snapshot(&registry);
        _seed_replay_expiry_side_effects(&mut ctx, b"replay-standalone-expiry");
        let mut player = AofReplayPlayer::new(&registry, &mut ctx);
        player
            .execute(&[b"EVAL", b"redis.call('set','replay-eval','1')", b"0"])
            .unwrap();
        _assert_replay_effects_cleared(&player);
        let standalone_expiry_effects = _drain_replay_expiry_side_effects(player.ctx);

        let library = b"#!lua name=replay\nserver.register_function('replay_fn', function(keys, args) redis.call('set','replay-fcall','1') end)";
        player.execute(&[b"FUNCTION", b"LOAD", library]).unwrap();
        _assert_replay_effects_cleared(&player);
        player.execute(&[b"FCALL", b"replay_fn", b"0"]).unwrap();
        _assert_replay_effects_cleared(&player);

        player.execute(&[b"MULTI"]).unwrap();
        player
            .execute(&[b"EVAL", b"redis.call('set','replay-exec','1')", b"0"])
            .unwrap();
        _seed_replay_expiry_side_effects(player.ctx, b"replay-exec-expiry");
        player.execute(&[b"EXEC"]).unwrap();
        _assert_replay_effects_cleared(&player);
        let exec_expiry_effects = _drain_replay_expiry_side_effects(player.ctx);

        assert_eq!(
            [standalone_expiry_effects, exec_expiry_effects],
            [(0, Vec::new()), (0, Vec::new())]
        );
    }
}
