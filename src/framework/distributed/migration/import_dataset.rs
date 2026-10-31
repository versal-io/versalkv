use crate::engine::distributed::cluster::migration::SlotRanges;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::framework::context::RuntimeConfig;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandRegistry};

use super::{MigrationApplyError, MigrationCommandPlayer};

#[derive(Debug, PartialEq, Eq)]
pub enum CommitImportError {
    DestinationCollision { slot: u16 },
}

pub struct ImportDataset {
    context: ServerContext,
    player: MigrationCommandPlayer,
    ranges: SlotRanges,
}

impl ImportDataset {
    pub fn new_like(serving: &DbSet, config: &RuntimeConfig, ranges: SlotRanges) -> Self {
        let mut dataset = Self {
            context: ServerContext::new_import_context(serving.empty_like(), config.clone()),
            player: MigrationCommandPlayer::new(),
            ranges,
        };
        dataset.preserve_expired_keys();
        dataset
    }

    pub fn from_dbs(dbs: DbSet, config: &RuntimeConfig, ranges: SlotRanges) -> Self {
        let mut dataset = Self {
            context: ServerContext::new_import_context(dbs, config.clone()),
            player: MigrationCommandPlayer::new(),
            ranges,
        };
        dataset.preserve_expired_keys();
        dataset
    }

    pub fn ranges(&self) -> &SlotRanges {
        &self.ranges
    }

    pub fn apply(
        &mut self,
        args: CommandArgs,
        registry: &CommandRegistry,
    ) -> Result<(), MigrationApplyError> {
        self.preserve_expired_keys();
        let result = self.player.apply(args, registry, &mut self.context);
        self.preserve_expired_keys();
        result
    }

    pub(crate) fn apply_with_execution(
        &mut self,
        args: CommandArgs,
        registry: &CommandRegistry,
    ) -> (
        Result<(), MigrationApplyError>,
        crate::framework::aof_replay::ReplayExecution,
    ) {
        self.preserve_expired_keys();
        let result = self
            .player
            .apply_with_execution(args, registry, &mut self.context);
        // Some handlers temporarily toggle this DB-local guard and restore it
        // for ordinary serving semantics. The isolated import dataset owns a
        // stronger lifetime invariant, so re-arm it after every command.
        self.preserve_expired_keys();
        result
    }

    pub fn dbs(&self) -> &DbSet {
        self.context.dbs()
    }

    pub fn dbs_mut(&mut self) -> &mut DbSet {
        self.preserve_expired_keys();
        self.context.dbs_mut()
    }

    fn preserve_expired_keys(&mut self) {
        self.context.dbs_mut().set_no_lazy_expire_all(true);
    }

    pub fn command_snapshot(&self, ranges: &SlotRanges) -> std::io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        crate::foundation::io::persistence::aof_rewrite::write_filtered_command_snapshot(
            self.context.dbs(),
            |_db, key| {
                ranges.contains(crate::engine::distributed::cluster::node::key_hash_slot(
                    key,
                ))
            },
            false,
            0,
            &mut bytes,
        )?;
        Ok(bytes)
    }

    pub fn commit_into(
        &mut self,
        serving: &mut DbSet,
        ranges: &SlotRanges,
    ) -> Result<usize, CommitImportError> {
        for range in ranges.iter() {
            for slot in range.start..=range.end {
                if serving.count_keys_in_slot(slot as usize) != 0 {
                    return Err(CommitImportError::DestinationCollision { slot });
                }
            }
        }
        let mut moved = 0;
        for range in ranges.iter() {
            for slot in range.start..=range.end {
                let entries = self.context.take_slot_entries(slot as usize);
                moved += entries.len();
                serving.restore_slot_entries(entries);
            }
        }
        Ok(moved)
    }
}

#[cfg(test)]
mod tests {
    use super::{CommitImportError, ImportDataset};
    use crate::engine::distributed::cluster::migration::SlotRanges;
    use crate::engine::distributed::cluster::node::key_hash_slot;
    use crate::engine::distributed::cluster::SlotRange;
    use crate::foundation::io::persistence::{load_aof_bytes, AofCommandPlayer};
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;
    use crate::framework::context::RuntimeConfig;
    use crate::framework::{CommandArgs, CommandRegistry};
    use bytes::Bytes;

    fn command(parts: &[&str]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect(),
        )
    }

    fn registry() -> CommandRegistry {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        registry
    }

    #[derive(Default)]
    struct SnapshotRecorder {
        commands: Vec<Vec<Vec<u8>>>,
    }

    impl AofCommandPlayer for SnapshotRecorder {
        fn select_db(&mut self, _dbid: u32) {}

        fn execute(&mut self, args: &[&[u8]]) -> Result<(), String> {
            if !args
                .first()
                .is_some_and(|name| name.eq_ignore_ascii_case(b"select"))
            {
                self.commands
                    .push(args.iter().map(|arg| arg.to_vec()).collect());
            }
            Ok(())
        }
    }

    #[test]
    fn applied_data_is_invisible_until_commit_and_preserves_db_and_ttl() {
        let serving = DbSet::new_with_slot_router(16, 14, |key| key_hash_slot(key) as usize);
        let mut importing =
            ImportDataset::new_like(&serving, &RuntimeConfig::new(), SlotRanges::default());
        let registry = registry();
        let key = "{atomic}key";
        let deadline = 9_999_999_999_999_u64;

        importing
            .apply(command(&["SELECT", "15"]), &registry)
            .unwrap();
        importing.apply(command(&["MULTI"]), &registry).unwrap();
        importing
            .apply(command(&["SET", key, "value"]), &registry)
            .unwrap();
        importing
            .apply(
                command(&["PEXPIREAT", key, &deadline.to_string()]),
                &registry,
            )
            .unwrap();
        importing.apply(command(&["EXEC"]), &registry).unwrap();

        assert_eq!(serving.db_size(15), 0);
        assert_eq!(importing.dbs().db_size(15), 1);
        assert_eq!(
            importing.dbs().get(15).unwrap().get_expire(key.as_bytes()),
            Some(deadline)
        );
    }

    #[test]
    fn expired_import_key_survives_lazy_lookup_until_commit() {
        let mut serving = DbSet::new_with_slot_router(1, 14, |key| key_hash_slot(key) as usize);
        let mut importing =
            ImportDataset::new_like(&serving, &RuntimeConfig::new(), SlotRanges::default());
        let registry = registry();
        let key = "{atomic}hash";

        importing
            .apply(command(&["HSET", key, "field", "value"]), &registry)
            .unwrap();
        importing
            .apply(command(&["PEXPIREAT", key, "1"]), &registry)
            .unwrap();
        let deadline = importing
            .dbs()
            .get(0)
            .unwrap()
            .get_expire(key.as_bytes())
            .expect("imported key must retain its deadline");
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(deadline <= crate::foundation::io::store::kv_engine::now_ms());
        importing
            .apply(command(&["HGET", key, "field"]), &registry)
            .unwrap();

        let db = importing.dbs().get(0).unwrap();
        assert!(db.get_allow_expired(key.as_bytes()).is_some());
        assert_eq!(db.get_expire(key.as_bytes()), Some(deadline));

        let slot = key_hash_slot(key.as_bytes());
        let ranges = SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap();
        assert_eq!(importing.commit_into(&mut serving, &ranges), Ok(1));
        assert_eq!(
            serving.get(0).unwrap().get_expire(key.as_bytes()),
            Some(deadline)
        );
        assert!(serving.get_mut(0).unwrap().get(key.as_bytes()).is_none());
        assert!(serving
            .get(0)
            .unwrap()
            .get_allow_expired(key.as_bytes())
            .is_none());
    }

    #[test]
    fn command_snapshot_keeps_expired_import_value_and_absolute_ttl() {
        let serving = DbSet::new_with_slot_router(1, 14, |key| key_hash_slot(key) as usize);
        let mut importing =
            ImportDataset::new_like(&serving, &RuntimeConfig::new(), SlotRanges::default());
        let registry = registry();
        let key = "{atomic}snapshot";
        importing
            .apply(command(&["SET", key, "value"]), &registry)
            .unwrap();
        importing
            .apply(command(&["PEXPIREAT", key, "1"]), &registry)
            .unwrap();
        let deadline = importing
            .dbs()
            .get(0)
            .unwrap()
            .get_expire(key.as_bytes())
            .expect("import snapshot key must retain its absolute deadline");
        std::thread::sleep(std::time::Duration::from_millis(5));

        let slot = key_hash_slot(key.as_bytes());
        let ranges = SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap();
        let snapshot = importing.command_snapshot(&ranges).unwrap();
        let mut recorder = SnapshotRecorder::default();
        load_aof_bytes(&snapshot, &mut recorder).unwrap();

        assert!(recorder.commands.iter().any(|args| {
            args == &vec![b"SET".to_vec(), key.as_bytes().to_vec(), b"value".to_vec()]
        }));
        assert!(recorder.commands.iter().any(|args| {
            args == &vec![
                b"PEXPIREAT".to_vec(),
                key.as_bytes().to_vec(),
                deadline.to_string().into_bytes(),
            ]
        }));
    }

    #[test]
    fn commit_moves_only_requested_slots_after_collision_preflight() {
        let mut serving = DbSet::new_with_slot_router(2, 14, |key| key_hash_slot(key) as usize);
        let mut importing =
            ImportDataset::new_like(&serving, &RuntimeConfig::new(), SlotRanges::default());
        let requested_key = b"{atomic}requested";
        let other_key = b"{other}other";
        let requested_slot = key_hash_slot(requested_key);
        let registry = registry();
        importing
            .apply(
                command(&["SET", std::str::from_utf8(requested_key).unwrap(), "value"]),
                &registry,
            )
            .unwrap();
        importing
            .apply(
                command(&["SET", std::str::from_utf8(other_key).unwrap(), "other"]),
                &registry,
            )
            .unwrap();
        let ranges =
            SlotRanges::new(vec![SlotRange::new(requested_slot, requested_slot).unwrap()]).unwrap();

        assert_eq!(importing.commit_into(&mut serving, &ranges), Ok(1));
        assert_eq!(serving.count_keys_in_slot(requested_slot as usize), 1);
        assert_eq!(
            importing
                .dbs()
                .count_keys_in_slot(key_hash_slot(other_key) as usize),
            1
        );
    }

    #[test]
    fn collision_rejects_commit_before_moving_any_data() {
        let mut serving = DbSet::new_with_slot_router(1, 14, |key| key_hash_slot(key) as usize);
        let key = b"{atomic}existing";
        let slot = key_hash_slot(key);
        serving
            .get_mut(0)
            .unwrap()
            .set(key.to_vec(), Robj::string("serving"));
        let mut importing =
            ImportDataset::new_like(&serving, &RuntimeConfig::new(), SlotRanges::default());
        importing
            .apply(command(&["SET", "{atomic}incoming", "value"]), &registry())
            .unwrap();
        let ranges = SlotRanges::new(vec![SlotRange::new(slot, slot).unwrap()]).unwrap();

        assert_eq!(
            importing.commit_into(&mut serving, &ranges),
            Err(CommitImportError::DestinationCollision { slot })
        );
        assert_eq!(serving.count_keys_in_slot(slot as usize), 1);
        assert_eq!(importing.dbs().count_keys_in_slot(slot as usize), 1);
    }
}
