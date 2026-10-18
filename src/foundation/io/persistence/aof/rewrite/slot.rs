use super::encode::{
    write_db_command_entries_including_expired, write_function_snapshot, DbCommandSnapshotOptions,
};
use crate::foundation::io::store::kv_engine::DbSet;
use std::io::Write;

pub fn write_filtered_command_snapshot(
    dbs: &DbSet,
    mut include: impl FnMut(u32, &[u8]) -> bool,
    include_functions: bool,
    key_delay_us: i64,
    writer: &mut impl Write,
) -> std::io::Result<usize> {
    let mut total = 0;
    let mut last_db = None;
    for dbid in 0..dbs.num_dbs() as u32 {
        let Some(db) = dbs.get(dbid) else { continue };
        let count = write_db_command_entries_including_expired(
            db,
            |key| include(dbid, key),
            writer,
            DbCommandSnapshotOptions::new(dbid, last_db != Some(dbid), key_delay_us),
        )?;
        if count != 0 {
            total += count;
            last_db = Some(dbid);
        }
    }
    if include_functions {
        write_function_snapshot(&[], writer)?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use crate::foundation::io::persistence::{load_aof_bytes, AofCommandPlayer};
    use crate::foundation::io::store::kv_engine::{now_ms, DbSet};
    use crate::foundation::io::store::Robj;

    #[derive(Default)]
    struct Recorder {
        db: u32,
        commands: Vec<(u32, Vec<Vec<u8>>)>,
    }

    impl AofCommandPlayer for Recorder {
        fn select_db(&mut self, dbid: u32) {
            self.db = dbid;
        }

        fn execute(&mut self, args: &[&[u8]]) -> Result<(), String> {
            if args
                .first()
                .is_some_and(|name| name.eq_ignore_ascii_case(b"select"))
            {
                self.db = std::str::from_utf8(args.get(1).copied().unwrap_or_default())
                    .map_err(|error| error.to_string())?
                    .parse::<u32>()
                    .map_err(|error| error.to_string())?;
                return Ok(());
            }
            self.commands
                .push((self.db, args.iter().map(|arg| arg.to_vec()).collect()));
            Ok(())
        }
    }

    #[test]
    fn filtered_snapshot_keeps_selected_keys_db_and_absolute_expiry() {
        let mut dbs = DbSet::new(16);
        dbs.get_mut(0)
            .unwrap()
            .set(b"keep-0".to_vec(), Robj::string("zero"));
        dbs.get_mut(0)
            .unwrap()
            .set_expire(b"keep-0", 9_999_999_999_999);
        dbs.get_mut(0)
            .unwrap()
            .set(b"drop-0".to_vec(), Robj::string("hidden"));
        dbs.get_mut(15)
            .unwrap()
            .set(b"keep-15".to_vec(), Robj::string("fifteen"));

        let mut bytes = Vec::new();
        let count = super::write_filtered_command_snapshot(
            &dbs,
            |_dbid, key| key.starts_with(b"keep"),
            false,
            0,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(count, 2);
        let mut recorder = Recorder::default();
        load_aof_bytes(&bytes, &mut recorder).unwrap();
        assert!(recorder.commands.iter().any(|(db, command)| {
            *db == 0 && command == &vec![b"SET".to_vec(), b"keep-0".to_vec(), b"zero".to_vec()]
        }));
        assert!(recorder.commands.iter().any(|(db, command)| {
            *db == 0 && command.first().is_some_and(|name| name == b"PEXPIREAT")
        }));
        assert!(recorder.commands.iter().any(|(db, command)| {
            *db == 15 && command == &vec![b"SET".to_vec(), b"keep-15".to_vec(), b"fifteen".to_vec()]
        }));
        assert!(!bytes
            .windows(b"drop-0".len())
            .any(|window| window == b"drop-0"));
    }

    #[test]
    fn filtered_slot_snapshot_keeps_physically_present_expired_keys() {
        let mut dbs = DbSet::new(1);
        let deadline = now_ms().saturating_sub(1);
        dbs.get_mut(0)
            .unwrap()
            .set(b"expired".to_vec(), Robj::string("value"));
        dbs.get_mut(0).unwrap().set_expire(b"expired", deadline);
        assert!(dbs.get(0).unwrap().get_allow_expired(b"expired").is_some());

        let mut bytes = Vec::new();
        let count =
            super::write_filtered_command_snapshot(&dbs, |_dbid, _key| true, false, 0, &mut bytes)
                .unwrap();

        assert_eq!(count, 1);
        let mut recorder = Recorder::default();
        load_aof_bytes(&bytes, &mut recorder).unwrap();
        assert!(recorder.commands.iter().any(|(_, command)| {
            command == &vec![b"SET".to_vec(), b"expired".to_vec(), b"value".to_vec()]
        }));
        assert!(recorder
            .commands
            .iter()
            .any(|(_, command)| { command.first().is_some_and(|name| name == b"PEXPIREAT") }));
    }

    #[test]
    fn filtered_snapshot_never_emits_functions_when_disabled() {
        let dbs = DbSet::new(1);
        let mut bytes = Vec::new();
        super::write_filtered_command_snapshot(&dbs, |_dbid, _key| true, false, 0, &mut bytes)
            .unwrap();
        assert!(!bytes
            .windows(b"FUNCTION".len())
            .any(|window| window == b"FUNCTION"));
    }
}
