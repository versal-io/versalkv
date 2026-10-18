use super::*;
use crate::foundation::io::persistence::{AofFsync, AofOpenMode};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct RecordingPlayer {
    db: Arc<Mutex<u32>>,
    cmds: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

impl RecordingPlayer {
    fn new() -> Self {
        Self {
            db: Arc::new(Mutex::new(0)),
            cmds: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl AofCommandPlayer for RecordingPlayer {
    fn select_db(&mut self, dbid: u32) {
        *self.db.lock().unwrap() = dbid;
    }

    fn execute(&mut self, args: &[&[u8]]) -> Result<(), String> {
        self.cmds
            .lock()
            .unwrap()
            .push(args.iter().map(|arg| arg.to_vec()).collect());
        Ok(())
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "valkey-aof-journal-{}-{}",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn journal_in(dir: &std::path::Path) -> AofJournal {
    let appender = AofAppender::open(
        dir.to_str().unwrap(),
        "appendonly.aof",
        "",
        AofOpenMode::CreateNew,
        AofFsync::No,
    )
    .unwrap();
    AofJournal::new(appender)
}

#[test]
fn append_replay_rewrite_and_truncate_operate_on_the_underlying_aof_file() {
    let dir = temp_dir("roundtrip");
    let mut journal = journal_in(&dir);
    let player = RecordingPlayer::new();
    let recorded = player.cmds.clone();
    journal.set_player(Box::new(player));

    journal
        .append(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
        .unwrap();
    assert!(std::fs::metadata(journal.appender.path()).unwrap().len() > 0);

    journal.replay().unwrap();
    assert_eq!(
        recorded.lock().unwrap()[0],
        vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()]
    );

    journal.truncate(0).unwrap();
    assert_eq!(std::fs::metadata(journal.appender.path()).unwrap().len(), 0);
    journal.append(b"*1\r\n$4\r\nPING\r\n").unwrap();
    journal.rewrite().unwrap();
    assert!(std::fs::metadata(journal.appender.path()).unwrap().len() > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replay_empty_file_succeeds_without_player_but_nonempty_file_requires_one() {
    let dir = temp_dir("replay");
    let mut journal = journal_in(&dir);

    journal.replay().unwrap();
    journal.append(b"*1\r\n$4\r\nPING\r\n").unwrap();

    let err = journal.replay().unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Other);
    assert!(err.to_string().contains("requires a player"));
    let _ = std::fs::remove_dir_all(&dir);
}
