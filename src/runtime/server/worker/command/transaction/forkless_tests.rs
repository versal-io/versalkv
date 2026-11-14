use super::{ExecForklessBarrier, ServerWorkerState};
use crate::engine::command::CommandArgs;
use crate::engine::extension::config::persistence::BgsaveMethod;
use crate::foundation::io::persistence::rdb::{RdbDecoder, RdbStep};
use crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::foundation::io::store::Robj;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::command::dispatch::CommandEffects;
use crate::runtime::server::worker::command::invocation::CommandTarget;
use crate::runtime::server::worker::command::outcome::CommandDisposition;
use bytes::Bytes;

struct CaptureFixture {
    backend: ServerWorkerState,
    dir: std::path::PathBuf,
}

impl CaptureFixture {
    fn new(name: &str, commands: Vec<CommandArgs>) -> Self {
        let mut backend = ServerWorkerState::new(3).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(7));
        connection.set_custom_reply_enabled(false);
        assert!(connection.transaction.begin(0, 0));
        for args in commands {
            assert!(connection.transaction.queue(1, args));
        }
        backend.connection_registry.insert(connection);
        for index in 0..256 {
            backend.ctx.db_mut(0).unwrap().set(
                format!("capture-{index}").into_bytes(),
                Robj::string_raw(Bytes::from_static(b"value")),
            );
        }
        backend.ctx.db_mut(0).unwrap().set(
            b"original".to_vec(),
            Robj::string_raw(Bytes::from_static(b"before-save")),
        );
        let dir = std::env::temp_dir().join(format!(
            "versal-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        backend.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        backend.ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;
        backend.ctx.config_mut().persistence.rdb_key_save_delay = 1_000;
        crate::framework::commands::server::save::start_bgsave(&mut backend.ctx).unwrap();
        Self { backend, dir }
    }
}

impl Drop for CaptureFixture {
    fn drop(&mut self) {
        self.backend.ctx.abort_forkless_save(0);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn command(parts: &[&[u8]]) -> CommandArgs {
    CommandArgs::from_parts(
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part))
            .collect(),
    )
}

#[test]
fn exec_resumes_after_target_capture_and_preserves_snapshot_values() {
    let mut fixture = CaptureFixture::new("forkless-exec-targets", Vec::new());
    let backend = &mut fixture.backend;
    // Startup pre-fills the serializer queue. Select a target that was not
    // handed off during that fill, so early priority capture is exercised.
    let original = (0..256)
        .map(|index| format!("capture-{index}").into_bytes())
        .find(|key| !backend.ctx.persistence().forkless_key_captured(0, key))
        .unwrap();
    for args in [
        command(&[b"SET", &original, b"updated"]),
        command(&[b"SET", b"new-key", b"created"]),
    ] {
        assert!(backend.connection_registry[Token(7)]
            .transaction
            .queue(3, args));
    }
    let target = CommandTarget::new(Token(7));
    let mut effects = CommandEffects::default();
    assert_eq!(
        backend.handle_exec(target, "exec", false, &mut effects),
        CommandDisposition::Blocked
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while !backend
        .ctx
        .persistence()
        .forkless_key_captured(0, &original)
        || !backend
            .ctx
            .persistence()
            .forkless_key_captured(0, b"new-key")
    {
        assert!(backend.ctx.persistence().forkless_capture_in_progress());
        assert!(
            std::time::Instant::now() < deadline,
            "target capture did not progress"
        );
        backend.ctx.drive_forkless_feed();
        std::thread::yield_now();
    }
    assert!(backend.ctx.persistence().forkless_capture_in_progress());
    assert_eq!(
        backend.handle_exec(target, "exec", false, &mut effects),
        CommandDisposition::Completed,
        "EXEC must resume after its target keys resolve, while unrelated capture continues"
    );
    assert_eq!(
        backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(&original)
            .unwrap()
            .as_bytes(),
        b"updated"
    );
    assert!(backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"new-key")
        .is_some());
    backend.ctx.persistence().set_forkless_key_delay(0);
    while backend.ctx.persistence().forkless_capture_in_progress() {
        backend.ctx.drive_forkless_feed();
    }
    assert!(matches!(
        backend.ctx.finish_primary_forkless_save_for_test(),
        Some(RdbSaveOutcome::Saved)
    ));
    let bytes = std::fs::read(fixture.dir.join("dump.rdb")).unwrap();
    let mut decoder = RdbDecoder::new(true);
    let mut restored = DbSet::new(3);
    decoder.feed(&bytes);
    loop {
        match decoder.step(&mut restored).unwrap() {
            RdbStep::RecordApplied => {}
            RdbStep::Finished(_) => break,
            RdbStep::NeedInput => panic!("complete snapshot requested more input"),
        }
    }
    assert_eq!(
        restored
            .get(0)
            .unwrap()
            .get_no_expire(&original)
            .unwrap()
            .as_bytes(),
        b"value"
    );
    assert!(restored.get(0).unwrap().get_no_expire(b"new-key").is_none());
}

#[test]
fn forkless_exec_tracks_valid_select_and_cross_database_targets() {
    use crate::framework::capabilities::connection::TransactionEnd;

    let mut fixture = CaptureFixture::new("forkless-exec-cross-db", Vec::new());
    let backend = &mut fixture.backend;
    let cases = [
        (
            vec![
                command(&[b"SELECT", b"1"]),
                command(&[b"SET", b"key", b"value"]),
            ],
            vec![(0, b"key".to_vec()), (1, b"key".to_vec())],
        ),
        (
            vec![
                command(&[b"SELECT", b"99"]),
                command(&[b"COPY", b"source", b"destination", b"DB", b"2"]),
            ],
            vec![(0, b"source".to_vec()), (2, b"destination".to_vec())],
        ),
        (
            vec![
                command(&[b"SELECT", b"1"]),
                command(&[b"MOVE", b"source", b"2"]),
            ],
            vec![
                (0, b"source".to_vec()),
                (1, b"source".to_vec()),
                (2, b"source".to_vec()),
            ],
        ),
    ];
    for (commands, mut expected) in cases {
        let transaction = &mut backend.connection_registry[Token(7)].transaction;
        transaction.end(TransactionEnd::Reset);
        assert!(transaction.begin(0, 0));
        for args in commands {
            assert!(transaction.queue(1, args));
        }
        let ExecForklessBarrier::WaitForKeys(mut actual) =
            backend.decide_exec_forkless_barrier(Token(7))
        else {
            panic!("cross-database writes must wait for their snapshot targets");
        };
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
}

#[test]
fn forkless_exec_allows_reads_and_keeps_script_writes_behind_the_barrier() {
    use crate::framework::capabilities::connection::TransactionEnd;

    let mut fixture = CaptureFixture::new(
        "forkless-exec-script",
        vec![command(&[b"GET", b"original"])],
    );
    assert!(matches!(
        fixture.backend.decide_exec_forkless_barrier(Token(7)),
        ExecForklessBarrier::Ready
    ));
    let transaction = &mut fixture.backend.connection_registry[Token(7)].transaction;
    transaction.end(TransactionEnd::Reset);
    assert!(transaction.begin(0, 0));
    assert!(transaction.queue(
        1,
        command(&[
            b"EVAL",
            b"return redis.call('SET', 'unlisted', 'value')",
            b"0"
        ])
    ));
    assert!(matches!(
        fixture.backend.decide_exec_forkless_barrier(Token(7)),
        ExecForklessBarrier::WaitForSnapshot
    ));
}

#[test]
fn forkless_exec_keeps_targets_when_acl_can_change_inside_the_transaction() {
    use crate::engine::extension::acl::{acl_users, AclUser};

    struct UserGuard(String);
    impl Drop for UserGuard {
        fn drop(&mut self) {
            acl_users().lock().unwrap().remove(&self.0);
        }
    }

    let username = "forkless-exec-changing-acl";
    let mut user = AclUser::new_disabled();
    user.flags = "on".to_string();
    user.root_selector.apply_rule("+@all").unwrap();
    user.root_selector.apply_rule("~*").unwrap();
    user.root_selector.apply_rule("-set").unwrap();
    acl_users()
        .lock()
        .unwrap()
        .insert(username.to_string(), user);
    let _guard = UserGuard(username.to_string());
    // These commands were queued while SET was allowed. It was revoked before
    // EXEC, and the first queued command restores it before the write executes.
    let mut fixture = CaptureFixture::new(
        "forkless-exec-changing-acl",
        vec![
            command(&[b"ACL", b"SETUSER", username.as_bytes(), b"+set"]),
            command(&[b"SET", b"new-key", b"value"]),
        ],
    );
    fixture.backend.connection_registry[Token(7)].username = username.to_string();

    let ExecForklessBarrier::WaitForKeys(keys) =
        fixture.backend.decide_exec_forkless_barrier(Token(7))
    else {
        panic!("writes allowed by an earlier ACL command still need snapshot protection");
    };
    assert_eq!(keys, vec![(0, b"new-key".to_vec())]);

    let transaction = &mut fixture.backend.connection_registry[Token(7)].transaction;
    transaction.end(crate::framework::capabilities::connection::TransactionEnd::Reset);
    assert!(transaction.begin(0, 0));
    for args in [
        command(&[b"ACL", b"SETUSER", username.as_bytes(), b"+set", b"-select"]),
        command(&[b"SELECT", b"1"]),
        command(&[b"SET", b"new-key", b"value"]),
    ] {
        assert!(transaction.queue(1, args));
    }
    let ExecForklessBarrier::WaitForKeys(mut keys) =
        fixture.backend.decide_exec_forkless_barrier(Token(7))
    else {
        panic!("a SELECT denied by an earlier ACL command must retain source protection");
    };
    keys.sort();
    assert_eq!(
        keys,
        vec![(0, b"new-key".to_vec()), (1, b"new-key".to_vec())]
    );
}
