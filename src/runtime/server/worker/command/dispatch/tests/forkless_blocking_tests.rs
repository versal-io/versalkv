use super::*;
use crate::engine::command::context::{BlockedState, CommandConn};
use crate::engine::extension::config::persistence::BgsaveMethod;
use crate::engine::extension::module::api::call::call_conn::ModuleCallConn;
use crate::engine::extension::module::api::call::pending::PendingModuleCall;
use crate::engine::extension::module::capability::ModuleRuntime;
use crate::engine::extension::module::registry::ModuleRef;
use crate::engine::persistence::ForklessProgressNotification;
use crate::foundation::io::persistence::rdb::{RdbDecoder, RdbStep};
use crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::framework::capabilities::module_call_snapshot::{
    plan_module_call_snapshot, prepare_module_call_snapshot, ModuleCallSnapshotPlan,
};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const SNAPSHOT_LISTS: usize = 1_024;

struct CaptureFixture {
    backend: ServerWorkerState,
    dir: PathBuf,
}

impl CaptureFixture {
    fn new(name: &str) -> Self {
        let mut backend = ServerWorkerState::new(1).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.set_command_registry_snapshot(&backend.registry);
        for index in 0..SNAPSHOT_LISTS {
            backend
                .ctx
                .db_mut(0)
                .unwrap()
                .get_or_create_list(format!("snapshot-list-{index}").as_bytes())
                .unwrap()
                .push_back(Bytes::from_static(b"before"));
        }
        let dir = std::env::temp_dir().join(format!(
            "versal-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config = &mut backend.ctx.config_mut().persistence;
        config.dir = dir.to_string_lossy().into_owned();
        config.dbfilename = "dump.rdb".to_string();
        config.bgsave_default_method = BgsaveMethod::Forkless;
        config.rdb_key_save_delay = 1_000;
        Self { backend, dir }
    }

    fn add_client(&mut self, token: Token) {
        let connection = Connection::new(token, ([127, 0, 0, 1], 0).into(), false);
        self.backend
            .connection_registry
            .register(connection)
            .unwrap();
    }

    fn block(&mut self, token: Token, args: &CommandArgs) -> BlockedState {
        let spec = self.backend.registry.lookup(&args.name).unwrap().clone();
        self.backend.ctx.begin_cmd();
        self.backend.connection_registry[token].begin_admitted_command(spec.name.as_str());
        // The source is absent, so this fixture execution cannot mutate data.
        // Complete through the real registration path, including its session
        // classification, without snapshot admission obscuring wakeup tests.
        let reply = self.backend.ctx.execute_command(
            &args.name,
            &mut self.backend.connection_registry[token],
            args,
        );
        let mut effects = CommandEffects::default();
        assert_eq!(
            self.backend.complete_command(
                CommandCompletion {
                    invocation: CommandInvocation::direct(
                        CommandTarget::new(token),
                        args,
                        spec.name.as_str(),
                        spec.flags,
                        spec.dispatch_group,
                        &[],
                    ),
                    reply,
                    status: CommandExecutionStatus::Executed,
                    duration_us: 0,
                    was_blocked_before: false,
                },
                None,
                &mut effects,
            ),
            CommandDisposition::Blocked
        );
        self.backend.connection_registry[token]
            .blocked
            .as_ref()
            .unwrap()
            .clone()
    }

    fn start(&mut self) {
        crate::framework::commands::server::save::start_bgsave(&mut self.backend.ctx).unwrap();
        assert!(self
            .backend
            .ctx
            .persistence()
            .forkless_capture_in_progress());
    }

    fn capture_targets(&mut self, keys: &[&[u8]]) {
        self.backend
            .ctx
            .prioritize_forkless_keys(0, keys.iter().map(|key| key.to_vec()));
        let deadline = Instant::now() + Duration::from_secs(3);
        while keys.iter().any(|key| {
            !self.backend.ctx.persistence().forkless_key_captured(0, key)
                || self.backend.ctx.persistence().forkless_key_blocked(0, key)
        }) {
            assert!(
                Instant::now() < deadline,
                "priority capture must make progress"
            );
            assert!(self
                .backend
                .ctx
                .persistence()
                .forkless_capture_in_progress());
            self.backend.ctx.drive_forkless_feed();
            thread::yield_now();
        }
        assert!(self
            .backend
            .ctx
            .persistence()
            .forkless_capture_in_progress());
    }

    fn admit(&mut self, token: Token, args: &CommandArgs) -> (CommandDisposition, CommandEffects) {
        let spec = self.backend.registry.lookup(&args.name).unwrap().clone();
        self.backend.ctx.begin_cmd();
        self.backend.connection_registry[token].begin_admitted_command(spec.name.as_str());
        let mut effects = CommandEffects::default();
        let disposition = self.backend.execute_command_and_handle_result(
            CommandInvocation::direct(
                CommandTarget::new(token),
                args,
                spec.name.as_str(),
                spec.flags,
                spec.dispatch_group,
                &[],
            ),
            None,
            None,
            &mut effects,
        );
        (disposition, effects)
    }

    fn push_without_waking(&mut self, source: &[u8], values: &[&[u8]]) {
        let mut parts = vec![b"RPUSH".as_slice(), source];
        parts.extend_from_slice(values);
        let args = make_args(&parts);
        let mut conn = ModuleCallConn::new(0);
        assert_eq!(
            self.backend
                .ctx
                .execute_command(&args.name, &mut conn, &args),
            Value::integer(values.len() as i64)
        );
    }

    fn process_source(&mut self, source: &[u8]) {
        self.backend
            .signal_keys_ready(&[(0, Bytes::copy_from_slice(source))]);
        self.backend.process_ready_keys();
    }

    fn resume_after_capture_release(&mut self, tokens: &[Token]) {
        self.backend.ctx.config_mut().persistence.rdb_key_save_delay = 1_000;
        self.backend.ctx.persistence().set_forkless_key_delay(1_000);
        let deadline = Instant::now() + Duration::from_secs(3);
        while tokens
            .iter()
            .any(|token| self.backend.connection_registry[*token].blocked.is_some())
        {
            assert!(
                Instant::now() < deadline,
                "capture release must requeue the deferred ready key without another writer"
            );
            self.backend.pump_forkless_capture();
            self.backend.process_ready_keys();
            thread::yield_now();
        }
        assert!(
            self.backend
                .ctx
                .persistence()
                .forkless_capture_in_progress(),
            "resolved waiters must run before unrelated snapshot capture finishes"
        );
    }

    fn finish_snapshot(&mut self) -> DbSet {
        self.backend.ctx.config_mut().persistence.rdb_key_save_delay = 0;
        self.backend.ctx.persistence().set_forkless_key_delay(0);
        let deadline = Instant::now() + Duration::from_secs(3);
        while self
            .backend
            .ctx
            .persistence()
            .forkless_capture_in_progress()
        {
            assert!(Instant::now() < deadline, "snapshot capture must finish");
            self.backend.pump_forkless_capture();
            self.backend.process_ready_keys();
            thread::yield_now();
        }
        assert!(matches!(
            self.backend.ctx.finish_primary_forkless_save_for_test(),
            Some(RdbSaveOutcome::Saved)
        ));
        self.reload_snapshot()
    }

    fn reload_snapshot(&self) -> DbSet {
        let bytes = std::fs::read(self.dir.join("dump.rdb")).unwrap();
        let mut restored = DbSet::new(1);
        let mut decoder = RdbDecoder::new(true);
        decoder.feed(&bytes);
        loop {
            match decoder.step(&mut restored).unwrap() {
                RdbStep::RecordApplied => {}
                RdbStep::Finished(_) => break,
                RdbStep::NeedInput => panic!("complete snapshot requested more input"),
                RdbStep::InlineCommand { .. } => panic!("snapshot has no inline commands"),
            }
        }
        assert_eq!(restored.get(0).unwrap().dbsize(), SNAPSHOT_LISTS);
        restored
    }
}

impl Drop for CaptureFixture {
    fn drop(&mut self) {
        self.backend.ctx.abort_forkless_save(0);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn assert_original_state(backend: &ServerWorkerState, token: Token, original: &BlockedState) {
    let blocked = backend.connection_registry[token]
        .blocked
        .as_ref()
        .expect("snapshot deferral must preserve the ordinary blocked command");
    assert_eq!(
        blocked.cmd, original.cmd,
        "do not wrap the waiter in Forkless"
    );
    assert_eq!(blocked.keys, original.keys);
    assert_eq!(blocked.start_time_ms, original.start_time_ms);
    assert_eq!(blocked.timeout_ms, original.timeout_ms);
    assert_eq!(blocked.req_bytes, original.req_bytes);
    assert_eq!(blocked.orig_args.name, original.orig_args.name);
    assert_eq!(blocked.orig_args.args, original.orig_args.args);
    assert!(blocked.orig_wrapped.is_none());
    assert!(backend
        .ctx
        .blocking_registry()
        .is_client_blocked(&backend.connection_registry[token].session_id()));
    assert!(backend.connection_registry[token].output.queue.is_empty());
}

fn list_values(dbs: &mut DbSet, key: &[u8]) -> Vec<Bytes> {
    dbs.get_mut(0)
        .unwrap()
        .get_list(key)
        .unwrap()
        .unwrap()
        .iter()
        .cloned()
        .collect()
}

fn prepare_module_progress_waiter(
    fixture: &mut CaptureFixture,
    waiter: Token,
) -> (Vec<u8>, BlockedState) {
    // Use the real scan order to make the ready source the first serialized
    // entry. Remove its bucket peers before registration, not during capture.
    let mut cursor = 0;
    let first_bucket = loop {
        let (next, keys) = fixture
            .backend
            .ctx
            .db(0)
            .unwrap()
            .snapshot_scan_bucket(cursor);
        if !keys.is_empty() {
            break keys;
        }
        assert_ne!(next, 0, "the snapshot fixture must contain a source");
        cursor = next;
    };
    let source = first_bucket[0].clone();
    for key in first_bucket {
        assert!(fixture.backend.ctx.db_mut(0).unwrap().del(&key));
    }
    fixture.add_client(waiter);
    let original = fixture.block(waiter, &make_args(&[b"BLPOP", &source, b"60"]));
    // A module can seed a blocked source and start BGSAVE before releasing
    // its lease. The first ready-key processing is deferred until below.
    fixture.push_without_waking(&source, &[b"before"]);
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 10_000_000;
    fixture.start();
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_delay_wait_for_test()
        != Some(10_000_000)
    {
        assert!(
            Instant::now() < deadline,
            "the serializer must publish its completion and enter the actual delay wait"
        );
        thread::yield_now();
    }
    let persistence = fixture.backend.ctx.persistence();
    assert!(persistence.forkless_key_captured(0, &source));
    assert!(
        persistence.forkless_key_blocked(0, &source),
        "the first completion must still await an owner-thread feed drive"
    );
    assert_eq!(persistence.forkless_progress().unwrap().processed, 0);
    fixture.process_source(&source);
    assert_original_state(&fixture.backend, waiter, &original);
    assert!(fixture
        .backend
        .ctx
        .blocking_registry()
        .is_forkless_ready_key_deferred(0, &source));
    assert!(fixture.backend.ready_keys.is_empty());
    assert!(!fixture.backend.ctx.has_forkless_waiters());
    (source, original)
}

fn assert_module_progress_requeues_on_next_turn(
    fixture: &mut CaptureFixture,
    waiter: Token,
    source: &[u8],
    original: &BlockedState,
) {
    let persistence = fixture.backend.ctx.persistence();
    assert!(persistence.forkless_capture_in_progress());
    let progress_before = persistence.forkless_progress().unwrap();
    assert_eq!(progress_before.processed, 0);
    assert!(progress_before.current_item_ms < 5_000);
    assert_eq!(
        persistence.forkless_key_delay_wait_for_test(),
        Some(10_000_000)
    );
    for (_, key) in super::super::forkless_blocked_key_targets(0, original) {
        assert!(persistence.forkless_key_captured(0, &key));
        assert!(!persistence.forkless_key_blocked(0, &key));
    }
    let shared_before: Vec<_> = (0..SNAPSHOT_LISTS)
        .map(|index| format!("snapshot-list-{index}").into_bytes())
        .filter(|key| persistence.forkless_key_blocked(0, key))
        .collect();
    assert_original_state(&fixture.backend, waiter, original);
    assert!(fixture
        .backend
        .ctx
        .blocking_registry()
        .is_forkless_ready_key_deferred(0, source));

    // No writer, extra ready signal, head exit, direct feed drain, or delay
    // adjustment may supply a replacement notification after module prepare.
    fixture.backend.pump_forkless_capture();
    let persistence = fixture.backend.ctx.persistence();
    let progress_after = persistence.forkless_progress().unwrap();
    assert_eq!(
        (
            progress_after.processed,
            progress_after.enqueued,
            progress_after.queue_length,
        ),
        (
            progress_before.processed,
            progress_before.enqueued,
            progress_before.queue_length,
        ),
        "maintenance must observe no new serializer or enqueue progress"
    );
    assert!(persistence.forkless_capture_in_progress());
    assert!(persistence.forkless_save_in_progress());
    assert_eq!(persistence.forkless_key_delay_for_test(), Some(10_000_000));
    assert_eq!(
        persistence.forkless_key_delay_wait_for_test(),
        Some(10_000_000)
    );
    assert_eq!(
        fixture.backend.ctx.config().persistence.rdb_key_save_delay,
        10_000_000
    );
    let shared_after: Vec<_> = (0..SNAPSHOT_LISTS)
        .map(|index| format!("snapshot-list-{index}").into_bytes())
        .filter(|key| persistence.forkless_key_blocked(0, key))
        .collect();
    assert_eq!(
        shared_after, shared_before,
        "maintenance must not get a new shared release that masks the lost module progress"
    );
    assert_original_state(&fixture.backend, waiter, original);
    assert_eq!(
        fixture.backend.ready_keys,
        vec![(0, Bytes::copy_from_slice(source))],
        "module feed progress must requeue the safe ordinary waiter on the next maintenance turn"
    );
    fixture.backend.process_ready_keys();
    assert!(fixture.backend.connection_registry[waiter]
        .blocked
        .is_none());
    let reply = Value::Array(vec![
        Value::bulk(Bytes::copy_from_slice(source)),
        Value::bulk(Bytes::from_static(b"before")),
    ]);
    let expected = crate::foundation::io::protocol::serializer::serialize_to_vec(&reply);
    assert_eq!(queued_flat_bytes(&fixture.backend, waiter), expected);
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    assert!(!fixture
        .backend
        .ctx
        .blocking_registry()
        .is_forkless_ready_key_deferred(0, source));
}

#[test]
fn module_preparation_shared_release_requeues_waiter_without_new_progress() {
    let mut fixture = CaptureFixture::new("forkless-module-shared-progress");
    let waiter = Token(41);
    let (source, original) = prepare_module_progress_waiter(&mut fixture, waiter);
    let args = make_args(&[b"RPUSH", b"module-unrelated", b"value"]);
    let command = fixture
        .backend
        .registry
        .lookup(&args.name)
        .unwrap()
        .descriptor();
    assert!(matches!(
        plan_module_call_snapshot(&fixture.backend.ctx, &command, &args, 0),
        ModuleCallSnapshotPlan::ResolveTargets(_)
    ));
    let _is_ready = prepare_module_call_snapshot(&mut fixture.backend.ctx, &command, &args, 0);
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_capture_in_progress());
    assert_eq!(
        fixture
            .backend
            .ctx
            .persistence()
            .forkless_capture_poll_delay(),
        Some(Duration::from_millis(1)),
        "the remaining feed must be backpressured throughout the serializer delay"
    );
    assert_module_progress_requeues_on_next_turn(&mut fixture, waiter, &source, &original);
}

#[test]
fn shared_barrier_completion_preserves_cross_key_wakeup_order() {
    let mut fixture = CaptureFixture::new("forkless-shared-cross-key-order");
    // The first entry publishes a completion before entering its delay.
    // Pick the following entries using the real scan, and keep their slots
    // intact so both remain blocked behind that actual serializer wait.
    let mut cursor = 0;
    let mut keys = Vec::new();
    while keys.len() < 3 {
        let (next, bucket) = fixture
            .backend
            .ctx
            .db(0)
            .unwrap()
            .snapshot_scan_bucket(cursor);
        keys.extend(bucket);
        assert!(
            next != 0 || keys.len() >= 3,
            "three scan entries are required"
        );
        cursor = next;
    }
    let source = &keys[1];
    let destination = &keys[2];
    for key in [source, destination] {
        let list = fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_or_create_list(key)
            .unwrap();
        assert_eq!(list.pop_front(), Some(Bytes::from_static(b"before")));
        assert!(list.is_empty());
    }
    let mover = Token(41);
    let popper = Token(42);
    fixture.add_client(mover);
    fixture.add_client(popper);
    let move_original = fixture.block(
        mover,
        &make_args(&[b"BLMOVE", source, destination, b"RIGHT", b"LEFT", b"60"]),
    );
    let pop_original = fixture.block(popper, &make_args(&[b"BLPOP", destination, b"60"]));
    // A single large y requires Shared handoff, not just the ordinary
    // scan's in-use marker for an independently copied small value.
    let destination_value = vec![b'y'; 64 * 1024 + 1];
    fixture.push_without_waking(source, &[b"x"]);
    fixture.push_without_waking(destination, &[&destination_value]);
    assert!(
        fixture
            .backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(destination)
            .unwrap()
            .estimated_size()
            > 64 * 1024
    );
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 10_000_000;
    fixture.start();
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_delay_wait_for_test()
        != Some(10_000_000)
    {
        assert!(
            Instant::now() < deadline,
            "serializer must enter its actual long key-delay wait"
        );
        thread::yield_now();
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        fixture.backend.pump_forkless_capture();
        let persistence = fixture.backend.ctx.persistence();
        if [source, destination]
            .iter()
            .all(|key| persistence.forkless_key_captured(0, key))
            && persistence.forkless_capture_poll_delay() == Some(Duration::from_millis(1))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "both targets must be captured while the feed is backpressured"
        );
        thread::yield_now();
    }
    for key in [source, destination] {
        assert!(fixture
            .backend
            .ctx
            .persistence()
            .forkless_key_blocked(0, key));
    }
    assert_eq!(
        fixture
            .backend
            .ctx
            .persistence()
            .forkless_key_delay_wait_for_test(),
        Some(10_000_000)
    );
    assert_eq!(
        fixture
            .backend
            .ctx
            .persistence_mut()
            .take_forkless_progress_notification(),
        ForklessProgressNotification::Unchanged
    );
    // The existing ready-key stack visits b before a. Both real waiters
    // defer, and snapshot completion must preserve that encounter order.
    fixture.backend.signal_keys_ready(&[
        (0, Bytes::copy_from_slice(source)),
        (0, Bytes::copy_from_slice(destination)),
    ]);
    fixture.backend.process_ready_keys();
    assert_original_state(&fixture.backend, mover, &move_original);
    assert_original_state(&fixture.backend, popper, &pop_original);
    for key in [source, destination] {
        assert!(fixture
            .backend
            .ctx
            .blocking_registry()
            .is_forkless_ready_key_deferred(0, key));
    }
    assert!(fixture.backend.ready_keys.is_empty());
    assert!(!fixture.backend.ctx.has_forkless_waiters());

    // Finish real serialization without consuming ready keys between partial
    // completions. The save's terminal notification releases the batch.
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 0;
    fixture.backend.ctx.persistence().set_forkless_key_delay(0);
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_capture_in_progress()
    {
        assert!(Instant::now() < deadline, "snapshot feed must finish");
        fixture.backend.ctx.drive_forkless_feed();
        thread::yield_now();
    }
    assert!(matches!(
        fixture.backend.ctx.finish_primary_forkless_save_for_test(),
        Some(RdbSaveOutcome::Saved)
    ));
    assert_original_state(&fixture.backend, mover, &move_original);
    assert_original_state(&fixture.backend, popper, &pop_original);
    fixture.backend.pump_forkless_capture();
    fixture.backend.process_ready_keys();
    assert!(fixture.backend.connection_registry[mover].blocked.is_none());
    assert!(fixture.backend.connection_registry[popper]
        .blocked
        .is_none());
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    assert!(fixture.backend.ready_keys.is_empty());
    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(source)
        .is_none());
    let destination_values = fixture
        .backend
        .ctx
        .db_mut(0)
        .unwrap()
        .get_list(destination)
        .unwrap()
        .unwrap()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut restored = fixture.reload_snapshot();
    assert_eq!(
        list_values(&mut restored, source),
        vec![Bytes::from_static(b"x")]
    );
    assert_eq!(
        list_values(&mut restored, destination),
        vec![Bytes::copy_from_slice(&destination_value)]
    );
    let pop_reply = Value::Array(vec![
        Value::bulk(Bytes::copy_from_slice(destination)),
        Value::bulk(Bytes::from(destination_value)),
    ]);
    let expected_pop_reply =
        crate::foundation::io::protocol::serializer::serialize_to_vec(&pop_reply);
    // Compare complete bytes and values, but keep a RED failure bounded
    // instead of printing the large Shared payload.
    assert_eq!(
        (
            queued_flat_bytes(&fixture.backend, popper) == expected_pop_reply,
            queued_flat_bytes(&fixture.backend, mover),
            destination_values == vec![Bytes::from_static(b"x")],
        ),
        (true, b"$1\r\nx\r\n".to_vec(), true),
        "the pop must consume the original y before the move inserts x"
    );
}

fn assert_shared_completion_ready_overlap(overlap: &str) {
    let mut fixture = CaptureFixture::new("forkless-shared-ready-overlap");
    let mut cursor = 0;
    let mut keys = Vec::new();
    while keys.len() < 3 {
        let (next, bucket) = fixture
            .backend
            .ctx
            .db(0)
            .unwrap()
            .snapshot_scan_bucket(cursor);
        keys.extend(bucket);
        assert!(next != 0 || keys.len() >= 3);
        cursor = next;
    }
    let source = &keys[1];
    let destination = &keys[2];
    for key in [source, destination] {
        let list = fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_or_create_list(key)
            .unwrap();
        assert_eq!(list.pop_front(), Some(Bytes::from_static(b"before")));
        assert!(list.is_empty());
    }
    let mover = Token(41);
    let popper = Token(42);
    let destination_writer = Token(43);
    let source_writer = Token(44);
    for token in [mover, popper, destination_writer, source_writer] {
        fixture.add_client(token);
    }
    let move_original = fixture.block(
        mover,
        &make_args(&[b"BLMOVE", source, destination, b"RIGHT", b"LEFT", b"60"]),
    );
    let pop_original = fixture.block(popper, &make_args(&[b"BLPOP", destination, b"60"]));
    let destination_value = vec![b'y'; 64 * 1024 + 1];
    fixture.push_without_waking(source, &[b"x"]);
    fixture.push_without_waking(destination, &[&destination_value]);
    assert!(
        fixture
            .backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(destination)
            .unwrap()
            .estimated_size()
            > 64 * 1024
    );
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 10_000_000;
    fixture.start();
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_delay_wait_for_test()
        != Some(10_000_000)
    {
        assert!(
            Instant::now() < deadline,
            "serializer must enter its actual delay"
        );
        thread::yield_now();
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        fixture.backend.pump_forkless_capture();
        let persistence = fixture.backend.ctx.persistence();
        if [source, destination]
            .iter()
            .all(|key| persistence.forkless_key_captured(0, key))
            && persistence.forkless_capture_poll_delay() == Some(Duration::from_millis(1))
        {
            break;
        }
        assert!(Instant::now() < deadline, "target capture must finish");
        thread::yield_now();
    }
    for key in [source, destination] {
        assert!(fixture
            .backend
            .ctx
            .persistence()
            .forkless_key_blocked(0, key));
    }
    assert_eq!(
        fixture
            .backend
            .ctx
            .persistence_mut()
            .take_forkless_progress_notification(),
        ForklessProgressNotification::Unchanged
    );
    fixture.backend.signal_keys_ready(&[
        (0, Bytes::copy_from_slice(source)),
        (0, Bytes::copy_from_slice(destination)),
    ]);
    fixture.backend.process_ready_keys();
    assert_original_state(&fixture.backend, mover, &move_original);
    assert_original_state(&fixture.backend, popper, &pop_original);
    for key in [destination, source] {
        assert!(fixture
            .backend
            .ctx
            .blocking_registry()
            .is_forkless_ready_key_deferred(0, key));
    }
    assert!(fixture.backend.ready_keys.is_empty());

    // The ordinary admission path postpones actual writers on the captured
    // targets. Maintenance must merge their dirty keys with the retained
    // destination-before-source wakeup order after real serialization.
    let writes_destination = matches!(overlap, "first" | "multiple");
    let writes_source = matches!(overlap, "last" | "multiple");
    for (should_write, token, args) in [
        (
            writes_destination,
            destination_writer,
            make_args(&[b"RPUSH", destination, b"z"]),
        ),
        (
            writes_source,
            source_writer,
            make_args(&[b"LPUSH", source, b"w"]),
        ),
    ] {
        if !should_write {
            continue;
        }
        let (disposition, effects) = fixture.admit(token, &args);
        assert_eq!(disposition, CommandDisposition::Blocked);
        assert!(effects.replies.is_empty());
        let blocked = fixture.backend.connection_registry[token]
            .blocked
            .as_ref()
            .unwrap();
        assert_eq!(blocked.cmd, BlockedCmd::Forkless);
        assert_eq!(blocked.orig_args.args, args.args);
        assert!(queued_flat_bytes(&fixture.backend, token).is_empty());
    }
    assert!(fixture.backend.ctx.has_forkless_waiters());
    assert!(fixture.backend.ready_keys.is_empty());
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 0;
    fixture.backend.ctx.persistence().set_forkless_key_delay(0);
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_capture_in_progress()
    {
        assert!(Instant::now() < deadline, "snapshot feed must finish");
        fixture.backend.ctx.drive_forkless_feed();
        thread::yield_now();
    }
    assert!(matches!(
        fixture.backend.ctx.finish_primary_forkless_save_for_test(),
        Some(RdbSaveOutcome::Saved)
    ));
    let mut restored = fixture.reload_snapshot();
    assert_eq!(
        list_values(&mut restored, source),
        vec![Bytes::from_static(b"x")]
    );
    assert_eq!(
        list_values(&mut restored, destination),
        vec![Bytes::copy_from_slice(&destination_value)]
    );
    assert_original_state(&fixture.backend, mover, &move_original);
    assert_original_state(&fixture.backend, popper, &pop_original);
    // Only this maintenance call releases the postponed writers and re-arms
    // the retained batch. No replacement ready signals are added by the test.
    fixture.backend.pump_forkless_capture();
    for (should_write, token) in [
        (writes_destination, destination_writer),
        (writes_source, source_writer),
    ] {
        assert!(fixture.backend.connection_registry[token].blocked.is_none());
        assert_eq!(
            queued_flat_bytes(&fixture.backend, token),
            if should_write {
                b":2\r\n".as_slice()
            } else {
                b""
            }
        );
    }
    fixture.backend.process_ready_keys();
    assert!(fixture.backend.connection_registry[mover].blocked.is_none());
    assert!(fixture.backend.connection_registry[popper]
        .blocked
        .is_none());
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    assert!(fixture.backend.ready_keys.is_empty());
    assert!(fixture.backend.ready_keys_set.is_empty());
    assert!(!fixture.backend.ctx.has_forkless_waiters());
    for key in [source, destination] {
        assert!(!fixture
            .backend
            .ctx
            .blocking_registry()
            .is_forkless_ready_key_deferred(0, key));
    }
    if writes_source {
        assert_eq!(
            fixture
                .backend
                .ctx
                .db_mut(0)
                .unwrap()
                .get_list(source)
                .unwrap()
                .unwrap()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![Bytes::from_static(b"w")]
        );
    } else {
        assert!(fixture
            .backend
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(source)
            .is_none());
    }
    let destination_values = fixture
        .backend
        .ctx
        .db_mut(0)
        .unwrap()
        .get_list(destination)
        .unwrap()
        .unwrap()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut expected_destination = vec![Bytes::from_static(b"x")];
    if writes_destination {
        expected_destination.push(Bytes::from_static(b"z"));
    }
    let pop_reply = Value::Array(vec![
        Value::bulk(Bytes::copy_from_slice(destination)),
        Value::bulk(Bytes::from(destination_value)),
    ]);
    let expected_pop_reply =
        crate::foundation::io::protocol::serializer::serialize_to_vec(&pop_reply);
    assert_eq!(
        (
            queued_flat_bytes(&fixture.backend, popper) == expected_pop_reply,
            queued_flat_bytes(&fixture.backend, mover),
            destination_values == expected_destination,
        ),
        (true, b"$1\r\nx\r\n".to_vec(), true),
        "{overlap}: the pop must consume original y before the move inserts x despite restored writers"
    );
}

#[test]
fn shared_completion_ready_overlap_at_first_retained_key_preserves_wakeup_order() {
    assert_shared_completion_ready_overlap("first");
}

#[test]
fn shared_completion_ready_overlap_at_last_retained_key_preserves_wakeup_order() {
    assert_shared_completion_ready_overlap("last");
}

#[test]
fn shared_completion_ready_overlap_at_multiple_retained_keys_preserves_wakeup_order() {
    assert_shared_completion_ready_overlap("multiple");
}

#[test]
fn forkless_ready_overlap_preserves_unselected_key_wakeup_order() {
    use crate::engine::state::blocking_registry::ForklessReadySelection;

    for selection in [
        ForklessReadySelection::All,
        ForklessReadySelection::HeadRechecks,
    ] {
        let mut fixture = CaptureFixture::new("forkless-ready-unselected-order");
        let mover = Token(41);
        let popper = Token(42);
        let first = Token(43);
        let last = Token(44);
        for token in [mover, popper, first, last] {
            fixture.add_client(token);
        }
        fixture.block(
            mover,
            &make_args(&[
                b"BLMOVE",
                b"outside-source",
                b"outside-destination",
                b"RIGHT",
                b"LEFT",
                b"60",
            ]),
        );
        fixture.block(
            popper,
            &make_args(&[b"BLPOP", b"outside-destination", b"60"]),
        );
        fixture.block(first, &make_args(&[b"BLPOP", b"batch-first", b"60"]));
        fixture.block(last, &make_args(&[b"BLPOP", b"batch-last", b"60"]));
        for (key, value) in [
            (b"outside-source".as_slice(), b"x".as_slice()),
            (b"outside-destination", b"y"),
            (b"batch-first", b"p"),
            (b"batch-last", b"q"),
        ] {
            fixture.push_without_waking(key, &[value]);
        }
        // Exercise the recovery merge contract independently of completion.
        // The real Shared completion cases above supply its production path.
        for key in [b"batch-first".as_slice(), b"batch-last"] {
            fixture
                .backend
                .ctx
                .blocking_registry_mut()
                .defer_forkless_ready_key(0, key);
        }
        let existing = [
            (0, Bytes::from_static(b"outside-source")),
            (0, Bytes::from_static(b"batch-first")),
            (0, Bytes::from_static(b"outside-destination")),
            (0, Bytes::from_static(b"batch-last")),
        ];
        fixture.backend.signal_keys_ready(&existing);
        // HeadRechecks selects nothing here. Both a nonempty merge and an
        // empty selection must leave outside-destination ahead of its mover.
        fixture.backend.requeue_forkless_ready_keys(selection);
        fixture.backend.signal_keys_ready(&existing);
        fixture.backend.process_ready_keys();
        for (token, reply) in [
            (mover, b"$1\r\nx\r\n".as_slice()),
            (popper, b"*2\r\n$19\r\noutside-destination\r\n$1\r\ny\r\n"),
        ] {
            assert!(fixture.backend.connection_registry[token].blocked.is_none());
            assert_eq!(queued_flat_bytes(&fixture.backend, token), reply);
        }
        if matches!(selection, ForklessReadySelection::HeadRechecks) {
            for token in [first, last] {
                assert!(fixture.backend.connection_registry[token].blocked.is_some());
                assert!(queued_flat_bytes(&fixture.backend, token).is_empty());
            }
            assert_eq!(fixture.backend.ctx.blocking_registry().count(), 2);
            for key in [b"batch-first".as_slice(), b"batch-last"] {
                assert!(fixture
                    .backend
                    .ctx
                    .blocking_registry()
                    .is_forkless_ready_key_deferred(0, key));
            }
        }
        fixture
            .backend
            .requeue_forkless_ready_keys(ForklessReadySelection::All);
        fixture.backend.process_ready_keys();
        for (token, reply) in [
            (first, b"*2\r\n$11\r\nbatch-first\r\n$1\r\np\r\n".as_slice()),
            (last, b"*2\r\n$10\r\nbatch-last\r\n$1\r\nq\r\n"),
        ] {
            assert!(fixture.backend.connection_registry[token].blocked.is_none());
            assert_eq!(queued_flat_bytes(&fixture.backend, token), reply);
        }
        assert_eq!(
            fixture
                .backend
                .ctx
                .db_mut(0)
                .unwrap()
                .get_list(b"outside-destination")
                .unwrap()
                .unwrap()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![Bytes::from_static(b"x")]
        );
        assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
        assert!(fixture.backend.ready_keys.is_empty());
        assert!(fixture.backend.ready_keys_set.is_empty());
        for key in [b"batch-first".as_slice(), b"batch-last"] {
            assert!(!fixture
                .backend
                .ctx
                .blocking_registry()
                .is_forkless_ready_key_deferred(0, key));
        }
        fixture.backend.process_ready_keys();
        assert_eq!(
            queued_flat_bytes(&fixture.backend, popper),
            b"*2\r\n$19\r\noutside-destination\r\n$1\r\ny\r\n"
        );
    }
}

#[test]
fn ordinary_move_waiter_allows_rpush_before_full_capture() {
    for blocked in [
        make_args(&[
            b"BLMOVE",
            b"source",
            b"destination",
            b"RIGHT",
            b"LEFT",
            b"60",
        ]),
        make_args(&[b"BRPOPLPUSH", b"source", b"destination", b"60"]),
    ] {
        let mut fixture = CaptureFixture::new("forkless-ordinary-writer");
        let waiter = Token(41);
        let writer = Token(42);
        fixture.add_client(waiter);
        fixture.add_client(writer);
        let original = fixture.block(waiter, &blocked);
        fixture.start();
        fixture.capture_targets(&[b"source"]);
        assert!(!fixture
            .backend
            .ctx
            .persistence()
            .forkless_key_captured(0, b"destination"));

        let (disposition, effects) =
            fixture.admit(writer, &make_args(&[b"RPUSH", b"source", b"value"]));
        assert_eq!(
            disposition,
            CommandDisposition::Completed,
            "an ordinary waiter must not turn RPUSH into a full-capture barrier"
        );
        assert_eq!(effects.replies, vec![b":1\r\n".to_vec()]);
        assert!(fixture
            .backend
            .ctx
            .persistence()
            .forkless_capture_in_progress());
        assert_original_state(&fixture.backend, waiter, &original);
    }
}

#[test]
fn uncaptured_move_destination_preserves_original_fifo_and_timeout() {
    let mut fixture = CaptureFixture::new("forkless-wakeup-fifo");
    let head = Token(41);
    let follower = Token(42);
    fixture.add_client(head);
    fixture.add_client(follower);
    let original = fixture.block(
        head,
        &make_args(&[
            b"BLMOVE",
            b"source",
            b"destination",
            b"RIGHT",
            b"LEFT",
            b"60",
        ]),
    );
    let follower_original = fixture.block(follower, &make_args(&[b"BLPOP", b"source", b"60"]));
    let queue = fixture
        .backend
        .ctx
        .blocking_registry()
        .waiters_for_key(0, b"source");
    fixture.start();
    fixture.capture_targets(&[b"source"]);
    fixture.push_without_waking(b"source", &[b"first", b"second"]);
    fixture.process_source(b"source");

    assert_original_state(&fixture.backend, head, &original);
    assert_original_state(&fixture.backend, follower, &follower_original);
    assert_eq!(
        fixture
            .backend
            .ctx
            .blocking_registry()
            .waiters_for_key(0, b"source"),
        queue
    );
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 2);
    assert_eq!(
        fixture.backend.next_blocked_timeout(original.start_time_ms),
        Some(Duration::from_secs(60)),
        "capture deferral must not restart or hide the original deadline"
    );
    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"destination")
        .is_none());
    fixture.backend.process_ready_keys();
    assert_original_state(&fixture.backend, head, &original);

    fixture.resume_after_capture_release(&[head, follower]);
    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"source")
        .is_none());
    assert_eq!(
        fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_list(b"destination")
            .unwrap()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Bytes::from_static(b"second")]
    );
    assert_eq!(
        queued_flat_bytes(&fixture.backend, head),
        b"$6\r\nsecond\r\n"
    );
    assert_eq!(
        queued_flat_bytes(&fixture.backend, follower),
        b"*2\r\n$6\r\nsource\r\n$5\r\nfirst\r\n"
    );
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    let restored = fixture.finish_snapshot();
    for key in [b"source".as_slice(), b"destination"] {
        assert!(restored.get(0).unwrap().get_no_expire(key).is_none());
    }
}

#[test]
fn shared_move_destination_waits_for_release_and_keeps_snapshot_value() {
    let mut fixture = CaptureFixture::new("forkless-shared-destination");
    let waiter = Token(41);
    fixture.add_client(waiter);
    fixture.start();
    fixture.capture_targets(&[b"source"]);
    fixture
        .backend
        .ctx
        .persistence()
        .set_forkless_key_delay(10_000_000);
    let destination = (0..SNAPSHOT_LISTS)
        .map(|index| format!("snapshot-list-{index}").into_bytes())
        .find(|key| {
            fixture
                .backend
                .ctx
                .persistence()
                .forkless_key_captured(0, key)
                && fixture
                    .backend
                    .ctx
                    .persistence()
                    .forkless_key_blocked(0, key)
        })
        .expect("the serializer queue must retain a captured shared list");
    let original = fixture.block(
        waiter,
        &make_args(&[b"BRPOPLPUSH", b"source", &destination, b"60"]),
    );
    fixture.push_without_waking(b"source", &[b"after"]);
    fixture.process_source(b"source");

    assert_original_state(&fixture.backend, waiter, &original);
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_blocked(0, &destination));
    assert_eq!(
        fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_list(&destination)
            .unwrap()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Bytes::from_static(b"before")]
    );
    fixture.resume_after_capture_release(&[waiter]);
    assert!(!fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_blocked(0, &destination));
    assert_eq!(
        fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_list(&destination)
            .unwrap()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Bytes::from_static(b"after"), Bytes::from_static(b"before")]
    );
    let mut restored = fixture.finish_snapshot();
    assert_eq!(
        list_values(&mut restored, &destination),
        vec![Bytes::from_static(b"before")]
    );
    assert!(restored.get(0).unwrap().get_no_expire(b"source").is_none());
}

#[test]
fn cascade_protects_each_wakeup_without_capturing_the_full_target_graph() {
    let mut fixture = CaptureFixture::new("forkless-progressive-cascade");
    let first = Token(41);
    let second = Token(42);
    fixture.add_client(first);
    fixture.add_client(second);
    fixture.block(
        first,
        &make_args(&[b"BLMOVE", b"source", b"middle", b"RIGHT", b"LEFT", b"60"]),
    );
    let second_original = fixture.block(
        second,
        &make_args(&[b"BLMOVE", b"middle", b"last", b"RIGHT", b"LEFT", b"60"]),
    );
    fixture.start();
    fixture.capture_targets(&[b"source", b"middle"]);
    assert!(!fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_captured(0, b"last"));
    fixture.push_without_waking(b"source", &[b"value"]);
    fixture.process_source(b"source");

    assert!(
        fixture.backend.connection_registry[first].blocked.is_none(),
        "the first move needs only its own resolved source and destination"
    );
    assert_original_state(&fixture.backend, second, &second_original);
    assert!(!fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_captured(0, b"last"));
    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"source")
        .is_none());
    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"last")
        .is_none());
    fixture.resume_after_capture_release(&[second]);
    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"middle")
        .is_none());
    assert_eq!(
        queued_flat_bytes(&fixture.backend, first),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(
        queued_flat_bytes(&fixture.backend, second),
        b"$5\r\nvalue\r\n"
    );
    let restored = fixture.finish_snapshot();
    for key in [b"source".as_slice(), b"middle", b"last"] {
        assert!(restored.get(0).unwrap().get_no_expire(key).is_none());
    }
}

#[test]
fn multikey_waiter_protects_all_normalized_sources_before_wakeup() {
    let mut fixture = CaptureFixture::new("forkless-multikey-wakeup");
    let waiter = Token(41);
    fixture.add_client(waiter);
    let original = fixture.block(
        waiter,
        &make_args(&[b"BLPOP", b"other-source", b"source", b"60"]),
    );
    fixture.start();
    fixture.capture_targets(&[b"source"]);
    fixture.push_without_waking(b"source", &[b"value"]);
    fixture.process_source(b"source");

    assert_original_state(&fixture.backend, waiter, &original);
    assert_eq!(
        fixture
            .backend
            .ctx
            .blocking_registry()
            .waiters_for_key(0, b"other-source"),
        vec![fixture.backend.connection_registry[waiter].session_id()]
    );
    fixture.resume_after_capture_release(&[waiter]);
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_captured(0, b"other-source"));
    assert_eq!(
        queued_flat_bytes(&fixture.backend, waiter),
        b"*2\r\n$6\r\nsource\r\n$5\r\nvalue\r\n"
    );
    let restored = fixture.finish_snapshot();
    for key in [b"source".as_slice(), b"other-source"] {
        assert!(restored.get(0).unwrap().get_no_expire(key).is_none());
    }
}

#[test]
fn deferred_waiter_times_out_and_does_not_leave_a_protected_session_marker() {
    let mut fixture = CaptureFixture::new("forkless-wakeup-timeout");
    let waiter = Token(41);
    fixture.add_client(waiter);
    fixture.block(
        waiter,
        &make_args(&[b"BRPOPLPUSH", b"source", b"destination", b"60"]),
    );
    fixture.backend.connection_registry[waiter]
        .blocked
        .as_mut()
        .unwrap()
        .start_time_ms = crate::foundation::io::sys::time::mstime() - 60_001;
    let original = fixture.backend.connection_registry[waiter]
        .blocked
        .as_ref()
        .unwrap()
        .clone();
    fixture.start();
    fixture.capture_targets(&[b"source"]);
    fixture.push_without_waking(b"source", &[b"value"]);
    fixture.process_source(b"source");

    assert_original_state(&fixture.backend, waiter, &original);
    assert_eq!(
        fixture
            .backend
            .next_blocked_timeout(crate::foundation::io::sys::time::mstime()),
        Some(Duration::ZERO)
    );
    fixture.backend.check_blocked_timeouts();
    assert!(fixture.backend.connection_registry[waiter]
        .blocked
        .is_none());
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    assert_eq!(queued_flat_bytes(&fixture.backend, waiter), b"*-1\r\n");
    assert!(fixture
        .backend
        .ctx
        .db(0)
        .unwrap()
        .get_no_expire(b"destination")
        .is_none());

    // An unclassified registration for the same session must not inherit the
    // ordinary waiter's marker after its terminal timeout.
    let session = fixture.backend.connection_registry[waiter].session_id();
    fixture.backend.ctx.blocking_registry_mut().register_client(
        0,
        &[Bytes::from_static(b"source")],
        session,
    );
    let writer = make_args(&[b"RPUSH", b"source", b"next"]);
    let spec = fixture.backend.registry.lookup(&writer.name).unwrap();
    let targets = ServerWorkerState::forkless_key_targets(&meta_from_spec(spec), &writer, 0);
    assert!(fixture.backend.forkless_command_needs_full_capture(
        &meta_from_spec(spec),
        &writer,
        &targets,
    ));
}

#[test]
fn cancelled_snapshot_requeues_ordinary_waiter_on_the_next_turn() {
    let mut fixture = CaptureFixture::new("forkless-wakeup-cancel");
    let waiter = Token(41);
    fixture.add_client(waiter);
    let original = fixture.block(
        waiter,
        &make_args(&[b"BRPOPLPUSH", b"source", b"destination", b"60"]),
    );
    fixture.start();
    fixture.capture_targets(&[b"source"]);
    fixture.push_without_waking(b"source", &[b"value"]);
    fixture.process_source(b"source");
    assert_original_state(&fixture.backend, waiter, &original);
    assert!(!fixture.backend.ctx.has_forkless_waiters());
    assert!(fixture
        .backend
        .ctx
        .blocking_registry()
        .is_forkless_ready_key_deferred(0, b"source"));

    fixture.backend.ctx.abort_forkless_save(0);
    fixture.backend.pump_forkless_capture();
    assert_original_state(&fixture.backend, waiter, &original);
    assert_eq!(
        fixture.backend.ready_keys,
        vec![(0, Bytes::from_static(b"source"))],
        "maintenance must signal, not execute, the ordinary waiter"
    );
    fixture.backend.process_ready_keys();
    assert!(fixture.backend.connection_registry[waiter]
        .blocked
        .is_none());
    assert_eq!(
        queued_flat_bytes(&fixture.backend, waiter),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    assert!(!fixture
        .backend
        .ctx
        .blocking_registry()
        .is_forkless_ready_key_deferred(0, b"source"));
}

#[test]
fn shared_tail_completion_requeues_waiter_after_capture_feed_finishes() {
    let mut fixture = CaptureFixture::new("forkless-wakeup-shared-tail");
    let waiter = Token(41);
    fixture.add_client(waiter);
    fixture.start();
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_capture_in_progress()
    {
        assert!(Instant::now() < deadline, "capture feed must reach EOF");
        fixture.backend.pump_forkless_capture();
        thread::yield_now();
    }
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 10_000_000;
    fixture
        .backend
        .ctx
        .persistence()
        .set_forkless_key_delay(10_000_000);
    let destination = (0..SNAPSHOT_LISTS)
        .map(|index| format!("snapshot-list-{index}").into_bytes())
        .find(|key| {
            fixture
                .backend
                .ctx
                .persistence()
                .forkless_key_blocked(0, key)
        })
        .expect("finished capture must retain queued shared tail entries");
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_save_in_progress());
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_captured(0, &destination));
    let original = fixture.block(
        waiter,
        &make_args(&[b"BRPOPLPUSH", b"source", &destination, b"60"]),
    );
    fixture.push_without_waking(b"source", &[b"after"]);
    fixture.process_source(b"source");
    assert_original_state(&fixture.backend, waiter, &original);
    assert!(!fixture.backend.ctx.has_forkless_waiters());

    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 0;
    fixture.backend.ctx.persistence().set_forkless_key_delay(0);
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture.backend.connection_registry[waiter]
        .blocked
        .is_some()
    {
        assert!(
            Instant::now() < deadline,
            "tail completion must re-arm its ready key"
        );
        fixture.backend.pump_forkless_capture();
        fixture.backend.process_ready_keys();
        thread::yield_now();
    }
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_save_in_progress());
    assert!(!fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_blocked(0, &destination));
    assert_eq!(
        queued_flat_bytes(&fixture.backend, waiter),
        b"$5\r\nafter\r\n"
    );
    let mut restored = fixture.finish_snapshot();
    assert_eq!(
        list_values(&mut restored, &destination),
        vec![Bytes::from_static(b"before")]
    );
    assert!(restored.get(0).unwrap().get_no_expire(b"source").is_none());
}

#[test]
fn ordinary_list_zset_and_stream_waiters_share_target_scoped_classification() {
    for (blocked, writer) in [
        (
            make_args(&[b"BLPOP", b"source", b"60"]),
            make_args(&[b"RPUSH", b"source", b"value"]),
        ),
        (
            make_args(&[b"BZPOPMIN", b"source", b"60"]),
            make_args(&[b"ZADD", b"source", b"1", b"member"]),
        ),
        (
            make_args(&[b"XREAD", b"BLOCK", b"60000", b"STREAMS", b"source", b"$"]),
            make_args(&[b"XADD", b"source", b"*", b"field", b"value"]),
        ),
    ] {
        let mut fixture = CaptureFixture::new("forkless-known-waiter-classification");
        let waiter = Token(41);
        let executor = Token(42);
        fixture.add_client(waiter);
        fixture.add_client(executor);
        fixture.block(waiter, &blocked);
        fixture.start();
        fixture.capture_targets(&[b"source"]);
        let spec = fixture
            .backend
            .registry
            .lookup(&writer.name)
            .unwrap()
            .clone();
        assert_eq!(
            fixture
                .backend
                .pre_dispatch_exec(executor.0, meta_from_spec(&spec), &writer)
                .action,
            DispatchAction::Execute,
            "direct admission must recognize an ordinary protected waiter"
        );
        assert!(
            matches!(
                plan_module_call_snapshot(&fixture.backend.ctx, &spec.descriptor(), &writer, 0),
                ModuleCallSnapshotPlan::Ready
            ),
            "module-call classification must agree with direct admission"
        );
        assert!(fixture.backend.connection_registry[executor]
            .transaction
            .begin(0, 0));
        assert!(fixture.backend.connection_registry[executor]
            .transaction
            .queue(1, writer));
        let mut effects = CommandEffects::default();
        assert_eq!(
            fixture
                .backend
                .handle_exec(CommandTarget::new(executor), "exec", false, &mut effects),
            CommandDisposition::Completed,
            "EXEC must not wait for the whole snapshot because of an ordinary waiter"
        );
        assert!(fixture
            .backend
            .ctx
            .persistence()
            .forkless_capture_in_progress());
    }
}

#[test]
fn module_unknown_and_loaded_module_waiters_keep_global_fallback() {
    for fallback in ["module", "unknown", "loaded"] {
        let mut fixture = CaptureFixture::new("forkless-waiter-fallback");
        let waiter = Token(41);
        let executor = Token(42);
        fixture.add_client(waiter);
        fixture.add_client(executor);
        match fallback {
            "module" => {
                let args = make_args(&[b"BLPOP", b"source", b"60"]);
                let mut conn = ModuleCallConn::new(0);
                conn.set_deny_blocking(false);
                fixture
                    .backend
                    .ctx
                    .execute_command(&args.name, &mut conn, &args);
                assert!(conn.blocked().is_some());
                let command = fixture
                    .backend
                    .registry
                    .lookup(&args.name)
                    .unwrap()
                    .descriptor();
                let call = PendingModuleCall::from_test_command(conn, command, &args);
                fixture
                    .backend
                    .ctx
                    .retain_pending_module_call(call)
                    .unwrap();
            }
            "unknown" => {
                fixture.backend.ctx.blocking_registry_mut().register_client(
                    0,
                    &[Bytes::from_static(b"source")],
                    fixture.backend.connection_registry[waiter].session_id(),
                );
            }
            "loaded" => {
                fixture.block(waiter, &make_args(&[b"BLPOP", b"source", b"60"]));
                let module = ModuleRef::new("snapshot-fallback", std::ptr::null_mut(), 1, true);
                // VALKEYMODULE_OPTIONS_HANDLE_FORKLESS permits snapshot startup,
                // but loaded modules must still retain global waiter barriers.
                module.options.set(1 << 6);
                fixture
                    .backend
                    .ctx
                    .module_registry_mut()
                    .register(module)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        fixture.start();
        fixture.capture_targets(&[b"source"]);
        let writer = make_args(&[b"RPUSH", b"source", b"value"]);
        let spec = fixture
            .backend
            .registry
            .lookup(&writer.name)
            .unwrap()
            .clone();
        assert_eq!(
            fixture
                .backend
                .pre_dispatch_exec(executor.0, meta_from_spec(&spec), &writer)
                .action,
            DispatchAction::Postpone(PostponeReason::ForklessKey),
            "{fallback} waiters must retain the conservative global barrier"
        );
        assert!(
            matches!(
                plan_module_call_snapshot(&fixture.backend.ctx, &spec.descriptor(), &writer, 0),
                ModuleCallSnapshotPlan::RequiresFullCapture
            ),
            "{fallback} module-call classification must retain the global barrier"
        );
        assert!(fixture.backend.connection_registry[executor]
            .transaction
            .begin(0, 0));
        assert!(fixture.backend.connection_registry[executor]
            .transaction
            .queue(1, writer));
        let mut effects = CommandEffects::default();
        assert_eq!(
            fixture
                .backend
                .handle_exec(CommandTarget::new(executor), "exec", false, &mut effects),
            CommandDisposition::Blocked,
            "{fallback} EXEC must retain the global barrier"
        );
    }
}

#[test]
fn cyclic_moves_resolve_each_wakeup_and_keep_snapshot_values() {
    let mut fixture = CaptureFixture::new("forkless-cyclic-moves");
    let forward = Token(41);
    let reverse = Token(42);
    let writer = Token(43);
    for token in [forward, reverse, writer] {
        fixture.add_client(token);
    }
    let source = b"snapshot-list-0".as_slice();
    let destination = b"snapshot-list-1".as_slice();
    for key in [source, destination] {
        let (disposition, effects) = fixture.admit(writer, &make_args(&[b"LPOP", key]));
        assert_eq!(disposition, CommandDisposition::Completed);
        assert_eq!(effects.replies, vec![b"$6\r\nbefore\r\n".to_vec()]);
    }
    let forward_original = fixture.block(
        forward,
        &make_args(&[b"BLMOVE", source, destination, b"RIGHT", b"LEFT", b"60"]),
    );
    let reverse_original = fixture.block(
        reverse,
        &make_args(&[b"BLMOVE", destination, source, b"RIGHT", b"LEFT", b"60"]),
    );
    // Seed both sources after registration but before the snapshot starts.
    // The reverse move must consume its original tail, not the forward reply.
    fixture.push_without_waking(source, &[b"forward-before"]);
    fixture.push_without_waking(destination, &[b"reverse-before"]);
    fixture.start();
    fixture.capture_targets(&[source]);
    assert!(!fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_captured(0, destination));
    fixture.process_source(source);

    assert_original_state(&fixture.backend, forward, &forward_original);
    assert_original_state(&fixture.backend, reverse, &reverse_original);
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 2);
    assert_eq!(
        fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_list(source)
            .unwrap()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Bytes::from_static(b"forward-before")]
    );
    assert_eq!(
        fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_list(destination)
            .unwrap()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Bytes::from_static(b"reverse-before")]
    );

    // Capture release must resume the deferred source, then signal the
    // reverse source, without a second writer or a full-capture graph barrier.
    fixture.resume_after_capture_release(&[forward, reverse]);
    assert_eq!(
        queued_flat_bytes(&fixture.backend, forward),
        b"$14\r\nforward-before\r\n"
    );
    assert_eq!(
        queued_flat_bytes(&fixture.backend, reverse),
        b"$14\r\nreverse-before\r\n"
    );
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    for (key, expected) in [
        (source, b"reverse-before".as_slice()),
        (destination, b"forward-before".as_slice()),
    ] {
        assert_eq!(
            fixture
                .backend
                .ctx
                .db_mut(0)
                .unwrap()
                .get_list(key)
                .unwrap()
                .unwrap()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![Bytes::copy_from_slice(expected)]
        );
    }
    fixture.process_source(source);
    fixture.process_source(destination);
    assert_eq!(
        queued_flat_bytes(&fixture.backend, forward),
        b"$14\r\nforward-before\r\n"
    );
    assert_eq!(
        queued_flat_bytes(&fixture.backend, reverse),
        b"$14\r\nreverse-before\r\n"
    );
    let mut restored = fixture.finish_snapshot();
    assert_eq!(
        list_values(&mut restored, source),
        vec![Bytes::from_static(b"forward-before")]
    );
    assert_eq!(
        list_values(&mut restored, destination),
        vec![Bytes::from_static(b"reverse-before")]
    );
}

#[test]
fn group_reader_reblocks_after_competition_and_keeps_target_scoped_admission() {
    use crate::foundation::io::store::structure::stream::StreamID;

    for origin in ["direct", "exec", "internal"] {
        let mut fixture = CaptureFixture::new("forkless-group-reblock");
        let head = Token(41);
        let follower = Token(42);
        let writer = Token(43);
        let unrelated = Token(44);
        for token in [head, follower, writer, unrelated] {
            fixture.add_client(token);
        }
        let source = b"snapshot-list-0".as_slice();
        let (disposition, effects) = fixture.admit(writer, &make_args(&[b"DEL", source]));
        assert_eq!(disposition, CommandDisposition::Completed);
        assert_eq!(effects.replies, vec![b":1\r\n".to_vec()]);
        let (disposition, effects) = fixture.admit(
            writer,
            &make_args(&[b"XGROUP", b"CREATE", source, b"group", b"0", b"MKSTREAM"]),
        );
        assert_eq!(disposition, CommandDisposition::Completed);
        assert_eq!(effects.replies, vec![b"+OK\r\n".to_vec()]);
        let blocked_at_ms = crate::foundation::io::sys::time::mstime() - 5_000;
        let original = {
            let _time =
                crate::foundation::io::sys::time::enter_command_time_snapshot(blocked_at_ms);
            fixture.block(
                head,
                &make_args(&[
                    b"XREADGROUP",
                    b"GROUP",
                    b"group",
                    b"head",
                    b"BLOCK",
                    b"60000",
                    b"STREAMS",
                    source,
                    b">",
                ]),
            );
            fixture.block(
                follower,
                &make_args(&[
                    b"XREADGROUP",
                    b"GROUP",
                    b"group",
                    b"follower",
                    b"BLOCK",
                    b"60000",
                    b"STREAMS",
                    source,
                    b">",
                ]),
            )
        };
        assert_eq!(original.start_time_ms, blocked_at_ms);
        let unrelated_original = fixture.block(
            unrelated,
            &make_args(&[
                b"BLMOVE",
                b"unrelated-source",
                b"unrelated-destination",
                b"RIGHT",
                b"LEFT",
                b"60",
            ]),
        );
        fixture.start();
        fixture.capture_targets(&[source]);
        let first = make_args(&[b"XADD", source, b"1-0", b"field", b"first"]);
        let mut conn = ModuleCallConn::new(0);
        assert_eq!(
            fixture
                .backend
                .ctx
                .execute_command(&first.name, &mut conn, &first),
            Value::bulk(Bytes::from_static(b"1-0"))
        );
        fixture.process_source(source);

        assert!(fixture.backend.connection_registry[head].blocked.is_none());
        assert_eq!(
            queued_flat_bytes(&fixture.backend, head),
            b"*1\r\n*2\r\n$15\r\nsnapshot-list-0\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$5\r\nfield\r\n$5\r\nfirst\r\n"
        );
        // XREADGROUP is eligible even after the head advances the group ID.
        // A fresh consumer seen time proves real executor entry; its empty
        // result then reaches wakeup_reblock_if_needed and re-registers.
        let group = fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_stream(source)
            .unwrap()
            .unwrap()
            .group(b"group")
            .unwrap();
        assert_eq!(group.last_id, StreamID::new(1, 0));
        assert_eq!(
            group.consumers[b"head".as_slice()]
                .pending
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![StreamID::new(1, 0)]
        );
        let consumer = &group.consumers[b"follower".as_slice()];
        assert!(
            consumer.seen_time_ms > original.start_time_ms,
            "the follower must really rerun after the head consumes the message"
        );
        assert_eq!(consumer.active_time_ms, None);
        assert!(consumer.pending.is_empty());
        assert_original_state(&fixture.backend, follower, &original);
        assert_eq!(
            fixture
                .backend
                .ctx
                .blocking_registry()
                .waiters_for_key(0, source),
            vec![fixture.backend.connection_registry[follower].session_id()]
        );
        assert_eq!(fixture.backend.ctx.blocking_registry().count(), 2);
        assert_eq!(
            fixture.backend.next_blocked_timeout(blocked_at_ms + 5_000),
            Some(Duration::from_secs(55)),
            "real reblocking must retain the original deadline"
        );
        assert!(!fixture
            .backend
            .ctx
            .persistence()
            .forkless_key_captured(0, b"unrelated-destination"));

        let next = make_args(&[b"XADD", source, b"2-0", b"field", b"second"]);
        let spec = fixture.backend.registry.lookup(&next.name).unwrap().clone();
        assert_eq!(
            fixture
                .backend
                .pre_dispatch_exec(writer.0, meta_from_spec(&spec), &next)
                .action,
            DispatchAction::Execute,
            "direct admission must retain the reblocked ordinary session's classification"
        );
        assert!(
            matches!(
                plan_module_call_snapshot(&fixture.backend.ctx, &spec.descriptor(), &next, 0),
                ModuleCallSnapshotPlan::Ready
            ),
            "internal admission must not inherit unrelated waiter targets after reblocking"
        );
        match origin {
            "direct" => {
                let (disposition, effects) = fixture.admit(writer, &next);
                assert_eq!(disposition, CommandDisposition::Completed);
                assert_eq!(effects.replies, vec![b"$3\r\n2-0\r\n".to_vec()]);
            }
            "exec" => {
                assert!(fixture.backend.connection_registry[writer]
                    .transaction
                    .begin(0, 0));
                assert!(fixture.backend.connection_registry[writer]
                    .transaction
                    .queue(1, next));
                let mut effects = CommandEffects::default();
                assert_eq!(
                    fixture.backend.handle_exec(
                        CommandTarget::new(writer),
                        "exec",
                        false,
                        &mut effects,
                    ),
                    CommandDisposition::Completed,
                    "EXEC must recognize the ordinary marker restored by real reblocking"
                );
                assert_eq!(effects.replies, vec![b"*1\r\n$3\r\n2-0\r\n".to_vec()]);
            }
            "internal" => {
                assert!(
                    crate::framework::capabilities::module_call_snapshot::prepare_module_call_snapshot(
                        &mut fixture.backend.ctx,
                        &spec.descriptor(),
                        &next,
                        0,
                    )
                );
                let mut conn = ModuleCallConn::new(0);
                assert_eq!(
                    fixture
                        .backend
                        .ctx
                        .execute_command(&next.name, &mut conn, &next),
                    Value::bulk(Bytes::from_static(b"2-0"))
                );
            }
            _ => unreachable!(),
        }
        fixture.process_source(source);
        assert!(fixture.backend.connection_registry[follower]
            .blocked
            .is_none());
        assert_eq!(
            queued_flat_bytes(&fixture.backend, follower),
            b"*1\r\n*2\r\n$15\r\nsnapshot-list-0\r\n*1\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$5\r\nfield\r\n$6\r\nsecond\r\n"
        );
        assert_original_state(&fixture.backend, unrelated, &unrelated_original);
        assert!(fixture
            .backend
            .ctx
            .persistence()
            .forkless_capture_in_progress());
        assert!(!fixture
            .backend
            .ctx
            .persistence()
            .forkless_key_captured(0, b"unrelated-destination"));
        assert!(fixture
            .backend
            .ctx
            .blocking_registry()
            .waiters_for_key(0, source)
            .is_empty());
        let mut restored = fixture.finish_snapshot();
        let stream = restored
            .get_mut(0)
            .unwrap()
            .get_stream(source)
            .unwrap()
            .unwrap();
        assert!(stream.is_empty());
        let group = stream.group(b"group").unwrap();
        assert_eq!(group.last_id, StreamID::zero());
        assert!(group.pending.is_empty());
        assert_eq!(group.consumers[b"head".as_slice()].active_time_ms, None);
        assert_eq!(group.consumers[b"follower".as_slice()].active_time_ms, None);
    }
}

fn prepare_shared_tail_fifo(
    fixture: &mut CaptureFixture,
    head: Token,
    follower: Token,
) -> (Vec<u8>, BlockedState, BlockedState) {
    fixture.add_client(head);
    fixture.add_client(follower);
    fixture.start();
    // Record the absent source before EOF so the follower has a captured,
    // unblocked target, rather than relying only on capture having finished.
    fixture.capture_targets(&[b"source"]);
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_capture_in_progress()
    {
        assert!(Instant::now() < deadline, "capture feed must reach EOF");
        fixture.backend.pump_forkless_capture();
        thread::yield_now();
    }
    fixture
        .backend
        .ctx
        .config_mut()
        .persistence
        .rdb_key_save_delay = 10_000_000;
    fixture
        .backend
        .ctx
        .persistence()
        .set_forkless_key_delay(10_000_000);
    // Completion is sent before finish_item enters its delay wait. Observe the
    // actual wait under its mutex, not the configuration or sampled progress.
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_delay_wait_for_test()
        != Some(10_000_000)
    {
        assert!(
            Instant::now() < deadline,
            "serializer must enter the actual long key-delay wait"
        );
        thread::yield_now();
    }
    // Consume published completions through maintenance before retaining any
    // ready fact, so old progress cannot rescue a broken head-recheck route.
    fixture.backend.pump_forkless_capture();
    assert_eq!(
        fixture
            .backend
            .ctx
            .persistence_mut()
            .take_forkless_progress_notification(),
        ForklessProgressNotification::Unchanged
    );
    let destination = (0..SNAPSHOT_LISTS)
        .map(|index| format!("snapshot-list-{index}").into_bytes())
        .find(|key| {
            fixture
                .backend
                .ctx
                .persistence()
                .forkless_key_blocked(0, key)
        })
        .expect("finished capture must retain queued shared tail entries");
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_save_in_progress());
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_captured(0, &destination));
    assert!(fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_captured(0, b"source"));
    assert!(!fixture
        .backend
        .ctx
        .persistence()
        .forkless_key_blocked(0, b"source"));

    let head_original = fixture.block(
        head,
        &make_args(&[b"BLMOVE", b"source", &destination, b"RIGHT", b"LEFT", b"60"]),
    );
    let follower_original = fixture.block(follower, &make_args(&[b"BLPOP", b"source", b"60"]));
    fixture.push_without_waking(b"source", &[b"first", b"second"]);
    fixture.process_source(b"source");
    assert_original_state(&fixture.backend, head, &head_original);
    assert_original_state(&fixture.backend, follower, &follower_original);
    assert_eq!(
        fixture
            .backend
            .ctx
            .blocking_registry()
            .waiters_for_key(0, b"source"),
        vec![
            fixture.backend.connection_registry[head].session_id(),
            fixture.backend.connection_registry[follower].session_id(),
        ]
    );
    assert!(fixture
        .backend
        .ctx
        .blocking_registry()
        .is_forkless_ready_key_deferred(0, b"source"));
    assert!(fixture.backend.ready_keys.is_empty());
    assert!(!fixture.backend.ctx.has_forkless_waiters());
    assert_eq!(
        fixture
            .backend
            .next_blocked_timeout(head_original.start_time_ms),
        Some(Duration::from_secs(60))
    );
    (destination, head_original, follower_original)
}

fn assert_safe_follower_resumes_on_next_shared_tail_turn(
    fixture: &mut CaptureFixture,
    follower: Token,
    destination: &[u8],
    original: &BlockedState,
) {
    assert_original_state(&fixture.backend, follower, original);
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 1);
    assert_eq!(
        fixture
            .backend
            .ctx
            .blocking_registry()
            .waiters_for_key(0, b"source"),
        vec![fixture.backend.connection_registry[follower].session_id()]
    );
    assert_eq!(
        fixture.backend.next_blocked_timeout(original.start_time_ms),
        Some(Duration::from_secs(60)),
        "removing the head must preserve the follower's original deadline"
    );
    let progress_before = fixture
        .backend
        .ctx
        .persistence()
        .forkless_progress()
        .unwrap();
    let shared_before: Vec<_> = (0..SNAPSHOT_LISTS)
        .map(|index| format!("snapshot-list-{index}").into_bytes())
        .filter(|key| {
            fixture
                .backend
                .ctx
                .persistence()
                .forkless_key_blocked(0, key)
        })
        .collect();
    assert!(shared_before.iter().any(|key| key == destination));

    // No writer, manual ready signal, capture release, or cancellation may
    // supply a new ready fact after the old head's terminal removal.
    fixture.backend.pump_forkless_capture();
    fixture.backend.process_ready_keys();

    let persistence = fixture.backend.ctx.persistence();
    let progress_after = persistence.forkless_progress().unwrap();
    assert_eq!(
        (
            progress_after.processed,
            progress_after.enqueued,
            progress_after.queue_length,
        ),
        (
            progress_before.processed,
            progress_before.enqueued,
            progress_before.queue_length,
        ),
        "the follower must resume without any serializer progress"
    );
    let shared_after: Vec<_> = (0..SNAPSHOT_LISTS)
        .map(|index| format!("snapshot-list-{index}").into_bytes())
        .filter(|key| persistence.forkless_key_blocked(0, key))
        .collect();
    assert_eq!(
        shared_after, shared_before,
        "unrelated shared completions must not re-arm the deferred key"
    );
    assert!(!persistence.forkless_capture_in_progress());
    assert!(persistence.forkless_save_in_progress());
    assert!(persistence.forkless_key_blocked(0, destination));
    assert!(persistence.forkless_key_captured(0, b"source"));
    assert!(!persistence.forkless_key_blocked(0, b"source"));
    assert_eq!(persistence.forkless_key_delay_for_test(), Some(10_000_000));
    assert_eq!(
        fixture.backend.ctx.config().persistence.rdb_key_save_delay,
        10_000_000
    );
    assert!(
        fixture.backend.connection_registry[follower]
            .blocked
            .is_none(),
        "the safe new head must consume on the next turn despite the old head's shared destination"
    );
    assert_eq!(
        queued_flat_bytes(&fixture.backend, follower),
        b"*2\r\n$6\r\nsource\r\n$5\r\nfirst\r\n"
    );
    assert_eq!(
        fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_list(b"source")
            .unwrap()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Bytes::from_static(b"second")]
    );
    assert_eq!(
        fixture
            .backend
            .ctx
            .db_mut(0)
            .unwrap()
            .get_list(destination)
            .unwrap()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Bytes::from_static(b"before")]
    );
    assert_eq!(fixture.backend.ctx.blocking_registry().count(), 0);
    assert!(fixture
        .backend
        .ctx
        .blocking_registry()
        .waiters_for_key(0, b"source")
        .is_empty());
    assert!(!fixture
        .backend
        .ctx
        .blocking_registry()
        .is_forkless_ready_key_deferred(0, b"source"));
    assert!(fixture.backend.ready_keys.is_empty());

    // Only after immediate consumption has been proved may snapshot cleanup
    // lower the delay; it cannot provide progress for the wakeup assertion.
    let mut restored = fixture.finish_snapshot();
    assert_eq!(
        list_values(&mut restored, destination),
        vec![Bytes::from_static(b"before")]
    );
    assert!(restored.get(0).unwrap().get_no_expire(b"source").is_none());
    assert_eq!(
        queued_flat_bytes(&fixture.backend, follower),
        b"*2\r\n$6\r\nsource\r\n$5\r\nfirst\r\n",
        "snapshot completion must not replay the already completed follower"
    );
}

#[test]
fn shared_tail_head_timeout_rechecks_safe_follower_without_snapshot_progress() {
    let mut fixture = CaptureFixture::new("forkless-shared-tail-head-timeout");
    let head = Token(41);
    let follower = Token(42);
    let (destination, head_original, follower_original) =
        prepare_shared_tail_fifo(&mut fixture, head, follower);
    let head_session = fixture.backend.connection_registry[head].session_id();
    // Expire only the head's original timeout without sleeping or restarting
    // the follower's timeout. Use the same terminal path as maintenance.
    fixture.backend.connection_registry[head]
        .blocked
        .as_mut()
        .unwrap()
        .start_time_ms =
        crate::foundation::io::sys::time::mstime() - head_original.timeout_ms as i64 - 1;
    assert_eq!(
        fixture
            .backend
            .next_blocked_timeout(crate::foundation::io::sys::time::mstime()),
        Some(Duration::ZERO)
    );
    fixture.backend.check_blocked_timeouts();
    assert!(fixture.backend.connection_registry[head].blocked.is_none());
    assert!(!fixture
        .backend
        .ctx
        .blocking_registry()
        .is_client_blocked(&head_session));
    assert_eq!(queued_flat_bytes(&fixture.backend, head), b"*-1\r\n");
    assert_safe_follower_resumes_on_next_shared_tail_turn(
        &mut fixture,
        follower,
        &destination,
        &follower_original,
    );
    assert_eq!(queued_flat_bytes(&fixture.backend, head), b"*-1\r\n");
}

#[test]
fn shared_tail_head_disconnect_rechecks_safe_follower_without_snapshot_progress() {
    let mut fixture = CaptureFixture::new("forkless-shared-tail-head-disconnect");
    let head = Token(41);
    let follower = Token(42);
    let (destination, _, follower_original) =
        prepare_shared_tail_fifo(&mut fixture, head, follower);
    let head_session = fixture.backend.connection_registry[head].session_id();
    // Close the owned connection through lifecycle cleanup, which discards
    // its blocked command and unregisters the ordinary session.
    fixture.backend.connection_registry[head].mark_closing();
    fixture.backend.cleanup_closed();
    assert!(!fixture.backend.connection_registry.contains(head));
    assert!(!fixture
        .backend
        .ctx
        .blocking_registry()
        .is_client_blocked(&head_session));
    assert_safe_follower_resumes_on_next_shared_tail_turn(
        &mut fixture,
        follower,
        &destination,
        &follower_original,
    );
    assert!(!fixture.backend.connection_registry.contains(head));
}
