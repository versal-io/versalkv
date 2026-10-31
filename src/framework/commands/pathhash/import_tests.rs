use bytes::Bytes;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::foundation::io::store::structure::path_hash::PathHash;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::{execute, TestCommandRuntime};

const KEY: &[u8] = b"h";

fn command(parts: &[&str]) -> CommandArgs {
    CommandArgs::from_parts(
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect(),
    )
}

fn seed(db: &mut Db, deadline: Option<u64>) {
    let mut tree = PathHash::new();
    for (path, value) in [
        (b"p".as_slice(), b"old".as_slice()),
        (b"keep", b"untouched"),
        (b"branch/a", b"a"),
        (b"branch/b", b"b"),
    ] {
        tree.set_pairs(
            path,
            &[(Bytes::from_static(b"f"), Bytes::copy_from_slice(value))],
            512,
            64,
        );
    }
    db.set(KEY.to_vec(), Robj::PathHash(Box::new(tree)));
    if let Some(deadline) = deadline {
        db.set_expire(KEY, deadline);
    }
}

fn tree(db: &Db) -> &PathHash {
    match db.get_allow_expired(KEY).unwrap() {
        Robj::PathHash(tree) => tree,
        value => panic!("expected a path hash, got {value:?}"),
    }
}

fn field(tree: &PathHash, path: &[u8], field: &[u8]) -> Option<Bytes> {
    tree.get(path)
        .and_then(|payload| payload.get(field))
        .map(Bytes::from)
}

fn assert_write(outcome: &CommandOutcome, expected: Value, dirty: i64, is_new: bool) {
    assert_eq!(outcome.reply, expected);
    assert_eq!(outcome.effects.dirty_delta, Some(dirty));
    assert_eq!(
        outcome
            .effects
            .events
            .iter()
            .any(|event| event.kind == "new"),
        is_new
    );
}

fn check_import_write(parts: &[&str], expected: Value, dirty: i64) {
    for should_suppress_lazy_expire in [false, true] {
        let mut runtime = TestCommandRuntime::new(1, 100);
        runtime.read_expired_values = true;
        let db = runtime.dbs.get_mut(0).unwrap();
        seed(db, Some(1));
        db.set_no_lazy_expire(should_suppress_lazy_expire);
        db.set_lru_idle(KEY, 100);
        let metadata = db.get_lru_raw(KEY);
        let snapshot = db.snapshot_value_for_background(KEY, 0).unwrap();
        assert!(snapshot.is_shared());

        let outcome = execute(&mut runtime, &command(parts));
        assert_write(&outcome, expected.clone(), dirty, false);
        let db = runtime.dbs.get(0).unwrap();
        assert_eq!(db.get_expire(KEY), Some(1));
        assert_eq!(db.get_lru_raw(KEY), metadata);
        assert_eq!(
            field(tree(db), b"keep", b"f"),
            Some(Bytes::from_static(b"untouched"))
        );
        match parts[0] {
            "PHSET" | "PHMSET" => {
                assert_eq!(
                    field(tree(db), b"p", b"f"),
                    Some(Bytes::from_static(b"old"))
                );
                assert_eq!(
                    field(tree(db), b"q", b"g"),
                    Some(Bytes::from_static(b"new"))
                );
            }
            "PHDEL" => assert!(tree(db).get(b"p").is_none()),
            "PHDELPREFIX" => {
                assert!(tree(db).get(b"branch/a").is_none());
                assert!(tree(db).get(b"branch/b").is_none());
                assert_eq!(
                    field(tree(db), b"p", b"f"),
                    Some(Bytes::from_static(b"old"))
                );
            }
            _ => unreachable!(),
        }
        let Robj::PathHash(old_tree) = snapshot.as_ref() else {
            panic!("snapshot must retain the path hash");
        };
        assert_eq!(
            field(old_tree, b"p", b"f"),
            Some(Bytes::from_static(b"old"))
        );
        assert!(old_tree.get(b"q").is_none());
        assert!(old_tree.get(b"branch/a").is_some());
        assert!(!std::ptr::eq(old_tree.as_ref(), tree(db)));
        assert!(runtime.dbs.get_mut(0).unwrap().expired_deletions.is_empty());
    }
}

#[test]
fn phset_expired_import_preserves_tree_ttl_and_snapshot() {
    check_import_write(
        &["PHSET", "h", "q", "FIELDS", "1", "g", "new"],
        Value::ok(),
        1,
    );
}

#[test]
fn phmset_expired_import_preserves_tree_ttl_and_snapshot() {
    check_import_write(
        &[
            "PHMSET", "h", "q", "FIELDS", "1", "g", "new", "r", "FIELDS", "1", "x", "batch",
        ],
        Value::ok(),
        2,
    );
}

#[test]
fn phdel_expired_import_preserves_tree_ttl_and_snapshot() {
    check_import_write(&["PHDEL", "h", "p", "f"], Value::integer(1), 1);
}

#[test]
fn phdelprefix_expired_import_preserves_tree_ttl_and_snapshot() {
    check_import_write(&["PHDELPREFIX", "h", "branch/"], Value::integer(2), 2);
}

#[test]
fn phset_import_conditions_use_physical_fields_atomically() {
    for (condition, existing_path, has_existing_field, should_write) in [
        ("FNX", true, true, false),
        ("FNX", true, false, true),
        ("FNX", false, false, true),
        ("FXX", true, true, true),
        ("FXX", true, false, false),
        ("FXX", false, false, false),
    ] {
        for should_suppress_lazy_expire in [false, true] {
            let mut runtime = TestCommandRuntime::new(1, 100);
            runtime.read_expired_values = true;
            let db = runtime.dbs.get_mut(0).unwrap();
            seed(db, Some(1));
            db.set_no_lazy_expire(should_suppress_lazy_expire);
            let path = if existing_path { "p" } else { "missing" };
            let name = if has_existing_field { "f" } else { "g" };
            let outcome = execute(
                &mut runtime,
                &command(&["PHSET", "h", path, condition, "FIELDS", "1", name, "new"]),
            );
            assert_write(
                &outcome,
                if should_write {
                    Value::ok()
                } else {
                    Value::null()
                },
                i64::from(should_write),
                false,
            );
            let db = runtime.dbs.get(0).unwrap();
            assert_eq!(db.get_expire(KEY), Some(1), "{condition}");
            assert_eq!(
                field(tree(db), path.as_bytes(), name.as_bytes()),
                if should_write {
                    Some(Bytes::from_static(b"new"))
                } else if has_existing_field {
                    Some(Bytes::from_static(b"old"))
                } else {
                    None
                },
                "{condition}"
            );
            assert_eq!(
                field(tree(db), b"keep", b"f"),
                Some(Bytes::from_static(b"untouched"))
            );
            assert!(runtime.dbs.get_mut(0).unwrap().expired_deletions.is_empty());
        }
    }

    for condition in ["FNX", "FXX"] {
        let mut runtime = TestCommandRuntime::new(1, 100);
        runtime.read_expired_values = true;
        seed(runtime.dbs.get_mut(0).unwrap(), Some(1));
        let outcome = execute(
            &mut runtime,
            &command(&[
                "PHSET", "h", "p", condition, "FIELDS", "2", "g", "new", "f", "changed",
            ]),
        );
        assert_write(&outcome, Value::null(), 0, false);
        let db = runtime.dbs.get(0).unwrap();
        assert_eq!(
            field(tree(db), b"p", b"f"),
            Some(Bytes::from_static(b"old"))
        );
        assert_eq!(field(tree(db), b"p", b"g"), None);
        assert_eq!(db.get_expire(KEY), Some(1));
    }
}

#[test]
fn pathhash_import_wrongtype_preserves_physical_value_and_ttl() {
    for parts in [
        vec!["PHSET", "h", "p", "FIELDS", "1", "f", "v"],
        vec!["PHSET", "h", "p", "FNX", "FIELDS", "1", "f", "v"],
        vec!["PHSET", "h", "p", "FXX", "FIELDS", "1", "f", "v"],
        vec!["PHMSET", "h", "p", "FIELDS", "1", "f", "v"],
        vec!["PHDEL", "h", "p"],
        vec!["PHDELPREFIX", "h", ""],
    ] {
        for should_suppress_lazy_expire in [false, true] {
            let mut runtime = TestCommandRuntime::new(1, 100);
            runtime.read_expired_values = true;
            let db = runtime.dbs.get_mut(0).unwrap();
            db.set(KEY.to_vec(), Robj::string("original"));
            db.set_expire(KEY, 1);
            db.set_no_lazy_expire(should_suppress_lazy_expire);
            let outcome = execute(&mut runtime, &command(&parts));
            assert_eq!(outcome.reply, CommandError::wrong_type(), "{parts:?}");
            assert!(outcome.effects.changed_keys.is_empty());
            assert!(outcome.effects.events.is_empty());
            let db = runtime.dbs.get(0).unwrap();
            assert_eq!(db.get_allow_expired(KEY).unwrap().as_bytes(), b"original");
            assert_eq!(db.get_expire(KEY), Some(1));
            assert!(runtime.dbs.get_mut(0).unwrap().expired_deletions.is_empty());
        }
    }
}

#[test]
fn pathhash_ordinary_writes_hide_and_expire_old_objects() {
    for (parts, expected, dirty, is_create) in [
        (
            vec!["PHSET", "h", "q", "FIELDS", "1", "g", "new"],
            Value::ok(),
            1,
            true,
        ),
        (
            vec!["PHMSET", "h", "q", "FIELDS", "1", "g", "new"],
            Value::ok(),
            1,
            true,
        ),
        (vec!["PHDEL", "h", "p"], Value::integer(0), 0, false),
        (vec!["PHDELPREFIX", "h", ""], Value::integer(0), 0, false),
        (
            vec!["PHSET", "h", "p", "FXX", "FIELDS", "1", "f", "new"],
            Value::null(),
            0,
            false,
        ),
        (
            vec!["PHSET", "h", "p", "FNX", "FIELDS", "1", "f", "new"],
            Value::ok(),
            1,
            true,
        ),
    ] {
        let mut runtime = TestCommandRuntime::new(1, 100);
        seed(runtime.dbs.get_mut(0).unwrap(), Some(1));
        let outcome = execute(&mut runtime, &command(&parts));
        assert_write(&outcome, expected, dirty, is_create);
        let db = runtime.dbs.get(0).unwrap();
        assert_eq!(db.get_expire(KEY), None);
        assert_eq!(db.get_allow_expired(KEY).is_some(), is_create);
        if is_create {
            assert!(tree(db).get(b"keep").is_none());
        }
        assert_eq!(
            runtime.dbs.get_mut(0).unwrap().expired_deletions,
            vec![KEY.to_vec()]
        );
    }
}

#[test]
fn pathhash_ordinary_no_lazy_expire_does_not_read_physical_values() {
    for parts in [
        vec!["PHSET", "h", "p", "FXX", "FIELDS", "1", "f", "new"],
        vec!["PHDEL", "h", "p"],
        vec!["PHDELPREFIX", "h", ""],
    ] {
        let mut runtime = TestCommandRuntime::new(1, 100);
        let db = runtime.dbs.get_mut(0).unwrap();
        seed(db, Some(1));
        db.set_no_lazy_expire(true);
        let outcome = execute(&mut runtime, &command(&parts));
        let expected = if parts[0] == "PHSET" {
            Value::null()
        } else {
            Value::integer(0)
        };
        assert_write(&outcome, expected, 0, false);
        let db = runtime.dbs.get(0).unwrap();
        assert_eq!(
            field(tree(db), b"p", b"f"),
            Some(Bytes::from_static(b"old"))
        );
        assert_eq!(db.get_expire(KEY), Some(1));
    }
}

#[test]
fn pathhash_mutation_accounts_cached_growth_and_shrink() {
    use crate::foundation::memory::metrics::memory_stats::{self, MemoryCategory};

    let (observations, is_empty) = {
        let _guard = memory_stats::lock_global_for_test();
        let mut runtime = TestCommandRuntime::new(1, 100);
        runtime.read_expired_values = true;
        seed(runtime.dbs.get_mut(0).unwrap(), Some(1));
        let mut observations = Vec::new();
        for parts in [
            vec![
                "PHSET",
                "h",
                "q",
                "FIELDS",
                "1",
                "g",
                "a-value-larger-than-before",
            ],
            vec![
                "PHMSET", "h", "q", "FIELDS", "1", "g", "x", "r", "FIELDS", "1", "x", "v",
            ],
            vec!["PHDEL", "h", "q"],
            vec!["PHDELPREFIX", "h", ""],
        ] {
            let old_size = tree(runtime.dbs.get(0).unwrap()).memory_usage();
            let before = memory_stats::get_tracker().get(MemoryCategory::Dataset);
            let outcome = execute(&mut runtime, &command(&parts));
            let db = runtime.dbs.get(0).unwrap();
            let new_size = tree(db).memory_usage();
            observations.push((
                parts,
                outcome.reply,
                memory_stats::get_tracker().get(MemoryCategory::Dataset) as i128 - before as i128,
                new_size as i128 - old_size as i128,
                db.get_expire(KEY),
            ));
        }
        (observations, tree(runtime.dbs.get(0).unwrap()).is_empty())
    };
    for (parts, reply, tracked_delta, expected_delta, deadline) in observations {
        assert!(!matches!(reply, Value::Error(_)));
        assert_eq!(tracked_delta, expected_delta, "{parts:?}");
        assert_eq!(deadline, Some(1));
    }
    assert!(is_empty);
}

#[cfg(not(feature = "wasm"))]
#[test]
fn pathhash_import_dataset_applies_writes_and_preserves_commit_deadline() {
    use crate::engine::distributed::cluster::migration::SlotRanges;
    use crate::engine::distributed::cluster::node::key_hash_slot;
    use crate::engine::distributed::cluster::SlotRange;
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::framework::context::RuntimeConfig;
    use crate::framework::distributed::migration::ImportDataset;
    use crate::framework::CommandRegistry;

    let mut serving = DbSet::new_with_slot_router(1, 14, |key| key_hash_slot(key) as usize);
    let ranges = SlotRanges::new(vec![
        SlotRange::new(key_hash_slot(KEY), key_hash_slot(KEY)).unwrap()
    ])
    .unwrap();
    let mut importing = ImportDataset::new_like(&serving, &RuntimeConfig::new(), ranges.clone());
    let mut registry = CommandRegistry::new();
    crate::framework::commands::register_all(&mut registry);
    for parts in [
        vec!["PHSET", "h", "p", "FIELDS", "1", "f", "old"],
        vec!["PEXPIREAT", "h", "1"],
        vec!["PHSET", "h", "p", "FXX", "FIELDS", "1", "f", "updated"],
        vec![
            "PHMSET", "h", "q", "FIELDS", "1", "g", "new", "branch/a", "FIELDS", "1", "f", "a",
        ],
        vec!["PHSET", "h", "branch/b", "FIELDS", "1", "f", "b"],
        vec!["PHDEL", "h", "branch/a"],
        vec!["PHDELPREFIX", "h", "branch/"],
    ] {
        importing.apply(command(&parts), &registry).unwrap();
    }
    let db = importing.dbs().get(0).unwrap();
    assert_eq!(
        field(tree(db), b"p", b"f"),
        Some(Bytes::from_static(b"updated"))
    );
    assert_eq!(
        field(tree(db), b"q", b"g"),
        Some(Bytes::from_static(b"new"))
    );
    assert!(tree(db).get(b"branch/a").is_none());
    assert!(tree(db).get(b"branch/b").is_none());
    assert_eq!(db.get_expire(KEY), Some(1));
    assert_eq!(importing.commit_into(&mut serving, &ranges), Ok(1));
    assert_eq!(serving.get(0).unwrap().get_expire(KEY), Some(1));
    assert!(serving.get_mut(0).unwrap().get(KEY).is_none());
    assert!(serving.get(0).unwrap().get_allow_expired(KEY).is_none());
}

#[cfg(not(feature = "wasm"))]
#[test]
fn pathhash_aof_loading_replays_physical_writes_and_deadline() {
    use crate::foundation::io::persistence::load_aof_bytes;
    use crate::framework::aof_replay::AofReplayPlayer;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandRegistry;

    let mut registry = CommandRegistry::new();
    crate::framework::commands::register_all(&mut registry);
    let mut ctx = ServerContext::new(1);
    ctx.set_loading(true);
    let mut bytes = Vec::new();
    for parts in [
        vec!["PHSET", "h", "p", "FIELDS", "1", "f", "old"],
        vec!["PEXPIREAT", "h", "1"],
        vec!["MULTI"],
        vec!["PHSET", "h", "p", "FXX", "FIELDS", "1", "f", "updated"],
        vec![
            "PHMSET", "h", "q", "FIELDS", "1", "g", "new", "branch/a", "FIELDS", "1", "f", "a",
        ],
        vec!["PHSET", "h", "branch/b", "FIELDS", "1", "f", "b"],
        vec!["PHDEL", "h", "branch/a"],
        vec!["PHDELPREFIX", "h", "branch/"],
        vec!["EXEC"],
    ] {
        crate::foundation::io::protocol::serializer::serialize_command(
            &parts.iter().map(|part| part.as_bytes()).collect::<Vec<_>>(),
            &mut bytes,
        );
    }
    let mut player = AofReplayPlayer::new(&registry, &mut ctx);
    load_aof_bytes(&bytes, &mut player).unwrap();
    let db = ctx.dbs().get(0).unwrap();
    assert_eq!(
        field(tree(db), b"p", b"f"),
        Some(Bytes::from_static(b"updated"))
    );
    assert_eq!(
        field(tree(db), b"q", b"g"),
        Some(Bytes::from_static(b"new"))
    );
    assert!(tree(db).get(b"branch/a").is_none());
    assert!(tree(db).get(b"branch/b").is_none());
    assert_eq!(db.get_expire(KEY), Some(1));
    ctx.set_loading(false);
    assert!(ctx.db_mut(0).unwrap().get(KEY).is_none());
    assert!(ctx.dbs().get(0).unwrap().get_allow_expired(KEY).is_none());
}
