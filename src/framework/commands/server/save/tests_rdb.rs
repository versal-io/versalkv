use super::*;
use crate::foundation::io::persistence as persistence;
use crate::foundation::io::store::structure::hash::HashHt;
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::structure::set::ValkeySet;
use crate::foundation::io::store::structure::zset::ZSetSl;
use crate::foundation::io::store::kv_engine::{now_ms, DbSet};
use crate::foundation::io::store::Robj;
use bytes::Bytes;
use indexmap::IndexMap;
use std::collections::HashSet;
use std::io::{self, Write};

#[path = "test_support.rs"]
mod test_support;
use test_support::FailAfterWrites;

fn wait_for_disk_sync_file_child(
    ctx: &mut crate::framework::context::ServerContext,
) -> super::DiskSyncChildExit {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match super::reap_disk_sync_file_child(ctx) {
            super::DiskSyncChildExit::Running => std::thread::yield_now(),
            status => return status,
        }
        assert!(std::time::Instant::now() < deadline, "disk-sync file child did not exit");
    }
}

fn disk_sync_streaming_fixture() -> DbSet {
    let mut list = ListValue::new();
    list.push_back(Bytes::from_static(b"a"));
    list.push_back(Bytes::from_static(b"b"));
    let mut set = HashSet::new();
    set.insert(Bytes::from_static(b"member"));
    let mut hash = IndexMap::new();
    hash.insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
    let mut zset = HashTable::new();
    zset.insert(Bytes::from_static(b"member"), 1.5);

    let mut dbs = DbSet::new(16);
    let db = dbs.get_mut(0).unwrap();
    db.set(b"string".to_vec(), Robj::string("value"));
    db.set(b"list".to_vec(), Robj::ListQuicklist(list));
    db.set(
        b"set".to_vec(),
        Robj::SetHashtable(ValkeySet::from_hashset(set)),
    );
    db.set(
        b"hash".to_vec(),
        Robj::HashHashtable(HashHt {
            data: Box::new(hash),
        }),
    );
    db.set(b"zset".to_vec(), Robj::ZSetSkiplist(ZSetSl::from_dict(zset)));
    dbs
}

fn canonicalize_replication_rdb(rdb: &[u8]) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>) {
    let mut pos = 9;
    let mut aux = Vec::new();
    while rdb[pos] == persistence::RDB_OPCODE_AUX {
        pos += 1;
        let key = persistence::load_string(rdb, &mut pos).unwrap();
        let value = persistence::load_string(rdb, &mut pos).unwrap();
        if key != b"ctime" && key != b"used-mem" {
            aux.push((key, value));
        }
    }
    (aux, rdb[pos..rdb.len() - 8].to_vec())
}

#[test]
fn bgsave_child_completion_is_idempotent_across_observers() {
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.persistence_mut().add_dirty(5);
    ctx.persistence_mut().begin_rdb_child(1234, 100).unwrap();

    super::apply_bgsave_child_completion(&mut ctx, 1234, true, 110).unwrap();
    super::apply_bgsave_child_completion(&mut ctx, 1234, true, 110).unwrap();

    assert_eq!(ctx.persistence().rdb_child_pid(), None);
    assert_eq!(ctx.persistence().rdb().last_save_time_sec, 110);
    assert_eq!(ctx.persistence().dirty(), 0);
}

#[test]
fn bgsave_child_failure_completion_is_idempotent() {
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.persistence_mut().add_dirty(5);
    ctx.persistence_mut().begin_rdb_child(1234, 100).unwrap();

    super::apply_bgsave_child_completion(&mut ctx, 1234, false, 110).unwrap();
    super::apply_bgsave_child_completion(&mut ctx, 1234, false, 110).unwrap();

    assert_eq!(ctx.persistence().rdb_child_pid(), None);
    assert_eq!(
        ctx.persistence().rdb().last_bgsave_status,
        crate::engine::persistence::OperationStatus::Err
    );
    assert_eq!(ctx.persistence().dirty(), 5);
}

#[test]
fn bgsave_child_completion_ignores_stale_pid() {
    let mut ctx = crate::framework::context::ServerContext::new(1);
    ctx.persistence_mut().add_dirty(5);
    ctx.persistence_mut().begin_rdb_child(1234, 100).unwrap();

    super::apply_bgsave_child_completion(&mut ctx, 4321, true, 110).unwrap();

    assert_eq!(ctx.persistence().rdb_child_pid(), Some(1234));
    assert_eq!(ctx.persistence().dirty(), 5);
}

#[test]
fn disk_rdb_streaming_writer_matches_buffered_serializer() {
    let dbs = disk_sync_streaming_fixture();
    let functions = vec![
        b"#!lua name=lib\nserver.register_function('f', function() return 1 end)".to_vec(),
    ];
    let replication = persistence::RdbReplicationInfo {
        repl_stream_db: 3,
        repl_id: "0123456789abcdef0123456789abcdef01234567".to_string(),
        repl_offset: 42,
        aux_fields: vec![(b"cluster-slot-states".to_vec(), Vec::new())],
    };
    let expected = persistence::serialize_all_with_functions_and_replication(
        &dbs,
        0,
        &functions,
        true,
        Some(&replication),
        &[],
    );
    let dir = std::env::temp_dir().join(format!(
        "valkey-disk-sync-streaming-writer-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.rdb");
    let mut writer = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());

    let written = super::fullsync_fork::write_replication_rdb_streaming(
        super::fullsync_fork::ReplicationRdbSource::new(
            &dbs,
            0,
            persistence::RdbSerializeOptions::default(),
            &functions,
            &replication,
            &[],
        ),
        &mut writer,
    )
    .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let actual = std::fs::read(&path).unwrap();
    assert_eq!(written, actual.len() as u64);
    assert_eq!(
        canonicalize_replication_rdb(&actual),
        canonicalize_replication_rdb(&expected)
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn disk_rdb_streaming_writer_propagates_write_failure() {
    let dbs = disk_sync_streaming_fixture();
    let replication = persistence::RdbReplicationInfo {
        repl_stream_db: 0,
        repl_id: "0123456789abcdef0123456789abcdef01234567".to_string(),
        repl_offset: 0,
        aux_fields: Vec::new(),
    };
    let mut writer = FailAfterWrites::new(0);

    let error = super::fullsync_fork::write_replication_rdb_streaming(
        super::fullsync_fork::ReplicationRdbSource::new(
            &dbs,
            0,
            persistence::RdbSerializeOptions::default(),
            &[],
            &replication,
            &[],
        ),
        &mut writer,
    )
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(writer.bytes.is_empty());
}

#[test]
fn disk_rdb_streaming_writer_honors_compression_option() {
    let value = "compressible-value-".repeat(64);
    let mut dbs = DbSet::new(1);
    dbs.get_mut(0).expect("DB 0 exists").set(
        b"key".to_vec(),
        Robj::string(value.clone()),
    );
    let replication = persistence::RdbReplicationInfo {
        repl_stream_db: 0,
        repl_id: "0123456789abcdef0123456789abcdef01234567".to_string(),
        repl_offset: 0,
        aux_fields: Vec::new(),
    };
    let mut uncompressed = Vec::new();
    super::fullsync_fork::write_replication_rdb_streaming(
        super::fullsync_fork::ReplicationRdbSource::new(
            &dbs,
            0,
            persistence::RdbSerializeOptions {
                compression: false,
                checksum: true,
            },
            &[],
            &replication,
            &[],
        ),
        &mut uncompressed,
    )
    .expect("write uncompressed replication RDB");
    let mut compressed = Vec::new();
    super::fullsync_fork::write_replication_rdb_streaming(
        super::fullsync_fork::ReplicationRdbSource::new(
            &dbs,
            0,
            persistence::RdbSerializeOptions {
                compression: true,
                checksum: true,
            },
            &[],
            &replication,
            &[],
        ),
        &mut compressed,
    )
    .expect("write compressed replication RDB");

    assert!(compressed.len() < uncompressed.len());
    assert!(uncompressed
        .windows(value.len())
        .any(|window| window == value.as_bytes()));
    assert!(!compressed
        .windows(value.len())
        .any(|window| window == value.as_bytes()));
    let mut restored = DbSet::new(1);
    persistence::load_rdb_bytes_checked(&compressed, &mut restored, true)
        .expect("load compressed replication RDB");
    assert_eq!(
        restored
            .get(0)
            .expect("DB 0 exists")
            .get_no_expire(b"key")
            .expect("key restored")
            .as_bytes(),
        value.as_bytes()
    );
}

#[test]
fn disk_sync_file_child_writes_rdb_and_reports_success() {
    use crate::framework::context::ServerContext;

    let _fork_guard = super::diskless_fork_test_guard();
    let dir = std::env::temp_dir().join(format!(
        "valkey-disk-sync-file-child-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.rdb");
    let mut ctx = ServerContext::new(16);
    ctx.dbs_mut()
        .get_mut(0)
        .unwrap()
        .set(b"key".to_vec(), Robj::string("value"));

    let pid = super::fork_disk_rdb_file_child(&mut ctx, &[], &path)
        .expect("disk-sync file child should fork");
    assert_eq!(ctx.persistence().disk_sync_child_pid(), Some(pid));
    assert_eq!(
        wait_for_disk_sync_file_child(&mut ctx),
        super::DiskSyncChildExit::Succeeded
    );
    assert!(!ctx.persistence().has_active_child());
    let mut restored = DbSet::new(16);
    let bytes = std::fs::read(&path).unwrap();
    persistence::load_rdb_bytes_checked(&bytes, &mut restored, true).unwrap();
    assert_eq!(
        restored.get(0).unwrap().get_no_expire(b"key").unwrap().as_bytes(),
        b"value"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn disk_sync_file_child_reports_failed_for_missing_parent() {
    use crate::framework::context::ServerContext;

    let _fork_guard = super::diskless_fork_test_guard();
    let dir = std::env::temp_dir().join(format!(
        "valkey-disk-sync-missing-parent-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.join("missing").join("snapshot.rdb");
    let mut ctx = ServerContext::new(16);

    super::fork_disk_rdb_file_child(&mut ctx, &[], &path)
        .expect("child creation should succeed before its file open fails");
    assert_eq!(
        wait_for_disk_sync_file_child(&mut ctx),
        super::DiskSyncChildExit::Failed
    );
    assert!(!ctx.persistence().has_active_child());
    assert!(!path.exists());
}

#[test]
fn rdb_header_and_footer() {
    let dbs = DbSet::new(16);
    let bytes = crate::foundation::io::persistence::serialize_all(&dbs, 0);
    assert_eq!(&bytes[..9], b"VALKEY080");
    let n = bytes.len();
    let crc = crate::foundation::io::persistence::crc64(0, &bytes[..n - 8]);
    assert_eq!(crc.to_le_bytes(), bytes[n - 8..]);
    assert_eq!(bytes[n - 9], crate::foundation::io::persistence::RDB_OPCODE_EOF);
}

#[test]
fn rdb_write_succeeds_with_partial_writes() {
    let mut dbs = DbSet::new(16);
    dbs.get_mut(0)
        .unwrap()
        .set(b"key".to_vec(), Robj::string("value"));
    let mut writer = FailAfterWrites::new(usize::MAX);

    write_rdb(&dbs, &mut writer, 0).unwrap();

    assert!(writer.writes > 1);
    let mut restored = DbSet::new(16);
    persistence::load_rdb_bytes_checked(&writer.bytes, &mut restored, true).unwrap();
    assert_eq!(
        restored
            .get(0)
            .unwrap()
            .get_no_expire(b"key")
            .unwrap()
            .as_bytes(),
        b"value"
    );
}

#[test]
fn rdb_write_propagates_immediate_io_failure() {
    let dbs = DbSet::new(16);
    let mut writer = FailAfterWrites::new(0);

    let error = write_rdb(&dbs, &mut writer, 0).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(writer.bytes.is_empty());
    assert_eq!(writer.writes, 0);
}

#[test]
fn rdb_write_propagates_midstream_io_failure() {
    let dbs = DbSet::new(16);
    let expected_len = crate::foundation::io::persistence::serialize_all(&dbs, 0).len();
    let mut writer = FailAfterWrites::new(2);

    let error = write_rdb(&dbs, &mut writer, 0).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(writer.writes, 2);
    assert!(!writer.bytes.is_empty());
    assert!(writer.bytes.len() < expected_len);
}

#[test]
fn rdb_contains_key_value() {
    let mut dbs = DbSet::new(16);
    dbs.get_mut(0)
        .unwrap()
        .set(b"foo".to_vec(), Robj::string("bar"));
    let bytes = crate::foundation::io::persistence::serialize_all(&dbs, 0);
    assert!(bytes.windows(3).any(|w| w == b"foo"));
    assert!(bytes.windows(3).any(|w| w == b"bar"));
    assert!(bytes.contains(&persistence::RDB_OPCODE_SELECTDB));
    assert!(bytes.contains(&persistence::RDB_OPCODE_RESIZEDB));
}

#[test]
fn rdb_expire_opcode_present() {
    let mut dbs = DbSet::new(16);
    {
        let db = dbs.get_mut(0).unwrap();
        db.set(b"k".to_vec(), Robj::string("v"));
        db.set_expire(b"k", now_ms() + 100_000);
    }
    let bytes = crate::foundation::io::persistence::serialize_all(&dbs, 0);
    assert!(bytes.contains(&persistence::RDB_OPCODE_EXPIRETIME_MS));
}

#[test]
fn rdb_full_roundtrip() {
    let mut dbs = DbSet::new(16);
    {
        let db = dbs.get_mut(0).unwrap();
        db.set(b"foo".to_vec(), Robj::string("bar"));
        db.set(b"counter".to_vec(), Robj::string("12345"));
        db.set_expire(b"foo", now_ms() + 100_000);
    }
    let bytes = crate::foundation::io::persistence::serialize_all(&dbs, 0);

    assert_eq!(&bytes[..9], b"VALKEY080");
    let n = bytes.len();
    let crc = persistence::crc64(0, &bytes[..n - 8]);
    assert_eq!(crc.to_le_bytes(), bytes[n - 8..]);

    let mut pos = 9usize;
    let mut found_foo = false;
    let mut found_counter = false;
    let mut foo_has_expire = false;
    loop {
        let op = bytes[pos];
        pos += 1;
        match op {
            persistence::RDB_OPCODE_EOF => break,
            persistence::RDB_OPCODE_AUX => {
                persistence::load_string(&bytes, &mut pos).unwrap();
                persistence::load_string(&bytes, &mut pos).unwrap();
            }
            persistence::RDB_OPCODE_SELECTDB => {
                persistence::load_len(&bytes, &mut pos).unwrap();
            }
            persistence::RDB_OPCODE_RESIZEDB => {
                persistence::load_len(&bytes, &mut pos).unwrap();
                persistence::load_len(&bytes, &mut pos).unwrap();
            }
            persistence::RDB_OPCODE_EXPIRETIME_MS => {
                pos += 8;
                let t = bytes[pos];
                pos += 1;
                assert_eq!(t, persistence::RDB_TYPE_STRING);
                let key = persistence::load_string(&bytes, &mut pos).unwrap();
                let val = persistence::load_string(&bytes, &mut pos).unwrap();
                if key == b"foo" {
                    foo_has_expire = true;
                    assert_eq!(val, b"bar");
                }
            }
            persistence::RDB_TYPE_STRING => {
                let key = persistence::load_string(&bytes, &mut pos).unwrap();
                let val = persistence::load_string(&bytes, &mut pos).unwrap();
                if key == b"foo" {
                    found_foo = true;
                    assert_eq!(val, b"bar");
                } else if key == b"counter" {
                    found_counter = true;
                    assert_eq!(val, b"12345");
                }
            }
            other => panic!("unexpected opcode {}", other),
        }
    }
    assert_eq!(pos, n - 8);
    assert!(foo_has_expire);
    assert!(found_counter);
    assert!(!found_foo);
}

#[test]
fn rdb_container_types_roundtrip() {
    let mut list = ListValue::new();
    list.push_back(Bytes::from_static(b"a"));
    list.push_back(Bytes::from_static(b"b"));
    list.push_back(Bytes::from_static(b"c"));

    let mut set = HashSet::new();
    set.insert(Bytes::from_static(b"x"));
    set.insert(Bytes::from_static(b"y"));

    let mut hash = IndexMap::new();
    hash.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
    hash.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));

    let mut zset = HashTable::new();
    zset.insert(Bytes::from_static(b"m1"), 1.5f64);
    zset.insert(Bytes::from_static(b"m2"), -2.25f64);

    let mut dbs = DbSet::new(16);
    {
        let db = dbs.get_mut(0).unwrap();
        db.set(b"mylist".to_vec(), Robj::ListQuicklist(list.clone()));
        db.set(
            b"myset".to_vec(),
            Robj::SetHashtable(ValkeySet::from_hashset(set.clone())),
        );
        db.set(
            b"myhash".to_vec(),
            Robj::HashHashtable(HashHt {
                data: Box::new(hash.clone()),
            }),
        );
        db.set(
            b"myzset".to_vec(),
            Robj::ZSetSkiplist(ZSetSl::from_dict(zset.clone())),
        );
    }
    let bytes = crate::foundation::io::persistence::serialize_all(&dbs, 0);

    let n = bytes.len();
    let crc = persistence::crc64(0, &bytes[..n - 8]);
    assert_eq!(crc.to_le_bytes(), bytes[n - 8..]);

    let mut pos = 9usize;
    let mut seen = 0;
    loop {
        let op = bytes[pos];
        pos += 1;
        match op {
            persistence::RDB_OPCODE_EOF => break,
            persistence::RDB_OPCODE_AUX => {
                persistence::load_string(&bytes, &mut pos).unwrap();
                persistence::load_string(&bytes, &mut pos).unwrap();
            }
            persistence::RDB_OPCODE_SELECTDB => {
                persistence::load_len(&bytes, &mut pos).unwrap();
            }
            persistence::RDB_OPCODE_RESIZEDB => {
                persistence::load_len(&bytes, &mut pos).unwrap();
                persistence::load_len(&bytes, &mut pos).unwrap();
            }
            type_byte => {
                let key = persistence::load_string(&bytes, &mut pos).unwrap();
                let obj = persistence::load_robj(&bytes, &mut pos, type_byte).unwrap();
                match key.as_slice() {
                    b"mylist" => assert_eq!(obj, Robj::ListQuicklist(list.clone())),
                    b"myset" => {
                        let Robj::SetHashtable(s) = obj else {
                            panic!("expected set")
                        };
                        assert_eq!(s.data.len(), set.len());
                        for elem in &set {
                            assert!(s.data.contains_key(elem.as_ref()));
                        }
                    }
                    b"myhash" => {
                        assert_eq!(
                            obj,
                            Robj::HashHashtable(HashHt {
                                data: Box::new(hash.clone()),
                            })
                        )
                    }
                    b"myzset" => {
                        assert_eq!(obj, Robj::ZSetSkiplist(ZSetSl::from_dict(zset.clone())))
                    }
                    other => panic!("unexpected key {:?}", other),
                }
                seen += 1;
            }
        }
    }
    assert_eq!(pos, n - 8);
    assert_eq!(seen, 4);
}
