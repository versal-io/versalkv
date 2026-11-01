use std::time::Instant;

use crate::foundation::io::store::kv_engine::now_ms;

const ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP: usize = 20;

const ACTIVE_EXPIRE_CYCLE_FAST_DURATION: u64 = 1000; // 1ms

const ACTIVE_EXPIRE_CYCLE_SLOW_DURATION: u64 = 25000; // 25ms

pub fn active_expire_cycle(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    fast: bool,
) -> (u64, usize) {
    active_expire_cycle_with_effort(db, fast, 1)
}

/// Active expiration with C Valkey's configurable `active-expire-effort`.
/// The public value is 1 through 10; higher effort increases both lookup
/// batches and the per-cycle time allowance by the same C-shaped increments.
pub fn active_expire_cycle_with_effort(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    fast: bool,
    effort: u8,
) -> (u64, usize) {
    active_expire_cycle_with_effort_and_filter(db, fast, effort, |_| true)
}

/// Restrict every key-level expiration step to values safe for mutation.
pub fn active_expire_cycle_with_effort_and_filter(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    fast: bool,
    effort: u8,
    should_expire_key: impl Fn(&[u8]) -> bool,
) -> (u64, usize) {
    let effort = effort.clamp(1, 10) as usize - 1;
    let timelimit = if fast {
        ACTIVE_EXPIRE_CYCLE_FAST_DURATION + ACTIVE_EXPIRE_CYCLE_FAST_DURATION / 4 * effort as u64
    } else {
        ACTIVE_EXPIRE_CYCLE_SLOW_DURATION + ACTIVE_EXPIRE_CYCLE_SLOW_DURATION / 4 * effort as u64
    };
    let start = Instant::now();
    let mut expired: u64 = 0;

    let volatile_hash_keys = db.hash_keys_with_volatile_fields();
    for key in &volatile_hash_keys {
        if should_expire_key(key) {
            expired += db.cleanup_expired_hash_fields(key);
        }
        if start.elapsed().as_micros() as u64 >= timelimit {
            return (
                expired,
                db.expires_count() + db.keys_with_volatile_items_count(),
            );
        }
    }

    if db.expires_count() == 0 {
        return (expired, db.keys_with_volatile_items_count());
    }

    loop {
        let lookups = ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP
            + ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP / 4 * effort;
        let num = lookups.min(db.expires_count());
        if num == 0 {
            break;
        }
        let now = now_ms();
        let mut to_delete: Vec<Vec<u8>> = Vec::with_capacity(num);
        let cursor = db.expiry_cursor;
        let next_cursor = db.scan_expiry(cursor, |key, &expire_time| {
            if to_delete.len() < num
                && now >= expire_time
                && db.should_expire_locally(key)
                && !db.is_importing_slot_key(key)
                && should_expire_key(key)
            {
                to_delete.push(key.to_vec());
            }
        });
        db.expiry_cursor = next_cursor;

        for key in &to_delete {
            db.del(key);
            db.active_expired_deletions.push(key.clone());
            expired += 1;
        }

        if start.elapsed().as_micros() as u64 >= timelimit {
            break;
        }

        if db.expires_count() == 0 {
            break;
        }

        if to_delete.is_empty() && next_cursor == 0 {
            break;
        }
    }

    let remaining = db.expires_count();
    (expired, remaining)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::kv_engine::Db;
    use crate::foundation::io::store::Robj;
    use crate::foundation::memory::metrics::stats::ServerStats;

    fn first_byte_slot(key: &[u8]) -> usize {
        key.first().copied().unwrap_or_default() as usize & 0b11
    }

    fn make_db_with_expired_keys(count: usize) -> Db {
        let mut db = Db::new();
        let now = crate::foundation::io::store::kv_engine::now_ms();
        for i in 0..count {
            let key = format!("k{}", i).into_bytes();
            db.set(key.clone(), Robj::string("v"));
            if i < count / 2 {
                db.set_expire(&key, now - 1000);
            } else {
                db.set_expire(&key, now + 10000);
            }
        }
        db
    }

    #[test]
    fn active_expire_empty_db_returns_zero() {
        let mut db = Db::new();
        let _stats = ServerStats::new();
        let (deleted, remaining) = active_expire_cycle(&mut db, false);
        assert_eq!(deleted, 0);
        assert_eq!(remaining, 0);
    }

    #[test]
    fn active_expire_deletes_expired_keys() {
        let mut db = make_db_with_expired_keys(10);
        let before_expired = db.expires_count();
        let _stats = ServerStats::new();
        let (deleted, remaining) = active_expire_cycle(&mut db, false);
        assert!(deleted > 0);
        assert!(remaining < before_expired);
        assert!(!db.active_expired_deletions.is_empty());
    }

    #[test]
    fn active_expire_fast_mode_runs() {
        let mut db = make_db_with_expired_keys(30);
        let _stats = ServerStats::new();
        let (deleted, _remaining) = active_expire_cycle(&mut db, true);
        assert!(deleted > 0);
    }

    #[test]
    fn active_expire_filter_protects_key_and_hash_field_metadata() {
        for should_allow_captured in [false, true] {
            let mut db = Db::new();
            let deadline = now_ms() - 1;
            for key in [b"captured".as_slice(), b"uncaptured".as_slice()] {
                db.set(key.to_vec(), Robj::string("value"));
                db.set_expire(key, deadline);
            }
            for key in [b"captured-hash".as_slice(), b"uncaptured-hash".as_slice()] {
                db.hash_set_with_limits(key, b"field", b"value", 512, 64)
                    .unwrap();
                db.set_hash_field_expire(key, b"field", deadline);
            }

            active_expire_cycle_with_effort_and_filter(&mut db, false, 10, |key| {
                should_allow_captured && key.starts_with(b"captured")
            });

            assert_eq!(
                db.get_allow_expired(b"captured").is_none(),
                should_allow_captured
            );
            assert_eq!(
                db.get_allow_expired(b"captured-hash").is_none(),
                should_allow_captured
            );
            assert_eq!(
                db.get_allow_expired(b"uncaptured").unwrap().as_bytes(),
                b"value"
            );
            assert_eq!(db.get_expire(b"uncaptured"), Some(deadline));
            assert_eq!(
                db.get_allow_expired(b"uncaptured-hash")
                    .unwrap()
                    .hash_get(b"field")
                    .unwrap(),
                b"value"
            );
            assert_eq!(
                db.get_hash_field_expire(b"uncaptured-hash", b"field"),
                Some(deadline)
            );
            assert!(db
                .active_expired_deletions
                .iter()
                .all(|key| key.as_slice() == b"captured"));
            assert!(db
                .pending_hexpired
                .iter()
                .all(|(key, _)| key.as_slice() == b"captured-hash"));
        }
    }

    #[test]
    fn active_expire_filter_preserves_import_and_replica_local_guards() {
        let mut db = Db::new_with_slot_router(2, first_byte_slot);
        let deadline = now_ms() - 1;
        for key in [b"\x01imported".as_slice(), b"\x02primary", b"\x03local"] {
            db.set(key.to_vec(), Robj::string("value"));
            db.set_expire(key, deadline);
        }
        db.set_slot_importing(1, true);
        db.set_replica_local_expire_only(true);
        db.mark_replica_local_expire(b"\x01imported");
        db.mark_replica_local_expire(b"\x03local");

        active_expire_cycle_with_effort_and_filter(&mut db, false, 1, |_| true);

        assert!(db.get_allow_expired(b"\x01imported").is_some());
        assert!(db.get_allow_expired(b"\x02primary").is_some());
        assert!(db.get_allow_expired(b"\x03local").is_none());
        assert_eq!(db.get_expire(b"\x01imported"), Some(deadline));
        assert_eq!(db.get_expire(b"\x02primary"), Some(deadline));
        // Imported slots are excluded from the serving keyspace counters.
        assert_eq!(db.expires_count(), 1);
    }

    #[test]
    fn higher_effort_uses_c_shaped_larger_lookup_batches() {
        let base = ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP;
        let high = base + base / 4 * 9;
        assert_eq!(base, 20);
        assert_eq!(high, 65);

        let mut db = make_db_with_expired_keys(80);
        let (deleted, _) = active_expire_cycle_with_effort(&mut db, false, 10);
        assert!(deleted > 0);
    }

    #[test]
    fn active_expire_scans_past_sparse_expiry_buckets() {
        let mut db = Db::new();
        let now = crate::foundation::io::store::kv_engine::now_ms();

        for index in 0..1000 {
            let key = format!("{{foo}}{index}").into_bytes();
            db.set(key.clone(), Robj::string("v"));
            db.set_expire(&key, now - 1);
        }
        let keep = b"{foo}100".to_vec();
        db.set_expire(&keep, now + 60_000);
        for key in [b"alice".as_slice(), b"key".as_slice()] {
            db.set(key.to_vec(), Robj::string("v"));
            db.set_expire(key, now - 1);
        }
        db.set_data_resize_policy(
            crate::foundation::io::store::structure::dict::ResizePolicy::Forbid,
        );
        for index in 0..1000 {
            if index != 100 {
                db.del(format!("{{foo}}{index}").as_bytes());
            }
        }

        let (deleted, _) = active_expire_cycle(&mut db, false);

        assert!(deleted > 0);
        assert!(db.get(&keep).is_some());
        assert_eq!(db.expires_count(), 1);
    }

    #[test]
    fn active_expire_skips_sparse_slot_dict_in_cluster_mode() {
        let mut db = Db::new_with_slot_router(2, first_byte_slot);
        let now = crate::foundation::io::store::kv_engine::now_ms();

        for index in 0..1000 {
            let key = vec![1, (index & 0xff) as u8, (index >> 8) as u8];
            db.set(key.clone(), Robj::string("v"));
            db.set_expire(&key, now - 1);
        }
        let other_slot_expired = vec![2, b'x'];
        db.set(other_slot_expired.clone(), Robj::string("v"));
        db.set_expire(&other_slot_expired, now - 1);

        db.set_data_resize_policy(
            crate::foundation::io::store::structure::dict::ResizePolicy::Forbid,
        );
        for index in 1..1000 {
            let key = vec![1, (index & 0xff) as u8, (index >> 8) as u8];
            db.del(&key);
        }

        let (deleted, remaining) = active_expire_cycle(&mut db, false);

        assert!(deleted > 0);
        assert_eq!(remaining, 1);
        assert_eq!(db.dbsize(), 1);
        assert!(db.get_allow_expired(&[1, 0, 0]).is_some());
    }
}
