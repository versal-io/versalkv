use crate::foundation::io::store::kv_engine::DbSet;
use crate::runtime::driver::{RuntimeClock, RuntimeInstant};

pub(super) fn delete_expired(dbs: &mut DbSet, clock: RuntimeClock) {
    let now_ms = clock.unix_ms().max(0) as u64;
    for index in 0..dbs.num_dbs() {
        if let Some(db) = dbs.get_mut(index as u32) {
            db.delete_expired_at(now_ms);
        }
    }
}

pub(super) fn next_deadline(dbs: &DbSet, now: RuntimeClock) -> Option<RuntimeInstant> {
    dbs.iter_dbs()
        .filter_map(|(_, db)| db.next_expire_at_ms())
        .min()
        .map(|deadline| now.deadline_for_unix_ms(deadline.min(i64::MAX as u64) as i64))
}
