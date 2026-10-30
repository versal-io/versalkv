pub mod db;
pub mod dbset;
mod eviction;
pub mod expire;
mod hash_field_expires;
mod module_values;
pub mod scan;

pub use crate::foundation::io::store::Robj;
pub(crate) use db::SnapshotValue;
pub use db::{Db, KeyMeta};
pub use dbset::{DbSet, SlotEntry};
pub use eviction::{EvictionCandidate, EvictionPolicy};

use crate::foundation::io::sys::time;

pub fn now_ms() -> u64 {
    time::mstime().max(0) as u64
}

#[cfg(test)]
mod metadata_tests;

#[cfg(test)]
mod tests;
