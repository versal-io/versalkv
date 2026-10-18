//! Process-wide keyed hashing for data dictionaries.

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) static HASH_SEED_0: AtomicU64 = AtomicU64::new(0);
pub(super) static HASH_SEED_1: AtomicU64 = AtomicU64::new(0);

fn process_seed_words() -> (u64, u64) {
    (
        HASH_SEED_0.load(Ordering::Relaxed),
        HASH_SEED_1.load(Ordering::Relaxed),
    )
}

#[allow(deprecated)]
fn seeded_hasher(seed: [u8; 16]) -> std::hash::SipHasher {
    let first = u64::from_le_bytes(seed[..8].try_into().expect("seed half is eight bytes"));
    let second = u64::from_le_bytes(seed[8..].try_into().expect("seed half is eight bytes"));
    std::hash::SipHasher::new_with_keys(first, second)
}

pub(crate) fn hash_bytes_with_seed(seed: [u8; 16], bytes: &[u8]) -> u64 {
    let mut hasher = seeded_hasher(seed);
    hasher.write(bytes);
    hasher.finish()
}

pub(crate) fn hash_value<T: Hash + ?Sized>(value: &T) -> u64 {
    let (first, second) = process_seed_words();
    #[allow(deprecated)]
    let mut hasher = std::hash::SipHasher::new_with_keys(first, second);
    value.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn hash_bytes(bytes: &[u8]) -> u64 {
    let (first, second) = process_seed_words();
    let mut seed = [0u8; 16];
    seed[..8].copy_from_slice(&first.to_le_bytes());
    seed[8..].copy_from_slice(&second.to_le_bytes());
    hash_bytes_with_seed(seed, bytes)
}
