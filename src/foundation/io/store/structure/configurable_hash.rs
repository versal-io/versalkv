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

/// Streaming djb2 hasher seeded with the process-wide random seed.
///
/// Every `write()` folds its bytes directly into the running djb2 state, so
/// keys of any length are hashed completely with no fixed-size buffer and no
/// truncation. C Valkey uses the same djb2 family for its main dict hash
/// function.
struct FastSeededHasher {
    state: u64,
    seed1: u64,
}

impl FastSeededHasher {
    fn new(seed: (u64, u64)) -> Self {
        Self {
            state: 5381 ^ seed.0,
            seed1: seed.1,
        }
    }
}

impl Hasher for FastSeededHasher {
    fn finish(&self) -> u64 {
        self.state ^ (self.state << 13) ^ self.seed1
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state = self.state.wrapping_mul(33).wrapping_add(byte as u64);
        }
    }
}

#[cfg(test)]
pub(crate) fn hash_bytes_with_seed(seed: [u8; 16], bytes: &[u8]) -> u64 {
    let first = u64::from_le_bytes(seed[..8].try_into().expect("seed half is eight bytes"));
    let second = u64::from_le_bytes(seed[8..].try_into().expect("seed half is eight bytes"));
    let mut hasher = FastSeededHasher::new((first, second));
    hasher.write(&bytes.len().to_le_bytes());
    hasher.write(bytes);
    hasher.finish()
}

/// Hash a `Hash` value through the streaming djb2 hasher.
///
/// For `[u8]` (and `StoredKey`, which delegates to `[u8]::hash`), the `Hash`
/// impl emits `usize::to_le_bytes(len)` followed by the key bytes. This is the
/// exact byte stream `hash_bytes` feeds, so sharing one hasher keeps the insert
/// and lookup paths identical for every key length.
pub(crate) fn hash_value<T: Hash + ?Sized>(value: &T) -> u64 {
    let mut hasher = FastSeededHasher::new(process_seed_words());
    value.hash(&mut hasher);
    hasher.finish()
}

/// Byte-slice hash that matches `hash_value::<[u8]>` and
/// `hash_value::<StoredKey>` exactly for keys of any length.
///
/// `[u8]::hash` writes `usize::to_le_bytes(len)` before the byte payload, so
/// the same prefix is written here before streaming the key into the shared
/// djb2 hasher.
pub(crate) fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = FastSeededHasher::new(process_seed_words());
    hasher.write(&bytes.len().to_le_bytes());
    hasher.write(bytes);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::{hash_bytes, hash_value};

    fn pattern(len: usize, kind: u8) -> Vec<u8> {
        match kind {
            0 => vec![0u8; len],
            1 => vec![0xffu8; len],
            2 => (0..len).map(|i| (i % 251) as u8).collect(),
            _ => (0..len)
                .map(|i| ((i.wrapping_mul(37)) ^ 0x5a) as u8)
                .collect(),
        }
    }

    #[test]
    fn hash_value_and_hash_bytes_agree_for_every_key_length() {
        for kind in 0u8..4 {
            for len in (0..=300).chain(vec![512, 1024, 4096]) {
                let key = pattern(len, kind);
                assert_eq!(
                    hash_value(key.as_slice()),
                    hash_bytes(&key),
                    "hash divergence at len={len}"
                );
            }
        }
    }

    #[test]
    fn long_keys_past_the_legacy_248_byte_limit_stay_consistent() {
        // Regression guard: the previous fixed 256-byte collector truncated the
        // insert path once the length prefix (eight bytes) plus the key exceeded
        // it, diverging from the full-length lookup path at key length 249.
        for len in [247, 248, 249, 256, 259, 300, 512] {
            let key = vec![0x7bu8; len];
            assert_eq!(hash_value(key.as_slice()), hash_bytes(&key));
        }
    }
}
