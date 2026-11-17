use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

/// FxHash-based hasher for internal HashMaps whose keys are never
/// attacker-controlled (command names, connection tokens, event sequence
/// numbers, etc.). It mixes each field with a single multiply-rotate, which is
/// ~3-5x faster than the default SipHash hasher for small keys.
///
/// This mirrors the algorithm used by `rustc_hash::FxHasher`. It is *not*
/// suitable for keys that originate from untrusted input and could be used to
/// mount hash-flooding attacks; such maps must keep the default SipHash.
#[derive(Default)]
pub struct FxHasher(u64);

impl Hasher for FxHasher {
    #[inline]
    fn write_u8(&mut self, val: u8) {
        self.write_u64(val as u64);
    }

    #[inline]
    fn write_u16(&mut self, val: u16) {
        self.write_u64(val as u64);
    }

    #[inline]
    fn write_u32(&mut self, val: u32) {
        self.write_u64(val as u64);
    }

    #[inline]
    fn write_usize(&mut self, val: usize) {
        self.write_u64(val as u64);
    }

    #[inline]
    fn write_u64(&mut self, val: u64) {
        self.0 = self.0.wrapping_add(val);
        self.0 = self.0.wrapping_mul(0x517cc1b727220a95);
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = self.0.rotate_left(5).wrapping_add(byte as u64);
            self.0 = self.0.wrapping_mul(0x517cc1b727220a95);
        }
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// A `HashMap` using [`FxHasher`]. Drop-in replacement for
/// `std::collections::HashMap` on hot paths with internal-only keys.
pub type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

/// A `HashSet` using [`FxHasher`]. Drop-in replacement for
/// `std::collections::HashSet` on hot paths with internal-only keys.
pub type FxHashSet<T> = HashSet<T, BuildHasherDefault<FxHasher>>;

/// An `IndexSet` using [`FxHasher`]. Drop-in replacement for
/// `indexmap::IndexSet` on hot paths with internal-only keys.
pub type FxIndexSet<T> = IndexSet<T, BuildHasherDefault<FxHasher>>;
