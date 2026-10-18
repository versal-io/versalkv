//! Native process hash-seed initialization and Cluster Scan fingerprinting.

use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

use super::configurable_hash::{HASH_SEED_0, HASH_SEED_1};

pub(crate) fn derive_seed(configured: &[u8]) -> [u8; 16] {
    let digest = Sha256::digest(configured);
    let mut seed = [0u8; 16];
    seed.copy_from_slice(&digest[..16]);
    seed
}

pub(crate) fn random_seed() -> [u8; 16] {
    let mut seed = [0u8; 16];
    for byte in &mut seed {
        *byte = crate::foundation::io::sys::rand::random_usize(256) as u8;
    }
    seed
}

pub(crate) fn install_process_seed(seed: [u8; 16]) {
    HASH_SEED_0.store(
        u64::from_le_bytes(seed[..8].try_into().expect("seed half is eight bytes")),
        Ordering::Relaxed,
    );
    HASH_SEED_1.store(
        u64::from_le_bytes(seed[8..].try_into().expect("seed half is eight bytes")),
        Ordering::Relaxed,
    );
}

fn wang_hash64(mut hash: u64) -> u64 {
    hash = (!hash).wrapping_add(hash << 21);
    hash ^= hash >> 24;
    hash = hash.wrapping_add(hash << 3).wrapping_add(hash << 8);
    hash ^= hash >> 14;
    hash = hash.wrapping_add(hash << 2).wrapping_add(hash << 4);
    hash ^= hash >> 28;
    hash.wrapping_add(hash << 31)
}

fn fingerprint_with_seed(seed: [u8; 16]) -> u32 {
    let first = u64::from_le_bytes(seed[..8].try_into().expect("seed half is eight bytes"));
    let second = u64::from_le_bytes(seed[8..].try_into().expect("seed half is eight bytes"));
    (wang_hash64(first ^ second) as u32).max(1)
}

pub(crate) fn process_seed_fingerprint() -> u32 {
    let first = HASH_SEED_0.load(Ordering::Relaxed);
    let second = HASH_SEED_1.load(Ordering::Relaxed);
    let mut seed = [0u8; 16];
    seed[..8].copy_from_slice(&first.to_le_bytes());
    seed[8..].copy_from_slice(&second.to_le_bytes());
    fingerprint_with_seed(seed)
}

#[cfg(test)]
mod tests {
    use super::{derive_seed, fingerprint_with_seed};
    use crate::foundation::io::store::structure::configurable_hash::hash_bytes_with_seed;

    #[test]
    fn derives_seed_from_all_bytes_including_embedded_nul() {
        let seed_a = derive_seed(b"valkey\0AAAAA");
        let seed_b = derive_seed(b"valkey\0BBBBB");

        assert_ne!(seed_a, seed_b);
        assert_eq!(
            seed_a,
            [
                0xdd, 0xc1, 0xf7, 0x59, 0xc0, 0xc6, 0xf8, 0x3a, 0xb1, 0x00, 0x10, 0x1a, 0x02, 0x78,
                0x6f, 0x4d,
            ]
        );
    }

    #[test]
    fn siphash24_matches_the_reference_empty_message_vector() {
        let seed = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];

        assert_eq!(hash_bytes_with_seed(seed, b""), 0x726f_db47_dd0e_0e31);
    }

    #[test]
    fn different_seeds_change_bucket_selection_for_a_fixed_corpus() {
        let seed_a = derive_seed(b"first");
        let seed_b = derive_seed(b"second");

        assert!((0u32..1024).any(|value| {
            let bytes = value.to_le_bytes();
            hash_bytes_with_seed(seed_a, &bytes) & 1023
                != hash_bytes_with_seed(seed_b, &bytes) & 1023
        }));
    }

    #[test]
    fn cluster_scan_fingerprint_matches_c_wang_hash() {
        let seed = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];

        assert_eq!(fingerprint_with_seed(seed), 0xe3c7_53fb);
        assert_ne!(
            fingerprint_with_seed(derive_seed(b"first")),
            fingerprint_with_seed(derive_seed(b"second"))
        );
    }
}
