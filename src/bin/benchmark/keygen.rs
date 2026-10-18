// TODO(dead_code): Task 3 exposes these primitives for later benchmark workload wiring.
#![allow(dead_code)]

use crate::cli::KeyPattern;

pub struct KeyGenConfig {
    pub prefix: String,
    pub minimum: u64,
    pub maximum: u64,
    pub pattern: KeyPattern,
    pub client_id: usize,
    pub total_clients: usize,
}

impl KeyGenConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.maximum < self.minimum {
            return Err("--key-maximum must be greater than or equal to --key-minimum".to_string());
        }
        if self.total_clients == 0 {
            return Err("total_clients must be greater than zero".to_string());
        }
        if self.client_id >= self.total_clients {
            return Err("client_id must be less than total_clients".to_string());
        }
        Ok(())
    }

    fn range_len(&self) -> u128 {
        u128::from(self.maximum) - u128::from(self.minimum) + 1
    }
}

pub struct KeyGenerator {
    cfg: KeyGenConfig,
    rng: FastRng,
    seq: u64,
}

impl KeyGenerator {
    pub fn new(cfg: KeyGenConfig, seed: u64) -> Self {
        cfg.validate()
            .expect("KeyGenConfig must be valid before constructing KeyGenerator");
        let seq = match cfg.pattern {
            KeyPattern::Parallel => parallel_start(&cfg),
            KeyPattern::Random | KeyPattern::Sequential => cfg.minimum,
        };
        Self {
            cfg,
            rng: FastRng::new(seed),
            seq,
        }
    }

    pub fn next_key(&mut self) -> Vec<u8> {
        let id = match self.cfg.pattern {
            KeyPattern::Random => self.next_random_id(),
            KeyPattern::Sequential => self.next_sequential_id(),
            KeyPattern::Parallel => self.next_parallel_id(),
        };
        let mut key = Vec::with_capacity(self.cfg.prefix.len() + 20);
        key.extend_from_slice(self.cfg.prefix.as_bytes());
        key.extend_from_slice(id.to_string().as_bytes());
        key
    }

    fn next_random_id(&mut self) -> u64 {
        let offset = u128::from(self.rng.next_u64()) % self.cfg.range_len();
        self.cfg.minimum.wrapping_add(offset as u64)
    }

    fn next_sequential_id(&mut self) -> u64 {
        let id = self.seq;
        self.seq = if self.seq >= self.cfg.maximum {
            self.cfg.minimum
        } else {
            self.seq + 1
        };
        id
    }

    fn next_parallel_id(&mut self) -> u64 {
        let id = self.seq;
        self.seq = next_parallel_value(&self.cfg, self.seq);
        id
    }
}

fn parallel_start(cfg: &KeyGenConfig) -> u64 {
    let range_len = cfg.range_len();
    let offset = (cfg.client_id as u128) % range_len;
    cfg.minimum.wrapping_add(offset as u64)
}

fn next_parallel_value(cfg: &KeyGenConfig, current: u64) -> u64 {
    let next = u128::from(current) + cfg.total_clients as u128;
    if next <= u128::from(cfg.maximum) {
        next as u64
    } else {
        parallel_start(cfg)
    }
}

struct FastRng(u64);

impl FastRng {
    fn new(seed: u64) -> Self {
        Self(nonzero_seed(seed))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn nonzero_seed(seed: u64) -> u64 {
    if seed == 0 {
        0x9e37_79b9_7f4a_7c15
    } else {
        seed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_keys_walk_range_and_wrap() {
        let cfg = KeyGenConfig {
            prefix: "memtier-".to_string(),
            minimum: 2,
            maximum: 4,
            pattern: KeyPattern::Sequential,
            client_id: 0,
            total_clients: 1,
        };
        let mut gen = KeyGenerator::new(cfg, 7);
        assert_eq!(gen.next_key(), b"memtier-2".to_vec());
        assert_eq!(gen.next_key(), b"memtier-3".to_vec());
        assert_eq!(gen.next_key(), b"memtier-4".to_vec());
        assert_eq!(gen.next_key(), b"memtier-2".to_vec());
    }

    #[test]
    fn parallel_keys_partition_by_client() {
        let cfg = KeyGenConfig {
            prefix: "k:".to_string(),
            minimum: 0,
            maximum: 9,
            pattern: KeyPattern::Parallel,
            client_id: 1,
            total_clients: 2,
        };
        let mut gen = KeyGenerator::new(cfg, 7);
        assert_eq!(gen.next_key(), b"k:1".to_vec());
        assert_eq!(gen.next_key(), b"k:3".to_vec());
        assert_eq!(gen.next_key(), b"k:5".to_vec());
    }

    #[test]
    fn random_keys_with_zero_seed_are_not_degenerate() {
        let cfg = KeyGenConfig {
            prefix: "k:".to_string(),
            minimum: 0,
            maximum: 9,
            pattern: KeyPattern::Random,
            client_id: 0,
            total_clients: 1,
        };
        let mut gen = KeyGenerator::new(cfg, 0);
        let keys: Vec<Vec<u8>> = (0..8).map(|_| gen.next_key()).collect();
        assert!(keys.iter().any(|key| key != b"k:0"));
    }
}
