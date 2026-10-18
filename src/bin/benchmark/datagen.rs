// TODO(dead_code): Task 3 exposes these primitives for later benchmark workload wiring.
#![allow(dead_code)]

pub struct DataGenConfig {
    pub size: usize,
    pub random: bool,
    pub range: Option<(usize, usize)>,
}

impl DataGenConfig {
    pub fn fixed(size: usize) -> Self {
        Self {
            size,
            random: false,
            range: None,
        }
    }

    pub fn range(min: usize, max: usize, random: bool) -> Self {
        Self {
            size: max,
            random,
            range: Some((min, max)),
        }
    }
}

pub struct DataGenerator {
    cfg: DataGenConfig,
    rng: FastRng,
    deterministic: Vec<u8>,
}

impl DataGenerator {
    pub fn new(cfg: DataGenConfig, seed: u64) -> Self {
        let deterministic = vec![b'x'; cfg.size];
        Self {
            cfg,
            rng: FastRng::new(seed),
            deterministic,
        }
    }

    pub fn next_value(&mut self) -> Vec<u8> {
        let len = self.next_len();
        if self.cfg.random {
            return (0..len).map(|_| self.rng.next_printable_ascii()).collect();
        }
        if self.deterministic.len() < len {
            self.deterministic.resize(len, b'x');
        }
        self.deterministic[..len].to_vec()
    }

    fn next_len(&mut self) -> usize {
        match self.cfg.range {
            Some((min, max)) if max > min => min + self.rng.next_usize(max - min + 1),
            Some((min, _)) => min,
            None => self.cfg.size,
        }
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

    fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        (self.next_u64() as usize) % max
    }

    fn next_printable_ascii(&mut self) -> u8 {
        b' ' + self.next_usize(95) as u8
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
    fn fixed_data_has_requested_size() {
        let mut gen = DataGenerator::new(DataGenConfig::fixed(8), 1);
        assert_eq!(gen.next_value().len(), 8);
    }

    #[test]
    fn ranged_data_stays_inside_bounds() {
        let mut gen = DataGenerator::new(DataGenConfig::range(4, 6, false), 1);
        for _ in 0..20 {
            let len = gen.next_value().len();
            assert!((4..=6).contains(&len));
        }
    }

    #[test]
    fn random_data_with_zero_seed_is_not_degenerate() {
        let mut gen = DataGenerator::new(DataGenConfig::range(8, 8, true), 0);
        let value = gen.next_value();
        assert!(value.iter().any(|byte| *byte != b' '));
    }
}
