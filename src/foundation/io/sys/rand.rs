use std::cell::RefCell;
use std::io::{self, Read};
use std::time::{SystemTime, UNIX_EPOCH};

struct Xoshiro256StarStar {
    s: [u64; 4],
}

impl Xoshiro256StarStar {
    fn new(seed: u64) -> Self {
        let mut s = [0u64; 4];
        let mut x = seed;
        for item in s.iter_mut() {
            x = x.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z = z ^ (z >> 31);
            *item = z;
        }
        Self { s }
    }

    fn next(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    fn next_usize(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        let max_u64 = max as u64;
        let threshold = (u64::MAX - max_u64 + 1) % max_u64;
        loop {
            let r = self.next();
            if r >= threshold {
                return (r % max_u64) as usize;
            }
        }
    }
}

thread_local! {
    static RNG: RefCell<Xoshiro256StarStar> = RefCell::new({
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let pid = std::process::id() as u64;
        let thread_addr = &RANDOM_THREAD_ADDR as *const u8 as u64;
        let seed = nanos ^ pid.wrapping_mul(0x9E3779B97F4A7C15) ^ thread_addr;
        Xoshiro256StarStar::new(seed)
    });
}

static RANDOM_THREAD_ADDR: u8 = 0;

/// Fill a buffer from the operating system cryptographic random source.
/// An error leaves the buffer unsuitable for use as random data.
pub fn fill_os_random_bytes(bytes: &mut [u8]) -> io::Result<()> {
    std::fs::File::open("/dev/urandom")?.read_exact(bytes)
}

pub fn random_usize(max: usize) -> usize {
    RNG.with(|rng| rng.borrow_mut().next_usize(max))
}

pub fn shuffle<T>(slice: &mut [T]) {
    RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        for i in (1..slice.len()).rev() {
            let j = rng.next_usize(i + 1);
            slice.swap(i, j);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chi_square(observed: &[usize], expected: f64) -> f64 {
        observed.iter().fold(0.0, |acc, &o| {
            let diff = o as f64 - expected;
            acc + diff * diff / expected
        })
    }

    #[test]
    fn test_fill_os_random_bytes() {
        fill_os_random_bytes(&mut []).expect("fill empty random buffer");
        let mut first = [0u8; 32];
        let mut second = [0u8; 32];
        fill_os_random_bytes(&mut first).expect("fill first random buffer");
        fill_os_random_bytes(&mut second).expect("fill second random buffer");
        assert_ne!(first, [0u8; 32]);
        assert_ne!(first, second);
    }

    #[test]
    fn test_random_usize_distribution() {
        let n_elements = 10;
        let n_trials = 10_000;
        let expected = n_trials as f64 / n_elements as f64;

        let mut counts = vec![0usize; n_elements];
        for _ in 0..n_trials {
            let idx = random_usize(n_elements);
            counts[idx] += 1;
        }

        let chi2 = chi_square(&counts, expected);
        let df = n_elements - 1;
        println!(
            "Chi-square test: chi2={:.2}, df={}, expected mean={:.1}",
            chi2, df, expected
        );
        println!("Observed counts: {:?}", counts);
        println!(
            "Min: {}, Max: {}",
            counts.iter().min().unwrap(),
            counts.iter().max().unwrap()
        );

        // Critical value for df=9, p=0.00001 is about 40
        // For 10000 trials, chi2 should usually be well under 40
        assert!(chi2 < 80.0, "Chi-square value too high: {:.2}", chi2);
    }

    #[test]
    fn test_shuffle_uniformity() {
        let n = 5;
        let n_trials = 10_000;
        let mut first_pos_counts = vec![vec![0usize; n]; n];

        for _ in 0..n_trials {
            let mut arr: Vec<usize> = (0..n).collect();
            shuffle(&mut arr);
            for (pos, &val) in arr.iter().enumerate() {
                first_pos_counts[val][pos] += 1;
            }
        }

        let expected = n_trials as f64 / n as f64;
        let mut max_chi2 = 0.0;
        for count in first_pos_counts[..n].iter() {
            let chi2 = chi_square(count, expected);
            if chi2 > max_chi2 {
                max_chi2 = chi2;
            }
        }
        println!(
            "Shuffle uniformity test: max chi2={:.2} across {} positions",
            max_chi2, n
        );
        assert!(max_chi2 < 100.0, "Max chi-square too high: {:.2}", max_chi2);
    }

    #[test]
    fn test_xoshiro_deterministic() {
        let mut rng1 = Xoshiro256StarStar::new(42);
        let mut rng2 = Xoshiro256StarStar::new(42);
        for _ in 0..100 {
            assert_eq!(rng1.next(), rng2.next());
        }
    }

    #[test]
    fn test_xoshiro_not_constant() {
        let mut rng = Xoshiro256StarStar::new(12345);
        let first = rng.next();
        let mut all_same = true;
        for _ in 0..100 {
            if rng.next() != first {
                all_same = false;
                break;
            }
        }
        assert!(!all_same, "RNG produces constant output");
    }
}
