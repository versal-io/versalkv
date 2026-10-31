use std::collections::HashMap;
use std::time::{Duration, Instant};

use sha1::Digest;

struct CachedScript {
    pub body: String,
    pub memory_bytes: usize,
    pub evictable: bool,
}

pub struct ScriptCache {
    scripts: HashMap<String, CachedScript>,
    lru_order: Vec<String>,
    total_memory: usize,
    eval_memory: usize,
    evictable_count: usize,
    evicted_scripts: usize,
}

impl ScriptCache {
    pub fn new() -> Self {
        Self {
            scripts: HashMap::new(),
            lru_order: Vec::new(),
            total_memory: 0,
            eval_memory: 0,
            evictable_count: 0,
            evicted_scripts: 0,
        }
    }

    pub fn compute_sha1(script: &str) -> String {
        let mut hasher = sha1::Sha1::new();
        hasher.update(script.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn lookup(&mut self, sha1: &str) -> Option<&str> {
        let sha1 = sha1.to_ascii_lowercase();
        if self.scripts.contains_key(&sha1) {
            if let Some(pos) = self.lru_order.iter().position(|x| x == &sha1) {
                self.lru_order.remove(pos);
                self.lru_order.push(sha1.clone());
            }
            self.scripts.get(&sha1).map(|s| s.body.as_str())
        } else {
            None
        }
    }

    pub fn peek(&self, sha1: &str) -> Option<&str> {
        self.scripts
            .get(&sha1.to_ascii_lowercase())
            .map(|s| s.body.as_str())
    }

    pub fn store(&mut self, sha1: String, body: String) -> Vec<String> {
        self.store_with_policy(sha1, body, true)
    }

    pub fn store_persistent(&mut self, sha1: String, body: String) {
        self.store_with_policy(sha1, body, false);
    }

    fn store_with_policy(&mut self, sha1: String, body: String, evictable: bool) -> Vec<String> {
        let memory_bytes = body.len() + sha1.len();

        if let Some(existing) = self.scripts.get_mut(&sha1) {
            if existing.evictable && !evictable {
                self.evictable_count -= 1;
                self.eval_memory -= existing.memory_bytes;
                existing.evictable = false;
            }
            if let Some(pos) = self.lru_order.iter().position(|x| x == &sha1) {
                self.lru_order.remove(pos);
            }
            if existing.evictable {
                self.lru_order.push(sha1);
            }
            return Vec::new();
        }

        let mut retired = Vec::new();
        if evictable {
            while self.evictable_count >= 500 {
                let Some(digest) = self.evict_oldest() else {
                    break;
                };
                retired.push(digest);
            }
        }

        self.scripts.insert(
            sha1.clone(),
            CachedScript {
                body,
                memory_bytes,
                evictable,
            },
        );
        self.total_memory += memory_bytes;
        if evictable {
            self.lru_order.push(sha1);
            self.evictable_count += 1;
            self.eval_memory += memory_bytes;
        }
        retired
    }

    pub fn exists(&self, sha1: &str) -> bool {
        self.scripts.contains_key(&sha1.to_ascii_lowercase())
    }

    pub fn flush(&mut self) {
        self.scripts.clear();
        self.lru_order.clear();
        self.total_memory = 0;
        self.eval_memory = 0;
        self.evictable_count = 0;
    }

    pub fn len(&self) -> usize {
        self.scripts.len()
    }

    pub fn memory_usage(&self) -> usize {
        // Valkey accounts for the separate LRU SHA allocation, its string
        // header, and the three pointers in each LRU list node.
        self.total_memory
            + self.scripts.len() * std::mem::size_of::<CachedScript>()
            + self.evictable_count * Self::lru_entry_bytes()
    }

    pub(crate) fn eval_memory_usage(&self) -> usize {
        self.eval_memory
            + self.evictable_count * (std::mem::size_of::<CachedScript>() + Self::lru_entry_bytes())
    }

    fn lru_entry_bytes() -> usize {
        40 + 4 * std::mem::size_of::<usize>()
    }

    /// Clear existing pressure without reserving space for the next script.
    /// The release callback is included in the sixteen-eviction time checks.
    pub(crate) fn evict_memory_slice(
        &mut self,
        limit: usize,
        mut release: impl FnMut(&str),
    ) -> bool {
        let start = Instant::now();
        let mut evictions = 0usize;
        while self.eval_memory_usage() > limit {
            let Some(digest) = self.evict_oldest() else {
                return false;
            };
            release(&digest);
            evictions += 1;
            if evictions.is_multiple_of(16) && start.elapsed() > Duration::from_micros(500) {
                break;
            }
        }
        self.eval_memory_usage() > limit
    }

    pub fn evicted_scripts(&self) -> usize {
        self.evicted_scripts
    }

    pub fn reset_stats(&mut self) {
        self.evicted_scripts = 0;
    }

    fn evict_oldest(&mut self) -> Option<String> {
        if self.lru_order.is_empty() {
            return None;
        }
        let oldest = self.lru_order.remove(0);
        let entry = self.scripts.remove(&oldest)?;
        self.total_memory -= entry.memory_bytes;
        self.eval_memory -= entry.memory_bytes;
        self.evictable_count -= 1;
        self.evicted_scripts += 1;
        Some(oldest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_compute() {
        let hash = ScriptCache::compute_sha1("return 42");
        assert_eq!(hash.len(), 40);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_store_and_lookup() {
        let mut cache = ScriptCache::new();
        let script = "return 42";
        let sha1 = ScriptCache::compute_sha1(script);
        cache.store(sha1.clone(), script.to_string());

        let found = cache.lookup(&sha1);
        assert_eq!(found, Some(script));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_lookup_accepts_uppercase_sha1() {
        let mut cache = ScriptCache::new();
        let script = "return 42";
        let sha1 = ScriptCache::compute_sha1(script);
        cache.store(sha1.clone(), script.to_string());

        assert_eq!(cache.lookup(&sha1.to_ascii_uppercase()), Some(script));
        assert!(cache.exists(&sha1.to_ascii_uppercase()));
    }

    #[test]
    fn test_lookup_miss() {
        let mut cache = ScriptCache::new();
        let result = cache.lookup("nonexistent");
        assert_eq!(result, None);
    }

    #[test]
    fn test_flush() {
        let mut cache = ScriptCache::new();
        let script = "return 1";
        let sha1 = ScriptCache::compute_sha1(script);
        cache.store(sha1.clone(), script.to_string());
        assert_eq!(cache.len(), 1);

        cache.flush();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.lookup(&sha1), None);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = ScriptCache::new();
        for i in 0..500 {
            let script = format!("return {}", i);
            let sha1 = ScriptCache::compute_sha1(&script);
            cache.store(sha1, script);
        }
        let first_sha1 = ScriptCache::compute_sha1("return 0");
        let second_sha1 = ScriptCache::compute_sha1("return 1");
        assert_eq!(cache.lookup(&first_sha1), Some("return 0"));
        assert!(cache.exists(&second_sha1));

        cache.store(
            ScriptCache::compute_sha1("return 500"),
            "return 500".to_string(),
        );
        assert_eq!(cache.len(), 500);
        assert_eq!(cache.evicted_scripts(), 1);
        assert!(cache.exists(&first_sha1));
        assert!(!cache.exists(&second_sha1));
    }

    #[test]
    fn test_default_budget_preserves_large_eval_scripts() {
        let mut cache = ScriptCache::new();
        for index in 0..500 {
            let body = format!("--{}\nreturn {index}", "x".repeat(100_000));
            cache.store(ScriptCache::compute_sha1(&body), body);
        }
        assert_eq!(cache.len(), 500);
        assert_eq!(cache.evicted_scripts(), 0);
    }

    #[test]
    fn test_memory_slice_uses_eval_only_pressure_and_soft_admission() {
        let mut cache = ScriptCache::new();
        let pinned = "p".repeat(1_000_000);
        cache.store_persistent("pinned".to_string(), pinned);
        assert_eq!(cache.eval_memory_usage(), 0);
        assert!(!cache.evict_memory_slice(128 * 1024, |_| panic!("LOAD must remain pinned")));

        let body = "x".repeat(1_000_000);
        cache.store("large".to_string(), body);
        assert!(cache.exists("large"));
        assert_eq!(cache.evicted_scripts(), 0);
        let mut released = Vec::new();
        assert!(!cache.evict_memory_slice(128 * 1024, |digest| {
            released.push(digest.to_string());
        }));
        assert_eq!(released, ["large"]);
        assert!(cache.exists("pinned"));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.eval_memory_usage(), 0);

        cache.store("promoted".to_string(), "return 1".to_string());
        let before = cache.memory_usage();
        cache.store_persistent("promoted".to_string(), "return 1".to_string());
        assert_eq!(cache.eval_memory_usage(), 0);
        assert!(before - cache.memory_usage() > 64);
        cache.store("promoted".to_string(), "return 1".to_string());
        assert_eq!(cache.eval_memory_usage(), 0);
        assert_eq!(cache.evicted_scripts(), 1);
    }

    #[test]
    fn test_memory_slice_checks_time_after_sixteen_releases() {
        let mut cache = ScriptCache::new();
        for index in 0..32 {
            cache.store(index.to_string(), "x".repeat(10_000));
        }
        let mut released = 0;
        assert!(cache.evict_memory_slice(0, |_| {
            released += 1;
            if released == 1 {
                std::thread::sleep(Duration::from_millis(2));
            }
        }));
        assert_eq!(released, 16);
        assert_eq!(cache.len(), 16);
        assert!(!cache.evict_memory_slice(0, |_| {}));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.evicted_scripts(), 32);
    }

    #[test]
    fn test_exists() {
        let mut cache = ScriptCache::new();
        let script = "return 42";
        let sha1 = ScriptCache::compute_sha1(script);
        assert!(!cache.exists(&sha1));

        cache.store(sha1.clone(), script.to_string());
        assert!(cache.exists(&sha1));
    }

    #[test]
    fn test_total_memory() {
        let mut cache = ScriptCache::new();
        let script = "return 42";
        let sha1 = ScriptCache::compute_sha1(script);
        cache.store(sha1, script.to_string());
        assert!(cache.total_memory > 0);
    }
}
