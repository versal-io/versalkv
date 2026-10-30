
use std::collections::HashMap;

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
    max_memory: usize,
    evictable_count: usize,
    evicted_scripts: usize,
}

impl ScriptCache {
    pub fn new() -> Self {
        Self {
            scripts: HashMap::new(),
            lru_order: Vec::new(),
            total_memory: 0,
            max_memory: 5 * 1024 * 1024, // 5 MB
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

    pub fn store(&mut self, sha1: String, body: String) {
        self.store_with_policy(sha1, body, true);
    }

    pub fn store_persistent(&mut self, sha1: String, body: String) {
        self.store_with_policy(sha1, body, false);
    }

    fn store_with_policy(&mut self, sha1: String, body: String, evictable: bool) {
        let memory_bytes = body.len() + sha1.len();

        if let Some(existing) = self.scripts.get_mut(&sha1) {
            self.total_memory = self
                .total_memory
                .saturating_sub(existing.memory_bytes)
                .saturating_add(memory_bytes);
            existing.body = body;
            if existing.evictable && !evictable {
                self.evictable_count -= 1;
                existing.evictable = false;
            }
            existing.memory_bytes = memory_bytes;
            if let Some(pos) = self.lru_order.iter().position(|x| x == &sha1) {
                self.lru_order.remove(pos);
            }
            if existing.evictable {
                self.lru_order.push(sha1);
            }
            return;
        }

        self.evict_until(Some(memory_bytes), evictable);

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
        }
    }

    pub fn exists(&self, sha1: &str) -> bool {
        self.scripts.contains_key(&sha1.to_ascii_lowercase())
    }

    pub fn flush(&mut self) {
        self.scripts.clear();
        self.lru_order.clear();
        self.total_memory = 0;
        self.evictable_count = 0;
    }

    pub fn len(&self) -> usize {
        self.scripts.len()
    }

    pub fn memory_usage(&self) -> usize {
        // Valkey accounts for the separate LRU SHA allocation, its string
        // header, and the three pointers in each LRU list node.
        let lru_entry_bytes = 40 + 4 * std::mem::size_of::<usize>();
        self.total_memory
            + self.scripts.len() * std::mem::size_of::<CachedScript>()
            + self.evictable_count * lru_entry_bytes
    }

    pub fn evicted_scripts(&self) -> usize {
        self.evicted_scripts
    }

    pub fn reset_stats(&mut self) {
        self.evicted_scripts = 0;
    }

    fn evict_until(&mut self, needed: Option<usize>, adding_evictable: bool) {
        let needed = needed.unwrap_or(0);
        let target = if needed > self.max_memory {
            needed
        } else {
            self.max_memory - needed
        };

        while (self.total_memory > target || (adding_evictable && self.evictable_count >= 500))
            && !self.lru_order.is_empty()
        {
            let Some(pos) = self
                .lru_order
                .iter()
                .position(|key| self.scripts.get(key).is_some_and(|entry| entry.evictable))
            else {
                break;
            };
            let oldest = self.lru_order.remove(pos);
            if let Some(entry) = self.scripts.remove(&oldest) {
                self.total_memory -= entry.memory_bytes;
                self.evictable_count -= usize::from(entry.evictable);
                self.evicted_scripts += usize::from(entry.evictable);
            }
        }
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
        cache.max_memory = 100; //

        let mut last_sha1 = String::new();
        for i in 0..20 {
            let script = format!("return {}", i);
            let sha1 = ScriptCache::compute_sha1(&script);
            cache.store(sha1.clone(), script);
            last_sha1 = sha1;
        }

        assert!(cache.exists(&last_sha1));

        let first_script = "return 0";
        let first_sha1 = ScriptCache::compute_sha1(first_script);
        let found = cache.lookup(&first_sha1);
        assert!(found.is_none() || cache.len() < 20);
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
