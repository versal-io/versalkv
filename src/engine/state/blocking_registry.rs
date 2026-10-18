use crate::engine::command::SessionId;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

pub struct BlockingClientRegistry {
    blocked_clients: HashMap<(u32, Bytes), Vec<SessionId>>,
    keyless_clients: HashSet<SessionId>,
    count: usize,
}

impl BlockingClientRegistry {
    pub fn new() -> Self {
        Self {
            blocked_clients: HashMap::new(),
            keyless_clients: HashSet::new(),
            count: 0,
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn key_count(&self) -> usize {
        self.blocked_clients.len()
    }

    pub fn keys_with_multiple_waiters(&self) -> usize {
        self.blocked_clients
            .values()
            .filter(|queue| queue.len() > 1)
            .count()
    }

    pub fn increment_count(&mut self) {
        self.count += 1;
    }

    pub fn decrement_count(&mut self) {
        self.count = self.count.saturating_sub(1);
    }

    pub fn register_keyless_client(&mut self, session: SessionId) {
        if self.is_client_blocked(&session) {
            return;
        }
        self.keyless_clients.insert(session);
        self.count += 1;
    }

    pub fn unregister_keyless_client(&mut self, session: &SessionId) {
        if self.keyless_clients.remove(session) {
            self.count = self.count.saturating_sub(1);
        }
    }

    pub fn iter_keys(&self) -> impl Iterator<Item = &(u32, Bytes)> {
        self.blocked_clients.keys()
    }

    pub fn register_client(&mut self, db_id: u32, keys: &[Bytes], session: SessionId) {
        if self.is_client_blocked(&session) {
            return;
        }
        for key in keys {
            self.blocked_clients
                .entry((db_id, key.clone()))
                .or_default()
                .push(session);
        }
        self.count += 1;
    }

    pub fn unregister_client(&mut self, db_id: u32, keys: &[Bytes], session: &SessionId) {
        let mut removed = false;
        for key in keys {
            if let Some(queue) = self.blocked_clients.get_mut(&(db_id, key.clone())) {
                let before = queue.len();
                queue.retain(|candidate| candidate != session);
                if queue.len() < before {
                    removed = true;
                }
                if queue.is_empty() {
                    self.blocked_clients.remove(&(db_id, key.clone()));
                }
            }
        }
        if removed {
            self.count = self.count.saturating_sub(1);
        }
    }

    pub fn is_client_blocked(&self, session: &SessionId) -> bool {
        self.keyless_clients.contains(session)
            || self
                .blocked_clients
                .values()
                .any(|queue| queue.contains(session))
    }

    pub fn waiters_for_key(&self, db_id: u32, key: &[u8]) -> Vec<SessionId> {
        self.blocked_clients
            .get(&(db_id, Bytes::copy_from_slice(key)))
            .cloned()
            .unwrap_or_default()
    }

    pub fn blocked_tokens(&self) -> Vec<SessionId> {
        self.blocked_clients
            .values()
            .flatten()
            .copied()
            .chain(self.keyless_clients.iter().copied())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::SessionId;

    fn key(name: &'static [u8]) -> Bytes {
        Bytes::from_static(name)
    }

    #[test]
    fn register_unregister_and_count() {
        let mut registry = BlockingClientRegistry::new();
        let session = SessionId(7);
        let keys = vec![key(b"a"), key(b"b")];

        registry.register_client(0, &keys, session);
        assert_eq!(registry.count(), 1);
        assert!(registry.is_client_blocked(&session));
        assert!(!registry.waiters_for_key(0, b"a").is_empty());
        assert!(!registry.waiters_for_key(0, b"b").is_empty());

        registry.unregister_client(0, &keys, &session);
        assert_eq!(registry.count(), 0);
        assert!(!registry.is_client_blocked(&session));
        assert!(registry.waiters_for_key(0, b"a").is_empty());
        assert!(registry.waiters_for_key(0, b"b").is_empty());
    }

    #[test]
    fn keyless_clients_are_counted_and_listed() {
        let mut registry = BlockingClientRegistry::new();
        registry.register_keyless_client(SessionId(3));
        registry.register_keyless_client(SessionId(3));

        assert_eq!(registry.count(), 1);
        assert!(registry.is_client_blocked(&SessionId(3)));
        assert_eq!(registry.blocked_tokens(), vec![SessionId(3)]);

        registry.unregister_keyless_client(&SessionId(3));
        assert_eq!(registry.count(), 0);
        assert!(!registry.is_client_blocked(&SessionId(3)));
    }

    #[test]
    fn peek_and_count() {
        let mut registry = BlockingClientRegistry::new();
        registry.register_client(0, &[key(b"l")], SessionId(1));
        registry.register_client(0, &[key(b"l")], SessionId(2));
        registry.register_client(0, &[key(b"l")], SessionId(3));

        assert_eq!(registry.waiters_for_key(0, b"l").len(), 3);
        assert_eq!(
            registry.waiters_for_key(0, b"l").first().copied(),
            Some(SessionId(1))
        );

        assert_eq!(registry.waiters_for_key(0, b"l").len(), 3);
    }

    #[test]
    fn unregister_removes_from_all_keys() {
        let mut registry = BlockingClientRegistry::new();
        let session = SessionId(1);
        registry.register_client(0, &[key(b"a"), key(b"b")], session);
        assert_eq!(registry.count(), 1);
        assert!(!registry.waiters_for_key(0, b"a").is_empty());
        assert!(!registry.waiters_for_key(0, b"b").is_empty());

        registry.unregister_client(0, &[key(b"a"), key(b"b")], &session);
        assert_eq!(registry.count(), 0);
        assert!(registry.waiters_for_key(0, b"a").is_empty());
        assert!(registry.waiters_for_key(0, b"b").is_empty());
    }

    #[test]
    fn blocked_tokens_are_unique_across_keys() {
        let mut registry = BlockingClientRegistry::new();
        registry.register_client(0, &[key(b"a"), key(b"b")], SessionId(1));
        registry.register_client(0, &[key(b"b")], SessionId(2));

        let mut tokens = registry.blocked_tokens();
        tokens.sort_by_key(|token| token.0);

        assert_eq!(tokens, vec![SessionId(1), SessionId(2)]);
    }
}
