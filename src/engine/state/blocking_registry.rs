use crate::engine::command::SessionId;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

const FORKLESS_DEFERRED_COMPACT_MIN_LEN: usize = 64;

#[derive(Clone, Copy)]
pub(crate) enum ForklessReadySelection {
    All,
    HeadRechecks,
}

enum ForklessReadyRoute {
    SnapshotProgress,
    HeadRecheck,
}

struct ForklessDeferredReadyKey {
    target: (u32, Bytes),
    route: ForklessReadyRoute,
}

pub struct BlockingClientRegistry {
    blocked_clients: HashMap<(u32, Bytes), Vec<SessionId>>,
    keyless_clients: HashSet<SessionId>,
    snapshot_guarded_clients: HashSet<SessionId>,
    forkless_deferred_ready_keys: Vec<Option<ForklessDeferredReadyKey>>,
    forkless_deferred_ready_index: HashMap<(u32, Bytes), usize>,
    count: usize,
}

impl BlockingClientRegistry {
    pub fn new() -> Self {
        Self {
            blocked_clients: HashMap::new(),
            keyless_clients: HashSet::new(),
            snapshot_guarded_clients: HashSet::new(),
            forkless_deferred_ready_keys: Vec::new(),
            forkless_deferred_ready_index: HashMap::new(),
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
            self.snapshot_guarded_clients.remove(session);
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

    /// Only the native runtime may opt known ordinary waiters into its
    /// pre-wakeup snapshot barrier. Generic and module registrations stay unknown.
    pub(crate) fn register_snapshot_guarded_client(
        &mut self,
        db_id: u32,
        keys: &[Bytes],
        session: SessionId,
    ) {
        if self.is_client_blocked(&session) {
            return;
        }
        self.register_client(db_id, keys, session);
        if !keys.is_empty() {
            self.snapshot_guarded_clients.insert(session);
        }
    }

    pub(crate) fn is_snapshot_guarded_client(&self, session: &SessionId) -> bool {
        self.snapshot_guarded_clients.contains(session)
    }

    pub(crate) fn defer_forkless_ready_key(&mut self, db_id: u32, key: &[u8]) {
        let target = (db_id, Bytes::copy_from_slice(key));
        // Pending head rechecks keep their marker until maintenance takes them,
        // so another signal cannot duplicate or replace the retained ready fact.
        if !self.forkless_deferred_ready_index.contains_key(&target) {
            self.forkless_deferred_ready_index
                .insert(target.clone(), self.forkless_deferred_ready_keys.len());
            self.forkless_deferred_ready_keys
                .push(Some(ForklessDeferredReadyKey {
                    target,
                    route: ForklessReadyRoute::SnapshotProgress,
                }));
            self.compact_forkless_deferred_ready_keys();
        }
    }

    pub(crate) fn is_forkless_ready_key_deferred(&self, db_id: u32, key: &[u8]) -> bool {
        !self.forkless_deferred_ready_index.is_empty()
            && self
                .forkless_deferred_ready_index
                .contains_key(&(db_id, Bytes::copy_from_slice(key)))
    }

    /// Select a single batch in first-encounter order, without grouping routes.
    pub(crate) fn take_forkless_deferred_ready_keys(
        &mut self,
        selection: ForklessReadySelection,
    ) -> Vec<(u32, Bytes)> {
        if self.forkless_deferred_ready_index.is_empty() {
            return Vec::new();
        }
        let mut keys = Vec::new();
        for slot in &mut self.forkless_deferred_ready_keys {
            let is_eligible = slot.as_ref().is_some_and(|deferred| {
                matches!(selection, ForklessReadySelection::All)
                    || matches!(deferred.route, ForklessReadyRoute::HeadRecheck)
            });
            if !is_eligible {
                continue;
            }
            if let Some(deferred) = slot.take() {
                self.forkless_deferred_ready_index.remove(&deferred.target);
                keys.push(deferred.target);
            }
        }
        self.compact_forkless_deferred_ready_keys();
        keys
    }

    fn compact_forkless_deferred_ready_keys(&mut self) {
        if self.forkless_deferred_ready_index.is_empty() {
            self.forkless_deferred_ready_keys.clear();
            return;
        }
        let slot_count = self.forkless_deferred_ready_keys.len();
        if slot_count <= FORKLESS_DEFERRED_COMPACT_MIN_LEN
            || slot_count / 2 < self.forkless_deferred_ready_index.len()
        {
            return;
        }
        // Compact only after at least half the slots are dead, amortizing
        // cancellation cost while preserving encounter order and live indices.
        let mut next_index = 0;
        self.forkless_deferred_ready_keys.retain(|slot| {
            let Some(deferred) = slot else {
                return false;
            };
            self.forkless_deferred_ready_index
                .insert(deferred.target.clone(), next_index);
            next_index += 1;
            true
        });
    }

    pub fn unregister_client(&mut self, db_id: u32, keys: &[Bytes], session: &SessionId) {
        let mut removed = false;
        for key in keys {
            let target = (db_id, key.clone());
            if let Some(queue) = self.blocked_clients.get_mut(&target) {
                let is_removing_head = queue.first() == Some(session);
                let before = queue.len();
                queue.retain(|candidate| candidate != session);
                if queue.len() < before {
                    removed = true;
                }
                if queue.is_empty() {
                    self.blocked_clients.remove(&target);
                    if let Some(index) = self.forkless_deferred_ready_index.remove(&target) {
                        self.forkless_deferred_ready_keys[index] = None;
                        self.compact_forkless_deferred_ready_keys();
                    }
                } else if is_removing_head {
                    if let Some(&index) = self.forkless_deferred_ready_index.get(&target) {
                        if let Some(deferred) = &mut self.forkless_deferred_ready_keys[index] {
                            // The old head's barrier no longer governs its followers.
                            // Preserve the ready fact for their next maintenance turn.
                            deferred.route = ForklessReadyRoute::HeadRecheck;
                        }
                    }
                }
            }
        }
        if removed {
            self.snapshot_guarded_clients.remove(session);
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

    #[test]
    fn snapshot_guard_is_explicit_and_cleared_by_unregister() {
        let mut registry = BlockingClientRegistry::new();
        let session = SessionId(1);
        let keys = [key(b"a")];
        registry.register_client(0, &keys, session);
        assert!(!registry.is_snapshot_guarded_client(&session));
        registry.register_snapshot_guarded_client(0, &keys, session);
        assert!(!registry.is_snapshot_guarded_client(&session));
        assert_eq!(registry.count(), 1);
        registry.unregister_client(0, &keys, &session);

        registry.register_snapshot_guarded_client(0, &keys, session);
        assert!(registry.is_snapshot_guarded_client(&session));
        registry.unregister_client(0, &keys, &session);
        assert!(!registry.is_snapshot_guarded_client(&session));
        registry.register_client(0, &keys, session);
        assert!(!registry.is_snapshot_guarded_client(&session));
    }

    #[test]
    fn deferred_ready_keys_keep_first_order_and_cleanup_last_waiter() {
        let mut registry = BlockingClientRegistry::new();
        registry.register_snapshot_guarded_client(0, &[key(b"a"), key(b"b")], SessionId(1));
        registry.register_snapshot_guarded_client(0, &[key(b"a")], SessionId(2));
        registry.register_snapshot_guarded_client(1, &[key(b"a")], SessionId(3));
        registry.defer_forkless_ready_key(0, b"b");
        registry.defer_forkless_ready_key(0, b"a");
        registry.defer_forkless_ready_key(0, b"b");
        registry.defer_forkless_ready_key(1, b"a");

        registry.unregister_client(0, &[key(b"a"), key(b"b")], &SessionId(1));
        assert!(!registry.is_forkless_ready_key_deferred(0, b"b"));
        assert!(registry.is_forkless_ready_key_deferred(0, b"a"));
        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::All),
            vec![(0, key(b"a")), (1, key(b"a"))]
        );
        assert!(!registry.is_forkless_ready_key_deferred(0, b"a"));
        registry.defer_forkless_ready_key(0, b"a");
        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::All),
            vec![(0, key(b"a"))]
        );
        registry.defer_forkless_ready_key(1, b"a");
        registry.unregister_client(1, &[key(b"a")], &SessionId(3));
        assert!(registry
            .take_forkless_deferred_ready_keys(ForklessReadySelection::All)
            .is_empty());
    }

    #[test]
    fn deferred_ready_selection_preserves_mixed_route_order() {
        let mut registry = BlockingClientRegistry::new();
        registry.register_client(0, &[key(b"a"), key(b"c")], SessionId(1));
        registry.register_client(0, &[key(b"a"), key(b"c")], SessionId(2));
        registry.register_client(1, &[key(b"b")], SessionId(3));
        registry.defer_forkless_ready_key(1, b"b");
        registry.defer_forkless_ready_key(0, b"a");
        registry.defer_forkless_ready_key(0, b"c");
        registry.unregister_client(0, &[key(b"a"), key(b"c")], &SessionId(1));
        registry.defer_forkless_ready_key(0, b"a");

        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::All),
            vec![(1, key(b"b")), (0, key(b"a")), (0, key(b"c"))]
        );
        assert!(!registry.is_forkless_ready_key_deferred(0, b"a"));
        assert!(!registry.is_forkless_ready_key_deferred(1, b"b"));
    }

    #[test]
    fn deferred_head_rechecks_leave_progress_markers_and_order_intact() {
        let mut registry = BlockingClientRegistry::new();
        registry.register_client(0, &[key(b"a"), key(b"c")], SessionId(1));
        registry.register_client(0, &[key(b"a"), key(b"c")], SessionId(2));
        registry.register_client(0, &[key(b"b")], SessionId(3));
        registry.defer_forkless_ready_key(0, b"c");
        registry.defer_forkless_ready_key(0, b"b");
        registry.defer_forkless_ready_key(0, b"a");
        registry.unregister_client(0, &[key(b"a"), key(b"c")], &SessionId(1));

        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::HeadRechecks),
            vec![(0, key(b"c")), (0, key(b"a"))]
        );
        assert!(registry
            .take_forkless_deferred_ready_keys(ForklessReadySelection::HeadRechecks)
            .is_empty());
        assert!(registry.is_forkless_ready_key_deferred(0, b"b"));
        assert!(!registry.is_forkless_ready_key_deferred(0, b"a"));
        registry.defer_forkless_ready_key(0, b"b");
        registry.defer_forkless_ready_key(0, b"a");
        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::All),
            vec![(0, key(b"b")), (0, key(b"a"))]
        );
    }

    #[test]
    fn deferred_ready_cancellation_compacts_and_updates_live_indices() {
        let mut registry = BlockingClientRegistry::new();
        for db_id in 0..256 {
            registry.register_client(db_id, &[key(b"a")], SessionId(u64::from(db_id) + 1));
            registry.defer_forkless_ready_key(db_id, b"a");
        }
        for db_id in 0..192 {
            registry.unregister_client(db_id, &[key(b"a")], &SessionId(u64::from(db_id) + 1));
            assert!(!registry.is_forkless_ready_key_deferred(db_id, b"a"));
            assert!(
                registry.forkless_deferred_ready_keys.len()
                    <= 64.max(2 * registry.forkless_deferred_ready_index.len())
            );
        }

        registry.register_client(200, &[key(b"a")], SessionId(1000));
        registry.unregister_client(200, &[key(b"a")], &SessionId(201));
        registry.unregister_client(250, &[key(b"a")], &SessionId(251));
        registry.register_client(300, &[key(b"a")], SessionId(1001));
        registry.unregister_client(300, &[key(b"a")], &SessionId(1001));
        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::HeadRechecks),
            vec![(200, key(b"a"))]
        );
        let expected: Vec<_> = (192..256)
            .filter(|db_id| *db_id != 200 && *db_id != 250)
            .map(|db_id| (db_id, key(b"a")))
            .collect();
        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::All),
            expected
        );
        assert!(registry.forkless_deferred_ready_keys.is_empty());
        assert!(registry.forkless_deferred_ready_index.is_empty());
    }

    #[test]
    fn deferred_ready_reregistration_does_not_revive_cancelled_slot() {
        let mut registry = BlockingClientRegistry::new();
        registry.register_client(0, &[key(b"a")], SessionId(1));
        registry.register_client(0, &[key(b"b")], SessionId(2));
        registry.defer_forkless_ready_key(0, b"a");
        registry.defer_forkless_ready_key(0, b"b");
        registry.unregister_client(0, &[key(b"a")], &SessionId(1));
        registry.register_client(0, &[key(b"a")], SessionId(3));
        registry.defer_forkless_ready_key(0, b"a");
        registry.defer_forkless_ready_key(0, b"a");
        for _ in 0..130 {
            registry.unregister_client(0, &[key(b"a")], &SessionId(3));
            registry.register_client(0, &[key(b"a")], SessionId(3));
            registry.defer_forkless_ready_key(0, b"a");
            assert!(
                registry.forkless_deferred_ready_keys.len()
                    <= 64.max(2 * registry.forkless_deferred_ready_index.len())
            );
        }
        assert_eq!(
            registry.take_forkless_deferred_ready_keys(ForklessReadySelection::All),
            vec![(0, key(b"b")), (0, key(b"a"))]
        );
    }
}
