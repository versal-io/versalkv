use std::collections::HashMap;

use bytes::Bytes;

use crate::engine::command::SessionId;

use super::RuntimeInstant;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContinuationId(u64);

impl ContinuationId {
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExternalEventKey(u64);

impl ExternalEventKey {
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitCondition {
    Keys { db: u32, keys: Vec<Bytes> },
    Deadline(RuntimeInstant),
    External(ExternalEventKey),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuspendedCommand<T> {
    pub session: SessionId,
    pub continuation: ContinuationId,
    pub wait: WaitCondition,
    pub state: T,
}

pub struct ContinuationRegistry<T> {
    entries: HashMap<ContinuationId, SuspendedCommand<T>>,
    next_id: u64,
}

impl<T> Default for ContinuationRegistry<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            next_id: 0,
        }
    }
}

impl<T> ContinuationRegistry<T> {
    pub fn register(
        &mut self,
        session: SessionId,
        wait: WaitCondition,
        state: T,
    ) -> ContinuationId {
        self.next_id = self.next_id.saturating_add(1).max(1);
        let continuation = ContinuationId(self.next_id);
        self.entries.insert(
            continuation,
            SuspendedCommand {
                session,
                continuation,
                wait,
                state,
            },
        );
        continuation
    }

    pub fn take(&mut self, continuation: ContinuationId) -> Option<SuspendedCommand<T>> {
        self.entries.remove(&continuation)
    }

    pub fn get(&self, continuation: ContinuationId) -> Option<&SuspendedCommand<T>> {
        self.entries.get(&continuation)
    }

    pub fn cancel_session(&mut self, session: SessionId) -> Vec<SuspendedCommand<T>> {
        self.take_matching(|entry| entry.session == session)
    }

    pub fn cancel_all(&mut self) -> Vec<SuspendedCommand<T>> {
        self.take_matching(|_| true)
    }

    pub fn signal_key(&mut self, db: u32, key: &[u8]) -> Vec<SuspendedCommand<T>> {
        self.take_matching(|entry| {
            matches!(
                &entry.wait,
                WaitCondition::Keys { db: wait_db, keys }
                    if *wait_db == db && keys.iter().any(|candidate| candidate.as_ref() == key)
            )
        })
    }

    pub fn take_due(&mut self, now: RuntimeInstant) -> Vec<SuspendedCommand<T>> {
        self.take_matching(
            |entry| matches!(entry.wait, WaitCondition::Deadline(deadline) if deadline <= now),
        )
    }

    pub fn signal_external(&mut self, event: ExternalEventKey) -> Vec<SuspendedCommand<T>> {
        self.take_matching(
            |entry| matches!(entry.wait, WaitCondition::External(candidate) if candidate == event),
        )
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn take_matching(
        &mut self,
        predicate: impl Fn(&SuspendedCommand<T>) -> bool,
    ) -> Vec<SuspendedCommand<T>> {
        let mut ids = self
            .entries
            .iter()
            .filter_map(|(id, entry)| predicate(entry).then_some(*id))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.into_iter()
            .filter_map(|id| self.entries.remove(&id))
            .collect()
    }
}
