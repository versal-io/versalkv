//! Per-turn write service state and slot-export output accounting.
//! `EventBatch` owns write runnable ordering and deduplication; these states
//! retain only service claims, byte budgets, and allocation accounting.

use super::pending_write::OrdinaryWriteScheduler;
use crate::framework::capabilities::io::Token;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct WriteSchedulers {
    pub(crate) ordinary: OrdinaryWriteScheduler,
    pub(crate) slot_exports: SlotExportWriteScheduler,
    pub(crate) slot_export_allocation_bytes: usize,
    pub(crate) slot_export_allocations: HashMap<Token, usize>,
    replica_writable_wait: HashSet<Token>,
}

impl WriteSchedulers {
    pub(crate) fn begin_turn(&mut self) {
        self.ordinary.begin_turn();
        self.slot_exports.begin_turn();
    }

    pub(crate) fn remove(&mut self, token: Token) {
        self.ordinary.remove(token);
        self.slot_exports.remove(token);
        self.replica_writable_wait.remove(&token);
    }

    pub(crate) fn mark_replica_waiting_for_writable(&mut self, token: Token) {
        self.replica_writable_wait.insert(token);
    }

    pub(crate) fn clear_replica_writable_wait(&mut self, token: Token) {
        self.replica_writable_wait.remove(&token);
    }

    pub(crate) fn replica_waiting_for_writable(&self, token: Token) -> bool {
        self.replica_writable_wait.contains(&token)
    }

    pub(crate) fn retain_streaming_replica_waiters(&mut self, streaming: &HashSet<Token>) {
        self.replica_writable_wait
            .retain(|token| streaming.contains(token));
    }
}

#[derive(Default)]
pub(crate) struct SlotExportWriteScheduler {
    serviced_this_turn: HashSet<Token>,
    remaining_budget: HashMap<Token, usize>,
}

impl SlotExportWriteScheduler {
    pub(crate) fn begin_turn(&mut self) {
        self.serviced_this_turn.clear();
        self.remaining_budget.clear();
    }

    pub(crate) fn claim_service(&mut self, token: Token) -> bool {
        if self.serviced_this_turn.contains(&token) {
            return false;
        }
        true
    }

    pub(crate) fn was_serviced(&self, token: Token) -> bool {
        self.serviced_this_turn.contains(&token)
    }

    pub(crate) fn remaining_budget(&mut self, token: Token, full_budget: usize) -> usize {
        if self.serviced_this_turn.insert(token) {
            self.remaining_budget.insert(token, full_budget);
        }
        self.remaining_budget.get(&token).copied().unwrap_or(0)
    }

    pub(crate) fn consume_budget(&mut self, token: Token, bytes: usize) {
        if let Some(remaining) = self.remaining_budget.get_mut(&token) {
            *remaining = remaining.saturating_sub(bytes);
        }
    }

    pub(crate) fn remove(&mut self, token: Token) {
        self.serviced_this_turn.remove(&token);
        self.remaining_budget.remove(&token);
    }
}
