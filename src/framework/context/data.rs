use super::ServerContext;
use crate::engine::state::blocking_registry::BlockingClientRegistry;
use crate::engine::state::db::ClientInfo;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::{Db, DbSet};
use crate::framework::CommandError;

impl ServerContext {
    // ── Database access ──
    pub fn num_dbs(&self) -> usize {
        self.db.dbs.num_dbs()
    }
    pub fn dbs(&self) -> &DbSet {
        &self.db.dbs
    }
    pub(crate) fn dbs_mut(&mut self) -> &mut DbSet {
        &mut self.db.dbs
    }

    // ── Db semantic methods ──
    pub fn flush_all_dbs(&mut self) {
        self.db.dbs.flush_all();
        self.reset_repl_seldb();
    }
    /// Reset DB metadata while returning detached values for an L5 lazyfree
    /// owner. Used only by full-sync replacement paths that have already made
    /// the serving dataset unavailable.
    pub fn drain_all_db_values_for_lazyfree(&mut self) -> Vec<crate::foundation::io::store::Robj> {
        let values = self.db.dbs.drain_all_values_for_lazyfree();
        self.reset_repl_seldb();
        values
    }
    pub fn db_size(&self, idx: u32) -> usize {
        self.db.dbs.db_size(idx)
    }
    pub fn drain_expired_deletions(&mut self) -> Vec<(u32, Vec<u8>)> {
        self.db.dbs.drain_expired_deletions()
    }

    pub fn drain_active_expired_deletions(&mut self) -> Vec<(u32, Vec<u8>)> {
        self.db.dbs.drain_active_expired_deletions()
    }
    pub fn drain_lazy_expired_hash_fields(&mut self) -> u64 {
        self.db.dbs.drain_lazy_expired_hash_fields()
    }
    pub fn drain_pending_hexpired(&mut self) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
        self.db.dbs.drain_pending_hexpired()
    }
    pub fn clear_expired_deletions(&mut self) {
        self.db.dbs.clear_expired_deletions();
    }
    pub fn update_dict_resize_policy(&mut self) {
        use crate::foundation::io::store::structure::dict::ResizePolicy;
        let policy = if !self.config().server.dict_resizing {
            ResizePolicy::Forbid
        } else if self.persistence().has_active_child() {
            ResizePolicy::Avoid
        } else {
            ResizePolicy::Allow
        };
        for idx in 0..self.db.dbs.num_dbs() as u32 {
            self.db.dbs.set_data_resize_policy(idx, policy);
        }
    }
    pub fn try_resize_all_dbs(&mut self, active_rehash: bool) {
        self.db.dbs.try_resize_all(active_rehash);
    }
    pub fn swap_dbs(&mut self, a: u32, b: u32) {
        self.db.dbs.swap(a, b);
    }
    pub fn sync_eviction_policy(&mut self) {
        let use_lfu = crate::foundation::io::store::kv_engine::EvictionPolicy::from_name(
            &self.config().server.maxmemory_policy,
        )
        .is_some_and(|policy| policy.uses_lfu());
        let log_factor = self.config().server.lfu_log_factor.max(0) as u32;
        let decay_time = self
            .config()
            .server
            .lfu_decay_time
            .clamp(0, i64::from(u16::MAX)) as u16;
        self.db
            .dbs
            .set_eviction_metadata_config(use_lfu, log_factor, decay_time);
    }
    pub fn refresh_dict_resize_budget(&mut self) {
        let budget = (self.config().server.maxmemory > 0).then(|| {
            let state = self.maxmemory_state();
            if state.would_exceed(0) {
                0
            } else {
                state.headroom()
            }
        });
        self.db.dbs.set_resize_budget(budget);
    }
    /// Run a closure with mutable access to a specific database.
    /// Returns `db_index_out_of_range` error if the index is invalid.
    pub(crate) fn with_db_mut<R: Into<Value>>(
        &mut self,
        idx: u32,
        f: impl FnOnce(&mut Db) -> R,
    ) -> Value {
        match self.db.dbs.get_mut(idx) {
            Some(db) => f(db).into(),
            None => CommandError::db_index_out_of_range(),
        }
    }
    /// Get immutable access to a specific database.
    /// Returns `None` if the index is out of range.
    pub fn db(&self, idx: u32) -> Option<&Db> {
        self.db.dbs.get(idx)
    }
    /// Get mutable access to a specific database.
    /// Returns `None` if the index is out of range.
    pub(crate) fn db_mut(&mut self, idx: u32) -> Option<&mut Db> {
        self.db.dbs.get_mut(idx)
    }
    /// Mutably access the client info registry.
    pub(crate) fn client_infos_mut(&mut self) -> &mut std::collections::HashMap<usize, ClientInfo> {
        &mut self.db.client_infos
    }
    /// Look up a client's info by token id.
    pub fn client_info(&self, id: usize) -> Option<&ClientInfo> {
        self.db.client_infos.get(&id)
    }
    /// Mutably look up a client's info by token id.
    pub(crate) fn client_info_mut(&mut self, id: usize) -> Option<&mut ClientInfo> {
        self.db.client_infos.get_mut(&id)
    }
    /// Count clients with at least one watched key and the total watched keys.
    pub fn watched_client_summary(&self) -> (usize, usize) {
        let mut clients = 0usize;
        let mut keys = 0usize;
        for info in self.db.client_infos.values() {
            if info.watched_keys == 0 {
                continue;
            }
            clients += 1;
            keys += info.watched_keys;
        }
        (clients, keys)
    }
    /// Access the blocking client registry.
    pub fn blocking_registry(&self) -> &BlockingClientRegistry {
        &self.db.blocking_registry
    }
    /// Mutably access the blocking client registry.
    pub(crate) fn blocking_registry_mut(&mut self) -> &mut BlockingClientRegistry {
        &mut self.db.blocking_registry
    }
}
