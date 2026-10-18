#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MaxmemoryState {
    pub reported: usize,
    pub not_counted: usize,
    pub logical: usize,
    pub to_free: usize,
    pub level: f64,
    maxmemory: usize,
}

impl MaxmemoryState {
    pub fn from_parts(reported: usize, maxmemory: usize, not_counted: usize) -> Self {
        let logical = reported.saturating_sub(not_counted);
        let to_free = if maxmemory == 0 {
            0
        } else {
            logical.saturating_sub(maxmemory)
        };
        let level = if maxmemory == 0 {
            0.0
        } else {
            logical as f64 / maxmemory as f64
        };
        Self {
            reported,
            not_counted,
            logical,
            to_free,
            level,
            maxmemory,
        }
    }

    pub fn is_over_limit(self) -> bool {
        self.maxmemory > 0 && self.logical > self.maxmemory
    }

    pub fn headroom(self) -> usize {
        if self.maxmemory == 0 {
            usize::MAX
        } else {
            self.maxmemory.saturating_sub(self.logical)
        }
    }

    pub fn would_exceed(self, additional: usize) -> bool {
        self.maxmemory > 0 && self.logical.saturating_add(additional) > self.maxmemory
    }
}

impl super::ServerContext {
    pub fn maxmemory_state(&self) -> MaxmemoryState {
        let reported = crate::foundation::memory::util_alloc::total_allocated();
        // C shares backlog blocks with replica clients. Rust currently owns
        // the retained backlog and each queued replica frame separately, so
        // maxmemory must exclude both physical allocations while INFO keeps
        // its C-facing shared-buffer projection.
        let repl_buffer = self
            .replication()
            .backlog
            .allocation_bytes()
            .saturating_add(self.replication().replica_output_buffer_allocation_bytes);
        let backlog = self.config().server.repl_backlog_size.max(0) as usize;
        let replication_not_counted = repl_buffer.saturating_sub(backlog);
        let aof_not_counted = if self.config().persistence.aof_enabled {
            self.aof().buffer_allocation_bytes()
        } else {
            0
        };
        let not_counted = replication_not_counted
            .saturating_add(aof_not_counted)
            .saturating_add(self.slot_export_output_memory);
        let maxmemory = usize::try_from(self.config().server.maxmemory).unwrap_or(usize::MAX);
        MaxmemoryState::from_parts(reported, maxmemory, not_counted)
    }

    pub(crate) fn set_slot_export_output_memory(&mut self, bytes: usize) {
        self.slot_export_output_memory = bytes;
    }
}
