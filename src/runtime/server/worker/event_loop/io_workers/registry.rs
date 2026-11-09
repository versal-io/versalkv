use super::{
    IoConnectionState, IoOperationId, IoThreadRuntime, ReadCompletion, ReadCompletionAction,
    ReadJob, Token, WriteCompletion, WriteCompletionAction,
};
use crate::foundation::io::transport::ThreadIoHandle;

impl IoThreadRuntime {
    pub(crate) fn register_connection(
        &mut self,
        token: Token,
        read_handle: ThreadIoHandle,
        write_handle: ThreadIoHandle,
    ) -> u64 {
        self.next_generation = self.next_generation.saturating_add(1).max(1);
        let generation = self.next_generation;
        self.connections.insert(
            token,
            IoConnectionState {
                is_cluster_bus: false,
                read_deferred: false,
                generation,
                next_sequence: 0,
                read_handle: Some(read_handle),
                write_handle: Some(write_handle),
                read_inflight: None,
                write_inflight: None,
                close_pending: false,
            },
        );
        generation
    }

    #[cfg(test)]
    pub(crate) fn connection_generation(&self, token: Token) -> Option<u64> {
        self.connections.get(&token).map(|state| state.generation)
    }

    pub(crate) fn has_connection(&self, token: Token) -> bool {
        self.connections.contains_key(&token)
    }

    pub(crate) fn register_cluster_bus(
        &mut self,
        token: Token,
        read: ThreadIoHandle,
        write: ThreadIoHandle,
    ) {
        self.register_connection(token, read, write);
        if let Some(state) = self.connections.get_mut(&token) {
            state.is_cluster_bus = true;
        }
    }

    pub(crate) fn is_cluster_bus(&self, token: Token) -> bool {
        self.connections
            .get(&token)
            .is_some_and(|state| state.is_cluster_bus)
    }

    pub(crate) fn cluster_read_deferred(&self, token: Token) -> bool {
        self.connections
            .get(&token)
            .is_some_and(|state| state.is_cluster_bus && state.read_deferred)
    }

    pub(crate) fn take_read_job(
        &mut self,
        token: Token,
        max_bytes: usize,
        drain_to_budget: bool,
    ) -> Option<ReadJob> {
        let state = self.connections.get_mut(&token)?;
        if state.close_pending || state.read_inflight.is_some() {
            return None;
        }
        let handle = state.read_handle.take()?;
        state.next_sequence = state.next_sequence.saturating_add(1).max(1);
        let id = IoOperationId {
            token,
            generation: state.generation,
            sequence: state.next_sequence,
        };
        state.read_inflight = Some(id);
        Some(ReadJob {
            id,
            handle,
            max_bytes,
            drain_to_budget,
        })
    }

    pub(super) fn restore_read_job(&mut self, job: ReadJob) {
        let Some(state) = self.connections.get_mut(&job.id.token) else {
            return;
        };
        if state.generation == job.id.generation && state.read_inflight == Some(job.id) {
            state.read_inflight = None;
            state.read_handle = Some(job.handle);
        }
    }

    pub(crate) fn begin_close(&mut self, token: Token) -> bool {
        let Some(state) = self.connections.get_mut(&token) else {
            return false;
        };
        state.close_pending = true;
        if state.read_inflight.is_none() && state.write_inflight.is_none() {
            self.connections.remove(&token);
            false
        } else {
            true
        }
    }

    pub(crate) fn is_close_pending(&self, token: Token) -> bool {
        self.connections
            .get(&token)
            .is_some_and(|state| state.close_pending)
    }

    pub(crate) fn is_write_inflight(&self, token: Token) -> bool {
        self.connections
            .get(&token)
            .is_some_and(|state| state.write_inflight.is_some())
    }

    pub(crate) fn read_inflight_id(&self, token: Token) -> Option<IoOperationId> {
        self.connections.get(&token)?.read_inflight
    }

    pub(crate) fn write_inflight_id(&self, token: Token) -> Option<IoOperationId> {
        self.connections.get(&token)?.write_inflight
    }

    pub(crate) fn complete_read(&mut self, completion: ReadCompletion) -> ReadCompletionAction {
        if completion.cancelled {
            self.pool.stats().cancel_read_submission();
        } else {
            self.pool.stats().record_read_completed();
        }
        let Some(state) = self.connections.get_mut(&completion.id.token) else {
            return ReadCompletionAction::Stale;
        };
        if state.generation != completion.id.generation
            || state.read_inflight != Some(completion.id)
        {
            return ReadCompletionAction::Stale;
        }
        state.read_inflight = None;
        if state.close_pending {
            let token = completion.id.token;
            if state.write_inflight.is_none() {
                self.connections.remove(&token);
                return ReadCompletionAction::CloseTransport(token);
            }
            return ReadCompletionAction::Stale;
        }
        state.read_handle = Some(completion.handle);
        if completion.cancelled {
            return ReadCompletionAction::Cancelled;
        }
        ReadCompletionAction::Apply {
            token: completion.id.token,
            bytes: completion.bytes,
            outcome: completion.outcome,
            drain_to_budget: completion.drain_to_budget,
        }
    }

    pub(crate) fn complete_write(&mut self, completion: WriteCompletion) -> WriteCompletionAction {
        if completion.cancelled {
            self.pool.stats().cancel_write_submission();
        } else {
            self.pool.stats().record_write_completed();
        }
        let Some(state) = self.connections.get_mut(&completion.id.token) else {
            return WriteCompletionAction::Stale;
        };
        if state.generation != completion.id.generation
            || state.write_inflight != Some(completion.id)
        {
            return WriteCompletionAction::Stale;
        }
        state.write_inflight = None;
        if state.close_pending {
            let token = completion.id.token;
            if state.read_inflight.is_none() {
                self.connections.remove(&token);
                return WriteCompletionAction::CloseTransport(token);
            }
            return WriteCompletionAction::Stale;
        }
        state.write_handle = Some(completion.handle);
        WriteCompletionAction::Apply {
            token: completion.id.token,
            frames: completion.frames,
            written: completion.written,
            released_allocation_bytes: completion.released_allocation_bytes,
            outcome: completion.outcome,
        }
    }
}
