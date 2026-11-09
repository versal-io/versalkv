use super::{IoOperationId, IoThreadRuntime, WriteJob, WriteSubmission};
use crate::framework::capabilities::io::Token;

impl IoThreadRuntime {
    pub(crate) fn submit_read(
        &mut self,
        token: Token,
        max_bytes: usize,
        drain_to_budget: bool,
    ) -> bool {
        if let Some(state) = self.connections.get_mut(&token) {
            if state.is_cluster_bus && state.write_inflight.is_some() {
                state.read_deferred = true;
                return true;
            }
            state.read_deferred = false;
        }
        if self
            .connections
            .get(&token)
            .is_some_and(|state| state.read_inflight.is_some())
        {
            return true;
        }
        if self.pool.active_workers() == 0 {
            return false;
        }
        let Some(job) = self.take_read_job(token, max_bytes, drain_to_budget) else {
            return false;
        };
        match self.pool.try_submit_read(job) {
            Ok(()) => true,
            Err(error) => {
                self.restore_read_job(error.into_inner());
                false
            }
        }
    }

    pub(crate) fn submit_write(
        &mut self,
        token: Token,
        frames: std::collections::VecDeque<Vec<u8>>,
        byte_budget: usize,
    ) -> WriteSubmission {
        if self.connections.get(&token).is_some_and(|state| {
            state.is_cluster_bus && (state.read_inflight.is_some() || state.read_deferred)
        }) {
            return WriteSubmission::AlreadyPending(frames);
        }
        if self
            .connections
            .get(&token)
            .is_some_and(|state| state.write_inflight.is_some())
        {
            return WriteSubmission::AlreadyPending(frames);
        }
        if self.pool.active_workers() == 0 {
            return WriteSubmission::Unavailable(frames);
        }
        let Some(state) = self.connections.get_mut(&token) else {
            return WriteSubmission::Unavailable(frames);
        };
        if state.close_pending {
            return WriteSubmission::Unavailable(frames);
        }
        let Some(handle) = state.write_handle.take() else {
            return WriteSubmission::Unavailable(frames);
        };
        state.next_sequence = state.next_sequence.saturating_add(1).max(1);
        let id = IoOperationId {
            token,
            generation: state.generation,
            sequence: state.next_sequence,
        };
        state.write_inflight = Some(id);
        let job = WriteJob {
            id,
            handle,
            frames,
            byte_budget,
        };
        match self.pool.try_submit_write(job) {
            Ok(()) => WriteSubmission::Submitted,
            Err(error) => {
                let job = error.into_inner();
                state.write_inflight = None;
                state.write_handle = Some(job.handle);
                WriteSubmission::Unavailable(job.frames)
            }
        }
    }
}
