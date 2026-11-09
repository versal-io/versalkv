use super::{
    IoCompletion, IoThreadRuntime, ReadCompletion, SharedIoPool, SubmitError, WriteCompletion,
};
use crate::framework::context::IoThreadInfo;
use crate::runtime::server::worker::event_loop::transport::RuntimePollJob;

impl IoThreadRuntime {
    pub(crate) fn pool(&self) -> &SharedIoPool {
        &self.pool
    }

    pub(crate) fn try_submit_poll(
        &mut self,
        job: RuntimePollJob,
    ) -> Result<(), SubmitError<RuntimePollJob>> {
        self.pool.try_submit_poll(job)
    }

    pub(crate) fn record_poll_completion(&self, cancelled: bool) {
        if cancelled {
            self.pool.stats().cancel_poll_submission();
        } else {
            self.pool.stats().record_poll_completed();
        }
    }

    pub(crate) fn discard_read_completion_for_shutdown(&self, completion: ReadCompletion) {
        if completion.cancelled {
            self.pool.stats().cancel_read_submission();
        } else {
            self.pool.stats().record_read_completed();
        }
    }

    pub(crate) fn discard_write_completion_for_shutdown(&self, completion: WriteCompletion) {
        if completion.cancelled {
            self.pool.stats().cancel_write_submission();
        } else {
            self.pool.stats().record_write_completed();
        }
    }

    pub(crate) fn shutdown_and_drain(&mut self) -> Vec<IoCompletion> {
        self.pool.shutdown_and_drain()
    }

    pub(crate) fn should_offload_poll(&self) -> bool {
        self.pool.configured_threads() > 1
            && self.pool.active_workers() > 0
            && self.pool.has_pending_io()
    }

    pub(crate) fn configure(
        &mut self,
        configured_threads: usize,
        always_active: bool,
    ) -> std::io::Result<()> {
        self.pool.resize(configured_threads)?;
        let active = if always_active {
            configured_threads.saturating_sub(1)
        } else {
            0
        };
        self.pool.set_active_workers(active);
        Ok(())
    }

    pub(crate) fn after_poll(&mut self, event_count: usize, always_active: bool) {
        let configured_workers = self.pool.configured_threads().saturating_sub(1);
        if configured_workers == 0 {
            self.pool.set_active_workers(0);
        } else if always_active && event_count > 0 {
            self.pool.set_active_workers(configured_workers);
        } else if !always_active && event_count >= 4 {
            let next = self
                .pool
                .active_workers()
                .saturating_add(1)
                .min(configured_workers);
            self.pool.set_active_workers(next);
        }
    }

    pub(crate) fn before_sleep(&mut self, always_active: bool) {
        if !always_active && !self.pool.has_pending_io() {
            self.pool.set_active_workers(0);
        }
    }

    pub(crate) fn observe_counter_reset(&mut self, generation: u64) {
        if generation == self.counter_reset_generation {
            return;
        }
        self.counter_reset_generation = generation;
        self.reads_processed_baseline = self.pool.stats().reads_processed();
        self.writes_processed_baseline = self.pool.stats().writes_processed();
        self.polls_processed_baseline = self.pool.stats().polls_processed();
    }

    pub(crate) fn info(&self) -> IoThreadInfo {
        let configured_threads = self.pool.configured_threads();
        IoThreadInfo {
            configured_threads,
            active_threads: self.pool.active_workers().saturating_add(1),
            active_time_us: (0..configured_threads)
                .map(|id| self.pool.stats().active_time_us(id))
                .collect(),
            reads_processed: self
                .pool
                .stats()
                .reads_processed()
                .saturating_sub(self.reads_processed_baseline),
            writes_processed: self
                .pool
                .stats()
                .writes_processed()
                .saturating_sub(self.writes_processed_baseline),
            polls_processed: self
                .pool
                .stats()
                .polls_processed()
                .saturating_sub(self.polls_processed_baseline),
            reads_pending: self.pool.stats().reads_pending(),
            writes_pending: self.pool.stats().writes_pending(),
            polls_pending: self.pool.stats().polls_pending(),
            main_thread_utilization_pct: 0,
            queue_depth_average: self.pool.queue_depth() as u64,
            counter_reset_generation: self.counter_reset_generation,
        }
    }
}
