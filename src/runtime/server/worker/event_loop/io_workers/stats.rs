use crate::engine::extension::config::server::IO_THREADS_MAX;
use crate::framework::capabilities::io::mt_driver::WorkerId;
use std::sync::atomic::{AtomicU64, Ordering};

/// Atomically decrement `counter` by one, saturating at zero.
///
/// Uses `compare_exchange_weak` instead of `fetch_update`/`try_update` for
/// toolchain compatibility (the former is stable across all supported
/// versions; `fetch_update` is deprecated in newer toolchains while
/// `try_update` is still unstable on older ones).
fn atomic_saturating_dec(counter: &AtomicU64, success: Ordering, failure: Ordering) {
    loop {
        let current = counter.load(failure);
        match counter.compare_exchange_weak(current, current.saturating_sub(1), success, failure) {
            Ok(_) => break,
            Err(_) => continue,
        }
    }
}

#[cfg(test)]
const ADAPTIVE_SAMPLE_COUNT: usize = 16;
#[cfg(test)]
const MAIN_THREAD_SAMPLE_MS: u64 = 50;
#[cfg(test)]
const QUEUE_SAMPLE_MS: u64 = 10;
#[cfg(test)]
const SCALE_DOWN_COOLDOWN_MS: u64 = 1_000;
#[cfg(test)]
const IGNITION_UTILIZATION_PERCENT: u64 = 30;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct AdaptiveIoState {
    last_main_sample_ms: Option<u64>,
    last_main_active_us: u64,
    main_utilization_samples: [u64; ADAPTIVE_SAMPLE_COUNT],
    main_sample_index: usize,
    last_queue_sample_ms: Option<u64>,
    queue_samples: [u64; ADAPTIVE_SAMPLE_COUNT],
    queue_sample_count: u64,
    last_scale_ms: u64,
}

#[cfg(test)]
impl AdaptiveIoState {
    pub(crate) fn observe_main_thread(&mut self, now_ms: u64, active_time_us: u64) {
        let Some(last_ms) = self.last_main_sample_ms else {
            self.last_main_sample_ms = Some(now_ms);
            self.last_main_active_us = active_time_us;
            return;
        };
        let elapsed_ms = now_ms.saturating_sub(last_ms);
        if elapsed_ms < MAIN_THREAD_SAMPLE_MS {
            return;
        }
        let active_delta = active_time_us.saturating_sub(self.last_main_active_us);
        let elapsed_us = elapsed_ms.saturating_mul(1_000).max(1);
        let utilization = active_delta.saturating_mul(100) / elapsed_us;
        self.main_utilization_samples[self.main_sample_index] = utilization;
        self.main_sample_index = (self.main_sample_index + 1) % ADAPTIVE_SAMPLE_COUNT;
        self.last_main_sample_ms = Some(now_ms);
        self.last_main_active_us = active_time_us;
    }

    pub(crate) fn recommend_workers(
        &mut self,
        now_ms: u64,
        active_workers: usize,
        configured_workers: usize,
        queue_depth: usize,
        target_worker_queue_empty: bool,
        shared_queue_empty: bool,
    ) -> usize {
        if configured_workers == 0 {
            return 0;
        }
        if active_workers == 0 {
            if self.main_thread_utilization_pct() > IGNITION_UTILIZATION_PERCENT {
                self.last_scale_ms = now_ms;
                return 1;
            }
            return 0;
        }

        if self
            .last_queue_sample_ms
            .is_some_and(|last| now_ms.saturating_sub(last) < QUEUE_SAMPLE_MS)
        {
            return active_workers;
        }
        self.last_queue_sample_ms = Some(now_ms);
        let index = self.queue_sample_count as usize % ADAPTIVE_SAMPLE_COUNT;
        self.queue_samples[index] = queue_depth as u64;
        self.queue_sample_count = self.queue_sample_count.saturating_add(1);
        if !self
            .queue_sample_count
            .is_multiple_of(ADAPTIVE_SAMPLE_COUNT as u64)
        {
            return active_workers;
        }

        let average = self.average_queue_depth();
        if average > 1 && active_workers < configured_workers {
            self.last_scale_ms = now_ms;
            return active_workers + 1;
        }
        if average == 0
            && now_ms.saturating_sub(self.last_scale_ms) > SCALE_DOWN_COOLDOWN_MS
            && target_worker_queue_empty
            && (active_workers > 1 || shared_queue_empty)
        {
            return active_workers.saturating_sub(1);
        }
        active_workers
    }

    pub(crate) fn main_thread_utilization_pct(&self) -> u64 {
        self.main_utilization_samples.iter().sum::<u64>() / ADAPTIVE_SAMPLE_COUNT as u64
    }

    pub(crate) fn average_queue_depth(&self) -> u64 {
        self.queue_samples.iter().sum::<u64>() / ADAPTIVE_SAMPLE_COUNT as u64
    }
}

pub(crate) struct IoThreadStats {
    active_time_us: Vec<AtomicU64>,
    queued_jobs: Vec<AtomicU64>,
    reads_processed: AtomicU64,
    writes_processed: AtomicU64,
    polls_processed: AtomicU64,
    reads_pending: AtomicU64,
    writes_pending: AtomicU64,
    polls_pending: AtomicU64,
}

impl IoThreadStats {
    pub(crate) fn new() -> Self {
        Self {
            active_time_us: (0..IO_THREADS_MAX).map(|_| AtomicU64::new(0)).collect(),
            queued_jobs: (0..IO_THREADS_MAX).map(|_| AtomicU64::new(0)).collect(),
            reads_processed: AtomicU64::new(0),
            writes_processed: AtomicU64::new(0),
            polls_processed: AtomicU64::new(0),
            reads_pending: AtomicU64::new(0),
            writes_pending: AtomicU64::new(0),
            polls_pending: AtomicU64::new(0),
        }
    }

    pub(crate) fn record_active_time(&self, worker_id: WorkerId, elapsed_us: u64) {
        if let Some(counter) = self.active_time_us.get(worker_id) {
            counter.fetch_add(elapsed_us.max(1), Ordering::Relaxed);
        }
    }

    pub(crate) fn active_time_us(&self, worker_id: WorkerId) -> u64 {
        self.active_time_us
            .get(worker_id)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub(crate) fn record_job_submitted(&self, worker_id: WorkerId) {
        if let Some(counter) = self.queued_jobs.get(worker_id) {
            counter.fetch_add(1, Ordering::Release);
        }
    }

    pub(crate) fn record_job_dequeued(&self, worker_id: WorkerId) {
        if let Some(counter) = self.queued_jobs.get(worker_id) {
            atomic_saturating_dec(counter, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    pub(crate) fn total_queue_depth(&self) -> usize {
        self.queued_jobs
            .iter()
            .map(|counter| counter.load(Ordering::Acquire) as usize)
            .sum()
    }

    pub(crate) fn record_read_submitted(&self) {
        self.reads_pending.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn cancel_read_submission(&self) {
        atomic_saturating_dec(&self.reads_pending, Ordering::Relaxed, Ordering::Relaxed);
    }

    pub(crate) fn record_read_completed(&self) {
        atomic_saturating_dec(&self.reads_pending, Ordering::Relaxed, Ordering::Relaxed);
        self.reads_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_write_submitted(&self) {
        self.writes_pending.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn cancel_write_submission(&self) {
        atomic_saturating_dec(&self.writes_pending, Ordering::Relaxed, Ordering::Relaxed);
    }

    pub(crate) fn record_write_completed(&self) {
        atomic_saturating_dec(&self.writes_pending, Ordering::Relaxed, Ordering::Relaxed);
        self.writes_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_poll_submitted(&self) {
        self.polls_pending.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn cancel_poll_submission(&self) {
        atomic_saturating_dec(&self.polls_pending, Ordering::Relaxed, Ordering::Relaxed);
    }

    pub(crate) fn record_poll_completed(&self) {
        atomic_saturating_dec(&self.polls_pending, Ordering::Relaxed, Ordering::Relaxed);
        self.polls_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reads_processed(&self) -> u64 {
        self.reads_processed.load(Ordering::Relaxed)
    }

    pub(crate) fn writes_processed(&self) -> u64 {
        self.writes_processed.load(Ordering::Relaxed)
    }

    pub(crate) fn polls_processed(&self) -> u64 {
        self.polls_processed.load(Ordering::Relaxed)
    }

    pub(crate) fn reads_pending(&self) -> u64 {
        self.reads_pending.load(Ordering::Relaxed)
    }

    pub(crate) fn writes_pending(&self) -> u64 {
        self.writes_pending.load(Ordering::Relaxed)
    }

    pub(crate) fn polls_pending(&self) -> u64 {
        self.polls_pending.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::AdaptiveIoState;

    #[test]
    fn adaptive_policy_ignites_only_after_sampled_main_utilization_exceeds_thirty_percent() {
        let mut state = AdaptiveIoState::default();
        state.observe_main_thread(0, 0);
        let mut active_workers = 0;

        for sample in 1..=4 {
            state.observe_main_thread(sample * 50, sample * 50_000);
            active_workers = state.recommend_workers(sample * 50, active_workers, 3, 0, true, true);
            assert_eq!(active_workers, 0);
        }

        state.observe_main_thread(250, 250_000);
        active_workers = state.recommend_workers(250, active_workers, 3, 0, true, true);
        assert_eq!(active_workers, 1);
        assert!(state.main_thread_utilization_pct() > 30);
    }

    #[test]
    fn adaptive_policy_scales_on_sixteen_queue_samples_and_honors_cooldown() {
        let mut state = AdaptiveIoState::default();
        let mut active_workers = 1;

        for sample in 1..=16 {
            active_workers = state.recommend_workers(sample * 10, active_workers, 3, 2, true, true);
        }
        assert_eq!(active_workers, 2);
        assert_eq!(state.average_queue_depth(), 2);

        for sample in 1..=16 {
            active_workers =
                state.recommend_workers(1_200 + sample * 10, active_workers, 3, 0, false, true);
        }
        assert_eq!(active_workers, 2, "busy target worker cannot be parked");

        for sample in 1..=16 {
            active_workers =
                state.recommend_workers(1_400 + sample * 10, active_workers, 3, 0, true, true);
        }
        assert_eq!(active_workers, 1);
    }
}
