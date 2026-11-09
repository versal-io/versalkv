use super::super::super::ServerWorkerState;
use crate::foundation::io::sys::process::{apply_prepared_cpu_affinity, PreparedCpuAffinity};
use crate::framework::distributed::replication::PendingReplicationBuffer;
use crate::runtime::server::worker::event_loop::io_workers::{
    WorkerCompletionEvent, WorkerCompletionSender,
};
#[cfg(test)]
use std::collections::VecDeque;
use std::io;
#[cfg(test)]
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
#[cfg(test)]
use std::time::Duration;

const LOGICAL_BLOCK_BYTES: usize = 16 * 1024;
const ASYNC_BLOCK_THRESHOLD: u64 = 64;

struct PendingBufferFreeJob {
    buffer: PendingReplicationBuffer,
    effort: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplicationLazyfreeCompletion {
    pub(crate) completed_effort: u64,
}

enum PendingReplicationLazyfreeJob {
    Free(PendingBufferFreeJob),
    ConfigureCpuAffinity {
        affinity: PreparedCpuAffinity,
        result: Sender<io::Result<()>>,
    },
}

enum ReplicationLazyfreeCompletionSink {
    #[cfg(test)]
    Local(Sender<ReplicationLazyfreeCompletion>),
    Shared(WorkerCompletionSender),
}

impl ReplicationLazyfreeCompletionSink {
    fn complete(&self, completion: ReplicationLazyfreeCompletion) -> bool {
        match self {
            #[cfg(test)]
            Self::Local(sender) => sender.send(completion).is_ok(),
            Self::Shared(sender) => {
                sender.enqueue(WorkerCompletionEvent::ReplicationLazyfree(completion))
            }
        }
    }
}

pub(crate) struct PendingReplicationLazyfree {
    jobs: Option<Sender<PendingReplicationLazyfreeJob>>,
    #[cfg(test)]
    completions: Option<Receiver<ReplicationLazyfreeCompletion>>,
    #[cfg(test)]
    completed: VecDeque<ReplicationLazyfreeCompletion>,
    worker: Option<JoinHandle<()>>,
}

impl PendingReplicationLazyfree {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::new_with_sink(None)
    }

    pub(crate) fn new_with_completion_sender(sender: WorkerCompletionSender) -> Self {
        Self::new_with_sink(Some(sender))
    }

    fn new_with_sink(shared_sender: Option<WorkerCompletionSender>) -> Self {
        let (job_tx, job_rx) = mpsc::channel::<PendingReplicationLazyfreeJob>();
        #[cfg(test)]
        let mut completion_rx = None;
        let completion_sink = match shared_sender {
            Some(sender) => ReplicationLazyfreeCompletionSink::Shared(sender),
            #[cfg(test)]
            None => {
                let (completion_tx, receiver) = mpsc::channel();
                completion_rx = Some(receiver);
                ReplicationLazyfreeCompletionSink::Local(completion_tx)
            }
            #[cfg(not(test))]
            None => {
                unreachable!("production replication lazyfree uses the shared completion queue")
            }
        };
        let worker = thread::Builder::new()
            .name("pkv-repl-lazyfree".to_string())
            .spawn(move || {
                while let Ok(job) = job_rx.recv() {
                    match job {
                        PendingReplicationLazyfreeJob::Free(PendingBufferFreeJob {
                            buffer,
                            effort,
                        }) => {
                            drop(buffer);
                            if !completion_sink.complete(ReplicationLazyfreeCompletion {
                                completed_effort: effort,
                            }) {
                                break;
                            }
                        }
                        PendingReplicationLazyfreeJob::ConfigureCpuAffinity {
                            affinity,
                            result,
                        } => {
                            let _ = result.send(apply_prepared_cpu_affinity(&affinity));
                        }
                    }
                }
            });
        let (jobs, worker) = match worker {
            Ok(worker) => (Some(job_tx), Some(worker)),
            Err(_) => (None, None),
        };
        Self {
            jobs,
            #[cfg(test)]
            completions: completion_rx,
            #[cfg(test)]
            completed: VecDeque::new(),
            worker,
        }
    }

    fn effort_for(buffer: &PendingReplicationBuffer) -> u64 {
        if buffer.is_empty() {
            return 0;
        }
        buffer.used_bytes().div_ceil(LOGICAL_BLOCK_BYTES) as u64
    }

    fn submit(&mut self, buffer: PendingReplicationBuffer) -> Result<u64, PendingBufferFreeJob> {
        let effort = Self::effort_for(&buffer);
        if effort <= ASYNC_BLOCK_THRESHOLD {
            return Ok(0);
        }
        let job = PendingBufferFreeJob { buffer, effort };
        let Some(sender) = self.jobs.as_ref() else {
            return Err(job);
        };
        sender
            .send(PendingReplicationLazyfreeJob::Free(job))
            .map(|_| effort)
            .map_err(|error| match error.0 {
                PendingReplicationLazyfreeJob::Free(job) => job,
                PendingReplicationLazyfreeJob::ConfigureCpuAffinity { .. } => {
                    unreachable!(
                        "replication lazyfree send failure preserves the submitted job kind"
                    )
                }
            })
    }

    pub(crate) fn configure_cpu_affinity(
        &self,
        affinity: Option<PreparedCpuAffinity>,
    ) -> io::Result<()> {
        let Some(affinity) = affinity else {
            return Ok(());
        };
        let sender = self.jobs.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "replication lazyfree worker unavailable",
            )
        })?;
        let (result_tx, result_rx) = mpsc::channel();
        sender
            .send(PendingReplicationLazyfreeJob::ConfigureCpuAffinity {
                affinity,
                result: result_tx,
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "replication lazyfree worker stopped",
                )
            })?;
        result_rx.recv().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "replication lazyfree worker did not report affinity setup",
            )
        })?
    }

    #[cfg(test)]
    fn poll_completions(&mut self) -> u64 {
        let mut completed = 0u64;
        while let Some(completion) = self.completed.pop_front() {
            completed = completed.saturating_add(completion.completed_effort);
        }
        if let Some(completions) = &self.completions {
            while let Ok(completion) = completions.try_recv() {
                completed = completed.saturating_add(completion.completed_effort);
            }
        }
        completed
    }

    #[cfg(test)]
    fn wait_for_completion_for_test(&mut self) {
        if !self.completed.is_empty() {
            return;
        }
        let Some(completions) = &self.completions else {
            return;
        };
        if let Ok(completion) = completions.recv_timeout(Duration::from_secs(5)) {
            self.completed.push_back(completion);
        }
    }

    #[cfg(test)]
    fn close_jobs_and_join_for_test(&mut self) {
        self.jobs.take();
        if let Some(worker) = self.worker.take() {
            worker.join().expect("replication lazyfree worker joins");
        }
    }

    #[cfg(test)]
    fn with_disconnected_job_receiver_for_test() -> Self {
        let (job_tx, job_rx) = mpsc::channel::<PendingReplicationLazyfreeJob>();
        drop(job_rx);
        let (_completion_tx, completion_rx) = mpsc::channel();
        Self {
            jobs: Some(job_tx),
            completions: Some(completion_rx),
            completed: VecDeque::new(),
            worker: None,
        }
    }
}

#[cfg(test)]
impl Default for PendingReplicationLazyfree {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PendingReplicationLazyfree {
    fn drop(&mut self) {
        self.jobs.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl ServerWorkerState {
    pub(crate) fn dispose_replica_pending_buffer(
        &mut self,
        buffer: Option<PendingReplicationBuffer>,
    ) {
        if let Some(buffer) = buffer {
            match self.replication_lazyfree.submit(buffer) {
                Ok(0) => {}
                Ok(effort) => self.ctx.add_lazyfree_pending_objects(effort),
                Err(_job) => {}
            }
        }
        self.refresh_replica_output_buffer_bytes();
    }

    pub(crate) fn take_and_dispose_replica_pending_buffer(&mut self) {
        self.clear_pending_replication_replay();
        let buffer = self.replication_runtime.take_replica_pending_buffer();
        self.dispose_replica_pending_buffer(buffer);
    }

    pub(crate) fn take_and_dispose_replica_pending_buffer_for(&mut self, session_id: u64) {
        if self
            .pending_replication_replay
            .as_ref()
            .is_some_and(|continuation| continuation.session_id == session_id)
        {
            self.clear_pending_replication_replay();
        }
        let buffer = self
            .replication_runtime
            .take_replica_pending_buffer_for(session_id);
        self.dispose_replica_pending_buffer(buffer);
    }

    #[cfg(test)]
    pub(crate) fn drain_replication_lazyfree_completions_for_test(&mut self) {
        let completed = self.replication_lazyfree.poll_completions();
        self.complete_replication_lazyfree(ReplicationLazyfreeCompletion {
            completed_effort: completed,
        });
        self.dispatch_worker_completion_events();
    }

    pub(crate) fn complete_replication_lazyfree(
        &mut self,
        completion: ReplicationLazyfreeCompletion,
    ) {
        if completion.completed_effort > 0 {
            self.ctx
                .complete_lazyfree_objects(completion.completed_effort);
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_for_replication_lazyfree_for_test(&mut self) {
        self.replication_lazyfree.wait_for_completion_for_test();
        let _ = self
            .worker_completions
            .wait_for_completion_for_test(Duration::from_secs(5));
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::super::ServerWorkerState;
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::framework::distributed::replication::{
        PendingReplicationBuffer, ReplicaEvent, ReplicationApply,
    };
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn pending_buffer_with_payload(payload_bytes: usize) -> (PendingReplicationBuffer, u64) {
        let mut buffer = PendingReplicationBuffer::new(usize::MAX, offset(100), 0);
        buffer
            .push(ReplicaEvent::Apply {
                apply: ReplicationApply::Command(CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"k"),
                    Bytes::from(vec![b'x'; payload_bytes]),
                ])),
                end_offset: offset(101),
            })
            .unwrap();
        let effort = buffer
            .used_bytes()
            .saturating_add(super::LOGICAL_BLOCK_BYTES - 1)
            / super::LOGICAL_BLOCK_BYTES;
        (buffer, effort as u64)
    }

    fn pending_buffer_with_exact_used_bytes(used_bytes: usize) -> (PendingReplicationBuffer, u64) {
        let (empty_payload, _) = pending_buffer_with_payload(0);
        let retained_overhead = empty_payload.used_bytes();
        assert!(used_bytes >= retained_overhead);
        let result = pending_buffer_with_payload(used_bytes - retained_overhead);
        assert_eq!(result.0.used_bytes(), used_bytes);
        result
    }

    #[test]
    fn replication_lazyfree_drops_small_pending_buffer_synchronously() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (buffer, _) = pending_buffer_with_payload(1024);

        backend.dispose_replica_pending_buffer(Some(buffer));
        backend.drain_replication_lazyfree_completions_for_test();

        assert_eq!(backend.ctx.stats().lazyfree_pending_objects, 0);
        assert_eq!(backend.ctx.stats().lazyfreed_objects, 0);
    }

    #[test]
    fn replication_lazyfree_uses_strict_64_block_threshold() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let (buffer, effort) = pending_buffer_with_exact_used_bytes(
            super::ASYNC_BLOCK_THRESHOLD as usize * super::LOGICAL_BLOCK_BYTES,
        );
        assert_eq!(effort, super::ASYNC_BLOCK_THRESHOLD);

        backend.dispose_replica_pending_buffer(Some(buffer));
        backend.drain_replication_lazyfree_completions_for_test();

        assert_eq!(backend.ctx.stats().lazyfree_pending_objects, 0);
        assert_eq!(backend.ctx.stats().lazyfreed_objects, 0);
    }

    #[test]
    fn replication_lazyfree_frees_large_pending_buffer_asynchronously() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let baseline_pending = 7;
        let baseline_freed = 11;
        backend.ctx.add_lazyfree_pending_objects(baseline_pending);
        backend.ctx.incr_lazyfreed_objects(baseline_freed);
        let (buffer, effort) = pending_buffer_with_exact_used_bytes(
            super::ASYNC_BLOCK_THRESHOLD as usize * super::LOGICAL_BLOCK_BYTES + 1,
        );
        assert_eq!(effort, super::ASYNC_BLOCK_THRESHOLD + 1);

        backend.dispose_replica_pending_buffer(Some(buffer));

        assert_eq!(
            backend.ctx.stats().lazyfree_pending_objects,
            baseline_pending + effort
        );
        assert_eq!(backend.ctx.stats().lazyfreed_objects, baseline_freed);

        backend.wait_for_replication_lazyfree_for_test();
        backend.drain_replication_lazyfree_completions_for_test();

        assert_eq!(
            backend.ctx.stats().lazyfree_pending_objects,
            baseline_pending
        );
        assert_eq!(
            backend.ctx.stats().lazyfreed_objects,
            baseline_freed + effort
        );
    }

    #[test]
    fn replication_lazyfree_without_sender_drops_inline_without_counter_claims() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.replication_lazyfree.close_jobs_and_join_for_test();
        let (buffer, effort) = pending_buffer_with_exact_used_bytes(
            super::ASYNC_BLOCK_THRESHOLD as usize * super::LOGICAL_BLOCK_BYTES + 1,
        );
        assert_eq!(effort, super::ASYNC_BLOCK_THRESHOLD + 1);

        backend.dispose_replica_pending_buffer(Some(buffer));

        assert_eq!(backend.ctx.stats().lazyfree_pending_objects, 0);
        assert_eq!(backend.ctx.stats().lazyfreed_objects, 0);
    }

    #[test]
    fn replication_lazyfree_disconnected_receiver_drops_returned_job_inline() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.replication_lazyfree =
            super::PendingReplicationLazyfree::with_disconnected_job_receiver_for_test();
        assert!(backend.replication_lazyfree.jobs.is_some());
        let baseline_pending = 7;
        let baseline_freed = 11;
        backend.ctx.add_lazyfree_pending_objects(baseline_pending);
        backend.ctx.incr_lazyfreed_objects(baseline_freed);
        let (buffer, effort) = pending_buffer_with_exact_used_bytes(
            super::ASYNC_BLOCK_THRESHOLD as usize * super::LOGICAL_BLOCK_BYTES + 1,
        );
        assert_eq!(effort, super::ASYNC_BLOCK_THRESHOLD + 1);

        backend.dispose_replica_pending_buffer(Some(buffer));

        assert_eq!(
            backend.ctx.stats().lazyfree_pending_objects,
            baseline_pending
        );
        assert_eq!(backend.ctx.stats().lazyfreed_objects, baseline_freed);
    }

    #[test]
    fn replication_lazyfree_drop_joins_with_a_queued_job() {
        let mut lazyfree = super::PendingReplicationLazyfree::new();
        let (buffer, effort) = pending_buffer_with_exact_used_bytes(
            super::ASYNC_BLOCK_THRESHOLD as usize * super::LOGICAL_BLOCK_BYTES + 1,
        );

        assert!(matches!(lazyfree.submit(buffer), Ok(actual) if actual == effort));
        drop(lazyfree);
    }
}
