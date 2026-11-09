use super::stats::IoThreadStats;
use super::{
    IoCompletion, IoJob, ReadCompletion, ReadJob, SubmitError, WorkerCompletionEvent,
    WorkerCompletionSender, WriteCompletion, WriteJob,
};
use crate::foundation::io::transport::TransportWakeHandle;
use crate::foundation::io::transport::{ReadOutcome, WriteOutcome};
use crate::framework::capabilities::io::mt_driver::IoDriverMtConfig;
use crate::runtime::server::worker::event_loop::transport::RuntimePollJob;
use std::collections::VecDeque;
use std::io::{self, IoSlice, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

const WORKER_QUEUE_CAPACITY: usize = 4096;

struct WorkerHandle {
    sender: SyncSender<IoJob>,
    join: Option<JoinHandle<()>>,
    stopping: Arc<AtomicBool>,
}

struct WorkerContext {
    id: usize,
    completions: mpsc::Sender<IoCompletion>,
    shared_completions: Option<WorkerCompletionSender>,
    wake: TransportWakeHandle,
    stats: Arc<IoThreadStats>,
    stopping: Arc<AtomicBool>,
}

pub(crate) struct SharedIoPool {
    workers: Vec<WorkerHandle>,
    completions_rx: Receiver<IoCompletion>,
    completions_tx: mpsc::Sender<IoCompletion>,
    shared_completions: Option<WorkerCompletionSender>,
    wake: TransportWakeHandle,
    stats: Arc<IoThreadStats>,
    configured_threads: usize,
    active_workers: usize,
    next_worker: usize,
    queue_capacity: usize,
}

impl SharedIoPool {
    #[cfg(test)]
    pub(crate) fn new(config: IoDriverMtConfig, wake: TransportWakeHandle) -> io::Result<Self> {
        Self::with_queue_capacity(config, wake, WORKER_QUEUE_CAPACITY)
    }

    #[cfg(test)]
    pub(crate) fn new_with_queue_capacity_for_test(
        config: IoDriverMtConfig,
        wake: TransportWakeHandle,
        queue_capacity: usize,
    ) -> io::Result<Self> {
        Self::with_queue_capacity(config, wake, queue_capacity)
    }

    #[cfg(test)]
    fn with_queue_capacity(
        config: IoDriverMtConfig,
        wake: TransportWakeHandle,
        queue_capacity: usize,
    ) -> io::Result<Self> {
        Self::with_queue_capacity_and_completion_sender(config, wake, queue_capacity, None)
    }

    pub(crate) fn with_completion_sender(
        config: IoDriverMtConfig,
        wake: TransportWakeHandle,
        shared_completions: WorkerCompletionSender,
    ) -> io::Result<Self> {
        Self::with_queue_capacity_and_completion_sender(
            config,
            wake,
            WORKER_QUEUE_CAPACITY,
            Some(shared_completions),
        )
    }

    fn with_queue_capacity_and_completion_sender(
        config: IoDriverMtConfig,
        wake: TransportWakeHandle,
        queue_capacity: usize,
        shared_completions: Option<WorkerCompletionSender>,
    ) -> io::Result<Self> {
        let configured_threads = config.n_workers();
        if configured_threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IO thread count must include the main thread",
            ));
        }
        let stats = Arc::new(IoThreadStats::new());
        let (completions_tx, completions_rx) = mpsc::channel();
        let workers = spawn_workers(
            configured_threads.saturating_sub(1),
            &completions_tx,
            shared_completions.as_ref(),
            &wake,
            &stats,
            queue_capacity,
        )?;
        Ok(Self {
            workers,
            completions_rx,
            completions_tx,
            shared_completions,
            wake,
            stats,
            configured_threads,
            active_workers: 0,
            next_worker: 0,
            queue_capacity,
        })
    }

    pub(crate) fn configured_threads(&self) -> usize {
        self.configured_threads
    }

    #[cfg(test)]
    pub(crate) fn worker_handles(&self) -> usize {
        self.workers.len()
    }

    pub(crate) fn set_active_workers(&mut self, workers: usize) {
        self.active_workers = workers.min(self.workers.len());
        self.next_worker %= self.active_workers.max(1);
    }

    pub(crate) fn active_workers(&self) -> usize {
        self.active_workers
    }

    pub(crate) fn has_pending_io(&self) -> bool {
        self.stats.reads_pending() != 0 || self.stats.writes_pending() != 0
    }

    pub(crate) fn queue_depth(&self) -> usize {
        self.stats.total_queue_depth()
    }

    pub(crate) fn stats(&self) -> &IoThreadStats {
        &self.stats
    }

    pub(crate) fn try_submit_read(&mut self, job: ReadJob) -> Result<(), SubmitError<ReadJob>> {
        if self.active_workers == 0 {
            return Err(SubmitError::Inactive(job));
        }
        let index = self.next_worker % self.active_workers;
        self.next_worker = (self.next_worker + 1) % self.active_workers;
        self.stats.record_job_submitted(index + 1);
        self.stats.record_read_submitted();
        match self.workers[index].sender.try_send(IoJob::Read(job)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(IoJob::Read(job))) => {
                self.stats.record_job_dequeued(index + 1);
                self.stats.cancel_read_submission();
                Err(SubmitError::Full(job))
            }
            Err(TrySendError::Disconnected(IoJob::Read(job))) => {
                self.stats.record_job_dequeued(index + 1);
                self.stats.cancel_read_submission();
                Err(SubmitError::Disconnected(job))
            }
            Err(TrySendError::Full(IoJob::Write(_)))
            | Err(TrySendError::Disconnected(IoJob::Write(_)))
            | Err(TrySendError::Full(IoJob::Poll(_)))
            | Err(TrySendError::Disconnected(IoJob::Poll(_)))
            | Err(TrySendError::Full(IoJob::Shutdown))
            | Err(TrySendError::Disconnected(IoJob::Shutdown)) => {
                unreachable!("read submission cannot become a shutdown job")
            }
        }
    }

    pub(crate) fn try_submit_write(&mut self, job: WriteJob) -> Result<(), SubmitError<WriteJob>> {
        if self.active_workers == 0 {
            return Err(SubmitError::Inactive(job));
        }
        let index = self.next_worker % self.active_workers;
        self.next_worker = (self.next_worker + 1) % self.active_workers;
        self.stats.record_job_submitted(index + 1);
        self.stats.record_write_submitted();
        match self.workers[index].sender.try_send(IoJob::Write(job)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(IoJob::Write(job))) => {
                self.stats.record_job_dequeued(index + 1);
                self.stats.cancel_write_submission();
                Err(SubmitError::Full(job))
            }
            Err(TrySendError::Disconnected(IoJob::Write(job))) => {
                self.stats.record_job_dequeued(index + 1);
                self.stats.cancel_write_submission();
                Err(SubmitError::Disconnected(job))
            }
            Err(TrySendError::Full(IoJob::Read(_)))
            | Err(TrySendError::Disconnected(IoJob::Read(_)))
            | Err(TrySendError::Full(IoJob::Poll(_)))
            | Err(TrySendError::Disconnected(IoJob::Poll(_)))
            | Err(TrySendError::Full(IoJob::Shutdown))
            | Err(TrySendError::Disconnected(IoJob::Shutdown)) => {
                unreachable!("write submission cannot become another job kind")
            }
        }
    }

    pub(crate) fn try_submit_poll(
        &mut self,
        job: RuntimePollJob,
    ) -> Result<(), SubmitError<RuntimePollJob>> {
        if self.active_workers == 0 {
            return Err(SubmitError::Inactive(job));
        }
        let index = self.next_worker % self.active_workers;
        self.next_worker = (self.next_worker + 1) % self.active_workers;
        self.stats.record_job_submitted(index + 1);
        self.stats.record_poll_submitted();
        match self.workers[index].sender.try_send(IoJob::Poll(job)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(IoJob::Poll(job))) => {
                self.stats.record_job_dequeued(index + 1);
                self.stats.cancel_poll_submission();
                Err(SubmitError::Full(job))
            }
            Err(TrySendError::Disconnected(IoJob::Poll(job))) => {
                self.stats.record_job_dequeued(index + 1);
                self.stats.cancel_poll_submission();
                Err(SubmitError::Disconnected(job))
            }
            Err(TrySendError::Full(IoJob::Read(_)))
            | Err(TrySendError::Disconnected(IoJob::Read(_)))
            | Err(TrySendError::Full(IoJob::Write(_)))
            | Err(TrySendError::Disconnected(IoJob::Write(_)))
            | Err(TrySendError::Full(IoJob::Shutdown))
            | Err(TrySendError::Disconnected(IoJob::Shutdown)) => {
                unreachable!("poll submission cannot become another job kind")
            }
        }
    }

    pub(crate) fn try_recv_completion(&self) -> Option<IoCompletion> {
        self.completions_rx.try_recv().ok()
    }

    pub(crate) fn recv_completion(&self) -> Result<IoCompletion, mpsc::RecvError> {
        self.completions_rx.recv()
    }

    #[cfg(test)]
    pub(crate) fn recv_completion_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<IoCompletion, mpsc::RecvTimeoutError> {
        self.completions_rx.recv_timeout(timeout)
    }

    pub(crate) fn resize(&mut self, configured_threads: usize) -> io::Result<()> {
        if configured_threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IO thread count must include the main thread",
            ));
        }
        if configured_threads == self.configured_threads {
            return Ok(());
        }
        let replacement = spawn_workers(
            configured_threads.saturating_sub(1),
            &self.completions_tx,
            self.shared_completions.as_ref(),
            &self.wake,
            &self.stats,
            self.queue_capacity,
        )?;
        shutdown_workers(&mut self.workers, &self.wake);
        self.workers = replacement;
        self.configured_threads = configured_threads;
        self.active_workers = self.active_workers.min(self.workers.len());
        self.next_worker = 0;
        Ok(())
    }

    pub(crate) fn shutdown_and_drain(&mut self) -> Vec<IoCompletion> {
        shutdown_workers(&mut self.workers, &self.wake);
        self.active_workers = 0;
        let mut completions = Vec::new();
        while let Ok(completion) = self.completions_rx.try_recv() {
            completions.push(completion);
        }
        completions
    }
}

impl Drop for SharedIoPool {
    fn drop(&mut self) {
        shutdown_workers(&mut self.workers, &self.wake);
    }
}

fn spawn_workers(
    count: usize,
    completions: &mpsc::Sender<IoCompletion>,
    shared_completions: Option<&WorkerCompletionSender>,
    wake: &TransportWakeHandle,
    stats: &Arc<IoThreadStats>,
    queue_capacity: usize,
) -> io::Result<Vec<WorkerHandle>> {
    let mut workers = Vec::with_capacity(count);
    let stopping = Arc::new(AtomicBool::new(false));
    for index in 0..count {
        let worker_id = index + 1;
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let completions = completions.clone();
        let shared_completions = shared_completions.cloned();
        let worker_wake = wake.clone();
        let stats = Arc::clone(stats);
        let worker_stopping = Arc::clone(&stopping);
        let join = match std::thread::Builder::new()
            .name(format!("io_thd_{worker_id}"))
            .spawn(move || {
                worker_main(
                    receiver,
                    WorkerContext {
                        id: worker_id,
                        completions,
                        shared_completions,
                        wake: worker_wake,
                        stats,
                        stopping: worker_stopping,
                    },
                )
            }) {
            Ok(join) => join,
            Err(error) => {
                shutdown_workers(&mut workers, wake);
                return Err(error);
            }
        };
        workers.push(WorkerHandle {
            sender,
            join: Some(join),
            stopping: Arc::clone(&stopping),
        });
    }
    Ok(workers)
}

fn shutdown_workers(workers: &mut Vec<WorkerHandle>, wake: &TransportWakeHandle) {
    for worker in workers.iter() {
        worker.stopping.store(true, Ordering::Release);
    }
    if !workers.is_empty() {
        let _ = wake.wake();
    }
    for worker in workers.iter() {
        let _ = worker.sender.send(IoJob::Shutdown);
    }
    for (index, worker) in workers.iter_mut().enumerate() {
        if let Some(join) = worker.join.take() {
            if join.join().is_ok() {
                crate::server_log!(notice, "IO thread(id:{}) terminated", index + 1);
            } else {
                crate::server_log!(warning, "IO thread(id:{}) could not be joined", index + 1);
            }
        }
    }
    workers.clear();
}

fn worker_main(receiver: Receiver<IoJob>, context: WorkerContext) {
    while let Ok(job) = receiver.recv() {
        if matches!(job, IoJob::Shutdown) {
            break;
        }
        context.stats.record_job_dequeued(context.id);
        let completion = if context.stopping.load(Ordering::Acquire) {
            job.cancel()
                .expect("non-shutdown IO jobs always produce a cancellation")
        } else {
            let started = Instant::now();
            let completion = job
                .execute()
                .expect("non-shutdown IO jobs always produce a completion");
            context
                .stats
                .record_active_time(context.id, started.elapsed().as_micros() as u64);
            completion
        };
        if !publish_completion(&context, completion) {
            break;
        }
    }
    let _ = context.completions.send(IoCompletion::WorkerStopped);
    let _ = context.wake.wake();
}

fn publish_completion(context: &WorkerContext, completion: IoCompletion) -> bool {
    if let Some(shared_completions) = &context.shared_completions {
        match completion {
            IoCompletion::Read(completion) => {
                return shared_completions.enqueue(WorkerCompletionEvent::SocketRead(completion));
            }
            IoCompletion::Write(completion) => {
                return shared_completions.enqueue(WorkerCompletionEvent::SocketWrite(completion));
            }
            IoCompletion::Poll(completion) => {
                if context
                    .completions
                    .send(IoCompletion::Poll(completion))
                    .is_err()
                {
                    return false;
                }
            }
            IoCompletion::WorkerStopped => {
                if context
                    .completions
                    .send(IoCompletion::WorkerStopped)
                    .is_err()
                {
                    return false;
                }
            }
        }
    } else if context.completions.send(completion).is_err() {
        return false;
    }
    let _ = context.wake.wake();
    true
}

pub(super) fn execute_write(mut job: WriteJob) -> WriteCompletion {
    let (written, released_allocation_bytes, outcome) =
        write_with_budget(&mut job.handle, &mut job.frames, job.byte_budget);
    WriteCompletion {
        id: job.id,
        handle: job.handle,
        frames: job.frames,
        written,
        released_allocation_bytes,
        outcome,
        cancelled: false,
    }
}

pub(super) fn write_with_budget(
    writer: &mut impl Write,
    frames: &mut VecDeque<Vec<u8>>,
    byte_budget: usize,
) -> (usize, usize, WriteOutcome) {
    const MAX_IOV: usize = 128;
    let mut written = 0usize;
    let mut released_allocation_bytes = 0usize;
    let outcome = loop {
        if frames.is_empty() {
            break WriteOutcome::Complete;
        }
        let remaining_budget = byte_budget.saturating_sub(written);
        if remaining_budget == 0 {
            break WriteOutcome::BudgetExhausted;
        }
        let (offered, result) = {
            let mut slices = [IoSlice::new(&[]); MAX_IOV];
            let mut offered = 0usize;
            let mut count = 0usize;
            for frame in frames.iter().take(MAX_IOV) {
                let available = remaining_budget.saturating_sub(offered);
                if available == 0 {
                    break;
                }
                let included = frame.len().min(available);
                slices[count] = IoSlice::new(&frame[..included]);
                offered += included;
                count += 1;
            }
            (offered, writer.write_vectored(&slices[..count]))
        };
        let count = match result {
            Ok(0) => break WriteOutcome::Failed(io::ErrorKind::WriteZero.into()),
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                break WriteOutcome::Backpressured;
            }
            Err(error) => break WriteOutcome::Failed(error),
        };
        written = written.saturating_add(count);
        let mut remaining = count;
        while remaining > 0 {
            let Some(front) = frames.front() else {
                break;
            };
            if remaining >= front.len() {
                let frame = frames.pop_front().expect("front exists");
                released_allocation_bytes = released_allocation_bytes.saturating_add(
                    crate::foundation::memory::util_alloc::vec_allocation_bytes(&frame),
                );
                remaining -= frame.len();
            } else {
                frames.front_mut().expect("front exists").drain(..remaining);
                break;
            }
        }
        if count < offered {
            break WriteOutcome::Backpressured;
        }
    };
    (written, released_allocation_bytes, outcome)
}

pub(super) fn execute_read(mut job: ReadJob) -> ReadCompletion {
    let (bytes, outcome) = read_with_budget(&mut job.handle, job.max_bytes);
    ReadCompletion {
        id: job.id,
        handle: job.handle,
        bytes,
        outcome,
        drain_to_budget: job.drain_to_budget,
        cancelled: false,
    }
}

pub(super) fn read_with_budget(reader: &mut impl Read, max_bytes: usize) -> (Vec<u8>, ReadOutcome) {
    let mut bytes = vec![0_u8; max_bytes];
    if max_bytes == 0 {
        return (bytes, ReadOutcome::BudgetExhausted);
    }
    let mut used = 0usize;
    let outcome = loop {
        match reader.read(&mut bytes[used..]) {
            Ok(0) => break ReadOutcome::PeerClosed,
            Ok(read) => {
                used += read;
                if used == bytes.len() {
                    break ReadOutcome::BudgetExhausted;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                break ReadOutcome::Quiescent;
            }
            Err(error) => break ReadOutcome::Failed(error),
        }
    };
    bytes.truncate(used);
    (bytes, outcome)
}
