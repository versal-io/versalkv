mod driver;
mod pool;
mod stats;

use crate::foundation::io::transport::{
    ReadOutcome, ThreadIoHandle, TransportWakeHandle, WriteOutcome,
};
use crate::framework::capabilities::io::mt_driver::IoDriverMtConfig;
use crate::framework::capabilities::io::Token;
use crate::framework::context::IoThreadInfo;
use crate::runtime::server::worker::event_loop::transport::{
    RuntimePollCompletion, RuntimePollJob,
};
use crate::runtime::server::worker::maintenance::lazyfree::DataLazyfreeCompletion;
use crate::runtime::server::worker::replication::pending_lazyfree::ReplicationLazyfreeCompletion;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};

pub(crate) use pool::SharedIoPool;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct IoOperationId {
    pub(crate) token: Token,
    pub(crate) generation: u64,
    pub(crate) sequence: u64,
}

pub(crate) struct ReadJob {
    pub(crate) id: IoOperationId,
    pub(crate) handle: ThreadIoHandle,
    pub(crate) max_bytes: usize,
    pub(crate) drain_to_budget: bool,
}

pub(crate) struct ReadCompletion {
    pub(crate) id: IoOperationId,
    pub(crate) handle: ThreadIoHandle,
    pub(crate) bytes: Vec<u8>,
    pub(crate) outcome: ReadOutcome,
    pub(crate) drain_to_budget: bool,
    pub(crate) cancelled: bool,
}

pub(crate) struct WriteJob {
    pub(crate) id: IoOperationId,
    pub(crate) handle: ThreadIoHandle,
    pub(crate) frames: VecDeque<Vec<u8>>,
    pub(crate) byte_budget: usize,
}

pub(crate) struct WriteCompletion {
    pub(crate) id: IoOperationId,
    pub(crate) handle: ThreadIoHandle,
    pub(crate) frames: VecDeque<Vec<u8>>,
    pub(crate) written: usize,
    pub(crate) released_allocation_bytes: usize,
    pub(crate) outcome: WriteOutcome,
    pub(crate) cancelled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PollCompletionMetrics {
    pub(crate) generation: u64,
    pub(crate) returned_events: usize,
    pub(crate) cancelled: bool,
}

pub(crate) enum WorkerCompletionEvent {
    SocketRead(ReadCompletion),
    SocketWrite(WriteCompletion),
    PollReturned(PollCompletionMetrics),
    DataLazyfree(DataLazyfreeCompletion),
    ReplicationLazyfree(ReplicationLazyfreeCompletion),
}

impl std::fmt::Debug for WorkerCompletionEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SocketRead(completion) => formatter
                .debug_struct("SocketRead")
                .field("token", &completion.id.token)
                .field("generation", &completion.id.generation)
                .field("sequence", &completion.id.sequence)
                .field("bytes", &completion.bytes.len())
                .field("cancelled", &completion.cancelled)
                .finish(),
            Self::SocketWrite(completion) => formatter
                .debug_struct("SocketWrite")
                .field("token", &completion.id.token)
                .field("generation", &completion.id.generation)
                .field("sequence", &completion.id.sequence)
                .field("written", &completion.written)
                .field("cancelled", &completion.cancelled)
                .finish(),
            Self::PollReturned(metrics) => formatter
                .debug_tuple("PollReturned")
                .field(metrics)
                .finish(),
            Self::DataLazyfree(completion) => formatter
                .debug_tuple("DataLazyfree")
                .field(completion)
                .finish(),
            Self::ReplicationLazyfree(completion) => formatter
                .debug_tuple("ReplicationLazyfree")
                .field(completion)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct WorkerCompletionSender {
    sender: mpsc::Sender<WorkerCompletionEvent>,
    pending: Arc<AtomicBool>,
    wake: TransportWakeHandle,
}

impl WorkerCompletionSender {
    pub(crate) fn enqueue(&self, event: WorkerCompletionEvent) -> bool {
        if self.sender.send(event).is_err() {
            return false;
        }
        if !self.pending.swap(true, Ordering::AcqRel) {
            let _ = self.wake.wake();
        }
        true
    }

    pub(crate) fn enqueue_without_wake(&self, event: WorkerCompletionEvent) -> bool {
        self.sender.send(event).is_ok()
    }
}

pub(crate) struct WorkerCompletionQueue {
    sender: WorkerCompletionSender,
    receiver: mpsc::Receiver<WorkerCompletionEvent>,
    completed: VecDeque<WorkerCompletionEvent>,
}

impl WorkerCompletionQueue {
    pub(crate) fn new(wake: TransportWakeHandle) -> Self {
        let (sender, receiver) = mpsc::channel();
        let sender = WorkerCompletionSender {
            sender,
            pending: Arc::new(AtomicBool::new(false)),
            wake,
        };
        Self {
            sender,
            receiver,
            completed: VecDeque::new(),
        }
    }

    pub(crate) fn sender(&self) -> WorkerCompletionSender {
        self.sender.clone()
    }

    pub(crate) fn stage_pending(&mut self) {
        self.sender.pending.store(false, Ordering::Release);
        while let Ok(event) = self.receiver.try_recv() {
            self.completed.push_back(event);
        }
    }

    pub(crate) fn pop_staged(&mut self) -> Option<WorkerCompletionEvent> {
        self.completed.pop_front()
    }

    pub(crate) fn drain(&mut self) -> Vec<WorkerCompletionEvent> {
        self.stage_pending();
        self.completed.drain(..).collect()
    }

    pub(crate) fn recv_write_completion(
        &mut self,
        id: IoOperationId,
    ) -> Result<WriteCompletion, mpsc::RecvError> {
        if let Some(index) = self.completed.iter().position(
            |event| matches!(event, WorkerCompletionEvent::SocketWrite(completion) if completion.id == id),
        ) {
            if let Some(WorkerCompletionEvent::SocketWrite(completion)) = self.completed.remove(index)
            {
                return Ok(completion);
            }
        }
        loop {
            let event = self.receiver.recv()?;
            match event {
                WorkerCompletionEvent::SocketWrite(completion) if completion.id == id => {
                    return Ok(completion);
                }
                event => self.completed.push_back(event),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_for_completion_for_test(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<(), mpsc::RecvTimeoutError> {
        if !self.completed.is_empty() {
            return Ok(());
        }
        let event = self.receiver.recv_timeout(timeout)?;
        self.completed.push_back(event);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn recv_for_test(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<WorkerCompletionEvent, mpsc::RecvTimeoutError> {
        if let Some(event) = self.completed.pop_front() {
            return Ok(event);
        }
        self.receiver.recv_timeout(timeout)
    }
}

pub(crate) enum IoJob {
    Read(ReadJob),
    Write(WriteJob),
    Poll(RuntimePollJob),
    Shutdown,
}

pub(crate) enum IoCompletion {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Poll(RuntimePollCompletion),
    WorkerStopped,
}

impl IoJob {
    pub(crate) fn execute(self) -> Option<IoCompletion> {
        match self {
            Self::Read(job) => Some(IoCompletion::Read(pool::execute_read(job))),
            Self::Write(job) => Some(IoCompletion::Write(pool::execute_write(job))),
            Self::Poll(job) => Some(IoCompletion::Poll(job.execute())),
            Self::Shutdown => None,
        }
    }

    pub(crate) fn cancel(self) -> Option<IoCompletion> {
        match self {
            Self::Read(job) => Some(IoCompletion::Read(ReadCompletion {
                id: job.id,
                handle: job.handle,
                bytes: Vec::new(),
                outcome: ReadOutcome::Quiescent,
                drain_to_budget: job.drain_to_budget,
                cancelled: true,
            })),
            Self::Write(job) => Some(IoCompletion::Write(WriteCompletion {
                id: job.id,
                handle: job.handle,
                frames: job.frames,
                written: 0,
                released_allocation_bytes: 0,
                outcome: WriteOutcome::Backpressured,
                cancelled: true,
            })),
            Self::Poll(job) => Some(IoCompletion::Poll(job.cancel())),
            Self::Shutdown => None,
        }
    }
}

pub(crate) enum SubmitError<T> {
    Inactive(T),
    Full(T),
    Disconnected(T),
}

impl<T> std::fmt::Debug for SubmitError<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Inactive(_) => "SubmitError::Inactive",
            Self::Full(_) => "SubmitError::Full",
            Self::Disconnected(_) => "SubmitError::Disconnected",
        })
    }
}

impl<T> SubmitError<T> {
    pub(crate) fn into_inner(self) -> T {
        match self {
            Self::Inactive(value) | Self::Full(value) | Self::Disconnected(value) => value,
        }
    }
}

struct IoConnectionState {
    is_cluster_bus: bool,
    read_deferred: bool,
    generation: u64,
    next_sequence: u64,
    read_handle: Option<ThreadIoHandle>,
    write_handle: Option<ThreadIoHandle>,
    read_inflight: Option<IoOperationId>,
    write_inflight: Option<IoOperationId>,
    close_pending: bool,
}

#[derive(Debug)]
pub(crate) enum ReadCompletionAction {
    Apply {
        token: Token,
        bytes: Vec<u8>,
        outcome: ReadOutcome,
        drain_to_budget: bool,
    },
    CloseTransport(Token),
    Cancelled,
    Stale,
}

pub(crate) enum WriteSubmission {
    Submitted,
    AlreadyPending(std::collections::VecDeque<Vec<u8>>),
    Unavailable(std::collections::VecDeque<Vec<u8>>),
}

pub(crate) enum WriteCompletionAction {
    Apply {
        token: Token,
        frames: std::collections::VecDeque<Vec<u8>>,
        written: usize,
        released_allocation_bytes: usize,
        outcome: WriteOutcome,
    },
    CloseTransport(Token),
    Stale,
}

pub(crate) struct IoThreadRuntime {
    pool: SharedIoPool,
    connections: HashMap<Token, IoConnectionState>,
    next_generation: u64,
    #[cfg(test)]
    wake_token: Token,
    counter_reset_generation: u64,
    reads_processed_baseline: u64,
    writes_processed_baseline: u64,
    polls_processed_baseline: u64,
}

impl IoThreadRuntime {
    #[cfg(test)]
    pub(crate) fn new(
        config: IoDriverMtConfig,
        wake_token: Token,
        wake: TransportWakeHandle,
    ) -> std::io::Result<Self> {
        #[cfg(not(test))]
        let _ = wake_token;
        Ok(Self {
            pool: SharedIoPool::new(config, wake)?,
            connections: HashMap::new(),
            next_generation: 0,
            #[cfg(test)]
            wake_token,
            counter_reset_generation: 0,
            reads_processed_baseline: 0,
            writes_processed_baseline: 0,
            polls_processed_baseline: 0,
        })
    }

    pub(crate) fn new_with_completion_sender(
        config: IoDriverMtConfig,
        wake_token: Token,
        wake: TransportWakeHandle,
        shared_completions: WorkerCompletionSender,
    ) -> std::io::Result<Self> {
        #[cfg(not(test))]
        let _ = wake_token;
        Ok(Self {
            pool: SharedIoPool::with_completion_sender(config, wake, shared_completions)?,
            connections: HashMap::new(),
            next_generation: 0,
            #[cfg(test)]
            wake_token,
            counter_reset_generation: 0,
            reads_processed_baseline: 0,
            writes_processed_baseline: 0,
            polls_processed_baseline: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_queue_capacity_for_test(
        config: IoDriverMtConfig,
        wake_token: Token,
        wake: TransportWakeHandle,
        queue_capacity: usize,
    ) -> std::io::Result<Self> {
        Ok(Self {
            pool: SharedIoPool::new_with_queue_capacity_for_test(config, wake, queue_capacity)?,
            connections: HashMap::new(),
            next_generation: 0,
            wake_token,
            counter_reset_generation: 0,
            reads_processed_baseline: 0,
            writes_processed_baseline: 0,
            polls_processed_baseline: 0,
        })
    }

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

    fn restore_read_job(&mut self, job: ReadJob) {
        let Some(state) = self.connections.get_mut(&job.id.token) else {
            return;
        };
        if state.generation == job.id.generation && state.read_inflight == Some(job.id) {
            state.read_inflight = None;
            state.read_handle = Some(job.handle);
        }
    }

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

    #[cfg(test)]
    pub(crate) fn wake_token(&self) -> Token {
        self.wake_token
    }

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

#[cfg(test)]
mod tests;
