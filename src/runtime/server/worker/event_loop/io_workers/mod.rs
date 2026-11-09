mod completions;
mod core;
mod lifecycle;
mod poll_offload;
mod pool;
mod registry;
mod stats;
mod submit;

use crate::foundation::io::transport::{
    ReadOutcome, ThreadIoHandle, TransportWakeHandle, WriteOutcome,
};
use crate::framework::capabilities::io::Token;
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

pub(crate) struct IoConnectionState {
    pub(crate) is_cluster_bus: bool,
    pub(crate) read_deferred: bool,
    pub(crate) generation: u64,
    pub(crate) next_sequence: u64,
    pub(crate) read_handle: Option<ThreadIoHandle>,
    pub(crate) write_handle: Option<ThreadIoHandle>,
    pub(crate) read_inflight: Option<IoOperationId>,
    pub(crate) write_inflight: Option<IoOperationId>,
    pub(crate) close_pending: bool,
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
    pub(crate) pool: SharedIoPool,
    pub(crate) connections: HashMap<Token, IoConnectionState>,
    pub(crate) next_generation: u64,
    #[cfg(test)]
    pub(crate) wake_token: Token,
    pub(crate) counter_reset_generation: u64,
    pub(crate) reads_processed_baseline: u64,
    pub(crate) writes_processed_baseline: u64,
    pub(crate) polls_processed_baseline: u64,
}

impl IoThreadRuntime {
    #[cfg(test)]
    pub(crate) fn wake_token(&self) -> Token {
        self.wake_token
    }
}

#[cfg(test)]
mod tests;
