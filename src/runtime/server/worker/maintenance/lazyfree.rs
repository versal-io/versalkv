use crate::foundation::io::store::Robj;
use crate::foundation::io::sys::process::{apply_prepared_cpu_affinity, PreparedCpuAffinity};
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

const ASYNC_OBJECT_THRESHOLD: u64 = 64;

struct ObjectFreeJob {
    object: Robj,
    effort: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DataLazyfreeCompletion {
    pub(crate) pending_effort: u64,
    pub(crate) freed_objects: u64,
}

enum DataLazyfreeJob {
    Free(ObjectFreeJob),
    ConfigureCpuAffinity {
        affinity: PreparedCpuAffinity,
        result: Sender<io::Result<()>>,
    },
}

enum DataLazyfreeCompletionSink {
    #[cfg(test)]
    Local(Sender<DataLazyfreeCompletion>),
    Shared(WorkerCompletionSender),
}

impl DataLazyfreeCompletionSink {
    fn complete(&self, completion: DataLazyfreeCompletion) -> bool {
        match self {
            #[cfg(test)]
            Self::Local(sender) => sender.send(completion).is_ok(),
            Self::Shared(sender) => sender.enqueue(WorkerCompletionEvent::DataLazyfree(completion)),
        }
    }
}

/// L5-owned worker for object drops that would otherwise monopolize the event
/// loop. Command handlers only transfer detached values through their
/// connection side-effect queue; the runtime owns worker lifetime and stats.
pub(crate) struct DataLazyfree {
    jobs: Option<Sender<DataLazyfreeJob>>,
    #[cfg(test)]
    completions: Option<Receiver<DataLazyfreeCompletion>>,
    #[cfg(test)]
    completed: VecDeque<DataLazyfreeCompletion>,
    worker: Option<JoinHandle<()>>,
}

impl DataLazyfree {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::new_with_sink(None)
    }

    pub(crate) fn new_with_completion_sender(sender: WorkerCompletionSender) -> Self {
        Self::new_with_sink(Some(sender))
    }

    fn new_with_sink(shared_sender: Option<WorkerCompletionSender>) -> Self {
        let (job_tx, job_rx) = mpsc::channel::<DataLazyfreeJob>();
        #[cfg(test)]
        let mut completion_rx = None;
        let completion_sink = match shared_sender {
            Some(sender) => DataLazyfreeCompletionSink::Shared(sender),
            #[cfg(test)]
            None => {
                let (completion_tx, receiver) = mpsc::channel();
                completion_rx = Some(receiver);
                DataLazyfreeCompletionSink::Local(completion_tx)
            }
            #[cfg(not(test))]
            None => unreachable!("production lazyfree uses the shared completion queue"),
        };
        let worker = thread::Builder::new()
            .name("pkv-lazyfree".to_string())
            .spawn(move || {
                while let Ok(job) = job_rx.recv() {
                    match job {
                        DataLazyfreeJob::Free(ObjectFreeJob { object, effort }) => {
                            drop(object);
                            if !completion_sink.complete(DataLazyfreeCompletion {
                                pending_effort: effort,
                                freed_objects: 1,
                            }) {
                                break;
                            }
                        }
                        DataLazyfreeJob::ConfigureCpuAffinity { affinity, result } => {
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

    fn effort_for(object: &Robj) -> u64 {
        object
            .as_collection()
            .map(|collection| collection.len() as u64)
            .unwrap_or(1)
    }

    /// Returns zero when C's strict lazyfree threshold keeps the drop on the
    /// event-loop thread. On enqueue failure the caller receives ownership
    /// back and drops it synchronously without claiming a completion.
    pub(crate) fn submit(&mut self, object: Robj) -> Result<u64, Robj> {
        let effort = Self::effort_for(&object);
        if effort <= ASYNC_OBJECT_THRESHOLD {
            drop(object);
            return Ok(0);
        }
        let job = ObjectFreeJob { object, effort };
        let Some(sender) = self.jobs.as_ref() else {
            return Err(job.object);
        };
        sender
            .send(DataLazyfreeJob::Free(job))
            .map(|_| effort)
            .map_err(|error| match error.0 {
                DataLazyfreeJob::Free(job) => job.object,
                DataLazyfreeJob::ConfigureCpuAffinity { .. } => {
                    unreachable!("data lazyfree send failure preserves the submitted job kind")
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
                "data lazyfree worker unavailable",
            )
        })?;
        let (result_tx, result_rx) = mpsc::channel();
        sender
            .send(DataLazyfreeJob::ConfigureCpuAffinity {
                affinity,
                result: result_tx,
            })
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "data lazyfree worker stopped")
            })?;
        result_rx.recv().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "data lazyfree worker did not report affinity setup",
            )
        })?
    }

    #[cfg(test)]
    pub(crate) fn poll_completions(&mut self) -> (u64, u64) {
        let mut pending_effort = 0u64;
        let mut freed_objects = 0u64;
        while let Some(completion) = self.completed.pop_front() {
            pending_effort = pending_effort.saturating_add(completion.pending_effort);
            freed_objects = freed_objects.saturating_add(completion.freed_objects);
        }
        if let Some(completions) = &self.completions {
            while let Ok(completion) = completions.try_recv() {
                pending_effort = pending_effort.saturating_add(completion.pending_effort);
                freed_objects = freed_objects.saturating_add(completion.freed_objects);
            }
        }
        (pending_effort, freed_objects)
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
}

#[cfg(test)]
impl Default for DataLazyfree {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DataLazyfree {
    fn drop(&mut self) {
        self.jobs.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl crate::runtime::server::worker::ServerWorkerState {
    pub(crate) fn dispose_data_lazyfree_objects(
        &mut self,
        objects: Vec<crate::foundation::io::store::Robj>,
    ) {
        for object in objects {
            match self.data_lazyfree.submit(object) {
                Ok(0) => {}
                Ok(effort) => self.ctx.add_lazyfree_pending_objects(effort),
                Err(_object) => {}
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn drain_data_lazyfree_completions_for_test(&mut self) {
        let (pending_effort, freed_objects) = self.data_lazyfree.poll_completions();
        self.complete_data_lazyfree(DataLazyfreeCompletion {
            pending_effort,
            freed_objects,
        });
        self.dispatch_worker_completion_events();
    }

    pub(crate) fn complete_data_lazyfree(&mut self, completion: DataLazyfreeCompletion) {
        let pending_effort = completion.pending_effort;
        let freed_objects = completion.freed_objects;
        if pending_effort > 0 {
            self.ctx
                .stats_mut()
                .complete_lazyfree_work(pending_effort, freed_objects);
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_for_data_lazyfree_for_test(&mut self) {
        self.data_lazyfree.wait_for_completion_for_test();
        let _ = self
            .worker_completions
            .wait_for_completion_for_test(Duration::from_secs(5));
    }
}

#[cfg(test)]
mod tests {
    use super::DataLazyfree;
    use crate::foundation::io::store::structure::list::ListValue;
    use crate::foundation::io::store::Robj;
    use crate::framework::capabilities::connection::CommandEffectDrain;
    use bytes::Bytes;
    use std::collections::VecDeque;

    fn large_list() -> Robj {
        Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(
            (0..65)
                .map(|_| Bytes::from_static(b"value"))
                .collect::<Vec<_>>(),
        )))
    }

    #[test]
    fn large_objects_are_freed_on_the_background_worker() {
        let mut lazyfree = DataLazyfree::new();

        assert_eq!(lazyfree.submit(large_list()).expect("queue object"), 65);

        lazyfree.wait_for_completion_for_test();
        assert_eq!(lazyfree.poll_completions(), (65, 1));
    }

    #[test]
    fn runtime_accounts_deferred_connection_objects_after_worker_completion() {
        let mut runtime =
            crate::runtime::server::worker::ServerWorkerState::new(1).expect("create runtime");
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.defer_command_lazyfree_object(large_list());
        let connection_token = runtime.connection_registry.insert(connection);

        let mut effects = runtime.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .take_command_effects(CommandEffectDrain::Completed)
        .expect("deferred object marks command effects pending");
        runtime.dispose_data_lazyfree_objects(effects.take_lazyfree_objects());
        assert_eq!(runtime.ctx.stats().lazyfree_pending_objects, 65);
        assert_eq!(runtime.ctx.stats().lazyfreed_objects, 0);

        runtime.wait_for_data_lazyfree_for_test();
        runtime.drain_data_lazyfree_completions_for_test();
        assert_eq!(runtime.ctx.stats().lazyfree_pending_objects, 0);
        assert_eq!(runtime.ctx.stats().lazyfreed_objects, 1);
    }
}
