use crate::runtime::server::worker::event_loop::io_workers::{
    IoCompletion, PollCompletionMetrics, WorkerCompletionEvent,
};
use crate::runtime::server::worker::event_loop::transport::RuntimeEvent;
use crate::runtime::server::worker::ServerWorkerState;

impl ServerWorkerState {
    pub(crate) fn poll_with_io_threads(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<(Vec<RuntimeEvent>, Vec<IoCompletion>)> {
        if !self.io_threads.should_offload_poll()
            || self.transport.poll_inflight()
            || self.transport.shutdown_started()
        {
            return self
                .transport
                .poll_events(timeout)
                .map(|events| (events, Vec::new()));
        }
        let job = match self.transport.begin_poll_offload(timeout) {
            Ok(job) => job,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                return self
                    .transport
                    .poll_events(timeout)
                    .map(|events| (events, Vec::new()));
            }
            Err(error) => return Err(error),
        };
        let generation = job.generation();
        match self.io_threads.try_submit_poll(job) {
            Ok(()) => self.wait_for_poll_completion(generation),
            Err(error) => self
                .transport
                .finish_poll_offload(error.into_inner().execute())
                .map(|events| (events, Vec::new())),
        }
    }

    fn wait_for_poll_completion(
        &mut self,
        generation: u64,
    ) -> std::io::Result<(Vec<RuntimeEvent>, Vec<IoCompletion>)> {
        let mut deferred = Vec::new();
        loop {
            let completion = self
                .io_threads
                .pool()
                .recv_completion()
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
            match completion {
                IoCompletion::Poll(completion) => {
                    let cancelled = completion.is_cancelled();
                    self.io_threads.record_poll_completion(cancelled);
                    let completion_generation = completion.generation();
                    let poll_result = self.transport.finish_poll_offload(completion);
                    let returned_events = poll_result.as_ref().map(Vec::len).unwrap_or(0);
                    self.enqueue_main_thread_worker_completion(
                        WorkerCompletionEvent::PollReturned(PollCompletionMetrics {
                            generation: completion_generation,
                            returned_events,
                            cancelled,
                        }),
                    );
                    if completion_generation != generation {
                        self.apply_deferred_io_completions(deferred);
                        return poll_result.and_then(|_| {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "unexpected poll completion generation",
                            ))
                        });
                    }
                    return match poll_result {
                        Ok(events) => Ok((events, deferred)),
                        Err(error) => {
                            self.apply_deferred_io_completions(deferred);
                            Err(error)
                        }
                    };
                }
                IoCompletion::Read(_) | IoCompletion::Write(_) => deferred.push(completion),
                IoCompletion::WorkerStopped => {}
            }
        }
    }

    pub(crate) fn apply_deferred_io_completions(&mut self, completions: Vec<IoCompletion>) {
        for completion in completions {
            self.enqueue_io_completion(completion);
        }
        self.dispatch_worker_completion_events();
        self.sync_io_thread_stats();
    }
}
