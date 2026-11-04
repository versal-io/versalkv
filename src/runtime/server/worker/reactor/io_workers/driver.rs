use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::io::Token;
#[cfg(test)]
use crate::runtime::server::worker::reactor::io_workers::IoOperationId;
use crate::runtime::server::worker::reactor::io_workers::{
    IoCompletion, PollCompletionMetrics, ReadCompletion, ReadCompletionAction,
    WorkerCompletionEvent, WriteCompletion,
};
use crate::runtime::server::worker::reactor::transport::{RuntimeEvent, RuntimePollCompletion};
#[cfg(test)]
use crate::runtime::server::worker::reactor::EventLoopEvent;
use crate::runtime::server::worker::PendingRemovalOwner;
use crate::runtime::server::worker::ServerWorkerState;
#[cfg(test)]
use std::time::Duration;

impl ServerWorkerState {
    pub(crate) fn configure_io_threads_from_config(&mut self) -> std::io::Result<()> {
        let configured = self.ctx.config().server.io_threads as usize;
        let always_active = self.ctx.config().server.io_threads_always_active;
        self.io_threads.configure(configured, always_active)?;
        self.sync_io_thread_stats();
        Ok(())
    }

    pub(crate) fn resolve_io_threads_reconfigure_request(&mut self) -> Option<Value> {
        let request = self.ctx.take_io_threads_reconfigure_request()?;
        let always_active = self.ctx.config().server.io_threads_always_active;
        match self.io_threads.configure(request.requested, always_active) {
            Ok(()) => Some(Value::ok()),
            Err(error) => {
                *self.ctx.config_mut() = request.previous;
                Some(Value::error(format!(
                    "ERR CONFIG SET failed (possibly related to argument 'io-threads') - {error}"
                )))
            }
        }
    }

    pub(crate) fn io_threads_after_poll(&mut self, event_count: usize) {
        let always_active = self.ctx.config().server.io_threads_always_active;
        self.io_threads.after_poll(event_count, always_active);
        self.sync_io_thread_stats();
    }

    pub(crate) fn io_threads_before_sleep(&mut self) {
        let always_active = self.ctx.config().server.io_threads_always_active;
        self.io_threads.before_sleep(always_active);
        self.sync_io_thread_stats();
    }

    pub(crate) fn try_submit_io_read(&mut self, token: Token, drain_to_budget: bool) -> bool {
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0))
            .then_some(token.0)
        else {
            return false;
        };
        let connection =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        if connection.blocked.is_some()
            || connection.protected
            || connection.slot_migration_role.is_some()
        {
            return false;
        }
        let submitted = self.io_threads.submit_read(
            token,
            if drain_to_budget {
                crate::runtime::server::worker::HUP_CLIENT_READ_BUDGET
            } else {
                crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET
            },
            drain_to_budget,
        );
        if submitted {
            if let Some(id) = self.io_threads.read_inflight_id(token) {
                self.acquire_io_worker_read_socket_lease(id);
            }
        }
        submitted
    }

    pub(crate) fn complete_io_read(&mut self, completion: ReadCompletion) {
        if self.io_threads.is_cluster_bus(completion.id.token) {
            self.complete_cluster_bus_read(completion);
            return;
        }
        let id = completion.id;
        match self.io_threads.complete_read(completion) {
            ReadCompletionAction::Apply {
                token,
                bytes,
                outcome,
                drain_to_budget,
            } => {
                let mode = if drain_to_budget {
                    crate::runtime::server::worker::sessions::ClientReadMode::DrainAfterHup
                } else {
                    crate::runtime::server::worker::sessions::ClientReadMode::Fair
                };
                self.process_client_read_outcome(token, mode, bytes, outcome);
            }
            ReadCompletionAction::CloseTransport(token) => {
                self.deregister_or_defer(token, PendingRemovalOwner::Client);
            }
            ReadCompletionAction::Cancelled => {}
            ReadCompletionAction::Stale => {}
        }
        self.release_io_worker_read_socket_lease(id);
    }

    pub(crate) fn drain_io_completions(&mut self) {
        while let Some(completion) = self.io_threads.pool().try_recv_completion() {
            self.enqueue_io_completion(completion);
        }
        self.dispatch_worker_completion_events();
        self.sync_io_thread_stats();
    }

    pub(crate) fn quiesce_ordinary_threaded_writes(&mut self) -> std::io::Result<()> {
        let pending: Vec<_> = self
            .connection_registry
            .iter()
            .filter_map(|(_, connection)| self.io_threads.write_inflight_id(connection.token()))
            .collect();
        // Each socket job is bounded and nonblocking. Reclaim its ownership
        // before entering the pause; retain reads and unrelated completions
        // without admitting commands that could start another write or fork.
        // A connection may have changed replication roles after submission;
        // the in-flight operation, rather than its current role, owns the I/O.
        for id in pending {
            if self.io_threads.write_inflight_id(id.token) != Some(id) {
                continue;
            }
            let completion = self
                .worker_completions
                .recv_write_completion(id)
                .map_err(std::io::Error::other)?;
            self.apply_threaded_write_completion(completion);
        }
        self.sync_io_thread_stats();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn wait_for_io_completion_for_test(
        &mut self,
        timeout: Duration,
    ) -> Result<(), std::sync::mpsc::RecvTimeoutError> {
        self.worker_completions
            .wait_for_completion_for_test(timeout)?;
        self.dispatch_worker_completion_events();
        self.sync_io_thread_stats();
        Ok(())
    }

    pub(crate) fn complete_io_write(&mut self, completion: WriteCompletion) {
        if self.io_threads.is_cluster_bus(completion.id.token) {
            self.complete_cluster_bus_write(completion);
            return;
        }
        self.apply_threaded_write_completion(completion);
    }

    pub(crate) fn sync_io_thread_stats(&mut self) {
        let reset_generation = self.ctx.io_thread_info().counter_reset_generation;
        self.io_threads.observe_counter_reset(reset_generation);
        self.ctx.set_io_thread_info(self.io_threads.info());
    }

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

    fn enqueue_io_completion(&mut self, completion: IoCompletion) {
        match completion {
            IoCompletion::Read(completion) => {
                self.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(completion));
            }
            IoCompletion::Write(completion) => {
                self.enqueue_worker_completion(WorkerCompletionEvent::SocketWrite(completion));
            }
            IoCompletion::Poll(completion) => self.complete_poll_worker_job(completion),
            IoCompletion::WorkerStopped => {}
        }
    }

    fn complete_poll_worker_job(&mut self, completion: RuntimePollCompletion) {
        let cancelled = completion.is_cancelled();
        let generation = completion.generation();
        self.io_threads.record_poll_completion(cancelled);
        match self.transport.finish_poll_offload(completion) {
            Ok(events) => {
                let returned_events = events.len();
                self.events_buf.extend(events);
                self.enqueue_main_thread_worker_completion(WorkerCompletionEvent::PollReturned(
                    PollCompletionMetrics {
                        generation,
                        returned_events,
                        cancelled,
                    },
                ));
            }
            Err(error) if cancelled && error.kind() == std::io::ErrorKind::Interrupted => {
                self.enqueue_main_thread_worker_completion(WorkerCompletionEvent::PollReturned(
                    PollCompletionMetrics {
                        generation,
                        returned_events: 0,
                        cancelled,
                    },
                ));
            }
            Err(error) => {
                crate::server_log!(warning, "Discarding failed IO poll: {error}");
                self.enqueue_main_thread_worker_completion(WorkerCompletionEvent::PollReturned(
                    PollCompletionMetrics {
                        generation,
                        returned_events: 0,
                        cancelled,
                    },
                ));
            }
        }
    }

    pub(crate) fn enqueue_worker_completion(&mut self, event: WorkerCompletionEvent) {
        let _ = self.worker_completions.sender().enqueue(event);
    }

    fn enqueue_main_thread_worker_completion(&mut self, event: WorkerCompletionEvent) {
        let _ = self.worker_completions.sender().enqueue_without_wake(event);
    }

    #[cfg(test)]
    pub(crate) fn complete_test_data_lazyfree(&mut self, pending_effort: u64, freed_objects: u64) {
        self.enqueue_worker_completion(WorkerCompletionEvent::DataLazyfree(
            crate::runtime::server::worker::flows::maintenance::lazyfree::DataLazyfreeCompletion {
                pending_effort,
                freed_objects,
            },
        ));
    }

    #[cfg(test)]
    pub(crate) fn complete_test_replication_lazyfree(&mut self, completed_effort: u64) {
        self.enqueue_worker_completion(WorkerCompletionEvent::ReplicationLazyfree(
            crate::runtime::server::worker::distributed::replication::pending_lazyfree::ReplicationLazyfreeCompletion {
                completed_effort,
            },
        ));
    }

    #[cfg(test)]
    pub(crate) fn complete_test_socket_read(&mut self, token: Token, bytes: Vec<u8>) {
        let (worker, _peer) =
            std::os::unix::net::UnixStream::pair().expect("test socket read handle");
        self.enqueue_worker_completion(WorkerCompletionEvent::SocketRead(ReadCompletion {
            id: IoOperationId {
                token,
                generation: 1,
                sequence: 1,
            },
            handle: crate::foundation::io::transport::ThreadIoHandle::Unix(worker),
            bytes,
            outcome: crate::foundation::io::transport::ReadOutcome::Quiescent,
            drain_to_budget: false,
            cancelled: false,
        }));
    }

    #[cfg(test)]
    pub(crate) fn collect_control_events_for_test(&mut self) -> Vec<EventLoopEvent> {
        self.collect_worker_completion_events()
    }

    #[cfg(test)]
    pub(crate) fn recv_worker_completion_for_test(
        &mut self,
        timeout: Duration,
    ) -> Result<WorkerCompletionEvent, std::sync::mpsc::RecvTimeoutError> {
        self.worker_completions.recv_for_test(timeout)
    }

    #[cfg(test)]
    pub(crate) fn enqueue_poll_returned_metric_for_test(&mut self) {
        self.enqueue_main_thread_worker_completion(WorkerCompletionEvent::PollReturned(
            PollCompletionMetrics {
                generation: 1,
                returned_events: 0,
                cancelled: false,
            },
        ));
    }

    #[cfg(test)]
    pub(crate) fn collect_worker_completion_events(&mut self) -> Vec<EventLoopEvent> {
        self.worker_completions
            .drain()
            .into_iter()
            .map(EventLoopEvent::worker_completion)
            .collect()
    }

    pub(crate) fn dispatch_worker_completion_events(&mut self) {
        // Keep undispatched completions reachable when a read command needs
        // to reclaim an in-flight write before entering a replication pause.
        self.worker_completions.stage_pending();
        self.begin_cluster_bus_receive_batch();
        while let Some(completion) = self.worker_completions.pop_staged() {
            self.dispatch_worker_completion_now(completion);
        }
        self.finish_cluster_bus_receive_batch();
    }

    pub(crate) fn apply_worker_completion(&mut self, completion: WorkerCompletionEvent) {
        match completion {
            WorkerCompletionEvent::SocketRead(completion) => self.complete_io_read(completion),
            WorkerCompletionEvent::SocketWrite(completion) => self.complete_io_write(completion),
            WorkerCompletionEvent::PollReturned(_) => {}
            WorkerCompletionEvent::DataLazyfree(completion) => {
                self.complete_data_lazyfree(completion);
            }
            WorkerCompletionEvent::ReplicationLazyfree(completion) => {
                self.complete_replication_lazyfree(completion);
            }
        }
    }

    pub(crate) fn shutdown_io_threads(&mut self) {
        self.transport.start_shutdown();
        let completions = self.io_threads.shutdown_and_drain();
        for completion in completions {
            match completion {
                IoCompletion::Poll(completion) => {
                    self.io_threads
                        .record_poll_completion(completion.is_cancelled());
                    let _ = self.transport.finish_poll_offload(completion);
                }
                IoCompletion::Read(completion) => self
                    .io_threads
                    .discard_read_completion_for_shutdown(completion),
                IoCompletion::Write(completion) => self
                    .io_threads
                    .discard_write_completion_for_shutdown(completion),
                IoCompletion::WorkerStopped => {}
            }
        }
        self.discard_worker_completion_events_for_shutdown();
        self.sync_io_thread_stats();
    }

    fn discard_worker_completion_events_for_shutdown(&mut self) {
        for completion in self.worker_completions.drain() {
            match completion {
                WorkerCompletionEvent::SocketRead(completion) => self
                    .io_threads
                    .discard_read_completion_for_shutdown(completion),
                WorkerCompletionEvent::SocketWrite(completion) => self
                    .io_threads
                    .discard_write_completion_for_shutdown(completion),
                WorkerCompletionEvent::PollReturned(_) => {}
                WorkerCompletionEvent::DataLazyfree(completion) => {
                    self.complete_data_lazyfree(completion);
                }
                WorkerCompletionEvent::ReplicationLazyfree(completion) => {
                    self.complete_replication_lazyfree(completion);
                }
            }
        }
    }
}
