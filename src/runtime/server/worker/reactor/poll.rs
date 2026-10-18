#[cfg(test)]
use crate::foundation::io::sys::time::MonotonicInstant;
use crate::runtime::server::worker::reactor::transport::RuntimeEvent;

use super::ServerWorkerState;
#[cfg(test)]
use super::TurnTracePoint;

impl ServerWorkerState {
    #[cfg(test)]
    pub(super) fn prioritize_upstream_readiness(
        &self,
        events: &mut [(
            crate::framework::capabilities::io::Token,
            bool,
            bool,
            bool,
            bool,
        )],
    ) {
        events.sort_by_key(|(token, _, _, _, _)| !self.is_master_link(token.0));
    }

    fn prioritize_runtime_events(&self, events: &mut [RuntimeEvent]) {
        events.sort_by_key(|event| !self.is_master_link(event.token.0));
    }

    #[cfg(test)]
    pub(super) fn poll_ready(
        &mut self,
    ) -> std::io::Result<Option<(std::time::Instant, Vec<RuntimeEvent>)>> {
        self.sync_module_timer_event();
        let poll_timeout = self.next_poll_timeout(MonotonicInstant::now());
        self.poll_ready_with_timeout(poll_timeout)
    }

    pub(crate) fn poll_ready_with_timeout(
        &mut self,
        poll_timeout: Option<std::time::Duration>,
    ) -> std::io::Result<Option<(std::time::Instant, Vec<RuntimeEvent>)>> {
        self.events_buf.clear();
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::Poll);
        crate::framework::capabilities::module::fire_module_event_loop(
            &mut self.ctx,
            crate::engine::extension::module::VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP,
        );
        let (events, deferred_io_completions) = self.poll_with_io_threads(poll_timeout)?;
        self.events_buf = events;
        crate::framework::capabilities::module::fire_module_event_loop(
            &mut self.ctx,
            crate::engine::extension::module::VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP,
        );
        self.io_threads_after_poll(self.events_buf.len());
        self.apply_deferred_io_completions(deferred_io_completions);
        self.drain_io_completions();
        #[cfg(test)]
        self.trace_turn_for_test(TurnTracePoint::AfterPoll);
        let cycle_start = std::time::Instant::now();
        if self.exit_if_shutdown_requested() {
            self.shutdown_io_threads();
            return Ok(None);
        }
        let mut events = std::mem::take(&mut self.events_buf);
        self.prioritize_runtime_events(&mut events);
        Ok(Some((cycle_start, events)))
    }

    #[cfg(test)]
    pub(super) fn dispatch_ready_batch(&mut self, events: Vec<RuntimeEvent>) {
        self.collect_after_poll_events(Some((std::time::Instant::now(), events)));
        self.dispatch_ready_phase(super::DispatchContext::Normal)
            .expect("dispatch test readiness batch");
    }
}
