use super::{IoThreadRuntime, SharedIoPool, TransportWakeHandle, WorkerCompletionSender};
use crate::framework::capabilities::io::mt_driver::IoDriverMtConfig;
use crate::framework::capabilities::io::Token;
use std::collections::HashMap;

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
}
