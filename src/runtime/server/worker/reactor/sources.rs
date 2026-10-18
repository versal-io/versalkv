use super::EventDependencies;
use crate::engine::distributed::cluster::migration::MigrationJobId;
use crate::framework::capabilities::io::Token;
use std::collections::{HashMap, VecDeque};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RuntimeSourceHandle {
    pub(crate) token: Token,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRoute {
    pub(crate) owner: SourceOwner,
    pub(crate) blocked_policy: BlockedPolicy,
}

impl SourceRoute {
    pub(crate) fn dependencies(
        &self,
        token: Token,
        readable: bool,
        writable: bool,
        read_closed: bool,
        error: bool,
    ) -> EventDependencies {
        match self.owner {
            SourceOwner::IoWorkerWake | SourceOwner::ControlWake | SourceOwner::ChildSignal(_) => {
                EventDependencies::safe_producer()
            }
            SourceOwner::InternalPipe(
                InternalPipeOwner::DisklessRdb { .. }
                | InternalPipeOwner::SlotSnapshot { .. }
                | InternalPipeOwner::DirectDualPhase { .. },
            ) => EventDependencies::safe_producer(),
            SourceOwner::Listener => EventDependencies::safe_producer(),
            SourceOwner::Connection | SourceOwner::ClusterBus | SourceOwner::Sentinel => {
                if readable || read_closed || error {
                    EventDependencies::client_read(token)
                } else if writable {
                    EventDependencies::client_write(token)
                } else {
                    EventDependencies::none()
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceOwner {
    IoWorkerWake,
    // Task 6 installs the shared control wake source.
    // TODO(dead_code): remove after the control wake source is registered.
    #[allow(dead_code)]
    ControlWake,
    Listener,
    Connection,
    ClusterBus,
    Sentinel,
    // Task 9 installs the compatibility pipe as a registered runtime source.
    // TODO(dead_code): remove after the pipe registration owns this route.
    #[allow(dead_code)]
    InternalPipe(InternalPipeOwner),
    // Task 7 installs child-completion readiness sources.
    // TODO(dead_code): remove after child signal sources are registered.
    #[allow(dead_code)]
    ChildSignal(ChildSignalOwner),
}

impl SourceOwner {
    fn blocked_policy(&self) -> BlockedPolicy {
        match self {
            Self::IoWorkerWake | Self::ControlWake | Self::ChildSignal(_) | Self::Listener => {
                BlockedPolicy::Always
            }
            Self::Connection | Self::ClusterBus | Self::Sentinel | Self::InternalPipe(_) => {
                BlockedPolicy::DependencyChecked
            }
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::IoWorkerWake => "IO worker wake",
            Self::ControlWake => "control wake",
            Self::Listener => "listener",
            Self::Connection => "connection",
            Self::ClusterBus => "cluster bus",
            Self::Sentinel => "sentinel",
            Self::InternalPipe(_) => "internal pipe",
            Self::ChildSignal(_) => "child signal",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChildSignalOwner {
    SelfPipe,
    // TODO(dead_code): Tasks 8-12 install concrete pidfd routes for each child owner.
    #[allow(dead_code)]
    PidFd(ChildHandle),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InternalPipeOwner {
    DisklessRdb { transfer_id: TransferId },
    SlotSnapshot { job_id: MigrationJobId },
    DirectDualPhase { child: ChildHandle },
}

// Tasks 9-11 assign transfer identifiers when the current internal pipe pumps
// become registered readiness sources.
// TODO(dead_code): remove after internal pipe source registration uses this handle.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TransferId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ChildHandle {
    pub(crate) id: u64,
    pub(crate) generation: u64,
}

// TODO(dead_code): Tasks 8-12 construct every owner while migrating child
// finalization from compatibility reapers to ChildCompletion events.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChildOwner {
    RdbSave,
    AofRewrite,
    DisklessReplication,
    DiskSyncReplication,
    SlotSnapshot(MigrationJobId),
    DirectDualRdb,
}

#[derive(Debug)]
pub(crate) enum ChildExitOutcome {
    Exited(i32),
    Signaled(i32),
    WaitFailed(std::io::Error),
}

impl Clone for ChildExitOutcome {
    fn clone(&self) -> Self {
        match self {
            Self::Exited(code) => Self::Exited(*code),
            Self::Signaled(signal) => Self::Signaled(*signal),
            Self::WaitFailed(error) => {
                Self::WaitFailed(std::io::Error::new(error.kind(), error.to_string()))
            }
        }
    }
}

impl PartialEq for ChildExitOutcome {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Exited(left), Self::Exited(right)) => left == right,
            (Self::Signaled(left), Self::Signaled(right)) => left == right,
            (Self::WaitFailed(left), Self::WaitFailed(right)) => {
                left.kind() == right.kind() && left.to_string() == right.to_string()
            }
            _ => false,
        }
    }
}

impl Eq for ChildExitOutcome {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChildCompletionEvent {
    pub(crate) child: ChildHandle,
    pub(crate) outcome: ChildExitOutcome,
}

#[derive(Clone, Debug)]
struct ChildEntry {
    handle: ChildHandle,
    owner: ChildOwner,
    pid: i32,
    completion: Option<ChildCompletionEvent>,
    completion_queued: bool,
    deferred: bool,
    pidfd_registered: bool,
    pidfd_source: Option<RuntimeSourceHandle>,
}

#[derive(Default)]
pub(crate) struct ChildRegistry {
    entries: HashMap<ChildHandle, ChildEntry>,
    pid_index: HashMap<i32, ChildHandle>,
    completions: VecDeque<ChildCompletionEvent>,
    next_id: u64,
}

impl ChildRegistry {
    #[cfg(test)]
    pub(crate) fn register_test_child(&mut self, owner: ChildOwner, pid: i32) -> ChildHandle {
        self.register_child(owner, pid)
    }

    // TODO(dead_code): concrete fork owners call this as Tasks 8-12 migrate.
    #[allow(dead_code)]
    pub(crate) fn register_child(&mut self, owner: ChildOwner, pid: i32) -> ChildHandle {
        self.next_id = self.next_id.saturating_add(1).max(1);
        let generation = self
            .entries
            .get(&ChildHandle {
                id: self.next_id,
                generation: 0,
            })
            .map_or(1, |entry| entry.handle.generation.saturating_add(1));
        let handle = ChildHandle {
            id: self.next_id,
            generation,
        };
        self.pid_index.insert(pid, handle);
        self.entries.insert(
            handle,
            ChildEntry {
                handle,
                owner,
                pid,
                completion: None,
                completion_queued: false,
                deferred: false,
                pidfd_registered: false,
                pidfd_source: None,
            },
        );
        handle
    }

    // TODO(dead_code): owner migration tests and handlers use this liveness probe.
    #[allow(dead_code)]
    pub(crate) fn contains(&self, child: ChildHandle) -> bool {
        self.entries.contains_key(&child)
    }

    // TODO(dead_code): SIGCHLD fallback and owner migration tests resolve by pid.
    #[allow(dead_code)]
    pub(crate) fn child_for_pid(&self, pid: i32) -> Option<ChildHandle> {
        self.pid_index.get(&pid).copied()
    }

    // TODO(dead_code): owner-specific completion dispatch starts in Task 8.
    #[allow(dead_code)]
    pub(crate) fn owner(&self, child: ChildHandle) -> Option<&ChildOwner> {
        self.entries.get(&child).map(|entry| &entry.owner)
    }

    pub(crate) fn mark_pidfd_registered(&mut self, child: ChildHandle) -> bool {
        let Some(entry) = self.entries.get_mut(&child) else {
            return false;
        };
        if entry.pidfd_registered {
            return false;
        }
        entry.pidfd_registered = true;
        true
    }

    pub(crate) fn set_pidfd_source(&mut self, child: ChildHandle, source: RuntimeSourceHandle) {
        if let Some(entry) = self.entries.get_mut(&child) {
            entry.pidfd_source = Some(source);
        }
    }

    pub(crate) fn collect_exited_children(&mut self) {
        let pending: Vec<(ChildHandle, i32)> = self
            .entries
            .values()
            .filter(|entry| !entry.completion_queued)
            .map(|entry| (entry.handle, entry.pid))
            .collect();
        for (child, pid) in pending {
            self.collect_child_exit_for_pid(child, pid);
        }
    }

    pub(crate) fn collect_child_exit(&mut self, child: ChildHandle) {
        let Some(pid) = self.entries.get(&child).map(|entry| entry.pid) else {
            return;
        };
        self.collect_child_exit_for_pid(child, pid);
    }

    fn collect_child_exit_for_pid(&mut self, child: ChildHandle, pid: i32) {
        let outcome = match crate::foundation::io::sys::process::wait_nonblocking(pid) {
            Ok(crate::foundation::io::sys::process::WaitOutcome::Running) => return,
            Ok(crate::foundation::io::sys::process::WaitOutcome::Exited(code)) => {
                ChildExitOutcome::Exited(code)
            }
            Ok(crate::foundation::io::sys::process::WaitOutcome::Signaled(signal)) => {
                ChildExitOutcome::Signaled(signal)
            }
            Ok(crate::foundation::io::sys::process::WaitOutcome::Other(status)) => {
                ChildExitOutcome::WaitFailed(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected wait status {status}"),
                ))
            }
            Err(error) => ChildExitOutcome::WaitFailed(error),
        };
        self.record_exit(child, outcome);
    }

    #[cfg(test)]
    pub(crate) fn record_exit_for_test(&mut self, child: ChildHandle, outcome: ChildExitOutcome) {
        self.record_exit(child, outcome);
    }

    // TODO(dead_code): SIGCHLD fallback records waitpid results by pid.
    #[allow(dead_code)]
    pub(crate) fn record_exit_for_pid(&mut self, pid: i32, outcome: ChildExitOutcome) {
        if let Some(child) = self.child_for_pid(pid) {
            self.record_exit(child, outcome);
        }
    }

    pub(crate) fn record_exit(&mut self, child: ChildHandle, outcome: ChildExitOutcome) {
        let Some(entry) = self.entries.get_mut(&child) else {
            return;
        };
        if entry.completion_queued {
            return;
        }
        let completion = ChildCompletionEvent { child, outcome };
        entry.completion = Some(completion.clone());
        entry.completion_queued = true;
        self.completions.push_back(completion);
    }

    pub(crate) fn drain_completions(&mut self) -> Vec<ChildCompletionEvent> {
        self.completions.drain(..).collect()
    }

    pub(crate) fn take_completion(&mut self, child: ChildHandle) -> Option<ChildCompletionEvent> {
        if let Some(position) = self
            .completions
            .iter()
            .position(|completion| completion.child == child)
        {
            return self.completions.remove(position);
        }
        self.entries
            .get(&child)
            .and_then(|entry| entry.completion.clone())
    }

    pub(crate) fn defer_completion(&mut self, child: ChildHandle) {
        if let Some(entry) = self.entries.get_mut(&child) {
            entry.deferred = true;
        }
    }

    pub(crate) fn finalize_completion(
        &mut self,
        child: ChildHandle,
    ) -> Result<Option<RuntimeSourceHandle>, ChildHandle> {
        let Some(entry) = self.entries.remove(&child) else {
            return Err(child);
        };
        self.pid_index.remove(&entry.pid);
        Ok(entry.pidfd_source)
    }

    pub(crate) fn retain_only_pid(&mut self, active_pid: Option<i32>) -> Vec<RuntimeSourceHandle> {
        let remove: Vec<ChildHandle> = self
            .entries
            .iter()
            .filter_map(|(handle, entry)| (Some(entry.pid) != active_pid).then_some(*handle))
            .collect();
        let mut removed_sources = Vec::new();
        for handle in remove {
            if let Ok(Some(source)) = self.finalize_completion(handle) {
                removed_sources.push(source);
            }
        }
        self.completions
            .retain(|completion| self.entries.contains_key(&completion.child));
        removed_sources
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlockedPolicy {
    Always,
    DependencyChecked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeSourceRegistryError {
    Duplicate(Token),
    Stale(Token),
}

impl fmt::Display for RuntimeSourceRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(token) => {
                write!(
                    formatter,
                    "runtime source token {} is already routed",
                    token.0
                )
            }
            Self::Stale(token) => {
                write!(formatter, "runtime source token {} is stale", token.0)
            }
        }
    }
}

impl std::error::Error for RuntimeSourceRegistryError {}

#[derive(Default)]
pub(crate) struct RuntimeSourceRegistry {
    routes: HashMap<Token, SourceRoute>,
}

impl RuntimeSourceRegistry {
    pub(crate) fn install(
        &mut self,
        token: Token,
        owner: SourceOwner,
    ) -> Result<RuntimeSourceHandle, RuntimeSourceRegistryError> {
        if self.routes.contains_key(&token) {
            return Err(RuntimeSourceRegistryError::Duplicate(token));
        }
        let blocked_policy = owner.blocked_policy();
        self.routes.insert(
            token,
            SourceRoute {
                owner,
                blocked_policy,
            },
        );
        Ok(RuntimeSourceHandle { token })
    }

    // Task 4 resolves queued handles directly when production dispatch moves
    // from raw readiness values to EventLoopEvent batches.
    // TODO(dead_code): remove after handle-based production dispatch lands.
    #[allow(dead_code)]
    pub(crate) fn resolve(&self, handle: RuntimeSourceHandle) -> Option<&SourceRoute> {
        self.resolve_token(handle.token)
    }

    pub(crate) fn resolve_token(&self, token: Token) -> Option<&SourceRoute> {
        self.routes.get(&token)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.routes.len()
    }

    pub(crate) fn retire(
        &mut self,
        handle: RuntimeSourceHandle,
    ) -> Result<SourceRoute, RuntimeSourceRegistryError> {
        self.routes
            .remove(&handle.token)
            .ok_or(RuntimeSourceRegistryError::Stale(handle.token))
    }
}
