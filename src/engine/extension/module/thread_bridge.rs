//! Owned messages between native module threads and the database owner.

use bytes::Bytes;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, TryLockError, Weak};
use std::time::Duration;

use super::api::call::ModuleCallFormat;
use super::registry::ModuleOptions;
use crate::engine::command::{CommandArgs, SessionId};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::transport::TransportWakeHandle;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static UNBOUND_THREAD_EXPORTS: AtomicBool = AtomicBool::new(false);

const MAX_PENDING_ACQUISITIONS: usize = 1024;
const MAX_PENDING_COMPLETIONS: usize = 4096;

pub(crate) fn next_id() -> Result<u64, libc::c_int> {
    NEXT_ID
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| libc::EOVERFLOW)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ModuleId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ModuleBlockedId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ModuleThreadContextId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModuleLease {
    pub id: u64,
    pub owner: u64,
}

pub struct ModuleIdentity {
    pub id: ModuleId,
    pub name: String,
    pub options: ModuleOptions,
    pub is_active: AtomicBool,
    pub exported_threads: AtomicBool,
    pub contexts: AtomicUsize,
    pub blocked_handles: AtomicUsize,
    pub(crate) invocations: AtomicUsize,
}

impl ModuleIdentity {
    pub(crate) fn new(name: String, options: ModuleOptions) -> Result<Arc<Self>, libc::c_int> {
        Ok(Arc::new(Self {
            id: ModuleId(next_id()?),
            name,
            options,
            is_active: AtomicBool::new(true),
            exported_threads: AtomicBool::new(false),
            contexts: AtomicUsize::new(0),
            blocked_handles: AtomicUsize::new(0),
            invocations: AtomicUsize::new(0),
        }))
    }

    pub fn has_active_threads(&self) -> bool {
        self.contexts.load(Ordering::Acquire) != 0
            || self.blocked_handles.load(Ordering::Acquire) != 0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ModuleContextKind {
    #[default]
    Synchronous,
    ThreadSafe,
    BlockedReply,
    BlockedTimeout,
    DisconnectedCleanup,
    Notification,
    KeyPosition,
    InternalCall,
}

#[derive(Clone)]
pub struct ModuleBlockOrigin {
    pub session: SessionId,
    pub token: crate::engine::command::context::Token,
    pub command_seq: u64,
    pub args: CommandArgs,
    pub request_bytes: usize,
    pub db_idx: u32,
    pub resp_version: u8,
    pub username: String,
    pub kind: ModuleContextKind,
    pub is_multi: bool,
    pub is_script: bool,
    pub deny_blocking: bool,
}

pub enum ModuleReplyData {
    Value {
        value: Value,
        exact: bool,
        is_resp3: bool,
    },
    Raw {
        proto: Bytes,
        exact: bool,
        is_resp3: bool,
    },
}

pub struct ModuleCallOutcome {
    pub reply: Option<ModuleReplyData>,
    pub call_errno: libc::c_int,
}

impl ModuleCallOutcome {
    pub fn cancelled(call_errno: libc::c_int) -> Self {
        Self {
            reply: None,
            call_errno,
        }
    }
}

pub(crate) struct OwnedModuleCall {
    pub context: ModuleThreadSnapshot,
    pub argv: Vec<Bytes>,
    pub format: ModuleCallFormat,
}

#[derive(Clone)]
pub struct ModuleThreadSnapshot {
    pub id: ModuleThreadContextId,
    pub module: Option<Arc<ModuleIdentity>>,
    pub db_idx: u32,
    pub resp_version: u8,
    pub username: String,
    pub auth_user: Option<String>,
}

impl ModuleThreadSnapshot {
    fn validate(&self) -> Result<(), libc::c_int> {
        if self
            .module
            .as_ref()
            .is_some_and(|module| !module.is_active.load(Ordering::Acquire))
        {
            return Err(libc::ENOENT);
        }
        Ok(())
    }
}

enum RpcState<T> {
    Waiting,
    Completed(T),
    Cancelled(libc::c_int),
    Taken,
}

pub struct RpcReply<T> {
    state: Arc<(Mutex<RpcState<T>>, Condvar)>,
}

impl<T> Clone for RpcReply<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Default for RpcReply<T> {
    fn default() -> Self {
        Self {
            state: Arc::new((Mutex::new(RpcState::Waiting), Condvar::new())),
        }
    }
}

impl<T> RpcReply<T> {
    pub fn complete(&self, value: T) {
        let mut state = lock(&self.state.0);
        if matches!(*state, RpcState::Waiting) {
            *state = RpcState::Completed(value);
            self.state.1.notify_all();
        }
    }

    pub fn cancel(&self, errno: libc::c_int) {
        let mut state = lock(&self.state.0);
        if matches!(*state, RpcState::Waiting) {
            *state = RpcState::Cancelled(errno);
            self.state.1.notify_all();
        }
    }

    pub fn wait(self) -> Result<T, libc::c_int> {
        let mut state = lock(&self.state.0);
        loop {
            if matches!(*state, RpcState::Waiting) {
                state = self
                    .state
                    .1
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
                continue;
            }
            match std::mem::replace(&mut *state, RpcState::Taken) {
                RpcState::Completed(value) => return Ok(value),
                RpcState::Cancelled(errno) => return Err(errno),
                RpcState::Waiting | RpcState::Taken => return Err(libc::EALREADY),
            }
        }
    }

    fn is_waiting(&self) -> bool {
        matches!(*lock(&self.state.0), RpcState::Waiting)
    }
}

pub(crate) enum ModuleThreadRequest {
    Call {
        lease: ModuleLease,
        request: OwnedModuleCall,
        reply: RpcReply<ModuleCallOutcome>,
    },
    ContextFlags {
        lease: ModuleLease,
        context: ModuleThreadSnapshot,
        reply: RpcReply<libc::c_int>,
    },
    SelectDb {
        lease: ModuleLease,
        db_idx: u32,
        reply: RpcReply<bool>,
    },
    Release {
        lease: ModuleLease,
        reply: RpcReply<()>,
    },
}

impl ModuleThreadRequest {
    fn lease(&self) -> ModuleLease {
        match self {
            Self::Call { lease, .. }
            | Self::ContextFlags { lease, .. }
            | Self::SelectDb { lease, .. }
            | Self::Release { lease, .. } => *lease,
        }
    }

    pub(crate) fn cancel(&self, errno: libc::c_int) {
        self.cancellation().cancel(errno);
    }

    fn cancellation(&self) -> RequestCancellation {
        match self {
            Self::Call { reply, .. } => RequestCancellation::Call(reply.clone()),
            Self::ContextFlags { reply, .. } => RequestCancellation::ContextFlags(reply.clone()),
            Self::SelectDb { reply, .. } => RequestCancellation::SelectDb(reply.clone()),
            Self::Release { reply, .. } => RequestCancellation::Release(reply.clone()),
        }
    }

    fn validate_context(&self) -> Result<(), libc::c_int> {
        match self {
            Self::Call { request, .. } => request.context.validate(),
            Self::ContextFlags { context, .. } => context.validate(),
            Self::SelectDb { .. } | Self::Release { .. } => Ok(()),
        }
    }

    fn is_release(&self) -> bool {
        matches!(self, Self::Release { .. })
    }
}

// The owner can hold a request outside the queue while shutdown cancels its
// response. Only owned reply slots cross this boundary, never an executor.
enum RequestCancellation {
    Call(RpcReply<ModuleCallOutcome>),
    ContextFlags(RpcReply<libc::c_int>),
    SelectDb(RpcReply<bool>),
    Release(RpcReply<()>),
}

impl RequestCancellation {
    fn cancel(&self, errno: libc::c_int) {
        match self {
            Self::Call(reply) => reply.cancel(errno),
            Self::ContextFlags(reply) => reply.cancel(errno),
            Self::SelectDb(reply) => reply.cancel(errno),
            Self::Release(reply) => reply.cancel(errno),
        }
    }

    fn is_waiting(&self) -> bool {
        match self {
            Self::Call(reply) => reply.is_waiting(),
            Self::ContextFlags(reply) => reply.is_waiting(),
            Self::SelectDb(reply) => reply.is_waiting(),
            Self::Release(reply) => reply.is_waiting(),
        }
    }
}

struct InFlightRequest {
    lease: ModuleLease,
    cancellation: RequestCancellation,
    is_release: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arbitration {
    MainExecuting,
    Available,
    Reserved(ModuleLease),
    Owned(ModuleLease),
    Closing,
}

struct Acquisition {
    lease: ModuleLease,
    reply: RpcReply<ModuleLease>,
}

struct BridgeState {
    arbitration: Arbitration,
    acquisitions: VecDeque<Acquisition>,
    requests: VecDeque<ModuleThreadRequest>,
    in_flight: Option<InFlightRequest>,
    is_releasing: bool,
    completions: VecDeque<ModuleBlockedId>,
}

impl BridgeState {
    fn retire_response(&mut self) {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|request| !request.cancellation.is_waiting())
        {
            self.in_flight = None;
        }
    }

    fn validate_lease(&self, lease: ModuleLease) -> Result<(), libc::c_int> {
        match self.arbitration {
            Arbitration::Closing => Err(libc::ESHUTDOWN),
            Arbitration::Reserved(current) | Arbitration::Owned(current) if current == lease => {
                Ok(())
            }
            _ => Err(libc::EPERM),
        }
    }

    fn has_ordinary_request(&self) -> bool {
        self.requests.iter().any(|request| !request.is_release())
            || self
                .in_flight
                .as_ref()
                .is_some_and(|request| !request.is_release)
    }
}

struct BridgeInner {
    id: u64,
    state: Mutex<BridgeState>,
    changed: Condvar,
    wake: Mutex<Option<TransportWakeHandle>>,
    unbound_contexts: AtomicUsize,
}

#[derive(Clone)]
pub struct ModuleThreadBridgeHandle(Arc<BridgeInner>);

impl ModuleThreadBridgeHandle {
    pub fn new() -> Result<Self, libc::c_int> {
        let mut requests = VecDeque::new();
        // Reserve both slots before exporting the bridge. Release cannot need
        // an allocation behind the owner's outstanding ordinary request.
        requests.try_reserve_exact(2).map_err(|_| libc::ENOMEM)?;
        Ok(Self(Arc::new(BridgeInner {
            id: next_id()?,
            state: Mutex::new(BridgeState {
                arbitration: Arbitration::MainExecuting,
                acquisitions: VecDeque::new(),
                requests,
                in_flight: None,
                is_releasing: false,
                completions: VecDeque::new(),
            }),
            changed: Condvar::new(),
            wake: Mutex::new(None),
            unbound_contexts: AtomicUsize::new(0),
        })))
    }

    pub fn install_waker(&self, wake: TransportWakeHandle) {
        *lock(&self.0.wake) = Some(wake);
        // Requests may have arrived before the runtime installed its wake source.
        if self.has_active_thread_work() || self.is_closing() {
            let _ = self.notify();
        }
    }

    fn notify(&self) -> Result<(), libc::c_int> {
        self.0.changed.notify_all();
        let wake = lock(&self.0.wake).clone();
        let result = wake.as_ref().map_or(Ok(()), |wake| wake.wake());
        if result.is_err() {
            self.close();
            return Err(libc::ESHUTDOWN);
        }
        Ok(())
    }

    fn owner(&self) -> Result<u64, libc::c_int> {
        THREAD_LEASES.with(|owners| {
            let mut owners = owners.borrow_mut();
            owners.retain(|_, owner| owner.bridge.strong_count() != 0);
            if let Some(owner) = owners.get(&self.0.id) {
                return Ok(owner.id);
            }
            let owner = next_id()?;
            owners.try_reserve(1).map_err(|_| libc::ENOMEM)?;
            owners.insert(
                self.0.id,
                ThreadOwner {
                    id: owner,
                    bridge: Arc::downgrade(&self.0),
                    lease: None,
                },
            );
            Ok(owner)
        })
    }

    fn remembered_lease(&self) -> Option<ModuleLease> {
        THREAD_LEASES.with(|owners| {
            owners
                .borrow()
                .get(&self.0.id)
                .and_then(|owner| owner.lease)
        })
    }

    pub fn current_lease(&self) -> Result<ModuleLease, libc::c_int> {
        let lease = self.remembered_lease();
        let state = lock(&self.0.state);
        if state.arbitration == Arbitration::Closing {
            self.remember_lease(None);
            return Err(libc::ESHUTDOWN);
        }
        let lease = lease.ok_or(libc::EPERM)?;
        if let Err(errno) = state.validate_lease(lease) {
            self.remember_lease(None);
            return Err(errno);
        }
        Ok(lease)
    }

    fn remember_lease(&self, lease: Option<ModuleLease>) {
        THREAD_LEASES.with(|owners| {
            if let Some(owner) = owners.borrow_mut().get_mut(&self.0.id) {
                owner.lease = lease;
            }
        });
    }

    pub fn acquire(&self) -> Result<(), libc::c_int> {
        match self.current_lease() {
            Ok(_) => return Err(libc::EDEADLK),
            Err(libc::EPERM) => {}
            Err(errno) => return Err(errno),
        }
        let lease = ModuleLease {
            owner: self.owner()?,
            id: next_id()?,
        };
        let reply = RpcReply::default();
        {
            let mut state = lock(&self.0.state);
            if state.arbitration == Arbitration::Closing {
                return Err(libc::ESHUTDOWN);
            }
            if state
                .acquisitions
                .iter()
                .any(|acquisition| acquisition.lease.owner == lease.owner)
            {
                return Err(libc::EDEADLK);
            }
            if state.acquisitions.len() == MAX_PENDING_ACQUISITIONS {
                return Err(libc::EAGAIN);
            }
            state
                .acquisitions
                .try_reserve(1)
                .map_err(|_| libc::ENOMEM)?;
            state.acquisitions.push_back(Acquisition {
                lease,
                reply: reply.clone(),
            });
        }
        self.notify()?;
        let lease = reply.wait()?;
        let state = lock(&self.0.state);
        state.validate_lease(lease)?;
        self.remember_lease(Some(lease));
        Ok(())
    }

    pub fn try_acquire(&self) -> Result<(), libc::c_int> {
        let mut state = match self.0.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return Err(libc::EBUSY),
        };
        if state.arbitration == Arbitration::Closing {
            self.remember_lease(None);
            return Err(libc::ESHUTDOWN);
        }
        if state.arbitration != Arbitration::Available || !state.acquisitions.is_empty() {
            return Err(libc::EBUSY);
        }
        state.retire_response();
        if state.in_flight.is_some() {
            return Err(libc::EBUSY);
        }
        let lease = ModuleLease {
            owner: self.owner()?,
            id: next_id()?,
        };
        state.arbitration = Arbitration::Reserved(lease);
        state.is_releasing = false;
        self.remember_lease(Some(lease));
        drop(state);
        // The wake handle is nonblocking; there is no owner acknowledgement here.
        if let Err(errno) = self.notify() {
            self.remember_lease(None);
            return Err(errno);
        }
        Ok(())
    }

    pub(crate) fn submit(&self, request: ModuleThreadRequest) -> Result<(), libc::c_int> {
        let mut state = lock(&self.0.state);
        state.retire_response();
        let lease = request.lease();
        let validation = state
            .validate_lease(lease)
            .and_then(|()| {
                if self.remembered_lease() == Some(lease) {
                    Ok(())
                } else {
                    Err(libc::EPERM)
                }
            })
            .and_then(|()| request.validate_context());
        if let Err(errno) = validation {
            request.cancel(errno);
            return Err(errno);
        }
        if state.is_releasing {
            request.cancel(libc::EPERM);
            return Err(libc::EPERM);
        }
        // One synchronous ordinary RPC plus an independent Release slot.
        // Taken requests remain counted until their response is terminal.
        if !request.is_release() && state.has_ordinary_request() {
            request.cancel(libc::EAGAIN);
            return Err(libc::EAGAIN);
        }
        if state.requests.try_reserve(1).is_err() {
            request.cancel(libc::ENOMEM);
            return Err(libc::ENOMEM);
        }
        state.is_releasing = request.is_release();
        state.requests.push_back(request);
        drop(state);
        self.notify()
    }

    pub fn release(&self) -> Result<(), libc::c_int> {
        let lease = self.current_lease()?;
        let reply = RpcReply::default();
        let result = self
            .submit(ModuleThreadRequest::Release {
                lease,
                reply: reply.clone(),
            })
            .and_then(|()| reply.wait());
        self.remember_lease(None);
        if result.is_err() {
            // A failed void Unlock must not strand an executable owner that
            // the calling thread can no longer release.
            let is_stranded = lock(&self.0.state).validate_lease(lease).is_ok();
            if is_stranded {
                self.close();
            }
        }
        result
    }

    /// Called only outside all synchronous module and hosted invocation scopes.
    pub fn take_lease(&self) -> Option<ModuleLease> {
        let mut state = lock(&self.0.state);
        state.retire_response();
        if state.in_flight.is_some() {
            return None;
        }
        match state.arbitration {
            Arbitration::Reserved(lease) => {
                state.arbitration = Arbitration::Owned(lease);
                Some(lease)
            }
            Arbitration::MainExecuting | Arbitration::Available => {
                let acquisition = state.acquisitions.pop_front()?;
                state.arbitration = Arbitration::Owned(acquisition.lease);
                state.is_releasing = false;
                acquisition.reply.complete(acquisition.lease);
                Some(acquisition.lease)
            }
            _ => None,
        }
    }

    pub(crate) fn wait_request(
        &self,
        lease: ModuleLease,
    ) -> Result<Option<ModuleThreadRequest>, libc::c_int> {
        let mut state = lock(&self.0.state);
        state.validate_lease(lease)?;
        if state.arbitration != Arbitration::Owned(lease) {
            return Err(libc::EPERM);
        }
        state.retire_response();
        if state.requests.is_empty() || state.in_flight.is_some() {
            let (next_state, _) = self
                .0
                .changed
                .wait_timeout(state, Duration::from_millis(100))
                .unwrap_or_else(|error| error.into_inner());
            state = next_state;
        }
        state.validate_lease(lease)?;
        if state.arbitration != Arbitration::Owned(lease) {
            return Err(libc::EPERM);
        }
        state.retire_response();
        if state.in_flight.is_some() {
            return Ok(None);
        }
        while let Some(request) = state.requests.pop_front() {
            if let Err(errno) = request.validate_context() {
                request.cancel(errno);
                continue;
            }
            let cancellation = request.cancellation();
            if !cancellation.is_waiting() {
                continue;
            }
            state.in_flight = Some(InFlightRequest {
                lease,
                cancellation,
                is_release: request.is_release(),
            });
            return Ok(Some(request));
        }
        Ok(None)
    }

    /// Recheck immediately before executing a taken request on the main thread.
    pub(crate) fn validate_request(
        &self,
        request: &ModuleThreadRequest,
    ) -> Result<(), libc::c_int> {
        let state = lock(&self.0.state);
        state.validate_lease(request.lease())?;
        if state.arbitration != Arbitration::Owned(request.lease())
            || !request.cancellation().is_waiting()
        {
            return Err(libc::ECANCELED);
        }
        request.validate_context()
    }

    pub fn finish_lease(&self, lease: ModuleLease) {
        let mut state = lock(&self.0.state);
        if state.arbitration == Arbitration::Owned(lease) {
            for request in state.requests.drain(..) {
                request.cancel(libc::ECANCELED);
            }
            if let Some(request) = &state.in_flight {
                if request.lease == lease && !request.is_release {
                    request.cancellation.cancel(libc::ECANCELED);
                }
            }
            state.retire_response();
            state.arbitration = Arbitration::MainExecuting;
            state.is_releasing = false;
            self.0.changed.notify_all();
        }
    }

    /// Publish the TryLock window only after BEFORE_SLEEP has returned.
    pub fn prepare_sleep(&self) -> bool {
        let mut state = lock(&self.0.state);
        state.retire_response();
        if state.arbitration != Arbitration::MainExecuting
            || !state.acquisitions.is_empty()
            || !state.completions.is_empty()
            || !state.requests.is_empty()
            || state.in_flight.is_some()
        {
            return false;
        }
        state.arbitration = Arbitration::Available;
        true
    }

    /// A successful reservation must be served before AFTER_SLEEP.
    pub fn resume_main(&self) -> bool {
        let mut state = lock(&self.0.state);
        match state.arbitration {
            Arbitration::Available | Arbitration::MainExecuting
                if state.acquisitions.is_empty() =>
            {
                state.arbitration = Arbitration::MainExecuting;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn accept_completion(
        &self,
        id: ModuleBlockedId,
        accept: impl FnOnce() -> Result<(), libc::c_int>,
    ) -> Result<(), libc::c_int> {
        let mut state = lock(&self.0.state);
        if state.arbitration == Arbitration::Closing {
            return Err(libc::ESHUTDOWN);
        }
        // Lifecycle traffic is not behind another thread's Acquire or Call.
        if state.completions.contains(&id) {
            return Err(libc::EINVAL);
        }
        if state.completions.len() == MAX_PENDING_COMPLETIONS {
            return Err(libc::EAGAIN);
        }
        state.completions.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        // This closure only seals owned state. It must not call external C,
        // wait for RPC, or reenter the bridge while holding its mutex.
        accept()?;
        state.completions.push_back(id);
        drop(state);
        // Accepted private data remains owned by the main-thread record even
        // if waking fails and closes the bridge.
        let _ = self.notify();
        Ok(())
    }

    pub fn take_completions(&self) -> Vec<ModuleBlockedId> {
        lock(&self.0.state).completions.drain(..).collect()
    }

    pub fn is_closing(&self) -> bool {
        lock(&self.0.state).arbitration == Arbitration::Closing
    }

    /// Conservatively reject unload while any thread owns or awaits execution.
    pub fn has_active_thread_work(&self) -> bool {
        let mut state = lock(&self.0.state);
        state.retire_response();
        matches!(
            state.arbitration,
            Arbitration::Reserved(_) | Arbitration::Owned(_)
        ) || !state.acquisitions.is_empty()
            || !state.requests.is_empty()
            || state.in_flight.is_some()
            || !state.completions.is_empty()
    }

    /// The native caller of a NULL-context export cannot be identified.
    /// Keep this fact even after every context and bridge has been dropped.
    pub fn has_unbound_thread_exports(&self) -> bool {
        UNBOUND_THREAD_EXPORTS.load(Ordering::Acquire)
    }

    pub fn has_unbound_thread_contexts(&self) -> bool {
        self.0.unbound_contexts.load(Ordering::Acquire) != 0
    }

    pub fn close(&self) {
        let mut state = lock(&self.0.state);
        state.arbitration = Arbitration::Closing;
        for acquisition in state.acquisitions.drain(..) {
            acquisition.reply.cancel(libc::ESHUTDOWN);
        }
        for request in state.requests.drain(..) {
            request.cancel(libc::ESHUTDOWN);
        }
        if let Some(request) = state.in_flight.take() {
            request.cancellation.cancel(libc::ESHUTDOWN);
        }
        // Accepted lifecycle IDs remain drainable after Closing; the main
        // thread still owns their private data and must run cleanup there.
        drop(state);
        self.0.changed.notify_all();
        let wake = lock(&self.0.wake).clone();
        if let Some(wake) = wake {
            let _ = wake.wake();
        }
    }
}

struct ThreadOwner {
    id: u64,
    bridge: Weak<BridgeInner>,
    lease: Option<ModuleLease>,
}

thread_local! {
    static THREAD_LEASES: RefCell<HashMap<u64, ThreadOwner>> = RefCell::new(HashMap::new());
}

static PROCESS_BRIDGES: OnceLock<Mutex<Vec<Weak<BridgeInner>>>> = OnceLock::new();

pub fn register_process_bridge(bridge: &ModuleThreadBridgeHandle) {
    let mut bridges = lock(PROCESS_BRIDGES.get_or_init(|| Mutex::new(Vec::new())));
    bridges.retain(|bridge| bridge.strong_count() != 0);
    let weak = Arc::downgrade(&bridge.0);
    if bridges.iter().any(|registered| registered.ptr_eq(&weak)) {
        return;
    }
    bridges.push(weak);
}

pub(crate) fn process_bridge() -> Option<ModuleThreadBridgeHandle> {
    let bridges = {
        let mut bridges = lock(PROCESS_BRIDGES.get_or_init(|| Mutex::new(Vec::new())));
        bridges.retain(|bridge| bridge.strong_count() != 0);
        bridges.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
    };
    let mut live = bridges
        .into_iter()
        .map(ModuleThreadBridgeHandle)
        .filter(|bridge| !bridge.is_closing());
    let bridge = live.next()?;
    // An unbound C context cannot choose between independent server owners.
    live.next().is_none().then_some(bridge)
}

pub struct ModuleThreadContext {
    pub bridge: ModuleThreadBridgeHandle,
    pub snapshot: ModuleThreadSnapshot,
    pub blocked: Option<Arc<super::blocking::ModuleBlockedShared>>,
    pub cancelled_errno: libc::c_int,
    id: ModuleThreadContextId,
    bridge_id: u64,
    registration: ContextRegistration,
}

enum ContextRegistration {
    Bound(Arc<ModuleIdentity>),
    Unbound(ModuleThreadBridgeHandle),
}

impl ModuleThreadContext {
    pub fn new(
        bridge: ModuleThreadBridgeHandle,
        module: Option<Arc<ModuleIdentity>>,
        db_idx: u32,
        resp_version: u8,
        username: String,
        blocked: Option<Arc<super::blocking::ModuleBlockedShared>>,
    ) -> Result<Self, libc::c_int> {
        let id = ModuleThreadContextId(next_id()?);
        let bridge_id = bridge.0.id;
        let state = lock(&bridge.0.state);
        if state.arbitration == Arbitration::Closing {
            return Err(libc::ESHUTDOWN);
        }
        if let Some(blocked) = &blocked {
            if !Arc::ptr_eq(&bridge.0, &blocked.bridge.0)
                || !module
                    .as_ref()
                    .is_some_and(|module| module.id == blocked.module.id)
            {
                return Err(libc::EINVAL);
            }
        }
        let registration = if let Some(module) = &module {
            if !module.is_active.load(Ordering::Acquire) {
                return Err(libc::ENOENT);
            }
            module.exported_threads.store(true, Ordering::Release);
            module
                .contexts
                .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .map_err(|_| libc::EOVERFLOW)?;
            if !module.is_active.load(Ordering::Acquire) {
                module.contexts.fetch_sub(1, Ordering::AcqRel);
                return Err(libc::ENOENT);
            }
            ContextRegistration::Bound(module.clone())
        } else {
            bridge
                .0
                .unbound_contexts
                .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .map_err(|_| libc::EOVERFLOW)?;
            UNBOUND_THREAD_EXPORTS.store(true, Ordering::Release);
            ContextRegistration::Unbound(bridge.clone())
        };
        drop(state);
        Ok(Self {
            bridge,
            snapshot: ModuleThreadSnapshot {
                id,
                module,
                db_idx,
                resp_version,
                username,
                auth_user: None,
            },
            blocked,
            cancelled_errno: 0,
            id,
            bridge_id,
            registration,
        })
    }

    pub fn lease(&self) -> Result<ModuleLease, libc::c_int> {
        self.validate_identity()?;
        if self.cancelled_errno != 0 {
            return Err(self.cancelled_errno);
        }
        self.bridge.current_lease()
    }

    pub(crate) fn validate_identity(&self) -> Result<(), libc::c_int> {
        if self.bridge.is_closing() {
            return Err(libc::ESHUTDOWN);
        }
        // DB and authentication fields may change; the registered identity may
        // not be replaced by a same-name module or a different context.
        let is_registered = self.bridge.0.id == self.bridge_id
            && self.snapshot.id == self.id
            && match &self.registration {
                ContextRegistration::Bound(module) => self
                    .snapshot
                    .module
                    .as_ref()
                    .is_some_and(|snapshot| module.id == snapshot.id),
                ContextRegistration::Unbound(_) => self.snapshot.module.is_none(),
            };
        if !is_registered {
            return Err(libc::ENOENT);
        }
        self.snapshot.validate()?;
        Ok(())
    }
}

impl Drop for ModuleThreadContext {
    fn drop(&mut self) {
        match &self.registration {
            ContextRegistration::Bound(module) => {
                module.contexts.fetch_sub(1, Ordering::AcqRel);
            }
            ContextRegistration::Unbound(bridge) => {
                bridge.0.unbound_contexts.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

#[cfg(test)]
#[path = "thread_bridge_tests.rs"]
mod thread_bridge_tests;
