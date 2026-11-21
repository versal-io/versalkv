//! Native BlockClient ownership, independent of asynchronous RM_Call clients.

use libc::c_void;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use super::api::ReplyBuilder;
use super::thread_bridge::{
    lock, ModuleBlockOrigin, ModuleBlockedId, ModuleIdentity, ModuleThreadBridgeHandle,
};
use super::{ValkeyModuleBlockedClient, ValkeyModuleCmdFunc, ValkeyModuleCtx};

pub type ModuleFreePrivateData = Option<extern "C" fn(*mut ValkeyModuleCtx, *mut c_void)>;
pub type ModuleDisconnectCallback =
    Option<extern "C" fn(*mut ValkeyModuleCtx, *mut ValkeyModuleBlockedClient)>;

#[derive(Clone, Copy)]
pub struct ModuleBlockCallbacks {
    pub reply: ValkeyModuleCmdFunc,
    pub timeout: ValkeyModuleCmdFunc,
    pub free: ModuleFreePrivateData,
    pub disconnect: ModuleDisconnectCallback,
}

/// A successful Unblock transfers the allocation to the main-thread callback.
/// This wrapper never dereferences the pointer and never runs a C destructor.
pub struct ModulePrivateData(pub *mut c_void);

// SAFETY: the module transfers private data on successful UnblockClient and
// keeps its allocation valid until the main-thread free callback returns.
// This wrapper only transports ownership; Rust never dereferences the pointer.
unsafe impl Send for ModulePrivateData {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModuleBlockStage {
    Publishing,
    Armed,
    DisconnectPending,
    DetachedTimeout,
    DetachedDisconnect,
    Completed,
}

pub struct ModuleBlockedLifecycle {
    pub stage: ModuleBlockStage,
    pub completion_queued: bool,
    pub private_data: ModulePrivateData,
}

pub struct ModuleBlockedReplies {
    pub builder: ReplyBuilder,
    pub is_sealed: bool,
}

pub struct ModuleBlockedShared {
    pub id: ModuleBlockedId,
    pub module: Arc<ModuleIdentity>,
    pub bridge: ModuleThreadBridgeHandle,
    pub origin: ModuleBlockOrigin,
    pub replies: Mutex<ModuleBlockedReplies>,
    pub lifecycle: Mutex<ModuleBlockedLifecycle>,
}

impl ModuleBlockedShared {
    pub fn new(
        id: ModuleBlockedId,
        module: Arc<ModuleIdentity>,
        bridge: ModuleThreadBridgeHandle,
        origin: ModuleBlockOrigin,
    ) -> Result<Arc<Self>, libc::c_int> {
        module.exported_threads.store(true, Ordering::Release);
        module
            .blocked_handles
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| libc::EOVERFLOW)?;
        Ok(Arc::new(Self {
            id,
            module,
            bridge,
            origin,
            replies: Mutex::new(ModuleBlockedReplies {
                builder: ReplyBuilder::new(),
                is_sealed: false,
            }),
            lifecycle: Mutex::new(ModuleBlockedLifecycle {
                stage: ModuleBlockStage::Publishing,
                completion_queued: false,
                private_data: ModulePrivateData(std::ptr::null_mut()),
            }),
        }))
    }

    pub fn accept_unblock(&self, data: *mut c_void) -> Result<(), libc::c_int> {
        self.bridge.accept_completion(self.id, || {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.completion_queued || lifecycle.stage == ModuleBlockStage::Completed {
                return Err(libc::EINVAL);
            }
            let mut replies = lock(&self.replies);
            replies.is_sealed = true;
            lifecycle.private_data = ModulePrivateData(data);
            lifecycle.completion_queued = true;
            Ok(())
        })
    }

    pub fn with_replies<R>(&self, callback: impl FnOnce(&mut ReplyBuilder) -> R) -> Option<R> {
        let mut replies = lock(&self.replies);
        if replies.is_sealed {
            return None;
        }
        Some(callback(&mut replies.builder))
    }

    /// # Safety
    /// The returned pointer owns one C reference until successful UnblockClient.
    // SAFETY: the exported Arc reference is transferred to the native caller.
    pub unsafe fn export(shared: Arc<Self>) -> *mut ValkeyModuleBlockedClient {
        Arc::into_raw(shared).cast_mut().cast()
    }

    /// # Safety
    /// handle is a live exported C reference, not already consumed by Unblock.
    // SAFETY: the caller retains the live export while its Arc count is increased.
    pub unsafe fn clone_export(handle: *mut ValkeyModuleBlockedClient) -> Option<Arc<Self>> {
        if handle.is_null() {
            return None;
        }
        let ptr = handle.cast::<Self>();
        // SAFETY: the caller holds the exported strong reference throughout.
        unsafe {
            Arc::increment_strong_count(ptr);
            Some(Arc::from_raw(ptr))
        }
    }

    /// # Safety
    /// handle owns the single C reference and Unblock was accepted exactly once.
    // SAFETY: successful Unblock transfers exactly one exported Arc reference.
    pub unsafe fn consume_export(handle: *mut ValkeyModuleBlockedClient) {
        // SAFETY: successful Unblock transfers and consumes the exported reference.
        unsafe { drop(Arc::from_raw(handle.cast::<Self>())) };
    }
}

impl Drop for ModuleBlockedShared {
    fn drop(&mut self) {
        self.module.blocked_handles.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct NativeModuleBlock {
    pub shared: Arc<ModuleBlockedShared>,
    pub callbacks: ModuleBlockCallbacks,
    pub active_time_usec: u64,
}

#[cfg(test)]
#[path = "blocking_tests.rs"]
mod blocking_tests;
