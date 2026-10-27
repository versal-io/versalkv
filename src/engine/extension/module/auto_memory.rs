use std::ptr::NonNull;

use super::api::call::{release_call_reply, ValkeyModuleCallReply};
use super::{ModuleInternalCtx, ModuleKeyHandle, ValkeyModuleCtx, ValkeyModuleString};

#[derive(Clone, Copy, PartialEq, Eq)]
enum AutoMemoryEntry {
    String(NonNull<ValkeyModuleString>),
    Key(NonNull<ModuleKeyHandle>),
    Reply(NonNull<ValkeyModuleCallReply>),
}

#[derive(Default)]
pub(super) struct ModuleAutoMemoryScope {
    pub(super) is_enabled: bool,
    entries: Vec<AutoMemoryEntry>,
}

// SAFETY: ctx is null or a live module context.
pub(super) unsafe fn internal_owner(
    ctx: *mut ValkeyModuleCtx,
) -> Option<NonNull<ModuleInternalCtx>> {
    // SAFETY: the caller guarantees a live context if non-null.
    unsafe { ctx.as_ref().and_then(|ctx| NonNull::new(ctx.internal)) }
}

// SAFETY: owner, if present, points to a live context.
pub(super) unsafe fn is_enabled(owner: Option<NonNull<ModuleInternalCtx>>) -> bool {
    // SAFETY: the optional pointer is live by the caller contract.
    unsafe { owner.is_some_and(|owner| owner.as_ref().auto_memory.is_enabled) }
}

// SAFETY: owner is live, and entry transfers one owned reference to its scope.
unsafe fn register(owner: NonNull<ModuleInternalCtx>, entry: AutoMemoryEntry) {
    // SAFETY: this short borrow ends before any object can be released.
    unsafe { (*owner.as_ptr()).auto_memory.entries.push(entry) };
}

// SAFETY: owner points to a live context.
unsafe fn remove(owner: NonNull<ModuleInternalCtx>, entry: AutoMemoryEntry) -> bool {
    // SAFETY: the queue is only borrowed while removing an entry, never across release.
    unsafe {
        let entries = &mut (*owner.as_ptr()).auto_memory.entries;
        if let Some(index) = entries.iter().rposition(|candidate| *candidate == entry) {
            entries.swap_remove(index);
            true
        } else {
            false
        }
    }
}

// SAFETY: ctx is null or live; the returned allocation owns one reference.
pub(super) unsafe fn create_auto_string(
    ctx: *mut ValkeyModuleCtx,
    bytes: &[u8],
) -> *mut ValkeyModuleString {
    // SAFETY: explicit context lookup never falls back to TLS.
    unsafe { create_string_for_owner(internal_owner(ctx), bytes) }
}

// SAFETY: owner, if present, is live.
pub(super) unsafe fn create_string_for_owner(
    owner: Option<NonNull<ModuleInternalCtx>>,
    bytes: &[u8],
) -> *mut ValkeyModuleString {
    let string = Box::into_raw(ValkeyModuleString::from_bytes(bytes));
    // SAFETY: string is freshly allocated and owner is live.
    unsafe {
        if is_enabled(owner) {
            if let Some(owner) = owner {
                ValkeyModuleString::set_auto_owner(string, Some(owner));
                register(
                    owner,
                    AutoMemoryEntry::String(NonNull::new_unchecked(string)),
                );
            }
        }
    }
    string
}

// SAFETY: string and owner, if present, are live.
pub(super) unsafe fn remove_string(
    owner: Option<NonNull<ModuleInternalCtx>>,
    string: *mut ValkeyModuleString,
) -> bool {
    let (Some(owner), Some(string_ptr)) = (owner, NonNull::new(string)) else {
        return false;
    };
    // SAFETY: both pointers are live; removing a queue entry does not release it.
    unsafe {
        if remove(owner, AutoMemoryEntry::String(string_ptr)) {
            ValkeyModuleString::set_auto_owner(string, None);
            true
        } else {
            false
        }
    }
}

// SAFETY: key is freshly allocated, and owner is live.
pub(super) unsafe fn register_key(key: *mut ModuleKeyHandle) {
    // SAFETY: key points to a live handle whose original context owns its scope.
    unsafe {
        let owner = (*key).owner;
        if is_enabled(owner) {
            if let Some(owner) = owner {
                register(owner, AutoMemoryEntry::Key(NonNull::new_unchecked(key)));
            }
        }
    }
}

// SAFETY: key is null or a live handle with a live owner.
pub(super) unsafe fn release_key(key: *mut ModuleKeyHandle) {
    if key.is_null() {
        return;
    }
    // SAFETY: cancel the entry before freeing the allocation.
    unsafe {
        if let Some(owner) = (*key).owner.take() {
            remove(owner, AutoMemoryEntry::Key(NonNull::new_unchecked(key)));
        }
        drop(Box::from_raw(key));
    }
}

// SAFETY: reply is a live root and owner is a live enabled context.
pub(super) unsafe fn register_reply(
    owner: NonNull<ModuleInternalCtx>,
    reply: *mut ValkeyModuleCallReply,
) {
    // SAFETY: reply is a non-null freshly built root.
    unsafe { register(owner, AutoMemoryEntry::Reply(NonNull::new_unchecked(reply))) };
}

// SAFETY: reply is a live root; its owner is live if present.
pub(super) unsafe fn remove_reply(reply: *mut ValkeyModuleCallReply) {
    // SAFETY: take the reverse owner before releasing any node in the tree.
    unsafe {
        if let Some(owner) = (*reply).auto_owner.take() {
            remove(owner, AutoMemoryEntry::Reply(NonNull::new_unchecked(reply)));
        }
    }
}

// SAFETY: internal is null or a live context; no queue borrow may cross a release.
pub(super) unsafe fn collect(internal: *mut ModuleInternalCtx) {
    if internal.is_null() {
        return;
    }
    // SAFETY: collection temporarily disables registration in this live scope.
    let was_enabled = unsafe { std::mem::replace(&mut (*internal).auto_memory.is_enabled, false) };
    loop {
        // SAFETY: this borrow ends before releasing the popped object.
        let entry = unsafe { (*internal).auto_memory.entries.pop() };
        let Some(entry) = entry else { break };
        // SAFETY: each entry owns one live reference; clear reverse ownership first.
        unsafe {
            match entry {
                AutoMemoryEntry::String(string) => {
                    ValkeyModuleString::set_auto_owner(string.as_ptr(), None);
                    ValkeyModuleString::release(string.as_ptr());
                }
                AutoMemoryEntry::Key(key) => {
                    (*key.as_ptr()).owner = None;
                    release_key(key.as_ptr());
                }
                AutoMemoryEntry::Reply(reply) => {
                    (*reply.as_ptr()).auto_owner = None;
                    release_call_reply(reply.as_ptr());
                }
            }
        }
    }
    // SAFETY: the queue is now empty and no release remains in flight.
    unsafe {
        (*internal).auto_memory.entries = Vec::new();
        (*internal).auto_memory.is_enabled = was_enabled;
    }
}

#[cfg(test)]
pub(super) unsafe fn entry_count(internal: *mut ModuleInternalCtx) -> usize {
    // SAFETY: test callers provide a live context.
    unsafe { (*internal).auto_memory.entries.len() }
}
