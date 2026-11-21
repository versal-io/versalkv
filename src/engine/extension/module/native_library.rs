//! Explicit process-lifetime ownership of libraries that exported native threads.

use libc::c_void;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

struct PinnedNativeLibrary {
    handle: *mut c_void,
}

// SAFETY: this owner only preserves a dlopen reference. No native data is
// dereferenced, no callback is invoked, and no thread may close its handle.
unsafe impl Send for PinnedNativeLibrary {}

impl Drop for PinnedNativeLibrary {
    fn drop(&mut self) {
        // A last Rust context does not prove a native thread has returned.
        // The operating system, not dlclose, reclaims this reference at exit.
        let _ = self.handle;
    }
}

#[derive(Default)]
struct NativeLibraryState {
    thread_api_exports: HashSet<usize>,
    pinned: Vec<PinnedNativeLibrary>,
}

static NATIVE_LIBRARIES: OnceLock<Mutex<NativeLibraryState>> = OnceLock::new();

/// Called before a thread context API address escapes a protected callback.
/// No reference is transferred until the loader eventually retires the library.
pub(crate) fn record_thread_api_export(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    super::thread_bridge::lock(
        NATIVE_LIBRARIES.get_or_init(|| Mutex::new(NativeLibraryState::default())),
    )
    .thread_api_exports
    .insert(handle as usize);
}

/// # Safety
/// handle owns one live dlopen reference and must be retired exactly once.
// SAFETY: the caller transfers a valid dlopen reference after synchronous callbacks end.
pub(crate) unsafe fn retire_native_library(handle: *mut c_void, should_pin: bool) {
    if handle.is_null() {
        return;
    }
    let is_pinned = {
        let mut libraries = super::thread_bridge::lock(
            NATIVE_LIBRARIES.get_or_init(|| Mutex::new(NativeLibraryState::default())),
        );
        let has_thread_api_export = libraries.thread_api_exports.remove(&(handle as usize));
        if should_pin || has_thread_api_export {
            libraries.pinned.push(PinnedNativeLibrary { handle });
            true
        } else {
            false
        }
    };
    if !is_pinned {
        // SAFETY: the caller transferred its live unexported dlopen reference.
        unsafe { libc::dlclose(handle) };
    }
}

#[cfg(all(test, unix))]
#[path = "native_library_tests.rs"]
mod native_library_tests;
