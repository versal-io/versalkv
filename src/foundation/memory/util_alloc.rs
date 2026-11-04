pub use super::external::ExternalMemoryError;
use super::external::MODULE_EXTERNAL_MEMORY;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    static THREAD_ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

#[cfg(target_os = "macos")]
extern "C" {
    fn malloc_size(ptr: *const std::ffi::c_void) -> usize;
}

#[cfg(target_os = "linux")]
extern "C" {
    fn malloc_usable_size(ptr: *mut std::ffi::c_void) -> usize;
}

#[inline]
fn malloc_real_size(ptr: *mut u8, layout: Layout) -> usize {
    if layout.size() == 0 {
        return 0;
    }
    #[cfg(target_os = "macos")]
    // SAFETY: malloc_size is a macOS libc function that reads the allocation size; ptr comes from a valid allocator allocation.
    unsafe {
        malloc_size(ptr as *const std::ffi::c_void)
    }
    #[cfg(target_os = "linux")]
    // SAFETY: malloc_usable_size is a Linux libc function that reads the allocation size; ptr comes from a valid allocator allocation.
    unsafe {
        malloc_usable_size(ptr as *mut std::ffi::c_void)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    layout.size()
}

pub struct LibcAllocator;

// SAFETY: LibcAllocator delegates raw memory operations to System, preserving
// the complete Layout contract, and only adds allocation-size accounting.
unsafe impl GlobalAlloc for LibcAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == 0 {
            return std::ptr::NonNull::dangling().as_ptr();
        }
        // SAFETY: System accepts this non-zero valid Layout and returns a block
        // satisfying its size and alignment requirements.
        let ptr = unsafe { System.alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        let real = malloc_real_size(ptr, layout);
        if real > 0 {
            let prev = ALLOCATED.fetch_add(real, Ordering::Relaxed);
            increase_thread_allocated(real);
            update_peak(prev + real);
        }
        ptr
    }

    // SAFETY: System preserves the requested alignment and initializes the
    // complete allocation to zero.
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() == 0 {
            return std::ptr::NonNull::dangling().as_ptr();
        }
        // SAFETY: System accepts this non-zero valid Layout and returns a
        // zero-initialized block satisfying its size and alignment requirements.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        let real = malloc_real_size(ptr, layout);
        if real > 0 {
            let prev = ALLOCATED.fetch_add(real, Ordering::Relaxed);
            increase_thread_allocated(real);
            update_peak(prev + real);
        }
        ptr
    }

    // SAFETY: ptr and layout came from a previous allocation by this allocator.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.size() == 0 {
            return;
        }
        let real = malloc_real_size(ptr, layout);
        if real > 0 {
            ALLOCATED.fetch_sub(real, Ordering::Relaxed);
            decrease_thread_allocated(real);
        }
        // SAFETY: ptr was allocated by System with this exact layout and has not
        // been deallocated yet, as required by the GlobalAlloc caller contract.
        unsafe { System.dealloc(ptr, layout) };
    }

    // SAFETY: ptr and layout came from a previous allocation by this allocator;
    // System preserves layout alignment while growing or shrinking the block.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size == 0 {
            let old_real = malloc_real_size(ptr, layout);
            if old_real > 0 {
                ALLOCATED.fetch_sub(old_real, Ordering::Relaxed);
                decrease_thread_allocated(old_real);
            }
            // SAFETY: ptr was allocated by System with this exact layout and is
            // consumed here instead of being passed to realloc with size zero.
            unsafe { System.dealloc(ptr, layout) };
            return std::ptr::NonNull::dangling().as_ptr();
        }
        let old_real = malloc_real_size(ptr, layout);
        let new_layout =
            Layout::from_size_align(new_size, layout.align()).expect("valid realloc layout");
        // SAFETY: ptr is live and was allocated by System with layout; new_size
        // is non-zero, and System preserves the original alignment.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if new_ptr.is_null() {
            std::alloc::handle_alloc_error(new_layout);
        }
        let new_real = malloc_real_size(new_ptr, new_layout);
        let diff = (new_real as isize) - (old_real as isize);
        if diff > 0 {
            let prev = ALLOCATED.fetch_add(diff as usize, Ordering::Relaxed);
            increase_thread_allocated(diff as usize);
            update_peak(prev + diff as usize);
        } else if diff < 0 {
            ALLOCATED.fetch_sub((-diff) as usize, Ordering::Relaxed);
            decrease_thread_allocated((-diff) as usize);
        }
        new_ptr
    }
}

// Miri cannot model libc's platform-specific malloc_usable_size. Its standard
// allocator preserves allocation semantics while allowing UB checks to run.
#[cfg(miri)]
#[global_allocator]
static GLOBAL: std::alloc::System = std::alloc::System;

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: LibcAllocator = LibcAllocator;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

static PEAK: AtomicUsize = AtomicUsize::new(0);

#[inline]
fn increase_thread_allocated(bytes: usize) {
    THREAD_ALLOCATED.with(|allocated| {
        allocated.set(allocated.get().saturating_add(bytes));
    });
}

#[inline]
fn decrease_thread_allocated(bytes: usize) {
    THREAD_ALLOCATED.with(|allocated| {
        allocated.set(allocated.get().saturating_sub(bytes));
    });
}

#[inline]
fn update_peak(candidate: usize) {
    PEAK.fetch_max(
        candidate.saturating_add(external_allocated()),
        Ordering::Relaxed,
    );
}

#[inline]
pub fn total_allocated() -> usize {
    ALLOCATED
        .load(Ordering::Relaxed)
        .saturating_add(external_allocated())
}

#[inline]
pub fn current_thread_allocated() -> usize {
    THREAD_ALLOCATED.with(Cell::get)
}

/// Return reported resident bytes not owned by the server allocator.
pub fn external_allocated() -> usize {
    MODULE_EXTERNAL_MEMORY.used()
}

/// Account for external resident memory without allocating or touching it.
pub fn increase_external_memory(bytes: usize) -> Result<(), ExternalMemoryError> {
    let external = MODULE_EXTERNAL_MEMORY.increase(bytes)?;
    PEAK.fetch_max(
        ALLOCATED.load(Ordering::Relaxed).saturating_add(external),
        Ordering::Relaxed,
    );
    Ok(())
}

/// Remove external accounting without freeing or touching the reported memory.
pub fn decrease_external_memory(bytes: usize) -> Result<(), ExternalMemoryError> {
    MODULE_EXTERNAL_MEMORY.decrease(bytes)?;
    Ok(())
}

/// Return the allocator-owned usable bytes for a live Vec backing allocation.
/// This includes allocator size-class slack, matching `total_allocated()`.
pub fn vec_allocation_bytes<T>(value: &Vec<T>) -> usize {
    if value.capacity() == 0 || std::mem::size_of::<T>() == 0 {
        return 0;
    }
    let layout = Layout::array::<T>(value.capacity()).expect("live Vec capacity has valid layout");
    #[cfg(miri)]
    {
        layout.size()
    }
    #[cfg(not(miri))]
    {
        malloc_real_size(value.as_ptr().cast_mut().cast::<u8>(), layout)
    }
}

#[inline]
pub fn peak_allocated() -> usize {
    PEAK.load(Ordering::Relaxed)
}

pub struct AllocatorStats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub muzzy: usize,
}

pub fn allocator_stats() -> AllocatorStats {
    let allocated = ALLOCATED.load(Ordering::Relaxed);
    let rss_val = rss();
    AllocatorStats {
        allocated,
        active: rss_val,
        resident: rss_val,
        muzzy: 0,
    }
}

pub fn rss() -> usize {
    #[cfg(target_os = "macos")]
    {
        #[allow(deprecated)]
        rss_macos()
    }
    #[cfg(target_os = "linux")]
    {
        rss_linux()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn rss_macos() -> usize {
    #[repr(C)]
    struct TaskBasicInfo {
        suspend_count: i32,
        virtual_size: u64,
        resident_size: u64,
        user_time: u64,
        system_time: u64,
        policy: i32,
    }
    const TASK_BASIC_INFO: u32 = 3; // MACH_TASK_BASIC_INFO
    const KERN_SUCCESS: i32 = 0;

    // SAFETY: mach_task_self() and task_info() are macOS libc syscalls operating on
    // a locally-owned stack-allocated TaskBasicInfo struct; all pointers are valid.
    unsafe {
        let task = libc::mach_task_self();
        let mut info = std::mem::zeroed::<TaskBasicInfo>();
        let mut count = 1u32; // sizeof(task_basic_info_data_t) / sizeof(natural_t)
        let ret = libc::task_info(
            task,
            TASK_BASIC_INFO,
            &mut info as *mut _ as libc::task_info_t,
            &mut count as *mut u32,
        );
        if ret == KERN_SUCCESS {
            info.resident_size as usize
        } else {
            // SAFETY: zeroed() is safe for POD types like rusage; getrusage is a
            // pure POSIX query that writes to the provided struct.
            let mut usage: libc::rusage = std::mem::zeroed();
            // SAFETY: getrusage is a pure POSIX query that writes to the provided
            // struct. RUSAGE_SELF is a valid constant. The call cannot fail with
            // invalid pointers since usage is a stack-local variable.
            if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
                usage.ru_maxrss as usize // macOS  ru_maxrss
            } else {
                0
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn rss_linux() -> usize {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some(pos) = stat.rfind(')') else {
        return 0;
    };
    let rest = &stat[pos + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() <= 21 {
        return 0;
    }
    let pages: i64 = fields[21].parse().unwrap_or(0);
    // SAFETY: sysconf() with _SC_PAGESIZE is a read-only POSIX syscall that always succeeds or returns -1.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (pages.max(0) as usize) * (page_size as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_allocations_aligned(
        layout: Layout,
        allocate: impl Fn(&LibcAllocator, Layout) -> *mut u8,
    ) {
        let allocator = LibcAllocator;
        let pointers: Vec<*mut u8> = (0..256)
            .map(|_| {
                let ptr = allocate(&allocator, layout);
                assert!(!ptr.is_null());
                ptr
            })
            .collect();

        for &ptr in &pointers {
            assert_eq!(
                ptr.addr() % layout.align(),
                0,
                "pointer {ptr:p} does not satisfy {}-byte alignment",
                layout.align()
            );
        }

        for ptr in pointers {
            // SAFETY: every pointer was returned by allocator.alloc for this
            // layout above and has not been deallocated.
            unsafe { allocator.dealloc(ptr, layout) };
        }
    }

    #[test]
    fn allocations_respect_overaligned_layouts() {
        for align in [32, 64, 128] {
            let layout = Layout::from_size_align(384, align).unwrap();
            // SAFETY: layout has non-zero size and valid alignment; the helper
            // verifies the returned pointer and deallocates it with this layout.
            assert_allocations_aligned(layout, |allocator, layout| unsafe {
                allocator.alloc(layout)
            });
        }
    }

    #[test]
    fn zeroed_allocations_respect_overaligned_layouts() {
        let allocator = LibcAllocator;
        for align in [32, 64, 128] {
            let layout = Layout::from_size_align(384, align).unwrap();
            // SAFETY: layout has non-zero size and valid alignment.
            let ptr = unsafe { allocator.alloc_zeroed(layout) };
            assert!(!ptr.is_null());
            assert_eq!(ptr.addr() % align, 0);
            // SAFETY: ptr is a live allocation of layout.size() bytes.
            let bytes = unsafe { std::slice::from_raw_parts(ptr, layout.size()) };
            assert!(bytes.iter().all(|&byte| byte == 0));
            // SAFETY: ptr was allocated above with this exact layout and is live.
            unsafe { allocator.dealloc(ptr, layout) };
        }
    }

    #[test]
    fn realloc_preserves_overaligned_layout() {
        let allocator = LibcAllocator;
        for align in [32, 64, 128] {
            let layout = Layout::from_size_align(384, align).unwrap();
            // SAFETY: layout has non-zero size and valid alignment.
            let ptr = unsafe { allocator.alloc(layout) };
            assert!(!ptr.is_null());

            // SAFETY: ptr is live and was allocated with layout; 768 is non-zero.
            let grown = unsafe { allocator.realloc(ptr, layout, 768) };
            assert!(!grown.is_null());
            assert_eq!(grown.addr() % align, 0);

            let grown_layout = Layout::from_size_align(768, align).unwrap();
            // SAFETY: grown is live and was allocated with grown_layout; 192 is
            // non-zero.
            let shrunk = unsafe { allocator.realloc(grown, grown_layout, 192) };
            assert!(!shrunk.is_null());
            assert_eq!(shrunk.addr() % align, 0);

            let shrunk_layout = Layout::from_size_align(192, align).unwrap();
            // SAFETY: shrunk is live and was allocated with shrunk_layout.
            unsafe { allocator.dealloc(shrunk, shrunk_layout) };
        }
    }

    #[test]
    fn total_allocated_is_positive() {
        let current = total_allocated();
        assert!(current > 0);
        let v: Vec<u8> = vec![0u8; 1024 * 1024];
        assert!(total_allocated() > 0);
        std::hint::black_box(&v);
        drop(v);
    }

    #[test]
    fn current_thread_allocated_excludes_other_threads() {
        let (allocated_tx, allocated_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let before = current_thread_allocated();
            let allocation = vec![0u8; 8 * 1024 * 1024];
            let delta = current_thread_allocated().saturating_sub(before);
            allocated_tx.send(delta).unwrap();
            release_rx.recv().unwrap();
            drop(allocation);
        });
        let before = current_thread_allocated();
        let worker_delta = allocated_rx.recv().unwrap();
        let local_delta = current_thread_allocated().saturating_sub(before);

        assert!(worker_delta >= 8 * 1024 * 1024);
        assert!(local_delta < worker_delta / 16);

        release_tx.send(()).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn peak_allocated_tracks_maximum() {
        let before_peak = peak_allocated();
        let v: Vec<u8> = vec![0u8; 8192];
        let during_peak = peak_allocated();
        assert!(during_peak >= before_peak);
        drop(v);
        let after_peak = peak_allocated();
        assert!(after_peak >= during_peak);
    }

    #[test]
    fn rss_returns_positive_value() {
        let r = rss();
        assert!(r > 0);
    }

    #[test]
    fn allocator_stats_has_all_fields() {
        let stats = allocator_stats();
        assert!(stats.allocated > 0);
        assert!(stats.active > 0);
        assert!(stats.resident > 0);
        assert_eq!(stats.muzzy, 0);
    }

    #[test]
    fn realloc_grows_and_shrinks() {
        let before_peak = peak_allocated();
        let mut v: Vec<u8> = Vec::with_capacity(100);
        v.reserve(100000);
        assert!(v.capacity() >= 100000);
        let after_grow_peak = peak_allocated();
        assert!(after_grow_peak >= before_peak);
        std::hint::black_box(&v);
        v.shrink_to(10);
        assert!(v.capacity() < 100000);
        assert!(total_allocated() > 0);
    }

    #[test]
    fn vec_allocation_bytes_reports_allocator_usable_size() {
        let mut bytes = Vec::with_capacity(4096);
        bytes.extend_from_slice(b"payload");

        assert!(vec_allocation_bytes(&bytes) >= bytes.capacity());
    }

    #[test]
    fn zeroed_allocations_actually_zeroed() {
        let v: Vec<u8> = vec![0u8; 512];
        assert!(v.iter().all(|&b| b == 0));
    }
}
