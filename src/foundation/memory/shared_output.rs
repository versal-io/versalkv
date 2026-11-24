//! Physical backing-store accounting for shared output owners.
//!
//! Reservations follow the allocation owner, not connections or queue views.
//! They describe memory already included in the allocator total and must not
//! increase that total again. Owner metadata remains ordinary reported memory.

use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(test))]
static SHARED_OUTPUT_BYTES: AtomicUsize = AtomicUsize::new(0);

/// A single reservation embedded in a shared allocation's final-drop owner.
/// Moving it across threads preserves accounting; cloning views creates no
/// new reservation. The original backing capacity is retained until drop.
pub(crate) struct SharedOutputAllocation {
    bytes: usize,
    #[cfg(test)]
    counter: Option<std::sync::Arc<AtomicUsize>>,
}

impl SharedOutputAllocation {
    pub(crate) fn new(bytes: usize) -> Self {
        #[cfg(not(test))]
        {
            SHARED_OUTPUT_BYTES.fetch_add(bytes, Ordering::Relaxed);
            Self { bytes }
        }
        #[cfg(test)]
        {
            let counter = TEST_COUNTER.with(|counter| counter.borrow().clone());
            if let Some(counter) = &counter {
                counter.fetch_add(bytes, Ordering::Relaxed);
            }
            Self { bytes, counter }
        }
    }
}

impl Drop for SharedOutputAllocation {
    fn drop(&mut self) {
        #[cfg(not(test))]
        SHARED_OUTPUT_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
        #[cfg(test)]
        if let Some(counter) = &self.counter {
            counter.fetch_sub(self.bytes, Ordering::Relaxed);
        }
    }
}

/// Return the capacity of all live shared backing stores, counted once each.
/// This is live even when the final view is in an I/O worker rather than in
/// the main thread's connection registry.
pub(crate) fn allocation_bytes() -> usize {
    #[cfg(not(test))]
    {
        SHARED_OUTPUT_BYTES.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    {
        TEST_COUNTER.with(|counter| {
            counter
                .borrow()
                .as_ref()
                .map_or(0, |counter| counter.load(Ordering::Relaxed))
        })
    }
}

// Parallel unit tests each model an independent server. Explicit test scopes
// isolate exact projection assertions from unrelated servers' output owners.
// The reservation captures its counter so a foreign-thread drop still updates
// the originating scope without requiring worker-thread TLS initialization.
#[cfg(test)]
thread_local! {
    static TEST_COUNTER: std::cell::RefCell<Option<std::sync::Arc<AtomicUsize>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) struct TestAccountingScope {
    previous: Option<std::sync::Arc<AtomicUsize>>,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(test)]
pub(crate) fn isolate_for_test() -> TestAccountingScope {
    let counter = std::sync::Arc::new(AtomicUsize::new(0));
    TestAccountingScope {
        previous: TEST_COUNTER.with(|current| current.replace(Some(counter))),
        _thread_bound: std::marker::PhantomData,
    }
}

#[cfg(test)]
impl Drop for TestAccountingScope {
    fn drop(&mut self) {
        TEST_COUNTER.with(|current| current.replace(self.previous.take()));
    }
}
