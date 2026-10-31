use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(1);

fn try_next_client_id(ids: &AtomicUsize) -> Option<usize> {
    let mut id = ids.load(Ordering::Relaxed);
    loop {
        let next = id.checked_add(1)?;
        match ids.compare_exchange_weak(id, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Some(id),
            Err(current) => id = current,
        }
    }
}

/// Allocate a process-wide identity shared by ordinary and temporary clients.
pub(crate) fn next_client_id() -> usize {
    #[cfg(test)]
    if let Some(id) = test_support::next_client_id() {
        return id;
    }
    // Never reuse an identity or allocate the usize::MAX fake-client sentinel.
    try_next_client_id(&NEXT_CLIENT_ID).expect("client identity space exhausted")
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::{try_next_client_id, AtomicUsize};
    use std::cell::RefCell;

    thread_local! {
        static CLIENT_IDS: RefCell<Option<AtomicUsize>> = const { RefCell::new(None) };
    }

    pub(crate) fn with_next_client_id<R>(first: usize, run: impl FnOnce() -> R) -> R {
        struct Restore(Option<AtomicUsize>);
        impl Drop for Restore {
            fn drop(&mut self) {
                CLIENT_IDS.with(|ids| *ids.borrow_mut() = self.0.take());
            }
        }
        let previous = CLIENT_IDS.with(|ids| ids.replace(Some(AtomicUsize::new(first))));
        let _restore = Restore(previous);
        run()
    }

    pub(crate) fn next_client_id() -> Option<usize> {
        CLIENT_IDS.with(|ids| {
            let ids = ids.borrow();
            Some(try_next_client_id(ids.as_ref()?).expect("test client identity space exhausted"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_identity_allocator_is_monotonic_across_previous_module_boundary() {
        for first in [1, 0x1000000, 0x1000001] {
            let ids = AtomicUsize::new(first);
            for expected in first..first + 4 {
                assert_eq!(try_next_client_id(&ids), Some(expected));
            }
        }
    }

    #[test]
    fn client_identity_allocator_is_unique_across_threads() {
        let ids = AtomicUsize::new(1);
        let mut allocated = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..256)
                            .map(|_| try_next_client_id(&ids).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        allocated.sort_unstable();
        assert_eq!(allocated, (1..=2048).collect::<Vec<_>>());
        assert_eq!(try_next_client_id(&ids), Some(2049));
    }

    #[test]
    fn client_identity_allocator_exhaustion_preserves_fake_sentinel_and_never_wraps() {
        let ids = AtomicUsize::new(usize::MAX - 1);
        assert_eq!(try_next_client_id(&ids), Some(usize::MAX - 1));
        for _ in 0..3 {
            assert_eq!(try_next_client_id(&ids), None);
            assert_eq!(ids.load(Ordering::Relaxed), usize::MAX);
        }
    }

    #[test]
    fn client_identity_test_scope_restores_after_nested_panic() {
        test_support::with_next_client_id(17, || {
            assert_eq!(next_client_id(), 17);
            let result = std::panic::catch_unwind(|| {
                test_support::with_next_client_id(42, || {
                    assert_eq!(next_client_id(), 42);
                    panic!("leave the nested identity scope");
                });
            });
            assert!(result.is_err());
            assert_eq!(next_client_id(), 18);
        });
        assert_eq!(test_support::next_client_id(), None);
    }
}
