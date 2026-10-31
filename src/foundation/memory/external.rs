use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalMemoryError {
    Overflow,
    Underflow,
}

pub(super) struct ExternalMemoryCounter {
    bytes: AtomicUsize,
}

impl ExternalMemoryCounter {
    const fn new(bytes: usize) -> Self {
        Self {
            bytes: AtomicUsize::new(bytes),
        }
    }

    pub(super) fn used(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    pub(super) fn increase(&self, bytes: usize) -> Result<usize, ExternalMemoryError> {
        let mut current = self.used();
        loop {
            let next = current
                .checked_add(bytes)
                .ok_or(ExternalMemoryError::Overflow)?;
            match self.bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(next),
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn decrease(&self, bytes: usize) -> Result<usize, ExternalMemoryError> {
        let mut current = self.used();
        loop {
            let next = current
                .checked_sub(bytes)
                .ok_or(ExternalMemoryError::Underflow)?;
            match self.bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(next),
                Err(observed) => current = observed,
            }
        }
    }
}

pub(super) static MODULE_EXTERNAL_MEMORY: ExternalMemoryCounter = ExternalMemoryCounter::new(0);

#[cfg(test)]
pub(crate) fn lock_external_memory() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn module_external_memory_checked_boundaries_leave_balance_unchanged() {
        let counter = ExternalMemoryCounter::new(0);
        assert_eq!(counter.increase(0), Ok(0));
        assert_eq!(counter.decrease(0), Ok(0));
        assert_eq!(counter.decrease(1), Err(ExternalMemoryError::Underflow));
        assert_eq!(counter.used(), 0);
        assert_eq!(counter.increase(usize::MAX), Ok(usize::MAX));
        assert_eq!(counter.increase(1), Err(ExternalMemoryError::Overflow));
        assert_eq!(counter.used(), usize::MAX);
        assert_eq!(counter.decrease(usize::MAX), Ok(0));
        assert_eq!(counter.increase(123), Ok(123));
        assert_eq!(counter.decrease(124), Err(ExternalMemoryError::Underflow));
        assert_eq!(counter.used(), 123);
        assert_eq!(counter.decrease(123), Ok(0));
    }

    #[test]
    fn module_external_memory_concurrent_updates_are_exact() {
        let counter = Arc::new(ExternalMemoryCounter::new(0));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let counter = Arc::clone(&counter);
                scope.spawn(move || {
                    for _ in 0..10_000 {
                        counter.increase(7).unwrap();
                        counter.decrease(7).unwrap();
                    }
                });
            }
        });
        assert_eq!(counter.used(), 0);
    }

    #[test]
    fn module_external_memory_concurrent_range_checks_do_not_wrap() {
        for should_increase in [true, false] {
            let counter = Arc::new(ExternalMemoryCounter::new(if should_increase {
                usize::MAX - 3
            } else {
                3
            }));
            let successes = AtomicUsize::new(0);
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    let counter = Arc::clone(&counter);
                    let successes = &successes;
                    scope.spawn(move || {
                        let result = if should_increase {
                            counter.increase(1)
                        } else {
                            counter.decrease(1)
                        };
                        if result.is_ok() {
                            successes.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            });
            assert_eq!(successes.load(Ordering::Relaxed), 3);
            assert_eq!(counter.used(), if should_increase { usize::MAX } else { 0 });
        }
    }
}
