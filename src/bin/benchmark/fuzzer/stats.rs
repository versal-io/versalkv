use std::sync::atomic::{AtomicU64, Ordering};

pub(super) const MAX_ERRORS: usize = 100;

pub(super) struct ErrorEntry {
    pub message: String,
    pub command: String,
}

#[derive(Default)]
pub(super) struct ErrorList {
    pub errors: Vec<ErrorEntry>,
    pub total: usize,
}

impl ErrorList {
    pub fn add(&mut self, message: String, command: String) {
        self.total += 1;
        if self.errors.len() < MAX_ERRORS {
            self.errors.push(ErrorEntry { message, command });
        }
    }
}

#[derive(Default)]
pub struct FuzzerStats {
    commands_sent: AtomicU64,
    success_replies: AtomicU64,
    err_replies: AtomicU64,
    malformed_replies: AtomicU64,
    total_errors: AtomicU64,
}

impl FuzzerStats {
    pub fn incr_sent(&self) {
        self.commands_sent.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_success(&self) {
        self.success_replies.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_err(&self) {
        self.err_replies.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_malformed(&self) {
        self.malformed_replies.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_total_err(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn sent(&self) -> u64 {
        self.commands_sent.load(Ordering::Relaxed)
    }
    pub fn success(&self) -> u64 {
        self.success_replies.load(Ordering::Relaxed)
    }
    pub fn errs(&self) -> u64 {
        self.err_replies.load(Ordering::Relaxed)
    }
    pub fn malformed(&self) -> u64 {
        self.malformed_replies.load(Ordering::Relaxed)
    }
    pub fn total_errs(&self) -> u64 {
        self.total_errors.load(Ordering::Relaxed)
    }
}
