//! Owned keyspace events and post-notification execution state.

use bytes::Bytes;
use libc::{c_int, c_void};
use std::collections::{HashMap, VecDeque};

use super::registry::ModuleOptions;
use super::{ValkeyModuleCtx, ValkeyModuleNotificationFunc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleKeyspaceEvent {
    pub event_type: c_int,
    pub event: String,
    pub db_idx: u32,
    pub key: Bytes,
}

pub(crate) struct ModuleNotificationSubscriber {
    pub module_name: String,
    pub subscription: usize,
    pub module_ref: *mut c_void,
    pub options: ModuleOptions,
    pub callback: ValkeyModuleNotificationFunc,
}

pub type ModulePostNotificationCallback = extern "C" fn(*mut ValkeyModuleCtx, *mut c_void);
pub type ModulePostNotificationFree = extern "C" fn(*mut c_void);

pub(crate) struct ModulePostNotificationJob {
    pub module_name: String,
    pub db_idx: u32,
    pub callback: ModulePostNotificationCallback,
    pub data: *mut c_void,
    pub free: Option<ModulePostNotificationFree>,
}

impl Drop for ModulePostNotificationJob {
    fn drop(&mut self) {
        if let Some(free) = self.free.take() {
            free(self.data);
        }
    }
}

#[derive(Default)]
pub(crate) struct ModuleNotificationState {
    jobs: VecDeque<ModulePostNotificationJob>,
    active_subscribers: HashMap<(String, usize), usize>,
    protected_modules: HashMap<String, usize>,
    is_running_jobs: bool,
}

impl ModuleNotificationState {
    pub(crate) fn enqueue(&mut self, job: ModulePostNotificationJob) {
        self.jobs.push_back(job);
    }

    pub(crate) fn protects_module(&self, name: &str) -> bool {
        self.protected_modules.contains_key(name)
            || self.jobs.iter().any(|job| job.module_name == name)
    }

    pub(crate) fn protect_module(&mut self, name: &str) {
        *self.protected_modules.entry(name.to_string()).or_default() += 1;
    }

    pub(crate) fn release_module(&mut self, name: &str) {
        if let Some(count) = self.protected_modules.get_mut(name) {
            *count -= 1;
            if *count == 0 {
                self.protected_modules.remove(name);
            }
        }
    }

    pub(crate) fn enter_subscriber(
        &mut self,
        name: &str,
        subscription: usize,
        allows_nested: bool,
    ) -> bool {
        let key = (name.to_string(), subscription);
        if self.active_subscribers.contains_key(&key) && !allows_nested {
            return false;
        }
        *self.active_subscribers.entry(key).or_default() += 1;
        true
    }

    pub(crate) fn leave_subscriber(&mut self, name: &str, subscription: usize) {
        let key = (name.to_string(), subscription);
        if let Some(count) = self.active_subscribers.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                self.active_subscribers.remove(&key);
            }
        }
    }

    pub(crate) fn begin_jobs(&mut self) -> bool {
        if self.is_running_jobs || self.jobs.is_empty() || !self.active_subscribers.is_empty() {
            return false;
        }
        self.is_running_jobs = true;
        true
    }

    pub(crate) fn pop_job(&mut self) -> Option<ModulePostNotificationJob> {
        let job = self.jobs.pop_front()?;
        self.protect_module(&job.module_name);
        Some(job)
    }

    pub(crate) fn end_jobs(&mut self) {
        self.is_running_jobs = false;
    }

    pub(crate) fn cancel_module_jobs(&mut self, name: &str) {
        self.jobs.retain(|job| job.module_name != name);
    }
}

#[cfg(test)]
#[path = "notifications_tests.rs"]
mod notifications_tests;
