//! Bounded Space-Saving summaries over measured, monotonic time windows.

use crate::foundation::io::sys::rand::random_usize;
use crate::foundation::memory::key_access::KeyAccessObserver;
use std::cell::RefCell;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct HotkeysConfig {
    pub top_k: usize,
    pub sampling_percentage: u8,
    pub window_seconds: u64,
}

#[derive(Clone)]
struct Entry {
    key: Vec<u8>,
    db_id: u32,
    hash: u32,
    count: u64,
    error: u64,
}

#[derive(Default)]
struct Window {
    entries: Vec<Entry>,
    total: u64,
    duration_us: u64,
    sampling_percentage: u8,
}

struct Manager {
    config: HotkeysConfig,
    started: Instant,
    live: Window,
    frozen: Window,
}

impl Manager {
    fn reset(&mut self, now: Instant) {
        self.live.entries.clear();
        self.live.total = 0;
        self.live.duration_us = 0;
        self.live.sampling_percentage = self.config.sampling_percentage;
        self.frozen.entries.clear();
        self.frozen.total = 0;
        self.frozen.duration_us = 0;
        self.started = now;
    }

    fn rotate(&mut self, now: Instant) {
        let elapsed_us = now.saturating_duration_since(self.started).as_micros() as u64;
        let length_us = self.config.window_seconds * 1_000_000;
        if elapsed_us < length_us {
            return;
        }
        if elapsed_us >= 2 * length_us {
            self.reset(now);
            return;
        }
        self.live.duration_us = elapsed_us;
        std::mem::swap(&mut self.live, &mut self.frozen);
        self.live.entries.clear();
        self.live.total = 0;
        self.live.duration_us = 0;
        self.live.sampling_percentage = self.config.sampling_percentage;
        self.started = now;
    }

    fn record_sample(&mut self, db_id: u32, key: &[u8]) {
        self.live.total = self.live.total.saturating_add(1);
        let hash = key.iter().fold(2_166_136_261_u32, |hash, byte| {
            (hash ^ u32::from(*byte)).wrapping_mul(16_777_619)
        }) ^ db_id;
        let mut minimum = 0;
        let mut minimum_count = u64::MAX;
        for (index, entry) in self.live.entries.iter_mut().enumerate() {
            if entry.hash == hash && entry.db_id == db_id && entry.key == key {
                entry.count = entry.count.saturating_add(1);
                return;
            }
            if entry.count < minimum_count {
                minimum = index;
                minimum_count = entry.count;
            }
        }
        let has_room = self.live.entries.len() < self.config.top_k;
        let entry = Entry {
            key: key.to_vec(),
            db_id,
            hash,
            count: if has_room {
                1
            } else {
                minimum_count.saturating_add(1)
            },
            error: if has_room { 0 } else { minimum_count },
        };
        if has_room {
            self.live.entries.push(entry);
        } else {
            self.live.entries[minimum] = entry;
        }
    }
}

#[derive(Default)]
pub struct Hotkeys {
    manager: RefCell<Option<Manager>>,
}

pub struct HotkeyReport {
    pub key: Vec<u8>,
    pub db_id: u32,
    pub qps: u64,
}

impl Hotkeys {
    pub fn configure(&self, config: HotkeysConfig, force: bool) {
        let mut manager = self.manager.borrow_mut();
        if config.top_k == 0 {
            *manager = None;
            return;
        }
        if let Some(manager) = manager.as_mut() {
            if !force && manager.config == config {
                return;
            }
            manager.live.entries.clear();
            manager.live.total = 0;
            manager.live.duration_us = 0;
            manager.live.sampling_percentage = config.sampling_percentage;
            manager.started = Instant::now();
            if manager.frozen.entries.len() > config.top_k {
                manager
                    .frozen
                    .entries
                    .sort_by_key(|entry| std::cmp::Reverse(entry.count));
                manager.frozen.entries.truncate(config.top_k);
            }
            manager.config = config;
        } else {
            *manager = Some(Manager {
                config,
                started: Instant::now(),
                live: Window {
                    entries: Vec::with_capacity(config.top_k),
                    sampling_percentage: config.sampling_percentage,
                    ..Window::default()
                },
                frozen: Window {
                    entries: Vec::with_capacity(config.top_k),
                    ..Window::default()
                },
            });
        }
    }

    pub fn rotate(&self) {
        if let Some(manager) = self.manager.borrow_mut().as_mut() {
            manager.rotate(Instant::now());
        }
    }

    pub fn reset(&self) {
        if let Some(manager) = self.manager.borrow_mut().as_mut() {
            manager.reset(Instant::now());
        }
    }

    pub fn purge(&self, mut predicate: impl FnMut(u32, &[u8]) -> bool) {
        if let Some(manager) = self.manager.borrow_mut().as_mut() {
            manager
                .live
                .entries
                .retain(|entry| !predicate(entry.db_id, &entry.key));
            manager
                .frozen
                .entries
                .retain(|entry| !predicate(entry.db_id, &entry.key));
        }
    }

    pub fn metrics(&self) -> (u64, u64) {
        self.manager
            .borrow()
            .as_ref()
            .map(|manager| (manager.frozen.total, manager.frozen.duration_us / 1000))
            .unwrap_or_default()
    }

    pub fn get(&self) -> Vec<HotkeyReport> {
        self.rotate();
        let manager = self.manager.borrow();
        let Some(manager) = manager.as_ref() else {
            return Vec::new();
        };
        let window = &manager.frozen;
        let denominator =
            2_u128 * u128::from(window.sampling_percentage) * u128::from(window.duration_us);
        let mut reports: Vec<_> = window
            .entries
            .iter()
            .map(|entry| {
                let numerator =
                    (2_u128 * u128::from(entry.count) - u128::from(entry.error)) * 100_000_000;
                HotkeyReport {
                    key: entry.key.clone(),
                    db_id: entry.db_id,
                    qps: (numerator + denominator / 2)
                        .checked_div(denominator)
                        .unwrap_or(0)
                        .min(u128::from(u64::MAX)) as u64,
                }
            })
            .collect();
        reports.sort_by_key(|entry| std::cmp::Reverse(entry.qps));
        reports.truncate(manager.config.top_k);
        reports
    }
}

impl KeyAccessObserver for Hotkeys {
    fn record(&self, db_id: u32, key: &[u8]) {
        let mut manager = self.manager.borrow_mut();
        let Some(manager) = manager.as_mut() else {
            return;
        };
        if manager.config.sampling_percentage < 100
            && random_usize(100) >= usize::from(manager.config.sampling_percentage)
        {
            return;
        }
        manager.record_sample(db_id, key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    fn configured(top_k: usize) -> Hotkeys {
        let hotkeys = Hotkeys::default();
        hotkeys.configure(
            HotkeysConfig {
                top_k,
                sampling_percentage: 100,
                window_seconds: 1,
            },
            false,
        );
        hotkeys
    }

    #[test]
    fn space_saving_bounds_cover_actual_binary_key_frequencies() {
        let hotkeys = configured(3);
        let mut actual = HashMap::new();
        for index in 0..5000 {
            let db_id = (index % 2) as u32;
            let key = if index % 5 != 0 {
                vec![0, 255]
            } else {
                vec![(index % 17) as u8]
            };
            *actual.entry((db_id, key.clone())).or_insert(0_u64) += 1;
            hotkeys.record(db_id, &key);
        }
        let state = hotkeys.manager.borrow();
        let live = &state.as_ref().unwrap().live;
        assert_eq!(live.total, 5000);
        assert_eq!(live.entries.len(), 3);
        for entry in &live.entries {
            let frequency = actual[&(entry.db_id, entry.key.clone())];
            assert!(entry.count - entry.error <= frequency);
            assert!(frequency <= entry.count);
        }
        for (identity, frequency) in actual {
            if frequency > live.total / 3 {
                assert!(live
                    .entries
                    .iter()
                    .any(|entry| (entry.db_id, entry.key.clone()) == identity));
            }
        }
    }

    #[test]
    fn rates_use_measured_duration_and_the_frozen_sampling_percentage() {
        let hotkeys = configured(3);
        {
            let mut state = hotkeys.manager.borrow_mut();
            let manager = state.as_mut().unwrap();
            manager.live.sampling_percentage = 50;
            for _ in 0..550 {
                manager.record_sample(7, b"key\0");
            }
            manager.rotate(manager.started + Duration::from_millis(1100));
        }
        hotkeys.configure(
            HotkeysConfig {
                top_k: 4,
                sampling_percentage: 10,
                window_seconds: 2,
            },
            true,
        );
        let report = hotkeys.get();
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].qps, 1000);
        assert_eq!(report[0].db_id, 7);
        assert_eq!(report[0].key, b"key\0");
        assert_eq!(hotkeys.metrics(), (550, 1100));
    }

    #[test]
    fn delayed_rotation_drops_both_windows_and_preserves_sampling() {
        let hotkeys = configured(3);
        let mut state = hotkeys.manager.borrow_mut();
        let manager = state.as_mut().unwrap();
        manager.record_sample(0, b"old");
        manager.rotate(manager.started + Duration::from_secs(1));
        manager.record_sample(0, b"new");
        manager.rotate(manager.started + Duration::from_secs(2));
        assert!(manager.live.entries.is_empty());
        assert!(manager.frozen.entries.is_empty());
        assert_eq!(manager.live.sampling_percentage, 100);
        assert_eq!(manager.frozen.duration_us, 0);
    }

    #[test]
    fn purges_remove_both_windows_without_changing_observation_totals() {
        let hotkeys = configured(3);
        {
            let mut state = hotkeys.manager.borrow_mut();
            let manager = state.as_mut().unwrap();
            manager.record_sample(1, b"same");
            manager.record_sample(2, b"same");
            manager.rotate(manager.started + Duration::from_secs(1));
            manager.record_sample(1, b"same");
        }
        hotkeys.purge(|db_id, _| db_id == 1);
        let state = hotkeys.manager.borrow();
        let manager = state.as_ref().unwrap();
        assert!(manager.live.entries.is_empty());
        assert_eq!(manager.frozen.entries.len(), 1);
        assert_eq!(manager.frozen.entries[0].db_id, 2);
        assert_eq!(manager.frozen.total, 2);
    }

    #[test]
    fn disabling_discards_the_frozen_report_before_reenabling() {
        let hotkeys = configured(3);
        {
            let mut state = hotkeys.manager.borrow_mut();
            let manager = state.as_mut().unwrap();
            manager.record_sample(0, b"old");
            manager.rotate(manager.started + Duration::from_secs(1));
        }
        hotkeys.configure(
            HotkeysConfig {
                top_k: 0,
                sampling_percentage: 100,
                window_seconds: 1,
            },
            true,
        );
        hotkeys.configure(
            HotkeysConfig {
                top_k: 3,
                sampling_percentage: 100,
                window_seconds: 1,
            },
            true,
        );
        assert!(hotkeys.get().is_empty());
        assert_eq!(hotkeys.metrics(), (0, 0));
    }
}
