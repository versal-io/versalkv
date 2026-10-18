use crate::foundation::io::store::kv_engine::EvictionPolicy;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::PauseAction;
use crate::runtime::server::worker::ServerWorkerState;
use bytes::Bytes;
use std::time::Instant;

pub(crate) struct EvictionOutcome {
    pub(crate) evicted: Vec<(u32, Bytes)>,
}

impl EvictionOutcome {
    fn empty() -> Self {
        Self {
            evicted: Vec::new(),
        }
    }
}

impl ServerWorkerState {
    pub(crate) fn perform_evictions(&mut self) -> EvictionOutcome {
        let maxmemory = self.ctx.config().server.maxmemory;
        if maxmemory == 0 {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        if self.ctx.replication().is_replica()
            && self.ctx.config().replication.replica_ignore_maxmemory
        {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        if self.ctx.client_pause().is_paused(mstime()) != PauseAction::None {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        let state = self.ctx.maxmemory_state();
        if !state.is_over_limit() {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        let policy = EvictionPolicy::from_name(&self.ctx.config().server.maxmemory_policy)
            .unwrap_or(EvictionPolicy::NoEviction);
        if self.ctx.config().server.import_mode || policy == EvictionPolicy::NoEviction {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }

        let tenacity = self
            .ctx
            .config()
            .server
            .maxmemory_eviction_tenacity
            .clamp(0, 100);
        let time_limit_us = if tenacity <= 10 {
            50u128 * tenacity as u128
        } else if tenacity < 100 {
            (500.0 * 1.15_f64.powi(tenacity as i32 - 10)) as u128
        } else {
            u128::MAX
        };
        let target = state.to_free;
        let samples = self.ctx.config().server.maxmemory_samples.max(1) as usize;
        let timer = Instant::now();
        let mut freed = 0usize;
        let mut evicted = Vec::new();

        while freed < target {
            let mut candidate: Option<(u32, Vec<u8>, u64)> = None;
            for db_idx in 0..self.ctx.num_dbs() {
                let Some(db) = self.ctx.db(db_idx as u32) else {
                    continue;
                };
                if let Some(sample) = db.sample_eviction_candidate(policy, samples) {
                    if policy.is_random()
                        || candidate
                            .as_ref()
                            .is_none_or(|(_, _, score)| sample.score > *score)
                    {
                        candidate = Some((db_idx as u32, sample.key, sample.score));
                    }
                    if policy.is_random() {
                        break;
                    }
                }
            }
            let Some((db_idx, key, _)) = candidate else {
                self.cancel_eviction_timer();
                return EvictionOutcome { evicted };
            };

            let before = crate::foundation::memory::util_alloc::total_allocated();
            let deleted = self.ctx.db_mut(db_idx).is_some_and(|db| db.del(&key));
            let after = crate::foundation::memory::util_alloc::total_allocated();
            if !deleted {
                continue;
            }
            freed = freed.saturating_add(before.saturating_sub(after));
            self.ctx.incr_evicted_keys(1);
            evicted.push((db_idx, Bytes::from(key)));

            if evicted.len() % 16 == 0 && timer.elapsed().as_micros() > time_limit_us {
                self.schedule_eviction();
                return EvictionOutcome { evicted };
            }
        }

        self.cancel_eviction_timer();
        EvictionOutcome { evicted }
    }
}

#[cfg(test)]
mod tests {
    use crate::foundation::io::store::Robj;
    use bytes::Bytes;

    const ISOLATION_MARKER: &str = "VALKEY_TEST_MAXMEMORY_EVICTION_CYCLE";

    fn enter_isolated_test() -> bool {
        if std::env::var_os(ISOLATION_MARKER).is_some() {
            return false;
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::server::worker::flows::maintenance::eviction::tests::one_eviction_cycle_frees_the_complete_initial_target",
            ])
            .env(ISOLATION_MARKER, "1")
            .status()
            .expect("run allocator-sensitive eviction test in isolation");
        assert_eq!(status.code(), Some(86));
        true
    }

    #[test]
    fn one_eviction_cycle_frees_the_complete_initial_target() {
        if enter_isolated_test() {
            return;
        }
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
        runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
        for index in 0..64 {
            runtime.ctx.db_mut(0).unwrap().set(
                format!("key-{index}").into_bytes(),
                Robj::string_raw(Bytes::from(vec![b'x'; 4096])),
            );
        }
        let before = runtime.ctx.maxmemory_state().logical;
        runtime.ctx.config_mut().server.maxmemory = before.saturating_sub(24 * 1024) as u64;

        let outcome = runtime.perform_evictions();

        assert!(outcome.evicted.len() > 1);
        drop(outcome);
        assert!(!runtime.ctx.maxmemory_state().is_over_limit());
        std::process::exit(86);
    }

    #[test]
    fn replica_ignore_maxmemory_skips_key_eviction() {
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().role =
            crate::engine::distributed::replication::ReplicationRole::Replica;
        runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
        runtime.ctx.config_mut().server.maxmemory = 1;
        runtime
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));

        let outcome = runtime.perform_evictions();

        assert!(outcome.evicted.is_empty());
        assert!(runtime
            .ctx
            .db(0)
            .unwrap()
            .get_allow_expired(b"key")
            .is_some());
    }

    #[test]
    fn paused_eviction_stops_the_pending_continuation() {
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory = 1;
        runtime.ctx.set_needs_eviction(true);
        runtime.ctx.client_pause_mut().action = crate::framework::context::PauseAction::All;
        runtime.ctx.client_pause_mut().end_time_ms = i64::MAX;

        runtime.perform_evictions();

        assert!(!runtime.ctx.needs_eviction());
        assert!(!runtime.time_events.has_due_kind(
            crate::runtime::server::worker::reactor::TimerEvent::EvictionContinuation
        ));
    }
}
