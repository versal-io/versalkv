use crate::foundation::io::store::kv_engine::{EvictionCandidate, EvictionPolicy};
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::PauseAction;
use crate::runtime::server::worker::ServerWorkerState;
use bytes::Bytes;
use std::time::Instant;

pub(crate) struct EvictionOutcome {
    pub(crate) evicted: Vec<(u32, Bytes)>,
}

enum DbEvictionRoute {
    Candidate(EvictionCandidate),
    CapturedFallback(Option<EvictionCandidate>),
    Empty,
}

impl EvictionOutcome {
    fn empty() -> Self {
        Self {
            evicted: Vec::new(),
        }
    }
}

impl ServerWorkerState {
    fn sample_db_eviction_candidate(
        &self,
        db_idx: u32,
        policy: EvictionPolicy,
        samples: usize,
    ) -> DbEvictionRoute {
        let Some(db) = self.ctx.db(db_idx) else {
            return DbEvictionRoute::Empty;
        };
        let Some(sample) = db.sample_eviction_candidate(policy, samples) else {
            return DbEvictionRoute::Empty;
        };
        let persistence = self.ctx.persistence();
        if !persistence.forkless_capture_in_progress()
            || persistence.forkless_key_captured(db_idx, &sample.key)
        {
            return DbEvictionRoute::Candidate(sample);
        }
        let mut best: Option<EvictionCandidate> = None;
        for key in persistence.forkless_eviction_candidates(db_idx, samples) {
            let Some(candidate) = db.eviction_candidate_for_key(key, policy) else {
                continue;
            };
            if policy.is_random() {
                return DbEvictionRoute::CapturedFallback(Some(candidate));
            }
            if best
                .as_ref()
                .is_none_or(|previous| candidate.score > previous.score)
            {
                best = Some(candidate);
            }
        }
        DbEvictionRoute::CapturedFallback(best)
    }

    pub(crate) fn perform_evictions(&mut self) -> EvictionOutcome {
        let maxmemory = self.ctx.config().server.maxmemory;
        if maxmemory == 0 {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        if self.ctx.suppress_eviction_until_write()
            || self.ctx.is_host_task_loading()
            || self.ctx.is_loading()
        {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        if self.ctx.replication().is_replica()
            && self.ctx.config().replication.replica_ignore_maxmemory
        {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        // Fast path: no client pause is scheduled, so skip the `mstime()`
        // syscall. `ClientPauseState::is_paused` returns None immediately when
        // `end_time_ms == 0`, so this covers the common no-pause workload.
        let client_pause = self.ctx.client_pause();
        if client_pause.end_time_ms != 0 && client_pause.is_paused(mstime()) != PauseAction::None {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        let state = self.ctx.maxmemory_state();
        if !state.is_over_limit() {
            self.cancel_eviction_timer();
            return EvictionOutcome::empty();
        }
        // Sent tracking invalidations can fill the idle reply pool. Reclaim
        // those reusable allocations before deleting more keys, otherwise
        // each eviction leaves new retained buffers behind after output drains.
        let state = if self.reply_buffer_pool.is_empty() {
            state
        } else {
            self.reply_buffer_pool.clear();
            self.ctx.maxmemory_state()
        };
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
            let mut has_uncaptured_candidate = false;
            for db_idx in 0..self.ctx.num_dbs() {
                let sample = match self.sample_db_eviction_candidate(db_idx as u32, policy, samples)
                {
                    DbEvictionRoute::Candidate(sample) => Some(sample),
                    DbEvictionRoute::CapturedFallback(sample) => {
                        has_uncaptured_candidate = true;
                        self.ctx
                            .persistence_mut()
                            .advance_forkless_eviction_candidates(db_idx as u32, samples);
                        sample
                    }
                    DbEvictionRoute::Empty => None,
                };
                if let Some(sample) = sample {
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
                if has_uncaptured_candidate {
                    // A rejected snapshot sample does not prove exhaustion.
                    // Preserve capture backpressure before sampling again.
                    let delay = self
                        .ctx
                        .persistence()
                        .forkless_capture_poll_delay()
                        .unwrap_or_default();
                    self.schedule_eviction_after(delay);
                } else {
                    // One complete DB pass found no eligible keys.
                    self.cancel_eviction_timer();
                }
                return EvictionOutcome { evicted };
            };

            // Shared values retained by snapshots are not released by deletion.
            let before = crate::foundation::memory::util_alloc::total_allocated();
            let deleted = self.ctx.db_mut(db_idx).is_some_and(|db| db.del(&key));
            let after = crate::foundation::memory::util_alloc::total_allocated();
            if !deleted {
                // Do not repeatedly sample a key that could not be deleted.
                self.cancel_eviction_timer();
                return EvictionOutcome { evicted };
            }
            freed = freed.saturating_add(before.saturating_sub(after));
            self.ctx.incr_evicted_keys(1);
            evicted.push((db_idx, Bytes::from(key)));

            if freed < target
                && evicted.len() % 16 == 0
                && timer.elapsed().as_micros() > time_limit_us
            {
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
    use crate::foundation::io::persistence::rdb::{
        compression::{RdbCompression, RdbSnapshotOptions},
        frozen::FrozenRdb,
        RdbSerializeOptions,
    };
    use crate::foundation::io::persistence::snapshot::background::BackgroundRdbSave;
    use crate::foundation::io::store::Robj;
    use crate::foundation::io::sys::time::MonotonicInstant;
    use crate::runtime::server::worker::event_loop::{EventLoopEvent, TimerEvent};
    use crate::runtime::server::worker::ServerWorkerState;
    use bytes::Bytes;
    use std::time::{Duration, Instant};

    const ISOLATION_MARKER: &str = "VALKEY_TEST_MAXMEMORY_EVICTION_CYCLE";

    fn assert_no_candidate_sampling_is_bounded(
        runtime: &mut crate::runtime::server::worker::ServerWorkerState,
    ) {
        runtime.ctx.config_mut().server.maxmemory = 1;
        runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
        runtime.ctx.config_mut().server.maxmemory_samples = 5;
        runtime.ctx.set_needs_eviction(true);
        assert!(runtime.ctx.maxmemory_state().is_over_limit());

        let outcome = runtime.perform_evictions();

        assert!(outcome.evicted.is_empty());
        assert!(!runtime.ctx.needs_eviction());
        assert!(!runtime.time_events.has_due_kind(
            crate::runtime::server::worker::event_loop::TimerEvent::EvictionContinuation
        ));
        let sample_calls: usize = (0..runtime.ctx.num_dbs())
            .map(|index| {
                runtime
                    .ctx
                    .db(index as u32)
                    .unwrap()
                    .eviction_sample_calls()
            })
            .sum();
        assert!(
            sample_calls <= runtime.ctx.num_dbs(),
            "an unchanged no-candidate dataset sampled DBs {sample_calls} times, expected at most {}",
            runtime.ctx.num_dbs()
        );
    }

    #[test]
    fn empty_sixteen_db_eviction_stops_after_one_sampling_pass() {
        for policy in ["allkeys-random", "allkeys-lru", "allkeys-lfu"] {
            let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(16).unwrap();
            runtime.ctx.config_mut().server.maxmemory_policy = policy.to_string();

            assert_no_candidate_sampling_is_bounded(&mut runtime);
        }
    }

    #[test]
    fn volatile_sixteen_db_eviction_without_ttls_stops_after_one_sampling_pass() {
        for policy in [
            "volatile-random",
            "volatile-lru",
            "volatile-lfu",
            "volatile-ttl",
        ] {
            let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(16).unwrap();
            for index in 0..16 {
                runtime.ctx.db_mut(index).unwrap().set(
                    b"persistent".to_vec(),
                    Robj::string_raw(Bytes::from(vec![b'x'; 1_024])),
                );
            }
            runtime.ctx.config_mut().server.maxmemory_policy = policy.to_string();

            assert_no_candidate_sampling_is_bounded(&mut runtime);

            for index in 0..16 {
                assert_eq!(runtime.ctx.db(index).unwrap().dbsize(), 1);
            }
        }
    }

    #[test]
    fn importing_sixteen_db_eviction_stops_after_one_sampling_pass() {
        for policy in ["allkeys-random", "allkeys-lru", "volatile-ttl"] {
            let mut runtime =
                crate::runtime::server::worker::ServerWorkerState::new_with_slot_router(
                    16,
                    14,
                    |_| 16_383,
                )
                .unwrap();
            for index in 0..16 {
                let db = runtime.ctx.db_mut(index).unwrap();
                db.set(
                    b"imported".to_vec(),
                    Robj::string_raw(Bytes::from(vec![b'x'; 1_024])),
                );
                db.set_expire(b"imported", i64::MAX as u64);
                db.set_slot_importing(16_383, true);
                assert_eq!(db.dbsize(), 0);
            }
            runtime.ctx.config_mut().server.maxmemory_policy = policy.to_string();

            assert_no_candidate_sampling_is_bounded(&mut runtime);

            for index in 0..16 {
                let db = runtime.ctx.db_mut(index).unwrap();
                assert!(db.get_allow_expired(b"imported").is_some());
                db.set_slot_importing(16_383, false);
                assert_eq!(db.random_eviction_key(), Some(b"imported".to_vec()));
            }
        }
    }

    #[test]
    fn eviction_does_not_count_retained_shared_values_as_freed() {
        const MARKER: &str = "VALKEY_TEST_RETAINED_EVICTION_VALUES";
        if std::env::var_os(MARKER).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::server::worker::maintenance::eviction::tests::eviction_does_not_count_retained_shared_values_as_freed",
                ])
                .env(MARKER, "1")
                .status()
                .expect("run retained-value eviction test in isolation");
            assert_eq!(status.code(), Some(86));
            return;
        }

        for should_share_snapshot in [false, true] {
            let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
            let retained = Bytes::from(vec![b'x'; 1_024 * 1_024]);
            let mut snapshots = Vec::new();
            for index in 0..32 {
                let key = format!("key-{index}").into_bytes();
                let db = runtime.ctx.db_mut(0).unwrap();
                db.set(key.clone(), Robj::string_raw(retained.clone()));
                if should_share_snapshot {
                    let snapshot = db.snapshot_value_for_background(&key, 0).unwrap();
                    assert!(snapshot.is_shared());
                    snapshots.push(snapshot);
                }
            }
            runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
            runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
            let before = runtime.ctx.maxmemory_state().logical;
            runtime.ctx.config_mut().server.maxmemory = before.saturating_sub(512 * 1_024) as u64;

            let outcome = runtime.perform_evictions();

            assert_eq!(
                outcome.evicted.len(),
                32,
                "retained values must not satisfy the release target merely by deleting their keys"
            );
            assert_eq!(runtime.ctx.db(0).unwrap().dbsize(), 0);
            assert!(runtime.ctx.maxmemory_state().is_over_limit());
            assert!(!runtime.ctx.needs_eviction());
            assert_eq!(retained.len(), 1_024 * 1_024);
            assert_eq!(snapshots.len(), if should_share_snapshot { 32 } else { 0 });
        }
        std::process::exit(86);
    }

    #[test]
    fn importing_only_dataset_does_not_stall_eviction_sampling() {
        let mut runtime =
            crate::runtime::server::worker::ServerWorkerState::new_with_slot_router(16, 14, |_| {
                16_383
            })
            .unwrap();
        runtime.ctx.db_mut(0).unwrap().set(
            b"imported".to_vec(),
            Robj::string_raw(Bytes::from(vec![b'x'; 1_024])),
        );
        runtime
            .ctx
            .db_mut(0)
            .unwrap()
            .set_slot_importing(16_383, true);
        assert_eq!(runtime.ctx.db(0).unwrap().dbsize(), 0);
        runtime.ctx.config_mut().server.maxmemory = 1;
        runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();

        let started = std::time::Instant::now();
        let outcome = runtime.perform_evictions();
        let elapsed = started.elapsed();
        eprintln!("importing-only eviction sampling elapsed: {elapsed:?}");

        assert!(outcome.evicted.is_empty());
        assert!(runtime
            .ctx
            .db(0)
            .unwrap()
            .get_allow_expired(b"imported")
            .is_some());
        assert!(
            // Leave ample headroom for loaded test hosts while catching the
            // multi-second full-directory scans that trigger cluster failover.
            elapsed < std::time::Duration::from_secs(4),
            "sampling an importing-only dataset stalled the event loop for {elapsed:?}"
        );
        runtime
            .ctx
            .db_mut(0)
            .unwrap()
            .set_slot_importing(16_383, false);
        assert_eq!(
            runtime.ctx.db(0).unwrap().random_eviction_key(),
            Some(b"imported".to_vec()),
            "completed import must make its keys eligible again"
        );
    }

    fn enter_isolated_test() -> bool {
        if std::env::var_os(ISOLATION_MARKER).is_some() {
            return false;
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::server::worker::maintenance::eviction::tests::one_eviction_cycle_frees_the_complete_initial_target",
            ])
            .env(ISOLATION_MARKER, "1")
            .status()
            .expect("run allocator-sensitive eviction test in isolation");
        assert_eq!(status.code(), Some(86));
        true
    }

    const FORKLESS_VICTIM: &[u8] = b"uncaptured-victim";

    struct ForklessEvictionFixture {
        runtime: ServerWorkerState,
        dir: std::path::PathBuf,
    }

    impl ForklessEvictionFixture {
        fn new() -> Self {
            let mut runtime = ServerWorkerState::new(2).unwrap();
            // Only DB 1 has a volatile candidate. DB 0 fills the real bounded
            // feed before capture can reach that candidate, regardless of RNG.
            for index in 0..256 {
                runtime.ctx.db_mut(0).unwrap().set(
                    format!("persistent-{index}").into_bytes(),
                    Robj::string_raw(Bytes::from(vec![b'x'; 1_024])),
                );
            }
            let db = runtime.ctx.db_mut(1).unwrap();
            db.set(
                FORKLESS_VICTIM.to_vec(),
                Robj::string_raw(Bytes::from(vec![b'x'; 1_024])),
            );
            db.set_expire(FORKLESS_VICTIM, i64::MAX as u64);
            runtime.ctx.config_mut().server.maxmemory = 1;
            runtime.ctx.config_mut().server.maxmemory_policy = "volatile-random".to_string();
            runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
            let dir = std::env::temp_dir().join(format!(
                "versal-forkless-eviction-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let frozen = FrozenRdb::capture_header(
                runtime.ctx.dbs(),
                &[],
                &[],
                None,
                RdbSerializeOptions {
                    compression: false,
                    checksum: true,
                },
            );
            let (task, feeder) = BackgroundRdbSave::start_incremental(
                frozen.total_keys(),
                dir.clone(),
                "dump.rdb".into(),
                1,
                // Hold the consumer until the test explicitly releases it.
                i64::MAX,
                false,
                RdbSnapshotOptions {
                    compression: RdbCompression::No,
                    checksum: true,
                },
            )
            .unwrap();
            let feed = frozen.into_incremental_feed(
                feeder,
                crate::foundation::io::store::kv_engine::now_ms(),
                false,
            );
            runtime
                .ctx
                .persistence_mut()
                .begin_forkless_save(task, 0, Vec::new(), Some(feed))
                .unwrap();
            Self { runtime, dir }
        }

        fn fill_feed(&mut self) {
            let started = Instant::now();
            while self.runtime.ctx.persistence().forkless_capture_poll_delay()
                != Some(Duration::from_millis(1))
            {
                assert!(started.elapsed() < Duration::from_secs(5));
                self.runtime.ctx.drive_forkless_feed();
            }
            assert!(self
                .runtime
                .ctx
                .persistence()
                .forkless_capture_in_progress());
            assert!(!self
                .runtime
                .ctx
                .persistence()
                .forkless_key_captured(1, FORKLESS_VICTIM));
        }

        fn capture_victim(&mut self) {
            self.runtime.ctx.persistence().set_forkless_key_delay(0);
            let started = Instant::now();
            while !self
                .runtime
                .ctx
                .persistence()
                .forkless_key_captured(1, FORKLESS_VICTIM)
            {
                assert!(started.elapsed() < Duration::from_secs(5));
                self.runtime.ctx.drive_forkless_feed();
                std::thread::yield_now();
            }
            assert_eq!(self.runtime.ctx.db(1).unwrap().dbsize(), 1);
        }
    }

    impl Drop for ForklessEvictionFixture {
        fn drop(&mut self) {
            self.runtime.ctx.abort_forkless_save(0);
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }

    fn collect_one_eviction_timer(runtime: &mut ServerWorkerState, origin: MonotonicInstant) {
        let delay = runtime
            .time_events
            .next_timeout(origin)
            .expect("eviction continuation remains registered");
        let deadline = origin.saturating_add(delay);
        // Advance an explicit clock to the real registered deadline, then use
        // the same pending-event and dispatch path as production collection.
        let mut due = runtime.time_events.take_due(deadline);
        assert_eq!(
            due.len(),
            1,
            "only one eviction continuation may be registered"
        );
        let event = due.pop().unwrap();
        assert_eq!(event.kind, TimerEvent::EvictionContinuation);
        runtime
            .event_batch
            .push_time(deadline, EventLoopEvent::timer(event.id, event.kind));
        runtime.pending_due_time_events.insert(event.id, event);
        assert_eq!(runtime.pending_due_time_events.len(), 1);
        assert!(runtime
            .event_batch
            .contains_timer(TimerEvent::EvictionContinuation));
        assert!(runtime
            .time_events
            .next_timeout(MonotonicInstant::now())
            .is_none());
    }

    fn assert_forkless_eviction_exhausted(runtime: &ServerWorkerState) {
        assert_eq!(runtime.ctx.stats().evicted_keys, 1);
        assert_eq!(runtime.ctx.db(1).unwrap().dbsize(), 0);
        assert_eq!(runtime.ctx.db(0).unwrap().dbsize(), 256);
        assert!(runtime.ctx.maxmemory_state().is_over_limit());
        assert!(!runtime.ctx.needs_eviction());
        assert!(runtime.pending_due_time_events.is_empty());
        assert!(!runtime.event_batch.has_due_time_events());
        assert!(runtime
            .time_events
            .next_timeout(MonotonicInstant::now())
            .is_none());
    }

    fn prepare_same_db_fallback(fixture: &mut ForklessEvictionFixture) -> (Vec<u8>, Vec<u8>) {
        use crate::foundation::io::store::kv_engine::EvictionPolicy;

        fixture.fill_feed();
        let runtime = &mut fixture.runtime;
        let keys: Vec<_> = (0..256)
            .map(|index| format!("persistent-{index}").into_bytes())
            .collect();
        let captured = keys
            .iter()
            .find(|key| runtime.ctx.persistence().forkless_key_captured(0, key))
            .unwrap()
            .clone();
        let uncaptured = keys
            .iter()
            .find(|key| !runtime.ctx.persistence().forkless_key_captured(0, key))
            .unwrap()
            .clone();
        let db = runtime.ctx.db_mut(0).unwrap();
        db.set_lru_idle(&uncaptured, 3_600);
        db.set_lru_idle(&captured, 0);
        db.set_eviction_sample_keys_for_test(vec![uncaptured.clone(), captured.clone()]);
        let ordinary = db
            .sample_eviction_candidate(EvictionPolicy::AllkeysLru, 2)
            .unwrap();
        assert_eq!(ordinary.key, uncaptured);
        assert!(ordinary.score > db.eviction_score(&captured, EvictionPolicy::AllkeysLru));
        runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-lru".into();
        runtime.ctx.config_mut().server.maxmemory_samples = 2;
        assert!(runtime.ctx.maxmemory_state().is_over_limit());
        (captured, uncaptured)
    }

    #[test]
    fn forkless_eviction_falls_back_when_same_db_best_is_uncaptured() {
        let mut fixture = ForklessEvictionFixture::new();
        let (_, uncaptured) = prepare_same_db_fallback(&mut fixture);
        let runtime = &mut fixture.runtime;
        let outcome = runtime.perform_evictions();

        eprintln!(
            "same-db fallback: ordinary_captured=false, safe_live=true, deleted={}, evicted_keys={}, save_active={}",
            outcome.evicted.len(),
            runtime.ctx.stats().evicted_keys,
            runtime.ctx.persistence().forkless_save_in_progress(),
        );
        assert!(
            runtime.ctx.stats().evicted_keys > 0,
            "an uncaptured ordinary best must not hide captured live candidates in the same DB"
        );
        assert!(!outcome.evicted.is_empty());
        assert!(outcome
            .evicted
            .iter()
            .all(|(db_id, key)| runtime.ctx.persistence().forkless_key_captured(*db_id, key)));
        assert!(runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(&uncaptured)
            .is_some());
        assert!(runtime.ctx.persistence().forkless_capture_in_progress());
        assert!(runtime.ctx.persistence().forkless_save_in_progress());
        assert_eq!(
            runtime.ctx.persistence().forkless_capture_poll_delay(),
            Some(Duration::from_millis(1))
        );
    }

    #[test]
    fn forkless_set_dispatch_evicts_immediately_and_preserves_original_snapshot() {
        use crate::engine::command::CommandArgs;
        use crate::foundation::io::persistence::rdb::{RdbDecoder, RdbStep};
        use crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome;
        use crate::foundation::io::store::kv_engine::DbSet;
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;
        use crate::runtime::server::worker::command::dispatch::{LoopAction, ReadBatchEffects};

        let mut fixture = ForklessEvictionFixture::new();
        let write_key = b"new-write-target";
        assert!(fixture
            .runtime
            .ctx
            .prioritize_forkless_keys(0, [write_key.to_vec()]));
        let (_, uncaptured) = prepare_same_db_fallback(&mut fixture);
        let runtime = &mut fixture.runtime;
        crate::framework::commands::register_all(&mut runtime.registry);
        let token = Token(42);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        connection.set_custom_reply_enabled(false);
        runtime.connection_registry.insert(connection);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(write_key),
            Bytes::from_static(b"updated"),
        ]);
        let mut effects = ReadBatchEffects::default();

        assert!(runtime
            .ctx
            .persistence()
            .forkless_key_captured(0, write_key));
        assert!(!runtime.ctx.persistence().forkless_key_blocked(0, write_key));
        let action = runtime.process_parsed_command(token, args, "set", false, &mut effects);
        eprintln!(
            "SET dispatch result: action={action:?}, blocked={}, evicted={}, replies={:?}",
            runtime.connection_registry[token].blocked.is_some(),
            runtime.ctx.stats().evicted_keys,
            effects.replies,
        );
        assert_eq!(action, LoopAction::Continue);

        assert!(runtime.ctx.stats().evicted_keys > 0);
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(write_key)
                .unwrap()
                .as_bytes(),
            b"updated"
        );
        assert!(runtime
            .ctx
            .db(0)
            .unwrap()
            .get_no_expire(&uncaptured)
            .is_some());
        assert!(runtime.ctx.persistence().forkless_capture_in_progress());
        assert!(runtime.ctx.persistence().forkless_save_in_progress());
        assert_eq!(
            runtime.ctx.persistence().forkless_key_delay_for_test(),
            Some(i64::MAX),
            "dispatch must preserve the configured serializer pace"
        );
        eprintln!(
            "same-db SET dispatch: immediate_evicted_keys={}, uncaptured_live=true, save_active=true",
            runtime.ctx.stats().evicted_keys
        );

        // Release only after checking the command's immediate save window.
        runtime.ctx.persistence().set_forkless_key_delay(0);
        let started = Instant::now();
        while runtime.ctx.persistence().forkless_capture_in_progress() {
            assert!(started.elapsed() < Duration::from_secs(5));
            runtime.ctx.drive_forkless_feed();
            std::thread::yield_now();
        }
        assert!(matches!(
            runtime.ctx.finish_primary_forkless_save_for_test(),
            Some(RdbSaveOutcome::Saved)
        ));
        let mut decoder = RdbDecoder::new(true);
        decoder.feed(&std::fs::read(fixture.dir.join("dump.rdb")).unwrap());
        let mut restored = DbSet::new(2);
        loop {
            match decoder.step(&mut restored).unwrap() {
                RdbStep::RecordApplied => {}
                RdbStep::Finished(_) => break,
                RdbStep::NeedInput => panic!("complete snapshot requested more input"),
                RdbStep::InlineCommand { .. } => panic!("snapshot has no inline commands"),
            }
        }
        assert_eq!(restored.get(0).unwrap().dbsize(), 256);
        assert_eq!(restored.get(1).unwrap().dbsize(), 1);
        assert!(restored.get(0).unwrap().get_no_expire(write_key).is_none());
        for index in 0..256 {
            let key = format!("persistent-{index}");
            assert_eq!(
                restored
                    .get(0)
                    .unwrap()
                    .get_no_expire(key.as_bytes())
                    .unwrap()
                    .as_bytes(),
                vec![b'x'; 1_024]
            );
        }
        assert_eq!(
            restored
                .get(1)
                .unwrap()
                .get_no_expire(FORKLESS_VICTIM)
                .unwrap()
                .as_bytes(),
            vec![b'x'; 1_024]
        );
        eprintln!("same-db SET snapshot: restored_original_keys=257, original_values_intact=true");
    }

    #[test]
    fn eviction_during_forkless_capture_only_removes_handed_off_keys() {
        let mut fixture = ForklessEvictionFixture::new();
        fixture.capture_victim();
        let runtime = &mut fixture.runtime;

        let outcome = runtime.perform_evictions();

        assert_eq!(
            outcome.evicted,
            vec![(1, Bytes::from_static(FORKLESS_VICTIM))],
            "the captured candidate must actually be evicted"
        );
        assert!(runtime
            .ctx
            .persistence()
            .forkless_key_captured(1, FORKLESS_VICTIM));
        assert_forkless_eviction_exhausted(runtime);
    }

    #[test]
    fn forkless_eviction_only_removes_keys_already_handed_to_snapshot() {
        let mut fixture = ForklessEvictionFixture::new();
        fixture.fill_feed();
        let runtime = &mut fixture.runtime;
        let origin = MonotonicInstant::now();

        for _ in 0..2 {
            assert!(runtime.perform_evictions().evicted.is_empty());
            assert_eq!(runtime.ctx.db(1).unwrap().dbsize(), 1);
            assert_eq!(runtime.ctx.stats().evicted_keys, 0);
            assert!(!runtime.ctx.needs_eviction());
        }
        let capture_delay = runtime.ctx.persistence().forkless_capture_poll_delay();
        let eviction_delay = runtime.time_events.next_timeout(origin).unwrap();
        let poll_delay = runtime.next_poll_timeout(origin).unwrap();
        eprintln!(
            "forkless eviction backpressure: capture_delay={capture_delay:?}, eviction_delay={eviction_delay:?}, poll_delay={poll_delay:?}, rejected=2, deleted=0"
        );
        assert_eq!(capture_delay, Some(Duration::from_millis(1)));
        assert!(runtime.time_events.take_due(origin).is_empty());
        assert!(
            eviction_delay >= capture_delay.unwrap(),
            "snapshot rejection must preserve the existing capture backpressure deadline"
        );
        assert_eq!(poll_delay, capture_delay.unwrap());

        collect_one_eviction_timer(runtime, origin);
        assert!(runtime.perform_evictions().evicted.is_empty());
        assert_eq!(runtime.pending_due_time_events.len(), 1);
        assert!(runtime.time_events.next_timeout(origin).is_none());
        let handled_before = runtime.ctx.stats().eventloop_events_handled;
        runtime.dispatch_time_phase().unwrap();
        assert_eq!(
            runtime.ctx.stats().eventloop_events_handled,
            handled_before + 1
        );
        assert_eq!(runtime.ctx.stats().evicted_keys, 0);
        assert_eq!(runtime.ctx.db(1).unwrap().dbsize(), 1);

        fixture.capture_victim();
        let runtime = &mut fixture.runtime;
        runtime.event_batch.begin_next_turn();
        collect_one_eviction_timer(runtime, origin);
        runtime.schedule_eviction();
        assert_eq!(runtime.pending_due_time_events.len(), 1);
        assert!(runtime.time_events.next_timeout(origin).is_none());
        runtime.dispatch_time_phase().unwrap();

        assert_eq!(
            runtime.ctx.stats().eventloop_events_handled,
            handled_before + 2
        );
        assert_forkless_eviction_exhausted(runtime);
        eprintln!(
            "forkless eviction resumed: captured_victim=1, deleted=1, handled_timers=2, remaining_persistent=256, pending_timers=0"
        );
    }

    #[test]
    fn forkless_eviction_rejection_without_backpressure_remains_zero_delay() {
        let mut fixture = ForklessEvictionFixture::new();
        let runtime = &mut fixture.runtime;
        let origin = MonotonicInstant::now();
        assert_eq!(
            runtime.ctx.persistence().forkless_capture_poll_delay(),
            Some(Duration::ZERO)
        );
        assert!(runtime.perform_evictions().evicted.is_empty());
        assert_eq!(runtime.ctx.db(1).unwrap().dbsize(), 1);
        assert_eq!(
            runtime.time_events.next_timeout(MonotonicInstant::now()),
            Some(Duration::ZERO)
        );
        collect_one_eviction_timer(runtime, origin);

        fixture.capture_victim();
        let runtime = &mut fixture.runtime;
        let handled_before = runtime.ctx.stats().eventloop_events_handled;
        runtime.dispatch_time_phase().unwrap();

        assert_eq!(
            runtime.ctx.stats().eventloop_events_handled,
            handled_before + 1
        );
        assert_forkless_eviction_exhausted(runtime);
        eprintln!(
            "forkless eviction without backpressure: initial_delay=0, captured_victim=1, deleted=1, handled_timers=1, pending_timers=0"
        );
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
    fn memory_pressure_releases_idle_reply_buffers_before_evicting_keys() {
        const MARKER: &str = "VALKEY_TEST_REPLY_POOL_PRESSURE";
        if std::env::var_os(MARKER).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::server::worker::maintenance::eviction::tests::memory_pressure_releases_idle_reply_buffers_before_evicting_keys",
                ])
                .env(MARKER, "1")
                .status()
                .expect("run reply buffer pressure test in isolation");
            assert_eq!(status.code(), Some(86));
            return;
        }
        for policy in ["allkeys-random", "noeviction"] {
            let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
            runtime.ctx.config_mut().server.maxmemory_policy = policy.to_string();
            runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
            for index in 0..64 {
                runtime.ctx.db_mut(0).unwrap().set(
                    format!("key-{index}").into_bytes(),
                    Robj::string_raw(Bytes::from(vec![b'x'; 1_024])),
                );
                runtime.reply_buffer_pool.push(Vec::with_capacity(1_024));
            }
            let unlimited = runtime.perform_evictions();
            assert!(unlimited.evicted.is_empty());
            assert_eq!(runtime.reply_buffer_pool.len(), 64);

            let before = runtime.ctx.maxmemory_state().logical;
            runtime.ctx.config_mut().server.maxmemory = before.saturating_sub(40 * 1_024) as u64;
            let outcome = runtime.perform_evictions();

            assert!(
                outcome.evicted.is_empty(),
                "idle reply allocations must be released before deleting dataset keys"
            );
            assert_eq!(runtime.ctx.db(0).unwrap().dbsize(), 64);
            assert!(runtime.reply_buffer_pool.is_empty());
            assert!(!runtime.ctx.maxmemory_state().is_over_limit());
        }
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
            crate::runtime::server::worker::event_loop::TimerEvent::EvictionContinuation
        ));
    }

    #[test]
    fn timer_eviction_defers_when_command_eviction_ran_in_the_same_turn() {
        let mut runtime = crate::runtime::server::worker::ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().server.maxmemory_policy = "allkeys-random".to_string();
        runtime.ctx.config_mut().server.maxmemory_eviction_tenacity = 100;
        for index in 0..128 {
            runtime.ctx.db_mut(0).unwrap().set(
                format!("key-{index}").into_bytes(),
                Robj::string_raw(Bytes::from(vec![b'x'; 4_096])),
            );
        }
        let initial = runtime.ctx.db(0).unwrap().dbsize();
        let before = runtime.ctx.maxmemory_state().logical;
        // Leave enough headroom that several eviction cycles are required.
        runtime.ctx.config_mut().server.maxmemory = (before / 2) as u64;

        // Simulate the command path evicting keys during the Ready phase.
        let first = runtime.perform_evictions();
        assert!(!first.evicted.is_empty());
        runtime.command_eviction_ran_this_turn = true;
        let after_command_cycle = runtime.ctx.db(0).unwrap().dbsize();
        assert!(after_command_cycle < initial);

        // The timer continuation in the same turn must not evict again before
        // the before-poll phase flushes tracking invalidation output.
        runtime.run_eviction();
        assert_eq!(
            runtime.ctx.db(0).unwrap().dbsize(),
            after_command_cycle,
            "timer eviction must be deferred to the next turn"
        );
        assert!(runtime.time_events.has_due_kind(
            crate::runtime::server::worker::event_loop::TimerEvent::EvictionContinuation
        ));

        // A new turn resets the marker. The continuation is no longer blocked
        // by the same-turn guard; it runs to completion (or no-ops if the
        // first cycle already reached the target) without extra deferral.
        runtime.command_eviction_ran_this_turn = false;
        let before_resume = runtime.ctx.db(0).unwrap().dbsize();
        runtime.run_eviction();
        assert!(runtime.ctx.db(0).unwrap().dbsize() <= before_resume);
    }
}
