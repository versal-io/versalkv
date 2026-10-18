use crate::engine::host::{
    yield_rdb, HostAbort, HostControl, HostDispatcher, HostError, LoadProgress,
};
use crate::foundation::io::persistence::{
    LoadError, RdbDecodePolicy, RdbDecodedMetadata, RdbDecoder, RdbStep,
};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::framework::context::ServerContext;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DatasetVisibility {
    Visible,
}

#[derive(Debug)]
pub(crate) enum RdbTaskError {
    Load(LoadError),
    Host(HostError),
    Aborted(HostAbort),
}

impl From<LoadError> for RdbTaskError {
    fn from(error: LoadError) -> Self {
        Self::Load(error)
    }
}

impl From<HostError> for RdbTaskError {
    fn from(error: HostError) -> Self {
        Self::Host(error)
    }
}

pub(crate) struct RdbLoadTask {
    bytes: Vec<u8>,
    source_offset: usize,
    decoder: RdbDecoder,
    dataset: DbSet,
    visibility: DatasetVisibility,
    progress_interval_bytes: u64,
}

pub(crate) struct DebugRdbTask {
    load: RdbLoadTask,
    replace_functions: bool,
    rollback_dataset: Option<DbSet>,
}

impl DebugRdbTask {
    pub(crate) fn new(
        load: RdbLoadTask,
        replace_functions: bool,
        rollback_dataset: Option<DbSet>,
    ) -> Self {
        Self {
            load,
            replace_functions,
            rollback_dataset,
        }
    }

    pub(crate) fn run(
        &mut self,
        dispatcher: &mut HostDispatcher<'_>,
    ) -> Result<RdbDecodedMetadata, RdbTaskError> {
        self.load.run(dispatcher)
    }

    pub(crate) fn finish(
        self,
        result: Result<RdbDecodedMetadata, RdbTaskError>,
        ctx: &mut ServerContext,
    ) -> Value {
        let loaded_dataset = self.load.into_dataset();
        ctx.set_host_task_loading(false);
        match result {
            Ok(metadata) => {
                if self.replace_functions
                    && replace_function_libraries(ctx, &metadata.function_codes).is_err()
                {
                    if let Some(dataset) = self.rollback_dataset {
                        *ctx.dbs_mut() = dataset;
                    }
                    return debug_rdb_error(&LoadError::Corrupt);
                }
                *ctx.dbs_mut() = loaded_dataset;
                Value::ok()
            }
            Err(RdbTaskError::Load(error)) => {
                *ctx.dbs_mut() = self.rollback_dataset.unwrap_or(loaded_dataset);
                debug_rdb_error(&error)
            }
            Err(RdbTaskError::Host(error)) => {
                *ctx.dbs_mut() = self.rollback_dataset.unwrap_or(loaded_dataset);
                crate::server_log!(warning, "DEBUG RELOAD host failure: {}", error);
                Value::error("ERR Error trying to load the RDB dump, check server logs.")
            }
            Err(RdbTaskError::Aborted(reason)) => {
                *ctx.dbs_mut() = self.rollback_dataset.unwrap_or(loaded_dataset);
                crate::server_log!(warning, "DEBUG RELOAD aborted: {:?}", reason);
                Value::error("ERR Error trying to load the RDB dump, check server logs.")
            }
        }
    }
}

pub(crate) fn replace_function_libraries(
    ctx: &mut ServerContext,
    codes: &[Vec<u8>],
) -> Result<(), ()> {
    let mut registry = crate::engine::extension::function::FunctionRegistry::new(
        &crate::engine::extension::lua::LUA_ENGINE,
    );
    for code in codes {
        let code = std::str::from_utf8(code).map_err(|_| ())?;
        let name =
            crate::engine::extension::lua::shebang::extract_shebang_name(code).map_err(|_| ())?;
        registry.load_library(&name, code).map_err(|_| ())?;
    }
    ctx.script_mut().function_registry = registry;
    Ok(())
}

fn debug_rdb_error(error: &LoadError) -> Value {
    if let LoadError::IncompatibleVersion(version) = error {
        crate::server_log!(warning, "Can't handle RDB format version {}", version);
    }
    crate::server_log!(warning, "DEBUG RELOAD failed to load RDB: {:?}", error);
    Value::error("ERR Error trying to load the RDB dump, check server logs.")
}

impl RdbLoadTask {
    pub(crate) fn new(
        bytes: Vec<u8>,
        dataset: DbSet,
        policy: RdbDecodePolicy,
        visibility: DatasetVisibility,
        progress_interval_bytes: u64,
    ) -> Self {
        Self {
            bytes,
            source_offset: 0,
            decoder: RdbDecoder::with_policy(policy),
            dataset,
            visibility,
            progress_interval_bytes,
        }
    }

    pub(crate) fn run(
        &mut self,
        dispatcher: &mut HostDispatcher<'_>,
    ) -> Result<RdbDecodedMetadata, RdbTaskError> {
        loop {
            match self.decoder.step(&mut self.dataset)? {
                RdbStep::Finished(metadata) => {
                    if metadata.empty_keys > 0 {
                        log::warn!("empty keys skipped: {}", metadata.empty_keys);
                    }
                    return Ok(metadata);
                }
                RdbStep::RecordApplied => continue,
                RdbStep::NeedInput => {}
            }

            if self.source_offset == self.bytes.len() {
                return Err(RdbTaskError::Load(LoadError::Corrupt));
            }

            let end = self.next_feed_end();
            self.decoder.feed(&self.bytes[self.source_offset..end]);
            self.source_offset = end;
            if self.should_yield() {
                let progress = LoadProgress {
                    loaded_bytes: self.source_offset as u64,
                    total_bytes: Some(self.bytes.len() as u64),
                    loaded_keys: self.decoder.loaded(),
                };
                let visible_dataset = match self.visibility {
                    DatasetVisibility::Visible => Some(&mut self.dataset),
                };
                match yield_rdb(dispatcher, progress, visible_dataset)? {
                    HostControl::Continue => {}
                    HostControl::Abort(reason) => return Err(RdbTaskError::Aborted(reason)),
                }
            }
        }
    }

    fn next_feed_end(&self) -> usize {
        if self.progress_interval_bytes == 0 {
            return self.bytes.len();
        }
        let interval = self.progress_interval_bytes;
        let offset = self.source_offset as u64;
        let next_boundary = offset
            .checked_div(interval)
            .and_then(|bucket| bucket.checked_add(1))
            .and_then(|bucket| bucket.checked_mul(interval))
            .unwrap_or(u64::MAX);
        self.bytes
            .len()
            .min(usize::try_from(next_boundary).unwrap_or(usize::MAX))
    }

    fn should_yield(&self) -> bool {
        self.progress_interval_bytes != 0
            && self.source_offset != self.bytes.len()
            && (self.source_offset as u64).is_multiple_of(self.progress_interval_bytes)
    }

    pub(crate) fn into_dataset(self) -> DbSet {
        self.dataset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::host::{HostReply, HostRequest};
    use crate::foundation::io::persistence::{serialize_all_with_options, RdbSerializeOptions};
    use crate::foundation::io::store::Robj;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static RDB_YIELDS: AtomicUsize = AtomicUsize::new(0);

    fn count_rdb_yield(request: HostRequest<'_>) -> Result<HostReply, HostError> {
        match request {
            HostRequest::Yield(crate::engine::host::HostYield::RdbProgress {
                progress,
                visible_dataset,
            }) => {
                RDB_YIELDS.fetch_add(1, Ordering::Relaxed);
                assert!(progress.loaded_bytes > 0);
                assert!(visible_dataset.is_some());
                Ok(HostReply::Continue)
            }
            _ => Err(HostError::Invariant("unexpected RDB task request")),
        }
    }

    #[test]
    fn large_record_yields_at_input_boundaries_before_it_can_be_applied() {
        let mut source = DbSet::new(1);
        source
            .get_mut(0)
            .expect("DB 0 exists")
            .set(b"large".to_vec(), Robj::string_raw("x".repeat(8192)));
        let bytes = serialize_all_with_options(
            &source,
            0,
            RdbSerializeOptions {
                compression: false,
                checksum: true,
            },
        );
        let mut task = RdbLoadTask::new(
            bytes,
            DbSet::new(1),
            RdbDecodePolicy::persisted(true, 0),
            DatasetVisibility::Visible,
            1024,
        );
        RDB_YIELDS.store(0, Ordering::Relaxed);
        let mut dispatcher = count_rdb_yield;

        let metadata = task.run(&mut dispatcher).expect("load RDB");

        assert!(RDB_YIELDS.load(Ordering::Relaxed) >= 8);
        assert_eq!(metadata.loaded, 1);
        assert_eq!(
            task.dataset
                .get(0)
                .expect("DB 0 exists")
                .get_no_expire(b"large")
                .expect("large key")
                .as_bytes()
                .len(),
            8192
        );
    }
}
