use crate::engine::extension::function::FunctionLib;
use crate::engine::host::{
    yield_rdb, FunctionLoadPolicy, HostAbort, HostControl, HostDispatcher, HostError, LoadProgress,
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
    skip_codec_checksums: bool,
    compressed_physical_bytes: Option<usize>,
    functions_applied: bool,
    function_policy: FunctionLoadPolicy,
}

pub(crate) struct DebugRdbTask {
    load: RdbLoadTask,
    rollback_dataset: Option<DbSet>,
    rollback_functions: Option<Vec<FunctionLib>>,
}

impl DebugRdbTask {
    pub(crate) fn new(
        mut load: RdbLoadTask,
        replace_functions: bool,
        rollback_dataset: Option<DbSet>,
    ) -> Self {
        load.function_policy = if replace_functions {
            FunctionLoadPolicy::Replace
        } else {
            FunctionLoadPolicy::Merge
        };
        Self {
            load,
            rollback_dataset,
            rollback_functions: None,
        }
    }

    pub(crate) fn begin_function_rollback(&mut self, ctx: &ServerContext) -> Result<(), HostError> {
        if self.rollback_functions.is_some() {
            return Err(HostError::Invariant(
                "DEBUG function rollback is already active",
            ));
        }
        ctx.script()
            .function_registry
            .begin_restore_transaction()
            .map_err(|error| HostError::Runtime(error.to_string()))?;
        self.rollback_functions = Some(
            ctx.script()
                .function_registry
                .list_libraries()
                .into_iter()
                .map(clone_function_library)
                .collect(),
        );
        Ok(())
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
        let function_policy = self.load.function_policy;
        let loaded_dataset = self.load.into_dataset();
        ctx.set_host_task_loading(false);
        let mut result = result.and_then(|metadata| {
            if !metadata.functions_applied {
                RdbLoadTask::install_function_libraries(
                    ctx,
                    &metadata.function_codes,
                    function_policy,
                )
                .map_err(|()| RdbTaskError::Load(LoadError::Corrupt))?;
            }
            if self.rollback_functions.is_some() {
                ctx.script()
                    .function_registry
                    .commit_restore_transaction()
                    .map_err(|error| RdbTaskError::Host(HostError::Runtime(error.to_string())))?;
            }
            Ok(())
        });
        if result.is_err() {
            if let Some(libraries) = self.rollback_functions {
                if let Err(error) = ctx
                    .script()
                    .function_registry
                    .rollback_restore_transaction()
                {
                    result = Err(RdbTaskError::Host(HostError::Runtime(error.to_string())));
                } else {
                    drop(ctx.script_mut().function_registry.take_all_libraries());
                    for library in libraries {
                        ctx.script_mut().function_registry.restore_library(library);
                    }
                }
            }
        }
        match result {
            Ok(()) => {
                *ctx.dbs_mut() = loaded_dataset;
                ctx.set_suppress_eviction_until_write(true);
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

fn clone_function_library(library: &FunctionLib) -> FunctionLib {
    FunctionLib {
        name: library.name.clone(),
        engine: library.engine,
        functions: library.functions.clone(),
        code: library.code.clone(),
    }
}

pub(crate) fn replace_function_libraries(
    ctx: &mut ServerContext,
    codes: &[Vec<u8>],
) -> Result<(), ()> {
    RdbLoadTask::install_function_libraries(ctx, codes, FunctionLoadPolicy::Replace)
}

impl RdbLoadTask {
    pub(crate) fn install_function_libraries(
        ctx: &mut ServerContext,
        codes: &[Vec<u8>],
        policy: FunctionLoadPolicy,
    ) -> Result<(), ()> {
        if policy == FunctionLoadPolicy::Merge && codes.is_empty() {
            return Ok(());
        }
        ctx.script()
            .function_registry
            .begin_restore_transaction()
            .map_err(|_| ())?;
        let old_libraries = ctx.script_mut().function_registry.take_all_libraries();
        if policy == FunctionLoadPolicy::Merge {
            for library in &old_libraries {
                ctx.script_mut()
                    .function_registry
                    .restore_library(clone_function_library(library));
            }
        }

        let replacement = (|| {
            let mut replaced_libraries = Vec::new();
            for code in codes {
                let code = std::str::from_utf8(code).map_err(|_| ())?;
                let name = crate::engine::extension::lua::shebang::extract_shebang_name(code)
                    .map_err(|_| ())?;
                if policy == FunctionLoadPolicy::Merge {
                    if let Some(library) = ctx.script_mut().function_registry.take_library(&name) {
                        replaced_libraries.push(library);
                    }
                }
                ctx.script_mut()
                    .function_registry
                    .load_library(&name, code)
                    .map_err(|_| ())?;
            }
            let removed_libraries = match policy {
                FunctionLoadPolicy::Replace => &old_libraries,
                FunctionLoadPolicy::Merge => &replaced_libraries,
            };
            for old_library in removed_libraries {
                ctx.script()
                    .function_registry
                    .finish_library_replacement(old_library)
                    .map_err(|_| ())?;
            }
            ctx.script()
                .function_registry
                .commit_restore_transaction()
                .map_err(|_| ())
        })();

        if replacement.is_ok() {
            drop(old_libraries);
            return Ok(());
        }

        drop(ctx.script_mut().function_registry.take_all_libraries());
        if ctx
            .script()
            .function_registry
            .rollback_restore_transaction()
            .is_ok()
        {
            for old_library in old_libraries {
                ctx.script_mut()
                    .function_registry
                    .restore_library(old_library);
            }
        }
        Err(())
    }
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
            skip_codec_checksums: policy.skip_checksum,
            compressed_physical_bytes: None,
            functions_applied: false,
            function_policy: FunctionLoadPolicy::Replace,
        }
    }

    pub(crate) fn with_skip_codec_checksums(mut self, skip: bool) -> Self {
        self.skip_codec_checksums = skip;
        if skip {
            self.decoder.skip_checksum();
        }
        self
    }

    pub(crate) fn run(
        &mut self,
        dispatcher: &mut HostDispatcher<'_>,
    ) -> Result<RdbDecodedMetadata, RdbTaskError> {
        if self.source_offset == 0 {
            let decoded = crate::foundation::io::persistence::rdb::compression::decode_rdb_stream(
                &self.bytes,
                self.skip_codec_checksums,
            )
            .map_err(|error| {
                crate::server_log!(warning, "{}", error.message());
                RdbTaskError::Load(LoadError::Corrupt)
            })?;
            if let Some(decoded) = decoded {
                crate::server_log!(
                    notice,
                    "Logical RDB CRC64 skipped for streaming-compressed input"
                );
                self.decoder.skip_checksum();
                self.compressed_physical_bytes = Some(decoded.physical_bytes);
                self.bytes = decoded.bytes;
            }
        }
        loop {
            match self.decoder.step(&mut self.dataset)? {
                RdbStep::Finished(mut metadata) => {
                    metadata.functions_applied = self.functions_applied;
                    if let Some(physical_bytes) = self.compressed_physical_bytes {
                        if metadata.consumed != self.bytes.len() {
                            return Err(RdbTaskError::Load(LoadError::Corrupt));
                        }
                        metadata.consumed = physical_bytes;
                    }
                    if metadata.empty_keys > 0 {
                        log::warn!("empty keys skipped: {}", metadata.empty_keys);
                    }
                    return Ok(metadata);
                }
                RdbStep::RecordApplied => continue,
                RdbStep::InlineCommand { db, args } => {
                    let args = crate::engine::command::CommandArgs::from_parts(args);
                    let value = crate::engine::host::dispatch_command(
                        dispatcher,
                        &args,
                        crate::engine::host::HostCommandOrigin::RdbReplay {
                            db,
                            dataset: &mut self.dataset,
                            function_codes: (!self.functions_applied)
                                .then(|| self.decoder.function_codes()),
                            function_policy: self.function_policy,
                        },
                    )?;
                    if let Value::Error(error) = value {
                        return Err(RdbTaskError::Host(HostError::Runtime(
                            String::from_utf8_lossy(&error).into_owned(),
                        )));
                    }
                    self.functions_applied = true;
                    continue;
                }
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
    use crate::engine::extension::script::ScriptRunCtx;
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

    fn call_function(ctx: &ServerContext, name: &str) -> Value {
        let compiled = ctx
            .script()
            .function_registry
            .find_function(name)
            .expect("function exists")
            .clone();
        let engine = ctx
            .script()
            .function_registry
            .function_engine(&compiled)
            .expect("function engine exists");
        let mut run = ScriptRunCtx::new_bare_for_test(compiled.flags);
        let mut host = count_rdb_yield;
        engine
            .call(&compiled, &mut run, &[], &[], &mut host)
            .expect("function call succeeds")
    }

    #[test]
    fn replace_function_libraries_rolls_back_callbacks_after_later_compile_error() {
        use crate::engine::extension::lua::reset_lua_runtime;
        use crate::foundation::memory::metrics::memory_stats::{self, MemoryCategory};

        let guard = memory_stats::lock_global_for_test();
        let tracker = memory_stats::get_tracker();
        let baseline = tracker.get(MemoryCategory::FunctionsVm);
        let old = "#!lua name=stateful\nlocal counter = 0\nserver.register_function('counter', function() counter = counter + 1 return counter end)";
        let replacement =
            "#!lua name=stateful\nserver.register_function('counter', function() return 100 end)";
        let invalid = "#!lua name=bad\nerror('boom')";

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reset_lua_runtime();
            {
                let mut fixture_ctx = ServerContext::new(1);
                fixture_ctx
                    .script_mut()
                    .function_registry
                    .load_library("stateful", replacement)
                    .unwrap();
                assert_eq!(call_function(&fixture_ctx, "counter"), Value::Integer(100));
                fixture_ctx.script_mut().function_registry.flush();
            }
            reset_lua_runtime();

            let mut ctx = ServerContext::new(1);
            ctx.script_mut()
                .function_registry
                .load_library("stateful", old)
                .unwrap();
            assert_eq!(call_function(&ctx, "counter"), Value::Integer(1));

            let result = replace_function_libraries(
                &mut ctx,
                &[replacement.as_bytes().to_vec(), invalid.as_bytes().to_vec()],
            );
            let callback_after_failure = call_function(&ctx, "counter");
            ctx.script_mut().function_registry.flush();

            (result, callback_after_failure)
        }));

        let current = tracker.get(MemoryCategory::FunctionsVm);
        if current > baseline {
            memory_stats::track_sub(MemoryCategory::FunctionsVm, current - baseline);
        } else if current < baseline {
            memory_stats::track_add(MemoryCategory::FunctionsVm, baseline - current);
        }
        reset_lua_runtime();
        drop(guard);

        match outcome {
            Ok((result, callback_after_failure)) => {
                assert!(result.is_err());
                assert_eq!(callback_after_failure, Value::Integer(2));
            }
            Err(payload) => std::panic::resume_unwind(payload),
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

    #[test]
    fn codec_fix_checksum_skip_preserves_rdb_structure_validation() {
        use crate::foundation::io::persistence::rdb::compression::{
            decode_rdb_stream, encode_rdb_stream,
        };
        let logical = serialize_all_with_options(
            &DbSet::new(1),
            0,
            RdbSerializeOptions {
                compression: false,
                checksum: true,
            },
        );
        let wire = encode_rdb_stream(&logical, true).unwrap();
        let mut checksum_only = wire.clone();
        *checksum_only.last_mut().unwrap() ^= 1;
        let mut truncated = wire.clone();
        truncated.pop();
        let mut envelope = wire.clone();
        envelope[5] = 1;
        let mut invalid_eof = decode_rdb_stream(&wire, false).unwrap().unwrap().bytes;
        let eof = invalid_eof.len() - 9;
        invalid_eof[eof] = 0;
        let invalid_eof = encode_rdb_stream(&invalid_eof, true).unwrap();
        let mut extra = logical.clone();
        extra.push(b'X');
        let extra = encode_rdb_stream(&extra, true).unwrap();
        for (bytes, should_load) in [
            (checksum_only, true),
            (truncated, false),
            (envelope, false),
            (invalid_eof, false),
            (extra, false),
        ] {
            let mut task = RdbLoadTask::new(
                bytes,
                DbSet::new(1),
                RdbDecodePolicy::persisted(true, 0),
                DatasetVisibility::Visible,
                0,
            )
            .with_skip_codec_checksums(true);
            assert_eq!(task.run(&mut count_rdb_yield).is_ok(), should_load);
        }
    }
}
