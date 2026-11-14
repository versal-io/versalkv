use std::collections::HashMap;

use crate::engine::extension::script::{CompiledFunction, ScriptEngine, ScriptError};
use crate::foundation::memory::metrics::memory_stats::{self, MemoryCategory};

pub struct EngineManager {
    engines: HashMap<&'static str, &'static dyn ScriptEngine>,
}

impl EngineManager {
    pub fn new(lua_engine: &'static dyn ScriptEngine) -> Self {
        let mut engines = HashMap::new();
        engines.insert(lua_engine.name(), lua_engine);
        Self { engines }
    }

    pub fn get(&self, name: &str) -> Option<&'static dyn ScriptEngine> {
        let upper = name.to_uppercase();
        self.engines.get(upper.as_str()).copied()
    }

    pub fn engines(&self) -> impl Iterator<Item = &'static dyn ScriptEngine> + '_ {
        // Construction currently installs exactly one Lua engine and there is no registration API.
        // Keep iteration here so registry operations remain independent of that storage detail.
        self.engines.values().copied()
    }
}

pub struct FunctionLib {
    pub name: String,
    pub engine: &'static dyn ScriptEngine,
    pub functions: Vec<CompiledFunction>,
    pub code: String,
}

pub struct FunctionRegistry {
    libraries: HashMap<String, FunctionLib>,
    functions: HashMap<String, CompiledFunction>,
    engine_mgr: EngineManager,
    memory_usage: usize,
}

impl Drop for FunctionRegistry {
    fn drop(&mut self) {
        let freed = std::mem::take(&mut self.memory_usage);
        if freed > 0 {
            memory_stats::track_sub(MemoryCategory::FunctionsVm, freed);
        }
    }
}

fn function_key(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn library_memory_usage(lib: &FunctionLib) -> usize {
    lib.name.len()
        + lib.code.len()
        + lib
            .functions
            .iter()
            .map(|function| function.name.len() + function.code.len())
            .sum::<usize>()
}

impl FunctionRegistry {
    pub fn new(lua_engine: &'static dyn ScriptEngine) -> Self {
        Self {
            libraries: HashMap::new(),
            functions: HashMap::new(),
            engine_mgr: EngineManager::new(lua_engine),
            memory_usage: 0,
        }
    }

    pub fn engine_mgr(&self) -> &EngineManager {
        &self.engine_mgr
    }

    pub fn load_library(&mut self, lib_name: &str, code: &str) -> Result<(), ScriptError> {
        if self.libraries.contains_key(lib_name) {
            return Err(ScriptError::Compile(format!(
                "Library {} already exists",
                lib_name
            )));
        }

        let engine_name = parse_engine_name(code);
        let engine = self
            .engine_mgr
            .get(&engine_name)
            .ok_or_else(|| ScriptError::Compile(format!("Unknown engine: {}", engine_name)))?;

        let flags = 0; //  shebang
        let compiled = engine.compile(code, flags)?;

        for func in &compiled {
            let key = function_key(&func.name);
            if self.functions.contains_key(&key) {
                return Err(ScriptError::Compile(format!(
                    "Function {} already exists",
                    func.name
                )));
            }
        }

        let lib = FunctionLib {
            name: lib_name.to_string(),
            engine,
            functions: compiled.clone(),
            code: code.to_string(),
        };

        let mut lib_memory = lib_name.len() + code.len();
        for func in &lib.functions {
            let key = function_key(&func.name);
            lib_memory += func.name.len() + func.code.len();
            self.functions.insert(key, func.clone());
        }
        self.memory_usage += lib_memory;
        memory_stats::track_add(MemoryCategory::FunctionsVm, lib_memory);
        self.libraries.insert(lib_name.to_string(), lib);

        engine.commit_compile(code, flags)?;

        Ok(())
    }

    /// Drop engine callbacks staged by a successful compilation when the
    /// metadata-level load is rejected afterwards. The active function table
    /// and the registered libraries are left untouched.
    pub fn abort_compile(&mut self) {
        for engine in self.engine_mgr.engines() {
            engine.discard_pending_compile();
        }
    }

    pub fn delete_library(&mut self, lib_name: &str) -> Result<bool, ScriptError> {
        let Some(lib) = self.libraries.get(lib_name) else {
            return Ok(false);
        };
        lib.engine.remove_functions(&lib.functions)?;
        Ok(self.take_library(lib_name).is_some())
    }

    pub fn take_library(&mut self, lib_name: &str) -> Option<FunctionLib> {
        if let Some(lib) = self.libraries.remove(lib_name) {
            for func in &lib.functions {
                self.functions.remove(&function_key(&func.name));
            }
            let freed = library_memory_usage(&lib);
            self.memory_usage = self.memory_usage.saturating_sub(freed);
            memory_stats::track_sub(MemoryCategory::FunctionsVm, freed);
            Some(lib)
        } else {
            None
        }
    }

    pub fn finish_library_replacement(&self, replaced: &FunctionLib) -> Result<(), ScriptError> {
        let stale = replaced
            .functions
            .iter()
            .filter(|function| {
                self.functions
                    .get(&function_key(&function.name))
                    .is_none_or(|current| current.name != function.name)
            })
            .cloned()
            .collect::<Vec<_>>();
        replaced.engine.remove_functions(&stale)
    }

    pub fn take_all_libraries(&mut self) -> Vec<FunctionLib> {
        let names = self.libraries.keys().cloned().collect::<Vec<_>>();
        names
            .into_iter()
            .filter_map(|name| self.take_library(&name))
            .collect()
    }

    pub fn begin_restore_transaction(&self) -> Result<(), ScriptError> {
        let mut started: Vec<&'static dyn ScriptEngine> = Vec::new();
        for engine in self.engine_mgr.engines() {
            if let Err(error) = engine.begin_function_restore_transaction() {
                let mut cleanup_errors = Vec::new();
                if let Err(cleanup_error) = engine.rollback_function_restore_transaction() {
                    cleanup_errors.push((engine.name(), cleanup_error));
                }
                for started_engine in started.into_iter().rev() {
                    if let Err(cleanup_error) =
                        started_engine.rollback_function_restore_transaction()
                    {
                        cleanup_errors.push((started_engine.name(), cleanup_error));
                    }
                }
                if !cleanup_errors.is_empty() {
                    let cleanup_errors = cleanup_errors
                        .into_iter()
                        .map(|(name, error)| format!("{name}: {error}"))
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(ScriptError::Runtime(format!(
                        "{error}; function restore cleanup failed: {cleanup_errors}"
                    )));
                }
                return Err(error);
            }
            started.push(engine);
        }
        Ok(())
    }

    pub fn commit_restore_transaction(&self) -> Result<(), ScriptError> {
        for engine in self.engine_mgr.engines() {
            engine.commit_function_restore_transaction()?;
        }
        Ok(())
    }

    pub fn rollback_restore_transaction(&self) -> Result<(), ScriptError> {
        let engines = self.engine_mgr.engines().collect::<Vec<_>>();
        let mut errors = Vec::new();
        for engine in engines.into_iter().rev() {
            if let Err(error) = engine.rollback_function_restore_transaction() {
                errors.push((engine.name(), error));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            let errors = errors
                .into_iter()
                .map(|(name, error)| format!("{name}: {error}"))
                .collect::<Vec<_>>()
                .join("; ");
            Err(ScriptError::Runtime(format!(
                "Function restore rollback failed: {errors}"
            )))
        }
    }

    pub fn restore_library(&mut self, lib: FunctionLib) {
        let lib_name = lib.name.clone();
        for func in &lib.functions {
            self.functions
                .insert(function_key(&func.name), func.clone());
        }
        let restored = library_memory_usage(&lib);
        self.memory_usage += restored;
        memory_stats::track_add(MemoryCategory::FunctionsVm, restored);
        self.libraries.insert(lib_name, lib);
    }

    pub fn find_function(&self, name: &str) -> Option<&CompiledFunction> {
        self.functions.get(&function_key(name))
    }

    pub fn function_engine(&self, func: &CompiledFunction) -> Option<&'static dyn ScriptEngine> {
        self.engine_mgr.get(func.engine)
    }

    pub fn flush(&mut self) {
        let freed = self.memory_usage;
        self.libraries.clear();
        self.functions.clear();
        self.memory_usage = 0;
        if freed > 0 {
            memory_stats::track_sub(MemoryCategory::FunctionsVm, freed);
        }
        for engine in self.engine_mgr.engines() {
            engine.clear_functions();
        }
    }

    pub fn list_libraries(&self) -> Vec<&FunctionLib> {
        self.libraries.values().collect()
    }

    pub fn get_library(&self, name: &str) -> Option<&FunctionLib> {
        self.libraries.get(name)
    }

    pub fn function_count(&self) -> usize {
        self.functions.len()
    }

    pub fn library_count(&self) -> usize {
        self.libraries.len()
    }

    pub fn dump(&self) -> Vec<u8> {
        let mut data = Vec::new();
        for lib in self.libraries.values() {
            data.extend_from_slice(lib.name.as_bytes());
            data.push(b'\n');
            data.extend_from_slice(lib.code.len().to_string().as_bytes());
            data.push(b'\n');
            data.extend_from_slice(lib.code.as_bytes());
            data.push(b'\n');
        }
        data
    }

    pub fn restore(&mut self, data: &[u8]) -> Result<usize, ScriptError> {
        let text = data;
        let mut pos = 0;
        let mut count = 0;

        while pos < text.len() {
            if text[pos] == b'\n' {
                pos += 1;
                continue;
            }

            let name_end = text[pos..]
                .iter()
                .position(|&b| b == b'\n')
                .ok_or_else(|| ScriptError::Restore("Invalid dump format: no name".to_string()))?;
            let name = String::from_utf8_lossy(&text[pos..pos + name_end]).to_string();
            pos += name_end + 1;

            if pos >= text.len() {
                return Err(ScriptError::Restore(
                    "Invalid dump format: no length".to_string(),
                ));
            }

            let len_end = text[pos..]
                .iter()
                .position(|&b| b == b'\n')
                .ok_or_else(|| {
                    ScriptError::Restore("Invalid dump format: no length".to_string())
                })?;
            let len_str = String::from_utf8_lossy(&text[pos..pos + len_end]).to_string();
            let code_len: usize = len_str
                .parse()
                .map_err(|_| ScriptError::Restore("Invalid dump format: bad length".to_string()))?;
            pos += len_end + 1;

            let code_end = pos
                .checked_add(code_len)
                .filter(|&end| end <= text.len())
                .ok_or_else(|| {
                    ScriptError::Restore("Invalid dump format: truncated code".to_string())
                })?;

            let code = std::str::from_utf8(&text[pos..code_end])
                .map_err(|_| {
                    ScriptError::Restore("Invalid dump format: code is not UTF-8".to_string())
                })?
                .to_string();
            pos = code_end;

            if pos < text.len() && text[pos] == b'\n' {
                pos += 1;
            }

            self.load_library(&name, &code)?;
            count += 1;
        }

        Ok(count)
    }
}

pub(crate) fn parse_engine_name(code: &str) -> String {
    if let Some(rest) = code.strip_prefix("#!") {
        if let Some(end) = rest.find(|c: char| c.is_whitespace() || c == '\n') {
            return rest[..end].to_string();
        }
        return rest.to_string();
    }
    "LUA".to_string()
}

#[cfg(test)]
mod transaction_tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::engine::extension::script::ScriptRunCtx;
    use crate::engine::host::HostDispatcher;
    use crate::foundation::io::protocol::Value;

    struct FakeEngine {
        name: &'static str,
        begin_fails: AtomicBool,
        commit_fails: AtomicBool,
        rollback_fails: AtomicBool,
        remove_fails: AtomicBool,
        events: Arc<Mutex<Vec<String>>>,
    }

    impl FakeEngine {
        fn new(name: &'static str, events: Arc<Mutex<Vec<String>>>) -> &'static Self {
            Box::leak(Box::new(Self {
                name,
                begin_fails: AtomicBool::new(false),
                commit_fails: AtomicBool::new(false),
                rollback_fails: AtomicBool::new(false),
                remove_fails: AtomicBool::new(false),
                events,
            }))
        }

        fn record(&self, operation: &str) {
            self.events
                .lock()
                .unwrap()
                .push(format!("{operation}:{}", self.name));
        }
    }

    impl ScriptEngine for FakeEngine {
        fn name(&self) -> &'static str {
            self.name
        }

        fn eval(
            &self,
            _code: &str,
            _ctx: &mut ScriptRunCtx,
            _dispatcher: &mut HostDispatcher<'_>,
        ) -> Result<Value, ScriptError> {
            unreachable!("fake restore transaction engine does not evaluate scripts")
        }

        fn begin_function_restore_transaction(&self) -> Result<(), ScriptError> {
            self.record("begin");
            if self.begin_fails.load(Ordering::Relaxed) {
                Err(ScriptError::Runtime(format!("{} begin failure", self.name)))
            } else {
                Ok(())
            }
        }

        fn commit_function_restore_transaction(&self) -> Result<(), ScriptError> {
            self.record("commit");
            if self.commit_fails.load(Ordering::Relaxed) {
                Err(ScriptError::Runtime(format!(
                    "{} commit failure",
                    self.name
                )))
            } else {
                Ok(())
            }
        }

        fn rollback_function_restore_transaction(&self) -> Result<(), ScriptError> {
            self.record("rollback");
            if self.rollback_fails.load(Ordering::Relaxed) {
                Err(ScriptError::Runtime(format!(
                    "{} rollback failure",
                    self.name
                )))
            } else {
                Ok(())
            }
        }

        fn remove_functions(&self, _functions: &[CompiledFunction]) -> Result<(), ScriptError> {
            self.record("remove");
            if self.remove_fails.load(Ordering::Relaxed) {
                Err(ScriptError::Runtime(format!(
                    "{} remove failure",
                    self.name
                )))
            } else {
                Ok(())
            }
        }

        fn call(
            &self,
            _func: &CompiledFunction,
            _ctx: &mut ScriptRunCtx,
            _keys: &[bytes::Bytes],
            _args: &[bytes::Bytes],
            _dispatcher: &mut HostDispatcher<'_>,
        ) -> Result<Value, ScriptError> {
            unreachable!("fake restore transaction engine does not call functions")
        }
    }

    fn registry_with_library(
        engine: &'static FakeEngine,
        lib_name: &str,
        function_name: &str,
    ) -> FunctionRegistry {
        let function = CompiledFunction {
            engine: engine.name(),
            name: function_name.to_string(),
            code: String::new(),
            description: String::new(),
            flags: 0,
        };
        let lib = FunctionLib {
            name: lib_name.to_string(),
            engine,
            functions: vec![function.clone()],
            code: String::new(),
        };
        let memory_usage = library_memory_usage(&lib);
        memory_stats::track_add(MemoryCategory::FunctionsVm, memory_usage);
        FunctionRegistry {
            libraries: HashMap::from([(lib_name.to_string(), lib)]),
            functions: HashMap::from([(function_key(function_name), function)]),
            engine_mgr: EngineManager {
                engines: HashMap::from([(engine.name(), engine as &'static dyn ScriptEngine)]),
            },
            memory_usage,
        }
    }

    #[test]
    fn delete_library_propagates_callback_removal_error_without_removing_metadata() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let engine = FakeEngine::new("FAKE", Arc::clone(&events));
        engine.remove_fails.store(true, Ordering::Relaxed);
        let mut registry = registry_with_library(engine, "library", "function");
        let memory_usage = registry.memory_usage;

        let error = registry.delete_library("library").unwrap_err();

        assert!(error.to_string().contains("FAKE remove failure"));
        assert!(registry.get_library("library").is_some());
        assert!(registry.find_function("function").is_some());
        assert_eq!(registry.memory_usage, memory_usage);
        assert_eq!(*events.lock().unwrap(), vec!["remove:FAKE"]);
    }

    #[test]
    fn finish_library_replacement_borrows_old_library_and_propagates_removal_error() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let engine = FakeEngine::new("FAKE", Arc::clone(&events));
        engine.remove_fails.store(true, Ordering::Relaxed);
        let mut old_registry = registry_with_library(engine, "library", "stale");
        let old = old_registry.take_library("library").unwrap();
        let registry = FunctionRegistry {
            libraries: HashMap::new(),
            functions: HashMap::new(),
            engine_mgr: EngineManager {
                engines: HashMap::from([(engine.name(), engine as &'static dyn ScriptEngine)]),
            },
            memory_usage: 0,
        };

        let error = registry.finish_library_replacement(&old).unwrap_err();

        assert!(error.to_string().contains("FAKE remove failure"));
        assert_eq!(old.name, "library");
        assert_eq!(*events.lock().unwrap(), vec!["remove:FAKE"]);
    }

    #[test]
    fn function_restore_begin_failure_rolls_back_current_and_started_engines() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let first = FakeEngine::new("FIRST", Arc::clone(&events));
        let second = FakeEngine::new("SECOND", Arc::clone(&events));
        let third = FakeEngine::new("THIRD", Arc::clone(&events));
        let engines = HashMap::from([
            (first.name(), first as &'static dyn ScriptEngine),
            (second.name(), second as &'static dyn ScriptEngine),
            (third.name(), third as &'static dyn ScriptEngine),
        ]);
        let registry = FunctionRegistry {
            libraries: HashMap::new(),
            functions: HashMap::new(),
            engine_mgr: EngineManager { engines },
            memory_usage: 0,
        };
        let begin_order = registry
            .engine_mgr
            .engines()
            .map(ScriptEngine::name)
            .collect::<Vec<_>>();
        let failing_name = *begin_order.last().unwrap();
        [first, second, third]
            .into_iter()
            .find(|engine| engine.name == failing_name)
            .unwrap()
            .begin_fails
            .store(true, Ordering::Relaxed);
        [first, second, third]
            .into_iter()
            .find(|engine| engine.name == failing_name)
            .unwrap()
            .rollback_fails
            .store(true, Ordering::Relaxed);

        let result = registry.begin_restore_transaction();

        let error = result.unwrap_err().to_string();
        assert!(error.contains(&format!("{failing_name} begin failure")));
        assert!(error.contains(&format!("{failing_name} rollback failure")));
        let mut expected = begin_order
            .iter()
            .map(|name| format!("begin:{name}"))
            .collect::<Vec<_>>();
        expected.extend(
            begin_order
                .iter()
                .rev()
                .map(|name| format!("rollback:{name}")),
        );
        assert_eq!(*events.lock().unwrap(), expected);
    }

    #[test]
    fn function_restore_commit_propagates_engine_error() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let first = FakeEngine::new("FIRST", Arc::clone(&events));
        let second = FakeEngine::new("SECOND", Arc::clone(&events));
        let engines = HashMap::from([
            (first.name(), first as &'static dyn ScriptEngine),
            (second.name(), second as &'static dyn ScriptEngine),
        ]);
        let registry = FunctionRegistry {
            libraries: HashMap::new(),
            functions: HashMap::new(),
            engine_mgr: EngineManager { engines },
            memory_usage: 0,
        };
        let first_engine_name = registry.engine_mgr.engines().next().unwrap().name();
        [first, second]
            .into_iter()
            .find(|engine| engine.name == first_engine_name)
            .unwrap()
            .commit_fails
            .store(true, Ordering::Relaxed);

        let error = registry.commit_restore_transaction().unwrap_err();

        assert!(error
            .to_string()
            .contains(&format!("{first_engine_name} commit failure")));
        assert_eq!(
            *events.lock().unwrap(),
            vec![format!("commit:{first_engine_name}")]
        );
    }

    #[test]
    fn function_restore_rollback_attempts_all_engines_and_reports_errors() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let first = FakeEngine::new("FIRST", Arc::clone(&events));
        let second = FakeEngine::new("SECOND", Arc::clone(&events));
        let engines = HashMap::from([
            (first.name(), first as &'static dyn ScriptEngine),
            (second.name(), second as &'static dyn ScriptEngine),
        ]);
        let registry = FunctionRegistry {
            libraries: HashMap::new(),
            functions: HashMap::new(),
            engine_mgr: EngineManager { engines },
            memory_usage: 0,
        };
        first.rollback_fails.store(true, Ordering::Relaxed);
        second.rollback_fails.store(true, Ordering::Relaxed);
        let mut rollback_order = registry
            .engine_mgr
            .engines()
            .map(ScriptEngine::name)
            .collect::<Vec<_>>();
        rollback_order.reverse();

        let error = registry.rollback_restore_transaction().unwrap_err();

        let error = error.to_string();
        assert!(error.contains("FIRST rollback failure"));
        assert!(error.contains("SECOND rollback failure"));
        let expected = rollback_order
            .into_iter()
            .map(|name| format!("rollback:{name}"))
            .collect::<Vec<_>>();
        assert_eq!(*events.lock().unwrap(), expected);
    }
}
