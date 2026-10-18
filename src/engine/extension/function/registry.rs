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

fn function_key(name: &str) -> String {
    name.to_ascii_lowercase()
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

    pub fn delete_library(&mut self, lib_name: &str) -> bool {
        if let Some(lib) = self.take_library(lib_name) {
            let mut freed = lib_name.len() + lib.code.len();
            for func in &lib.functions {
                freed += func.name.len() + func.code.len();
            }
            self.memory_usage = self.memory_usage.saturating_sub(freed);
            memory_stats::track_sub(MemoryCategory::FunctionsVm, freed);
            true
        } else {
            false
        }
    }

    pub fn take_library(&mut self, lib_name: &str) -> Option<FunctionLib> {
        if let Some(lib) = self.libraries.remove(lib_name) {
            for func in &lib.functions {
                self.functions.remove(&function_key(&func.name));
            }
            Some(lib)
        } else {
            None
        }
    }

    pub fn restore_library(&mut self, lib: FunctionLib) {
        let lib_name = lib.name.clone();
        for func in &lib.functions {
            self.functions
                .insert(function_key(&func.name), func.clone());
        }
        let lib_memory = lib_name.len() + lib.code.len();
        let mut funcs_memory = 0;
        for func in &lib.functions {
            funcs_memory += func.name.len() + func.code.len();
        }
        self.memory_usage += lib_memory + funcs_memory;
        memory_stats::track_add(MemoryCategory::FunctionsVm, lib_memory + funcs_memory);
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
                break;
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

            if pos + code_len > text.len() {
                break;
            }

            let code = String::from_utf8_lossy(&text[pos..pos + code_len]).to_string();
            pos += code_len;

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
