use super::*;
use crate::engine::extension::module::loader::ModuleLoader;
use crate::engine::extension::module::ModuleRegistry;
use std::ffi::CString;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture {
    directory: PathBuf,
    library: PathBuf,
}

impl Fixture {
    fn new(should_export: bool, should_fail: bool) -> Self {
        Self::from_source(&format!(
            r#"
typedef int (*GetApi)(const char *, void **);
typedef void (*SetAttribs)(void *, const char *, int, int);
typedef struct {{ void *getapi; void *internal; }} ModuleCtx;

int ValkeyModule_OnLoad(void *opaque, void **argv, int argc) {{
    (void)argv;
    (void)argc;
    ModuleCtx *ctx = opaque;
    GetApi getapi = (GetApi)ctx->getapi;
    void *address = 0;
    if ({should_export} &&
        getapi("ValkeyModule_GetThreadSafeContext", &address) != 0) return 1;
    if ({should_fail}) return 1;
    if (getapi("ValkeyModule_SetModuleAttribs", &address) != 0) return 1;
    ((SetAttribs)address)(opaque, "native-export", 1, 1);
    return 0;
}}
"#,
            should_export = i32::from(should_export),
            should_fail = i32::from(should_fail),
        ))
    }

    fn from_source(contents: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "versal-native-export-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let source = directory.join("module.c");
        let library = directory.join("module.so");
        std::fs::write(&source, contents).unwrap();
        let mut compiler = Command::new(std::env::var_os("CC").unwrap_or_else(|| "clang".into()));
        compiler.args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"]);
        if cfg!(target_os = "linux") {
            compiler.arg("-fuse-ld=lld");
        }
        let output = compiler
            .arg(&source)
            .arg("-o")
            .arg(&library)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "native fixture compilation failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Self { directory, library }
    }

    fn is_mapped(&self) -> bool {
        let path = CString::new(self.library.to_str().unwrap()).unwrap();
        // SAFETY: the path is a live C string. NOLOAD probes an existing mapping
        // and a successful probe reference is closed before this function returns.
        unsafe {
            let handle = libc::dlopen(path.as_ptr(), libc::RTLD_NOW | libc::RTLD_NOLOAD);
            if handle.is_null() {
                false
            } else {
                assert_eq!(libc::dlclose(handle), 0);
                true
            }
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn load_and_retire(fixture: &Fixture) {
    let mut registry = ModuleRegistry::new();
    let name = ModuleLoader::load_dynamic(
        fixture.library.to_str().unwrap(),
        &[],
        &mut registry,
        std::ptr::null_mut(),
    )
    .unwrap();
    assert_eq!(name, "native-export");
    assert!(fixture.is_mapped());
    // No context has been constructed: this reproduces the cached false
    // decision before a native thread first uses its already-exported API.
    let should_pin = registry.should_pin_native_library(&name);
    assert!(!should_pin);
    let module = registry.finish_unload(&name).unwrap();
    assert!(registry.lookup(&name).is_none());
    // SAFETY: finish_unload transferred the sole loader reference after
    // callbacks and registrations ended; it is retired exactly once.
    unsafe { retire_native_library(module.handle, should_pin) };
}

#[test]
fn native_thread_api_export_pins_library_before_context_creation() {
    let fixture = Fixture::new(true, false);
    load_and_retire(&fixture);
    assert!(
        fixture.is_mapped(),
        "exported thread context code must survive physical retirement"
    );
}

#[test]
fn native_thread_api_export_keeps_failed_onload_library_mapped() {
    let fixture = Fixture::new(true, true);
    let mut registry = ModuleRegistry::new();
    assert!(ModuleLoader::load_dynamic(
        fixture.library.to_str().unwrap(),
        &[],
        &mut registry,
        std::ptr::null_mut(),
    )
    .is_err());
    assert!(registry.lookup("native-export").is_none());
    assert!(
        fixture.is_mapped(),
        "failed OnLoad must preserve code after exporting a native thread API"
    );
}

#[test]
fn native_library_without_thread_api_exports_closes_normally() {
    let fixture = Fixture::new(false, false);
    load_and_retire(&fixture);
    assert!(
        !fixture.is_mapped(),
        "a library without thread API exports must release its loader reference"
    );
}

#[test]
fn native_query_thread_api_exports_keep_callback_library_mapped() {
    for api in [
        "ValkeyModule_GetThreadSafeContext",
        "ValkeyModule_GetDetachedThreadSafeContext",
    ] {
        let fixture = Fixture::from_source(&format!(
            r#"
typedef int (*GetApi)(const char *, void **);
typedef void (*SetAttribs)(void *, const char *, int, int);
typedef int (*Callback)(void *, void **, int);
typedef int (*CreateCommand)(void *, const char *, Callback, const char *, int, int, int);
typedef int (*IsQuery)(void *);
typedef void (*KeyAtPos)(void *, int);
typedef struct {{ void *getapi; void *internal; }} ModuleCtx;
static IsQuery is_query;
static KeyAtPos key_at_pos;
static void *thread_api;
static int query(void *opaque, void **argv, int argc) {{
    (void)argv;
    (void)argc;
    if (is_query(opaque)) {{
        GetApi getapi = (GetApi)((ModuleCtx *)opaque)->getapi;
        if (getapi("{api}", &thread_api) != 0 || !thread_api) return 1;
        key_at_pos(opaque, 1);
    }}
    return 0;
}}
int ValkeyModule_OnLoad(void *opaque, void **argv, int argc) {{
    (void)argv;
    (void)argc;
    GetApi getapi = (GetApi)((ModuleCtx *)opaque)->getapi;
    void *address = 0;
    if (getapi("ValkeyModule_SetModuleAttribs", &address) != 0) return 1;
    ((SetAttribs)address)(opaque, "native-query", 1, 1);
    if (getapi("ValkeyModule_IsKeysPositionRequest", (void **)&is_query) != 0) return 1;
    if (getapi("ValkeyModule_KeyAtPos", (void **)&key_at_pos) != 0) return 1;
    if (getapi("ValkeyModule_CreateCommand", &address) != 0) return 1;
    return ((CreateCommand)address)(opaque, "native-query.keys", query,
                                  "getkeys-api", 0, 0, 0);
}}
"#
        ));
        let mut registry = ModuleRegistry::new();
        let name = ModuleLoader::load_dynamic(
            fixture.library.to_str().unwrap(),
            &[],
            &mut registry,
            std::ptr::null_mut(),
        )
        .unwrap();
        let entry = registry.lookup_cmd_func("native-query.keys").unwrap();
        let keys = crate::engine::extension::module::api::query_module_key_positions(
            entry,
            registry.lookup(&entry.module_name).unwrap().handle,
            b"native-query.keys",
            &[bytes::Bytes::from_static(b"key")],
        )
        .unwrap();
        assert_eq!(keys, vec![(1, vec!["RW", "access", "update"])]);
        let should_pin = registry.should_pin_native_library(&name);
        assert!(!should_pin);
        let module = registry.finish_unload(&name).unwrap();
        // SAFETY: the registry transferred its live loader reference, which
        // is retired once after the synchronous query callback ended.
        unsafe { retire_native_library(module.handle, should_pin) };
        assert!(
            fixture.is_mapped(),
            "query-exported thread API must keep its callback code mapped: {api}"
        );
    }
}
