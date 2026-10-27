use libc::{c_int, c_void};
use std::ffi::{CStr, CString};

use super::api::{module_get_api_impl, ActiveModuleRegistryScope};
use super::{
    create_module_ctx, create_module_internal_ctx, free_module_ctx, ModuleLoadFunc, ModuleRef,
    ModuleRegistry, ModuleUnloadFunc, ValkeyModuleString, VALKEYMODULE_OK,
};

pub struct ModuleLoader;

impl ModuleLoader {
    pub fn load_dynamic(
        path: &str,
        args: &[&[u8]],
        registry: &mut ModuleRegistry,
        runtime_handle: *mut c_void,
    ) -> Result<String, super::registry::ModuleError> {
        use super::registry::ModuleError;

        let cpath = CString::new(path).map_err(|_| ModuleError::InvalidPath(path.to_string()))?;

        // 2. dlopen
        let flags = libc::RTLD_NOW | libc::RTLD_LOCAL;
        #[cfg(target_os = "linux")]
        let flags = flags | libc::RTLD_DEEPBIND;
        // SAFETY: `cpath` is a valid C string (checked by CString::new above). dlopen
        // is a libc function that is safe to call from any thread; it returns null on
        // failure, which we check immediately after. Linux deep binding keeps the
        // official header's common API-pointer symbols local to the module instead of
        // preempting them with the executable's same-named exported functions.
        let handle = unsafe { libc::dlopen(cpath.as_ptr(), flags) };
        if handle.is_null() {
            // SAFETY: dlerror is thread-local and returns a pointer to a null-terminated
            // string. It is safe to call after dlopen failure. We check for null and
            // only call CStr::from_ptr when it is non-null.
            let err = unsafe {
                let ptr = libc::dlerror();
                if ptr.is_null() {
                    "unknown error".to_string()
                } else {
                    CStr::from_ptr(ptr).to_string_lossy().into_owned()
                }
            };
            return Err(ModuleError::LoadFailed(format!("dlopen failed: {}", err)));
        }

        let onload_name = CString::new("ValkeyModule_OnLoad").unwrap();
        // SAFETY: dlsym is a libc function. The handle is a valid non-null dlopen handle
        // (checked above). The symbol name is a valid C string. We check the return value
        // for null to detect symbol-not-found. In the legacy fallback path, the same
        // safety argument applies to the second dlsym call as well.
        let onload_ptr = unsafe {
            let ptr = libc::dlsym(handle, onload_name.as_ptr());
            if ptr.is_null() {
                let legacy = CString::new("RedisModule_OnLoad").unwrap();
                let ptr2 = libc::dlsym(handle, legacy.as_ptr());
                if ptr2.is_null() {
                    libc::dlclose(handle);
                    let eptr = libc::dlerror();
                    let err = if eptr.is_null() {
                        "symbol not found".to_string()
                    } else {
                        CStr::from_ptr(eptr).to_string_lossy().into_owned()
                    };
                    return Err(ModuleError::LoadFailed(format!(
                        "Module does not export ValkeyModule_OnLoad or RedisModule_OnLoad: {}",
                        err
                    )));
                }
                log::info!("Legacy Redis Module {} found", path);
                ptr2
            } else {
                ptr
            }
        };
        if onload_ptr.is_null() {
            return Err(ModuleError::LoadFailed(
                "Failed to resolve OnLoad symbol".to_string(),
            ));
        }
        // SAFETY: transmute from a raw function pointer (returned by dlsym) to the
        // ModuleLoadFunc type is safe because dlsym is guaranteed to return a function
        // pointer of the correct signature for ValkeyModule_OnLoad. The C module ABI
        // matches the extern "C" calling convention of ModuleLoadFunc.
        let onload: ModuleLoadFunc = unsafe { std::mem::transmute(onload_ptr) };

        // SAFETY: handle remains open. Both optional symbols have the module
        // OnUnload ABI; a missing symbol stays None and is never called.
        let onunload: Option<ModuleUnloadFunc> = unsafe {
            let mut ptr = libc::dlsym(handle, c"ValkeyModule_OnUnload".as_ptr());
            if ptr.is_null() {
                ptr = libc::dlsym(handle, c"RedisModule_OnUnload".as_ptr());
            }
            if ptr.is_null() {
                None
            } else {
                Some(std::mem::transmute::<*mut c_void, ModuleUnloadFunc>(ptr))
            }
        };

        let internal = create_module_internal_ctx("pending", std::ptr::null_mut());
        let internal_ptr = Box::into_raw(internal);
        // SAFETY: internal_ptr was just allocated via Box::into_raw and is uniquely
        // owned. Writing runtime_handle into the field is safe because no other code
        // references this memory yet.
        unsafe {
            (*internal_ptr).runtime_handle = runtime_handle;
        }
        let ctx = create_module_ctx(internal_ptr);
        let ctx_ptr = Box::into_raw(ctx);

        let mut arg_strings: Vec<*mut ValkeyModuleString> = args
            .iter()
            .map(|arg| Box::into_raw(ValkeyModuleString::from_bytes(arg)))
            .collect();
        let argv = arg_strings.as_mut_ptr() as *mut *mut c_void;

        // SAFETY: ctx_ptr was just allocated via Box::into_raw and is uniquely owned.
        // Writing to the getapifuncptr field is safe because no other code references
        // this memory yet.
        unsafe {
            (*ctx_ptr).getapifuncptr = module_get_api_impl as *mut c_void;
        }

        let ret = {
            let _registry_scope = ActiveModuleRegistryScope::enter(registry as *mut ModuleRegistry)
                .expect("module registry pointer is non-null");
            let _context_scope = super::api::ActiveModuleContextScope::enter(ctx_ptr);
            onload(ctx_ptr as *mut c_void, argv, arg_strings.len() as c_int)
        };
        // SAFETY: OnLoad has returned; collect before rollback or unloading code.
        unsafe { super::auto_memory::collect(internal_ptr) };
        for string in arg_strings.drain(..) {
            // SAFETY: each argument string was allocated for this synchronous
            // OnLoad call and is no longer borrowed after OnLoad returns.
            unsafe {
                ValkeyModuleString::release(string);
            }
        }

        // SAFETY: internal_ptr is a valid non-null pointer obtained from
        // Box::into_raw. The module_name field is a CString that was set by
        // create_module_internal_ctx and is guaranteed to be non-null initially.
        let module_name = unsafe {
            if (*internal_ptr).module_name.is_null() {
                "unknown"
            } else {
                CStr::from_ptr((*internal_ptr).module_name)
                    .to_str()
                    .unwrap_or("unknown")
            }
        };

        if ret != VALKEYMODULE_OK {
            // SAFETY: internal_ptr is a valid non-null pointer from Box::into_raw.
            // Reading module_ref is safe because the struct is still alive.
            let module_registered = unsafe { !(*internal_ptr).module_ref.is_null() };
            if module_registered {
                log::warn!(
                    "Module '{}' initialization failed. Module not loaded.",
                    module_name
                );
                let command_names = registry
                    .lookup(module_name)
                    .map(|module| module.commands.clone())
                    .unwrap_or_default();
                if let Some(runtime) =
                    // SAFETY: runtime_handle points to the live handle used for
                    // this synchronous module load.
                    unsafe { super::capability::runtime_from_opaque(runtime_handle) }
                {
                    for command_name in command_names {
                        runtime.unregister_module_command(&command_name);
                    }
                }
                let _ = registry.abort_load(module_name);
            } else {
                log::warn!(
                    "Module '{}' initialization failed. Module name is busy.",
                    path
                );
                // SAFETY: `handle` is a valid dlopen handle (checked non-null after dlopen)
                // and the module has not been fully initialized, so it is safe to close it.
            }
            // SAFETY: the handle is no longer needed after the failed
            // initialization has been rolled back.
            unsafe {
                libc::dlclose(handle);
            }
            let error = if module_registered {
                ModuleError::LoadFailed("module initialization failed".to_string())
            } else {
                ModuleError::LoadFailed(
                    "module initialization failed, module name is busy".to_string(),
                )
            };
            // SAFETY: ctx_ptr is a valid Box allocation from Box::into_raw.
            // free_module_ctx takes ownership and drops both the ctx and its internal.
            unsafe {
                free_module_ctx(ctx_ptr);
            }
            return Err(error);
        }

        if module_name == "pending" {
            // SAFETY: ctx_ptr is a valid Box allocation from Box::into_raw.
            // free_module_ctx takes ownership of both the ValkeyModuleCtx and its
            // internal ModuleInternalCtx, properly dropping them.
            unsafe {
                free_module_ctx(ctx_ptr);
                libc::dlclose(handle);
            }
            return Err(ModuleError::InitFailed(
                "Module did not call ValkeyModule_Init during OnLoad".to_string(),
            ));
        }

        if registry.lookup(module_name).is_none() {
            let module_ref = ModuleRef::new(module_name, handle, 1, false);
            let _ = registry.register(module_ref);
        }

        if let Some(m) = registry.lookup_mut(module_name) {
            m.handle = handle;
            m.onunload = onunload;
        }
        // SAFETY: internal_ptr is a valid non-null pointer from Box::into_raw.
        // Writing onload is safe because the struct is still alive and uniquely
        // owned by the module initialization path.
        unsafe {
            (*internal_ptr).onload = 0;
        }

        // SAFETY: ctx_ptr is a unique Box allocation from Box::into_raw; taking
        // ownership via Box::from_raw is correct. internal_ptr is handed off to
        // registry.register_internal_ctx which takes ownership of the lifetime.
        unsafe {
            let _ = Box::from_raw(ctx_ptr); //  ctx
            registry.register_internal_ctx(internal_ptr);
        }

        log::info!("Module '{}' loaded from {}", module_name, path);
        Ok(module_name.to_string())
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::engine::extension::module::lifecycle_test_support::{snapshot, Drops};
    use std::cell::Cell;

    thread_local! {
        static UNLOAD_DROPS: Cell<Option<Drops>> = const { Cell::new(None) };
    }

    extern "C" fn observe_cleanup() {
        UNLOAD_DROPS.with(|observed| observed.set(Some(snapshot())));
    }

    struct FixtureDir(std::path::PathBuf);

    impl Drop for FixtureDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn compile_fixture() -> (FixtureDir, String) {
        static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp = FixtureDir(std::env::temp_dir().join(format!(
            "versal-module-lifecycle-{}-{id}",
            std::process::id(),
        )));
        std::fs::create_dir(&temp.0).unwrap();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let path = temp.0.join("auto_memory_lifecycle.so");
        let mut compiler =
            std::process::Command::new(std::env::var_os("CC").unwrap_or_else(|| "cc".into()));
        compiler
            .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
            .arg("-I")
            .arg(root.join("thirdparty/valkey/src"))
            .arg("-o")
            .arg(&path)
            .arg(root.join("tests/fixtures/module_ffi/auto_memory_lifecycle.c"));
        #[cfg(target_os = "macos")]
        compiler.arg("-Wl,-undefined,dynamic_lookup");
        let result = compiler.output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        (temp, path.to_string_lossy().into_owned())
    }

    #[test]
    fn failed_onload_collects_automatic_strings_before_dlclose() {
        let (_temp, path) = compile_fixture();
        let mut registry = ModuleRegistry::new();
        let probe = format!("probe:{:x}", observe_cleanup as *const () as usize);
        let before = snapshot();
        let result = ModuleLoader::load_dynamic(
            &path,
            &[b"fail", probe.as_bytes()],
            &mut registry,
            std::ptr::null_mut(),
        );
        assert!(result.is_err());
        assert!(registry.lookup("auto-memory-lifecycle").is_none());
        assert_eq!(snapshot().strings - before.strings, 5);
        UNLOAD_DROPS.with(|observed| {
            assert_eq!(
                observed.get().expect("dlclose must invoke probe").strings - before.strings,
                5
            );
        });
    }

    #[test]
    fn successful_onload_collects_scope_immediately_but_keeps_retained_argument() {
        let (_temp, path) = compile_fixture();
        let mut registry = ModuleRegistry::new();
        let before = snapshot();
        assert_eq!(
            ModuleLoader::load_dynamic(&path, &[b"argument"], &mut registry, std::ptr::null_mut(),)
                .unwrap(),
            "auto-memory-lifecycle"
        );
        assert_eq!(snapshot().strings - before.strings, 3);
        let handle = registry.lookup("auto-memory-lifecycle").unwrap().handle;
        let callback = registry
            .lookup_cmd_func("lifecycle.argv")
            .unwrap()
            .func
            .unwrap();
        let internal = super::super::create_module_cmd_ctx(
            "auto-memory-lifecycle",
            "lifecycle.argv",
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
        assert_eq!(callback(ctx, std::ptr::null_mut(), 0), VALKEYMODULE_OK);
        assert_eq!(snapshot().strings - before.strings, 4);
        // SAFETY: the invocation context is owned and no callback remains active.
        unsafe { free_module_ctx(ctx) };
        drop(registry);
        // SAFETY: test owns the fixture's successful dlopen handle; callbacks are gone.
        unsafe { assert_eq!(libc::dlclose(handle), 0) };
    }
}
