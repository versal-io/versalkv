//! Context management and command registration API.
//! Extracted from `api/mod.rs` to reduce file size.
//! Contains ValkeyModule_Init, ValkeyModule_CreateCommand, module command dispatcher,
//! and related context management functions.

#![allow(non_snake_case)]

use libc::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};

use crate::engine::command::{
    CmdFlags, CMD_ADMIN, CMD_ALLOW_BUSY, CMD_ALL_DBS, CMD_ASKING, CMD_BLOCKING, CMD_DENYOOM,
    CMD_FAST, CMD_LOADING, CMD_MAY_REPLICATE, CMD_MOVABLE_KEYS, CMD_NOSCRIPT, CMD_NO_ASYNC_LOADING,
    CMD_NO_AUTH, CMD_NO_MANDATORY_KEYS, CMD_NO_MULTI, CMD_PROTECTED, CMD_PUBSUB, CMD_READONLY,
    CMD_SENTINEL, CMD_SKIP_COMMANDLOG, CMD_SKIP_MONITOR, CMD_SLOW, CMD_STALE,
    CMD_TOUCHES_ARBITRARY_KEYS, CMD_WRITE,
};
use crate::engine::extension::module::capability::runtime_from_opaque;

use super::super::{
    ModuleRef, ValkeyModuleCmdFunc, ValkeyModuleCtx, VALKEYMODULE_ERR, VALKEYMODULE_OK,
};
use super::active_registry::with_active_registry;
use crate::engine::extension::module::registry::ModuleCommandHandle;

// ============================================================================
// ============================================================================

#[allow(dead_code)]
// SAFETY: Called by C module OnLoad; ctx is a valid ValkeyModuleCtx allocated by the C module loader, and name is a valid C string provided by the module
pub unsafe extern "C" fn ValkeyModule_Init(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
    ver: c_int,
    apiver: c_int,
) -> c_int {
    unsafe { init_module_attribs(ctx, name, ver, apiver) }
}

// SAFETY: Called by ValkeyModule_Init or ValkeyModule_SetModuleAttribs; ctx is a valid ValkeyModuleCtx pointer from the C module loader, and name is a non-null C string
unsafe fn init_module_attribs(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
    ver: c_int,
    apiver: c_int,
) -> c_int {
    unsafe {
        if ctx.is_null() || name.is_null() {
            return VALKEYMODULE_ERR;
        }

        let module_name = match CStr::from_ptr(name).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return VALKEYMODULE_ERR,
        };

        if apiver != super::super::VALKEYMODULE_APIVER_1 {
            log::warn!(
                "Module '{}' requested API version {}, expected {}",
                module_name,
                apiver,
                super::super::VALKEYMODULE_APIVER_1
            );
            return VALKEYMODULE_ERR;
        }

        let internal = (*ctx).internal;
        if !internal.is_null() {
            if !(*internal).module_name.is_null() {
                let _ = CString::from_raw((*internal).module_name);
            }
            let cname = CString::new(module_name.clone()).unwrap();
            (*internal).module_name = cname.into_raw();
        }

        let name_busy =
            with_active_registry(|reg| reg.lookup(&module_name).is_some()).unwrap_or(true);
        if name_busy {
            log::warn!("Module name is busy: {}", module_name);
            return VALKEYMODULE_ERR;
        }

        with_active_registry(|reg| {
            let module_ref = ModuleRef::new(&module_name, std::ptr::null_mut(), ver, false);
            match reg.register(module_ref) {
                Ok(()) => {
                    log::info!("Module '{}' initialized (v{})", module_name, ver);
                    if !internal.is_null() {
                        if let Some(m) = reg.lookup_mut(&module_name) {
                            (*internal).module_ref = m as *mut ModuleRef as *mut c_void;
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Module '{}' registration failed: {}", module_name, e);
                    return VALKEYMODULE_ERR;
                }
            }
            VALKEYMODULE_OK
        })
        .unwrap_or(VALKEYMODULE_ERR)
    }
}

// SAFETY: Called by C module's valkeymodule.h inline initialization path; ctx is a valid ValkeyModuleCtx pointer, name is a non-null C string
pub unsafe extern "C" fn ValkeyModule_SetModuleAttribs(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
    ver: c_int,
    apiver: c_int,
) {
    unsafe {
        if init_module_attribs(ctx, name, ver, apiver) != VALKEYMODULE_OK {
            log::error!("Failed to initialize module attributes");
        }
    }
}

// SAFETY: Called by C module code; name is a non-null C string provided by the module
pub unsafe extern "C" fn ValkeyModule_IsModuleNameBusy(name: *const c_char) -> c_int {
    unsafe {
        if name.is_null() {
            return 1;
        }
        let name = match CStr::from_ptr(name).to_str() {
            Ok(name) => name,
            Err(_) => return 1,
        };
        with_active_registry(|reg| c_int::from(reg.lookup(name).is_some())).unwrap_or(1)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx, name is a non-null C string, and func is a valid C function pointer provided by the module
pub unsafe extern "C" fn ValkeyModule_CreateCommand(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
    func: ValkeyModuleCmdFunc,
    strflags: *const c_char,
    firstkey: c_int,
    lastkey: c_int,
    keystep: c_int,
) -> c_int {
    // SAFETY: the validated ABI context owns its internal state and name points
    // to a NUL-terminated string for this synchronous registration.
    unsafe {
        if ctx.is_null() || name.is_null() {
            return VALKEYMODULE_ERR;
        }
        if (*ctx).internal.is_null() || (*(*ctx).internal).onload == 0 {
            return VALKEYMODULE_ERR;
        }

        let cmd_name = match CStr::from_ptr(name).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return VALKEYMODULE_ERR,
        };
        if !is_module_command_name_valid(&cmd_name) {
            return VALKEYMODULE_ERR;
        }

        let module_name = get_module_name(ctx);
        if module_name.is_empty() {
            return VALKEYMODULE_ERR;
        }

        let flags_str = if strflags.is_null() {
            ""
        } else {
            CStr::from_ptr(strflags).to_str().unwrap_or("")
        };

        log::info!(
        "Module '{}' registered command '{}' (flags: {}, firstkey: {}, lastkey: {}, keystep: {})",
        module_name,
        cmd_name,
        flags_str,
        firstkey,
        lastkey,
        keystep
    );

        with_active_registry(|reg| {
            let _ = reg.register_command(&module_name, &cmd_name, func);

            let internal = (*ctx).internal;
            let runtime_handle = if internal.is_null() {
                std::ptr::null_mut()
            } else {
                (*internal).runtime_handle
            };
            let Some(runtime) = runtime_from_opaque(runtime_handle) else {
                log::warn!(
                    "Module runtime not set yet; command '{}' registered in module only",
                    cmd_name
                );
                return VALKEYMODULE_OK;
            };

            let leaked_name: &'static str = Box::leak(cmd_name.clone().into_boxed_str());
            let flags = parse_module_flags(flags_str);
            let arity = if func.is_some() { -1 } else { -2 };
            if !runtime.register_module_command(
                leaked_name,
                arity,
                flags,
                firstkey,
                lastkey,
                keystep,
            ) {
                return VALKEYMODULE_ERR;
            }
            let getkeys_api = flags_str
                .split_whitespace()
                .any(|flag| flag.eq_ignore_ascii_case("getkeys-api"));
            let _ = reg.set_legacy_command_info(
                &cmd_name,
                (firstkey, lastkey, keystep),
                getkeys_api,
                arity,
            );

            log::info!(
                "Module command '{}' registered in CommandRegistry",
                leaked_name
            );
            VALKEYMODULE_OK
        })
        .unwrap_or(VALKEYMODULE_ERR)
    }
}

// SAFETY: Called by C module code; ctx is a valid ValkeyModuleCtx, name is a non-null C string
pub unsafe extern "C" fn ValkeyModule_GetCommand(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
) -> *mut c_void {
    unsafe {
        if ctx.is_null() || name.is_null() {
            return std::ptr::null_mut();
        }
        let cmd_name = match CStr::from_ptr(name).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return std::ptr::null_mut(),
        };
        let module_name = get_module_name(ctx);
        if module_name.is_empty() {
            return std::ptr::null_mut();
        }
        let internal = (*ctx).internal;
        let runtime_handle = if internal.is_null() {
            std::ptr::null_mut()
        } else {
            (*internal).runtime_handle
        };
        with_active_registry(|reg| {
            if !reg.command_exists(&cmd_name) {
                return std::ptr::null_mut();
            }
            reg.new_command_handle(module_name, cmd_name, runtime_handle) as *mut c_void
        })
        .unwrap_or(std::ptr::null_mut())
    }
}

// SAFETY: Called by C module code; parent is an opaque pointer, name is a non-null C string, func is a C function pointer
pub unsafe extern "C" fn ValkeyModule_CreateSubcommand(
    parent: *mut c_void,
    name: *const c_char,
    func: ValkeyModuleCmdFunc,
    strflags: *const c_char,
    firstkey: c_int,
    lastkey: c_int,
    keystep: c_int,
) -> c_int {
    // SAFETY: parent is the live opaque command handle described by the ABI and
    // name remains a NUL-terminated string for this registration.
    unsafe {
        if parent.is_null() || name.is_null() {
            return VALKEYMODULE_ERR;
        }
        let sub_name = match CStr::from_ptr(name).to_str() {
            Ok(s) => s,
            Err(_) => return VALKEYMODULE_ERR,
        };
        if !is_module_subcommand_name_valid(sub_name) {
            return VALKEYMODULE_ERR;
        }
        let parent = &*(parent as *mut ModuleCommandHandle);
        if parent.command_name.contains('|') {
            return VALKEYMODULE_ERR;
        }
        let full_name = format!("{}|{}", parent.command_name, sub_name);
        let flags_str = if strflags.is_null() {
            ""
        } else {
            CStr::from_ptr(strflags).to_str().unwrap_or("")
        };
        with_active_registry(|reg| {
            if reg
                .lookup_cmd_func(&parent.command_name)
                .is_none_or(|entry| entry.func.is_some())
            {
                return VALKEYMODULE_ERR;
            }
            if reg
                .register_command(&parent.module_name, &full_name, func)
                .is_err()
            {
                return VALKEYMODULE_ERR;
            }
            let Some(runtime) = runtime_from_opaque(parent.runtime_handle) else {
                return VALKEYMODULE_ERR;
            };
            let leaked_name: &'static str = Box::leak(full_name.into_boxed_str());
            let flags = parse_module_flags(flags_str);
            if runtime.register_module_command(leaked_name, -2, flags, firstkey, lastkey, keystep) {
                VALKEYMODULE_OK
            } else {
                VALKEYMODULE_ERR
            }
        })
        .unwrap_or(VALKEYMODULE_ERR)
    }
}

// ============================================================================
// ============================================================================

fn parse_module_flags(flags_str: &str) -> CmdFlags {
    let mut flags: CmdFlags = 0;
    for flag in flags_str.split_whitespace() {
        match flag.to_ascii_lowercase().as_str() {
            "write" => flags |= CMD_WRITE,
            "deny-oom" => flags |= CMD_DENYOOM,
            "fast" => flags |= CMD_FAST,
            "readonly" => flags |= CMD_READONLY,
            // C Valkey accepts this deprecated declaration but deliberately
            // excludes it from COMMAND INFO. Nondeterminism is described by
            // command tips instead of a module command flag.
            "random" => {}
            "admin" => flags |= CMD_ADMIN,
            "allow-loading" | "loading" => flags |= CMD_LOADING,
            "allow-stale" | "stale" => flags |= CMD_STALE,
            "deny-script" | "noscript" => flags |= CMD_NOSCRIPT,
            "no-auth" => flags |= CMD_NO_AUTH,
            "no-monitor" | "skip-monitor" => flags |= CMD_SKIP_MONITOR,
            "no-async-loading" => flags |= CMD_NO_ASYNC_LOADING,
            "pubsub" => flags |= CMD_PUBSUB,
            "slow" => flags |= CMD_SLOW,
            "movable-keys" => flags |= CMD_MOVABLE_KEYS,
            "getkeys-api" | "getchannels-api" | "no-cluster" => {}
            "blocking" => flags |= CMD_BLOCKING,
            "no-multi" => flags |= CMD_NO_MULTI,
            "allow-busy" => flags |= CMD_ALLOW_BUSY,
            "all-dbs" => flags |= CMD_ALL_DBS,
            "asking" => flags |= CMD_ASKING,
            "may-replicate" => flags |= CMD_MAY_REPLICATE,
            "no-mandatory-keys" => flags |= CMD_NO_MANDATORY_KEYS,
            "protected" => flags |= CMD_PROTECTED,
            "sentinel" => flags |= CMD_SENTINEL,
            "no-slowlog" | "no-commandlog" | "skip-commandlog" => {
                flags |= CMD_SKIP_COMMANDLOG;
            }
            "touches-arbitrary-keys" => flags |= CMD_TOUCHES_ARBITRARY_KEYS,
            _ => {
                log::warn!("Unknown module command flag: '{}'", flag);
            }
        }
    }
    flags
}

fn is_module_command_name_valid(name: &str) -> bool {
    !name
        .bytes()
        .any(|byte| matches!(byte, b' ' | b'\r' | b'\n' | b'|' | b'@' | b'=' | b','))
}

fn is_module_subcommand_name_valid(name: &str) -> bool {
    is_module_command_name_valid(name)
}

// ============================================================================
// ============================================================================

// SAFETY: ctx must be a valid ValkeyModuleCtx pointer (or null, which is handled); the internal->module_name C string must be valid
pub(crate) unsafe fn get_module_name(ctx: *mut ValkeyModuleCtx) -> String {
    unsafe {
        if ctx.is_null() {
            return String::new();
        }
        let internal = (*ctx).internal;
        if internal.is_null() || (*internal).module_name.is_null() {
            return String::new();
        }
        CStr::from_ptr((*internal).module_name)
            .to_str()
            .unwrap_or("")
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_module_command_name_valid, is_module_subcommand_name_valid, parse_module_flags,
    };
    use crate::engine::command::{
        CMD_LOADING, CMD_NOSCRIPT, CMD_SKIP_COMMANDLOG, CMD_SKIP_MONITOR, CMD_STALE, CMD_WRITE,
    };

    #[test]
    fn module_flags_match_c_names_and_ignore_deprecated_random() {
        let flags = parse_module_flags(
            "WRITE allow-loading allow-stale deny-script no-monitor no-slowlog random",
        );

        assert_ne!(flags & CMD_WRITE, 0);
        assert_ne!(flags & CMD_LOADING, 0);
        assert_ne!(flags & CMD_STALE, 0);
        assert_ne!(flags & CMD_NOSCRIPT, 0);
        assert_ne!(flags & CMD_SKIP_MONITOR, 0);
        assert_ne!(flags & CMD_SKIP_COMMANDLOG, 0);
        assert_eq!(
            parse_module_flags("write deny-oom random fast"),
            parse_module_flags("write deny-oom fast")
        );
    }

    #[test]
    fn module_command_names_reject_whitespace_controls() {
        for name in ["module.char\r", "module.char\n", "module.char "] {
            assert!(!is_module_command_name_valid(name), "{name:?}");
        }
        assert!(is_module_command_name_valid("module.command"));
    }

    #[test]
    fn module_subcommand_names_reject_command_separators() {
        for name in ["char|", "char@", "char="] {
            assert!(!is_module_subcommand_name_valid(name), "{name:?}");
        }
        assert!(is_module_subcommand_name_valid("get_fullname"));
    }
}
