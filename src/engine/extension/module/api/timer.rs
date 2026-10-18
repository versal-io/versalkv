//! Timer APIs for module callbacks.

#![allow(non_snake_case)]

use libc::{c_int, c_void};

use super::super::{ValkeyModuleCtx, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::active_registry::with_active_registry;
use super::ctx::get_module_name;
use crate::engine::extension::module::ValkeyModuleTimerProc;

// SAFETY: The module supplies a live context and callback. The registry scope
// keeps the module registry alive for the synchronous registration.
pub unsafe extern "C" fn ValkeyModule_CreateTimer(
    ctx: *mut ValkeyModuleCtx,
    period_ms: i64,
    callback: Option<ValkeyModuleTimerProc>,
    data: *mut c_void,
) -> u64 {
    unsafe {
        if ctx.is_null() || callback.is_none() {
            return 0;
        }
        let module_name = get_module_name(ctx);
        with_active_registry(|registry| {
            registry
                .create_timer(
                    &module_name,
                    (*(*ctx).internal).db_idx as u32,
                    period_ms,
                    callback.expect("callback checked above"),
                    data,
                )
                .unwrap_or(0)
        })
        .unwrap_or(0)
    }
}

// SAFETY: The module supplies a live context and timer id. The callback is
// synchronous and only returns the data pointer owned by that timer.
pub unsafe extern "C" fn ValkeyModule_StopTimer(
    ctx: *mut ValkeyModuleCtx,
    id: u64,
    data: *mut *mut c_void,
) -> c_int {
    unsafe {
        if ctx.is_null() {
            return VALKEYMODULE_ERR;
        }
        let module_name = get_module_name(ctx);
        let result =
            with_active_registry(|registry| registry.stop_timer(&module_name, id)).flatten();
        let Some(timer_data) = result else {
            return VALKEYMODULE_ERR;
        };
        if !data.is_null() {
            *data = timer_data;
        }
        VALKEYMODULE_OK
    }
}

// SAFETY: The module supplies a live context and timer id; output pointers are
// optional and are written only when non-null.
pub unsafe extern "C" fn ValkeyModule_GetTimerInfo(
    ctx: *mut ValkeyModuleCtx,
    id: u64,
    remaining_ms: *mut u64,
    data: *mut *mut c_void,
) -> c_int {
    unsafe {
        if ctx.is_null() {
            return VALKEYMODULE_ERR;
        }
        let module_name = get_module_name(ctx);
        let result = with_active_registry(|registry| registry.timer_info(&module_name, id));
        let Some(Some((remaining, timer_data))) = result else {
            return VALKEYMODULE_ERR;
        };
        if !remaining_ms.is_null() {
            *remaining_ms = remaining;
        }
        if !data.is_null() {
            *data = timer_data;
        }
        VALKEYMODULE_OK
    }
}
