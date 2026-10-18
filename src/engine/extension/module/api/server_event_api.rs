//! Server event subscription APIs supported by the current module runtime.

use libc::{c_int, c_longlong};

use super::super::{
    ValkeyModuleCtx, ValkeyModuleEvent, ValkeyModuleEventCallback, VALKEYMODULE_ERR,
    VALKEYMODULE_EVENT_EVENTLOOP, VALKEYMODULE_OK,
};

/// Subscribe, replace, or remove this module's callback for a server event.
// SAFETY: `ctx` must be null or point to a live module context. A non-null
// callback must remain executable until it is replaced, removed, or the module
// unloads, matching the Valkey Module ABI contract.
pub unsafe extern "C" fn ValkeyModule_SubscribeToServerEvent(
    ctx: *mut ValkeyModuleCtx,
    event: ValkeyModuleEvent,
    callback: Option<ValkeyModuleEventCallback>,
) -> c_int {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() || (*(*ctx).internal).module_ref.is_null() {
            return VALKEYMODULE_ERR;
        }
        let module_name = super::ctx::get_module_name(ctx);
        super::active_registry::with_active_registry(|registry| {
            if registry.subscribe_server_event(&module_name, event, callback) {
                VALKEYMODULE_OK
            } else {
                VALKEYMODULE_ERR
            }
        })
        .unwrap_or(VALKEYMODULE_ERR)
    }
}

/// Report support for the two EventLoop subevents exposed by this runtime.
pub extern "C" fn ValkeyModule_IsSubEventSupported(
    event: ValkeyModuleEvent,
    subevent: c_longlong,
) -> c_int {
    c_int::from(event.id == VALKEYMODULE_EVENT_EVENTLOOP && subevent < 2)
}
