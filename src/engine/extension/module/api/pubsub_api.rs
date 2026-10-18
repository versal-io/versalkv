#![allow(non_snake_case)]

use bytes::Bytes;
use libc::c_int;

use super::super::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR};
use crate::engine::extension::module::capability::runtime_from_opaque;

// SAFETY: ctx, channel, and message must be null or live module objects for the
// duration of this synchronous call; null pointers are rejected before access.
unsafe fn publish_message(
    ctx: *mut ValkeyModuleCtx,
    channel: *mut ValkeyModuleString,
    message: *mut ValkeyModuleString,
    is_shard: bool,
) -> c_int {
    unsafe {
        if ctx.is_null() || channel.is_null() || message.is_null() {
            return VALKEYMODULE_ERR;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return VALKEYMODULE_ERR;
        }
        let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
            return VALKEYMODULE_ERR;
        };
        let channel = Bytes::copy_from_slice((*channel).as_bytes());
        let message = Bytes::copy_from_slice((*message).as_bytes());
        runtime
            .publish_module_message(channel, message, is_shard)
            .and_then(|receivers| c_int::try_from(receivers).ok())
            .unwrap_or(VALKEYMODULE_ERR)
    }
}

// SAFETY: The module API passes a live command context and valid module strings.
pub unsafe extern "C" fn ValkeyModule_PublishMessage(
    ctx: *mut ValkeyModuleCtx,
    channel: *mut ValkeyModuleString,
    message: *mut ValkeyModuleString,
) -> c_int {
    unsafe { publish_message(ctx, channel, message, false) }
}

// SAFETY: The module API passes a live command context and valid module strings.
pub unsafe extern "C" fn ValkeyModule_PublishMessageShard(
    ctx: *mut ValkeyModuleCtx,
    channel: *mut ValkeyModuleString,
    message: *mut ValkeyModuleString,
) -> c_int {
    unsafe { publish_message(ctx, channel, message, true) }
}
