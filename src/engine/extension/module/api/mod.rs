//! # Submodules
//! - `block` — native blocked client ownership
//! - `ctx` — context management and command registration
//! - `call` — call, reply, and string API implementations
//! - `active_registry` — thread-local module registry scope management
//! - `reply_builder` — nested array/reply builder
//! - `reply_api` — ValkeyModule_ReplyWith* FFI functions
//! - `key_api` — ValkeyModule_OpenKey/KeyType/... FFI functions
//! - `memory_api` — ValkeyModule_Alloc/Free/... FFI functions
//! - `string_obj_api` — ValkeyModule_CreateString/... FFI functions
//! - `datatype_api` — ValkeyModule_CreateDataType/... FFI functions

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub(crate) mod acl;
mod active_registry;
pub mod block;
pub mod call;
mod client_api;
mod cluster_slot_api;
pub(crate) mod command_args;
mod command_info_api;
pub mod ctx;
mod datatype_api;
mod get_api;
mod hash_api;
mod key_api;
mod key_eviction_api;
mod key_position;
#[cfg(test)]
mod lifecycle_tests;
mod list_api;
mod memory_api;
pub(crate) mod notifications;
mod pubsub_api;
mod reply_api;
mod reply_builder;
mod scan;
mod server_event_api;
mod stream;
mod string_obj_api;
mod thread_safe_context_api;
mod time_api;
mod timer;

#[cfg(test)]
pub(crate) use active_registry::with_active_registry;
pub(crate) use active_registry::{ActiveModuleContextScope, ActiveModuleRegistryScope};
#[cfg(test)]
pub(crate) use command_info_api::{
    ValkeyModuleCommandInfo, ValkeyModuleCommandInfoVersion, ValkeyModuleCommandKeySpec,
    ValkeyModule_SetCommandInfo,
};
pub(crate) use get_api::module_get_api_impl;
#[cfg(test)]
pub(crate) use key_api::{ValkeyModule_CloseKey, ValkeyModule_OpenKey};
pub(crate) use key_position::query_module_key_positions;
use libc::{c_char, c_int, size_t};
pub(crate) use reply_builder::{ModuleReplyFrame, ReplyBuilder};
#[cfg(test)]
pub(crate) use scan::{
    ValkeyModuleScanCursor, ValkeyModule_ScanKey, ValkeyModule_ScanKeyRawBorrowed,
};

use super::{ValkeyModuleCtx, VALKEYMODULE_ERR, VALKEYMODULE_OK};

// SAFETY: Called by C module code via direct symbol reference; ctx is a valid
// ValkeyModuleCtx pointer and proto points to proto_len readable bytes.
#[no_mangle]
pub unsafe extern "C" fn VM_ReplyRaw(
    ctx: *mut ValkeyModuleCtx,
    proto: *const c_char,
    proto_len: size_t,
) -> c_int {
    unsafe {
        if ctx.is_null() || (proto.is_null() && proto_len != 0) {
            return VALKEYMODULE_ERR;
        }
        let reply = if proto_len == 0 {
            bytes::Bytes::new()
        } else {
            bytes::Bytes::copy_from_slice(std::slice::from_raw_parts(proto.cast::<u8>(), proto_len))
        };
        if reply_api::with_reply_builder(ctx, |builder| builder.add_raw(reply)) == Some(true) {
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}
