#![deny(unsafe_op_in_unsafe_fn)]

pub mod api;
pub mod capability;
mod context;
pub mod loader;
pub mod registry;
mod types;

use libc::c_int;

pub use context::{
    create_module_cmd_ctx, create_module_ctx, create_module_internal_ctx, free_module_ctx,
};
pub use registry::{
    ModuleCommandInfo, ModuleKeyBeginSearch, ModuleKeyFindKeys, ModuleKeySpec, ModuleRef,
    ModuleRegistry,
};
pub use types::*;

// ============================================================================
// ============================================================================

pub const VALKEYMODULE_OK: c_int = 0;
pub const VALKEYMODULE_ERR: c_int = 1;

pub const VALKEYMODULE_APIVER_1: c_int = 1;
pub const VALKEYMODULE_EVENT_EVENTLOOP: u64 = 15;
pub const VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP: u64 = 0;
pub const VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP: u64 = 1;
