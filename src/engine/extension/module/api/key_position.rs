use libc::c_int;

use super::super::capability::{ActiveModuleHostScope, ActiveModuleRuntimeScope};
use super::super::registry::ModuleCmdEntry;
use super::super::{
    create_module_ctx, free_module_ctx, ModuleRegistry, ValkeyModuleCtx, ValkeyModuleString,
};
use super::{ActiveModuleContextScope, ActiveModuleRegistryScope};

struct KeyPositionInvocation {
    ctx: *mut ValkeyModuleCtx,
    argv: Vec<*mut ValkeyModuleString>,
}

impl Drop for KeyPositionInvocation {
    fn drop(&mut self) {
        // SAFETY: this invocation owns one reference to each argv string and
        // uniquely owns the context, including its automatic resources.
        unsafe {
            for string in self.argv.drain(..) {
                ValkeyModuleString::release(string);
            }
            free_module_ctx(self.ctx);
        }
    }
}

pub(crate) fn query_module_key_positions(
    entry: &ModuleCmdEntry,
    native_library_handle: *mut libc::c_void,
    command_name: &[u8],
    args: &[bytes::Bytes],
) -> Option<Vec<(usize, Vec<&'static str>)>> {
    let callback = entry.func?;
    let argc = args.len().checked_add(1)?;
    let argc_abi = c_int::try_from(argc).ok()?;
    let mut internal = super::super::context::create_module_key_position_ctx(
        &entry.module_name,
        std::str::from_utf8(command_name).ok()?,
        argc,
    );
    // Preserve only library ownership; the query still cannot inherit mutable
    // registry state or its ancestor's execution host.
    internal.native_library_handle = native_library_handle;
    let mut ctx = create_module_ctx(Box::into_raw(internal));
    ctx.getapifuncptr = super::module_get_api_impl as *mut libc::c_void;
    let mut invocation = KeyPositionInvocation {
        ctx: Box::into_raw(ctx),
        argv: std::iter::once(command_name)
            .chain(args.iter().map(|arg| arg.as_ref()))
            .map(|arg| Box::into_raw(ValkeyModuleString::from_bytes(arg)))
            .collect(),
    };
    // A query must not inherit an ancestor's mutable registry or execution host.
    let mut query_registry = ModuleRegistry::new();
    let _registry_scope = ActiveModuleRegistryScope::enter(&mut query_registry);
    let _runtime_scope = ActiveModuleRuntimeScope::suspend();
    let _host_scope = ActiveModuleHostScope::enter(None);
    let _context_scope = ActiveModuleContextScope::enter(invocation.ctx);
    callback(invocation.ctx, invocation.argv.as_mut_ptr(), argc_abi);
    // SAFETY: the invocation retains its exclusively owned context until drop.
    let request = unsafe { &mut *(*invocation.ctx).internal }
        .key_position_request
        .as_mut()?;
    request
        .is_valid
        .then(|| std::mem::take(&mut request.positions))
}

// SAFETY: ctx is null or a live module context for this synchronous call.
pub(super) unsafe extern "C" fn ValkeyModule_IsKeysPositionRequest(
    ctx: *mut ValkeyModuleCtx,
) -> c_int {
    // SAFETY: optional context pointers are dereferenced only while live.
    let internal = unsafe { ctx.as_ref().and_then(|ctx| ctx.internal.as_ref()) };
    c_int::from(internal.is_some_and(|internal| internal.key_position_request.is_some()))
}

// SAFETY: ctx is null or an exclusively accessible live module context.
pub(super) unsafe extern "C" fn ValkeyModule_KeyAtPosWithFlags(
    ctx: *mut ValkeyModuleCtx,
    pos: c_int,
    flags: c_int,
) {
    // SAFETY: a live context owns its internal request and no reference is retained.
    let internal = unsafe { ctx.as_ref().and_then(|ctx| ctx.internal.as_mut()) };
    let Some(request) = internal.and_then(|internal| internal.key_position_request.as_mut()) else {
        return;
    };
    if pos <= 0 {
        return;
    }
    let pos = pos as usize;
    if pos >= request.argc {
        request.is_valid = false;
        request.positions.clear();
        return;
    }
    request
        .positions
        .push((pos, super::command_info_api::key_flag_names(flags as u64)));
}

// SAFETY: ctx is null or an exclusively accessible live module context.
pub(super) unsafe extern "C" fn ValkeyModule_KeyAtPos(ctx: *mut ValkeyModuleCtx, pos: c_int) {
    // SAFETY: the same caller-owned context is forwarded without retaining it.
    unsafe { ValkeyModule_KeyAtPosWithFlags(ctx, pos, (1 << 1) | (1 << 4) | (1 << 5)) };
}
