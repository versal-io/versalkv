use crate::engine::command::CMD_MAY_REPLICATE;
use crate::engine::extension::module::api::call::{
    ValkeyModuleCallReply, ValkeyModule_CallReplyProto, ValkeyModule_FreeCallReply,
    ValkeyModule_ReplicateVerbatim,
};
use crate::engine::extension::module::capability::ModuleRuntimeHandle;
use crate::engine::extension::module::{
    create_module_cmd_ctx, create_module_ctx, free_module_ctx, ValkeyModuleCtx,
};
use crate::foundation::io::sys::os;
use crate::framework::context::{PauseAction, ServerContext};

// SAFETY: callers keep the context live and match every variadic input to its format specifier.
type Call = unsafe extern "C" fn(
    *mut ValkeyModuleCtx,
    *const libc::c_char,
    *const libc::c_char,
    ...
) -> *mut ValkeyModuleCallReply;
// SAFETY: callers keep the context live and match every variadic input to its format specifier.
type Replicate = unsafe extern "C" fn(
    *mut ValkeyModuleCtx,
    *const libc::c_char,
    *const libc::c_char,
    ...
) -> libc::c_int;

fn shim_functions() -> (Call, Replicate) {
    let mut call = std::ptr::null_mut();
    let mut replicate = std::ptr::null_mut();
    // SAFETY: the resolver writes the public C shim addresses into live output slots.
    unsafe {
        assert_eq!(
            crate::engine::extension::module::api::module_get_api_impl(
                c"ValkeyModule_Call".as_ptr(),
                &mut call
            ),
            0
        );
        assert_eq!(
            crate::engine::extension::module::api::module_get_api_impl(
                c"ValkeyModule_Replicate".as_ptr(),
                &mut replicate
            ),
            0
        );
        (
            std::mem::transmute::<*mut libc::c_void, Call>(call),
            std::mem::transmute::<*mut libc::c_void, Replicate>(replicate),
        )
    }
}

struct Context {
    ctx: *mut ValkeyModuleCtx,
    _handle: Box<ModuleRuntimeHandle>,
}

impl Context {
    fn new(runtime: &mut ServerContext) -> Self {
        runtime
            .script_mut()
            .command_registry
            .register(crate::framework::commands::string::set::SET_SPEC);
        runtime
            .script_mut()
            .command_registry
            .register(crate::framework::commands::string::get::GET_SPEC);
        let mut may_replicate = crate::framework::commands::string::get::GET_SPEC;
        may_replicate.name = crate::engine::command::CommandName::Static("mayreplicate");
        may_replicate.flags = CMD_MAY_REPLICATE;
        runtime
            .script_mut()
            .command_registry
            .register(may_replicate);
        let mut handle = Box::new(ModuleRuntimeHandle::new(runtime));
        let ctx = Box::into_raw(create_module_ctx(Box::into_raw(create_module_cmd_ctx(
            "test",
            "test.pause",
            std::ptr::null_mut(),
            (&mut *handle as *mut ModuleRuntimeHandle).cast(),
        ))));
        Self {
            ctx,
            _handle: handle,
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: this fixture owns the context and keeps its runtime handle alive until release.
        unsafe { free_module_ctx(self.ctx) };
    }
}

fn pause(runtime: &mut ServerContext, action: PauseAction, end_time_ms: i64) {
    let state = runtime.client_pause_mut();
    state.action = action;
    state.end_time_ms = end_time_ms;
}

#[test]
fn module_pause_call_rejects_propagating_writes_with_null_and_enospc() {
    let mut runtime = ServerContext::new(1);
    pause(&mut runtime, PauseAction::Write, i64::MAX);
    let context = Context::new(&mut runtime);
    let (call, _) = shim_functions();
    os::set_errno(0);
    // SAFETY: the context and C string arguments remain live for the variadic call.
    let reply = unsafe {
        call(
            context.ctx,
            c"SET".as_ptr(),
            c"cc!".as_ptr(),
            c"paused-key".as_ptr(),
            c"value".as_ptr(),
        )
    };
    let error = os::errno();
    let is_null = reply.is_null();
    if !is_null {
        // SAFETY: the shim returned a live owned reply which is freed once.
        unsafe { ValkeyModule_FreeCallReply(reply) };
    }
    assert!(
        is_null,
        "propagating writes must return no reply while paused"
    );
    assert_eq!(error, libc::ENOSPC);
    assert!(runtime
        .db_mut(0)
        .unwrap()
        .get_no_expire(b"paused-key")
        .is_none());
}

#[test]
fn module_pause_call_returns_requested_error_for_write_and_may_replicate() {
    for command in [c"SET", c"mayreplicate"] {
        let mut runtime = ServerContext::new(1);
        pause(&mut runtime, PauseAction::All, i64::MAX);
        let context = Context::new(&mut runtime);
        let (call, _) = shim_functions();
        os::set_errno(0);
        // SAFETY: each format exactly matches the live C string arguments.
        let reply = unsafe {
            if command == c"SET" {
                call(
                    context.ctx,
                    command.as_ptr(),
                    c"cc!E".as_ptr(),
                    c"paused-key".as_ptr(),
                    c"value".as_ptr(),
                )
            } else {
                call(
                    context.ctx,
                    command.as_ptr(),
                    c"c!E".as_ptr(),
                    c"paused-key".as_ptr(),
                )
            }
        };
        let error = os::errno();
        assert!(!reply.is_null(), "requested pause errors must have a reply");
        // SAFETY: reply is a live owned root and remains live during protocol inspection.
        let proto = unsafe {
            let mut len = 0;
            let ptr = ValkeyModule_CallReplyProto(reply, &mut len);
            std::slice::from_raw_parts(ptr.cast::<u8>(), len).to_vec()
        };
        // SAFETY: the owned reply is no longer borrowed and is released once.
        unsafe { ValkeyModule_FreeCallReply(reply) };
        assert!(String::from_utf8(proto)
            .unwrap()
            .contains("was called while the server is paused for writes."));
        assert_eq!(error, libc::ENOSPC);
        assert!(runtime
            .db_mut(0)
            .unwrap()
            .get_no_expire(b"paused-key")
            .is_none());
    }
}

#[test]
fn module_pause_call_allows_reads_nonpropagating_writes_and_expired_pause() {
    for (command, format, action, end_time_ms, should_write) in [
        (c"GET", c"c!", PauseAction::All, i64::MAX, false),
        (c"SET", c"cc", PauseAction::Write, i64::MAX, true),
        (c"SET", c"cc!", PauseAction::Write, 1, true),
        (c"SET", c"cc!", PauseAction::None, i64::MAX, true),
    ] {
        let mut runtime = ServerContext::new(1);
        pause(&mut runtime, action, end_time_ms);
        let context = Context::new(&mut runtime);
        let (call, _) = shim_functions();
        // SAFETY: surplus variadic arguments are unused; all consumed arguments match the format.
        let reply = unsafe {
            call(
                context.ctx,
                command.as_ptr(),
                format.as_ptr(),
                c"allowed-key".as_ptr(),
                c"value".as_ptr(),
            )
        };
        assert!(!reply.is_null());
        // SAFETY: the returned owned reply is released once.
        unsafe { ValkeyModule_FreeCallReply(reply) };
        assert_eq!(
            runtime
                .db_mut(0)
                .unwrap()
                .get_no_expire(b"allowed-key")
                .is_some(),
            should_write
        );
    }
}

#[test]
fn module_pause_replicate_and_verbatim_reject_before_propagation() {
    use super::ModuleCommandConnScope;
    use crate::framework::capabilities::connection::Connection;
    for action in [PauseAction::Write, PauseAction::All, PauseAction::None] {
        let mut runtime = ServerContext::new(1);
        pause(&mut runtime, action, i64::MAX);
        let context = Context::new(&mut runtime);
        let (_, replicate) = shim_functions();
        let mut conn = Connection::new_fake();
        let scope = ModuleCommandConnScope::enter(&mut conn);
        // SAFETY: context and exactly matching variadic inputs live through both calls.
        let (result, verbatim) = unsafe {
            (
                replicate(
                    context.ctx,
                    c"SET".as_ptr(),
                    c"cc".as_ptr(),
                    c"paused-key".as_ptr(),
                    c"value".as_ptr(),
                ),
                ValkeyModule_ReplicateVerbatim(context.ctx),
            )
        };
        drop(scope);
        let expected = if action == PauseAction::None { 0 } else { 1 };
        assert_eq!(result, expected);
        assert_eq!(verbatim, expected);
        assert_eq!(conn.custom_propagation().len(), usize::from(expected == 0));
    }
}
