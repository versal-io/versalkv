use bytes::Bytes;
use libc::{c_int, c_void};
use std::ffi::CString;
use std::ptr::NonNull;

use crate::engine::command::context::CommandConn;
use crate::engine::command::CommandArgs;
use crate::engine::extension::module::api::{
    ActiveModuleContextScope, ActiveModuleRegistryScope, ReplyBuilder,
};
use crate::engine::extension::module::capability::{ActiveModuleRuntimeScope, ModuleRuntimeHandle};

#[cfg(test)]
#[path = "module_command_info_tests.rs"]
mod module_command_info_tests;
#[cfg(test)]
#[path = "module_script_tracking_tests.rs"]
mod module_script_tracking_tests;
use crate::engine::extension::module::{
    create_module_cmd_ctx, create_module_ctx, free_module_ctx, ModuleRef, ModuleRegistry,
    ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_OK,
};
use crate::foundation::io::protocol::Value;
pub(crate) use crate::framework::capabilities::runtime::ModuleCommandConnScope;
use crate::framework::context::ServerContext;

pub(crate) struct ModuleInvocationScope {
    module_ctx: NonNull<ValkeyModuleCtx>,
    argv: Vec<*mut ValkeyModuleString>,
    registry: NonNull<ModuleRegistry>,
    runtime_handle: Box<ModuleRuntimeHandle>,
}

impl ModuleInvocationScope {
    pub(crate) fn new(
        ctx: &mut ServerContext,
        module_name: &str,
        command_name: &str,
        db_idx: u32,
        resp_version: u8,
        username: Option<&str>,
        argv: Vec<*mut ValkeyModuleString>,
    ) -> Self {
        let registry = NonNull::from(ctx.module_registry_mut());
        let (module_ref_ptr, module_options) = {
            // SAFETY: registry points into ctx. The shared lookup only copies
            // an identity token and options owner before invoking external code.
            unsafe { registry.as_ref() }
                .lookup(module_name)
                .map(|module| {
                    (
                        (module as *const ModuleRef).cast_mut().cast::<c_void>(),
                        Some(module.options.clone()),
                    )
                })
                .unwrap_or((std::ptr::null_mut(), None))
        };
        let mut runtime_handle = Box::new(ModuleRuntimeHandle::new(ctx));
        let mut internal_ctx = create_module_cmd_ctx(
            module_name,
            command_name,
            module_ref_ptr,
            (&mut *runtime_handle as *mut ModuleRuntimeHandle).cast(),
        );
        internal_ctx.module_options = module_options;
        if let Some(username) = username {
            if !internal_ctx.username.is_null() {
                // SAFETY: create_module_cmd_ctx allocated this CString and the
                // invocation still owns the internal context exclusively.
                unsafe {
                    let _ = CString::from_raw(internal_ctx.username);
                }
            }
            internal_ctx.username = CString::new(username)
                .unwrap_or_else(|_| CString::new("default").unwrap())
                .into_raw();
        }
        internal_ctx.db_idx = db_idx as c_int;
        internal_ctx.resp_version = resp_version as c_int;
        let internal_ptr = Box::into_raw(internal_ctx);
        let mut ctx_box = create_module_ctx(internal_ptr);
        ctx_box.getapifuncptr =
            crate::engine::extension::module::api::module_get_api_impl as *mut c_void;
        let module_ctx =
            NonNull::new(Box::into_raw(ctx_box)).expect("module context allocation is non-null");
        Self {
            module_ctx,
            argv,
            registry,
            runtime_handle,
        }
    }

    pub(crate) fn module_ctx(&self) -> *mut ValkeyModuleCtx {
        self.module_ctx.as_ptr()
    }

    pub(crate) fn argv(&self) -> *mut *mut ValkeyModuleString {
        self.argv.as_ptr() as *mut *mut ValkeyModuleString
    }

    pub(crate) fn argc(&self) -> c_int {
        self.argv.len() as c_int
    }

    pub(crate) fn runtime_handle(&mut self) -> *mut ModuleRuntimeHandle {
        &mut *self.runtime_handle
    }

    pub(crate) fn registry(&self) -> *mut ModuleRegistry {
        self.registry.as_ptr()
    }

    pub(crate) fn take_reply(&mut self) -> Value {
        // SAFETY: module_ctx and its internal reply pointer are owned by this
        // invocation; clearing reply_ptr transfers the builder exactly once.
        unsafe {
            let internal = self.module_ctx.as_ref().internal;
            if internal.is_null() || (*internal).reply_ptr.is_null() {
                return Value::Null;
            }
            let builder = Box::from_raw((*internal).reply_ptr as *mut ReplyBuilder);
            (*internal).reply_ptr = std::ptr::null_mut();
            builder.into_value()
        }
    }
}

impl Drop for ModuleInvocationScope {
    fn drop(&mut self) {
        // SAFETY: registry and module_ctx remain owned by the ServerContext and
        // this invocation respectively until drop completes.
        unsafe {
            free_argv(&mut self.argv);
            free_module_ctx(self.module_ctx.as_ptr());
        }
    }
}

pub fn module_cmd_dispatcher(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    // Inline execution can hold a runtime borrow in its caller. It must not
    // re-enter an ancestor's host dispatcher from that borrowed context.
    let _host_scope =
        crate::engine::extension::module::capability::ActiveModuleHostScope::enter(None);
    let cmd_name = match std::str::from_utf8(&args.name) {
        Ok(name) => name,
        Err(_) => return Value::error("ERR invalid command name encoding"),
    };
    let dispatch_name = module_dispatch_name(cmd_name, args, ctx.module_registry());
    let entry = match ctx.module_registry().lookup_cmd_func(&dispatch_name) {
        Some(entry) => entry.clone(),
        None => return Value::error(format!("ERR module command '{}' not found", dispatch_name)),
    };
    let Some(command_func) = entry.func else {
        return Value::error(format!(
            "ERR unknown subcommand for module command '{}'",
            dispatch_name
        ));
    };
    let argv = module_argv(args, ctx.module_registry());
    let mut invocation = ModuleInvocationScope::new(
        ctx,
        &entry.module_name,
        &dispatch_name,
        conn.selected_db(),
        conn.resp_version(),
        Some(conn.username()),
        argv,
    );
    let module_ctx = invocation.module_ctx();
    let argv = invocation.argv();
    let argc = invocation.argc();
    let runtime_handle = invocation.runtime_handle();
    let registry = invocation.registry();
    let ret = {
        let _conn_scope = ModuleCommandConnScope::enter(conn);
        let _ctx_scope = ActiveModuleContextScope::enter(module_ctx);
        let _runtime_scope =
            ActiveModuleRuntimeScope::enter(runtime_handle).expect("runtime handle is non-null");
        let _scope = ActiveModuleRegistryScope::enter(registry)
            .expect("module registry pointer is non-null");
        command_func(module_ctx, argv, argc)
    };
    let reply = invocation.take_reply();
    if ret != VALKEYMODULE_OK {
        log::warn!(
            "Module command '{}' returned error code {}",
            dispatch_name,
            ret
        );
    }
    reply
}

pub(crate) fn module_dispatch_name(
    cmd_name: &str,
    args: &CommandArgs,
    registry: &ModuleRegistry,
) -> String {
    if let Some(first) = args.args.first() {
        let subcommand_name = format!("{}|{}", cmd_name, String::from_utf8_lossy(first));
        if registry.lookup_cmd_func(&subcommand_name).is_some() {
            return subcommand_name;
        }
    }
    cmd_name.to_string()
}

pub(crate) fn module_argv(
    args: &CommandArgs,
    registry: &ModuleRegistry,
) -> Vec<*mut ValkeyModuleString> {
    let subcommand_name = if args.args.is_empty() {
        None
    } else {
        Some(format!(
            "{}|{}",
            String::from_utf8_lossy(&args.name),
            String::from_utf8_lossy(&args.args[0])
        ))
    };
    let is_subcommand = subcommand_name
        .as_deref()
        .is_some_and(|name| registry.lookup_cmd_func(name).is_some());
    let command_name = if is_subcommand {
        Bytes::from(subcommand_name.expect("checked above"))
    } else {
        args.name.clone()
    };
    std::iter::once(&command_name)
        .chain(args.args.iter())
        .map(|arg| Box::into_raw(ValkeyModuleString::from_bytes(arg)))
        .collect()
}

fn free_argv(argv: &mut Vec<*mut ValkeyModuleString>) {
    for ptr in argv.drain(..) {
        if ptr.is_null() {
            continue;
        }
        // SAFETY: every pointer was allocated by module_argv and is consumed once.
        unsafe {
            ValkeyModuleString::release(ptr);
        }
    }
}

pub(crate) fn invoke_module_timer(
    timer: crate::engine::extension::module::registry::ModuleTimer,
    ctx: &mut ServerContext,
) {
    let _host_scope =
        crate::engine::extension::module::capability::ActiveModuleHostScope::enter(None);
    let mut module_conn =
        crate::engine::extension::module::api::call::call_conn::ModuleCallConn::with_tracking(
            timer.db_idx,
            None,
        );
    let callback = timer.callback;
    let data = timer.data;
    let mut invocation = ModuleInvocationScope::new(
        ctx,
        &timer.module_name,
        "module.timer",
        timer.db_idx,
        2,
        None,
        Vec::new(),
    );
    let module_ctx = invocation.module_ctx();
    let runtime_handle = invocation.runtime_handle();
    let registry = invocation.registry();
    let _conn_scope = ModuleCommandConnScope::enter(&mut module_conn);
    let _ctx_scope = ActiveModuleContextScope::enter(module_ctx);
    let _runtime_scope =
        ActiveModuleRuntimeScope::enter(runtime_handle).expect("runtime handle is non-null");
    let _registry_scope =
        ActiveModuleRegistryScope::enter(registry).expect("module registry pointer is non-null");
    callback(module_ctx, data);
}

pub(crate) fn fire_module_event_loop(ctx: &mut ServerContext, subevent: u64) {
    let _host_scope =
        crate::engine::extension::module::capability::ActiveModuleHostScope::enter(None);
    poll_pending_module_calls(ctx);
    let listeners = ctx
        .module_registry()
        .server_event_listeners(crate::engine::extension::module::VALKEYMODULE_EVENT_EVENTLOOP);
    for listener in listeners {
        let mut module_conn =
            crate::engine::extension::module::api::call::call_conn::ModuleCallConn::with_tracking(
                0, None,
            );
        let mut invocation = ModuleInvocationScope::new(
            ctx,
            &listener.module_name,
            "module.eventloop",
            0,
            2,
            None,
            Vec::new(),
        );
        let module_ctx = invocation.module_ctx();
        let runtime_handle = invocation.runtime_handle();
        let registry = invocation.registry();
        let _conn_scope = ModuleCommandConnScope::enter(&mut module_conn);
        let _ctx_scope = ActiveModuleContextScope::enter(module_ctx);
        let _runtime_scope =
            ActiveModuleRuntimeScope::enter(runtime_handle).expect("runtime handle is non-null");
        let _registry_scope = ActiveModuleRegistryScope::enter(registry)
            .expect("module registry pointer is non-null");
        (listener.callback)(module_ctx, listener.event, subevent, std::ptr::null_mut());
    }
}

pub(crate) fn poll_pending_module_calls(ctx: &mut ServerContext) {
    let ids: Vec<usize> = ctx
        .module_registry()
        .pending_calls
        .keys()
        .copied()
        .collect();
    for id in ids {
        let effects = complete_pending_module_call(ctx, id, None);
        ctx.module_registry_mut().pending_effects.extend(effects);
    }
}

pub(crate) fn complete_pending_module_call(
    ctx: &mut ServerContext,
    id: usize,
    ready_key: Option<(u32, &[u8])>,
) -> Vec<crate::engine::extension::module::api::call::pending::ModuleCallEffects> {
    use crate::engine::command::context::BlockedCmd;
    use crate::engine::extension::module::capability::ModuleRuntime;

    let _host_scope =
        crate::engine::extension::module::capability::ActiveModuleHostScope::enter(None);
    let mut effects = Vec::new();
    let now_ms = crate::foundation::io::sys::time::mstime();
    let Some(mut call) = ctx.module_registry_mut().pending_calls.remove(&id) else {
        return effects;
    };
    let Some(blocked) = call.conn.blocked().cloned() else {
        return effects;
    };
    let is_timed_out = blocked.timeout_ms != 0
        && now_ms.saturating_sub(blocked.start_time_ms) as u64 >= blocked.timeout_ms;
    let has_ready_key = ready_key.is_some_and(|(db_idx, key)| {
        call.conn.selected_db() == db_idx
            && blocked
                .keys
                .iter()
                .any(|candidate| candidate.as_ref() == key)
    });
    let terminal_reply = match blocked.cmd {
        BlockedCmd::Wait {
            required,
            target_offset,
        } => {
            let acked = ctx.replication().count_replicas_acked_to(target_offset);
            (is_timed_out || acked >= required).then(|| Value::integer(acked))
        }
        BlockedCmd::WaitAof {
            numlocal,
            numreplicas,
            target_offset,
        } => {
            let local =
                crate::framework::commands::generic::waitaof::waitaof_local_ack(ctx, target_offset);
            let replicas = ctx.replication().count_replicas_aof_acked_to(target_offset);
            (is_timed_out || (local >= numlocal && replicas >= numreplicas))
                .then(|| Value::Array(vec![Value::integer(local), Value::integer(replicas)]))
        }
        _ => None,
    };
    if terminal_reply.is_none() && !is_timed_out && !has_ready_key {
        ctx.module_registry_mut().pending_calls.insert(id, call);
        return effects;
    }
    call.conn.set_blocked(None);
    let result = if let Some(reply) = terminal_reply {
        reply
    } else if is_timed_out {
        match blocked.cmd {
            BlockedCmd::Blpop
            | BlockedCmd::Brpop
            | BlockedCmd::Bzpopmin
            | BlockedCmd::Bzpopmax
            | BlockedCmd::Bzmpop { .. }
            | BlockedCmd::Blmpop { .. }
            | BlockedCmd::Xread
            | BlockedCmd::Xreadgroup => Value::NullArray,
            _ => Value::Null,
        }
    } else {
        let result =
            ctx.execute_command(&blocked.orig_args.name, &mut call.conn, &blocked.orig_args);
        if let Some(reblocked) = call.conn.blocked().cloned() {
            let mut preserved = reblocked;
            preserved.start_time_ms = blocked.start_time_ms;
            preserved.timeout_ms = blocked.timeout_ms;
            call.conn.set_blocked(Some(preserved));
            let partial = call.conn.take_call_effects(
                &call.command,
                &blocked.orig_args,
                call.format.propagation_targets(),
                false,
            );
            if !partial.is_empty() {
                effects.push(partial);
            }
            // Keep the original queue position when a ready key cannot satisfy this call.
            ctx.module_registry_mut().pending_calls.insert(id, call);
            return effects;
        }
        result
    };
    call.conn.set_blocked(Some(blocked.clone()));
    crate::framework::capabilities::runtime::unregister_pending_module_call(ctx, &call.conn);
    call.conn.set_blocked(None);
    let completed_effects = call.conn.take_call_effects(
        &call.command,
        &blocked.orig_args,
        call.format.propagation_targets(),
        !is_timed_out && !matches!(result, Value::Error(_) | Value::Null | Value::NullArray),
    );
    if !completed_effects.is_empty() {
        effects.push(completed_effects);
    }
    let db_idx = call.conn.selected_db();
    let resp_version = call.conn.resp_version();
    let mut invocation = ModuleInvocationScope::new(
        ctx,
        &call.module_name,
        "module.call-completion",
        db_idx,
        resp_version,
        None,
        Vec::new(),
    );
    let mut callback_conn =
        crate::engine::extension::module::api::call::call_conn::ModuleCallConn::with_tracking(
            db_idx, None,
        );
    {
        let _conn_scope = ModuleCommandConnScope::enter(&mut callback_conn);
        let _ctx_scope = ActiveModuleContextScope::enter(invocation.module_ctx());
        let _runtime_scope = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
        let _registry_scope = ActiveModuleRegistryScope::enter(invocation.registry());
        // SAFETY: the fresh invocation and copied callbacks remain live until completion ends.
        unsafe { call.complete(invocation.module_ctx(), result) };
    }
    drop(invocation);
    let callback_effects = callback_conn.take_recorded_effects();
    if !callback_effects.is_empty() {
        effects.push(callback_effects);
    }
    effects
}

pub(crate) fn load_module(
    path: &str,
    args: &[&[u8]],
    ctx: &mut ServerContext,
) -> Result<String, crate::engine::extension::module::registry::ModuleError> {
    let _host_scope =
        crate::engine::extension::module::capability::ActiveModuleHostScope::enter(None);
    let registry = NonNull::from(ctx.module_registry_mut());
    let mut runtime_handle = Box::new(ModuleRuntimeHandle::new(ctx));
    let _runtime_scope =
        ActiveModuleRuntimeScope::enter(&mut *runtime_handle).expect("runtime handle is non-null");
    // SAFETY: registry points into ctx and runtime_handle borrows that same
    // context only for this synchronous loader invocation. Module callbacks run
    // before either pointer can escape this function.
    unsafe {
        crate::engine::extension::module::loader::ModuleLoader::load_dynamic(
            path,
            args,
            &mut *registry.as_ptr(),
            (&mut *runtime_handle as *mut ModuleRuntimeHandle).cast(),
        )
    }
}

pub(crate) fn unload_module(
    name: &str,
    ctx: &mut ServerContext,
) -> Result<(), crate::engine::extension::module::registry::ModuleError> {
    let _host_scope =
        crate::engine::extension::module::capability::ActiveModuleHostScope::enter(None);
    use crate::engine::extension::module::capability::ModuleRuntime;
    use crate::engine::extension::module::registry::ModuleError;

    let callback = { ctx.module_registry().prepare_unload(name)? };
    if let Some(callback) = callback {
        // Preparation borrows have ended. The invocation owns only allocations
        // and raw handles; no runtime or registry reference spans module code.
        let ret = {
            let mut module_conn =
                crate::engine::extension::module::api::call::call_conn::ModuleCallConn::with_tracking(
                    0, None,
                );
            let mut invocation =
                ModuleInvocationScope::new(ctx, name, "module.unload", 0, 2, None, Vec::new());
            let module_ctx = invocation.module_ctx();
            let runtime_handle = invocation.runtime_handle();
            let registry = invocation.registry();
            let _conn_scope = ModuleCommandConnScope::enter(&mut module_conn);
            let _ctx_scope = ActiveModuleContextScope::enter(module_ctx);
            let _runtime_scope = ActiveModuleRuntimeScope::enter(runtime_handle)
                .expect("runtime handle is non-null");
            let _registry_scope = ActiveModuleRegistryScope::enter(registry)
                .expect("module registry pointer is non-null");
            callback(module_ctx.cast())
        };
        // Invocation drop collects automatic resources while the library and
        // registrations are still live, including when OnUnload refuses.
        if ret != VALKEYMODULE_OK {
            return Err(ModuleError::InitFailed(
                "the module's OnUnload function returned an error".to_string(),
            ));
        }
    }
    let module = { ctx.module_registry_mut().finish_unload(name)? };
    for command_name in &module.commands {
        <ServerContext as ModuleRuntime>::unregister_module_command(ctx, command_name);
    }
    if !module.is_static && !module.handle.is_null() {
        // SAFETY: all callbacks have returned, invocation resources and
        // registrations are gone, and module owns this live dlopen handle.
        unsafe { libc::dlclose(module.handle) };
    }
    Ok(())
}

#[cfg(test)]
#[path = "module_loader_tests.rs"]
mod loader_tests;

#[cfg(test)]
mod tests {
    use super::{fire_module_event_loop, invoke_module_timer};
    use crate::engine::extension::module::capability::record_module_latency;
    use crate::engine::extension::module::registry::ModuleTimer;
    use crate::engine::extension::module::{
        ModuleRef, ValkeyModuleCtx, ValkeyModuleEvent, VALKEYMODULE_EVENT_EVENTLOOP,
        VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP, VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP,
    };
    use crate::framework::context::ServerContext;
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, Weak};

    #[test]
    fn allowed_calls_from_one_host_use_distinct_temporary_identities() {
        use super::ModuleInvocationScope;
        use crate::engine::command::context::CommandConn;
        use crate::engine::extension::module::api::call::VM_CallArgv;
        use crate::engine::extension::module::capability::{
            ActiveModuleRuntimeScope, ModuleRuntime,
        };
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::capabilities::io::Token;
        use crate::framework::capabilities::runtime::ModuleCommandConnScope;
        use bytes::Bytes;

        let mut ctx = ServerContext::new(1);
        let mut registry = crate::framework::CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        ctx.set_command_registry_snapshot(&registry);
        let mut outer = Connection::new_fake();
        outer.set_token_for_test(Token(42));
        let outer_session = outer.session_id();
        ctx.blocking_registry_mut().register_client(
            0,
            &[Bytes::from_static(b"outer")],
            outer_session,
        );
        let mut invocation = ModuleInvocationScope::new(
            &mut ctx,
            "pending-test",
            "pending.call",
            0,
            2,
            None,
            Vec::new(),
        );
        {
            let _conn_scope = ModuleCommandConnScope::enter(&mut outer);
            let _runtime_scope = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
            let _ctx_scope = crate::engine::extension::module::api::ActiveModuleContextScope::enter(
                invocation.module_ctx(),
            );
            for name in [b"first".as_slice(), b"second".as_slice()] {
                let mut argv: Vec<_> = [b"BLPOP".as_slice(), name, b"0"]
                    .into_iter()
                    .map(|value| {
                        Box::into_raw(
                            crate::engine::extension::module::ValkeyModuleString::from_bytes(value),
                        )
                    })
                    .collect();
                // SAFETY: the invocation owns the context; argv contains live caller-owned strings.
                unsafe {
                    assert_eq!(
                        VM_CallArgv(
                            invocation.module_ctx(),
                            argv.as_mut_ptr(),
                            argv.len() as libc::c_int,
                            (1 << 11) | (1 << 8),
                            std::ptr::null(),
                            std::ptr::null_mut(),
                        ),
                        crate::engine::extension::module::VALKEYMODULE_OK
                    );
                    for string in argv {
                        crate::engine::extension::module::ValkeyModuleString::release(string);
                    }
                }
            }
        }
        drop(invocation);
        assert_eq!(ctx.module_registry().pending_calls.len(), 2);
        assert_eq!(ctx.blocking_registry().count(), 3);
        let ids: Vec<_> = ctx
            .module_registry()
            .pending_calls
            .keys()
            .copied()
            .collect();
        assert_ne!(ids[0], ids[1]);
        for id in ids {
            assert_ne!(id, outer.token().0);
            assert!(ctx.abort_pending_module_call(id));
            assert!(!ctx.abort_pending_module_call(id));
        }
        assert!(ctx.blocking_registry().is_client_blocked(&outer_session));
        assert_eq!(ctx.blocking_registry().count(), 1);
        assert!(outer.blocked().is_none());
        assert_eq!(outer.output_buffer_bytes(), 0);
    }

    #[test]
    fn module_server_info_exports_owned_replication_snapshot() {
        use crate::engine::distributed::replication::{ReplicationOffset, ReplicationRole};
        use crate::engine::extension::module::api::call::{
            ValkeyModule_FreeServerInfo, ValkeyModule_GetServerInfo,
            ValkeyModule_ServerInfoGetFieldC, ValkeyModule_ServerInfoGetFieldSigned,
        };
        use crate::engine::extension::module::capability::{
            ActiveModuleRuntimeScope, ModuleRuntimeHandle,
        };

        let mut ctx = ServerContext::new(1);
        ctx.replication_mut().role = ReplicationRole::Replica;
        ctx.replication_mut().slave_read_repl_offset = ReplicationOffset::try_from(123).unwrap();
        let mut handle = ModuleRuntimeHandle::new(&mut ctx);
        let _scope = ActiveModuleRuntimeScope::enter(&mut handle).unwrap();
        // SAFETY: strings are static C strings; the returned snapshot is freed once.
        unsafe {
            let info = ValkeyModule_GetServerInfo(std::ptr::null_mut(), c"replication".as_ptr());
            assert!(!info.is_null());
            let value = ValkeyModule_ServerInfoGetFieldC(info, c"slave_read_repl_offset".as_ptr());
            assert!(
                !value.is_null(),
                "replication INFO must contain the real read offset"
            );
            ctx.replication_mut().slave_read_repl_offset =
                ReplicationOffset::try_from(456).unwrap();
            assert_eq!(std::ffi::CStr::from_ptr(value).to_bytes(), b"123");
            let mut err = -1;
            assert_eq!(
                ValkeyModule_ServerInfoGetFieldSigned(
                    info,
                    c"slave_read_repl_offset".as_ptr(),
                    &mut err
                ),
                123
            );
            assert_eq!(err, 0);
            assert_eq!(
                ValkeyModule_ServerInfoGetFieldSigned(info, c"role".as_ptr(), &mut err),
                0
            );
            assert_eq!(err, 1);
            assert!(ValkeyModule_ServerInfoGetFieldC(info, c"mem_allocator".as_ptr()).is_null());
            ValkeyModule_FreeServerInfo(std::ptr::null_mut(), info);
        }
    }

    thread_local! {
        static CAPTURED_NAMES: RefCell<Vec<Weak<str>>> = const { RefCell::new(Vec::new()) };
        static TIMER_CALLBACK_PROBE: RefCell<Option<TimerCallbackProbe>> = const { RefCell::new(None) };
    }

    struct TimerCallbackProbe {
        id: u64,
        should_stop: bool,
        was_registered: bool,
        stopped_data: Option<*mut libc::c_void>,
        calls: usize,
    }

    extern "C" fn inspect_timer_registration(_ctx: *mut ValkeyModuleCtx, _data: *mut libc::c_void) {
        use crate::engine::extension::module::api::with_active_registry;

        TIMER_CALLBACK_PROBE.with(|probe| {
            let mut probe = probe.borrow_mut();
            let probe = probe.as_mut().unwrap();
            probe.calls += 1;
            probe.was_registered = with_active_registry(|registry| {
                registry
                    .timer_info("timer-callback-probe", probe.id)
                    .is_some()
            }) == Some(true);
            if probe.should_stop {
                probe.stopped_data = with_active_registry(|registry| {
                    registry.stop_timer("timer-callback-probe", probe.id)
                })
                .flatten();
            }
        });
    }

    #[test]
    fn module_timer_callback_keeps_registration_until_callback_completion() {
        use crate::foundation::io::sys::time::MonotonicInstant;
        use std::time::Duration;

        for should_stop in [false, true] {
            let mut ctx = ServerContext::new(1);
            ctx.module_registry_mut()
                .register(ModuleRef::new(
                    "timer-callback-probe",
                    std::ptr::null_mut(),
                    1,
                    true,
                ))
                .unwrap();
            let mut data = 0u8;
            let data_ptr = (&mut data as *mut u8).cast();
            let id = ctx
                .module_registry_mut()
                .create_timer(
                    "timer-callback-probe",
                    0,
                    0,
                    inspect_timer_registration,
                    data_ptr,
                )
                .unwrap();
            TIMER_CALLBACK_PROBE.with(|probe| {
                *probe.borrow_mut() = Some(TimerCallbackProbe {
                    id,
                    should_stop,
                    was_registered: false,
                    stopped_data: None,
                    calls: 0,
                });
            });
            ctx.run_expired_module_timers(
                MonotonicInstant::now().saturating_add(Duration::from_secs(1)),
            );
            TIMER_CALLBACK_PROBE.with(|probe| {
                let probe = probe.borrow_mut().take().unwrap();
                assert!(probe.was_registered);
                assert_eq!(probe.calls, 1);
                assert_eq!(probe.stopped_data, should_stop.then_some(data_ptr));
            });
            assert_eq!(
                ctx.module_registry().timer_info("timer-callback-probe", id),
                None
            );
            ctx.run_expired_module_timers(
                MonotonicInstant::now().saturating_add(Duration::from_secs(2)),
            );
        }
    }

    fn capture_command_names(
        _conn: &mut dyn crate::engine::command::context::CommandConn,
        args: &crate::engine::command::CommandArgs,
        ctx: &mut ServerContext,
    ) -> crate::foundation::io::protocol::Value {
        let prefix = std::str::from_utf8(&args.args[0]).unwrap();
        let names = ctx
            .command_registry()
            .iter()
            .filter(|spec| spec.name.as_str().starts_with(prefix))
            .map(|spec| match &spec.name {
                crate::engine::command::CommandName::Shared(owner) => Arc::downgrade(owner),
                crate::engine::command::CommandName::Static(_) => {
                    panic!("module command names must be shared")
                }
            })
            .collect();
        CAPTURED_NAMES.with(|captured| *captured.borrow_mut() = names);
        crate::foundation::io::protocol::Value::ok()
    }

    #[test]
    fn module_names_release_on_unload_and_failed_onload_rollback() {
        use crate::engine::command::CommandName;
        use crate::framework::{CommandExecutor, CommandRegistry};
        use std::path::PathBuf;
        use std::process::Command;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let dir =
            std::env::temp_dir().join(format!("versal-name-lifecycle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let library = dir.join("name_lifecycle.so");
        let mut compiler = Command::new(std::env::var_os("CC").unwrap_or_else(|| "clang".into()));
        if cfg!(target_os = "macos") {
            compiler.args(["-dynamiclib", "-undefined", "dynamic_lookup"]);
        } else {
            compiler.args(["-shared", "-fPIC"]);
        }
        if cfg!(target_os = "linux") {
            compiler.arg("-fuse-ld=lld");
        }
        let output = compiler
            .arg("-I")
            .arg(root.join("thirdparty/valkey/src"))
            .arg(root.join("tests/fixtures/module_ffi/name_lifecycle.c"))
            .arg("-o")
            .arg(&library)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut registry = Box::new(CommandRegistry::new());
        let mut capture = crate::framework::commands::string::get::GET_SPEC;
        capture.name = CommandName::Static("name.capture");
        capture.first_key = 0;
        capture.last_key = 0;
        capture.executor = CommandExecutor::Immediate(capture_command_names);
        registry.register(capture);
        let mut ctx = ServerContext::new(1);
        ctx.set_command_registry_snapshot(&registry);
        ctx.set_module_command_registry(&mut *registry);
        ctx.register_monitor_client(7);

        for index in 0..6 {
            let prefix = format!("NameMixed{}", index / 2);
            let should_fail = index % 2 != 0;
            let mut args = vec![prefix.as_bytes()];
            if should_fail {
                args.push(b"fail");
            }
            let result = super::load_module(library.to_str().unwrap(), &args, &mut ctx);
            let names = CAPTURED_NAMES.with(|captured| std::mem::take(&mut *captured.borrow_mut()));
            assert_eq!(names.len(), 3);
            assert!(!ctx.script().pending_internal_monitor.is_empty());
            ctx.script_mut().pending_internal_monitor.clear();
            if should_fail {
                assert!(result.is_err());
                assert!(names.iter().all(|name| name.upgrade().is_none()));
                assert!(ctx.module_registry().lookup("name-lifecycle").is_none());
                continue;
            }
            assert_eq!(result.unwrap(), "name-lifecycle");
            let run = format!("{prefix}.Run");
            let parent = format!("{prefix}.Root");
            let child = format!("{parent}|Leaf");
            let spec = registry.lookup(run.as_bytes()).unwrap().clone();
            let resolved = registry.resolve(child.as_bytes()).unwrap();
            let descriptor = resolved.descriptor();
            let snapshot = ctx.command_registry().clone();
            registry.rename(&run, "name.alias");
            ctx.script_mut().command_registry.rename(&parent, "");

            super::unload_module("name-lifecycle", &mut ctx).unwrap();
            for name in [&run, &parent, &child, "name.alias"] {
                assert!(registry.lookup(name.as_bytes()).is_none());
                assert!(registry
                    .lookup_original(&name.to_ascii_lowercase())
                    .is_none());
                assert!(ctx.command_registry().lookup(name.as_bytes()).is_none());
                assert!(ctx
                    .command_registry()
                    .lookup_original(&name.to_ascii_lowercase())
                    .is_none());
            }
            assert!(registry.executor(resolved.clone()).is_none());
            assert!(names.iter().all(|name| name.upgrade().is_some()));
            assert_eq!(spec.name.as_str(), run);
            assert_eq!(descriptor.name.as_str(), child);
            drop(snapshot);
            assert_eq!(
                names.iter().filter(|name| name.upgrade().is_some()).count(),
                2
            );
            assert_eq!(resolved.descriptor().name.as_str(), child);
            drop(spec);
            drop(descriptor);
            drop(resolved);
            assert!(names.iter().all(|name| name.upgrade().is_none()));
        }
        ctx.set_module_command_registry(std::ptr::null_mut());
        drop(ctx);
        std::fs::remove_dir_all(dir).unwrap();
    }

    static TIMER_RAN_IN_RUNTIME_SCOPE: AtomicBool = AtomicBool::new(false);
    static EVENT_RAN_IN_RUNTIME_SCOPE: AtomicBool = AtomicBool::new(false);
    static EVENT_PHASES: Mutex<Vec<u64>> = Mutex::new(Vec::new());

    extern "C" fn record_latency_timer(
        ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
        _data: *mut libc::c_void,
    ) {
        TIMER_RAN_IN_RUNTIME_SCOPE.store(
            !ctx.is_null() && record_module_latency("module-timer", 7),
            Ordering::SeqCst,
        );
    }

    extern "C" fn event_loop_callback(
        ctx: *mut ValkeyModuleCtx,
        event: ValkeyModuleEvent,
        subevent: u64,
        data: *mut libc::c_void,
    ) {
        EVENT_RAN_IN_RUNTIME_SCOPE.store(
            !ctx.is_null()
                && event.id == VALKEYMODULE_EVENT_EVENTLOOP
                && data.is_null()
                && record_module_latency("module-event-loop", 7),
            Ordering::SeqCst,
        );
        EVENT_PHASES.lock().unwrap().push(subevent);
    }

    #[test]
    fn module_timer_runs_with_scoped_context_and_runtime() {
        TIMER_RAN_IN_RUNTIME_SCOPE.store(false, Ordering::SeqCst);
        let mut ctx = ServerContext::new(1);
        ctx.latency_mut().set_threshold(1);
        let timer = ModuleTimer::for_test(
            "timer-fixture",
            0,
            record_latency_timer,
            std::ptr::null_mut(),
        );

        invoke_module_timer(timer, &mut ctx);

        assert!(TIMER_RAN_IN_RUNTIME_SCOPE.load(Ordering::SeqCst));
        assert_eq!(ctx.latency().history("module-timer").len(), 1);
        assert!(!record_module_latency("outside-scope", 9));
    }

    #[test]
    fn event_loop_callbacks_run_in_registration_order_with_scoped_runtime() {
        EVENT_RAN_IN_RUNTIME_SCOPE.store(false, Ordering::SeqCst);
        EVENT_PHASES.lock().unwrap().clear();
        let mut ctx = ServerContext::new(1);
        ctx.latency_mut().set_threshold(1);
        ctx.module_registry_mut()
            .register(ModuleRef::new(
                "event-loop-fixture",
                std::ptr::null_mut(),
                1,
                true,
            ))
            .unwrap();
        assert!(ctx.module_registry_mut().subscribe_server_event(
            "event-loop-fixture",
            ValkeyModuleEvent {
                id: VALKEYMODULE_EVENT_EVENTLOOP,
                dataver: 1,
            },
            Some(event_loop_callback),
        ));

        fire_module_event_loop(&mut ctx, VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP);
        fire_module_event_loop(&mut ctx, VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP);

        assert_eq!(
            *EVENT_PHASES.lock().unwrap(),
            vec![
                VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP,
                VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP,
            ]
        );
        assert!(EVENT_RAN_IN_RUNTIME_SCOPE.load(Ordering::SeqCst));
        let latest = ctx.latency().latest();
        let event = latest
            .iter()
            .find(|event| event.0 == "module-event-loop")
            .expect("both callbacks must record module latency");
        assert_eq!((event.2, event.3, event.4, event.5), (7, 7, 14, 2));
        assert!(!record_module_latency("outside-event-scope", 9));
    }
}
