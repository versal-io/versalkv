use bytes::Bytes;
use libc::{c_int, c_void};
use std::ffi::CString;
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::engine::command::context::CommandConn;
use crate::engine::command::CommandArgs;
use crate::engine::extension::module::api::{
    ActiveModuleContextScope, ActiveModuleRegistryScope, ReplyBuilder,
};
use crate::engine::extension::module::capability::{ActiveModuleRuntimeScope, ModuleRuntimeHandle};
use crate::engine::extension::module::{
    create_module_cmd_ctx, create_module_ctx, free_module_ctx, ModuleRef, ModuleRegistry,
    ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_OK,
};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::runtime::ModuleCommandConnScope;
use crate::framework::context::ServerContext;

pub(crate) struct ModuleInvocationScope<'a> {
    module_ctx: NonNull<ValkeyModuleCtx>,
    argv: Vec<*mut ValkeyModuleString>,
    registry: NonNull<ModuleRegistry>,
    runtime_handle: Box<ModuleRuntimeHandle>,
    _context: PhantomData<&'a mut ServerContext>,
}

impl<'a> ModuleInvocationScope<'a> {
    fn new(
        ctx: &'a mut ServerContext,
        module_name: &str,
        command_name: &str,
        db_idx: u32,
        resp_version: u8,
        username: Option<&str>,
        argv: Vec<*mut ValkeyModuleString>,
    ) -> Self {
        let registry = NonNull::from(ctx.module_registry_mut());
        // SAFETY: registry points into ctx and remains live for the invocation.
        let module_ref_ptr = unsafe { registry.as_ref() }
            .lookup(module_name)
            .map(|module| module as *const ModuleRef as *mut c_void)
            .unwrap_or(std::ptr::null_mut());
        let mut runtime_handle = Box::new(ModuleRuntimeHandle::new(ctx));
        let mut internal_ctx = create_module_cmd_ctx(
            module_name,
            command_name,
            module_ref_ptr,
            (&mut *runtime_handle as *mut ModuleRuntimeHandle).cast(),
        );
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
        let module_ctx = NonNull::new(Box::into_raw(create_module_ctx(internal_ptr)))
            .expect("module context allocation is non-null");
        Self {
            module_ctx,
            argv,
            registry,
            runtime_handle,
            _context: PhantomData,
        }
    }

    fn module_ctx(&self) -> *mut ValkeyModuleCtx {
        self.module_ctx.as_ptr()
    }

    fn argv(&self) -> *mut *mut ValkeyModuleString {
        self.argv.as_ptr() as *mut *mut ValkeyModuleString
    }

    fn argc(&self) -> c_int {
        self.argv.len() as c_int
    }

    fn runtime_handle(&mut self) -> *mut ModuleRuntimeHandle {
        &mut *self.runtime_handle
    }

    fn registry(&self) -> *mut ModuleRegistry {
        self.registry.as_ptr()
    }

    fn take_reply(&mut self) -> Value {
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

impl Drop for ModuleInvocationScope<'_> {
    fn drop(&mut self) {
        // SAFETY: registry and module_ctx remain owned by the ServerContext and
        // this invocation respectively until drop completes.
        unsafe {
            free_argv(&mut self.argv, self.registry.as_ref());
            free_module_ctx(self.module_ctx.as_ptr());
        }
    }
}

pub fn module_cmd_dispatcher(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
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

fn module_dispatch_name(cmd_name: &str, args: &CommandArgs, registry: &ModuleRegistry) -> String {
    if let Some(first) = args.args.first() {
        let subcommand_name = format!("{}|{}", cmd_name, String::from_utf8_lossy(first));
        if registry.lookup_cmd_func(&subcommand_name).is_some() {
            return subcommand_name;
        }
    }
    cmd_name.to_string()
}

fn module_argv(args: &CommandArgs, registry: &ModuleRegistry) -> Vec<*mut ValkeyModuleString> {
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

fn free_argv(argv: &mut Vec<*mut ValkeyModuleString>, registry: &ModuleRegistry) {
    for ptr in argv.drain(..) {
        if ptr.is_null() {
            continue;
        }
        if registry.is_string_retained(ptr) {
            continue;
        }
        // SAFETY: every pointer was allocated by module_argv and is consumed once.
        unsafe {
            let mut value = Box::from_raw(ptr);
            value.free();
        }
    }
}

pub(crate) fn invoke_module_timer(
    timer: crate::engine::extension::module::registry::ModuleTimer,
    ctx: &mut ServerContext,
) {
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

pub(crate) fn load_module(
    path: &str,
    args: &[&[u8]],
    ctx: &mut ServerContext,
) -> Result<String, crate::engine::extension::module::registry::ModuleError> {
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
    use crate::engine::extension::module::capability::ModuleRuntime;

    let command_names = ctx
        .module_registry()
        .lookup(name)
        .map(|module| module.commands.clone())
        .unwrap_or_default();
    ctx.module_registry_mut().unload(name)?;
    for command_name in command_names {
        <ServerContext as ModuleRuntime>::unregister_module_command(ctx, &command_name);
    }
    Ok(())
}

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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

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
