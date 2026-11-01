#![allow(non_snake_case)]

use bytes::Bytes;
use libc::{c_char, c_double, c_int, c_longlong, c_void, size_t};
use std::cell::Cell;
use std::ffi::{CStr, CString};

use super::super::super::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::call_conn::ModuleCallConn;
use super::call_reply::{
    finish_call_reply, CallReplyKind, CallReplyNode, ValkeyModuleCallReply,
    ValkeyModule_FreeCallReply,
};
use crate::engine::command::context::CommandConn;
use crate::engine::command::{
    CommandArgs, CMD_DENYOOM, CMD_MAY_REPLICATE, CMD_NOSCRIPT, CMD_STALE, CMD_WRITE,
};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::engine::extension::module::capability::runtime_from_opaque;
use crate::foundation::io::protocol::Value;

thread_local! {
    static IN_MODULE_NOTIFICATION: Cell<bool> = const { Cell::new(false) };
}

const CALL_REPLICATE: c_int = 1 << 0;
const CALL_NO_AOF: c_int = 1 << 1;
const CALL_NO_REPLICAS: c_int = 1 << 2;
const CALL_RESP3: c_int = 1 << 3;
const CALL_RESP_AUTO: c_int = 1 << 4;
const CALL_RUN_AS_USER: c_int = 1 << 5;
const CALL_SCRIPT_MODE: c_int = 1 << 6;
const CALL_NO_WRITES: c_int = 1 << 7;
const CALL_ERRORS_AS_REPLIES: c_int = 1 << 8;
const CALL_RESPECT_DENY_OOM: c_int = 1 << 9;
const CALL_DRY_RUN: c_int = 1 << 10;
const CALL_ALLOW_BLOCK: c_int = 1 << 11;
const CALL_REPLY_EXACT: c_int = 1 << 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ModuleCallResponse {
    Resp2,
    Resp3,
    Auto,
}

impl ModuleCallResponse {
    fn version(self, client_version: u8) -> u8 {
        match self {
            Self::Resp2 => 2,
            Self::Resp3 => 3,
            Self::Auto => client_version,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ModuleCallFormat(c_int);

impl ModuleCallFormat {
    pub(crate) fn from_raw(raw: c_int) -> Self {
        Self(raw)
    }

    fn has(self, flag: c_int) -> bool {
        self.0 & flag != 0
    }

    pub(crate) fn response(self) -> ModuleCallResponse {
        if self.has(CALL_RESP3) {
            ModuleCallResponse::Resp3
        } else if self.has(CALL_RESP_AUTO) {
            ModuleCallResponse::Auto
        } else {
            ModuleCallResponse::Resp2
        }
    }

    pub(crate) fn run_as_user(self) -> bool {
        self.has(CALL_RUN_AS_USER)
    }
    pub(crate) fn replicate(self) -> bool {
        self.has(CALL_REPLICATE)
    }
    pub(crate) fn propagation_targets(
        self,
    ) -> crate::engine::extension::script::PropagationTargets {
        use crate::engine::extension::script::PropagationTargets;
        match (
            self.replicate() && !self.has(CALL_NO_AOF),
            self.replicate() && !self.has(CALL_NO_REPLICAS),
        ) {
            (true, true) => PropagationTargets::Both,
            (true, false) => PropagationTargets::Aof,
            (false, true) => PropagationTargets::Replication,
            (false, false) => PropagationTargets::None,
        }
    }
    pub(crate) fn errors_as_replies(self) -> bool {
        self.has(CALL_ERRORS_AS_REPLIES) || self.dry_run()
    }
    pub(crate) fn script_mode(self) -> bool {
        self.has(CALL_SCRIPT_MODE)
    }
    pub(crate) fn no_writes(self) -> bool {
        self.has(CALL_NO_WRITES)
    }
    pub(crate) fn respect_deny_oom(self) -> bool {
        self.has(CALL_RESPECT_DENY_OOM)
    }
    pub(crate) fn dry_run(self) -> bool {
        self.has(CALL_DRY_RUN)
    }
    pub(crate) fn allow_block(self) -> bool {
        self.has(CALL_ALLOW_BLOCK)
    }
    pub(crate) fn reply_exact(self) -> bool {
        self.has(CALL_REPLY_EXACT) || self.response() == ModuleCallResponse::Resp3
    }
}

#[no_mangle]
// SAFETY: Called by Valkey module C code; ctx must be non-null and point to a valid ValkeyModuleCtx,
// argv must be a valid array of argc ValkeyModuleString pointers. The caller guarantees the lifetime
// of these pointers is valid for the duration of the call.
pub unsafe extern "C" fn VM_CallWithReply(
    ctx: *mut ValkeyModuleCtx,
    argv: *mut *mut ValkeyModuleString,
    argc: c_int,
    flags: c_int,
) -> *mut ValkeyModuleCallReply {
    // SAFETY: the internal implementation only borrows the caller's argv.
    let reply = unsafe { call_with_reply(ctx, argv, argc, flags, None) };
    if reply.is_null() {
        return reply;
    }
    // SAFETY: every non-null internal result is a fresh owned root.
    unsafe { finish_call_reply(ctx, Box::from_raw(reply)) }
}

// SAFETY: ctx and argv follow VM_CallWithReply's synchronous borrowed-input contract.
unsafe fn call_with_reply(
    ctx: *mut ValkeyModuleCtx,
    argv: *mut *mut ValkeyModuleString,
    argc: c_int,
    flags: c_int,
    pending_conn: Option<&mut Option<(ModuleCallConn, crate::engine::command::CommandDescriptor)>>,
) -> *mut ValkeyModuleCallReply {
    if ctx.is_null() || argv.is_null() || argc < 1 {
        log::warn!("VM_CallWithReply: invalid args");
        return std::ptr::null_mut();
    }

    let format = ModuleCallFormat::from_raw(flags);
    // SAFETY: ctx was checked non-null and the ABI contract guarantees a live context.
    let internal = unsafe { (*ctx).internal };
    if internal.is_null() {
        log::warn!("VM_CallWithReply: no internal context");
        return std::ptr::null_mut();
    }
    // SAFETY: internal belongs to the live context for this synchronous invocation.
    let runtime_handle = unsafe { (*internal).runtime_handle };
    let (cmd_name_lower, spec, cmd_args, mut conn, reply) = {
        // SAFETY: the live handle is exclusively accessed until this scope ends.
        let Some(runtime) = (unsafe { runtime_from_opaque(runtime_handle) }) else {
            log::warn!("VM_CallWithReply: no module runtime");
            return std::ptr::null_mut();
        };

        // SAFETY: argc is positive and argv points to that many live module strings.
        let cmd_name_bytes = unsafe { (**argv).as_bytes().to_vec() };
        let cmd_name_lower: Vec<u8> = cmd_name_bytes
            .iter()
            .map(|&b| b.to_ascii_lowercase())
            .collect();

        let cmd_name_raw = String::from_utf8_lossy(&cmd_name_bytes).to_string();

        let spec = match runtime.lookup_command(&cmd_name_lower) {
            Some(s) => s,
            None => {
                return Box::into_raw(ValkeyModuleCallReply::from_value(
                    &Value::error(format!(
                        "ERR unknown command '{}', with args beginning with: ",
                        cmd_name_raw
                    )),
                    false,
                ));
            }
        };

        let cmd_name_lower_str = String::from_utf8_lossy(&cmd_name_lower);
        // SAFETY: argv contains argc live module-string pointers for this call.
        let parts: Vec<Bytes> = unsafe {
            (0..argc as usize)
                .map(|i| Bytes::copy_from_slice((**argv.add(i)).as_bytes()))
                .collect()
        };
        let cmd_args = CommandArgs::from_parts(parts);
        if !valid_arity(spec.arity, cmd_args.args.len() as i32 + 1) {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
                &Value::error(format!(
                    "ERR wrong number of arguments for '{}' command",
                    spec.name
                )),
                false,
            ));
        }

        if format.script_mode() && spec.flags & CMD_NOSCRIPT != 0 {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
                &Value::error(format!(
                    "ERR command '{}' is not allowed on script mode",
                    cmd_name_lower_str
                )),
                false,
            ));
        }
        if format.no_writes() && spec.flags & CMD_WRITE != 0 {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
                &Value::error(format!(
                    "ERR Write command '{}' was called while write is not allowed.",
                    cmd_name_lower_str
                )),
                false,
            ));
        }
        if format.script_mode() && runtime.is_stale_replica() && spec.flags & CMD_STALE == 0 {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
            &Value::error(
                "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.",
            ),
            false,
        ));
        }
        if format.script_mode()
            && spec.flags & CMD_WRITE != 0
            && !runtime.has_enough_replicas_to_write()
        {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
                &Value::error("NOREPLICAS Not enough good replicas to write."),
                false,
            ));
        }
        if format.script_mode()
            && spec.flags & CMD_WRITE != 0
            && runtime.has_persistence_write_error()
        {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
            &Value::error(
                "MISCONF Errors writing to the RDB file, writes are disabled because stop-writes-on-bgsave-error is enabled.",
            ),
            false,
        ));
        }
        if format.script_mode() && spec.flags & CMD_WRITE != 0 && runtime.is_readonly_replica() {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
                &Value::error("READONLY You can't write against a read only replica."),
                false,
            ));
        }
        if format.respect_deny_oom() && spec.flags & CMD_DENYOOM != 0 && runtime.is_oom() {
            return Box::into_raw(ValkeyModuleCallReply::from_value(
                &Value::error("OOM command not allowed when used memory > 'maxmemory'."),
                false,
            ));
        }
        if format.replicate()
            && spec.flags & (CMD_WRITE | CMD_MAY_REPLICATE) != 0
            && runtime.has_paused_writes()
        {
            crate::foundation::io::sys::os::set_errno(libc::ENOSPC);
            if format.errors_as_replies() {
                return Box::into_raw(ValkeyModuleCallReply::from_value(
                    &Value::error(format!(
                        "ERR Write command '{}' was called while the server is paused for writes.",
                        spec.name
                    )),
                    false,
                ));
            }
            return std::ptr::null_mut();
        }
        if format.run_as_user() {
            if let Some(error) = runtime.module_acl_error(spec.clone(), &cmd_args) {
                return Box::into_raw(ValkeyModuleCallReply::from_value(
                    &Value::error(error),
                    false,
                ));
            }
        }
        if format.dry_run() {
            return std::ptr::null_mut();
        }

        // SAFETY: internal was obtained from the validated live context above.
        let (db_idx, client_resp) = unsafe { ((*internal).db_idx, (*internal).resp_version) };

        let tracking = runtime.module_client_tracking();
        let mut conn = ModuleCallConn::with_tracking(db_idx as u32, tracking);
        if format.allow_block() && pending_conn.is_some() {
            conn.use_temporary_identity();
        }
        conn.set_allow_oom(!format.respect_deny_oom());
        conn.set_deny_blocking(!format.allow_block());
        let response_version = format.response().version(client_resp as u8);
        conn.set_resp_version(response_version);

        let reply = runtime.execute_command(&cmd_name_lower, &mut conn, &cmd_args);
        let monitor_argv = std::iter::once(cmd_args.name.clone())
            .chain(cmd_args.args.iter().enumerate().map(|(index, argument)| {
                if conn.is_redacted_command_argument(index + 1) {
                    Bytes::from_static(b"(redacted)")
                } else {
                    argument.clone()
                }
            }))
            .collect();
        let monitor_source = if format.script_mode() { "lua" } else { "" };
        runtime.record_internal_monitor(
            spec.clone(),
            conn.selected_db(),
            monitor_source,
            monitor_argv,
        );
        (cmd_name_lower, spec, cmd_args, conn, reply)
    };

    if !IN_MODULE_NOTIFICATION.with(Cell::get) {
        let event = module_keyspace_event(&cmd_name_lower, &cmd_args, &reply);
        if let Some((event_type, event_name, key)) = event {
            if let Ok(event_name) = CString::new(event_name) {
                let key = Box::into_raw(ValkeyModuleString::from_bytes(key));
                IN_MODULE_NOTIFICATION.with(|active| {
                    active.set(true);
                    // SAFETY: context and host key remain live throughout dispatch.
                    unsafe { dispatch_keyspace_callbacks(ctx, event_type, &event_name, key) };
                    active.set(false);
                });
                // SAFETY: key was allocated immediately above and is no longer
                // referenced after all callbacks return.
                unsafe {
                    ValkeyModuleString::release(key);
                }
            }
        }
    }
    {
        // SAFETY: the first runtime borrow ended before notification dispatch, and
        // all synchronous callbacks have returned before this exclusive reborrow.
        let Some(runtime) = (unsafe { runtime_from_opaque(runtime_handle) }) else {
            log::warn!("VM_CallWithReply: no module runtime");
            return std::ptr::null_mut();
        };
        for (channel, message) in conn.take_pending_notifications() {
            runtime.publish_module_message(channel, message, false);
        }
        for (channel, message) in conn.take_pending_shard_notifications() {
            runtime.publish_module_message(channel, message, true);
        }
        if spec.flags & CMD_WRITE == 0 && spec.flags & crate::engine::command::CMD_READONLY != 0 {
            let should_track = conn.tracking()
                && !conn.tracking_bcast()
                && (!conn.tracking_optin() || conn.tracking_caching())
                && (!conn.tracking_optout() || !conn.tracking_caching());
            conn.set_tracking_caching(false);
            if should_track {
                if let Some(keys) = spec.extract_keys(&cmd_args) {
                    let client_id = conn.tracking_client_id();
                    for key in keys {
                        runtime.remember_module_tracked_key(client_id, &key);
                    }
                }
            }
        }
    }

    let reply_is_resp3 = conn.resp_version() == 3;
    let effects = conn.take_call_effects(
        &spec,
        &cmd_args,
        format.propagation_targets(),
        !matches!(reply, Value::Error(_)),
    );
    if !effects.is_empty() {
        // SAFETY: command execution and notification callbacks ended before this reborrow.
        if let Some(runtime) = unsafe { runtime_from_opaque(runtime_handle) } {
            runtime.apply_module_call_effects(effects);
        }
    }
    if conn.blocked().is_some() {
        if let Some(pending_conn) = pending_conn {
            *pending_conn = Some((conn, spec));
            return std::ptr::null_mut();
        }
    }
    if let Some(raw_reply) = conn.take_raw_reply() {
        return Box::into_raw(ValkeyModuleCallReply::from_raw_proto_with_resp(
            raw_reply,
            reply_is_resp3,
        ));
    }

    match ValkeyModuleCallReply::try_from_value_with_resp(
        &reply,
        format.reply_exact(),
        reply_is_resp3,
    ) {
        Ok(reply) => Box::into_raw(reply),
        Err(error) => {
            log::warn!("VM_CallWithReply: {error}");
            std::ptr::null_mut()
        }
    }
}

// SAFETY: source and key are live for all synchronous callbacks.
unsafe fn dispatch_keyspace_callbacks(
    source: *mut ValkeyModuleCtx,
    event_type: c_int,
    event: &CString,
    key: *mut ValkeyModuleString,
) {
    use crate::engine::extension::module::{
        create_module_cmd_ctx, create_module_ctx, free_module_ctx,
    };
    // SAFETY: source is a live context; preserve the existing per-module dispatch boundary.
    let module_name = unsafe { super::super::ctx::get_module_name(source) };
    let callbacks = super::super::active_registry::with_active_registry(|registry| {
        registry.keyspace_callbacks(&module_name, event_type)
    })
    .unwrap_or_default();
    // SAFETY: copy inherited state before invoking any reentrant module callback.
    let (runtime_handle, db_idx, resp_version, getapi, username, module_options) = unsafe {
        let internal = (*source).internal;
        let username = if (*internal).username.is_null() {
            CString::new("default").expect("static username has no NUL")
        } else {
            CStr::from_ptr((*internal).username).to_owned()
        };
        (
            (*internal).runtime_handle,
            (*internal).db_idx,
            (*internal).resp_version,
            (*source).getapifuncptr,
            username,
            (*internal).module_options.clone(),
        )
    };
    for (name, module_ref, callback) in callbacks {
        let mut internal = create_module_cmd_ctx(&name, "", module_ref, runtime_handle);
        internal.module_options = module_options.clone();
        internal.db_idx = db_idx;
        internal.resp_version = resp_version;
        // SAFETY: the constructor's username allocation is uniquely owned.
        unsafe { drop(CString::from_raw(internal.username)) };
        internal.username = username.clone().into_raw();
        let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
        // SAFETY: ctx is uniquely owned; its scope starts with auto memory disabled.
        unsafe { (*ctx).getapifuncptr = getapi };
        {
            let _context_scope = super::super::ActiveModuleContextScope::enter(ctx);
            callback(ctx, event_type, event.as_ptr(), key);
        }
        // SAFETY: callback returned; collect its independent scope before freeing ctx.
        unsafe { free_module_ctx(ctx) };
    }
}

fn valid_arity(arity: i32, argc: i32) -> bool {
    if arity > 0 {
        argc == arity
    } else {
        argc >= -arity
    }
}

fn module_keyspace_event<'a>(
    command: &[u8],
    args: &'a CommandArgs,
    reply: &Value,
) -> Option<(c_int, &'static str, &'a [u8])> {
    let key = args.args.first()?.as_ref();
    match command {
        b"set" => Some((1 << 3, "set", key)),
        b"sadd" => Some((1 << 5, "sadd", key)),
        b"hset" => Some((1 << 6, "hset", key)),
        b"get" | b"smembers"
            if matches!(reply, Value::Null | Value::NullArray)
                || matches!(reply, Value::Array(items) if items.is_empty())
                || matches!(reply, Value::Map(items) if items.is_empty())
                || matches!(reply, Value::Set(items) if items.is_empty()) =>
        {
            Some((1 << 11, "keymiss", key))
        }
        _ => None,
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct ValkeyModuleReplyHandlers {
    version: u64,
    null: Option<extern "C" fn(*mut c_void)>,
    nullBulkString: Option<extern "C" fn(*mut c_void)>,
    nullArray: Option<extern "C" fn(*mut c_void)>,
    bulkString: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    simpleString: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    verbatimString: Option<extern "C" fn(*mut c_void, *const c_char, size_t, *const c_char)>,
    error: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    integer: Option<extern "C" fn(*mut c_void, c_longlong)>,
    doubleVal: Option<extern "C" fn(*mut c_void, c_double)>,
    bigNumber: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    boolVal: Option<extern "C" fn(*mut c_void, c_int)>,
    attributeStart: Option<extern "C" fn(*mut c_void, size_t)>,
    attributeEnd: Option<extern "C" fn(*mut c_void)>,
    arrayStart: Option<extern "C" fn(*mut c_void, size_t)>,
    arrayEnd: Option<extern "C" fn(*mut c_void)>,
    mapStart: Option<extern "C" fn(*mut c_void, size_t)>,
    mapEnd: Option<extern "C" fn(*mut c_void)>,
    setStart: Option<extern "C" fn(*mut c_void, size_t)>,
    setEnd: Option<extern "C" fn(*mut c_void)>,
    replyParsingError: Option<extern "C" fn(*mut c_void)>,
    pub(crate) onBlocked: Option<extern "C" fn(*mut c_void, *mut ValkeyModuleCtx, *mut c_void)>,
    onRespAvailable:
        Option<extern "C" fn(*mut c_void, *mut ValkeyModuleCtx, *const c_char, size_t) -> c_int>,
}

#[no_mangle]
// SAFETY: Called by Valkey module C code; ctx must be non-null and point to a valid ValkeyModuleCtx,
// argv must be a valid array of argc ValkeyModuleString pointers. resp_handlers and reply_ctx may be
// null; if non-null, they must point to valid ValkeyModuleReplyHandlers and reply context respectively.
// The function calls VM_CallWithReply which performs its own null checks, and handles recursive
// dereferencing of the reply tree via call_reply_handlers.
pub unsafe extern "C" fn VM_CallArgv(
    ctx: *mut ValkeyModuleCtx,
    argv: *mut *mut ValkeyModuleString,
    argc: c_int,
    flags: c_int,
    resp_handlers: *const ValkeyModuleReplyHandlers,
    reply_ctx: *mut c_void,
) -> c_int {
    let format = ModuleCallFormat::from_raw(flags);
    // SAFETY: the ABI contract above covers the context, argv, handler table,
    // and reply tree for this synchronous traversal.
    unsafe {
        if ctx.is_null() || argv.is_null() || argc < 1 {
            return VALKEYMODULE_ERR;
        }

        let mut pending_conn = None;
        let reply = call_with_reply(ctx, argv, argc, flags, Some(&mut pending_conn));
        if let Some((conn, command)) = pending_conn {
            let internal = (*ctx).internal;
            let module_name = super::super::ctx::get_module_name(ctx);
            let call = super::pending::PendingModuleCall::new(
                module_name,
                conn,
                argv,
                argc,
                resp_handlers,
                reply_ctx,
                command,
                format,
            );
            let callback = call.handlers.and_then(|handlers| handlers.onBlocked);
            let handle = {
                let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
                    return VALKEYMODULE_ERR;
                };
                runtime.retain_pending_module_call(call)
            };
            let Some(handle) = handle else {
                return VALKEYMODULE_ERR;
            };
            if let Some(callback) = callback {
                callback(reply_ctx, ctx, std::ptr::without_provenance_mut(handle));
            }
            return VALKEYMODULE_OK;
        }
        if reply.is_null() {
            if format.dry_run() {
                if !resp_handlers.is_null() {
                    if let Some(on_resp) = (*resp_handlers).onRespAvailable {
                        on_resp(reply_ctx, ctx, std::ptr::null(), 0);
                    }
                }
                return VALKEYMODULE_OK;
            }
            return VALKEYMODULE_ERR;
        }

        if !resp_handlers.is_null() {
            let result = deliver_call_reply(ctx, reply, &*resp_handlers, reply_ctx);
            ValkeyModule_FreeCallReply(reply);
            if result.is_err() {
                return VALKEYMODULE_ERR;
            }
            return VALKEYMODULE_OK;
        }

        ValkeyModule_FreeCallReply(reply);
        VALKEYMODULE_OK
    }
}

#[no_mangle]
// SAFETY: the opaque handle is only used as an identity, never dereferenced.
pub unsafe extern "C" fn VM_CallArgvAbort(handle: *mut c_void) -> c_int {
    if handle.is_null() {
        return VALKEYMODULE_ERR;
    }
    let did_abort = crate::engine::extension::module::capability::with_active_runtime(|runtime| {
        runtime.abort_pending_module_call(handle.addr())
    })
    .unwrap_or(false);
    if did_abort {
        VALKEYMODULE_OK
    } else {
        VALKEYMODULE_ERR
    }
}

// SAFETY: reply is a live owned root and ctx is live for the copied callbacks.
pub(crate) unsafe fn deliver_call_reply(
    ctx: *mut ValkeyModuleCtx,
    reply: *mut ValkeyModuleCallReply,
    handlers: &ValkeyModuleReplyHandlers,
    reply_ctx: *mut c_void,
) -> Result<(), ()> {
    if let Some(on_resp) = handlers.onRespAvailable {
        // SAFETY: reply belongs to this call until all callbacks return.
        let (mut proto, proto_len) = unsafe { value_to_resp_proto(&*reply) };
        proto.push(0);
        if on_resp(reply_ctx, ctx, proto.as_ptr().cast(), proto_len) == 0 {
            return Ok(());
        }
    }
    // SAFETY: the live reply tree is retained throughout typed callback dispatch.
    unsafe { call_reply_handlers(reply, handlers, reply_ctx) }
}

#[no_mangle]
// SAFETY: Called by the C variadic shim; ctx must be a live ValkeyModuleCtx and
// argv must point to argc live ValkeyModuleString pointers for the duration of this call.
pub unsafe extern "C" fn VM_ReplicateCommand(
    ctx: *mut ValkeyModuleCtx,
    argv: *mut *mut ValkeyModuleString,
    argc: c_int,
) -> c_int {
    unsafe {
        if ctx.is_null() || argv.is_null() || argc < 1 {
            return VALKEYMODULE_ERR;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return VALKEYMODULE_ERR;
        }
        let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
            return VALKEYMODULE_ERR;
        };
        if runtime.has_paused_writes() {
            return VALKEYMODULE_ERR;
        }
        let parts: Vec<&[u8]> = (0..argc as usize)
            .map(|i| (**argv.add(i)).as_bytes())
            .collect();
        if runtime.propagate_module_command(encode_command(&parts)) {
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}

#[no_mangle]
// SAFETY: Called by C modules; ctx must be a live command context.
pub unsafe extern "C" fn ValkeyModule_ReplicateVerbatim(ctx: *mut ValkeyModuleCtx) -> c_int {
    if ctx.is_null() {
        return VALKEYMODULE_ERR;
    }
    // SAFETY: ctx is live; a non-null internal context owns a live runtime
    // handle which is only borrowed during this synchronous pause check.
    let is_paused = unsafe {
        let internal = (*ctx).internal;
        !internal.is_null()
            && runtime_from_opaque((*internal).runtime_handle)
                .is_some_and(|runtime| runtime.has_paused_writes())
    };
    if is_paused {
        return VALKEYMODULE_ERR;
    }
    VALKEYMODULE_OK
}

// SAFETY: reply must be a non-null pointer to a valid ValkeyModuleCallReply; handlers must be a
// valid reference to ValkeyModuleReplyHandlers. reply_ctx is passed to external C function pointers
// which must be valid for the lifetime of the call. The function recursively dereferences elements
// pointers which must be within bounds of the allocated array.
unsafe fn call_reply_handlers(
    reply: *mut ValkeyModuleCallReply,
    handlers: &ValkeyModuleReplyHandlers,
    reply_ctx: *mut c_void,
) -> Result<(), ()> {
    if reply.is_null() {
        return Ok(());
    }
    // SAFETY: the caller contract guarantees a complete live reply tree.
    let node = unsafe { CallReplyNode::from_reply(&*reply) };
    dispatch_reply_with_attribute(node, handlers, reply_ctx)
}

fn dispatch_reply_with_attribute(
    node: CallReplyNode<'_>,
    handlers: &ValkeyModuleReplyHandlers,
    reply_ctx: *mut c_void,
) -> Result<(), ()> {
    if let Some(attribute) = node.attribute() {
        dispatch_container(attribute, handlers, reply_ctx)?;
    }
    dispatch_reply_node(node, handlers, reply_ctx)
}

fn dispatch_reply_node(
    node: CallReplyNode<'_>,
    handlers: &ValkeyModuleReplyHandlers,
    reply_ctx: *mut c_void,
) -> Result<(), ()> {
    match node.effective_kind() {
        CallReplyKind::Null | CallReplyKind::NullArray => dispatch_null(node, handlers, reply_ctx),
        CallReplyKind::String
        | CallReplyKind::SimpleString
        | CallReplyKind::Error
        | CallReplyKind::BigNumber
        | CallReplyKind::VerbatimString => dispatch_text(node, handlers, reply_ctx),
        CallReplyKind::Integer | CallReplyKind::Double | CallReplyKind::Bool => {
            dispatch_number(node, handlers, reply_ctx)
        }
        CallReplyKind::Array
        | CallReplyKind::Map
        | CallReplyKind::Set
        | CallReplyKind::Attribute => dispatch_container(node, handlers, reply_ctx)?,
        CallReplyKind::Unknown => {}
    }
    Ok(())
}

fn dispatch_null(node: CallReplyNode<'_>, handlers: &ValkeyModuleReplyHandlers, ctx: *mut c_void) {
    match node.effective_kind() {
        CallReplyKind::Null => handlers.null.map(|callback| callback(ctx)),
        CallReplyKind::NullArray => handlers.nullArray.map(|callback| callback(ctx)),
        _ => None,
    };
}

fn dispatch_number(
    node: CallReplyNode<'_>,
    handlers: &ValkeyModuleReplyHandlers,
    ctx: *mut c_void,
) {
    let reply = node.reply();
    match node.effective_kind() {
        CallReplyKind::Integer => handlers
            .integer
            .map(|callback| callback(ctx, reply.int_val)),
        CallReplyKind::Double => handlers
            .doubleVal
            .map(|callback| callback(ctx, reply.double_val)),
        CallReplyKind::Bool => handlers
            .boolVal
            .map(|callback| callback(ctx, reply.bool_val)),
        _ => None,
    };
}

fn dispatch_text(node: CallReplyNode<'_>, handlers: &ValkeyModuleReplyHandlers, ctx: *mut c_void) {
    let reply = node.reply();
    let string = reply.str_data.cast_const();
    match node.effective_kind() {
        CallReplyKind::String => handlers
            .bulkString
            .map(|callback| callback(ctx, string, reply.str_len)),
        CallReplyKind::SimpleString => handlers
            .simpleString
            .map(|callback| callback(ctx, string, reply.str_len)),
        CallReplyKind::Error => handlers
            .error
            .map(|callback| callback(ctx, string, reply.str_len)),
        CallReplyKind::BigNumber => handlers
            .bigNumber
            .map(|callback| callback(ctx, reply.big_num_str.cast_const(), reply.big_num_len)),
        CallReplyKind::VerbatimString => handlers.verbatimString.map(|callback| {
            callback(
                ctx,
                reply.verbatim_str.cast_const(),
                reply.verbatim_len,
                reply.verbatim_fmt.cast_const(),
            )
        }),
        _ => None,
    };
}

fn dispatch_container(
    node: CallReplyNode<'_>,
    handlers: &ValkeyModuleReplyHandlers,
    ctx: *mut c_void,
) -> Result<(), ()> {
    let len = node.logical_len();
    match node.kind() {
        CallReplyKind::Array => handlers.arrayStart.map(|callback| callback(ctx, len)),
        CallReplyKind::Map => handlers.mapStart.map(|callback| callback(ctx, len)),
        CallReplyKind::Set => handlers.setStart.map(|callback| callback(ctx, len)),
        CallReplyKind::Attribute => handlers.attributeStart.map(|callback| callback(ctx, len)),
        _ => None,
    };
    for child in node.children() {
        dispatch_reply_with_attribute(child, handlers, ctx)?;
    }
    match node.kind() {
        CallReplyKind::Array => handlers.arrayEnd.map(|callback| callback(ctx)),
        CallReplyKind::Map => handlers.mapEnd.map(|callback| callback(ctx)),
        CallReplyKind::Set => handlers.setEnd.map(|callback| callback(ctx)),
        CallReplyKind::Attribute => handlers.attributeEnd.map(|callback| callback(ctx)),
        _ => None,
    };
    Ok(())
}

// SAFETY: reply is a reference to a valid ValkeyModuleCallReply. The function dereferences
// str_data, big_num_str, verbatim_str, verbatim_fmt, and elements pointers; these must be
// null or point to valid C-allocated or Box-allocated memory (depending on the field).
unsafe fn reply_to_resp_proto(reply: &ValkeyModuleCallReply, buf: &mut Vec<u8>) {
    // SAFETY: the caller guarantees a complete live reply tree.
    let node = unsafe { CallReplyNode::from_reply(reply) };
    serialize_reply_with_attribute(node, buf);
}

fn serialize_reply_with_attribute(node: CallReplyNode<'_>, buf: &mut Vec<u8>) {
    if let Some(proto) = node.reply().raw_proto() {
        buf.extend_from_slice(proto);
        return;
    }
    if let Some(attribute) = node.attribute() {
        serialize_container(attribute, buf);
    }
    serialize_reply_node(node, buf);
}

fn serialize_reply_node(node: CallReplyNode<'_>, buf: &mut Vec<u8>) {
    match node.kind() {
        CallReplyKind::String
        | CallReplyKind::SimpleString
        | CallReplyKind::Error
        | CallReplyKind::BigNumber
        | CallReplyKind::VerbatimString => serialize_text(node, buf),
        CallReplyKind::Integer
        | CallReplyKind::Double
        | CallReplyKind::Bool
        | CallReplyKind::Null
        | CallReplyKind::NullArray => serialize_number_or_null(node, buf),
        CallReplyKind::Array
        | CallReplyKind::Map
        | CallReplyKind::Set
        | CallReplyKind::Attribute => serialize_container(node, buf),
        CallReplyKind::Unknown => buf.extend_from_slice(b"_\r\n"),
    }
}

fn append_payload(buf: &mut Vec<u8>, payload: Option<&[u8]>) {
    if let Some(payload) = payload {
        buf.extend_from_slice(payload);
    }
}

fn serialize_text(node: CallReplyNode<'_>, buf: &mut Vec<u8>) {
    match node.kind() {
        CallReplyKind::SimpleString => {
            buf.push(b'+');
            append_payload(buf, node.string_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        CallReplyKind::String => {
            buf.push(b'$');
            let payload = node.string_bytes().unwrap_or_default();
            buf.extend_from_slice(format!("{}\r\n", payload.len()).as_bytes());
            buf.extend_from_slice(payload);
            buf.extend_from_slice(b"\r\n");
        }
        CallReplyKind::Error => {
            buf.push(b'-');
            append_payload(buf, node.string_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        CallReplyKind::BigNumber => {
            buf.push(b'(');
            append_payload(buf, node.big_number_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        CallReplyKind::VerbatimString => {
            buf.push(b'=');
            let data = node.verbatim_bytes().unwrap_or_default();
            buf.extend_from_slice(format!("{}\r\n", data.len() + 4).as_bytes());
            if let Some(format) = node.verbatim_format() {
                buf.extend_from_slice(&format);
            }
            buf.push(b':');
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        _ => {}
    }
}

fn serialize_number_or_null(node: CallReplyNode<'_>, buf: &mut Vec<u8>) {
    let reply = node.reply();
    match node.kind() {
        CallReplyKind::Integer => {
            buf.extend_from_slice(format!(":{}\r\n", reply.int_val).as_bytes())
        }
        CallReplyKind::Double => match node.decimal_double() {
            Some(value) => {
                crate::foundation::io::protocol::serializer::serialize_resp3(
                    &Value::DecimalDouble(value.clone()),
                    buf,
                );
            }
            None => buf.extend_from_slice(format!(",{}\r\n", reply.double_val).as_bytes()),
        },
        CallReplyKind::Bool if reply.bool_val != 0 => buf.extend_from_slice(b"#t\r\n"),
        CallReplyKind::Bool => buf.extend_from_slice(b"#f\r\n"),
        CallReplyKind::Null => buf.extend_from_slice(b"_\r\n"),
        CallReplyKind::NullArray => buf.extend_from_slice(b"*-1\r\n"),
        _ => {}
    }
}

fn serialize_container(node: CallReplyNode<'_>, buf: &mut Vec<u8>) {
    let marker = match node.kind() {
        CallReplyKind::Array => b'*',
        CallReplyKind::Map => b'%',
        CallReplyKind::Set => b'~',
        CallReplyKind::Attribute => b'|',
        _ => return,
    };
    buf.push(marker);
    buf.extend_from_slice(format!("{}\r\n", node.logical_len()).as_bytes());
    for child in node.children() {
        serialize_reply_with_attribute(child, buf);
    }
}

// SAFETY: reply is a reference to a valid ValkeyModuleCallReply. Delegates to reply_to_resp_proto
// which has its own safety requirements on the reply's interior pointers.
unsafe fn value_to_resp_proto(reply: &ValkeyModuleCallReply) -> (Vec<u8>, size_t) {
    unsafe {
        let mut buf = Vec::new();
        reply_to_resp_proto(reply, &mut buf);
        let len = buf.len();
        (buf, len)
    }
}

// SAFETY: Called by Valkey module C code; ctx must be non-null and point to a valid ValkeyModuleCtx
// with a valid internal field pointing to a ValkeyModuleCtxInternal whose runtime handle may be null
// (handled by check). All pointer dereferences are guarded by null checks.
pub unsafe extern "C" fn ValkeyModule_GetContextFlags(ctx: *mut ValkeyModuleCtx) -> c_int {
    unsafe {
        if ctx.is_null() {
            return 0;
        }
        let internal = (*ctx).internal;
        if internal.is_null() {
            return 0;
        }
        if (*internal).runtime_handle.is_null() {
            return 0;
        }

        let Some(runtime) = runtime_from_opaque((*internal).runtime_handle) else {
            return 0;
        };
        let mut flags = runtime.context_flags();

        // VALKEYMODULE_CTX_FLAGS_RESP3 (1<<22) — based on client resp version
        if (*internal).onload == 0 && (*internal).resp_version == 3 {
            flags |= 1 << 22;
        }

        flags
    }
}

#[cfg(test)]
mod format_tests {
    use super::{
        ModuleCallFormat, ModuleCallResponse, CALL_ALLOW_BLOCK, CALL_DRY_RUN, CALL_NO_WRITES,
        CALL_REPLY_EXACT, CALL_RESP3, CALL_RESPECT_DENY_OOM, CALL_RUN_AS_USER, CALL_SCRIPT_MODE,
    };

    #[test]
    fn call_format_types_every_consumed_behavior_flag() {
        let flags = CALL_RESP3
            | CALL_RUN_AS_USER
            | CALL_SCRIPT_MODE
            | CALL_NO_WRITES
            | CALL_RESPECT_DENY_OOM
            | CALL_DRY_RUN
            | CALL_ALLOW_BLOCK
            | CALL_REPLY_EXACT;
        let format = ModuleCallFormat::from_raw(flags);

        assert!(format.run_as_user());
        assert!(format.script_mode());
        assert!(format.no_writes());
        assert!(format.respect_deny_oom());
        assert!(format.dry_run());
        assert!(format.allow_block());
        assert!(format.reply_exact());
    }

    #[test]
    fn call_format_selects_response_mode_with_resp3_precedence() {
        assert_eq!(
            ModuleCallFormat::from_raw(0).response(),
            ModuleCallResponse::Resp2
        );
        assert_eq!(
            ModuleCallFormat::from_raw(1 << 4).response(),
            ModuleCallResponse::Auto
        );
        assert_eq!(
            ModuleCallFormat::from_raw(1 << 3).response(),
            ModuleCallResponse::Resp3
        );
        assert_eq!(
            ModuleCallFormat::from_raw((1 << 3) | (1 << 4)).response(),
            ModuleCallResponse::Resp3
        );
        assert!(!ModuleCallFormat::from_raw(1 << 4).reply_exact());
        assert!(ModuleCallFormat::from_raw(1 << 3).reply_exact());
        assert!(ModuleCallFormat::from_raw(1 << 12).reply_exact());
    }
}

#[cfg(test)]
mod traversal_tests {
    use super::{
        call_reply_handlers, value_to_resp_proto, ValkeyModuleCallReply, ValkeyModuleReplyHandlers,
    };
    use crate::foundation::io::protocol::Value;
    use bytes::Bytes;
    use libc::{c_char, c_longlong, c_void, size_t};

    #[derive(Debug, Eq, PartialEq)]
    enum Event {
        AttributeStart(usize),
        AttributeEnd,
        ArrayStart(usize),
        ArrayEnd,
        MapStart(usize),
        MapEnd,
        SetStart(usize),
        SetEnd,
        String,
        Integer(i64),
    }

    fn push_event(ctx: *mut c_void, event: Event) {
        // SAFETY: tests pass a live Vec<Event> as reply_ctx for every callback.
        unsafe { (&mut *ctx.cast::<Vec<Event>>()).push(event) };
    }

    extern "C" fn attribute_start(ctx: *mut c_void, len: size_t) {
        push_event(ctx, Event::AttributeStart(len));
    }
    extern "C" fn attribute_end(ctx: *mut c_void) {
        push_event(ctx, Event::AttributeEnd);
    }
    extern "C" fn array_start(ctx: *mut c_void, len: size_t) {
        push_event(ctx, Event::ArrayStart(len));
    }
    extern "C" fn array_end(ctx: *mut c_void) {
        push_event(ctx, Event::ArrayEnd);
    }
    extern "C" fn map_start(ctx: *mut c_void, len: size_t) {
        push_event(ctx, Event::MapStart(len));
    }
    extern "C" fn map_end(ctx: *mut c_void) {
        push_event(ctx, Event::MapEnd);
    }
    extern "C" fn set_start(ctx: *mut c_void, len: size_t) {
        push_event(ctx, Event::SetStart(len));
    }
    extern "C" fn set_end(ctx: *mut c_void) {
        push_event(ctx, Event::SetEnd);
    }
    extern "C" fn string(ctx: *mut c_void, _data: *const c_char, _len: size_t) {
        push_event(ctx, Event::String);
    }
    extern "C" fn integer(ctx: *mut c_void, value: c_longlong) {
        push_event(ctx, Event::Integer(value));
    }

    fn handlers() -> ValkeyModuleReplyHandlers {
        ValkeyModuleReplyHandlers {
            version: 1,
            null: None,
            nullBulkString: None,
            nullArray: None,
            bulkString: Some(string),
            simpleString: Some(string),
            verbatimString: None,
            error: None,
            integer: Some(integer),
            doubleVal: None,
            bigNumber: None,
            boolVal: None,
            attributeStart: Some(attribute_start),
            attributeEnd: Some(attribute_end),
            arrayStart: Some(array_start),
            arrayEnd: Some(array_end),
            mapStart: Some(map_start),
            mapEnd: Some(map_end),
            setStart: Some(set_start),
            setEnd: Some(set_end),
            replyParsingError: None,
            onBlocked: None,
            onRespAvailable: None,
        }
    }

    fn collect_events(value: &Value) -> Vec<Event> {
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(value, true, true);
        let mut events = Vec::new();
        // SAFETY: reply, handlers, and the Vec<Event> reply context remain live
        // for the synchronous traversal.
        unsafe {
            call_reply_handlers(
                &mut *reply,
                &handlers(),
                (&mut events as *mut Vec<Event>).cast(),
            )
            .expect("owned reply tree must be traversable");
            reply.free();
        }
        events
    }

    #[test]
    fn decimal_double_module_rebuild_and_callbacks_use_their_respective_payloads() {
        use crate::foundation::io::protocol::DecimalDoubleText;
        extern "C" fn double(ctx: *mut c_void, value: libc::c_double) {
            // SAFETY: the test passes a live f64 destination during synchronous traversal.
            unsafe { *ctx.cast::<f64>() = value };
        }
        let scalar = Value::DecimalDouble(DecimalDoubleText::new("9007199254740993").unwrap());
        let value = Value::attribute(
            vec![(Value::bulk("meta"), scalar.clone())],
            Value::Array(vec![scalar.clone()]),
        );
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);
        let expected = crate::foundation::io::protocol::serializer::serialize_to_vec_resp3(&value);
        // SAFETY: the protocol routine borrows this complete live owned tree.
        let (proto, len) = unsafe { value_to_resp_proto(&reply) };
        assert_eq!(proto, expected);
        assert_eq!(len, expected.len());
        // SAFETY: this test uniquely owns the reply and releases it once.
        unsafe { reply.free() };
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&scalar, true, true);
        let mut callbacks = handlers();
        callbacks.doubleVal = Some(double);
        let mut received = f64::NAN;
        // SAFETY: all inputs and the f64 destination remain live for synchronous callbacks.
        unsafe {
            call_reply_handlers(&mut *reply, &callbacks, (&mut received as *mut f64).cast())
                .unwrap();
            reply.free();
        }
        assert_eq!(received, 9007199254740992.0);
    }

    #[test]
    fn nested_containers_emit_their_matching_callback_kinds() {
        let value = Value::Array(vec![
            Value::Map(vec![(Value::bulk("key"), Value::Integer(1))]),
            Value::Set(vec![Value::Integer(2)]),
        ]);

        assert_eq!(
            collect_events(&value),
            vec![
                Event::ArrayStart(2),
                Event::MapStart(1),
                Event::String,
                Event::Integer(1),
                Event::MapEnd,
                Event::SetStart(1),
                Event::Integer(2),
                Event::SetEnd,
                Event::ArrayEnd,
            ]
        );
    }

    #[test]
    fn attributes_emit_pairs_then_the_wrapped_reply() {
        let value = Value::attribute(
            vec![(Value::bulk("meta"), Value::Integer(9))],
            Value::Integer(100),
        );

        assert_eq!(
            collect_events(&value),
            vec![
                Event::AttributeStart(1),
                Event::String,
                Event::Integer(9),
                Event::AttributeEnd,
                Event::Integer(100),
            ]
        );
    }

    #[test]
    fn attribute_resp_serialization_precedes_the_wrapped_reply() {
        let value = Value::attribute(
            vec![(Value::bulk("meta"), Value::Integer(9))],
            Value::Integer(100),
        );
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);

        // SAFETY: reply owns a complete live tree until it is freed below.
        let (proto, len) = unsafe { value_to_resp_proto(&reply) };
        assert_eq!(len, proto.len());
        assert_eq!(proto, b"|1\r\n$4\r\nmeta\r\n:9\r\n:100\r\n");
        // SAFETY: serialization only borrowed the reply tree.
        unsafe { reply.free() };
    }

    #[test]
    fn binary_payload_serialization_uses_stored_lengths() {
        let values = [
            (
                Value::Bulk(Bytes::from_static(b"a\0b")),
                b"$3\r\na\0b\r\n".as_slice(),
            ),
            (
                Value::verbatim(*b"txt", Bytes::from_static(b"x\0y")),
                b"=7\r\ntxt:x\0y\r\n".as_slice(),
            ),
        ];

        for (value, expected) in values {
            let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);
            // SAFETY: reply owns a complete live tree for serialization.
            let (proto, len) = unsafe { value_to_resp_proto(&reply) };
            assert_eq!(len, proto.len());
            assert_eq!(proto, expected);
            // SAFETY: serialization only borrowed the reply tree.
            unsafe { reply.free() };
        }
    }
}

/// ValkeyModule_SetContextProtectedMode — toggles protected mode for selected
/// APIs in C Valkey. The current module runtime has no protected-context state,
/// so this smoke-compatible implementation validates the context and accepts
/// both enabled and disabled transitions.
// SAFETY: Called by Valkey module C code; ctx must be non-null and point to a
// valid ValkeyModuleCtx. The function does not dereference internal runtime
// data beyond the null check.
pub unsafe extern "C" fn ValkeyModule_SetContextProtectedMode(
    ctx: *mut ValkeyModuleCtx,
    enabled: c_int,
) -> c_int {
    if ctx.is_null() {
        return VALKEYMODULE_ERR;
    }
    log::trace!(
        "ValkeyModule_SetContextProtectedMode({:p}, {}) called",
        ctx,
        enabled
    );
    VALKEYMODULE_OK
}

/// ValkeyModule_NotifyKeyspaceEvent — emits a module keyspace event.
// SAFETY: Called by Valkey module C code; ctx must be non-null. event may be
// null, and key is opaque to this smoke-level hook.
pub unsafe extern "C" fn ValkeyModule_NotifyKeyspaceEvent(
    ctx: *mut ValkeyModuleCtx,
    event_type: c_int,
    event: *const c_char,
    key: *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        if ctx.is_null() {
            return VALKEYMODULE_ERR;
        }
        let event_name = if event.is_null() {
            ""
        } else {
            CStr::from_ptr(event).to_str().unwrap_or("")
        };
        log::trace!(
            "ValkeyModule_NotifyKeyspaceEvent({:p}, {}, {}, {:p}) called",
            ctx,
            event_type,
            event_name,
            key
        );
        VALKEYMODULE_OK
    }
}
