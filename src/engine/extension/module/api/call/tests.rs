use super::super::VM_ReplyRaw;
use super::test_support::{record_proto_len, TestReplyHandlers};
use super::{VM_CallArgv, VM_CallWithReply, ValkeyModule_FreeCallReply};
use crate::engine::command::context::CommandConn;
use crate::engine::command::{
    CmdFlags, CommandArgs, CommandDescriptor, CMD_DENYOOM, CMD_READONLY, CMD_WRITE,
};
use crate::engine::extension::module::capability::{
    ModuleClientInfo, ModuleRuntime, ModuleRuntimeHandle,
};
use crate::engine::extension::module::{
    create_module_cmd_ctx, create_module_ctx, free_module_ctx, ValkeyModuleString,
};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::{Db, DbSet};
use bytes::Bytes;

struct TrackingRuntime {
    dbs: DbSet,
    tracked: Vec<(usize, Bytes)>,
    raw_replies: Vec<Vec<u8>>,
    published: Vec<(Bytes, Bytes, bool)>,
    is_oom: bool,
    has_enough_replicas_to_write: bool,
    is_readonly_replica: bool,
    is_stale_replica: bool,
    has_persistence_write_error: bool,
    module_acl_error: Option<String>,
    command_calls: usize,
}

impl ModuleRuntime for TrackingRuntime {
    fn num_dbs(&self) -> usize {
        self.dbs.num_dbs()
    }

    fn db_mut(&mut self, db_idx: u32) -> Option<&mut Db> {
        self.dbs.get_mut(db_idx)
    }

    fn flush_all_dbs(&mut self) {
        self.dbs.flush_all();
    }

    fn uses_lfu(&self) -> bool {
        false
    }

    fn is_oom(&self) -> bool {
        self.is_oom
    }

    fn has_enough_replicas_to_write(&self) -> bool {
        self.has_enough_replicas_to_write
    }

    fn is_readonly_replica(&self) -> bool {
        self.is_readonly_replica
    }

    fn is_stale_replica(&self) -> bool {
        self.is_stale_replica
    }

    fn has_persistence_write_error(&self) -> bool {
        self.has_persistence_write_error
    }

    fn module_acl_error(&self, _command: CommandDescriptor, _args: &CommandArgs) -> Option<String> {
        self.module_acl_error.clone()
    }

    fn module_client_id(&self) -> u64 {
        42
    }

    fn module_client_info(&self, client_id: u64) -> Option<ModuleClientInfo> {
        (client_id == 42).then_some(ModuleClientInfo {
            id: 42,
            peer_addr: ([127, 0, 0, 1], 6379).into(),
            db: 0,
            is_pubsub: false,
            is_blocked: false,
            is_tracking: true,
            is_multi: false,
            is_readonly: false,
            is_replica: false,
            is_monitor: false,
            is_authenticated: true,
            was_authenticated: true,
        })
    }

    fn module_client_name(&self, _client_id: u64) -> Option<Bytes> {
        None
    }

    fn set_module_client_name(&mut self, _client_id: u64, _name: Bytes) -> bool {
        false
    }

    fn module_client_tracking(
        &self,
    ) -> Option<crate::engine::extension::module::capability::ModuleClientTracking> {
        Some(
            crate::engine::extension::module::capability::ModuleClientTracking {
                id: 42,
                is_tracking: true,
                is_broadcast: false,
                is_optin: false,
                is_optout: false,
                is_caching: false,
            },
        )
    }

    fn remember_module_tracked_key(&mut self, client_id: usize, key: &[u8]) {
        self.tracked.push((client_id, Bytes::copy_from_slice(key)));
        if key == b"notification-reentry" {
            NOTIFICATION_CALL_ORDER.with(|order| order.borrow_mut().push("track-outer"));
        }
    }

    fn reply_module_raw(&mut self, reply: Vec<u8>) -> bool {
        self.raw_replies.push(reply);
        true
    }

    fn list_max_listpack_size(&self) -> i64 {
        -2
    }

    fn lookup_command(&self, name: &[u8]) -> Option<CommandDescriptor> {
        if name.eq_ignore_ascii_case(b"set") {
            return Some(CommandDescriptor {
                name: crate::engine::command::CommandName::Static("set"),
                arity: 3,
                flags: CMD_DENYOOM | CMD_WRITE,
                first_key: 1,
                last_key: 1,
                key_step: 1,
                container: None,
                acl_categories: &["STRING", "WRITE"],
            });
        }
        if name.eq_ignore_ascii_case(b"get") {
            return Some(CommandDescriptor {
                name: crate::engine::command::CommandName::Static("get"),
                arity: 2,
                flags: CMD_READONLY,
                first_key: 1,
                last_key: 1,
                key_step: 1,
                container: None,
                acl_categories: &["READ", "STRING"],
            });
        }
        name.eq_ignore_ascii_case(b"publish")
            .then_some(CommandDescriptor {
                name: crate::engine::command::CommandName::Static("publish"),
                arity: 3,
                flags: 0,
                first_key: 0,
                last_key: 0,
                key_step: 0,
                container: None,
                acl_categories: &["FAST", "PUBSUB"],
            })
    }

    fn execute_command(
        &mut self,
        name: &[u8],
        conn: &mut dyn CommandConn,
        args: &CommandArgs,
    ) -> Value {
        self.command_calls += 1;
        if args
            .args
            .first()
            .is_some_and(|key| key.as_ref() == b"notification-reentry")
        {
            let db = self.dbs.get_mut(conn.selected_db()).unwrap();
            let is_set = name.eq_ignore_ascii_case(b"set");
            let phase = if is_set { "inner" } else { "outer" };
            NOTIFICATION_CALL_ORDER.with(|order| {
                order
                    .borrow_mut()
                    .push(if is_set { "execute-set" } else { "execute-get" });
            });
            conn.record_notification((
                Bytes::from(format!("notification-{phase}")),
                Bytes::from_static(b"normal"),
            ));
            conn.record_shard_notification((
                Bytes::from(format!("notification-{phase}")),
                Bytes::from_static(b"shard"),
            ));
            if is_set {
                db.set(
                    args.args[0].to_vec(),
                    crate::foundation::io::store::Robj::string(args.args[1].clone()),
                );
                return Value::ok();
            }
            return db
                .get_no_expire(&args.args[0])
                .map_or(Value::Null, |value| {
                    Value::bulk(Bytes::copy_from_slice(value.as_bytes()))
                });
        }
        if args
            .args
            .first()
            .is_some_and(|key| key.as_ref() == b"raw-lifecycle")
        {
            conn.queue_reply(b"+RAW\r\n".to_vec());
            conn.set_custom_reply(true);
            return Value::Null;
        }
        if name.eq_ignore_ascii_case(b"publish") {
            conn.record_notification((args.args[0].clone(), args.args[1].clone()));
            return Value::integer(1);
        }
        Value::bulk(Bytes::from_static(b"value"))
    }

    fn register_module_command(
        &mut self,
        _name: &str,
        _arity: i32,
        _flags: CmdFlags,
        _first_key: i32,
        _last_key: i32,
        _key_step: i32,
    ) -> bool {
        false
    }

    fn update_module_command(
        &mut self,
        _name: &str,
        _arity: Option<i32>,
        _first_key: i32,
        _last_key: i32,
        _key_step: i32,
    ) -> bool {
        false
    }

    fn publish_module_message(
        &mut self,
        channel: Bytes,
        message: Bytes,
        is_shard: bool,
    ) -> Option<usize> {
        if channel.as_ref() == b"notification-inner" || channel.as_ref() == b"notification-outer" {
            let event = match (channel.as_ref() == b"notification-inner", is_shard) {
                (true, false) => "publish-inner",
                (true, true) => "publish-inner-shard",
                (false, false) => "publish-outer",
                (false, true) => "publish-outer-shard",
            };
            NOTIFICATION_CALL_ORDER.with(|order| order.borrow_mut().push(event));
        }
        self.published.push((channel, message, is_shard));
        Some(1)
    }

    fn propagate_module_command(&mut self, _frame: Vec<u8>) -> bool {
        false
    }

    fn module_scan_keys(&mut self, _db_idx: u32) -> Vec<Vec<u8>> {
        vec![b"scan-key".to_vec()]
    }

    fn module_scan_key(
        &mut self,
        db_idx: u32,
        _key: &[u8],
    ) -> Result<crate::engine::extension::module::capability::ModuleKeyScan, ()> {
        Ok(
            crate::engine::extension::module::capability::ModuleKeyScan {
                entries: vec![(db_idx.to_string().into_bytes(), Some(b"value".to_vec()))],
                is_incremental: true,
            },
        )
    }

    fn record_internal_monitor(
        &mut self,
        command: CommandDescriptor,
        _db: u32,
        _source: &str,
        argv: Vec<Bytes>,
    ) {
        if argv
            .get(1)
            .is_some_and(|key| key.as_ref() == b"notification-reentry")
        {
            NOTIFICATION_CALL_ORDER.with(|order| {
                order.borrow_mut().push(if command.name.as_str() == "set" {
                    "monitor-set"
                } else {
                    "monitor-get"
                });
            });
        }
    }
}

fn lifecycle_runtime() -> TrackingRuntime {
    TrackingRuntime {
        dbs: DbSet::new(2),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    }
}

fn lifecycle_context(
    handle: &mut ModuleRuntimeHandle,
) -> *mut crate::engine::extension::module::ValkeyModuleCtx {
    Box::into_raw(create_module_ctx(Box::into_raw(create_module_cmd_ctx(
        "test",
        "test.lifecycle",
        std::ptr::null_mut(),
        (handle as *mut ModuleRuntimeHandle).cast(),
    ))))
}

#[test]
fn lifecycle_call_registers_all_nonempty_exits_and_balances_shim_slots() {
    use super::super::lifecycle_tests::shim_functions;
    use super::super::string_obj_api::{ValkeyModule_AutoMemory, ValkeyModule_CreateString};
    use crate::engine::extension::module::auto_memory::entry_count;
    use crate::engine::extension::module::lifecycle_test_support::snapshot;
    let mut runtime = lifecycle_runtime();
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let ctx = lifecycle_context(&mut handle);
    let (call, replicate) = shim_functions();
    let before = snapshot();
    // SAFETY: formats describe live inputs and each returned owned root is freed once.
    unsafe {
        ValkeyModule_AutoMemory(ctx);
        let string = ValkeyModule_CreateString(ctx, b"key".as_ptr().cast(), 3);
        for (command, format) in [
            (c"GET", c"s"),
            (c"missing", c"s"),
            (c"SET", c"s"),
            (c"SET", c"sW"),
        ] {
            let reply = call(ctx, command.as_ptr(), format.as_ptr(), string);
            assert!(!reply.is_null());
            assert_eq!(ValkeyModuleString::ref_count(string), 1);
            assert_eq!(entry_count((*ctx).internal), 2);
            ValkeyModule_FreeCallReply(reply);
            assert_eq!(entry_count((*ctx).internal), 1);
        }
        let raw = call(
            ctx,
            c"GET".as_ptr(),
            c"c".as_ptr(),
            c"raw-lifecycle".as_ptr(),
        );
        assert_eq!((*raw).raw_proto(), Some(b"+RAW\r\n".as_slice()));
        assert_eq!(entry_count((*ctx).internal), 2);
        ValkeyModule_FreeCallReply(raw);
        assert!(call(ctx, c"GET".as_ptr(), c"sD".as_ptr(), string).is_null());
        assert_eq!(entry_count((*ctx).internal), 1);
        assert_eq!(
            replicate(ctx, c"SET".as_ptr(), c"ss".as_ptr(), string, string),
            1
        );
        assert_eq!(ValkeyModuleString::ref_count(string), 1);
        let mut vector = vec![string; 160];
        let large = call(
            ctx,
            c"GET".as_ptr(),
            c"vs".as_ptr(),
            vector.as_mut_ptr(),
            vector.len(),
            string,
        );
        assert!(!large.is_null());
        assert_eq!(ValkeyModuleString::ref_count(string), 1);
        ValkeyModule_FreeCallReply(large);
        free_module_ctx(ctx);
    }
    // Nine command/argument temporaries, one raw keymiss host key, and the automatic string.
    assert_eq!(snapshot().strings - before.strings, 11);
    assert_eq!(snapshot().replies - before.replies, 6);
}

#[derive(Clone, Copy, Default)]
struct NotificationObservation {
    ctx: usize,
    key: usize,
    was_enabled: bool,
    is_normal: bool,
    has_identity: bool,
    reentered: bool,
    db_idx: i32,
    resp_version: i32,
    wrong_module_calls: usize,
}

thread_local! {
    static NOTIFICATION_OBSERVATION: std::cell::Cell<NotificationObservation> =
        std::cell::Cell::new(NotificationObservation::default());
}

extern "C" fn lifecycle_notification(
    ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
    _event_type: libc::c_int,
    _event: *const libc::c_char,
    key: *mut ValkeyModuleString,
) -> libc::c_int {
    use super::super::string_obj_api::{ValkeyModule_AutoMemory, ValkeyModule_CreateString};
    // SAFETY: dispatch provides a live independent context and a borrowed host key.
    unsafe {
        let internal = (*ctx).internal;
        let was_enabled = (*internal).auto_memory.is_enabled;
        let reentered = super::super::ctx::ValkeyModule_IsModuleNameBusy(c"test".as_ptr()) == 1;
        let has_identity = !(*internal).module_ref.is_null()
            && super::super::ctx::get_module_name(ctx) == "test"
            && super::super::active_registry::active_module_context() == Some(ctx);
        ValkeyModule_AutoMemory(ctx);
        ValkeyModule_CreateString(ctx, b"temporary".as_ptr().cast(), 9);
        super::ValkeyModule_RetainString(ctx, key);
        let wrong_module_calls =
            NOTIFICATION_OBSERVATION.with(|observed| observed.get().wrong_module_calls);
        NOTIFICATION_OBSERVATION.with(|observed| {
            observed.set(NotificationObservation {
                ctx: ctx as usize,
                key: key as usize,
                was_enabled,
                reentered,
                has_identity,
                is_normal: (*internal).onload == 0,
                db_idx: (*internal).db_idx,
                resp_version: (*internal).resp_version,
                wrong_module_calls,
            })
        });
    }
    0
}

extern "C" fn wrong_module_notification(
    _ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
    _event_type: libc::c_int,
    _event: *const libc::c_char,
    _key: *mut ValkeyModuleString,
) -> libc::c_int {
    NOTIFICATION_OBSERVATION.with(|observed| {
        let mut value = observed.get();
        value.wrong_module_calls += 1;
        observed.set(value);
    });
    0
}

#[test]
fn lifecycle_notification_isolated_context_preserves_filter_and_retained_host_key() {
    use crate::engine::extension::module::lifecycle_test_support::snapshot;
    use crate::engine::extension::module::registry::ModuleNotification;
    use crate::engine::extension::module::{ModuleRef, ModuleRegistry};
    let mut runtime = lifecycle_runtime();
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let ctx = lifecycle_context(&mut handle);
    let mut registry = ModuleRegistry::new();
    NOTIFICATION_OBSERVATION.with(|observed| observed.set(NotificationObservation::default()));
    let mut module = ModuleRef::new("test", std::ptr::null_mut(), 1, true);
    module.notifications.push(ModuleNotification {
        types: 1 << 3,
        callback: lifecycle_notification,
    });
    registry.register(module).unwrap();
    let mut other = ModuleRef::new("other", std::ptr::null_mut(), 1, true);
    other.notifications.push(ModuleNotification {
        types: 1 << 3,
        callback: wrong_module_notification,
    });
    registry.register(other).unwrap();
    let mut argv = [b"SET".as_slice(), b"key", b"value"]
        .map(|bytes| Box::into_raw(ValkeyModuleString::from_bytes(bytes)));
    let before = snapshot();
    // SAFETY: registry's borrow is limited to snapshotting; callbacks may reenter it.
    unsafe {
        (*(*ctx).internal).db_idx = 1;
        (*(*ctx).internal).resp_version = 3;
        let _registry = super::super::ActiveModuleRegistryScope::enter(&mut registry).unwrap();
        let reply = VM_CallWithReply(ctx, argv.as_mut_ptr(), 3, 0);
        let observed = NOTIFICATION_OBSERVATION.with(std::cell::Cell::get);
        assert_ne!(observed.ctx, ctx as usize);
        assert!(!observed.was_enabled);
        assert!(observed.is_normal && observed.has_identity && observed.reentered);
        assert_eq!((observed.db_idx, observed.resp_version), (1, 3));
        assert_eq!(observed.wrong_module_calls, 0);
        assert!(!(*(*ctx).internal).auto_memory.is_enabled);
        assert_eq!(snapshot().strings - before.strings, 1);
        let retained = observed.key as *mut ValkeyModuleString;
        assert_eq!((*retained).as_bytes(), b"key");
        assert_eq!(ValkeyModuleString::ref_count(retained), 1);
        ValkeyModuleString::release(retained);
        ValkeyModule_FreeCallReply(reply);
        for string in argv {
            ValkeyModuleString::release(string);
        }
        free_module_ctx(ctx);
    }
    assert_eq!(snapshot().strings - before.strings, 5);
}

#[derive(Clone, Copy, Default)]
struct NotificationCallObservation {
    ctx: usize,
    internal: usize,
    runtime_handle: usize,
    key: usize,
    calls: usize,
    initial_entries: usize,
    final_entries: usize,
    key_refs: usize,
    was_enabled: bool,
    has_identity: bool,
    is_keymiss: bool,
    nested_ok: bool,
    db_idx: i32,
    resp_version: i32,
}

thread_local! {
    static NOTIFICATION_CALL_OBSERVATION: std::cell::Cell<NotificationCallObservation> =
        std::cell::Cell::new(NotificationCallObservation::default());
    static NOTIFICATION_CALL_ORDER: std::cell::RefCell<Vec<&'static str>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

extern "C" fn notification_nested_call(
    ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
    event_type: libc::c_int,
    event: *const libc::c_char,
    key: *mut ValkeyModuleString,
) -> libc::c_int {
    use super::super::lifecycle_tests::shim_functions;
    use super::super::string_obj_api::{ValkeyModule_AutoMemory, ValkeyModule_CreateString};
    use crate::engine::extension::module::auto_memory::entry_count;
    NOTIFICATION_CALL_ORDER.with(|order| order.borrow_mut().push("callback-start"));
    // SAFETY: dispatch supplies a live independent context and borrowed host key;
    // the nested shim call borrows both arguments only until it returns.
    unsafe {
        let internal = (*ctx).internal;
        let was_enabled = (*internal).auto_memory.is_enabled;
        let initial_entries = entry_count(internal);
        let has_identity = (*internal).onload == 0
            && !(*internal).module_ref.is_null()
            && super::super::ctx::get_module_name(ctx) == "test"
            && super::super::active_registry::active_module_context() == Some(ctx);
        ValkeyModule_AutoMemory(ctx);
        let value = ValkeyModule_CreateString(ctx, b"inner-value".as_ptr().cast(), 11);
        let opened = super::super::key_api::ValkeyModule_OpenKey(ctx, key, 2);
        super::ValkeyModule_RetainString(ctx, key);
        let (call, _) = shim_functions();
        let reply = call(ctx, c"SET".as_ptr(), c"ss".as_ptr(), key, value);
        let nested_ok = !opened.is_null()
            && !reply.is_null()
            && (*reply).effective_type() == super::call_reply::VALKEYMODULE_REPLY_STRING
            && std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len)
                == b"OK";
        NOTIFICATION_CALL_OBSERVATION.with(|observed| {
            observed.set(NotificationCallObservation {
                ctx: ctx as usize,
                internal: internal as usize,
                runtime_handle: (*internal).runtime_handle as usize,
                key: key as usize,
                calls: observed.get().calls + 1,
                initial_entries,
                final_entries: entry_count(internal),
                key_refs: ValkeyModuleString::ref_count(key),
                was_enabled,
                has_identity,
                is_keymiss: event_type == 1 << 11
                    && std::ffi::CStr::from_ptr(event).to_bytes() == b"keymiss",
                nested_ok,
                db_idx: (*internal).db_idx,
                resp_version: (*internal).resp_version,
            });
        });
    }
    NOTIFICATION_CALL_ORDER.with(|order| order.borrow_mut().push("callback-end"));
    0
}

#[test]
fn lifecycle_notification_nested_call_preserves_outer_tracking_and_resources() {
    use super::super::string_obj_api::{ValkeyModule_AutoMemory, ValkeyModule_CreateString};
    use crate::engine::extension::module::auto_memory::entry_count;
    use crate::engine::extension::module::lifecycle_test_support::snapshot;
    use crate::engine::extension::module::registry::ModuleNotification;
    use crate::engine::extension::module::{ModuleRef, ModuleRegistry};

    for is_outer_auto in [false, true] {
        let mut runtime = lifecycle_runtime();
        let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
        let ctx = lifecycle_context(&mut handle);
        let mut registry = ModuleRegistry::new();
        NOTIFICATION_CALL_OBSERVATION
            .with(|observed| observed.set(NotificationCallObservation::default()));
        NOTIFICATION_OBSERVATION.with(|observed| observed.set(NotificationObservation::default()));
        NOTIFICATION_CALL_ORDER.with(|order| order.borrow_mut().clear());
        let mut module = ModuleRef::new("test", std::ptr::null_mut(), 1, true);
        module.notifications.push(ModuleNotification {
            types: (1 << 11) | (1 << 3),
            callback: notification_nested_call,
        });
        registry.register(module).unwrap();
        let mut other = ModuleRef::new("other", std::ptr::null_mut(), 1, true);
        other.notifications.push(ModuleNotification {
            types: (1 << 11) | (1 << 3),
            callback: wrong_module_notification,
        });
        registry.register(other).unwrap();
        let before = snapshot();
        // SAFETY: contexts, registry, handle and argv remain live for synchronous
        // calls; retained host key owns a separate reference after callback cleanup.
        unsafe {
            (*(*ctx).internal).db_idx = 1;
            (*(*ctx).internal).resp_version = 3;
            if is_outer_auto {
                ValkeyModule_AutoMemory(ctx);
            }
            let outer = ValkeyModule_CreateString(ctx, b"outer".as_ptr().cast(), 5);
            let mut argv = [b"GET".as_slice(), b"notification-reentry"]
                .map(|bytes| Box::into_raw(ValkeyModuleString::from_bytes(bytes)));
            let reply = {
                let _registry =
                    super::super::ActiveModuleRegistryScope::enter(&mut registry).unwrap();
                let _context = super::super::ActiveModuleContextScope::enter(ctx);
                let reply = VM_CallWithReply(ctx, argv.as_mut_ptr(), 2, 1 << 4);
                assert_eq!(
                    super::super::active_registry::active_module_context(),
                    Some(ctx)
                );
                reply
            };
            assert!(!reply.is_null());
            assert_eq!(
                (*reply).effective_type(),
                super::call_reply::VALKEYMODULE_REPLY_NULL
            );
            let observed = NOTIFICATION_CALL_OBSERVATION.with(std::cell::Cell::get);
            assert_eq!(
                observed.calls, 1,
                "nested SET must not dispatch another notification"
            );
            assert!(observed.is_keymiss && observed.nested_ok && observed.has_identity);
            assert!(!observed.was_enabled);
            assert_eq!((observed.initial_entries, observed.final_entries), (0, 3));
            assert_ne!(observed.ctx, ctx as usize);
            assert_ne!(observed.internal, (*ctx).internal as usize);
            assert_eq!(
                observed.runtime_handle,
                (*(*ctx).internal).runtime_handle as usize
            );
            assert_eq!((observed.db_idx, observed.resp_version), (1, 3));
            assert_eq!(
                observed.key_refs, 2,
                "shim must release its borrowed host-key slot"
            );
            assert_eq!(
                NOTIFICATION_OBSERVATION.with(|observed| observed.get().wrong_module_calls),
                0
            );
            assert_eq!((*(*ctx).internal).auto_memory.is_enabled, is_outer_auto);
            assert_eq!(
                entry_count((*ctx).internal),
                if is_outer_auto { 2 } else { 0 }
            );
            assert_eq!(ValkeyModuleString::ref_count(outer), 1);
            assert_eq!((*outer).as_bytes(), b"outer");
            assert_eq!(snapshot().strings - before.strings, 2);
            assert_eq!(snapshot().keys - before.keys, 1);
            assert_eq!(snapshot().replies - before.replies, 1);
            if !is_outer_auto {
                ValkeyModuleString::release(outer);
                ValkeyModule_FreeCallReply(reply);
            }
            for string in argv {
                assert_eq!(ValkeyModuleString::ref_count(string), 1);
                ValkeyModuleString::release(string);
            }
            free_module_ctx(ctx);
            let retained = observed.key as *mut ValkeyModuleString;
            assert_eq!((*retained).as_bytes(), b"notification-reentry");
            assert_eq!(ValkeyModuleString::ref_count(retained), 1);
            ValkeyModuleString::release(retained);
        }
        drop(handle);
        assert_eq!(runtime.command_calls, 2);
        assert_eq!(
            runtime.tracked,
            vec![(42, Bytes::from_static(b"notification-reentry"))]
        );
        assert!(runtime
            .dbs
            .get_mut(0)
            .unwrap()
            .get_no_expire(b"notification-reentry")
            .is_none());
        assert_eq!(
            runtime
                .dbs
                .get_mut(1)
                .unwrap()
                .get_no_expire(b"notification-reentry")
                .unwrap()
                .as_bytes(),
            b"inner-value"
        );
        assert_eq!(snapshot().strings - before.strings, 6);
        assert_eq!(snapshot().keys - before.keys, 1);
        assert_eq!(snapshot().replies - before.replies, 2);
        NOTIFICATION_CALL_ORDER.with(|order| {
            assert_eq!(
                *order.borrow(),
                [
                    "execute-get",
                    "monitor-get",
                    "callback-start",
                    "execute-set",
                    "monitor-set",
                    "publish-inner",
                    "publish-inner-shard",
                    "callback-end",
                    "publish-outer",
                    "publish-outer-shard",
                    "track-outer",
                ]
            );
        });
    }
}

extern "C" fn retain_scan_key(
    ctx: *mut crate::engine::extension::module::ValkeyModuleCtx,
    key: *mut ValkeyModuleString,
    _handle: *mut crate::engine::extension::module::ValkeyModuleKey,
    private: *mut libc::c_void,
) {
    // SAFETY: Scan supplies a borrowed key; private is a live writable retained-pointer slot.
    unsafe {
        super::ValkeyModule_RetainString(ctx, key);
        *private.cast::<*mut ValkeyModuleString>() = key;
        super::super::string_obj_api::ValkeyModule_CreateString(ctx, b"auto".as_ptr().cast(), 4);
    }
}

extern "C" fn retain_scan_field(
    _key: *mut crate::engine::extension::module::ValkeyModuleKey,
    field: *mut ValkeyModuleString,
    value: *mut ValkeyModuleString,
    private: *mut libc::c_void,
) {
    // SAFETY: ScanKey supplies live borrowed strings and private points to two writable slots.
    unsafe {
        super::ValkeyModule_RetainString(std::ptr::null_mut(), field);
        super::ValkeyModule_RetainString(std::ptr::null_mut(), value);
        *private.cast::<*mut ValkeyModuleString>() = field;
        *private.cast::<*mut ValkeyModuleString>().add(1) = value;
    }
}

#[test]
fn lifecycle_scan_retains_borrowed_values_and_uses_original_key_context() {
    use super::super::scan::{ValkeyModuleScanCursor, ValkeyModule_Scan, ValkeyModule_ScanKey};
    use crate::engine::extension::module::auto_memory::entry_count;
    use crate::engine::extension::module::{ModuleKeyHandle, ValkeyModuleKey};
    let mut runtime = lifecycle_runtime();
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let a = lifecycle_context(&mut handle);
    let b = lifecycle_context(&mut handle);
    // SAFETY: both contexts, key, cursor and callback storage stay live for each scan.
    unsafe {
        super::super::string_obj_api::ValkeyModule_AutoMemory(a);
        (*(*b).internal).db_idx = 1;
        let mut retained = std::ptr::null_mut::<ValkeyModuleString>();
        let mut cursor = ValkeyModuleScanCursor {
            index: 0,
            done: false,
        };
        ValkeyModule_Scan(
            a,
            &mut cursor,
            Some(retain_scan_key),
            (&mut retained as *mut *mut ValkeyModuleString).cast(),
        );
        assert_eq!((*retained).as_bytes(), b"scan-key");
        assert_eq!(ValkeyModuleString::ref_count(retained), 1);
        assert_eq!(entry_count((*a).internal), 1);
        let mut key = ModuleKeyHandle::new(std::ptr::null_mut(), b"key".to_vec(), 1, -2, false);
        key.owner = std::ptr::NonNull::new((*a).internal);
        let key = ValkeyModuleKey::from_handle(Box::into_raw(key));
        let _tls = super::super::ActiveModuleContextScope::enter(b);
        let mut outputs = [std::ptr::null_mut::<ValkeyModuleString>(); 2];
        cursor.index = 0;
        cursor.done = false;
        ValkeyModule_ScanKey(
            key,
            &mut cursor,
            Some(retain_scan_field),
            outputs.as_mut_ptr().cast(),
        );
        assert_eq!((*outputs[0]).as_bytes(), b"0");
        assert_eq!((*outputs[1]).as_bytes(), b"value");
        super::super::key_api::ValkeyModule_CloseKey(key);
        free_module_ctx(a);
        free_module_ctx(b);
        assert_eq!((*retained).as_bytes(), b"scan-key");
        ValkeyModuleString::release(retained);
        for string in outputs {
            ValkeyModuleString::release(string);
        }
    }
}

#[test]
fn module_call_tracks_read_keys_for_the_outer_client() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"GET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 2, 0) };

    assert!(!reply.is_null());
    // SAFETY: reply was allocated by VM_CallWithReply and is consumed once.
    unsafe { ValkeyModule_FreeCallReply(reply) };
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.tracked, vec![(42, Bytes::from_static(b"key"))]);
}

#[test]
fn reply_raw_forwards_the_exact_protocol_bytes() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.call_argv",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let protocol = b"*2\r\n$3\r\none\r\n$5\r\ntw\0o!\r\n";

    // SAFETY: ctx is live and protocol points to protocol.len() bytes.
    let result = unsafe {
        VM_ReplyRaw(
            ctx,
            protocol.as_ptr().cast::<libc::c_char>(),
            protocol.len(),
        )
    };

    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(result, crate::engine::extension::module::VALKEYMODULE_OK);
    assert_eq!(runtime.raw_replies, vec![protocol.to_vec()]);
}

#[test]
fn allowed_block_sync_read_tracks_original_client_identity() {
    let mut runtime = lifecycle_runtime();
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let ctx = lifecycle_context(&mut handle);
    let mut argv = [b"GET".as_slice(), b"tracked"]
        .map(|value| Box::into_raw(ValkeyModuleString::from_bytes(value)));
    // SAFETY: the context and caller-owned argv remain live until the synchronous call ends.
    unsafe {
        assert_eq!(
            VM_CallArgv(
                ctx,
                argv.as_mut_ptr(),
                2,
                1 << 11,
                std::ptr::null(),
                std::ptr::null_mut()
            ),
            crate::engine::extension::module::VALKEYMODULE_OK
        );
        for string in argv {
            ValkeyModuleString::release(string);
        }
        free_module_ctx(ctx);
    }
    drop(handle);
    assert_eq!(runtime.tracked, vec![(42, Bytes::from_static(b"tracked"))]);
}

#[test]
fn module_call_forwards_pending_publish_to_the_outer_client() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"PUBLISH")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"channel")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"message")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 3, 0) };

    assert!(!reply.is_null());
    // SAFETY: reply was allocated by VM_CallWithReply and is consumed once.
    unsafe { ValkeyModule_FreeCallReply(reply) };
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(
        runtime.published,
        vec![(
            Bytes::from_static(b"channel"),
            Bytes::from_static(b"message"),
            false,
        )]
    );
}

#[test]
fn module_call_respects_deny_oom_flag_before_invoking_handler() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: true,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 3, 1 << 9) };

    assert!(!reply.is_null());
    // SAFETY: reply points to a live call reply until it is freed below.
    unsafe {
        assert_eq!(
            (*reply).effective_type(),
            super::call_reply::VALKEYMODULE_REPLY_ERROR
        );
        assert_eq!(
            std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len),
            b"OOM command not allowed when used memory > 'maxmemory'."
        );
        ValkeyModule_FreeCallReply(reply);
    }
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_script_mode_rejects_writes_without_enough_replicas() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: false,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 3, 1 << 6) };

    assert!(!reply.is_null());
    // SAFETY: reply points to a live call reply until it is freed below.
    unsafe {
        assert_eq!(
            (*reply).effective_type(),
            super::call_reply::VALKEYMODULE_REPLY_ERROR
        );
        assert_eq!(
            std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len),
            b"NOREPLICAS Not enough good replicas to write."
        );
        ValkeyModule_FreeCallReply(reply);
    }
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_script_mode_rejects_writes_on_readonly_replica() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: true,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 3, 1 << 6) };

    assert!(!reply.is_null());
    // SAFETY: reply points to a live call reply until it is freed below.
    unsafe {
        assert_eq!(
            (*reply).effective_type(),
            super::call_reply::VALKEYMODULE_REPLY_ERROR
        );
        assert_eq!(
            std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len),
            b"READONLY You can't write against a read only replica."
        );
        ValkeyModule_FreeCallReply(reply);
    }
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_script_mode_rejects_commands_on_stale_replica() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: true,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"GET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 2, 1 << 6) };

    assert!(!reply.is_null());
    // SAFETY: reply points to a live call reply until it is freed below.
    unsafe {
        assert_eq!(
            (*reply).effective_type(),
            super::call_reply::VALKEYMODULE_REPLY_ERROR
        );
        assert_eq!(
            std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len),
            b"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
        );
        ValkeyModule_FreeCallReply(reply);
    }
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_script_mode_rejects_writes_after_bgsave_error() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: true,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 3, 1 << 6) };

    assert!(!reply.is_null());
    // SAFETY: reply points to a live call reply until it is freed below.
    unsafe {
        assert_eq!(
            (*reply).effective_type(),
            super::call_reply::VALKEYMODULE_REPLY_ERROR
        );
        assert_eq!(
            std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len),
            b"MISCONF Errors writing to the RDB file, writes are disabled because stop-writes-on-bgsave-error is enabled."
        );
        ValkeyModule_FreeCallReply(reply);
    }
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_dry_run_returns_no_reply_without_invoking_handler() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 3, 1 << 10) };

    assert!(reply.is_null());
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_argv_dry_run_succeeds_without_invoking_handler() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.vm_call_argv_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let result = unsafe {
        VM_CallArgv(
            ctx,
            strings.as_mut_ptr(),
            3,
            1 << 10,
            std::ptr::null(),
            std::ptr::null_mut(),
        )
    };

    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(result, crate::engine::extension::module::VALKEYMODULE_OK);
    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_argv_dry_run_reports_an_empty_protocol_reply() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: None,
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.vm_call_argv_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];
    let handlers = TestReplyHandlers::with_on_resp_available(record_proto_len);
    let mut observed_len = usize::MAX;

    // SAFETY: all pointers remain live for this synchronous call, and TestReplyHandlers
    // has the same repr(C) field layout as the production reply handler structure.
    let result = unsafe {
        VM_CallArgv(
            ctx,
            strings.as_mut_ptr(),
            3,
            1 << 10,
            (&handlers as *const TestReplyHandlers).cast(),
            (&mut observed_len as *mut usize).cast(),
        )
    };

    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(result, crate::engine::extension::module::VALKEYMODULE_OK);
    assert_eq!(observed_len, 0);
    assert_eq!(runtime.command_calls, 0);
}

#[test]
fn module_call_dry_run_as_user_returns_acl_error_without_invoking_handler() {
    let mut runtime = TrackingRuntime {
        dbs: DbSet::new(1),
        tracked: Vec::new(),
        raw_replies: Vec::new(),
        published: Vec::new(),
        is_oom: false,
        has_enough_replicas_to_write: true,
        is_readonly_replica: false,
        is_stale_replica: false,
        has_persistence_write_error: false,
        module_acl_error: Some("NOPERM No permissions to access a key".to_string()),
        command_calls: 0,
    };
    let mut handle = Box::new(ModuleRuntimeHandle::new(&mut runtime));
    let internal = create_module_cmd_ctx(
        "test",
        "test.rm_call_flags",
        std::ptr::null_mut(),
        (&mut *handle as *mut ModuleRuntimeHandle).cast(),
    );
    let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
    let mut strings = [
        Box::into_raw(ValkeyModuleString::from_bytes(b"SET")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"key")),
        Box::into_raw(ValkeyModuleString::from_bytes(b"value")),
    ];

    // SAFETY: all context and argv pointers remain live for this synchronous call.
    let reply = unsafe { VM_CallWithReply(ctx, strings.as_mut_ptr(), 3, (1 << 10) | (1 << 5)) };

    assert!(!reply.is_null());
    // SAFETY: reply points to a live call reply until it is freed below.
    unsafe {
        assert_eq!(
            (*reply).effective_type(),
            super::call_reply::VALKEYMODULE_REPLY_ERROR
        );
        assert_eq!(
            std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len),
            b"NOPERM No permissions to access a key"
        );
        ValkeyModule_FreeCallReply(reply);
    }
    for string in strings {
        // SAFETY: each module string was allocated above and is consumed once.
        unsafe {
            ValkeyModuleString::release(string);
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}
