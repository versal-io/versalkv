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
                name: "set",
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
                name: "get",
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
                name: "publish",
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
        if name.eq_ignore_ascii_case(b"publish") {
            conn.record_notification((args.args[0].clone(), args.args[1].clone()));
            return Value::integer(1);
        }
        Value::bulk(Bytes::from_static(b"value"))
    }

    fn register_module_command(
        &mut self,
        _name: &'static str,
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
        self.published.push((channel, message, is_shard));
        Some(1)
    }

    fn propagate_module_command(&mut self, _frame: Vec<u8>) -> bool {
        false
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
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
            let mut string = Box::from_raw(string);
            string.free();
        }
    }
    // SAFETY: ctx was allocated above and is no longer used after this call.
    unsafe { free_module_ctx(ctx) };
    drop(handle);

    assert_eq!(runtime.command_calls, 0);
}
