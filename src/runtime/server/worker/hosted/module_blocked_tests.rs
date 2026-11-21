//! Deterministic native completion tests, without running an event-loop turn.

use super::*;
use crate::engine::command::context::{BlockedState, Token};
use crate::engine::extension::module::blocking::{ModuleBlockCallbacks, ModuleBlockedShared};
use crate::engine::extension::module::thread_bridge::{
    next_id, ModuleBlockOrigin, ModuleThreadBridgeHandle,
};
use crate::engine::extension::module::{
    ModuleRef, ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_OK,
};
use crate::framework::capabilities::connection::Connection;
use libc::c_void;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

#[derive(Default)]
struct CallbackTrace {
    shared: Weak<ModuleBlockedShared>,
    replies: usize,
    frees: usize,
    free_data: usize,
    has_unlocked_lifecycle: bool,
    has_unlocked_replies: bool,
    effects: Vec<ModuleCallEffects>,
}

thread_local! {
    static CALLBACK_TRACE: RefCell<CallbackTrace> = RefCell::new(CallbackTrace::default());
}

extern "C" fn reply_callback(
    _ctx: *mut ValkeyModuleCtx,
    _argv: *mut *mut ValkeyModuleString,
    _argc: libc::c_int,
) -> libc::c_int {
    let effects = CALLBACK_TRACE.with(|trace| {
        let mut trace = trace.borrow_mut();
        trace.replies += 1;
        std::mem::take(&mut trace.effects)
    });
    for effect in effects {
        assert!(ModuleCallEffectScope::collect(effect).is_none());
    }
    VALKEYMODULE_OK
}

extern "C" fn free_callback(_ctx: *mut ValkeyModuleCtx, data: *mut c_void) {
    CALLBACK_TRACE.with(|trace| {
        let mut trace = trace.borrow_mut();
        trace.frees += 1;
        trace.free_data = data as usize;
        if let Some(shared) = trace.shared.upgrade() {
            trace.has_unlocked_lifecycle = shared.lifecycle.try_lock().is_ok();
            trace.has_unlocked_replies = shared.replies.try_lock().is_ok();
        }
    });
}

fn fixture() -> (ServerWorkerState, Token, Arc<ModuleBlockedShared>) {
    let mut worker = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut worker.registry);
    worker.ctx.set_command_registry_snapshot(&worker.registry);
    worker
        .ctx
        .module_registry_mut()
        .register(ModuleRef::new(
            "native-lifecycle",
            std::ptr::null_mut(),
            1,
            true,
        ))
        .unwrap();
    let identity = worker
        .ctx
        .module_registry()
        .lookup("native-lifecycle")
        .unwrap()
        .identity
        .clone()
        .unwrap();
    let bridge = ModuleThreadBridgeHandle::new().unwrap();
    worker.ctx.module_registry_mut().thread_bridge = Some(bridge.clone());
    let mut conn = Connection::new_fake();
    let token = Token(crate::foundation::memory::client_id::next_client_id());
    conn.set_token_for_test(token);
    conn.begin_admitted_command("native-lifecycle.wait");
    let args = crate::engine::command::CommandArgs::from_parts(vec![Bytes::from_static(
        b"native-lifecycle.wait",
    )]);
    let id = ModuleBlockedId(next_id().unwrap());
    let shared = ModuleBlockedShared::new(
        id,
        identity,
        bridge,
        ModuleBlockOrigin {
            session: conn.session_id(),
            token,
            command_seq: conn.command_seq,
            args: args.clone(),
            request_bytes: 32,
            db_idx: 0,
            resp_version: 2,
            username: "default".into(),
            kind: ModuleContextKind::Synchronous,
            is_multi: false,
            is_script: false,
            deny_blocking: false,
        },
    )
    .unwrap();
    conn.blocked = Some(BlockedState {
        cmd: BlockedCmd::Module { id },
        keys: Vec::new(),
        timeout_ms: 0,
        start_time_ms: 0,
        orig_args: args,
        req_bytes: 32,
        orig_wrapped: None,
    });
    worker
        .ctx
        .blocking_registry_mut()
        .register_keyless_client(conn.session_id());
    worker.connection_registry.register_for_test(conn);
    worker.ctx.module_registry_mut().native_blocks.insert(
        id,
        NativeModuleBlock {
            shared: shared.clone(),
            callbacks: ModuleBlockCallbacks {
                reply: Some(reply_callback),
                timeout: None,
                free: Some(free_callback),
                disconnect: None,
            },
            active_time_usec: 0,
        },
    );
    CALLBACK_TRACE.with(|trace| {
        *trace.borrow_mut() = CallbackTrace {
            shared: Arc::downgrade(&shared),
            ..CallbackTrace::default()
        }
    });
    (worker, token, shared)
}

fn assert_cleanup(shared: &Arc<ModuleBlockedShared>, data: *mut c_void, replies: usize) {
    let lifecycle = lock(&shared.lifecycle);
    assert_eq!(lifecycle.stage, ModuleBlockStage::Completed);
    assert!(lifecycle.private_data.0.is_null());
    drop(lifecycle);
    assert!(shared
        .with_replies(|_| panic!("completed replies must stay sealed"))
        .is_none());
    CALLBACK_TRACE.with(|trace| {
        let trace = trace.borrow();
        assert_eq!(trace.replies, replies);
        assert_eq!(trace.frees, 1);
        assert_eq!(trace.free_data, data as usize);
        assert!(trace.has_unlocked_lifecycle);
        assert!(trace.has_unlocked_replies);
    });
}

#[test]
fn native_completion_waits_for_armed_transfer_and_cleans_once_outside_mutexes() {
    let (mut worker, token, shared) = fixture();
    let identity = shared.module.clone();
    let mut data = Box::new(7_u8);
    let data_ptr = (&mut *data as *mut u8).cast();
    assert_eq!(
        shared.with_replies(|builder| builder.add_integer(73)),
        Some(())
    );
    assert_eq!(shared.accept_unblock(data_ptr), Ok(()));
    worker.service_native_module_completions();
    worker.service_native_module_completions();
    assert_eq!(lock(&shared.lifecycle).stage, ModuleBlockStage::Publishing);
    assert!(worker
        .ctx
        .module_registry()
        .native_blocks
        .contains_key(&shared.id));
    assert_eq!(worker.connection_registry[token].output_buffer_bytes(), 0);
    assert!(worker.connection_registry[token].blocked.is_some());
    CALLBACK_TRACE.with(|trace| {
        assert_eq!(trace.borrow().replies, 0);
        assert_eq!(trace.borrow().frees, 0);
    });
    // The fixture now represents the outer command's completed ownership transfer.
    lock(&shared.lifecycle).stage = ModuleBlockStage::Armed;
    worker.service_native_module_completions();
    assert_cleanup(&shared, data_ptr, 1);
    assert!(!worker
        .ctx
        .module_registry()
        .native_blocks
        .contains_key(&shared.id));
    assert!(worker.connection_registry[token].blocked.is_none());
    assert_eq!(
        worker.connection_registry[token].output_buffer_bytes(),
        b":73\r\n".len()
    );
    let output_bytes = worker.connection_registry[token].output_buffer_bytes();
    worker.service_native_module_completions();
    worker.shutdown_native_module_blocks();
    assert_cleanup(&shared, data_ptr, 1);
    assert_eq!(
        worker.connection_registry[token].output_buffer_bytes(),
        output_bytes
    );
    assert_eq!(identity.blocked_handles.load(Ordering::Acquire), 1);
    drop(shared);
    assert_eq!(identity.blocked_handles.load(Ordering::Acquire), 0);
}

#[test]
fn native_completion_never_borrows_a_reused_token_or_later_command() {
    for mismatch in ["session", "command", "blocked-id"] {
        let (mut worker, token, shared) = fixture();
        let mut data = Box::new(8_u8);
        let data_ptr = (&mut *data as *mut u8).cast();
        assert_eq!(
            shared.with_replies(|builder| builder.add_integer(99)),
            Some(())
        );
        assert_eq!(shared.accept_unblock(data_ptr), Ok(()));
        lock(&shared.lifecycle).stage = ModuleBlockStage::Armed;
        match mismatch {
            "session" => {
                let old = worker.connection_registry.remove(token).unwrap();
                let mut replacement = Connection::new(token, ([127, 0, 0, 1], 0).into(), false);
                assert_ne!(replacement.session_id(), old.session_id());
                replacement.blocked = old.blocked.clone();
                replacement.command_seq = old.command_seq;
                worker.connection_registry.register_for_test(replacement);
            }
            "command" => worker.connection_registry[token].command_seq += 1,
            _ => {
                worker.connection_registry[token]
                    .blocked
                    .as_mut()
                    .unwrap()
                    .cmd = BlockedCmd::Module {
                    id: ModuleBlockedId(next_id().unwrap()),
                }
            }
        }
        let session = worker.connection_registry[token].session_id();
        let command_seq = worker.connection_registry[token].command_seq;
        let blocked_cmd = worker.connection_registry[token]
            .blocked
            .as_ref()
            .unwrap()
            .cmd
            .clone();
        let output_before = worker.connection_registry[token].output_buffer_bytes();
        worker.service_native_module_completions();
        worker.service_native_module_completions();
        assert_cleanup(&shared, data_ptr, 0);
        let current = &worker.connection_registry[token];
        assert_eq!(current.session_id(), session, "{mismatch}");
        assert_eq!(current.command_seq, command_seq, "{mismatch}");
        assert_eq!(
            current.blocked.as_ref().unwrap().cmd,
            blocked_cmd,
            "{mismatch}"
        );
        assert_eq!(current.output_buffer_bytes(), output_before, "{mismatch}");
        assert!(!worker
            .ctx
            .module_registry()
            .native_blocks
            .contains_key(&shared.id));
    }
}

#[test]
fn thread_runtime_review_reserved_owner_defers_native_completion() {
    let (mut worker, token, shared) = fixture();
    let bridge = shared.bridge.clone();
    lock(&shared.lifecycle).stage = ModuleBlockStage::Armed;
    shared
        .with_replies(|builder| builder.add_integer(41))
        .unwrap();
    shared.accept_unblock(std::ptr::null_mut()).unwrap();
    bridge.take_completions();
    assert!(bridge.prepare_sleep());
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let native = std::thread::spawn(move || {
        bridge.try_acquire().unwrap();
        ready_tx.send(()).unwrap();
        finish_rx.recv().unwrap();
        bridge.release().unwrap();
    });
    ready_rx.recv().unwrap();
    worker.service_native_module_completions();
    assert!(worker.connection_registry[token].blocked.is_some());
    assert_eq!(worker.connection_registry[token].output_buffer_bytes(), 0);
    CALLBACK_TRACE.with(|trace| assert_eq!(trace.borrow().replies, 0));
    finish_tx.send(()).unwrap();
    worker.service_module_thread_leases();
    native.join().unwrap();
    assert!(worker.connection_registry[token].blocked.is_none());
    CALLBACK_TRACE.with(|trace| assert_eq!(trace.borrow().replies, 1));
}

fn install_aof(worker: &mut ServerWorkerState, label: &str) -> std::path::PathBuf {
    use crate::foundation::io::persistence::{AofAppender, AofFsync};
    let path = std::env::temp_dir().join(format!(
        "thread-runtime-review-{label}-{}-{}.aof",
        std::process::id(),
        next_id().unwrap(),
    ));
    let appender = AofAppender::open_append_path(&path, AofFsync::Always).unwrap();
    worker.ctx.config_mut().persistence.aof_enabled = true;
    worker.ctx.config_mut().persistence.aof_fsync = AofFsync::Always;
    worker.ctx.aof_mut().journal =
        Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
    worker.ctx.aof_mut().fsynced_repl_offset = Some(0);
    path
}

#[test]
fn thread_runtime_review_native_replies_follow_aof_write_and_fsync() {
    for callback in [NativeModuleCallback::Reply, NativeModuleCallback::Timeout] {
        let (mut worker, token, shared) = fixture();
        let path = install_aof(&mut worker, "completion");
        let frame = crate::engine::distributed::replication::protocol::encode_command(&[
            b"SET",
            b"callback-key",
            b"value",
        ]);
        worker.feed_aof(&frame);
        assert!(std::fs::read(&path).unwrap().is_empty());
        lock(&shared.lifecycle).stage = ModuleBlockStage::Armed;
        match callback {
            NativeModuleCallback::Reply => {
                shared
                    .with_replies(|builder| builder.add_integer(7))
                    .unwrap();
                shared.accept_unblock(std::ptr::null_mut()).unwrap();
                worker.service_native_module_completions();
            }
            _ => {
                worker
                    .ctx
                    .module_registry_mut()
                    .native_blocks
                    .get_mut(&shared.id)
                    .unwrap()
                    .callbacks
                    .timeout = Some(reply_callback);
                assert!(worker.timeout_native_module_client(token, Some(Value::Integer(7))));
            }
        }
        assert_eq!(
            worker.connection_registry[token].output_buffer_bytes(),
            b":7\r\n".len()
        );
        assert_eq!(std::fs::read(&path).unwrap(), frame);
        assert_eq!(worker.ctx.aof().fsynced_repl_offset, Some(0));
        assert_eq!(
            worker
                .ctx
                .aof()
                .journal
                .as_ref()
                .unwrap()
                .appender
                .fsynced_reported_offset(),
            Some(0)
        );
        drop(worker);
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn thread_runtime_review_aof_barrier_preserves_business_waiters() {
    use crate::runtime::server::worker::connection::write::AofBarrierOutcome;
    let (mut worker, token, _) = fixture();
    worker.connection_registry[token]
        .blocked
        .as_mut()
        .unwrap()
        .cmd = BlockedCmd::Wait {
        required: 0,
        target_offset: crate::engine::distributed::replication::ReplicationOffset::ZERO,
    };
    let path = install_aof(&mut worker, "barrier");
    worker.feed_aof(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(
        worker.flush_native_module_aof_barrier(),
        AofBarrierOutcome::Ready
    );
    assert!(worker.connection_registry[token].blocked.is_some());
    assert_eq!(worker.connection_registry[token].output_buffer_bytes(), 0);
    assert_eq!(std::fs::read(&path).unwrap(), b"*1\r\n$4\r\nPING\r\n");
    worker.flush_aof_if_needed();
    assert!(worker.connection_registry[token].blocked.is_none());
    drop(worker);
    std::fs::remove_file(path).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn thread_runtime_review_failed_aof_barrier_never_queues_success() {
    use crate::foundation::io::persistence::{AofAppender, AofFsync};
    let (mut worker, token, shared) = fixture();
    worker.ctx.config_mut().persistence.aof_enabled = true;
    worker.ctx.config_mut().persistence.aof_fsync = AofFsync::Always;
    worker.ctx.aof_mut().journal = Some(
        crate::foundation::io::persistence::journal::AofJournal::new(
            AofAppender::open_append_path(std::path::Path::new("/dev/full"), AofFsync::Always)
                .unwrap(),
        ),
    );
    worker.feed_aof(b"*1\r\n$4\r\nPING\r\n");
    lock(&shared.lifecycle).stage = ModuleBlockStage::Armed;
    shared
        .with_replies(|builder| builder.add_integer(99))
        .unwrap();
    shared.accept_unblock(std::ptr::null_mut()).unwrap();
    worker.service_native_module_completions();
    assert!(worker.ctx.is_shutdown_requested());
    assert!(shared.bridge.is_closing());
    assert!(worker.connection_registry[token].blocked.is_some());
    assert_eq!(worker.connection_registry[token].output_buffer_bytes(), 0);
}

#[derive(Clone, Copy, Debug)]
enum InitialRewrite {
    Active,
    Scheduled,
}

fn transition_call(
    parts: &[&[u8]],
) -> crate::engine::extension::module::thread_bridge::OwnedModuleCall {
    use crate::engine::extension::module::api::call::ModuleCallFormat;
    use crate::engine::extension::module::thread_bridge::{
        ModuleThreadContextId, ModuleThreadSnapshot, OwnedModuleCall,
    };
    OwnedModuleCall {
        context: ModuleThreadSnapshot {
            id: ModuleThreadContextId(next_id().unwrap()),
            module: None,
            db_idx: 0,
            resp_version: 2,
            username: "default".into(),
            auth_user: None,
        },
        argv: parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part))
            .collect(),
        format: ModuleCallFormat::from_raw(1),
    }
}

fn install_initial_rewrite(worker: &mut ServerWorkerState, rewrite: InitialRewrite) {
    match rewrite {
        InitialRewrite::Active => {
            worker.ctx.config_mut().persistence.aof_enabled = true;
            worker.ctx.aof_mut().aof_wait_rewrite = true;
            // Register owned state only: this test must not fork or reap a child.
            worker
                .ctx
                .persistence_mut()
                .begin_aof_rewrite_child(i32::MAX, 0)
                .unwrap();
        }
        InitialRewrite::Scheduled => {
            worker
                .ctx
                .persistence_mut()
                .begin_diskless_child(i32::MAX)
                .unwrap();
            let result = crate::framework::capabilities::module::execute_module_thread_call(
                &mut worker.ctx,
                transition_call(&[b"CONFIG", b"SET", b"appendonly", b"yes"]),
            );
            assert_eq!(result.call_errno, 0);
            assert!(matches!(
                result.reply,
                Some(
                    crate::engine::extension::module::thread_bridge::ModuleReplyData::Value {
                        value: Value::SimpleString(_),
                        ..
                    }
                )
            ));
            assert!(worker.ctx.persistence().aof_rewrite_scheduled());
            assert!(worker.ctx.persistence().aof_rewrite_child_pid().is_none());
        }
    }
    assert!(worker.ctx.config().persistence.aof_enabled);
    assert!(worker.ctx.aof().aof_wait_rewrite);
    assert!(worker.ctx.aof().journal.is_none());
}

fn transition_set_effects(worker: &mut ServerWorkerState, key: &[u8]) -> Vec<ModuleCallEffects> {
    let scope = ModuleCallEffectScope::enter();
    let outcome = crate::framework::capabilities::module::execute_module_thread_call(
        &mut worker.ctx,
        transition_call(&[b"SET", key, b"value"]),
    );
    assert_eq!(outcome.call_errno, 0);
    assert!(matches!(
        outcome.reply,
        Some(
            crate::engine::extension::module::thread_bridge::ModuleReplyData::Value {
                value: Value::SimpleString(_),
                ..
            }
        )
    ));
    let effects = scope.finish();
    assert!(!effects.is_empty());
    effects
}

fn transition_expected_increment(key: &[u8]) -> Vec<u8> {
    use crate::engine::distributed::replication::protocol::encode_command;
    let mut frames = encode_command(&[b"SELECT", b"0"]);
    frames.extend(encode_command(&[b"SET", key, b"value"]));
    frames
}

fn run_transition_lease(
    worker: &mut ServerWorkerState,
    calls: Vec<crate::engine::extension::module::thread_bridge::OwnedModuleCall>,
) -> (Vec<libc::c_int>, Result<(), libc::c_int>) {
    use crate::engine::extension::module::thread_bridge::{ModuleThreadRequest, RpcReply};
    let bridge = worker.ctx.module_registry().thread_bridge.clone().unwrap();
    bridge.take_completions();
    assert!(bridge.prepare_sleep());
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let native = std::thread::spawn(move || {
        bridge.try_acquire().unwrap();
        ready_tx.send(()).unwrap();
        let mut results = Vec::new();
        for request in calls {
            let reply = RpcReply::default();
            let result = bridge
                .current_lease()
                .and_then(|lease| {
                    bridge.submit(ModuleThreadRequest::Call {
                        lease,
                        request,
                        reply: reply.clone(),
                    })
                })
                .and_then(|()| reply.wait())
                .map_or_else(|errno| errno, |outcome| outcome.call_errno);
            results.push(result);
        }
        let released = bridge.release();
        (results, released)
    });
    ready_rx.recv().unwrap();
    worker.service_module_thread_leases();
    native.join().unwrap()
}

fn assert_transition_data(worker: &ServerWorkerState, rewrite: InitialRewrite, key: &[u8]) {
    assert!(worker.ctx.db(0).unwrap().exists_no_expire(key));
    match rewrite {
        InitialRewrite::Active => assert_eq!(
            worker.ctx.aof().rewrite.buffer,
            transition_expected_increment(key)
        ),
        InitialRewrite::Scheduled => {
            // No sink exists yet. The future snapshot, not a hidden frame queue,
            // represents this dataset mutation.
            assert!(worker.ctx.aof().rewrite.buffer.is_empty());
            assert!(worker.ctx.persistence().aof_rewrite_scheduled());
        }
    }
    assert!(worker.ctx.aof().journal.is_none());
    assert_eq!(worker.ctx.aof().fsynced_repl_offset, Some(17));
    assert_eq!(worker.ctx.aof().fsynced_repl_offset_pending, 19);
}

#[test]
fn aof_transition_initial_rewrite_releases_empty_and_writing_leases() {
    use crate::foundation::io::persistence::AofFsync;
    for rewrite in [InitialRewrite::Active, InitialRewrite::Scheduled] {
        for fsync in [AofFsync::Always, AofFsync::EverySec, AofFsync::No] {
            for has_write in [false, true] {
                let (mut worker, _, shared) = fixture();
                install_initial_rewrite(&mut worker, rewrite);
                worker.ctx.config_mut().persistence.aof_fsync = fsync;
                worker.ctx.aof_mut().fsynced_repl_offset = Some(17);
                worker.ctx.aof_mut().fsynced_repl_offset_pending = 19;
                let calls = if has_write {
                    vec![transition_call(&[b"SET", b"lease-transition", b"value"])]
                } else {
                    Vec::new()
                };
                let (results, release) = run_transition_lease(&mut worker, calls);
                assert_eq!(release, Ok(()), "{rewrite:?}, {fsync:?}, write={has_write}");
                assert!(results.iter().all(|errno| *errno == 0));
                assert!(!worker.ctx.is_shutdown_requested());
                assert!(!shared.bridge.is_closing());
                assert!(shared.bridge.resume_main());
                assert_eq!(worker.ctx.aof().fsynced_repl_offset, Some(17));
                assert_eq!(worker.ctx.aof().fsynced_repl_offset_pending, 19);
                if has_write {
                    assert_transition_data(&worker, rewrite, b"lease-transition");
                } else {
                    assert!(worker.ctx.aof().rewrite.buffer.is_empty());
                }
                worker.ctx.persistence_mut().abort_child();
            }
        }
    }
}

#[test]
fn aof_transition_initial_rewrite_completes_reply_and_timeout_effects() {
    for rewrite in [InitialRewrite::Active, InitialRewrite::Scheduled] {
        for is_timeout in [false, true] {
            for has_write in [false, true] {
                let (mut worker, token, shared) = fixture();
                install_initial_rewrite(&mut worker, rewrite);
                worker.ctx.aof_mut().fsynced_repl_offset = Some(17);
                worker.ctx.aof_mut().fsynced_repl_offset_pending = 19;
                if has_write {
                    let effects = transition_set_effects(&mut worker, b"callback-transition");
                    CALLBACK_TRACE.with(|trace| trace.borrow_mut().effects = effects);
                }
                lock(&shared.lifecycle).stage = ModuleBlockStage::Armed;
                if is_timeout {
                    worker
                        .ctx
                        .module_registry_mut()
                        .native_blocks
                        .get_mut(&shared.id)
                        .unwrap()
                        .callbacks
                        .timeout = Some(reply_callback);
                    // No forced reply: actually invoke the timeout callback.
                    assert!(worker.timeout_native_module_client(token, None));
                    assert_eq!(
                        lock(&shared.lifecycle).stage,
                        ModuleBlockStage::DetachedTimeout
                    );
                } else {
                    shared
                        .with_replies(|builder| builder.add_integer(7))
                        .unwrap();
                    shared.accept_unblock(std::ptr::null_mut()).unwrap();
                    worker.service_native_module_completions();
                    assert_eq!(
                        worker.connection_registry[token].output_buffer_bytes(),
                        b":7\r\n".len()
                    );
                }
                assert!(
                    !worker.ctx.is_shutdown_requested(),
                    "{rewrite:?}, timeout={is_timeout}"
                );
                assert!(!shared.bridge.is_closing());
                assert!(worker.connection_registry[token].blocked.is_none());
                CALLBACK_TRACE.with(|trace| assert_eq!(trace.borrow().replies, 1));
                assert_eq!(worker.ctx.aof().fsynced_repl_offset, Some(17));
                assert_eq!(worker.ctx.aof().fsynced_repl_offset_pending, 19);
                if has_write {
                    assert_transition_data(&worker, rewrite, b"callback-transition");
                }
                worker.ctx.persistence_mut().abort_child();
            }
        }
    }
}

#[test]
fn aof_transition_pure_barrier_preserves_waiters_and_unknown_sources() {
    for rewrite in [InitialRewrite::Active, InitialRewrite::Scheduled] {
        let (mut worker, token, _) = fixture();
        install_initial_rewrite(&mut worker, rewrite);
        worker.connection_registry[token]
            .blocked
            .as_mut()
            .unwrap()
            .cmd = BlockedCmd::Wait {
            required: 0,
            target_offset: crate::engine::distributed::replication::ReplicationOffset::ZERO,
        };
        worker.ctx.aof_mut().fsynced_repl_offset_pending = 19;
        let outcome = worker.flush_native_module_aof_barrier();
        assert_eq!(outcome, AofBarrierOutcome::PendingInitialRewrite);
        assert!(worker.connection_registry[token].blocked.is_some());
        assert_eq!(worker.connection_registry[token].output_buffer_bytes(), 0);
        assert_eq!(worker.ctx.aof().fsynced_repl_offset, None);
        assert_eq!(worker.ctx.aof().fsynced_repl_offset_pending, 19);
        worker.ctx.set_loading(true);
        assert_eq!(
            worker.flush_native_module_aof_barrier(),
            AofBarrierOutcome::Failed
        );
        worker.ctx.set_loading(false);
        worker.ctx.aof_mut().aof_wait_rewrite = false;
        assert_eq!(
            worker.flush_native_module_aof_barrier(),
            AofBarrierOutcome::Failed
        );
        assert!(worker.ctx.aof().journal.is_none());
        assert!(worker.connection_registry[token].blocked.is_some());
        assert_eq!(worker.connection_registry[token].output_buffer_bytes(), 0);
        assert_eq!(worker.ctx.aof().fsynced_repl_offset, None);
        assert_eq!(worker.ctx.aof().fsynced_repl_offset_pending, 19);
        worker.ctx.persistence_mut().abort_child();
    }
    for source in [
        "loading",
        "scheduled-without-initial",
        "initial-without-rewrite",
        "missing-sink",
    ] {
        let (mut worker, _, _) = fixture();
        let dir = std::env::temp_dir().join(format!(
            "aof-transition-unknown-{}-{}",
            std::process::id(),
            next_id().unwrap(),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        worker.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        worker.ctx.config_mut().persistence.aof_enabled = true;
        match source {
            "loading" => worker.ctx.set_loading(true),
            "scheduled-without-initial" => {
                worker.ctx.persistence_mut().set_aof_rewrite_scheduled(true)
            }
            "initial-without-rewrite" => worker.ctx.aof_mut().aof_wait_rewrite = true,
            _ => {}
        }
        worker.feed_aof(b"*1\r\n$4\r\nPING\r\n");
        let outcome = worker.flush_native_module_aof_barrier();
        assert_eq!(outcome, AofBarrierOutcome::Failed, "{source}");
        assert!(worker.ctx.aof().journal.is_none(), "{source}");
        assert!(worker.ctx.aof().rewrite.buffer.is_empty(), "{source}");
        assert!(!worker.ctx.is_shutdown_requested(), "{source}");
        assert_eq!(worker.ctx.aof().fsynced_repl_offset, None);
        drop(worker);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(target_os = "linux")]
fn install_full_aof(
    worker: &mut ServerWorkerState,
    fsync: crate::foundation::io::persistence::AofFsync,
) {
    use crate::foundation::io::persistence::{journal::AofJournal, AofAppender};
    worker.ctx.config_mut().persistence.aof_enabled = true;
    worker.ctx.config_mut().persistence.aof_fsync = fsync;
    worker.ctx.aof_mut().journal = Some(AofJournal::new(
        AofAppender::open_append_path(std::path::Path::new("/dev/full"), fsync).unwrap(),
    ));
    worker.ctx.aof_mut().fsynced_repl_offset = Some(0);
}

#[cfg(target_os = "linux")]
#[test]
fn aof_transition_full_device_lease_and_completion_policy_matrix() {
    use crate::foundation::io::persistence::AofFsync;
    for fsync in [AofFsync::Always, AofFsync::EverySec, AofFsync::No] {
        for has_no_fsync_gate in [false, true] {
            for entry in ["lease", "reply", "timeout"] {
                let (mut worker, token, shared) = fixture();
                install_full_aof(&mut worker, fsync);
                worker.ctx.config_mut().persistence.aof_no_fsync_on_rewrite = has_no_fsync_gate;
                if has_no_fsync_gate {
                    worker
                        .ctx
                        .persistence_mut()
                        .begin_diskless_child(i32::MAX)
                        .unwrap();
                }
                let is_fatal = fsync == AofFsync::Always;
                if entry == "lease" {
                    let (calls, release) = run_transition_lease(
                        &mut worker,
                        vec![transition_call(&[b"SET", b"full-device", b"value"])],
                    );
                    assert_eq!(calls, vec![0]);
                    assert_eq!(
                        release.is_err(),
                        is_fatal,
                        "{fsync:?}, gate={has_no_fsync_gate}"
                    );
                } else {
                    let effects = transition_set_effects(&mut worker, b"full-device");
                    CALLBACK_TRACE.with(|trace| trace.borrow_mut().effects = effects);
                    lock(&shared.lifecycle).stage = ModuleBlockStage::Armed;
                    if entry == "reply" {
                        shared
                            .with_replies(|builder| builder.add_integer(7))
                            .unwrap();
                        shared.accept_unblock(std::ptr::null_mut()).unwrap();
                        worker.service_native_module_completions();
                    } else {
                        worker
                            .ctx
                            .module_registry_mut()
                            .native_blocks
                            .get_mut(&shared.id)
                            .unwrap()
                            .callbacks
                            .timeout = Some(reply_callback);
                        assert!(worker.timeout_native_module_client(token, None));
                    }
                    assert_eq!(
                        worker.connection_registry[token].blocked.is_some(),
                        is_fatal
                    );
                    if entry == "reply" {
                        assert_eq!(
                            worker.connection_registry[token].output_buffer_bytes(),
                            if is_fatal { 0 } else { b":7\r\n".len() }
                        );
                    }
                }
                assert_eq!(
                    worker.ctx.is_shutdown_requested(),
                    is_fatal,
                    "{fsync:?}, {entry}"
                );
                assert_eq!(shared.bridge.is_closing(), is_fatal);
                assert_eq!(
                    worker.ctx.aof().last_write_status,
                    crate::engine::persistence::OperationStatus::Err
                );
                assert_eq!(worker.ctx.aof().fsynced_repl_offset, Some(0));
                assert_eq!(worker.ctx.aof().fsynced_repl_offset_pending, 0);
                assert_eq!(
                    worker.flush_native_module_aof_barrier(),
                    if is_fatal {
                        AofBarrierOutcome::Failed
                    } else {
                        AofBarrierOutcome::RetryPending
                    }
                );
                let appender = &mut worker.ctx.aof_mut().journal.as_mut().unwrap().appender;
                assert_eq!(appender.fsynced_reported_offset(), None);
                assert!(!appender.last_write_ok());
                let size = appender.current_size();
                // An empty buffer would return Ok without touching /dev/full.
                // Repeated failure proves the real write buffer was retained.
                assert!(appender.flush(false).is_err());
                assert_eq!(appender.current_size(), size);
                worker.ctx.persistence_mut().abort_child();
            }
        }
    }
}
