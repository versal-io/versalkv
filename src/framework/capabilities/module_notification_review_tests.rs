use super::*;
use crate::engine::extension::module::api::call::VM_ReplicateCommand;
use crate::engine::extension::module::api::call::{
    VM_CallWithReply, ValkeyModule_FreeCallReply, VALKEYMODULE_REPLY_ERROR,
    VALKEYMODULE_REPLY_INTEGER,
};
use crate::engine::extension::module::api::notifications::ValkeyModule_AddPostNotificationJob;
use crate::engine::extension::module::capability::ModuleRuntime;
use crate::engine::extension::module::notifications::ModulePostNotificationJob;
use crate::engine::extension::module::registry::{ModuleNotification, ModuleRef};
use crate::engine::extension::module::{ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_OK};
use crate::engine::extension::script::{PropagationOp, PropagationTargets};
use crate::foundation::io::protocol::parser::Parser;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::framework::CommandRegistry;
use bytes::Bytes;
use libc::{c_char, c_int, c_void};
use std::cell::{Cell, RefCell};
use std::ffi::CStr;

const REPLICATE: c_int = 1;

#[derive(Clone, Copy)]
enum CallbackMode {
    Forward,
    Reverse,
    Move,
    Script,
}

thread_local! {
    static MODE: Cell<CallbackMode> = const { Cell::new(CallbackMode::Forward) };
    static FLAGS: Cell<c_int> = const { Cell::new(REPLICATE) };
    static FAILURES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static OBSERVED: Cell<i64> = const { Cell::new(-1) };
    static JOB_RUNS: Cell<usize> = const { Cell::new(0) };
}

fn argv(parts: &[&[u8]]) -> Vec<*mut ValkeyModuleString> {
    parts
        .iter()
        .map(|part| Box::into_raw(ValkeyModuleString::from_bytes(part)))
        .collect()
}

fn call(ctx: *mut ValkeyModuleCtx, parts: &[&[u8]], flags: c_int) -> (c_int, i64) {
    let mut args = argv(parts);
    // SAFETY: tests pass a live synchronous context and own every argument;
    // the returned root and all argument references are released exactly once.
    unsafe {
        let reply = VM_CallWithReply(ctx, args.as_mut_ptr(), args.len() as c_int, flags);
        let result = if reply.is_null() {
            (-1, 0)
        } else {
            let result = ((*reply).reply_type, (*reply).int_val);
            ValkeyModule_FreeCallReply(reply);
            result
        };
        for arg in args {
            ValkeyModuleString::release(arg);
        }
        result
    }
}

fn write(ctx: *mut ValkeyModuleCtx, value: &[u8], flags: c_int) {
    let (kind, _) = call(ctx, &[b"SET", b"marker", value], flags);
    if kind < 0 || kind == VALKEYMODULE_REPLY_ERROR {
        FAILURES.with(|failures| failures.borrow_mut().push(format!("SET failed: {kind}")));
    }
}

fn replicate(ctx: *mut ValkeyModuleCtx, value: &[u8]) {
    let mut args = argv(&[b"SET", b"marker", value]);
    // SAFETY: the callback context and all owned argv entries remain live;
    // replication copies their bytes before each reference is released.
    unsafe {
        let result = VM_ReplicateCommand(ctx, args.as_mut_ptr(), args.len() as c_int);
        if result != VALKEYMODULE_OK {
            FAILURES.with(|failures| failures.borrow_mut().push("Replicate failed".into()));
        }
        for arg in args {
            ValkeyModuleString::release(arg);
        }
    }
}

fn interleave(ctx: *mut ValkeyModuleCtx) {
    match MODE.with(Cell::get) {
        CallbackMode::Forward => {
            write(ctx, b"one", FLAGS.with(Cell::get));
            write(ctx, b"two", 0);
            replicate(ctx, b"two");
        }
        CallbackMode::Reverse => {
            write(ctx, b"one", 0);
            replicate(ctx, b"one");
            write(ctx, b"two", FLAGS.with(Cell::get));
        }
        CallbackMode::Move => {
            write(ctx, b"moved", 0);
            replicate(ctx, b"moved");
        }
        CallbackMode::Script => write(ctx, b"job", REPLICATE),
    }
}

extern "C" fn subscriber(
    ctx: *mut ValkeyModuleCtx,
    _event_type: c_int,
    event: *const c_char,
    _key: *mut ValkeyModuleString,
) -> c_int {
    // SAFETY: notification dispatch supplies a live NUL-terminated event name.
    let event = unsafe { CStr::from_ptr(event) }.to_bytes();
    if matches!(MODE.with(Cell::get), CallbackMode::Script) {
        if event == b"phdel" {
            let (kind, exists) = call(ctx, &[b"EXISTS", b"ph-notify"], 0);
            OBSERVED.with(|slot| slot.set(exists));
            if kind != VALKEYMODULE_REPLY_INTEGER {
                FAILURES.with(|failures| failures.borrow_mut().push("EXISTS failed".into()));
            }
            let (kind, _) = call(ctx, &[b"INCR", b"notification-count"], REPLICATE);
            if kind != VALKEYMODULE_REPLY_INTEGER {
                FAILURES.with(|failures| failures.borrow_mut().push("INCR failed".into()));
            }
            write(ctx, b"callback", REPLICATE);
        } else if event == b"del" {
            // SAFETY: the live subscriber context owns the admission state; this job
            // retains no private data and is run only at the outer execution boundary.
            let result = unsafe {
                ValkeyModule_AddPostNotificationJob(ctx, Some(job), std::ptr::null_mut(), None)
            };
            if result != VALKEYMODULE_OK {
                FAILURES.with(|failures| failures.borrow_mut().push("AddPostJob failed".into()));
            }
        }
        return VALKEYMODULE_OK;
    }
    let is_matching = match MODE.with(Cell::get) {
        CallbackMode::Move => event == b"move_to",
        _ => event == b"del",
    };
    if is_matching {
        interleave(ctx);
    }
    VALKEYMODULE_OK
}

extern "C" fn job(ctx: *mut ValkeyModuleCtx, _data: *mut c_void) {
    JOB_RUNS.with(|slot| slot.set(slot.get() + 1));
    interleave(ctx);
}

fn fixture(mode: CallbackMode, flags: c_int) -> ServerContext {
    MODE.with(|slot| slot.set(mode));
    FLAGS.with(|slot| slot.set(flags));
    FAILURES.with(|failures| failures.borrow_mut().clear());
    OBSERVED.with(|slot| slot.set(-1));
    JOB_RUNS.with(|slot| slot.set(0));
    let mut ctx = ServerContext::new(2);
    let mut commands = CommandRegistry::new();
    crate::framework::commands::register_all(&mut commands);
    ctx.set_command_registry_snapshot(&commands);
    let module = ModuleRef::new("notification-review", std::ptr::null_mut(), 1, true);
    ctx.module_registry_mut().register(module).unwrap();
    ctx.db_mut(0)
        .unwrap()
        .set(b"source".to_vec(), Robj::string("value"));
    ctx
}

fn subscribe(ctx: &mut ServerContext) {
    ctx.module_registry_mut()
        .lookup_mut("notification-review")
        .unwrap()
        .notifications
        .push(ModuleNotification {
            types: (1 << 2)
                | if matches!(MODE.with(Cell::get), CallbackMode::Script) {
                    1 << 15
                } else {
                    0
                },
            callback: subscriber,
        });
}

fn execute_source(
    ctx: &mut ServerContext,
    conn: &mut ModuleCallConn,
    parts: &[&[u8]],
    targets: PropagationTargets,
) -> ModuleCallEffects {
    let args = crate::engine::command::CommandArgs::from_parts(
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part))
            .collect(),
    );
    let spec = ModuleRuntime::lookup_command(ctx, parts[0]).unwrap();
    let reply = ModuleRuntime::execute_command(ctx, parts[0], conn, &args);
    assert_eq!(reply, Value::Integer(1));
    conn.take_call_effects(&spec, &args, targets, true)
}

fn frames(ops: &[PropagationOp]) -> Vec<(u32, PropagationTargets, Vec<Bytes>)> {
    let mut output = Vec::new();
    for op in ops {
        let mut parser = Parser::new();
        parser.feed(&op.bytes);
        while let Some(parts) = parser.try_parse().unwrap() {
            output.push((op.db_id, op.targets, parts));
        }
        assert!(!parser.has_inflight_request());
    }
    output
}

fn replay(ops: &[PropagationOp], is_aof: bool) -> ServerContext {
    let mut ctx = ServerContext::new(2);
    let mut commands = CommandRegistry::new();
    crate::framework::commands::register_all(&mut commands);
    ctx.set_command_registry_snapshot(&commands);
    ctx.db_mut(0)
        .unwrap()
        .set(b"source".to_vec(), Robj::string("value"));
    for (db, targets, parts) in frames(ops) {
        let is_selected = if is_aof {
            targets.includes_aof()
        } else {
            targets.includes_replication()
        };
        if is_selected {
            let mut conn = ModuleCallConn::new(db);
            let args = crate::engine::command::CommandArgs::from_parts(parts);
            let reply = ModuleRuntime::execute_command(&mut ctx, &args.name, &mut conn, &args);
            assert!(!matches!(reply, Value::Error(_)));
        }
    }
    ctx
}

fn read(ctx: &mut ServerContext, db: u32) -> Value {
    let args = crate::engine::command::CommandArgs::from_parts(vec![
        Bytes::from_static(b"GET"),
        Bytes::from_static(b"marker"),
    ]);
    ModuleRuntime::execute_command(ctx, b"GET", &mut ModuleCallConn::new(db), &args)
}

fn assert_interleaving(
    effects: ModuleCallEffects,
    mode: CallbackMode,
    targets: PropagationTargets,
    source_targets: PropagationTargets,
) {
    assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    let decoded = frames(&effects.propagation);
    assert_eq!(decoded.len(), 3);
    assert_eq!(
        decoded[0].2,
        vec![Bytes::from_static(b"DEL"), Bytes::from_static(b"source")]
    );
    assert_eq!(decoded[0].1, source_targets);
    for (index, value) in [b"one".as_slice(), b"two"].into_iter().enumerate() {
        assert_eq!(decoded[index + 1].0, 0);
        assert_eq!(
            decoded[index + 1].2,
            vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"marker"),
                Bytes::copy_from_slice(value)
            ]
        );
    }
    let expected_targets = match mode {
        CallbackMode::Forward => [targets, PropagationTargets::Both],
        _ => [PropagationTargets::Both, targets],
    };
    assert_eq!([decoded[1].1, decoded[2].1], expected_targets);
    for is_aof in [false, true] {
        let mut replica = replay(&effects.propagation, is_aof);
        let has_second = if is_aof {
            decoded[2].1.includes_aof()
        } else {
            decoded[2].1.includes_replication()
        };
        let expected = if has_second {
            b"two".as_slice()
        } else {
            b"one"
        };
        assert_eq!(
            read(&mut replica, 0),
            Value::Bulk(Bytes::copy_from_slice(expected))
        );
        assert_eq!(read(&mut replica, 1), Value::Null);
        let has_source_deletion = if is_aof {
            source_targets.includes_aof()
        } else {
            source_targets.includes_replication()
        };
        assert_eq!(
            replica.db_mut(0).unwrap().exists_no_expire(b"source"),
            !has_source_deletion
        );
    }
}

#[test]
fn notification_review_callbacks_keep_interleaved_propagation_order() {
    for mode in [CallbackMode::Forward, CallbackMode::Reverse] {
        for (flags, targets) in [
            (REPLICATE, PropagationTargets::Both),
            (REPLICATE | (1 << 1), PropagationTargets::Replication),
            (REPLICATE | (1 << 2), PropagationTargets::Aof),
        ] {
            let mut ctx = fixture(mode, flags);
            subscribe(&mut ctx);
            let mut conn = ModuleCallConn::new(0);
            let mut effects = execute_source(
                &mut ctx,
                &mut conn,
                &[b"DEL", b"source"],
                PropagationTargets::Both,
            );
            effects
                .append(dispatch_module_keyspace_notifications(&mut conn, &mut ctx))
                .unwrap();
            assert_eq!(read(&mut ctx, 0), Value::Bulk(Bytes::from_static(b"two")));
            assert_interleaving(effects, mode, targets, PropagationTargets::Both);
        }
    }
}

#[test]
fn notification_review_jobs_keep_interleaved_propagation_order() {
    for mode in [CallbackMode::Forward, CallbackMode::Reverse] {
        for source_targets in [
            PropagationTargets::Both,
            PropagationTargets::Aof,
            PropagationTargets::Replication,
        ] {
            let mut ctx = fixture(mode, REPLICATE);
            let mut conn = ModuleCallConn::new(0);
            let mut effects =
                execute_source(&mut ctx, &mut conn, &[b"DEL", b"source"], source_targets);
            ctx.module_registry_mut()
                .notifications
                .enqueue(ModulePostNotificationJob {
                    module_name: "notification-review".into(),
                    db_idx: 0,
                    callback: job,
                    data: std::ptr::null_mut(),
                    free: None,
                });
            let jobs = finish_module_post_notification_jobs(&mut ctx);
            assert_eq!(jobs.len(), 1);
            for job in jobs {
                effects.append(job).unwrap();
            }
            assert!(finish_module_post_notification_jobs(&mut ctx).is_empty());
            assert_interleaving(effects, mode, PropagationTargets::Both, source_targets);
        }
    }
}

#[test]
fn notification_review_unscoped_replication_keeps_operation_order_and_db() {
    for mode in [CallbackMode::Forward, CallbackMode::Reverse] {
        let mut ctx = fixture(mode, REPLICATE);
        let mut conn = ModuleCallConn::new(0);
        let mut invocation = ModuleInvocationScope::new(
            &mut ctx,
            "notification-review",
            "review.call",
            1,
            2,
            None,
            Vec::new(),
        );
        {
            let _conn = ModuleCommandConnScope::enter(&mut conn);
            let _runtime = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
            interleave(invocation.module_ctx());
        }
        drop(invocation);
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        assert_eq!(conn.selected_db(), 0);
        let decoded = frames(&conn.take_recorded_effects().propagation);
        assert_eq!(decoded.len(), 2);
        assert_eq!([decoded[0].0, decoded[1].0], [1, 1]);
        assert_eq!(decoded[0].2[2], Bytes::from_static(b"one"));
        assert_eq!(decoded[1].2[2], Bytes::from_static(b"two"));
    }
}

#[test]
fn notification_review_move_callback_replicates_in_the_event_db() {
    let mut ctx = fixture(CallbackMode::Move, REPLICATE);
    subscribe(&mut ctx);
    let mut conn = ModuleCallConn::new(0);
    let mut effects = execute_source(
        &mut ctx,
        &mut conn,
        &[b"MOVE", b"source", b"1"],
        PropagationTargets::Both,
    );
    effects
        .append(dispatch_module_keyspace_notifications(&mut conn, &mut ctx))
        .unwrap();
    assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    assert_eq!(conn.selected_db(), 0);
    assert_eq!(read(&mut ctx, 0), Value::Null);
    assert_eq!(read(&mut ctx, 1), Value::Bulk(Bytes::from_static(b"moved")));
    let decoded = frames(&effects.propagation);
    assert_eq!(decoded.len(), 2);
    assert_eq!([decoded[0].0, decoded[1].0], [0, 1]);
    for is_aof in [false, true] {
        let mut replica = replay(&effects.propagation, is_aof);
        assert_eq!(read(&mut replica, 0), Value::Null);
        assert_eq!(
            read(&mut replica, 1),
            Value::Bulk(Bytes::from_static(b"moved"))
        );
    }
}

const INLINE_SCRIPT: &[u8] = b"redis.call('PHDEL','ph-notify','p','f'); \
    local count=redis.call('GET','notification-count'); \
    redis.call('PHSET','ph-notify','p','FIELDS',1,'f','later'); \
    return tonumber(count or '0')";

#[test]
fn notification_review_inline_scripts_notify_before_the_next_command_and_defer_jobs() {
    for is_module_call in [true, false] {
        for flags in [REPLICATE, REPLICATE | (1 << 1), REPLICATE | (1 << 2)] {
            let mut ctx = fixture(CallbackMode::Script, REPLICATE);
            let mut conn = ModuleCallConn::new(0);
            let seed = crate::engine::command::CommandArgs::from_parts(
                [
                    b"PHSET".as_slice(),
                    b"ph-notify",
                    b"p",
                    b"FIELDS",
                    b"1",
                    b"f",
                    b"seed",
                ]
                .into_iter()
                .map(Bytes::copy_from_slice)
                .collect(),
            );
            assert_eq!(
                ModuleRuntime::execute_command(&mut ctx, b"PHSET", &mut conn, &seed),
                Value::ok()
            );
            conn.take_recorded_effects();
            subscribe(&mut ctx);
            let parent = ModuleCallEffectScope::enter();
            let mut effects = if is_module_call {
                let mut invocation = ModuleInvocationScope::new(
                    &mut ctx,
                    "notification-review",
                    "review.eval",
                    0,
                    2,
                    None,
                    Vec::new(),
                );
                {
                    let _conn = ModuleCommandConnScope::enter(&mut conn);
                    let _runtime = ActiveModuleRuntimeScope::enter(invocation.runtime_handle());
                    let _registry = ActiveModuleRegistryScope::enter(invocation.registry());
                    assert_eq!(
                        call(
                            invocation.module_ctx(),
                            &[b"EVAL", INLINE_SCRIPT, b"0"],
                            flags
                        ),
                        (VALKEYMODULE_REPLY_INTEGER, 1)
                    );
                }
                drop(invocation);
                let collected = parent.finish();
                let mut effects = ModuleCallEffects::default();
                for effect in collected {
                    effects.append(effect).unwrap();
                }
                effects
            } else {
                let args = crate::engine::command::CommandArgs::from_parts(
                    [b"EVAL".as_slice(), INLINE_SCRIPT, b"0"]
                        .into_iter()
                        .map(Bytes::copy_from_slice)
                        .collect(),
                );
                assert_eq!(
                    crate::framework::commands::scripting::eval::eval(&mut conn, &args, &mut ctx),
                    Value::Integer(1)
                );
                assert!(parent.finish().is_empty());
                conn.take_recorded_effects()
            };
            assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
            assert_eq!(OBSERVED.with(Cell::get), 0);
            assert_eq!(JOB_RUNS.with(Cell::get), 0);
            assert!(conn.take_module_keyspace_events().is_empty());
            assert_eq!(conn.selected_db(), 0);
            assert!(ctx.module_registry().pending_effects.is_empty());
            assert_eq!(
                read(&mut ctx, 0),
                Value::Bulk(Bytes::from_static(b"callback"))
            );
            let decoded = frames(&effects.propagation);
            assert_eq!(
                decoded
                    .iter()
                    .map(|(_, _, parts)| parts[0].as_ref())
                    .collect::<Vec<_>>(),
                vec![b"PHDEL".as_slice(), b"INCR", b"SET", b"PHSET"]
            );
            let expected_targets = if !is_module_call || flags == REPLICATE {
                PropagationTargets::Both
            } else if flags & (1 << 1) != 0 {
                PropagationTargets::Replication
            } else {
                PropagationTargets::Aof
            };
            assert!(decoded
                .iter()
                .all(|(_, targets, _)| *targets == expected_targets));
            for job in finish_module_post_notification_jobs(&mut ctx) {
                effects.append(job).unwrap();
            }
            assert_eq!(JOB_RUNS.with(Cell::get), 1);
            assert!(finish_module_post_notification_jobs(&mut ctx).is_empty());
            let decoded = frames(&effects.propagation);
            assert_eq!(decoded.len(), 5);
            assert_eq!(
                decoded[4].2,
                vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"marker"),
                    Bytes::from_static(b"job")
                ]
            );
            assert_eq!(decoded[4].1, PropagationTargets::Both);
            for is_aof in [false, true] {
                let mut replica = replay(&effects.propagation, is_aof);
                assert_eq!(
                    read(&mut replica, 0),
                    Value::Bulk(Bytes::from_static(b"job"))
                );
            }
        }
    }
}
