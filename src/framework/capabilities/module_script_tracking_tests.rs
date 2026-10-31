use super::{ModuleCommandConnScope, ModuleInvocationScope};
use crate::engine::command::context::CommandConn;
use crate::engine::command::CommandArgs;
use crate::engine::extension::module::api::call::call_conn::ModuleCallConn;
use crate::engine::extension::module::api::call::{
    VM_CallWithReply, ValkeyModule_FreeCallReply, VALKEYMODULE_REPLY_STRING,
};
use crate::engine::extension::module::capability::{
    ActiveModuleRuntimeScope, ModuleClientTracking,
};
use crate::engine::extension::module::ValkeyModuleString;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::context::ServerContext;
use crate::framework::CommandRegistry;
use bytes::Bytes;

const SCRIPT: &[u8] = b"return redis.call('GET', KEYS[1])";
const MULTI_READ_SCRIPT: &[u8] =
    b"local first=redis.call('GET',KEYS[1]); return redis.call('GET',KEYS[2])";

fn fixture() -> (ServerContext, Connection) {
    let mut ctx = ServerContext::new(1);
    let mut registry = CommandRegistry::new();
    crate::framework::commands::register_all(&mut registry);
    ctx.set_command_registry_snapshot(&registry);
    ctx.db_mut(0)
        .unwrap()
        .set(b"read-key".to_vec(), Robj::string(b"value".as_slice()));
    let mut outer = Connection::new_fake();
    outer.set_token_for_test(Token(crate::foundation::memory::client_id::next_client_id()));
    (ctx, outer)
}

#[test]
fn module_scripts_track_actual_reads_for_the_external_client() {
    let cases = [
        ("enabled", true, false, false, false, false, true),
        ("disabled", false, false, false, false, false, false),
        ("broadcast", true, true, false, false, false, false),
        ("optin-unselected", true, false, true, false, false, false),
        ("optin-selected", true, false, true, false, true, true),
        ("optout-selected", true, false, false, true, false, true),
        ("optout-excluded", true, false, false, true, true, false),
    ];
    for flags in [0, 1 << 11] {
        for (name, enabled, broadcast, optin, optout, caching, should_track) in cases {
            let (mut ctx, mut outer) = fixture();
            outer.set_tracking(enabled);
            outer.set_tracking_bcast(broadcast);
            outer.set_tracking_optin(optin);
            outer.set_tracking_optout(optout);
            outer.set_tracking_caching(caching);
            let owner = outer.token().0;
            let argv = [b"EVAL".as_slice(), SCRIPT, b"2", b"read-key", b"unread-key"]
                .into_iter()
                .map(|value| Box::into_raw(ValkeyModuleString::from_bytes(value)))
                .collect();
            let mut invocation = ModuleInvocationScope::new(
                &mut ctx,
                "tracking-test",
                "tracking.eval",
                0,
                2,
                None,
                argv,
            );
            {
                let _conn_scope = ModuleCommandConnScope::enter(&mut outer);
                let _runtime_scope =
                    ActiveModuleRuntimeScope::enter(invocation.runtime_handle()).unwrap();
                // SAFETY: invocation owns the live context and argv throughout
                // the synchronous call, and the returned reply is freed once.
                unsafe {
                    let reply = VM_CallWithReply(
                        invocation.module_ctx(),
                        invocation.argv(),
                        invocation.argc(),
                        flags,
                    );
                    assert!(!reply.is_null(), "{name}, flags={flags}");
                    assert_eq!((*reply).reply_type, VALKEYMODULE_REPLY_STRING, "{name}");
                    assert_eq!(
                        std::slice::from_raw_parts(
                            (*reply).str_data.cast::<u8>(),
                            (*reply).str_len
                        ),
                        b"value",
                        "{name}, flags={flags}"
                    );
                    ValkeyModule_FreeCallReply(reply);
                }
            }
            drop(invocation);
            let expected = if should_track {
                vec![owner]
            } else {
                Vec::new()
            };
            assert_eq!(
                ctx.tracking_mut()
                    .take_clients_for_key(&Bytes::from_static(b"read-key")),
                expected,
                "{name}, flags={flags}"
            );
            assert!(
                ctx.tracking_mut()
                    .take_clients_for_key(&Bytes::from_static(b"unread-key"))
                    .is_empty(),
                "{name}, flags={flags}: declared but unread key"
            );
            assert!(ctx.tracking().keys.is_empty(), "{name}, flags={flags}");
            assert!(ctx.module_registry().pending_calls.is_empty());
            assert!(outer.blocked().is_none());
        }
    }
}

#[test]
fn ordinary_scripts_track_actual_reads_for_the_connection() {
    let (mut ctx, mut conn) = fixture();
    conn.set_tracking(true);
    let args = CommandArgs::from_parts(
        [b"EVAL".as_slice(), SCRIPT, b"2", b"read-key", b"unread-key"]
            .into_iter()
            .map(Bytes::copy_from_slice)
            .collect(),
    );
    assert_eq!(
        crate::framework::commands::scripting::eval::eval(&mut conn, &args, &mut ctx),
        Value::Bulk(Bytes::from_static(b"value"))
    );
    assert_eq!(
        ctx.tracking_mut()
            .take_clients_for_key(&Bytes::from_static(b"read-key")),
        vec![conn.token().0]
    );
    assert!(ctx.tracking().keys.is_empty());
}

fn run_module_script(
    ctx: &mut ServerContext,
    conn: &mut dyn CommandConn,
    command: &[u8],
    script: &[u8],
    flags: i32,
) {
    let argv = [command, script, b"2", b"read-key", b"second-key"]
        .into_iter()
        .map(|value| Box::into_raw(ValkeyModuleString::from_bytes(value)))
        .collect();
    let mut invocation =
        ModuleInvocationScope::new(ctx, "tracking-test", "tracking.eval", 0, 2, None, argv);
    let _conn_scope = ModuleCommandConnScope::enter(conn);
    let _runtime_scope = ActiveModuleRuntimeScope::enter(invocation.runtime_handle()).unwrap();
    // SAFETY: the invocation owns its live context and arguments for the
    // synchronous call, and the returned reply is freed exactly once.
    unsafe {
        let reply = VM_CallWithReply(
            invocation.module_ctx(),
            invocation.argv(),
            invocation.argc(),
            flags,
        );
        assert!(!reply.is_null(), "flags={flags}");
        assert_eq!((*reply).reply_type, VALKEYMODULE_REPLY_STRING);
        assert_eq!(
            std::slice::from_raw_parts((*reply).str_data.cast::<u8>(), (*reply).str_len),
            b"value"
        );
        ValkeyModule_FreeCallReply(reply);
    }
}

#[test]
fn nested_module_scripts_preserve_the_external_tracking_owner() {
    for flags in [0, 1 << 11] {
        let (mut ctx, mut outer) = fixture();
        outer.set_tracking(true);
        let owner = outer.token().0;
        let mut module_conn = ModuleCallConn::with_tracking(
            0,
            Some(ModuleClientTracking {
                id: owner,
                is_tracking: true,
                is_broadcast: false,
                is_optin: false,
                is_optout: false,
                is_caching: false,
            }),
        );
        assert_ne!(module_conn.token().0, owner);
        run_module_script(&mut ctx, &mut module_conn, b"EVAL", SCRIPT, flags);
        assert_eq!(
            ctx.tracking_mut()
                .take_clients_for_key(&Bytes::from_static(b"read-key")),
            vec![owner],
            "flags={flags}"
        );
        assert!(ctx.tracking().keys.is_empty());
        assert!(ctx.module_registry().pending_calls.is_empty());
        assert!(module_conn.blocked().is_none());
    }
}

fn check_module_multiple_read_selection(is_optin: bool) {
    for flags in [0, 1 << 11] {
        for (caching, should_track) in [(true, is_optin), (false, !is_optin)] {
            let (mut ctx, mut outer) = fixture();
            ctx.db_mut(0)
                .unwrap()
                .set(b"second-key".to_vec(), Robj::string(b"value".as_slice()));
            outer.set_tracking(true);
            outer.set_tracking_optin(is_optin);
            outer.set_tracking_optout(!is_optin);
            outer.set_tracking_caching(caching);
            let owner = outer.token().0;
            run_module_script(&mut ctx, &mut outer, b"EVAL", MULTI_READ_SCRIPT, flags);
            for key in [b"read-key".as_slice(), b"second-key"] {
                assert_eq!(
                    ctx.tracking_mut()
                        .take_clients_for_key(&Bytes::copy_from_slice(key)),
                    if should_track {
                        vec![owner]
                    } else {
                        Vec::new()
                    },
                    "is_optin={is_optin}, caching={caching}, flags={flags}, key={key:?}"
                );
            }
            assert!(ctx.tracking().keys.is_empty());
            assert!(ctx.module_registry().pending_calls.is_empty());
            assert!(outer.blocked().is_none());
        }
    }
}

#[test]
fn module_scripts_keep_optin_selection_for_all_reads() {
    check_module_multiple_read_selection(true);
}

#[test]
fn module_scripts_keep_optout_selection_for_all_reads() {
    check_module_multiple_read_selection(false);
}

#[test]
fn ordinary_scripts_keep_selection_for_all_reads() {
    for (is_optin, caching, should_track) in [
        (true, true, true),
        (true, false, false),
        (false, true, false),
        (false, false, true),
    ] {
        let (mut ctx, mut conn) = fixture();
        ctx.db_mut(0)
            .unwrap()
            .set(b"second-key".to_vec(), Robj::string(b"value".as_slice()));
        conn.set_tracking(true);
        conn.set_tracking_optin(is_optin);
        conn.set_tracking_optout(!is_optin);
        conn.set_tracking_caching(caching);
        let args = CommandArgs::from_parts(
            [
                b"EVAL".as_slice(),
                MULTI_READ_SCRIPT,
                b"2",
                b"read-key",
                b"second-key",
            ]
            .into_iter()
            .map(Bytes::copy_from_slice)
            .collect(),
        );
        assert_eq!(
            crate::framework::commands::scripting::eval::eval(&mut conn, &args, &mut ctx),
            Value::Bulk(Bytes::from_static(b"value"))
        );
        for key in [b"read-key".as_slice(), b"second-key"] {
            assert_eq!(
                ctx.tracking_mut()
                    .take_clients_for_key(&Bytes::copy_from_slice(key)),
                if should_track {
                    vec![conn.token().0]
                } else {
                    Vec::new()
                },
                "is_optin={is_optin}, caching={caching}, key={key:?}"
            );
        }
        assert!(ctx.tracking().keys.is_empty());
    }
}

#[test]
fn readonly_module_scripts_track_only_actual_reads() {
    for flags in [0, 1 << 11] {
        let (mut ctx, mut outer) = fixture();
        outer.set_tracking(true);
        run_module_script(&mut ctx, &mut outer, b"EVAL_RO", SCRIPT, flags);
        assert_eq!(
            ctx.tracking_mut()
                .take_clients_for_key(&Bytes::from_static(b"read-key")),
            vec![outer.token().0],
            "flags={flags}"
        );
        assert!(
            ctx.tracking().keys.is_empty(),
            "unread declaration, flags={flags}"
        );
    }
}
