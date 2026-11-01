use super::*;
use crate::engine::command::context::CommandConn;
use crate::engine::command::CommandArgs;
use crate::engine::extension::module::api::call::call_conn::ModuleCallConn;
use crate::engine::extension::module::api::call::pending::PendingModuleCall;
use crate::engine::extension::module::capability::ModuleRuntime;
use crate::framework::capabilities::connection::Connection;

fn args(parts: &[&[u8]]) -> CommandArgs {
    CommandArgs::from_parts(
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part))
            .collect(),
    )
}

fn backend() -> ServerWorkerState {
    let mut backend = ServerWorkerState::new(1).unwrap();
    crate::framework::commands::register_all(&mut backend.registry);
    backend.ctx.set_command_registry_snapshot(&backend.registry);
    backend
}

fn add_module_waiter(backend: &mut ServerWorkerState, name: &[u8]) -> usize {
    let args = args(&[name, b"fifo", b"0"]);
    let mut conn = ModuleCallConn::new(0);
    conn.set_deny_blocking(false);
    assert!(matches!(
        backend.ctx.execute_command(&args.name, &mut conn, &args),
        Value::NullArray | Value::Null
    ));
    assert!(conn.blocked().is_some());
    let command = backend.registry.lookup(name).unwrap().descriptor();
    let call = PendingModuleCall::from_test_command(conn, command, &args);
    backend.ctx.retain_pending_module_call(call).unwrap()
}

fn add_ordinary_waiter(backend: &mut ServerWorkerState) -> Token {
    let mut conn = Connection::new_fake();
    conn.set_token_for_test(Token(42));
    let token = Token(backend.connection_registry.insert(conn));
    let args = args(&[b"BRPOP", b"fifo", b"0"]);
    crate::framework::commands::list::brpop::brpop(
        &mut backend.connection_registry[token],
        &args,
        &mut backend.ctx,
    );
    assert!(backend.connection_registry[token].blocked.is_some());
    let session = backend.connection_registry[token].session_id();
    backend
        .ctx
        .blocking_registry_mut()
        .register_client(0, &[Bytes::from_static(b"fifo")], session);
    token
}

fn add_identity_ordinary_client(backend: &mut ServerWorkerState) -> Token {
    let conn = Connection::new(Token(42), ([127, 0, 0, 1], 0).into(), false);
    let token = conn.token();
    backend.connection_registry.register(conn).unwrap();
    token
}

fn block_identity_ordinary_client(backend: &mut ServerWorkerState, token: Token, parts: &[&[u8]]) {
    let args = args(parts);
    backend
        .ctx
        .execute_command(&args.name, &mut backend.connection_registry[token], &args);
    let conn = &backend.connection_registry[token];
    let blocked = conn.blocked.as_ref().unwrap();
    let session = conn.session_id();
    let keys = blocked.keys.clone();
    if keys.is_empty() {
        backend
            .ctx
            .blocking_registry_mut()
            .register_keyless_client(session);
    } else {
        backend
            .ctx
            .blocking_registry_mut()
            .register_client(0, &keys, session);
    }
}

fn add_identity_module_waiter(backend: &mut ServerWorkerState, parts: &[&[u8]]) -> usize {
    let args = args(parts);
    let mut conn = ModuleCallConn::new(0);
    conn.use_temporary_identity();
    conn.set_deny_blocking(false);
    backend.ctx.execute_command(&args.name, &mut conn, &args);
    assert!(conn.blocked().is_some());
    let command = backend.registry.lookup(&args.name).unwrap().descriptor();
    let call = PendingModuleCall::from_test_command(conn, command, &args);
    backend.ctx.retain_pending_module_call(call).unwrap()
}

#[test]
fn module_identity_pending_abort_preserves_ordinary_registration() {
    for parts in [
        &[b"BLPOP".as_slice(), b"fifo", b"0"][..],
        &[b"WAIT".as_slice(), b"1", b"0"][..],
    ] {
        crate::foundation::memory::client_id::test_support::with_next_client_id(0x1000001, || {
            let mut backend = backend();
            let ordinary = add_identity_ordinary_client(&mut backend);
            block_identity_ordinary_client(&mut backend, ordinary, parts);
            let session = backend.connection_registry[ordinary].session_id();
            let pending = add_identity_module_waiter(&mut backend, parts);
            assert!(backend.ctx.abort_pending_module_call(pending));
            assert!(
                backend.ctx.blocking_registry().is_client_blocked(&session),
                "aborting a module call must not unregister the ordinary client"
            );
            assert_eq!(backend.ctx.blocking_registry().count(), 1);
            assert!(!backend.ctx.abort_pending_module_call(pending));
            assert!(backend.connection_registry[ordinary].blocked.is_some());
        });
    }
}

#[test]
fn module_identity_ordinary_unregister_preserves_pending_registration() {
    crate::foundation::memory::client_id::test_support::with_next_client_id(0x1000001, || {
        let mut backend = backend();
        let ordinary = add_identity_ordinary_client(&mut backend);
        let parts = &[b"BLPOP".as_slice(), b"fifo", b"0"];
        block_identity_ordinary_client(&mut backend, ordinary, parts);
        let session = backend.connection_registry[ordinary].session_id();
        let pending = add_identity_module_waiter(&mut backend, parts);
        backend.ctx.blocking_registry_mut().unregister_client(
            0,
            &[Bytes::from_static(b"fifo")],
            &session,
        );
        assert_eq!(
            backend.ctx.blocking_registry().waiters_for_key(0, b"fifo"),
            vec![crate::engine::command::SessionId(pending as u64)]
        );
        assert_eq!(backend.ctx.blocking_registry().count(), 1);
        assert!(backend.ctx.abort_pending_module_call(pending));
        assert_eq!(backend.ctx.blocking_registry().count(), 0);
    });
}

#[test]
fn module_identity_pending_wakes_beside_unblocked_ordinary_client() {
    crate::foundation::memory::client_id::test_support::with_next_client_id(0x1000001, || {
        let mut backend = backend();
        let ordinary = add_identity_ordinary_client(&mut backend);
        let pending = add_identity_module_waiter(&mut backend, &[b"BLPOP", b"fifo", b"0"]);
        let mut writer = ModuleCallConn::new(0);
        let push = args(&[b"LPUSH", b"fifo", b"value"]);
        backend.ctx.execute_command(&push.name, &mut writer, &push);
        let mut effects = EffectQueue::default();
        backend.wakeup_blocked_on_keys_with_effect_queue(
            &[(0, Bytes::from_static(b"fifo"))],
            None,
            None,
            Some(&mut effects),
        );
        assert!(
            !backend
                .ctx
                .module_registry()
                .pending_calls
                .contains_key(&pending),
            "an unblocked ordinary client must not intercept a module wakeup"
        );
        assert!(backend.connection_registry[ordinary].blocked.is_none());
        assert_eq!(backend.ctx.blocking_registry().count(), 0);
        assert!(!backend.ctx.abort_pending_module_call(pending));
        let frame =
            crate::engine::distributed::replication::protocol::encode_command(&[b"LPOP", b"fifo"]);
        let select =
            crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"0"]);
        assert_eq!(
            effects.replication_only,
            vec![select.clone(), frame.clone()]
        );
        assert_eq!(effects.aof_only, vec![select, frame]);
    });
}

#[test]
fn module_identity_mixed_waiters_keep_fifo_at_previous_module_id_boundary() {
    for module_first in [false, true] {
        crate::foundation::memory::client_id::test_support::with_next_client_id(0x1000001, || {
            let mut backend = backend();
            let ordinary = add_identity_ordinary_client(&mut backend);
            let module = module_first
                .then(|| add_identity_module_waiter(&mut backend, &[b"BLPOP", b"fifo", b"0"]));
            block_identity_ordinary_client(&mut backend, ordinary, &[b"BRPOP", b"fifo", b"0"]);
            let module = module.unwrap_or_else(|| {
                add_identity_module_waiter(&mut backend, &[b"BLPOP", b"fifo", b"0"])
            });
            let ordinary_session = backend.connection_registry[ordinary].session_id();
            let module_session = crate::engine::command::SessionId(module as u64);
            let expected = if module_first {
                vec![module_session, ordinary_session]
            } else {
                vec![ordinary_session, module_session]
            };
            assert_eq!(
                backend.ctx.blocking_registry().waiters_for_key(0, b"fifo"),
                expected
            );
            let mut writer = ModuleCallConn::new(0);
            for value in [b"first".as_slice(), b"second"] {
                let push = args(&[b"LPUSH", b"fifo", value]);
                backend.ctx.execute_command(&push.name, &mut writer, &push);
                backend.wakeup_blocked_on_keys(&[(0, Bytes::from_static(b"fifo"))], None, None);
            }
            assert!(!backend
                .ctx
                .module_registry()
                .pending_calls
                .contains_key(&module));
            assert!(backend.connection_registry[ordinary].blocked.is_none());
            assert_eq!(backend.ctx.blocking_registry().count(), 0);
        });
    }
}

#[test]
fn module_identity_mixed_constructors_share_monotonic_ids_and_tracking_owner() {
    use crate::engine::extension::module::capability::ModuleClientTracking;

    for first in [1, 0x1000000, 0x1000001] {
        crate::foundation::memory::client_id::test_support::with_next_client_id(first, || {
            let ordinary = Connection::new(Token(42), ([127, 0, 0, 1], 0).into(), false);
            let owner = ordinary.client_id() as usize;
            let tracking = ModuleClientTracking {
                id: owner,
                is_tracking: true,
                is_broadcast: false,
                is_optin: false,
                is_optout: false,
                is_caching: false,
            };
            let mut tracked = ModuleCallConn::with_tracking(0, Some(tracking));
            let untracked = ModuleCallConn::new(0);
            let ids = [
                ordinary.client_id(),
                tracked.client_id(),
                untracked.client_id(),
            ];
            assert!(
                ids.windows(2).all(|pair| pair[0] < pair[1]),
                "ordinary and module constructors must share a monotonic identity source: {ids:?}"
            );
            assert_eq!(tracked.tracking_client_id(), owner);
            tracked.use_temporary_identity();
            assert!(tracked.client_id() > untracked.client_id());
            assert_eq!(tracked.tracking_client_id(), owner);
        });
    }
}

#[test]
fn module_and_ordinary_waiters_share_fifo_and_propagation_order() {
    for module_first in [true, false] {
        let mut backend = backend();
        let first_module = module_first.then(|| add_module_waiter(&mut backend, b"BLPOP"));
        let ordinary = add_ordinary_waiter(&mut backend);
        let module = first_module.unwrap_or_else(|| add_module_waiter(&mut backend, b"BLPOP"));
        let mut writer = ModuleCallConn::new(0);
        let push = args(&[b"LPUSH", b"fifo", b"value"]);
        backend.ctx.execute_command(&push.name, &mut writer, &push);
        let mut effects = EffectQueue::default();
        effects.replication_only.push(b"outer".to_vec());
        effects.aof_only.push(b"outer".to_vec());
        backend.wakeup_blocked_on_keys_with_effect_queue(
            &[(0, Bytes::from_static(b"fifo"))],
            None,
            None,
            Some(&mut effects),
        );
        assert_eq!(
            backend
                .ctx
                .module_registry()
                .pending_calls
                .contains_key(&module),
            !module_first
        );
        assert_eq!(
            backend.connection_registry[ordinary].blocked.is_some(),
            module_first
        );
        let pop = if module_first {
            b"LPOP".as_slice()
        } else {
            b"RPOP".as_slice()
        };
        let frame =
            crate::engine::distributed::replication::protocol::encode_command(&[pop, b"fifo"]);
        assert_eq!(effects.replication_only.last(), Some(&frame));
        assert_eq!(effects.aof_only.last(), Some(&frame));
        assert_eq!(effects.replication_only.first(), Some(&b"outer".to_vec()));
        assert!(backend.ctx.module_registry().pending_effects.is_empty());
    }
}

#[test]
fn multiple_module_waiters_keep_fifo_and_cancelled_waiters_release_registration() {
    let mut backend = backend();
    let cancelled = add_module_waiter(&mut backend, b"BRPOP");
    let first = add_module_waiter(&mut backend, b"BLPOP");
    let second = add_module_waiter(&mut backend, b"BRPOP");
    assert!(backend.ctx.abort_pending_module_call(cancelled));
    assert_eq!(backend.ctx.blocking_registry().count(), 2);
    let mut writer = ModuleCallConn::new(0);
    let push = args(&[b"LPUSH", b"fifo", b"value"]);
    backend.ctx.execute_command(&push.name, &mut writer, &push);
    let mut replication = vec![b"outer".to_vec()];
    let mut aof = vec![b"outer".to_vec()];
    backend.wakeup_blocked_on_keys(
        &[(0, Bytes::from_static(b"fifo"))],
        Some(&mut replication),
        Some(&mut aof),
    );
    assert!(!backend
        .ctx
        .module_registry()
        .pending_calls
        .contains_key(&first));
    assert!(backend
        .ctx
        .module_registry()
        .pending_calls
        .contains_key(&second));
    let frame =
        crate::engine::distributed::replication::protocol::encode_command(&[b"LPOP", b"fifo"]);
    assert_eq!(replication.last(), Some(&frame));
    assert_eq!(aof.last(), Some(&frame));
    assert!(backend.ctx.abort_pending_module_call(second));
    assert_eq!(backend.ctx.blocking_registry().count(), 0);
}
