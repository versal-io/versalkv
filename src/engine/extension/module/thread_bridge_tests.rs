use super::*;
use std::sync::mpsc;
use std::time::Instant;

const WAIT_LIMIT: Duration = Duration::from_secs(2);

struct CloseOnDrop(ModuleThreadBridgeHandle);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn context(bridge: &ModuleThreadBridgeHandle, module: &Arc<ModuleIdentity>) -> ModuleThreadContext {
    ModuleThreadContext::new(
        bridge.clone(),
        Some(module.clone()),
        0,
        2,
        "default".into(),
        None,
    )
    .unwrap()
}

fn owned_bridge() -> (CloseOnDrop, ModuleLease) {
    let bridge = ModuleThreadBridgeHandle::new().unwrap();
    let guard = CloseOnDrop(bridge);
    assert!(guard.0.prepare_sleep());
    assert_eq!(guard.0.try_acquire(), Ok(()));
    let lease = guard.0.current_lease().unwrap();
    assert_eq!(guard.0.take_lease(), Some(lease));
    (guard, lease)
}

fn wait_for_acquisition(bridge: &ModuleThreadBridgeHandle) {
    let deadline = Instant::now() + WAIT_LIMIT;
    let mut state = lock(&bridge.0.state);
    while state.acquisitions.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "Acquire did not enter its queue");
        state = bridge.0.changed.wait_timeout(state, remaining).unwrap().0;
    }
    assert_eq!(state.acquisitions.len(), 1);
}

fn wait_for_request(bridge: &ModuleThreadBridgeHandle, lease: ModuleLease) -> ModuleThreadRequest {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        if let Some(request) = bridge.wait_request(lease).unwrap() {
            return request;
        }
        assert!(Instant::now() < deadline, "owner request did not arrive");
    }
}

fn terminal_reply<T>(reply: RpcReply<T>) -> Result<T, libc::c_int> {
    assert!(
        !reply.is_waiting(),
        "response must be terminal before waiting"
    );
    reply.wait()
}

#[test]
fn bridge_contexts_share_only_the_calling_thread_and_bridge_lease() {
    let (guard, lease) = owned_bridge();
    let bridge = &guard.0;
    let module = ModuleIdentity::new("bridge-contexts".into(), ModuleOptions::default()).unwrap();
    let first = context(bridge, &module);
    let second = context(bridge, &module);
    let other = CloseOnDrop(ModuleThreadBridgeHandle::new().unwrap());
    let other_context = context(&other.0, &module);
    assert_ne!(first.snapshot.id, second.snapshot.id);
    assert_eq!(first.lease(), Ok(lease));
    assert_eq!(second.lease(), Ok(lease));
    assert_eq!(other_context.lease(), Err(libc::EPERM));
    assert_eq!(bridge.acquire(), Err(libc::EDEADLK));
    assert_eq!(bridge.try_acquire(), Err(libc::EBUSY));

    let copied_bridge = bridge.clone();
    let copied_module = module.clone();
    std::thread::spawn(move || {
        let thread_context = context(&copied_bridge, &copied_module);
        assert_eq!(thread_context.lease(), Err(libc::EPERM));
        let reply = RpcReply::default();
        assert_eq!(
            copied_bridge.submit(ModuleThreadRequest::SelectDb {
                lease,
                db_idx: 1,
                reply: reply.clone(),
            }),
            Err(libc::EPERM)
        );
        assert_eq!(terminal_reply(reply), Err(libc::EPERM));
    })
    .join()
    .unwrap();

    let server_bridge = bridge.clone();
    let server = std::thread::spawn(move || {
        let _close = CloseOnDrop(server_bridge.clone());
        let request = wait_for_request(&server_bridge, lease);
        assert_eq!(server_bridge.validate_request(&request), Ok(()));
        let ModuleThreadRequest::Release { reply, .. } = request else {
            panic!("expected Release from the second context");
        };
        server_bridge.finish_lease(lease);
        reply.complete(());
    });
    assert_eq!(second.bridge.release(), Ok(()));
    server.join().unwrap();
    assert_eq!(first.lease(), Err(libc::ESHUTDOWN));
    drop((first, second, other_context));
    assert_eq!(module.contexts.load(Ordering::Acquire), 0);
}

#[test]
fn bridge_owner_rpc_and_release_progress_while_another_acquire_waits() {
    std::thread::scope(|scope| {
        let (guard, lease) = owned_bridge();
        let bridge = &guard.0;
        let waiter_bridge = bridge.clone();
        let waiter = scope.spawn(move || {
            waiter_bridge.acquire()?;
            waiter_bridge.current_lease()
        });
        wait_for_acquisition(bridge);

        let first = RpcReply::default();
        assert_eq!(
            bridge.submit(ModuleThreadRequest::SelectDb {
                lease,
                db_idx: 1,
                reply: first.clone(),
            }),
            Ok(())
        );
        let taken = bridge.wait_request(lease).unwrap().unwrap();
        assert_eq!(bridge.validate_request(&taken), Ok(()));
        assert!(matches!(
            taken,
            ModuleThreadRequest::SelectDb { db_idx: 1, .. }
        ));

        let rejected = RpcReply::default();
        assert_eq!(
            bridge.submit(ModuleThreadRequest::SelectDb {
                lease,
                db_idx: 2,
                reply: rejected.clone(),
            }),
            Err(libc::EAGAIN)
        );
        assert_eq!(terminal_reply(rejected), Err(libc::EAGAIN));

        let release = RpcReply::default();
        assert_eq!(
            bridge.submit(ModuleThreadRequest::Release {
                lease,
                reply: release.clone(),
            }),
            Ok(())
        );
        let after_release = RpcReply::default();
        assert_eq!(
            bridge.submit(ModuleThreadRequest::SelectDb {
                lease,
                db_idx: 3,
                reply: after_release.clone(),
            }),
            Err(libc::EPERM)
        );
        assert_eq!(terminal_reply(after_release), Err(libc::EPERM));
        assert!(bridge.wait_request(lease).unwrap().is_none());
        assert!(release.is_waiting());

        first.complete(true);
        assert_eq!(terminal_reply(first), Ok(true));
        let request = bridge.wait_request(lease).unwrap().unwrap();
        assert!(matches!(request, ModuleThreadRequest::Release { .. }));
        bridge.finish_lease(lease);
        assert!(bridge.has_active_thread_work());
        assert_eq!(bridge.take_lease(), None);
        assert!(release.is_waiting());

        release.complete(());
        assert_eq!(terminal_reply(release), Ok(()));
        let next = bridge.take_lease().unwrap();
        assert_ne!(next.owner, lease.owner);
        assert_eq!(waiter.join().unwrap(), Ok(next));
        bridge.finish_lease(next);
        assert!(!bridge.has_active_thread_work());
    });
}

#[test]
fn bridge_try_acquire_returns_before_owner_acknowledgement() {
    let guard = CloseOnDrop(ModuleThreadBridgeHandle::new().unwrap());
    let bridge = &guard.0;
    assert_eq!(bridge.try_acquire(), Err(libc::EBUSY));
    assert!(bridge.prepare_sleep());

    std::thread::scope(|scope| {
        let state = lock(&bridge.0.state);
        let copied = bridge.clone();
        let (tx, rx) = mpsc::channel();
        let contender = scope.spawn(move || tx.send(copied.try_acquire()).unwrap());
        let result = rx.recv_timeout(WAIT_LIMIT);
        // Release the mutex before checking the deadline so failure cleanup can close.
        drop(state);
        if result.is_err() {
            bridge.close();
        }
        contender.join().unwrap();
        assert_eq!(result.unwrap(), Err(libc::EBUSY));
    });

    assert_eq!(bridge.try_acquire(), Ok(()));
    let reserved = bridge.current_lease().unwrap();
    assert!(!bridge.resume_main());
    assert_eq!(bridge.wait_request(reserved).err(), Some(libc::EPERM));
    assert_eq!(bridge.take_lease(), Some(reserved));
    assert!(!bridge.resume_main());
    assert_eq!(bridge.try_acquire(), Err(libc::EBUSY));
    bridge.finish_lease(reserved);
    assert!(bridge.resume_main());
}

fn assert_cancelled_once<T>(reply: &RpcReply<T>, late_value: T) {
    assert!(matches!(
        terminal_reply(reply.clone()),
        Err(libc::ESHUTDOWN)
    ));
    reply.complete(late_value);
    reply.cancel(libc::ECANCELED);
    assert!(matches!(terminal_reply(reply.clone()), Err(libc::EALREADY)));
}

#[test]
fn bridge_close_cancels_each_taken_response_without_late_revival() {
    for kind in 0..4 {
        let (guard, lease) = owned_bridge();
        let bridge = &guard.0;
        let module = ModuleIdentity::new("bridge-close".into(), ModuleOptions::default()).unwrap();
        let context = context(bridge, &module);
        let request = match kind {
            0 => ModuleThreadRequest::Call {
                lease,
                request: OwnedModuleCall {
                    context: context.snapshot.clone(),
                    argv: vec![Bytes::from_static(b"PING")],
                    format: ModuleCallFormat::from_raw(0),
                },
                reply: RpcReply::default(),
            },
            1 => ModuleThreadRequest::ContextFlags {
                lease,
                context: context.snapshot.clone(),
                reply: RpcReply::default(),
            },
            2 => ModuleThreadRequest::SelectDb {
                lease,
                db_idx: 0,
                reply: RpcReply::default(),
            },
            _ => ModuleThreadRequest::Release {
                lease,
                reply: RpcReply::default(),
            },
        };
        assert_eq!(bridge.submit(request), Ok(()));
        let taken = bridge.wait_request(lease).unwrap().unwrap();
        assert_eq!(bridge.validate_request(&taken), Ok(()));
        bridge.close();
        assert_eq!(bridge.validate_request(&taken), Err(libc::ESHUTDOWN));
        assert_eq!(context.lease(), Err(libc::ESHUTDOWN));
        match taken {
            ModuleThreadRequest::Call { reply, .. } => {
                assert_cancelled_once(&reply, ModuleCallOutcome::cancelled(0));
            }
            ModuleThreadRequest::ContextFlags { reply, .. } => assert_cancelled_once(&reply, 7),
            ModuleThreadRequest::SelectDb { reply, .. } => assert_cancelled_once(&reply, true),
            ModuleThreadRequest::Release { reply, .. } => assert_cancelled_once(&reply, ()),
        }
        assert_eq!(bridge.take_lease(), None);
        assert!(!bridge.resume_main());
    }
}

#[test]
fn bridge_completed_response_releases_the_ordinary_slot_once() {
    let (guard, lease) = owned_bridge();
    let bridge = &guard.0;
    for expected in [true, false] {
        let reply = RpcReply::default();
        assert_eq!(
            bridge.submit(ModuleThreadRequest::SelectDb {
                lease,
                db_idx: 0,
                reply: reply.clone(),
            }),
            Ok(())
        );
        let request = bridge.wait_request(lease).unwrap().unwrap();
        assert_eq!(bridge.validate_request(&request), Ok(()));
        reply.complete(expected);
        reply.cancel(libc::ECANCELED);
        assert_eq!(terminal_reply(reply.clone()), Ok(expected));
        reply.complete(!expected);
        assert_eq!(terminal_reply(reply), Err(libc::EALREADY));
    }
    bridge.finish_lease(lease);
    assert!(!bridge.has_active_thread_work());
}
