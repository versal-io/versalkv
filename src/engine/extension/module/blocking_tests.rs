use super::*;
use crate::engine::command::context::Token;
use crate::engine::command::{CommandArgs, SessionId};
use crate::engine::extension::module::api::ModuleReplyFrame;
use crate::engine::extension::module::registry::ModuleOptions;
use crate::engine::extension::module::thread_bridge::{next_id, ModuleContextKind};
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

fn shared() -> Arc<ModuleBlockedShared> {
    let module = ModuleIdentity::new("blocked-state".into(), ModuleOptions::default()).unwrap();
    let bridge = ModuleThreadBridgeHandle::new().unwrap();
    ModuleBlockedShared::new(
        ModuleBlockedId(next_id().unwrap()),
        module,
        bridge,
        ModuleBlockOrigin {
            session: SessionId(next_id().unwrap()),
            token: Token(17),
            command_seq: 1,
            args: CommandArgs::from_parts(vec![Bytes::from_static(b"blocked-state.wait")]),
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
    .unwrap()
}

#[test]
fn native_block_early_unblock_seals_replies_without_arming_or_replacing_data() {
    let shared = shared();
    let identity = shared.module.clone();
    let mut first_data = Box::new(1_u8);
    let mut second_data = Box::new(2_u8);
    let first_ptr = (&mut *first_data as *mut u8).cast();
    let second_ptr = (&mut *second_data as *mut u8).cast();
    assert_eq!(identity.blocked_handles.load(Ordering::Acquire), 1);
    assert_eq!(
        shared.with_replies(|builder| builder.add_integer(41)),
        Some(())
    );
    assert_eq!(shared.accept_unblock(first_ptr), Ok(()));
    assert_eq!(shared.accept_unblock(second_ptr), Err(libc::EINVAL));
    assert_eq!(
        shared.with_replies(|_| panic!("sealed replies must reject the writer")),
        None::<()>
    );
    {
        let lifecycle = lock(&shared.lifecycle);
        assert_eq!(lifecycle.stage, ModuleBlockStage::Publishing);
        assert!(lifecycle.completion_queued);
        assert_eq!(lifecycle.private_data.0, first_ptr);
    }
    shared.bridge.close();
    assert_eq!(shared.bridge.take_completions(), vec![shared.id]);
    assert!(shared.bridge.take_completions().is_empty());
    assert_eq!(shared.accept_unblock(second_ptr), Err(libc::ESHUTDOWN));
    let frames = {
        let mut replies = lock(&shared.replies);
        assert!(replies.is_sealed);
        std::mem::replace(&mut replies.builder, ReplyBuilder::new()).into_frames()
    };
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        frames.as_slice(),
        [ModuleReplyFrame::Value(Value::Integer(41))]
    ));
    drop(shared);
    assert_eq!(identity.blocked_handles.load(Ordering::Acquire), 0);
}

#[test]
fn native_block_rejected_unblock_keeps_caller_data_and_reply_builder_open() {
    let shared = shared();
    let mut data = Box::new(3_u8);
    shared.bridge.close();
    assert_eq!(
        shared.accept_unblock((&mut *data as *mut u8).cast()),
        Err(libc::ESHUTDOWN)
    );
    {
        let lifecycle = lock(&shared.lifecycle);
        assert_eq!(lifecycle.stage, ModuleBlockStage::Publishing);
        assert!(!lifecycle.completion_queued);
        assert!(lifecycle.private_data.0.is_null());
    }
    assert_eq!(
        shared.with_replies(|builder| builder.add_integer(9)),
        Some(())
    );
    assert!(!lock(&shared.replies).is_sealed);
    assert!(shared.bridge.take_completions().is_empty());
    assert_eq!(*data, 3);
}
