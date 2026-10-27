use super::call::{
    ValkeyModuleCallReply, ValkeyModule_CallReplyArrayElement, ValkeyModule_FreeCallReply,
    ValkeyModule_RetainString, ValkeyModule_StringAppendBuffer,
};
use super::string_obj_api::{
    ValkeyModule_AutoMemory, ValkeyModule_CreateString, ValkeyModule_FreeString,
};
use crate::engine::extension::module::auto_memory::{collect, entry_count};
use crate::engine::extension::module::lifecycle_test_support::snapshot;
use crate::engine::extension::module::{
    create_module_cmd_ctx, create_module_ctx, free_module_ctx, ValkeyModuleString, VALKEYMODULE_ERR,
};
use crate::foundation::io::protocol::Value;

fn new_context() -> *mut crate::engine::extension::module::ValkeyModuleCtx {
    Box::into_raw(create_module_ctx(Box::into_raw(create_module_cmd_ctx(
        "lifecycle",
        "lifecycle.test",
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    ))))
}

#[test]
fn auto_memory_collects_new_strings_at_context_exit() {
    let internal = Box::into_raw(create_module_cmd_ctx(
        "lifecycle",
        "lifecycle.test",
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    ));
    let ctx = Box::into_raw(create_module_ctx(internal));
    let before = snapshot();
    // SAFETY: ctx owns a fresh context, and auto-created strings are not used after its exit.
    unsafe {
        ValkeyModule_AutoMemory(ctx);
        ValkeyModule_CreateString(ctx, b"owned".as_ptr().cast(), 5);
        free_module_ctx(ctx);
    }
    assert_eq!(snapshot().strings - before.strings, 1);
}

#[test]
fn shared_string_append_rejects_mutation() {
    let value = Box::into_raw(ValkeyModuleString::from_bytes(b"shared"));
    // SAFETY: both references remain owned until the append result is checked.
    unsafe {
        ValkeyModule_RetainString(std::ptr::null_mut(), value);
        assert_eq!(
            ValkeyModule_StringAppendBuffer(std::ptr::null_mut(), value, b"x".as_ptr().cast(), 1),
            VALKEYMODULE_ERR
        );
        ValkeyModule_FreeString(std::ptr::null_mut(), value);
        ValkeyModule_FreeString(std::ptr::null_mut(), value);
    }
}

#[test]
fn borrowed_reply_child_free_leaves_the_tree_owned_by_its_root() {
    let root = Box::into_raw(ValkeyModuleCallReply::from_value(
        &Value::Array(vec![Value::bulk("child")]),
        false,
    ));
    let before = snapshot();
    // SAFETY: root and child are live; the drop assertion runs before any further
    // tree access, so an incorrect public free cannot cause a test-side UAF.
    unsafe {
        let child = ValkeyModule_CallReplyArrayElement(root, 0);
        ValkeyModule_FreeCallReply(child);
        assert_eq!(snapshot().replies, before.replies);
        ValkeyModule_FreeCallReply(root);
    }
    assert_eq!(snapshot().replies - before.replies, 2);
}

#[test]
fn enable_is_idempotent_and_does_not_adopt_old_or_null_context_allocations() {
    let ctx = new_context();
    let before = snapshot();
    // SAFETY: each allocation is used only while its own reference remains live.
    unsafe {
        let old = ValkeyModule_CreateString(ctx, b"old".as_ptr().cast(), 3);
        ValkeyModule_AutoMemory(ctx);
        ValkeyModule_AutoMemory(ctx);
        let _tls = super::ActiveModuleContextScope::enter(ctx);
        let manual = ValkeyModule_CreateString(std::ptr::null_mut(), b"manual".as_ptr().cast(), 6);
        let auto = ValkeyModule_CreateString(ctx, b"auto".as_ptr().cast(), 4);
        assert_eq!(entry_count((*ctx).internal), 1);
        ValkeyModule_FreeString(ctx, auto);
        assert_eq!(entry_count((*ctx).internal), 0);
        collect((*ctx).internal);
        collect((*ctx).internal);
        assert!((*(*ctx).internal).auto_memory.is_enabled);
        assert_eq!((*old).as_bytes(), b"old");
        assert_eq!((*manual).as_bytes(), b"manual");
        ValkeyModule_FreeString(ctx, old);
        ValkeyModule_FreeString(std::ptr::null_mut(), manual);
        free_module_ctx(ctx);
    }
    assert_eq!(snapshot().strings - before.strings, 3);
}

#[test]
fn matching_retain_transfers_once_then_adds_real_references() {
    let ctx = new_context();
    let before = snapshot();
    // SAFETY: explicit retain transfers or adds references before they are released.
    unsafe {
        ValkeyModule_AutoMemory(ctx);
        let string = ValkeyModule_CreateString(ctx, b"a".as_ptr().cast(), 1);
        ValkeyModule_RetainString(ctx, string);
        assert_eq!(ValkeyModuleString::ref_count(string), 1);
        assert_eq!(entry_count((*ctx).internal), 0);
        assert_eq!(
            ValkeyModule_StringAppendBuffer(ctx, string, b"b".as_ptr().cast(), 1),
            0
        );
        ValkeyModule_RetainString(ctx, string);
        assert_eq!(ValkeyModuleString::ref_count(string), 2);
        ValkeyModule_FreeString(ctx, string);
        free_module_ctx(ctx);
        assert_eq!((*string).as_bytes(), b"ab");
        ValkeyModule_FreeString(std::ptr::null_mut(), string);
    }
    assert_eq!(snapshot().strings - before.strings, 1);
}

#[test]
fn null_retain_keeps_auto_reference_for_both_release_orders() {
    for release_manual_first in [false, true] {
        let ctx = new_context();
        let before = snapshot();
        // SAFETY: null retain owns a separate reference regardless of TLS state.
        unsafe {
            ValkeyModule_AutoMemory(ctx);
            let string = ValkeyModule_CreateString(ctx, b"alive".as_ptr().cast(), 5);
            let _tls = super::ActiveModuleContextScope::enter(ctx);
            ValkeyModule_RetainString(std::ptr::null_mut(), string);
            assert_eq!(ValkeyModuleString::ref_count(string), 2);
            assert_eq!(entry_count((*ctx).internal), 1);
            if release_manual_first {
                ValkeyModule_FreeString(std::ptr::null_mut(), string);
                assert_eq!(entry_count((*ctx).internal), 1);
                assert_eq!(ValkeyModuleString::ref_count(string), 1);
                collect((*ctx).internal);
            } else {
                collect((*ctx).internal);
                assert_eq!(ValkeyModuleString::ref_count(string), 1);
                assert_eq!((*string).as_bytes(), b"alive");
                ValkeyModule_FreeString(std::ptr::null_mut(), string);
            }
            assert_eq!(entry_count((*ctx).internal), 0);
            free_module_ctx(ctx);
        }
        assert_eq!(snapshot().strings - before.strings, 1);
    }
}

#[test]
fn null_final_free_cancels_reverse_owner_and_cross_context_retain_does_not() {
    let a = new_context();
    let b = new_context();
    let before = snapshot();
    // SAFETY: a and b are distinct live scopes; only owned references are released.
    unsafe {
        ValkeyModule_AutoMemory(a);
        ValkeyModule_AutoMemory(b);
        let final_free = ValkeyModule_CreateString(a, b"x".as_ptr().cast(), 1);
        ValkeyModule_FreeString(std::ptr::null_mut(), final_free);
        assert_eq!(entry_count((*a).internal), 0);
        let shared = ValkeyModule_CreateString(a, b"shared".as_ptr().cast(), 6);
        ValkeyModule_RetainString(b, shared);
        assert_eq!(entry_count((*a).internal), 1);
        assert_eq!(entry_count((*b).internal), 0);
        assert_eq!(ValkeyModuleString::ref_count(shared), 2);
        free_module_ctx(a);
        assert_eq!((*shared).as_bytes(), b"shared");
        ValkeyModule_FreeString(b, shared);
        free_module_ctx(b);
    }
    assert_eq!(snapshot().strings - before.strings, 2);
}

#[test]
fn retained_host_argument_can_be_freed_immediately_or_survive_host_exit() {
    for release_module_first in [false, true] {
        let ctx = new_context();
        let before = snapshot();
        let argv = Box::into_raw(ValkeyModuleString::from_bytes(b"argument"));
        // SAFETY: the host owns one argv reference and retain adds a distinct module reference.
        unsafe {
            ValkeyModule_AutoMemory(ctx);
            ValkeyModule_RetainString(ctx, argv);
            assert_eq!(entry_count((*ctx).internal), 0);
            assert_eq!(ValkeyModuleString::ref_count(argv), 2);
            if release_module_first {
                ValkeyModule_FreeString(ctx, argv);
                ValkeyModuleString::release(argv);
                free_module_ctx(ctx);
            } else {
                ValkeyModuleString::release(argv);
                free_module_ctx(ctx);
                assert_eq!((*argv).as_bytes(), b"argument");
                ValkeyModule_FreeString(std::ptr::null_mut(), argv);
            }
        }
        assert_eq!(snapshot().strings - before.strings, 1);
    }
}

#[test]
fn append_preserves_binary_data_and_supports_repeated_self_append() {
    let string = Box::into_raw(ValkeyModuleString::from_bytes(b"a\0b"));
    let before = snapshot();
    // SAFETY: the source aliases string, which append must copy before mutation.
    unsafe {
        for _ in 0..3 {
            let source = (*string).as_bytes();
            assert_eq!(
                ValkeyModule_StringAppendBuffer(
                    std::ptr::null_mut(),
                    string,
                    source.as_ptr().cast(),
                    source.len(),
                ),
                0
            );
        }
        assert_eq!((*string).as_bytes(), b"a\0ba\0ba\0ba\0ba\0ba\0ba\0ba\0b");
        let bytes = (*string).as_bytes();
        assert_eq!(*bytes.as_ptr().add(bytes.len()), 0);
        assert_eq!(snapshot().strings, before.strings);
        ValkeyModule_RetainString(std::ptr::null_mut(), string);
        assert_eq!(
            ValkeyModule_StringAppendBuffer(std::ptr::null_mut(), string, b"x".as_ptr().cast(), 1,),
            VALKEYMODULE_ERR
        );
        assert_eq!((*string).as_bytes().len(), 24);
        ValkeyModuleString::release(string);
        ValkeyModuleString::release(string);
    }
    assert_eq!(snapshot().strings - before.strings, 1);
}

#[test]
fn reply_scope_owns_roots_and_copied_strings_independently_of_nested_tls() {
    use super::call::{finish_call_reply, ValkeyModule_CreateStringFromCallReply};
    let a = new_context();
    let b = new_context();
    let before = snapshot();
    // SAFETY: all borrowed children remain live until their root is freed.
    unsafe {
        ValkeyModule_AutoMemory(a);
        ValkeyModule_AutoMemory(b);
        let root = finish_call_reply(
            a,
            ValkeyModuleCallReply::from_value_with_resp(
                &Value::attribute(
                    vec![(Value::bulk("meta"), Value::Integer(9))],
                    Value::Array(vec![
                        Value::Map(vec![(Value::bulk("key"), Value::bulk("value"))]),
                        Value::Set(vec![Value::Integer(2)]),
                    ]),
                ),
                true,
                true,
            ),
        );
        assert_eq!(entry_count((*a).internal), 1);
        let map = ValkeyModule_CallReplyArrayElement(root, 0);
        let child = *(*map).elements.add(1);
        assert_eq!((*child).auto_owner, std::ptr::NonNull::new((*a).internal));
        let _tls = super::ActiveModuleContextScope::enter(b);
        let copied = ValkeyModule_CreateStringFromCallReply(child);
        assert_eq!(entry_count((*a).internal), 2);
        assert_eq!(entry_count((*b).internal), 0);
        for borrowed in [
            map,
            child,
            (*root).attribute,
            *(*(*root).attribute).elements,
        ] {
            ValkeyModule_FreeCallReply(borrowed);
        }
        assert_eq!(snapshot().replies, before.replies);
        ValkeyModule_FreeCallReply(root);
        assert_eq!((*copied).as_bytes(), b"value");
        assert_eq!(snapshot().replies - before.replies, 9);
        free_module_ctx(a);
        free_module_ctx(b);
    }
    assert_eq!(snapshot().strings - before.strings, 1);
}

#[test]
fn automatic_raw_and_error_reply_roots_are_collected_once() {
    use super::call::finish_call_reply;
    let ctx = new_context();
    let before = snapshot();
    // SAFETY: replies are owned by ctx and never accessed after collection.
    unsafe {
        ValkeyModule_AutoMemory(ctx);
        finish_call_reply(
            ctx,
            ValkeyModuleCallReply::from_value(&Value::error("ERR test"), false),
        );
        finish_call_reply(
            ctx,
            ValkeyModuleCallReply::from_raw_proto_with_resp(b"+RAW\r\n".to_vec(), false),
        );
        assert_eq!(entry_count((*ctx).internal), 2);
        collect((*ctx).internal);
        collect((*ctx).internal);
        assert_eq!(snapshot().replies - before.replies, 2);
        free_module_ctx(ctx);
    }
}

#[test]
fn manual_reply_does_not_retain_a_context_owner() {
    use super::call::{finish_call_reply, ValkeyModule_CreateStringFromCallReply};
    let ctx = new_context();
    // SAFETY: manual reply is independent of ctx and freed after copying its payload.
    unsafe {
        let root = finish_call_reply(
            ctx,
            ValkeyModuleCallReply::from_value(&Value::bulk("manual"), false),
        );
        assert!((*root).auto_owner.is_none());
        free_module_ctx(ctx);
        let string = ValkeyModule_CreateStringFromCallReply(root);
        ValkeyModule_FreeCallReply(root);
        assert_eq!((*string).as_bytes(), b"manual");
        ValkeyModuleString::release(string);
    }
}

#[test]
fn key_outputs_follow_original_owner_and_survive_close_and_iterator_stop() {
    use super::stream::{ValkeyModule_StreamIteratorNextField, ValkeyModule_StreamIteratorStop};
    use crate::engine::extension::module::{
        ModuleKeyHandle, ModuleStreamIterator, ValkeyModuleKey,
    };
    use crate::foundation::io::store::structure::stream::{StreamEntry, StreamID};
    let a = new_context();
    let b = new_context();
    let before = snapshot();
    let mut handle = ModuleKeyHandle::new(std::ptr::null_mut(), b"stream".to_vec(), 1, -2, false);
    // SAFETY: contexts and the owned key remain live until explicit cleanup.
    unsafe {
        ValkeyModule_AutoMemory(a);
        ValkeyModule_AutoMemory(b);
        handle.owner = std::ptr::NonNull::new((*a).internal);
        handle.stream_iterator = Some(ModuleStreamIterator {
            entries: Vec::new(),
            index: 0,
            field_index: 0,
            current: Some(StreamEntry {
                id: StreamID::new(1, 0),
                fields: vec![(
                    bytes::Bytes::from_static(b"field"),
                    bytes::Bytes::from_static(b"value"),
                )],
            }),
        });
        let handle = Box::into_raw(handle);
        crate::engine::extension::module::auto_memory::register_key(handle);
        let key = ValkeyModuleKey::from_handle(handle);
        let _tls = super::ActiveModuleContextScope::enter(b);
        let mut field = std::ptr::null_mut();
        let mut value = std::ptr::null_mut();
        assert_eq!(
            ValkeyModule_StreamIteratorNextField(key, &mut field, &mut value),
            0
        );
        assert_eq!(entry_count((*a).internal), 3);
        assert_eq!(entry_count((*b).internal), 0);
        ValkeyModule_StreamIteratorStop(key);
        super::key_api::ValkeyModule_CloseKey(key);
        assert_eq!(entry_count((*a).internal), 2);
        assert_eq!((*field).as_bytes(), b"field");
        assert_eq!((*value).as_bytes(), b"value");
        free_module_ctx(a);
        free_module_ctx(b);
    }
    assert_eq!(snapshot().strings - before.strings, 2);
    assert_eq!(snapshot().keys - before.keys, 1);
}

#[test]
fn automatic_key_is_collected_without_explicit_close() {
    use crate::engine::extension::module::ModuleKeyHandle;
    let ctx = new_context();
    let before = snapshot();
    let mut handle = ModuleKeyHandle::new(std::ptr::null_mut(), vec![], 1, -2, false);
    // SAFETY: registration transfers the sole key allocation to the live scope.
    unsafe {
        ValkeyModule_AutoMemory(ctx);
        handle.owner = std::ptr::NonNull::new((*ctx).internal);
        crate::engine::extension::module::auto_memory::register_key(Box::into_raw(handle));
        free_module_ctx(ctx);
    }
    assert_eq!(snapshot().keys - before.keys, 1);
}

#[test]
fn retained_loader_context_uses_the_same_scope_cleanup_on_registry_drop() {
    let ctx = new_context();
    let before = snapshot();
    let mut registry = crate::engine::extension::module::ModuleRegistry::new();
    // SAFETY: registry takes ownership of the internal allocation, not the outer shell.
    unsafe {
        ValkeyModule_AutoMemory(ctx);
        ValkeyModule_CreateString(ctx, b"a".as_ptr().cast(), 1);
        registry.register_internal_ctx((*ctx).internal);
        drop(Box::from_raw(ctx));
    }
    drop(registry);
    assert_eq!(snapshot().strings - before.strings, 1);
}
#[test]
fn detached_context_starts_with_an_independent_disabled_scope() {
    use super::thread_safe_context_api::{
        ValkeyModule_FreeThreadSafeContext, ValkeyModule_GetDetachedThreadSafeContext,
    };
    let source = new_context();
    let before = snapshot();
    // SAFETY: detached owns a separate context and allocations from source.
    unsafe {
        ValkeyModule_AutoMemory(source);
        ValkeyModule_CreateString(source, b"a".as_ptr().cast(), 1);
        let detached = ValkeyModule_GetDetachedThreadSafeContext(source);
        assert!(!(*(*detached).internal).auto_memory.is_enabled);
        assert_eq!(entry_count((*detached).internal), 0);
        ValkeyModule_AutoMemory(detached);
        ValkeyModule_CreateString(detached, b"b".as_ptr().cast(), 1);
        ValkeyModule_FreeThreadSafeContext(detached);
        assert_eq!(entry_count((*source).internal), 1);
        assert_eq!(snapshot().strings - before.strings, 1);
        free_module_ctx(source);
    }
    assert_eq!(snapshot().strings - before.strings, 2);
}

// SAFETY: the context is live and each variadic argument matches its format specifier.
pub(super) type ShimCall = unsafe extern "C" fn(
    *mut crate::engine::extension::module::ValkeyModuleCtx,
    *const libc::c_char,
    *const libc::c_char,
    ...
) -> *mut ValkeyModuleCallReply;
// SAFETY: the context is live and each variadic argument matches its format specifier.
pub(super) type ShimReplicate = unsafe extern "C" fn(
    *mut crate::engine::extension::module::ValkeyModuleCtx,
    *const libc::c_char,
    *const libc::c_char,
    ...
) -> libc::c_int;

pub(super) fn shim_functions() -> (ShimCall, ShimReplicate) {
    let mut call = std::ptr::null_mut();
    let mut replicate = std::ptr::null_mut();
    // SAFETY: the API resolver writes the actual C variadic shim entry points.
    unsafe {
        assert_eq!(
            super::module_get_api_impl(c"ValkeyModule_Call".as_ptr(), &mut call),
            0
        );
        assert_eq!(
            super::module_get_api_impl(c"ValkeyModule_Replicate".as_ptr(), &mut replicate),
            0
        );
        (
            std::mem::transmute::<*mut libc::c_void, ShimCall>(call),
            std::mem::transmute::<*mut libc::c_void, ShimReplicate>(replicate),
        )
    }
}

#[test]
fn shim_balances_mixed_and_duplicate_slots_and_partial_build_failures() {
    let ctx = new_context();
    let before = snapshot();
    let (call, replicate) = shim_functions();
    // SAFETY: each format exactly describes its varargs; invalid vectors fail before dereference.
    unsafe {
        ValkeyModule_AutoMemory(ctx);
        let string = ValkeyModule_CreateString(ctx, b"shared".as_ptr().cast(), 6);
        let vector = [string, string];
        let reply = call(
            ctx,
            c"unused".as_ptr(),
            c"clbsv".as_ptr(),
            c"text".as_ptr(),
            7i64,
            b"a\0b".as_ptr(),
            3usize,
            string,
            vector.as_ptr(),
            2usize,
        );
        assert!(reply.is_null());
        assert_eq!(ValkeyModuleString::ref_count(string), 1);
        assert_eq!(entry_count((*ctx).internal), 1);
        assert_eq!(snapshot().strings - before.strings, 4);
        assert_eq!(
            replicate(
                ctx,
                c"unused".as_ptr(),
                c"clbsv".as_ptr(),
                c"text".as_ptr(),
                7i64,
                b"a\0b".as_ptr(),
                3usize,
                string,
                vector.as_ptr(),
                2usize
            ),
            1
        );
        assert_eq!(ValkeyModuleString::ref_count(string), 1);
        assert_eq!(snapshot().strings - before.strings, 8);
        for format in [c"csvq", c"csv"] {
            let partial = [string, std::ptr::null_mut()];
            let count = if format == c"csvq" { 1 } else { 2 };
            assert!(call(
                ctx,
                c"unused".as_ptr(),
                format.as_ptr(),
                c"text".as_ptr(),
                string,
                partial.as_ptr(),
                count as usize
            )
            .is_null());
            assert_eq!(
                replicate(
                    ctx,
                    c"unused".as_ptr(),
                    format.as_ptr(),
                    c"text".as_ptr(),
                    string,
                    partial.as_ptr(),
                    count as usize
                ),
                1
            );
            assert_eq!(ValkeyModuleString::ref_count(string), 1);
            assert_eq!(entry_count((*ctx).internal), 1);
        }
        assert!(call(
            ctx,
            c"unused".as_ptr(),
            c"csv".as_ptr(),
            c"text".as_ptr(),
            string,
            vector.as_ptr(),
            usize::MAX
        )
        .is_null());
        assert_eq!(ValkeyModuleString::ref_count(string), 1);
        assert_eq!(snapshot().strings - before.strings, 18);
        free_module_ctx(ctx);
    }
    assert_eq!(snapshot().strings - before.strings, 19);
}
