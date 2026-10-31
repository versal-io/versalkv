#![allow(non_snake_case)]

use bytes::Bytes;
use libc::c_int;

use super::super::super::{ValkeyModuleCtx, VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::super::ReplyBuilder;

use super::call_reply::{CallReplyKind, CallReplyNode, ValkeyModuleCallReply};

// ============================================================================
// ============================================================================

// SAFETY: Called from C shim or module code; caller must pass valid or NULL ValkeyModuleCtx and ValkeyModuleCallReply pointers.
pub unsafe extern "C" fn ValkeyModule_ReplyWithCallReply(
    ctx: *mut ValkeyModuleCtx,
    reply: *mut ValkeyModuleCallReply,
) -> c_int {
    unsafe {
        if ctx.is_null() || reply.is_null() {
            return VALKEYMODULE_ERR;
        }
        let internal = (*ctx).internal;
        if internal.is_null() || (*internal).reply_ptr.is_null() {
            return VALKEYMODULE_ERR;
        }
        let node = CallReplyNode::from_reply(&*reply);
        let resp3_only = is_resp3_only(node);
        if (*internal).resp_version < 3 && resp3_only {
            return VALKEYMODULE_ERR;
        }
        let builder = &mut *((*internal).reply_ptr as *mut ReplyBuilder);

        // Recursively append CallReply to builder
        append_call_reply_to_builder(reply, builder);
        VALKEYMODULE_OK
    }
}

// SAFETY: Helper called from ValkeyModule_ReplyWithCallReply; caller must ensure the reply pointer is valid or NULL, and the builder reference is valid for the lifetime of the call.
unsafe fn append_call_reply_to_builder(
    reply: *mut ValkeyModuleCallReply,
    builder: &mut ReplyBuilder,
) {
    if reply.is_null() {
        builder.add_null();
        return;
    }
    // SAFETY: the caller guarantees a complete live reply tree.
    let node = unsafe { CallReplyNode::from_reply(&*reply) };
    append_node_to_builder(node, builder);
}

fn is_resp3_only(node: CallReplyNode<'_>) -> bool {
    let current_is_resp3 = matches!(
        node.kind(),
        CallReplyKind::Map
            | CallReplyKind::Set
            | CallReplyKind::Bool
            | CallReplyKind::Double
            | CallReplyKind::BigNumber
            | CallReplyKind::VerbatimString
            | CallReplyKind::Attribute
    ) || (node.has_resp3_provenance()
        && matches!(node.kind(), CallReplyKind::Null | CallReplyKind::NullArray))
        || node.attribute().is_some();
    current_is_resp3 || node.children().any(is_resp3_only)
}

fn append_node_to_builder(node: CallReplyNode<'_>, builder: &mut ReplyBuilder) {
    if let Some(attribute) = node.attribute() {
        builder.add_attribute(attribute.logical_len() as c_int);
        append_children(attribute, builder);
    }
    append_node_value(node, builder);
}

fn append_children(node: CallReplyNode<'_>, builder: &mut ReplyBuilder) {
    for child in node.children() {
        append_node_to_builder(child, builder);
    }
}

fn append_resp3_scalar(node: CallReplyNode<'_>, builder: &mut ReplyBuilder) {
    match node.kind() {
        CallReplyKind::BigNumber => match node.big_number_bytes() {
            Some(value) => builder.add_big_number(Bytes::copy_from_slice(value)),
            None => builder.add_null(),
        },
        CallReplyKind::VerbatimString => match (node.verbatim_format(), node.verbatim_bytes()) {
            (Some(format), Some(data)) => {
                builder.add_verbatim(format, Bytes::copy_from_slice(data))
            }
            _ => builder.add_null(),
        },
        _ => builder.add_null(),
    }
}

fn append_node_value(node: CallReplyNode<'_>, builder: &mut ReplyBuilder) {
    let reply = node.reply();
    match node.kind() {
        CallReplyKind::Null => builder.add_null(),
        CallReplyKind::NullArray => builder.add_null_array(),
        CallReplyKind::String => match node.string_bytes() {
            Some(data) => builder.add_bulk_string(Bytes::copy_from_slice(data)),
            None => builder.add_null(),
        },
        CallReplyKind::SimpleString => builder.add_simple_string(
            node.string_bytes()
                .map(Bytes::copy_from_slice)
                .unwrap_or_else(|| Bytes::from_static(b"OK")),
        ),
        CallReplyKind::Error => builder.add_error(
            node.string_bytes()
                .map(Bytes::copy_from_slice)
                .unwrap_or_else(|| Bytes::from_static(b"ERR")),
        ),
        CallReplyKind::Integer => builder.add_integer(reply.int_val),
        CallReplyKind::Double => match node.decimal_double() {
            Some(value) => builder.add_decimal_double(value.clone()),
            None => builder.add_double(reply.double_val),
        },
        CallReplyKind::Bool => builder.add_bool(reply.bool_val != 0),
        CallReplyKind::BigNumber | CallReplyKind::VerbatimString => {
            append_resp3_scalar(node, builder)
        }
        CallReplyKind::Array => {
            builder.add_array(node.logical_len() as c_int);
            append_children(node, builder);
        }
        CallReplyKind::Map => {
            builder.add_map(node.logical_len() as c_int);
            append_children(node, builder);
        }
        CallReplyKind::Set => {
            builder.add_set(node.logical_len() as c_int);
            append_children(node, builder);
        }
        _ => builder.add_null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::module::{
        create_module_cmd_ctx, create_module_ctx, free_module_ctx, ValkeyModuleCtx,
    };
    use crate::foundation::io::protocol::Value;

    struct ReplyTestContext {
        ctx: *mut ValkeyModuleCtx,
    }

    impl ReplyTestContext {
        fn new(resp_version: c_int) -> Self {
            let mut internal = create_module_cmd_ctx(
                "test",
                "test.reply_with_call_reply",
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            internal.resp_version = resp_version;
            let ctx = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
            Self { ctx }
        }

        fn take_reply(&mut self) -> Value {
            // SAFETY: ctx and its ReplyBuilder allocation remain owned by this fixture.
            unsafe {
                let internal = (*self.ctx).internal;
                let builder = &mut *((*internal).reply_ptr.cast::<ReplyBuilder>());
                std::mem::replace(builder, ReplyBuilder::new()).into_value()
            }
        }
    }

    impl Drop for ReplyTestContext {
        fn drop(&mut self) {
            // SAFETY: the fixture exclusively owns this context and frees it once.
            unsafe { free_module_ctx(self.ctx) };
        }
    }

    fn attributed_value() -> Value {
        Value::attribute(
            vec![(Value::bulk("meta"), Value::Integer(9))],
            Value::Integer(100),
        )
    }

    fn free_reply(reply: &mut ValkeyModuleCallReply) {
        // SAFETY: each test owns the full reply tree and frees it exactly once.
        unsafe { reply.free() };
    }

    fn assert_resp2_rejects(value: Value) {
        let mut context = ReplyTestContext::new(2);
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);

        // SAFETY: context and reply remain live for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) };
        let forwarded = context.take_reply();
        free_reply(&mut reply);

        assert_eq!(result, VALKEYMODULE_ERR, "accepted {value:?}");
        assert_eq!(forwarded, Value::Null, "mutated builder for {value:?}");
    }

    fn assert_resp3_round_trip(value: Value) {
        let mut context = ReplyTestContext::new(3);
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);

        // SAFETY: context and reply remain live for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) };
        let forwarded = context.take_reply();
        free_reply(&mut reply);

        assert_eq!(result, VALKEYMODULE_OK, "rejected {value:?}");
        assert_eq!(forwarded, value);
    }

    #[test]
    fn decimal_double_module_forwarding_preserves_exact_nested_payloads() {
        use crate::foundation::io::protocol::DecimalDoubleText;
        for text in ["9007199254740993", "1e4932", "inf", "-inf"] {
            let scalar = Value::DecimalDouble(DecimalDoubleText::new(text.to_owned()).unwrap());
            assert_resp3_round_trip(scalar.clone());
            assert_resp3_round_trip(Value::Array(vec![scalar.clone()]));
            assert_resp3_round_trip(Value::Set(vec![scalar.clone()]));
            assert_resp3_round_trip(Value::Map(vec![(scalar.clone(), scalar.clone())]));
            assert_resp3_round_trip(Value::attribute(
                vec![(Value::bulk("meta"), scalar.clone())],
                scalar.clone(),
            ));
            assert_resp2_rejects(scalar.clone());
            let mut context = ReplyTestContext::new(2);
            let mut reply = ValkeyModuleCallReply::from_value_with_resp(&scalar, true, false);
            // SAFETY: the fixture and this uniquely owned reply remain live for the call.
            assert_eq!(
                unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) },
                VALKEYMODULE_OK
            );
            assert_eq!(context.take_reply(), Value::bulk(text.to_owned()));
            free_reply(&mut reply);
        }
    }

    fn assert_resp2_round_trip(value: Value, exact: bool) {
        let mut context = ReplyTestContext::new(2);
        let mut reply = ValkeyModuleCallReply::from_value(&value, exact);

        // SAFETY: context and reply remain live for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) };
        let forwarded = context.take_reply();
        free_reply(&mut reply);

        assert_eq!(result, VALKEYMODULE_OK, "rejected {value:?}");
        assert_eq!(forwarded, value);
    }

    fn assert_resp2_rejects_resp3_null(value: Value) {
        let mut context = ReplyTestContext::new(2);
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);

        // SAFETY: context and reply remain live for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) };
        let forwarded = context.take_reply();
        free_reply(&mut reply);

        assert_eq!(result, VALKEYMODULE_ERR, "accepted {value:?}");
        assert_eq!(forwarded, Value::Null, "mutated builder for {value:?}");
    }

    fn assert_wire_forward(
        source_resp3: bool,
        target_resp: c_int,
        semantic: &Value,
        expected: &Value,
    ) {
        let mut context = ReplyTestContext::new(target_resp);
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(semantic, true, source_resp3);

        // SAFETY: context and reply remain live for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) };
        let forwarded = context.take_reply();
        free_reply(&mut reply);

        assert_eq!(result, VALKEYMODULE_OK, "rejected {semantic:?}");
        assert_eq!(&forwarded, expected);
    }

    fn semantic_protocol_matrix_value() -> Value {
        Value::Array(vec![
            Value::Map(vec![(
                Value::SimpleString(Bytes::from_static(b"key")),
                Value::Boolean(true),
            )]),
            Value::Set(vec![Value::Double(1.5)]),
            Value::Push(vec![Value::BigNumber(Bytes::from_static(b"123"))]),
            Value::verbatim(*b"txt", Bytes::from_static(b"text")),
            Value::attribute(
                vec![(Value::bulk("ignored"), Value::Integer(9))],
                Value::Boolean(false),
            ),
            Value::Bulk(Bytes::from_static(b"bulk")),
            Value::Error(Bytes::from_static(b"ERR test")),
            Value::Integer(7),
            Value::Null,
            Value::NullArray,
        ])
    }

    fn normalized_resp2_matrix_value() -> Value {
        Value::Array(vec![
            Value::Array(vec![
                Value::SimpleString(Bytes::from_static(b"key")),
                Value::Integer(1),
            ]),
            Value::Array(vec![Value::Bulk(Bytes::from_static(b"1.5"))]),
            Value::Array(vec![Value::Bulk(Bytes::from_static(b"123"))]),
            Value::Bulk(Bytes::from_static(b"text")),
            Value::Integer(0),
            Value::Bulk(Bytes::from_static(b"bulk")),
            Value::Error(Bytes::from_static(b"ERR test")),
            Value::Integer(7),
            Value::Null,
            Value::NullArray,
        ])
    }

    fn native_call_reply_matrix_value() -> Value {
        let mut value = semantic_protocol_matrix_value();
        let Value::Array(items) = &mut value else {
            unreachable!("matrix fixture is an array")
        };
        items[2] = Value::Array(vec![Value::BigNumber(Bytes::from_static(b"123"))]);
        value
    }

    #[test]
    fn protocol_matrix_normalizes_source_wire_before_target_forwarding() {
        let semantic = semantic_protocol_matrix_value();
        let normalized = normalized_resp2_matrix_value();
        let native = native_call_reply_matrix_value();

        assert_wire_forward(false, 2, &semantic, &normalized);
        assert_wire_forward(false, 3, &semantic, &normalized);
        assert_resp2_rejects(semantic.clone());
        assert_wire_forward(true, 3, &semantic, &native);
    }

    #[test]
    fn binary_bulk_and_verbatim_survive_top_level_and_nested_forwarding() {
        let bulk = Value::Bulk(Bytes::from_static(b"a\0b"));
        let verbatim = Value::verbatim(*b"txt", Bytes::from_static(b"x\0y"));

        assert_wire_forward(false, 2, &bulk, &bulk);
        assert_wire_forward(true, 3, &verbatim, &verbatim);
        assert_wire_forward(
            true,
            3,
            &Value::Array(vec![bulk.clone(), verbatim.clone()]),
            &Value::Array(vec![bulk, verbatim]),
        );
    }

    #[test]
    fn resp2_rejects_arrays_with_nested_resp3_scalars_and_containers() {
        let resp3_children = vec![
            Value::Map(vec![(Value::bulk("key"), Value::Integer(1))]),
            Value::Set(vec![Value::Integer(2)]),
            Value::Boolean(true),
            Value::Double(1.5),
            Value::BigNumber(Bytes::from_static(b"12345678901234567890")),
            Value::verbatim(*b"txt", "payload"),
        ];

        for child in resp3_children {
            assert_resp2_rejects(Value::Array(vec![child]));
        }
    }

    #[test]
    fn resp2_rejects_arrays_with_nested_attribute_and_deeper_resp3_child() {
        let resp3_children = vec![
            attributed_value(),
            Value::Array(vec![Value::Boolean(false)]),
        ];

        for child in resp3_children {
            assert_resp2_rejects(Value::Array(vec![child]));
        }
    }

    #[test]
    fn resp3_preserves_top_level_and_nested_big_number_and_verbatim() {
        let big_number = Value::BigNumber(Bytes::from_static(b"12345678901234567890"));
        let verbatim = Value::verbatim(*b"mkd", "# heading");

        assert_resp3_round_trip(big_number.clone());
        assert_resp3_round_trip(verbatim.clone());
        assert_resp3_round_trip(Value::Array(vec![big_number, verbatim]));
    }

    #[test]
    fn exact_resp2_nulls_are_accepted_and_keep_their_builder_types() {
        assert_resp2_round_trip(Value::Null, true);
        assert_resp2_round_trip(Value::NullArray, true);
        assert_resp2_round_trip(Value::Array(vec![Value::Null, Value::NullArray]), true);
    }

    #[test]
    fn actual_resp3_nulls_are_rejected_top_level_and_nested_by_resp2() {
        assert_resp2_rejects_resp3_null(Value::Null);
        assert_resp2_rejects_resp3_null(Value::NullArray);
        assert_resp2_rejects_resp3_null(Value::Array(vec![Value::Null, Value::NullArray]));
    }

    #[test]
    fn resp2_rejects_wrapped_reply_with_attached_attribute() {
        let mut context = ReplyTestContext::new(2);
        let mut reply =
            ValkeyModuleCallReply::from_value_with_resp(&attributed_value(), true, true);

        // SAFETY: context and reply remain live for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) };

        assert_eq!(result, VALKEYMODULE_ERR);
        assert_eq!(context.take_reply(), Value::Null);
        free_reply(&mut reply);
    }

    #[test]
    fn resp2_rejects_direct_attribute_node() {
        let mut context = ReplyTestContext::new(2);
        let mut reply =
            ValkeyModuleCallReply::from_value_with_resp(&attributed_value(), true, true);
        assert!(!reply.attribute.is_null());

        // SAFETY: attribute is a live child of reply for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, reply.attribute) };

        assert_eq!(result, VALKEYMODULE_ERR);
        assert_eq!(context.take_reply(), Value::Null);
        free_reply(&mut reply);
    }

    #[test]
    fn resp3_accepts_and_preserves_wrapped_reply_with_attribute() {
        let mut context = ReplyTestContext::new(3);
        let expected = attributed_value();
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&expected, true, true);

        // SAFETY: context and reply remain live for the synchronous ABI call.
        let result = unsafe { ValkeyModule_ReplyWithCallReply(context.ctx, &mut *reply) };

        assert_eq!(result, VALKEYMODULE_OK);
        assert_eq!(context.take_reply(), expected);
        free_reply(&mut reply);
    }

    #[test]
    fn builder_conversion_preserves_attribute_pairs_and_wrapped_reply() {
        let value = Value::attribute(
            vec![(Value::bulk("meta"), Value::Integer(9))],
            Value::Integer(100),
        );
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);
        let mut builder = ReplyBuilder::new();

        // SAFETY: reply owns a complete live tree for the conversion.
        unsafe { append_call_reply_to_builder(&mut *reply, &mut builder) };
        assert_eq!(builder.into_value(), value);
        // SAFETY: conversion only borrowed the tree, which is freed once here.
        unsafe { reply.free() };
    }
}
