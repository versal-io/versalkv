#![allow(non_snake_case)]

use bytes::Bytes;
use libc::{c_char, c_double, c_int, c_longlong, size_t};
use std::ptr::NonNull;

use super::super::super::auto_memory;
use super::super::super::{
    ModuleInternalCtx, ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK,
};
use crate::foundation::io::protocol::serializer::format_double;
use crate::foundation::io::protocol::Value;

// ============================================================================
// ============================================================================

pub const VALKEYMODULE_REPLY_UNKNOWN: c_int = -1;
pub const VALKEYMODULE_REPLY_STRING: c_int = 0;
pub const VALKEYMODULE_REPLY_ERROR: c_int = 1;
pub const VALKEYMODULE_REPLY_INTEGER: c_int = 2;
pub const VALKEYMODULE_REPLY_ARRAY: c_int = 3;
pub const VALKEYMODULE_REPLY_NULL: c_int = 4;
pub const VALKEYMODULE_REPLY_MAP: c_int = 5;
pub const VALKEYMODULE_REPLY_SET: c_int = 6;
pub const VALKEYMODULE_REPLY_BOOL: c_int = 7;
pub const VALKEYMODULE_REPLY_DOUBLE: c_int = 8;
pub const VALKEYMODULE_REPLY_BIG_NUMBER: c_int = 9;
pub const VALKEYMODULE_REPLY_VERBATIM_STRING: c_int = 10;
pub const VALKEYMODULE_REPLY_ATTRIBUTE: c_int = 11;
pub const VALKEYMODULE_REPLY_SIMPLE_STRING: c_int = 13;
pub const VALKEYMODULE_REPLY_ARRAY_NULL: c_int = 14;

// ============================================================================
// ============================================================================

#[repr(C)]
pub struct ValkeyModuleCallReply {
    pub reply_type: c_int,
    pub exact: bool,
    resp3: bool,
    pub(crate) is_root: bool,
    pub(crate) auto_owner: Option<NonNull<ModuleInternalCtx>>,
    pub str_data: *mut c_char,
    pub str_len: size_t,
    pub int_val: c_longlong,
    pub double_val: c_double,
    pub bool_val: c_int,
    pub num_elements: size_t,
    pub elements: *mut *mut ValkeyModuleCallReply,
    pub attribute: *mut ValkeyModuleCallReply,
    pub big_num_str: *mut c_char,
    pub big_num_len: size_t,
    pub verbatim_str: *mut c_char,
    pub verbatim_len: size_t,
    pub verbatim_fmt: *mut c_char,
    string_storage: Option<Box<[u8]>>,
    big_number_storage: Option<Box<[u8]>>,
    verbatim_storage: Option<Box<[u8]>>,
    verbatim_format_storage: Option<Box<[u8]>>,
    raw_proto: Option<Box<[u8]>>,
}

#[cfg(test)]
impl Drop for ValkeyModuleCallReply {
    fn drop(&mut self) {
        crate::engine::extension::module::lifecycle_test_support::reply_dropped();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallReplyKind {
    Unknown,
    String,
    Error,
    Integer,
    Array,
    Null,
    Map,
    Set,
    Bool,
    Double,
    BigNumber,
    VerbatimString,
    Attribute,
    SimpleString,
    NullArray,
}

impl CallReplyKind {
    fn from_raw(reply_type: c_int) -> Self {
        match reply_type {
            VALKEYMODULE_REPLY_STRING => Self::String,
            VALKEYMODULE_REPLY_ERROR => Self::Error,
            VALKEYMODULE_REPLY_INTEGER => Self::Integer,
            VALKEYMODULE_REPLY_ARRAY => Self::Array,
            VALKEYMODULE_REPLY_NULL => Self::Null,
            VALKEYMODULE_REPLY_MAP => Self::Map,
            VALKEYMODULE_REPLY_SET => Self::Set,
            VALKEYMODULE_REPLY_BOOL => Self::Bool,
            VALKEYMODULE_REPLY_DOUBLE => Self::Double,
            VALKEYMODULE_REPLY_BIG_NUMBER => Self::BigNumber,
            VALKEYMODULE_REPLY_VERBATIM_STRING => Self::VerbatimString,
            VALKEYMODULE_REPLY_ATTRIBUTE => Self::Attribute,
            VALKEYMODULE_REPLY_SIMPLE_STRING => Self::SimpleString,
            VALKEYMODULE_REPLY_ARRAY_NULL => Self::NullArray,
            _ => Self::Unknown,
        }
    }
}

fn reply_child_count(kind: CallReplyKind, logical_len: usize) -> usize {
    match kind {
        CallReplyKind::Map | CallReplyKind::Attribute => logical_len.checked_mul(2).unwrap_or(0),
        CallReplyKind::Array | CallReplyKind::Set => logical_len,
        _ => 0,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CallReplyNode<'a> {
    reply: &'a ValkeyModuleCallReply,
}

impl<'a> CallReplyNode<'a> {
    // SAFETY: reply must be part of a live, module-owned reply tree whose
    // element and attribute pointers remain valid for the returned borrow.
    // Non-null scalar pointers must span their recorded byte lengths.
    pub(crate) unsafe fn from_reply(reply: &'a ValkeyModuleCallReply) -> Self {
        Self { reply }
    }

    pub(crate) fn reply(self) -> &'a ValkeyModuleCallReply {
        self.reply
    }

    pub(crate) fn kind(self) -> CallReplyKind {
        CallReplyKind::from_raw(self.reply.reply_type)
    }

    pub(crate) fn effective_kind(self) -> CallReplyKind {
        CallReplyKind::from_raw(self.reply.effective_type())
    }

    pub(crate) fn has_resp3_provenance(self) -> bool {
        self.reply.resp3
    }

    fn bytes(self, ptr: *const c_char, len: usize) -> Option<&'a [u8]> {
        if ptr.is_null() {
            return None;
        }
        // SAFETY: the constructor invariant guarantees this live byte span.
        Some(unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) })
    }

    pub(crate) fn string_bytes(self) -> Option<&'a [u8]> {
        self.bytes(self.reply.str_data, self.reply.str_len)
    }

    pub(crate) fn big_number_bytes(self) -> Option<&'a [u8]> {
        self.bytes(self.reply.big_num_str, self.reply.big_num_len)
    }

    pub(crate) fn verbatim_bytes(self) -> Option<&'a [u8]> {
        self.bytes(self.reply.verbatim_str, self.reply.verbatim_len)
    }

    pub(crate) fn verbatim_format(self) -> Option<[u8; 3]> {
        self.bytes(self.reply.verbatim_fmt, 3)?.try_into().ok()
    }

    pub(crate) fn logical_len(self) -> usize {
        match self.kind() {
            CallReplyKind::Array
            | CallReplyKind::Map
            | CallReplyKind::Set
            | CallReplyKind::Attribute => self.reply.num_elements,
            _ => 0,
        }
    }

    fn child_count(self) -> usize {
        reply_child_count(self.kind(), self.logical_len())
    }

    pub(crate) fn children(self) -> CallReplyCursor<'a> {
        CallReplyCursor::new(self.reply.elements, self.child_count())
    }

    pub(crate) fn child_ptr(self, index: usize) -> Option<*mut ValkeyModuleCallReply> {
        self.children().raw_at(index)
    }

    pub(crate) fn attribute(self) -> Option<Self> {
        if self.reply.attribute.is_null() {
            return None;
        }
        // SAFETY: the constructor invariant covers the separately owned attribute node.
        Some(unsafe { Self::from_reply(&*self.reply.attribute) })
    }
}

pub(crate) struct CallReplyCursor<'a> {
    elements: &'a [*mut ValkeyModuleCallReply],
    index: usize,
}

impl<'a> CallReplyCursor<'a> {
    fn new(elements: *mut *mut ValkeyModuleCallReply, count: usize) -> Self {
        let elements = if elements.is_null() || count == 0 {
            &[]
        } else {
            // SAFETY: CallReplyNode's constructor guarantees that the pointer
            // spans the container width for the lifetime of the borrowed tree.
            unsafe { std::slice::from_raw_parts(elements, count) }
        };
        Self { elements, index: 0 }
    }

    fn raw_at(&self, index: usize) -> Option<*mut ValkeyModuleCallReply> {
        self.elements
            .get(index)
            .copied()
            .filter(|reply| !reply.is_null())
    }
}

impl<'a> Iterator for CallReplyCursor<'a> {
    type Item = CallReplyNode<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.elements.len() {
            let reply = self.elements[self.index];
            self.index += 1;
            if reply.is_null() {
                continue;
            }
            // SAFETY: the cursor slice and each non-null child pointer are
            // bounded by the live parent-tree borrow.
            return Some(unsafe { CallReplyNode::from_reply(&*reply) });
        }
        None
    }
}

fn normalize_resp2(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(normalize_resp2).collect()),
        Value::Map(entries) => Value::Array(
            entries
                .iter()
                .flat_map(|(key, value)| [normalize_resp2(key), normalize_resp2(value)])
                .collect(),
        ),
        Value::Set(items) | Value::Push(items) => {
            Value::Array(items.iter().map(normalize_resp2).collect())
        }
        Value::Boolean(value) => Value::Integer(i64::from(*value)),
        Value::Double(value) => Value::Bulk(Bytes::from(format_double(*value))),
        Value::BigNumber(value) => Value::Bulk(value.clone()),
        Value::Verbatim { data, .. } => Value::Bulk(data.clone()),
        Value::Attribute { inner, .. } => normalize_resp2(inner),
        other => other.clone(),
    }
}

impl ValkeyModuleCallReply {
    fn empty(reply_type: c_int, exact: bool, resp3: bool) -> Self {
        ValkeyModuleCallReply {
            reply_type,
            exact,
            resp3,
            is_root: true,
            auto_owner: None,
            str_data: std::ptr::null_mut(),
            str_len: 0,
            int_val: 0,
            double_val: 0.0,
            bool_val: 0,
            num_elements: 0,
            elements: std::ptr::null_mut(),
            attribute: std::ptr::null_mut(),
            big_num_str: std::ptr::null_mut(),
            big_num_len: 0,
            verbatim_str: std::ptr::null_mut(),
            verbatim_len: 0,
            verbatim_fmt: std::ptr::null_mut(),
            string_storage: None,
            big_number_storage: None,
            verbatim_storage: None,
            verbatim_format_storage: None,
            raw_proto: None,
        }
    }

    fn owned_bytes(bytes: &[u8]) -> (Box<[u8]>, *mut c_char) {
        let storage = bytes.to_vec().into_boxed_slice();
        let ptr = storage.as_ptr().cast_mut().cast::<c_char>();
        (storage, ptr)
    }

    pub(crate) fn from_raw_proto_with_resp(proto: Vec<u8>, resp3: bool) -> Box<Self> {
        let mut reply = Self::empty(VALKEYMODULE_REPLY_UNKNOWN, true, resp3);
        reply.raw_proto = Some(proto.into_boxed_slice());
        Box::new(reply)
    }

    pub fn raw_proto(&self) -> Option<&[u8]> {
        self.raw_proto.as_deref()
    }

    fn build_elements<'a>(
        items: impl Iterator<Item = &'a Value>,
        exact: bool,
        resp3: bool,
    ) -> (*mut *mut ValkeyModuleCallReply, size_t) {
        let mut elements: Vec<*mut ValkeyModuleCallReply> = Vec::new();
        for item in items {
            let mut child = Self::from_wire_value(item, exact, resp3);
            child.is_root = false;
            elements.push(Box::into_raw(child));
        }
        if elements.is_empty() {
            (std::ptr::null_mut(), 0)
        } else {
            let count = elements.len() as size_t;
            let boxed = elements.into_boxed_slice();
            (
                Box::into_raw(boxed) as *mut *mut ValkeyModuleCallReply,
                count,
            )
        }
    }

    pub fn from_value(value: &Value, exact: bool) -> Box<Self> {
        Self::from_value_with_resp(value, exact, false)
    }

    pub(crate) fn from_value_with_resp(value: &Value, exact: bool, resp3: bool) -> Box<Self> {
        if resp3 {
            return Self::from_wire_value(value, exact, true);
        }
        Self::from_wire_value(&normalize_resp2(value), exact, false)
    }

    fn from_wire_value(value: &Value, exact: bool, resp3: bool) -> Box<Self> {
        match value {
            Value::Null
            | Value::NullArray
            | Value::Integer(_)
            | Value::Double(_)
            | Value::Boolean(_) => Self::from_scalar(value, exact, resp3),
            Value::SimpleString(value) => {
                Self::from_string(value, VALKEYMODULE_REPLY_SIMPLE_STRING, exact, resp3)
            }
            Value::Bulk(value) => Self::from_string(value, VALKEYMODULE_REPLY_STRING, exact, resp3),
            Value::Error(value) => Self::from_string(value, VALKEYMODULE_REPLY_ERROR, exact, resp3),
            Value::BigNumber(value) => Self::from_big_number(value, exact, resp3),
            Value::Verbatim { format, data } => Self::from_verbatim(*format, data, exact, resp3),
            Value::Array(items) => {
                Self::from_sequence(items, VALKEYMODULE_REPLY_ARRAY, exact, resp3)
            }
            Value::Set(items) => Self::from_sequence(items, VALKEYMODULE_REPLY_SET, exact, resp3),
            Value::Push(items) => {
                Self::from_sequence(items, VALKEYMODULE_REPLY_ARRAY, exact, resp3)
            }
            Value::Map(entries) => Self::from_map(entries, exact, resp3),
            Value::Attribute { attrs, inner } => Self::from_attribute(attrs, inner, exact, resp3),
        }
    }

    fn from_scalar(value: &Value, exact: bool, resp3: bool) -> Box<Self> {
        let (reply_type, integer, double, boolean) = match value {
            Value::Null => (VALKEYMODULE_REPLY_NULL, 0, 0.0, 0),
            Value::NullArray => (VALKEYMODULE_REPLY_ARRAY_NULL, 0, 0.0, 0),
            Value::Integer(value) => (VALKEYMODULE_REPLY_INTEGER, *value, 0.0, 0),
            Value::Double(value) => (VALKEYMODULE_REPLY_DOUBLE, 0, *value, 0),
            Value::Boolean(value) => (VALKEYMODULE_REPLY_BOOL, 0, 0.0, i32::from(*value)),
            _ => unreachable!("scalar constructor called for container"),
        };
        let mut reply = Self::empty(reply_type, exact, resp3);
        reply.int_val = integer;
        reply.double_val = double;
        reply.bool_val = boolean;
        Box::new(reply)
    }

    fn from_string(value: &Bytes, reply_type: c_int, exact: bool, resp3: bool) -> Box<Self> {
        let mut reply = Self::empty(reply_type, exact, resp3);
        let (storage, ptr) = Self::owned_bytes(value);
        reply.str_data = ptr;
        reply.str_len = value.len();
        reply.string_storage = Some(storage);
        Box::new(reply)
    }

    fn from_big_number(value: &Bytes, exact: bool, resp3: bool) -> Box<Self> {
        let mut reply = Self::empty(VALKEYMODULE_REPLY_BIG_NUMBER, exact, resp3);
        let (storage, ptr) = Self::owned_bytes(value);
        reply.big_num_str = ptr;
        reply.big_num_len = value.len();
        reply.big_number_storage = Some(storage);
        Box::new(reply)
    }

    fn from_verbatim(format: [u8; 3], data: &Bytes, exact: bool, resp3: bool) -> Box<Self> {
        let mut reply = Self::empty(VALKEYMODULE_REPLY_VERBATIM_STRING, exact, resp3);
        let (storage, ptr) = Self::owned_bytes(data);
        let (format_storage, format_ptr) = Self::owned_bytes(&format);
        reply.verbatim_str = ptr;
        reply.verbatim_len = data.len();
        reply.verbatim_fmt = format_ptr;
        reply.verbatim_storage = Some(storage);
        reply.verbatim_format_storage = Some(format_storage);
        Box::new(reply)
    }

    fn from_sequence(items: &[Value], reply_type: c_int, exact: bool, resp3: bool) -> Box<Self> {
        let (elements, count) = Self::build_elements(items.iter(), exact, resp3);
        let mut reply = Self::empty(reply_type, exact, resp3);
        reply.num_elements = count;
        reply.elements = elements;
        Box::new(reply)
    }

    fn from_map(entries: &[(Value, Value)], exact: bool, resp3: bool) -> Box<Self> {
        let items = entries.iter().flat_map(|(key, value)| [key, value]);
        let (elements, _) = Self::build_elements(items, exact, resp3);
        let mut reply = Self::empty(VALKEYMODULE_REPLY_MAP, exact, resp3);
        reply.num_elements = entries.len();
        reply.elements = elements;
        Box::new(reply)
    }

    fn from_attribute(
        attrs: &[(Value, Value)],
        inner: &Value,
        exact: bool,
        resp3: bool,
    ) -> Box<Self> {
        let mut reply = Self::from_wire_value(inner, exact, resp3);
        let items = attrs.iter().flat_map(|(key, value)| [key, value]);
        let (elements, _) = Self::build_elements(items, exact, resp3);
        let mut attribute = Self::empty(VALKEYMODULE_REPLY_ATTRIBUTE, exact, resp3);
        attribute.is_root = false;
        attribute.num_elements = attrs.len();
        attribute.elements = elements;
        reply.attribute = Box::into_raw(Box::new(attribute));
        reply
    }

    pub fn effective_type(&self) -> c_int {
        if self.exact {
            self.reply_type
        } else {
            match self.reply_type {
                VALKEYMODULE_REPLY_SIMPLE_STRING => VALKEYMODULE_REPLY_STRING,
                VALKEYMODULE_REPLY_ARRAY_NULL => VALKEYMODULE_REPLY_NULL,
                other => other,
            }
        }
    }

    // SAFETY: Called by ValkeyModule_FreeCallReply for a tree created by the
    // constructors above. Every raw child/attribute pointer is uniquely owned.
    pub unsafe fn free(&mut self) {
        unsafe {
            self.str_data = std::ptr::null_mut();
            self.big_num_str = std::ptr::null_mut();
            self.verbatim_str = std::ptr::null_mut();
            self.verbatim_fmt = std::ptr::null_mut();
            self.string_storage.take();
            self.big_number_storage.take();
            self.verbatim_storage.take();
            self.verbatim_format_storage.take();
            if !self.elements.is_null() {
                let count =
                    reply_child_count(CallReplyKind::from_raw(self.reply_type), self.num_elements);
                let slice = std::ptr::slice_from_raw_parts_mut(self.elements, count);
                let mut elements = Box::from_raw(slice);
                for elem in elements.iter_mut() {
                    if !elem.is_null() {
                        let mut child = Box::from_raw(*elem);
                        child.free();
                    }
                }
                self.elements = std::ptr::null_mut();
            }
            if !self.attribute.is_null() {
                let mut attribute = Box::from_raw(self.attribute);
                attribute.free();
                self.attribute = std::ptr::null_mut();
            }
        }
    }
}

// SAFETY: ctx is null or live, and reply is a freshly allocated owned root.
pub(crate) unsafe fn finish_call_reply(
    ctx: *mut ValkeyModuleCtx,
    reply: Box<ValkeyModuleCallReply>,
) -> *mut ValkeyModuleCallReply {
    let reply = Box::into_raw(reply);
    // SAFETY: the explicit context owns an enabled scope if present.
    unsafe {
        let owner = auto_memory::internal_owner(ctx);
        if auto_memory::is_enabled(owner) {
            if let Some(owner) = owner {
                set_reply_owner(reply, Some(owner));
                auto_memory::register_reply(owner, reply);
            }
        }
    }
    reply
}

// SAFETY: reply owns a complete live tree, and owner is null or live.
unsafe fn set_reply_owner(
    reply: *mut ValkeyModuleCallReply,
    owner: Option<NonNull<ModuleInternalCtx>>,
) {
    // SAFETY: each child and attribute is uniquely owned by this tree.
    unsafe {
        (*reply).auto_owner = owner;
        let count = reply_child_count(
            CallReplyKind::from_raw((*reply).reply_type),
            (*reply).num_elements,
        );
        for index in 0..count {
            set_reply_owner(*(*reply).elements.add(index), owner);
        }
        if !(*reply).attribute.is_null() {
            set_reply_owner((*reply).attribute, owner);
        }
    }
}

// SAFETY: reply is a live owned root not registered in an automatic scope.
pub(crate) unsafe fn release_call_reply(reply: *mut ValkeyModuleCallReply) {
    // SAFETY: private recursive cleanup frees children regardless of is_root.
    unsafe {
        (*reply).free();
        drop(Box::from_raw(reply));
    }
}

// ============================================================================
// ============================================================================

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply obtained from a previous module API call (e.g., VM_CallWithReply).
pub unsafe extern "C" fn ValkeyModule_CallReplyType(reply: *mut ValkeyModuleCallReply) -> c_int {
    unsafe {
        if reply.is_null() {
            return VALKEYMODULE_REPLY_UNKNOWN;
        }
        (*reply).effective_type()
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply obtained from a previous module API call.
pub unsafe extern "C" fn ValkeyModule_CallReplyLength(reply: *mut ValkeyModuleCallReply) -> size_t {
    unsafe {
        if reply.is_null() {
            return 0;
        }
        let r = &*reply;
        match r.reply_type {
            VALKEYMODULE_REPLY_ARRAY
            | VALKEYMODULE_REPLY_MAP
            | VALKEYMODULE_REPLY_SET
            | VALKEYMODULE_REPLY_ATTRIBUTE => r.num_elements,
            VALKEYMODULE_REPLY_STRING
            | VALKEYMODULE_REPLY_ERROR
            | VALKEYMODULE_REPLY_SIMPLE_STRING => r.str_len,
            _ => 0,
        }
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply whose reply_type is ARRAY. idx must be less than num_elements.
pub unsafe extern "C" fn ValkeyModule_CallReplyArrayElement(
    reply: *mut ValkeyModuleCallReply,
    idx: size_t,
) -> *mut ValkeyModuleCallReply {
    unsafe {
        if reply.is_null() {
            return std::ptr::null_mut();
        }
        let r = &*reply;
        if r.reply_type != VALKEYMODULE_REPLY_ARRAY {
            return std::ptr::null_mut();
        }
        if idx >= r.num_elements {
            return std::ptr::null_mut();
        }
        let node = CallReplyNode::from_reply(r);
        node.child_ptr(idx).unwrap_or(std::ptr::null_mut())
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply obtained from a previous module API call.
pub unsafe extern "C" fn ValkeyModule_CallReplyInteger(
    reply: *mut ValkeyModuleCallReply,
) -> c_longlong {
    unsafe {
        if reply.is_null() {
            return 0;
        }
        (*reply).int_val
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply obtained from a previous module API call.
pub unsafe extern "C" fn ValkeyModule_CallReplyDouble(
    reply: *mut ValkeyModuleCallReply,
) -> c_double {
    unsafe {
        if reply.is_null() {
            return 0.0;
        }
        (*reply).double_val
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply obtained from a previous module API call.
pub unsafe extern "C" fn ValkeyModule_CallReplyBool(reply: *mut ValkeyModuleCallReply) -> c_int {
    unsafe {
        if reply.is_null() {
            return 0;
        }
        (*reply).bool_val
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply. len may be null; if non-null, it must point to a valid size_t.
pub unsafe extern "C" fn ValkeyModule_CallReplyStringPtr(
    reply: *mut ValkeyModuleCallReply,
    len: *mut size_t,
) -> *const c_char {
    unsafe {
        if reply.is_null() {
            return std::ptr::null();
        }
        let r = &*reply;
        match r.reply_type {
            VALKEYMODULE_REPLY_STRING
            | VALKEYMODULE_REPLY_ERROR
            | VALKEYMODULE_REPLY_SIMPLE_STRING => {
                if !len.is_null() {
                    *len = r.str_len;
                }
                r.str_data as *const c_char
            }
            VALKEYMODULE_REPLY_INTEGER => {
                let s = format!("{}", r.int_val);
                if !len.is_null() {
                    *len = s.len() as size_t;
                }
                std::ptr::null()
            }
            _ => std::ptr::null(),
        }
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply. len may be null; if non-null, it must point to a valid size_t.
pub unsafe extern "C" fn ValkeyModule_CallReplyProto(
    reply: *mut ValkeyModuleCallReply,
    len: *mut size_t,
) -> *const c_char {
    unsafe {
        if reply.is_null() {
            return std::ptr::null();
        }
        if let Some(proto) = (*reply).raw_proto() {
            if !len.is_null() {
                *len = proto.len();
            }
            return proto.as_ptr().cast::<c_char>();
        }
        ValkeyModule_CallReplyStringPtr(reply, len)
    }
}

#[export_name = "VM_FreeCallReply"]
// SAFETY: Called by Valkey module C code; reply must be a non-null pointer obtained from a previous
// module API call (e.g., VM_CallWithReply) that was created via Box::into_raw. The function
// reconstructs the Box and drops it, which is safe only if the pointer was obtained from
// Box::into_raw and has not been double-freed.
pub unsafe extern "C" fn ValkeyModule_FreeCallReply(reply: *mut ValkeyModuleCallReply) {
    unsafe {
        if reply.is_null() || !(*reply).is_root {
            return;
        }
        auto_memory::remove_reply(reply);
        release_call_reply(reply);
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply obtained from a previous module API call.
pub unsafe extern "C" fn ValkeyModule_CreateStringFromCallReply(
    reply: *mut ValkeyModuleCallReply,
) -> *mut ValkeyModuleString {
    unsafe {
        if reply.is_null() {
            return std::ptr::null_mut();
        }
        let r = &*reply;
        let node = CallReplyNode::from_reply(r);
        let data = match node.kind() {
            CallReplyKind::String | CallReplyKind::Error | CallReplyKind::SimpleString => {
                let Some(data) = node.string_bytes() else {
                    return std::ptr::null_mut();
                };
                data.to_vec()
            }
            CallReplyKind::Integer => format!("{}", r.int_val).into_bytes(),
            CallReplyKind::Double => format!("{}", r.double_val).into_bytes(),
            CallReplyKind::Bool => {
                if r.bool_val != 0 {
                    b"true".to_vec()
                } else {
                    b"false".to_vec()
                }
            }
            _ => return std::ptr::null_mut(),
        };
        auto_memory::create_string_for_owner(r.auto_owner, &data)
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply. len may be null; if non-null, it must point to a valid size_t.
pub unsafe extern "C" fn ValkeyModule_CallReplyBigNumber(
    reply: *mut ValkeyModuleCallReply,
    len: *mut size_t,
) -> *const c_char {
    unsafe {
        if reply.is_null() {
            return std::ptr::null();
        }
        let r = &*reply;
        if r.big_num_str.is_null() {
            return std::ptr::null();
        }
        if !len.is_null() {
            *len = r.big_num_len;
        }
        r.big_num_str as *const c_char
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply. len and format may be null; if non-null, they must point to valid
// size_t and *const c_char respectively.
pub unsafe extern "C" fn ValkeyModule_CallReplyVerbatim(
    reply: *mut ValkeyModuleCallReply,
    len: *mut size_t,
    format: *mut *const c_char,
) -> *const c_char {
    unsafe {
        if reply.is_null() {
            return std::ptr::null();
        }
        let r = &*reply;
        if r.verbatim_str.is_null() {
            return std::ptr::null();
        }
        if !len.is_null() {
            *len = r.verbatim_len;
        }
        if !format.is_null() {
            *format = r.verbatim_fmt as *const c_char;
        }
        r.verbatim_str as *const c_char
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply with reply_type SET. idx must be less than num_elements.
pub unsafe extern "C" fn ValkeyModule_CallReplySetElement(
    reply: *mut ValkeyModuleCallReply,
    idx: size_t,
) -> *mut ValkeyModuleCallReply {
    unsafe {
        if reply.is_null() {
            return std::ptr::null_mut();
        }
        let r = &*reply;
        if r.reply_type != VALKEYMODULE_REPLY_SET {
            return std::ptr::null_mut();
        }
        if idx >= r.num_elements {
            return std::ptr::null_mut();
        }
        let node = CallReplyNode::from_reply(r);
        node.child_ptr(idx).unwrap_or(std::ptr::null_mut())
    }
}

// SAFETY: Called by Valkey module C code; reply, key, and val must be non-null and point to
// valid pointers. The function writes to *key and *val, which must be valid write targets.
pub unsafe extern "C" fn ValkeyModule_CallReplyMapElement(
    reply: *mut ValkeyModuleCallReply,
    idx: size_t,
    key: *mut *mut ValkeyModuleCallReply,
    val: *mut *mut ValkeyModuleCallReply,
) -> c_int {
    unsafe {
        if reply.is_null() || key.is_null() || val.is_null() {
            return VALKEYMODULE_ERR;
        }
        let r = &*reply;
        if r.reply_type != VALKEYMODULE_REPLY_MAP {
            return VALKEYMODULE_ERR;
        }
        if idx >= r.num_elements {
            return VALKEYMODULE_ERR;
        }
        let node = CallReplyNode::from_reply(r);
        *key = node.child_ptr(idx * 2).unwrap_or(std::ptr::null_mut());
        *val = node.child_ptr(idx * 2 + 1).unwrap_or(std::ptr::null_mut());
        VALKEYMODULE_OK
    }
}

// SAFETY: Called by Valkey module C code; reply must be non-null and point to a valid
// ValkeyModuleCallReply obtained from a previous module API call.
pub unsafe extern "C" fn ValkeyModule_CallReplyAttribute(
    reply: *mut ValkeyModuleCallReply,
) -> *mut ValkeyModuleCallReply {
    unsafe {
        if reply.is_null() {
            return std::ptr::null_mut();
        }
        let r = &*reply;
        if r.attribute.is_null() {
            return std::ptr::null_mut();
        }
        r.attribute
    }
}

// SAFETY: Called by Valkey module C code; reply, key, and val must be non-null and point to
// valid pointers. The function dereferences reply->attribute and writes to *key and *val.
pub unsafe extern "C" fn ValkeyModule_CallReplyAttributeElement(
    reply: *mut ValkeyModuleCallReply,
    idx: size_t,
    key: *mut *mut ValkeyModuleCallReply,
    val: *mut *mut ValkeyModuleCallReply,
) -> c_int {
    unsafe {
        if reply.is_null() || key.is_null() || val.is_null() {
            return VALKEYMODULE_ERR;
        }
        let r = &*reply;
        let node = CallReplyNode::from_reply(r);
        let attr = if node.kind() == CallReplyKind::Attribute {
            node
        } else if let Some(attribute) = node.attribute() {
            attribute
        } else {
            return VALKEYMODULE_ERR;
        };
        if idx >= attr.logical_len() {
            return VALKEYMODULE_ERR;
        }
        *key = attr.child_ptr(idx * 2).unwrap_or(std::ptr::null_mut());
        *val = attr.child_ptr(idx * 2 + 1).unwrap_or(std::ptr::null_mut());
        VALKEYMODULE_OK
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::protocol::Value;
    use bytes::Bytes;

    // SAFETY: callers pass Box values returned by from_value and never reuse them after free().
    #[allow(clippy::boxed_local)] // The Box owns the top-level reply after recursive free().
    unsafe fn free_reply(b: Box<ValkeyModuleCallReply>) {
        unsafe {
            let mut b = *b;
            b.free();
            // b drops here, children already freed by free().
        }
    }

    #[test]
    fn scalar_variants_have_expected_fields() {
        // SAFETY: each reply is allocated by from_value, inspected before free,
        // and released exactly once by the test helper.
        unsafe {
            let r = ValkeyModuleCallReply::from_value(&Value::Integer(42), true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_INTEGER);
            assert!(r.exact);
            assert_eq!(r.int_val, 42);
            assert!(r.str_data.is_null());
            assert!(r.elements.is_null());
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value_with_resp(&Value::Double(1.5), false, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_DOUBLE);
            assert!(!r.exact);
            assert_eq!(r.double_val, 1.5);
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value_with_resp(&Value::Boolean(true), true, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_BOOL);
            assert_eq!(r.bool_val, 1);
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value(&Value::Null, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_NULL);
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value(&Value::NullArray, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_ARRAY_NULL);
            free_reply(r);
        }
    }

    #[test]
    fn string_variants_carry_original_len_and_type() {
        // SAFETY: each reply is allocated by from_value, inspected before free,
        // and released exactly once by the test helper.
        unsafe {
            let r = ValkeyModuleCallReply::from_value(&Value::bulk("hello"), true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_STRING); // Bulk
            assert_eq!(r.str_len, 5);
            assert!(!r.str_data.is_null());
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value(&Value::SimpleString("ok".into()), true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_SIMPLE_STRING);
            assert_eq!(r.str_len, 2);
            assert!(!r.str_data.is_null());
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value(&Value::Error("ERR x".into()), true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_ERROR);
            assert_eq!(r.str_len, 5);
            assert!(!r.str_data.is_null());
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value_with_resp(
                &Value::BigNumber("123".into()),
                true,
                true,
            );
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_BIG_NUMBER);
            assert_eq!(r.big_num_len, 3);
            assert!(!r.big_num_str.is_null());
            assert!(r.str_data.is_null());
            free_reply(r);

            let r = ValkeyModuleCallReply::from_value_with_resp(
                &Value::verbatim(*b"txt", "abcd"),
                true,
                true,
            );
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_VERBATIM_STRING);
            assert_eq!(r.verbatim_len, 4);
            assert!(!r.verbatim_str.is_null());
            assert!(!r.verbatim_fmt.is_null());
            free_reply(r);
        }
    }

    #[test]
    fn binary_payload_accessors_use_stored_lengths() {
        // SAFETY: every pointer is borrowed from a live reply and each owned
        // reply/string is released exactly once below.
        unsafe {
            let mut bulk =
                ValkeyModuleCallReply::from_value(&Value::Bulk(Bytes::from_static(b"a\0b")), true);
            let mut len = 0;
            let ptr = ValkeyModule_CallReplyStringPtr(&mut *bulk, &mut len);
            assert_eq!(std::slice::from_raw_parts(ptr.cast::<u8>(), len), b"a\0b");
            let string = ValkeyModule_CreateStringFromCallReply(&mut *bulk);
            assert_eq!((*string).as_bytes(), b"a\0b");
            ValkeyModuleString::release(string);
            free_reply(bulk);

            let mut error = ValkeyModuleCallReply::from_value(
                &Value::Error(Bytes::from_static(b"ERR a\0b")),
                true,
            );
            let ptr = ValkeyModule_CallReplyStringPtr(&mut *error, &mut len);
            assert_eq!(
                std::slice::from_raw_parts(ptr.cast::<u8>(), len),
                b"ERR a\0b"
            );
            free_reply(error);

            let mut verbatim = ValkeyModuleCallReply::from_value_with_resp(
                &Value::verbatim(*b"txt", Bytes::from_static(b"x\0y")),
                true,
                true,
            );
            let mut format = std::ptr::null();
            let ptr = ValkeyModule_CallReplyVerbatim(&mut *verbatim, &mut len, &mut format);
            assert_eq!(std::slice::from_raw_parts(format.cast::<u8>(), 3), b"txt");
            assert_eq!(std::slice::from_raw_parts(ptr.cast::<u8>(), len), b"x\0y");
            free_reply(verbatim);
        }
    }

    #[test]
    fn raw_unknown_reply_preserves_binary_protocol_bytes() {
        let proto = b"$3\r\na\0b\r\n".to_vec();
        let mut reply = ValkeyModuleCallReply::from_raw_proto_with_resp(proto.clone(), false);

        assert_eq!(reply.reply_type, VALKEYMODULE_REPLY_UNKNOWN);
        assert_eq!(reply.raw_proto(), Some(proto.as_slice()));
        let mut len = 0;
        // SAFETY: reply is live and len is a valid output pointer.
        let ptr = unsafe { ValkeyModule_CallReplyProto(&mut *reply, &mut len) };
        assert!(!ptr.is_null());
        assert_eq!(len, proto.len());
        // SAFETY: the raw reply owns this byte span until it is freed below.
        assert_eq!(
            unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) },
            proto
        );
        // SAFETY: reply owns its raw protocol bytes and is released once.
        unsafe { reply.free() };
    }

    #[test]
    fn container_variants_build_elements_and_counts() {
        // SAFETY: each reply is allocated by from_value, inspected before free,
        // and released exactly once by the test helper.
        unsafe {
            let arr = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
            let r = ValkeyModuleCallReply::from_value(&arr, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_ARRAY);
            assert_eq!(r.num_elements, 2);
            assert!(!r.elements.is_null());
            free_reply(r);

            let empty = Value::Array(vec![]);
            let r = ValkeyModuleCallReply::from_value(&empty, true);
            assert_eq!(r.num_elements, 0);
            assert!(r.elements.is_null()); // empty → null
            free_reply(r);

            let set = Value::Set(vec![Value::Integer(7)]);
            let r = ValkeyModuleCallReply::from_value_with_resp(&set, true, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_SET);
            assert_eq!(r.num_elements, 1);
            assert!(!r.elements.is_null());
            free_reply(r);

            let push = Value::push(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]);
            let r = ValkeyModuleCallReply::from_value_with_resp(&push, true, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_ARRAY); // Push maps to ARRAY
            assert_eq!(r.num_elements, 3);
            assert!(!r.elements.is_null());
            free_reply(r);

            let map = Value::Map(vec![
                (Value::bulk("k1"), Value::Integer(1)),
                (Value::bulk("k2"), Value::Integer(2)),
            ]);
            let r = ValkeyModuleCallReply::from_value_with_resp(&map, true, true);
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_MAP);
            assert_eq!(r.num_elements, 2); // pair count
            assert!(!r.elements.is_null());
            free_reply(r);

            let attr = Value::attribute(
                vec![(Value::bulk("a"), Value::Integer(9))],
                Value::Integer(100),
            );
            let r = ValkeyModuleCallReply::from_value_with_resp(&attr, true, true);
            // inner is Integer(100), attribute attached separately
            assert_eq!(r.reply_type, VALKEYMODULE_REPLY_INTEGER);
            assert_eq!(r.int_val, 100);
            assert!(!r.attribute.is_null());
            assert_eq!((*r.attribute).reply_type, VALKEYMODULE_REPLY_ATTRIBUTE);
            assert_eq!((*r.attribute).num_elements, 1); // pair count
            free_reply(r);
        }
    }

    #[test]
    fn reply_cursor_bounds_container_children_without_taking_ownership() {
        let value = Value::Map(vec![
            (Value::Integer(1), Value::Integer(2)),
            (Value::Integer(3), Value::Integer(4)),
        ]);
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);

        {
            // SAFETY: reply owns a complete live tree for this scope.
            let node = unsafe { CallReplyNode::from_reply(&reply) };
            assert_eq!(node.kind(), CallReplyKind::Map);
            assert_eq!(node.logical_len(), 2);
            let children: Vec<_> = node.children().collect();
            assert_eq!(children.len(), 4);
            assert_eq!(children[0].reply().int_val, 1);
            assert_eq!(children[3].reply().int_val, 4);
        }

        assert_eq!(reply.num_elements, 2);
        // SAFETY: the cursor only borrowed the tree; reply still owns every child
        // and is released exactly once here.
        unsafe { reply.free() };
    }

    #[test]
    fn attribute_cursor_exposes_pair_children_separately_from_wrapped_value() {
        let value = Value::attribute(
            vec![(Value::bulk("meta"), Value::Integer(9))],
            Value::Integer(100),
        );
        let mut reply = ValkeyModuleCallReply::from_value_with_resp(&value, true, true);

        {
            // SAFETY: reply owns a complete live tree for this scope.
            let wrapped = unsafe { CallReplyNode::from_reply(&reply) };
            assert_eq!(wrapped.kind(), CallReplyKind::Integer);
            assert_eq!(wrapped.reply().int_val, 100);
            let attribute = wrapped.attribute().expect("attribute must be borrowed");
            assert_eq!(attribute.kind(), CallReplyKind::Attribute);
            assert_eq!(attribute.logical_len(), 1);
            assert_eq!(attribute.children().count(), 2);
        }

        // SAFETY: neither node nor cursor transfers ownership from reply.
        unsafe { reply.free() };
    }

    #[test]
    fn element_accessors_reject_out_of_range_container_indices() {
        // SAFETY: every reply is live for each accessor call and freed once below.
        unsafe {
            let mut array =
                ValkeyModuleCallReply::from_value(&Value::Array(vec![Value::Integer(1)]), true);
            assert!(ValkeyModule_CallReplyArrayElement(&mut *array, 1).is_null());
            free_reply(array);

            let mut set = ValkeyModuleCallReply::from_value_with_resp(
                &Value::Set(vec![Value::Integer(2)]),
                true,
                true,
            );
            assert!(ValkeyModule_CallReplySetElement(&mut *set, 1).is_null());
            free_reply(set);

            let mut map = ValkeyModuleCallReply::from_value_with_resp(
                &Value::Map(vec![(Value::Integer(3), Value::Integer(4))]),
                true,
                true,
            );
            let mut key = std::ptr::null_mut();
            let mut value = std::ptr::null_mut();
            assert_eq!(
                ValkeyModule_CallReplyMapElement(&mut *map, 1, &mut key, &mut value),
                VALKEYMODULE_ERR
            );
            assert!(key.is_null());
            assert!(value.is_null());
            free_reply(map);
        }
    }

    #[test]
    fn cursor_and_array_accessor_treat_a_null_child_slot_as_absent() {
        let mut reply =
            ValkeyModuleCallReply::from_value(&Value::Array(vec![Value::Integer(1)]), true);

        // SAFETY: elements points to one owned child slot. The child pointer is
        // restored before freeing, so ownership remains unchanged.
        unsafe {
            let child = *reply.elements;
            *reply.elements = std::ptr::null_mut();
            let node = CallReplyNode::from_reply(&reply);
            assert_eq!(node.children().count(), 0);
            assert!(ValkeyModule_CallReplyArrayElement(&mut *reply, 0).is_null());
            *reply.elements = child;
            reply.free();
        }
    }
}
