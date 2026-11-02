use crate::engine::extension::module::ValkeyModuleCtx;
use libc::{c_char, c_double, c_int, c_longlong, c_void, size_t};

#[repr(C)]
pub(super) struct TestReplyHandlers {
    version: u64,
    null: Option<extern "C" fn(*mut c_void)>,
    null_bulk_string: Option<extern "C" fn(*mut c_void)>,
    null_array: Option<extern "C" fn(*mut c_void)>,
    bulk_string: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    simple_string: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    verbatim_string: Option<extern "C" fn(*mut c_void, *const c_char, size_t, *const c_char)>,
    error: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    integer: Option<extern "C" fn(*mut c_void, c_longlong)>,
    double_val: Option<extern "C" fn(*mut c_void, c_double)>,
    big_number: Option<extern "C" fn(*mut c_void, *const c_char, size_t)>,
    bool_val: Option<extern "C" fn(*mut c_void, c_int)>,
    attribute_start: Option<extern "C" fn(*mut c_void, size_t)>,
    attribute_end: Option<extern "C" fn(*mut c_void)>,
    array_start: Option<extern "C" fn(*mut c_void, size_t)>,
    array_end: Option<extern "C" fn(*mut c_void)>,
    map_start: Option<extern "C" fn(*mut c_void, size_t)>,
    map_end: Option<extern "C" fn(*mut c_void)>,
    set_start: Option<extern "C" fn(*mut c_void, size_t)>,
    set_end: Option<extern "C" fn(*mut c_void)>,
    reply_parsing_error: Option<extern "C" fn(*mut c_void)>,
    on_blocked: Option<extern "C" fn(*mut c_void, *mut ValkeyModuleCtx, *mut c_void)>,
    on_resp_available:
        Option<extern "C" fn(*mut c_void, *mut ValkeyModuleCtx, *const c_char, size_t) -> c_int>,
}

impl TestReplyHandlers {
    pub(super) fn with_on_blocked(
        callback: extern "C" fn(*mut c_void, *mut ValkeyModuleCtx, *mut c_void),
    ) -> Self {
        Self {
            version: 1,
            null: None,
            null_bulk_string: None,
            null_array: None,
            bulk_string: None,
            simple_string: None,
            verbatim_string: None,
            error: None,
            integer: None,
            double_val: None,
            big_number: None,
            bool_val: None,
            attribute_start: None,
            attribute_end: None,
            array_start: None,
            array_end: None,
            map_start: None,
            map_end: None,
            set_start: None,
            set_end: None,
            reply_parsing_error: None,
            on_blocked: Some(callback),
            on_resp_available: None,
        }
    }

    pub(super) fn with_on_resp_available(
        callback: extern "C" fn(*mut c_void, *mut ValkeyModuleCtx, *const c_char, size_t) -> c_int,
    ) -> Self {
        Self {
            version: 1,
            null: None,
            null_bulk_string: None,
            null_array: None,
            bulk_string: None,
            simple_string: None,
            verbatim_string: None,
            error: None,
            integer: None,
            double_val: None,
            big_number: None,
            bool_val: None,
            attribute_start: None,
            attribute_end: None,
            array_start: None,
            array_end: None,
            map_start: None,
            map_end: None,
            set_start: None,
            set_end: None,
            reply_parsing_error: None,
            on_blocked: None,
            on_resp_available: Some(callback),
        }
    }
}

pub(super) extern "C" fn record_proto_len(
    reply_ctx: *mut c_void,
    _ctx: *mut ValkeyModuleCtx,
    _proto: *const c_char,
    proto_len: size_t,
) -> c_int {
    // SAFETY: the test passes a live usize pointer as reply_ctx.
    unsafe { *reply_ctx.cast::<usize>() = proto_len };
    0
}
