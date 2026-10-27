use libc::{c_char, c_int, c_void};
use std::ptr::NonNull;

pub type ValkeyModuleCmdFunc = Option<
    extern "C" fn(
        ctx: *mut ValkeyModuleCtx,
        argv: *mut *mut ValkeyModuleString,
        argc: c_int,
    ) -> c_int,
>;
pub type ValkeyModuleTimerProc = extern "C" fn(*mut ValkeyModuleCtx, *mut c_void);
pub type ValkeyModuleInfoFunc = extern "C" fn(*mut c_void, c_int);
pub type ValkeyModuleNotificationFunc =
    extern "C" fn(*mut ValkeyModuleCtx, c_int, *const c_char, *mut ValkeyModuleString) -> c_int;
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValkeyModuleEvent {
    pub id: u64,
    pub dataver: u64,
}
pub type ValkeyModuleEventCallback =
    extern "C" fn(*mut ValkeyModuleCtx, ValkeyModuleEvent, u64, *mut c_void);
pub type ValkeyModuleScanCB =
    extern "C" fn(*mut ValkeyModuleCtx, *mut ValkeyModuleString, *mut ValkeyModuleKey, *mut c_void);
pub type ValkeyModuleScanKeyCB = extern "C" fn(
    *mut ValkeyModuleKey,
    *mut ValkeyModuleString,
    *mut ValkeyModuleString,
    *mut c_void,
);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ValkeyModuleStreamID {
    pub ms: u64,
    pub seq: u64,
}

pub type ModuleLoadFunc =
    extern "C" fn(ctx: *mut c_void, argv: *mut *mut c_void, argc: c_int) -> c_int;

pub type ModuleUnloadFunc = extern "C" fn(ctx: *mut c_void) -> c_int;

#[repr(C)]
pub struct ValkeyModuleCtx {
    pub getapifuncptr: *mut c_void,
    pub internal: *mut ModuleInternalCtx,
}

#[repr(C)]
pub struct ModuleInternalCtx {
    pub module_name: *mut c_char,
    pub module_ref: *mut c_void,
    pub(crate) module_options: Option<super::registry::ModuleOptions>,
    pub onload: c_int,
    pub reply_ptr: *mut c_void,
    pub runtime_handle: *mut c_void,
    pub db_idx: c_int,
    pub resp_version: c_int,
    pub username: *mut c_char,
    pub module_auth_user: *mut c_char,
    pub current_command_name: *mut c_char,
    pub(super) auto_memory: super::auto_memory::ModuleAutoMemoryScope,
}

#[repr(C)]
pub struct ValkeyModuleString {
    _data: *mut c_void,
}

struct ModuleStringData {
    bytes: Vec<u8>,
    len: usize,
    ref_count: usize,
    auto_owner: Option<NonNull<ModuleInternalCtx>>,
}

#[cfg(test)]
impl Drop for ModuleStringData {
    fn drop(&mut self) {
        super::lifecycle_test_support::string_dropped();
    }
}

impl ValkeyModuleString {
    pub fn from_bytes(data: &[u8]) -> Box<Self> {
        let mut bytes = Vec::with_capacity(data.len() + 1);
        bytes.extend_from_slice(data);
        bytes.push(0);
        let value = Box::new(ModuleStringData {
            bytes,
            len: data.len(),
            ref_count: 1,
            auto_owner: None,
        });
        Box::new(ValkeyModuleString {
            _data: Box::into_raw(value).cast(),
        })
    }

    // SAFETY: The caller must ensure that `self._data` points to live
    // ModuleStringData allocated by `from_bytes` and has not been freed.
    pub unsafe fn as_bytes(&self) -> &[u8] {
        unsafe {
            if self._data.is_null() {
                &[]
            } else {
                let value = &*(self._data as *const ModuleStringData);
                &value.bytes[..value.len]
            }
        }
    }

    // SAFETY: string is a live module string.
    pub unsafe fn incr_ref(string: *mut Self) {
        // SAFETY: the caller owns a live reference to the shell and payload.
        unsafe {
            let value = &mut *(*string)._data.cast::<ModuleStringData>();
            let Some(count) = value.ref_count.checked_add(1) else {
                std::process::abort();
            };
            value.ref_count = count;
        }
    }

    // SAFETY: string is null or owns one live reference; its auto owner is live.
    pub unsafe fn release(string: *mut Self) {
        if string.is_null() {
            return;
        }
        // SAFETY: only the final reference reclaims the shell and payload.
        unsafe {
            let payload = (*string)._data.cast::<ModuleStringData>();
            (*payload).ref_count -= 1;
            if (*payload).ref_count != 0 {
                return;
            }
            let owner = (*payload).auto_owner;
            super::auto_memory::remove_string(owner, string);
            drop(Box::from_raw(payload));
            drop(Box::from_raw(string));
        }
    }

    // SAFETY: string points to a live shell and payload.
    pub(super) unsafe fn set_auto_owner(
        string: *mut Self,
        owner: Option<NonNull<ModuleInternalCtx>>,
    ) {
        // SAFETY: the caller guarantees a live payload.
        unsafe { (*(*string)._data.cast::<ModuleStringData>()).auto_owner = owner };
    }

    // SAFETY: string points to a live shell and payload.
    pub(super) unsafe fn ref_count(string: *mut Self) -> usize {
        // SAFETY: the payload remains live while reading its count.
        unsafe { (*(*string)._data.cast::<ModuleStringData>()).ref_count }
    }

    // SAFETY: string is live; bytes have been copied if they alias its payload.
    pub(super) unsafe fn append(string: *mut Self, bytes: &[u8]) -> bool {
        // SAFETY: mutate the existing payload only when its reference is unique.
        unsafe {
            let value = &mut *(*string)._data.cast::<ModuleStringData>();
            if value.ref_count != 1 {
                return false;
            }
            value.bytes.pop();
            value.bytes.extend_from_slice(bytes);
            value.len = value.bytes.len();
            value.bytes.push(0);
            true
        }
    }
}

pub const VALKEYMODULE_KEYTYPE_EMPTY: c_int = 0;
pub const VALKEYMODULE_KEYTYPE_STRING: c_int = 1;
pub const VALKEYMODULE_KEYTYPE_LIST: c_int = 2;
pub const VALKEYMODULE_KEYTYPE_HASH: c_int = 3;
pub const VALKEYMODULE_KEYTYPE_SET: c_int = 4;
pub const VALKEYMODULE_KEYTYPE_ZSET: c_int = 5;
pub const VALKEYMODULE_KEYTYPE_MODULE: c_int = 6;
pub const VALKEYMODULE_KEYTYPE_STREAM: c_int = 7;

#[repr(C)]
pub struct ModuleKeyHandle {
    pub db: *mut c_void,
    pub key: Vec<u8>,
    pub mode: c_int,
    pub list_max_listpack_size: i64,
    pub uses_lfu: bool,
    pub stream_iterator: Option<ModuleStreamIterator>,
    pub(super) owner: Option<NonNull<ModuleInternalCtx>>,
}

#[cfg(test)]
impl Drop for ModuleKeyHandle {
    fn drop(&mut self) {
        super::lifecycle_test_support::key_dropped();
    }
}

pub struct ModuleStreamIterator {
    pub entries: Vec<crate::foundation::io::store::structure::stream::StreamEntry>,
    pub index: usize,
    pub current: Option<crate::foundation::io::store::structure::stream::StreamEntry>,
    pub field_index: usize,
}

impl ModuleKeyHandle {
    pub fn new(
        db: *mut c_void,
        key: Vec<u8>,
        mode: c_int,
        list_max_listpack_size: i64,
        uses_lfu: bool,
    ) -> Box<Self> {
        Box::new(Self {
            db,
            key,
            mode,
            list_max_listpack_size,
            uses_lfu,
            stream_iterator: None,
            owner: None,
        })
    }
}

#[repr(C)]
pub struct ValkeyModuleUser {
    pub(crate) user: *mut c_void,
    pub(crate) username: *mut c_char,
    pub(crate) is_module_user: c_int,
}

#[repr(C)]
pub struct ValkeyModuleKey {
    _data: *mut c_void,
}

impl ValkeyModuleKey {
    pub fn from_handle(handle: *mut ModuleKeyHandle) -> *mut Self {
        handle as *mut Self
    }

    pub fn as_handle<'a>(key: *mut Self) -> Option<&'a mut ModuleKeyHandle> {
        if key.is_null() {
            None
        } else {
            // SAFETY: The caller has verified that `key` is non-null. ValkeyModuleKey
            // is a transparent wrapper around ModuleKeyHandle (both are #[repr(C)] and
            // from_handle casts ModuleKeyHandle* to ValkeyModuleKey*), so casting back
            // is valid. The caller must ensure that the returned reference does not
            // outlive the underlying ModuleKeyHandle allocation.
            Some(unsafe { &mut *(key as *mut ModuleKeyHandle) })
        }
    }
}

#[repr(C)]
pub struct ValkeyModuleIO {
    _data: *mut c_void,
}

#[repr(C)]
pub struct ValkeyModuleType {
    pub id: u64,
    pub module: *mut c_void,
    pub rdb_load: Option<extern "C" fn(*mut ValkeyModuleIO, c_int) -> *mut c_void>,
    pub rdb_save: Option<extern "C" fn(*mut ValkeyModuleIO, *mut c_void)>,
    pub aof_rewrite: Option<extern "C" fn(*mut ValkeyModuleIO, *mut c_void, *mut c_void)>,
    pub mem_usage: Option<extern "C" fn(*const c_void) -> usize>,
    pub digest: Option<extern "C" fn(*mut ValkeyModuleDigest, *mut c_void)>,
    pub free: Option<extern "C" fn(*mut c_void)>,
    pub free_effort: Option<extern "C" fn(*mut c_void, *const c_void) -> usize>,
    pub unlink: Option<extern "C" fn(*mut c_void, *mut c_void)>,
    pub copy: Option<extern "C" fn(*mut c_void, *mut c_void, *const c_void) -> *mut c_void>,
    pub defrag:
        Option<extern "C" fn(*mut ValkeyModuleDefragCtx, *mut c_void, *mut *mut c_void) -> c_int>,
    pub mem_usage2:
        Option<extern "C" fn(*mut ValkeyModuleKeyOptCtx, *const c_void, usize) -> usize>,
    pub free2: Option<extern "C" fn(*mut ValkeyModuleKeyOptCtx, *mut c_void)>,
    pub free_effort2: Option<extern "C" fn(*mut ValkeyModuleKeyOptCtx, *const c_void) -> usize>,
    pub unlink2: Option<extern "C" fn(*mut ValkeyModuleKeyOptCtx, *mut c_void)>,
    pub copy2: Option<extern "C" fn(*mut ValkeyModuleKeyOptCtx, *const c_void) -> *mut c_void>,
    pub aux_save: Option<extern "C" fn(*mut ValkeyModuleIO, c_int)>,
    pub aux_save2: Option<extern "C" fn(*mut ValkeyModuleIO, c_int)>,
    pub aux_load: Option<extern "C" fn(*mut ValkeyModuleIO, c_int, c_int) -> c_int>,
    pub aux_save_triggers: c_int,
    pub name: [c_char; 10],
}

#[repr(C)]
pub struct ValkeyModuleDigest {
    _data: [u8; 40],
}

#[repr(C)]
pub struct ValkeyModuleDefragCtx {
    _data: *mut c_void,
}

#[repr(C)]
pub struct ValkeyModuleKeyOptCtx {
    _data: *mut c_void,
}

#[cfg(test)]
mod tests {
    use super::ValkeyModuleString;

    #[test]
    fn module_string_preserves_interior_nul_bytes() {
        let value = Box::into_raw(ValkeyModuleString::from_bytes(b"1\0x"));

        // SAFETY: value owns a live module string for the duration of the assertion.
        assert_eq!(unsafe { (*value).as_bytes() }, b"1\0x");

        // SAFETY: value is uniquely owned and has not previously been freed.
        unsafe { ValkeyModuleString::release(value) };
    }

    #[test]
    fn module_string_append_keeps_payload_allocation() {
        use crate::engine::extension::module::api::call::ValkeyModule_StringAppendBuffer;
        let value = Box::into_raw(ValkeyModuleString::from_bytes(b"a\0b"));
        let before = super::super::lifecycle_test_support::snapshot();
        // SAFETY: value is a live, uniquely owned string throughout this test.
        unsafe {
            let payload = (*value)._data;
            assert_eq!(
                ValkeyModule_StringAppendBuffer(
                    std::ptr::null_mut(),
                    value,
                    b"x".as_ptr().cast(),
                    1
                ),
                0
            );
            assert_eq!(
                super::super::lifecycle_test_support::snapshot().strings,
                before.strings
            );
            assert_eq!((*value)._data, payload);
            assert_eq!((*value).as_bytes(), b"a\0bx");
            ValkeyModuleString::release(value);
        }
    }
}
