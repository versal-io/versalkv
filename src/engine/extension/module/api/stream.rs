//! Stream APIs exposed to module code.

#![allow(non_snake_case)]

use libc::{c_int, c_longlong};

use super::super::{
    ModuleStreamIterator, ValkeyModuleCtx, ValkeyModuleKey, ValkeyModuleStreamID,
    ValkeyModuleString, VALKEYMODULE_ERR, VALKEYMODULE_OK,
};
use crate::foundation::io::store::structure::stream::StreamID;

const STREAM_ADD_AUTOID: c_int = 1;
const STREAM_ITERATOR_REVERSE: c_int = 1 << 1;
const STREAM_TRIM_APPROX: c_int = 1;

fn set_errno(value: i32) {
    crate::foundation::io::sys::os::set_errno(value);
}

fn id_to_stream(id: &ValkeyModuleStreamID) -> StreamID {
    StreamID::new(id.ms, id.seq)
}

// SAFETY: out is null or points to writable stream-ID storage supplied by the caller.
unsafe fn stream_id_to_c(id: StreamID, out: *mut ValkeyModuleStreamID) {
    unsafe {
        if !out.is_null() {
            (*out).ms = id.ms;
            (*out).seq = id.seq;
        }
    }
}

// SAFETY: id is null or points to a valid stream ID supplied by module code.
pub unsafe extern "C" fn ValkeyModule_CreateStringFromStreamID(
    ctx: *mut ValkeyModuleCtx,
    id: *const ValkeyModuleStreamID,
) -> *mut ValkeyModuleString {
    unsafe {
        if id.is_null() {
            return std::ptr::null_mut();
        }
        let text = format!("{}-{}", (*id).ms, (*id).seq);
        super::super::auto_memory::create_auto_string(ctx, text.as_bytes())
    }
}

// SAFETY: string and id are null or valid module ABI pointers for this call.
pub unsafe extern "C" fn ValkeyModule_StringToStreamID(
    string: *const ValkeyModuleString,
    id: *mut ValkeyModuleStreamID,
) -> c_int {
    unsafe {
        if string.is_null() || id.is_null() {
            return VALKEYMODULE_ERR;
        }
        let text = match std::str::from_utf8((*string).as_bytes()) {
            Ok(text) => text,
            Err(_) => return VALKEYMODULE_ERR,
        };
        if text == "-" {
            (*id).ms = 0;
            (*id).seq = 0;
            return VALKEYMODULE_OK;
        }
        if text == "+" {
            (*id).ms = u64::MAX;
            (*id).seq = u64::MAX;
            return VALKEYMODULE_OK;
        }
        let Some((ms, seq)) = text.split_once('-') else {
            return VALKEYMODULE_ERR;
        };
        let (Ok(ms), Ok(seq)) = (ms.parse::<u64>(), seq.parse::<u64>()) else {
            return VALKEYMODULE_ERR;
        };
        (*id).ms = ms;
        (*id).seq = seq;
        VALKEYMODULE_OK
    }
}

// SAFETY: key, id, argv, and their pointees are validated module ABI objects.
pub unsafe extern "C" fn ValkeyModule_StreamAdd(
    key: *mut ValkeyModuleKey,
    flags: c_int,
    id: *mut ValkeyModuleStreamID,
    argv: *mut *mut ValkeyModuleString,
    num_fields: c_longlong,
) -> c_int {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            return VALKEYMODULE_ERR;
        };
        if handle.db.is_null() || handle.mode & 2 == 0 || num_fields < 0 {
            return VALKEYMODULE_ERR;
        }
        if flags & !STREAM_ADD_AUTOID != 0 || flags & STREAM_ADD_AUTOID == 0 && id.is_null() {
            return VALKEYMODULE_ERR;
        }
        if num_fields > 0 && argv.is_null() {
            return VALKEYMODULE_ERR;
        }
        let mut fields = Vec::with_capacity(num_fields as usize);
        for index in 0..num_fields as usize {
            let field = *argv.add(index * 2);
            let value = *argv.add(index * 2 + 1);
            if field.is_null() || value.is_null() {
                return VALKEYMODULE_ERR;
            }
            fields.push((
                bytes::Bytes::copy_from_slice((*field).as_bytes()),
                bytes::Bytes::copy_from_slice((*value).as_bytes()),
            ));
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let stream = match db.get_or_create_stream(&handle.key) {
            Ok(stream) => stream,
            Err(_) => return VALKEYMODULE_ERR,
        };
        let explicit = if id.is_null() {
            StreamID::zero()
        } else {
            id_to_stream(&*id)
        };
        let Ok(added) = stream.append(
            explicit,
            fields,
            flags & STREAM_ADD_AUTOID == 0,
            flags & STREAM_ADD_AUTOID != 0,
            crate::foundation::io::sys::time::mstime() as u64,
        ) else {
            return VALKEYMODULE_ERR;
        };
        stream_id_to_c(added, id);
        VALKEYMODULE_OK
    }
}

// SAFETY: key and id are null or valid live module ABI objects.
pub unsafe extern "C" fn ValkeyModule_StreamDelete(
    key: *mut ValkeyModuleKey,
    id: *mut ValkeyModuleStreamID,
) -> c_int {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            return VALKEYMODULE_ERR;
        };
        if handle.db.is_null() || handle.mode & 2 == 0 || id.is_null() {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let Ok(Some(stream)) = db.get_stream_mut(&handle.key) else {
            set_errno(libc::ENOTSUP);
            return VALKEYMODULE_ERR;
        };
        if stream.delete(&id_to_stream(&*id)) {
            VALKEYMODULE_OK
        } else {
            set_errno(libc::ENOENT);
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: key, start, and end are null or valid live module ABI objects.
pub unsafe extern "C" fn ValkeyModule_StreamIteratorStart(
    key: *mut ValkeyModuleKey,
    flags: c_int,
    start: *mut ValkeyModuleStreamID,
    end: *mut ValkeyModuleStreamID,
) -> c_int {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        if handle.db.is_null()
            || flags & !(STREAM_ITERATOR_REVERSE | 1) != 0
            || handle.stream_iterator.is_some()
        {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let Ok(Some(stream)) = db.get_stream_mut(&handle.key) else {
            set_errno(libc::ENOTSUP);
            return VALKEYMODULE_ERR;
        };
        let lower = if start.is_null() {
            StreamID::zero()
        } else {
            id_to_stream(&*start)
        };
        let upper = if end.is_null() {
            StreamID::max()
        } else {
            id_to_stream(&*end)
        };
        let entries = stream.range(
            &lower,
            &upper,
            flags & 1 != 0,
            flags & 1 != 0,
            None,
            flags & STREAM_ITERATOR_REVERSE != 0,
        );
        handle.stream_iterator = Some(ModuleStreamIterator {
            entries,
            index: 0,
            current: None,
            field_index: 0,
        });
        VALKEYMODULE_OK
    }
}

// SAFETY: key is null or a live module key handle owned by the caller.
pub unsafe extern "C" fn ValkeyModule_StreamIteratorStop(key: *mut ValkeyModuleKey) -> c_int {
    let Some(handle) = ValkeyModuleKey::as_handle(key) else {
        set_errno(libc::EINVAL);
        return VALKEYMODULE_ERR;
    };
    if handle.stream_iterator.take().is_some() {
        VALKEYMODULE_OK
    } else {
        set_errno(libc::EBADF);
        VALKEYMODULE_ERR
    }
}

// SAFETY: key is live and id/num_fields are null or writable caller storage.
pub unsafe extern "C" fn ValkeyModule_StreamIteratorNextID(
    key: *mut ValkeyModuleKey,
    id: *mut ValkeyModuleStreamID,
    num_fields: *mut i64,
) -> c_int {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        let Some(iterator) = handle.stream_iterator.as_mut() else {
            set_errno(libc::EBADF);
            return VALKEYMODULE_ERR;
        };
        let Some(entry) = iterator.entries.get(iterator.index).cloned() else {
            iterator.current = None;
            set_errno(libc::ENOENT);
            return VALKEYMODULE_ERR;
        };
        iterator.index += 1;
        iterator.field_index = 0;
        stream_id_to_c(entry.id, id);
        if !num_fields.is_null() {
            *num_fields = entry.fields.len() as i64;
        }
        iterator.current = Some(entry);
        VALKEYMODULE_OK
    }
}

// SAFETY: key is live and field/value are null or writable module-string slots.
pub unsafe extern "C" fn ValkeyModule_StreamIteratorNextField(
    key: *mut ValkeyModuleKey,
    field: *mut *mut ValkeyModuleString,
    value: *mut *mut ValkeyModuleString,
) -> c_int {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        let Some(iterator) = handle.stream_iterator.as_mut() else {
            set_errno(libc::EBADF);
            return VALKEYMODULE_ERR;
        };
        let Some(entry) = iterator.current.as_ref() else {
            set_errno(libc::ENOENT);
            return VALKEYMODULE_ERR;
        };
        let Some((field_bytes, value_bytes)) = entry.fields.get(iterator.field_index) else {
            set_errno(libc::ENOENT);
            return VALKEYMODULE_ERR;
        };
        iterator.field_index += 1;
        if !field.is_null() {
            *field = super::super::auto_memory::create_string_for_owner(handle.owner, field_bytes);
        }
        if !value.is_null() {
            *value = super::super::auto_memory::create_string_for_owner(handle.owner, value_bytes);
        }
        VALKEYMODULE_OK
    }
}

// SAFETY: key is null or a live module key handle owned by the caller.
pub unsafe extern "C" fn ValkeyModule_StreamIteratorDelete(key: *mut ValkeyModuleKey) -> c_int {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        if handle.db.is_null() || handle.mode & 2 == 0 {
            set_errno(libc::EBADF);
            return VALKEYMODULE_ERR;
        }
        let Some(entry) = handle
            .stream_iterator
            .as_ref()
            .and_then(|iterator| iterator.current.as_ref())
        else {
            set_errno(libc::ENOENT);
            return VALKEYMODULE_ERR;
        };
        let id = entry.id;
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let Ok(Some(stream)) = db.get_stream_mut(&handle.key) else {
            return VALKEYMODULE_ERR;
        };
        if stream.delete(&id) {
            VALKEYMODULE_OK
        } else {
            set_errno(libc::ENOENT);
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: key is null or a live writable module key handle.
pub unsafe extern "C" fn ValkeyModule_StreamTrimByLength(
    key: *mut ValkeyModuleKey,
    flags: c_int,
    length: i64,
) -> i64 {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            return -1;
        };
        if handle.db.is_null() || handle.mode & 2 == 0 {
            return -1;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let Ok(Some(stream)) = db.get_stream_mut(&handle.key) else {
            return -1;
        };
        stream.trim_by_length_limit(
            length.max(0) as usize,
            flags & STREAM_TRIM_APPROX != 0,
            None,
            100,
        ) as i64
    }
}

// SAFETY: key and id are null or valid live writable module ABI objects.
pub unsafe extern "C" fn ValkeyModule_StreamTrimByID(
    key: *mut ValkeyModuleKey,
    flags: c_int,
    id: *mut ValkeyModuleStreamID,
) -> i64 {
    unsafe {
        let Some(handle) = ValkeyModuleKey::as_handle(key) else {
            return -1;
        };
        if handle.db.is_null() || handle.mode & 2 == 0 || id.is_null() {
            return -1;
        }
        let db = &mut *(handle.db as *mut crate::foundation::io::store::kv_engine::Db);
        let Ok(Some(stream)) = db.get_stream_mut(&handle.key) else {
            return -1;
        };
        stream.trim_by_id_limit(
            &id_to_stream(&*id),
            flags & STREAM_TRIM_APPROX != 0,
            None,
            100,
        ) as i64
    }
}
