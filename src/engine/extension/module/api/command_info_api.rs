#![allow(non_snake_case)]

use libc::{c_char, c_int, c_void, size_t};
use std::ffi::CStr;

use super::super::capability::runtime_from_opaque;
use super::super::registry::{
    ModuleCommandHandle, ModuleCommandInfo, ModuleKeyBeginSearch, ModuleKeyFindKeys, ModuleKeySpec,
};
use super::super::{VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::active_registry::with_active_registry;

const BS_INVALID: c_int = 0;
const BS_UNKNOWN: c_int = 1;
const BS_INDEX: c_int = 2;
const BS_KEYWORD: c_int = 3;
const FK_OMITTED: c_int = 0;
const FK_UNKNOWN: c_int = 1;
const FK_RANGE: c_int = 2;
const FK_KEYNUM: c_int = 3;

#[repr(C)]
pub struct ValkeyModuleCommandInfoVersion {
    version: c_int,
    sizeof_historyentry: size_t,
    sizeof_keyspec: size_t,
    sizeof_arg: size_t,
}

#[repr(C)]
pub struct ValkeyModuleCommandInfo {
    version: *const ValkeyModuleCommandInfoVersion,
    summary: *const c_char,
    complexity: *const c_char,
    since: *const c_char,
    history: *mut c_void,
    tips: *const c_char,
    arity: c_int,
    key_specs: *mut ValkeyModuleCommandKeySpec,
    args: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct BeginSearchIndex {
    pos: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct BeginSearchKeyword {
    keyword: *const c_char,
    start_from: c_int,
}

#[repr(C)]
union BeginSearchSpec {
    index: BeginSearchIndex,
    keyword: BeginSearchKeyword,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct FindKeysRange {
    last_key: c_int,
    key_step: c_int,
    limit: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct FindKeysKeyNum {
    key_num_idx: c_int,
    first_key: c_int,
    key_step: c_int,
}

#[repr(C)]
union FindKeysSpec {
    range: FindKeysRange,
    key_num: FindKeysKeyNum,
}

#[repr(C)]
pub struct ValkeyModuleCommandKeySpec {
    notes: *const c_char,
    flags: u64,
    begin_search_type: c_int,
    begin_search: BeginSearchSpec,
    find_keys_type: c_int,
    find_keys: FindKeysSpec,
}

// SAFETY: command and info are pointers supplied by a loaded module and must
// follow the official valkeymodule.h v1 layouts.
pub unsafe extern "C" fn ValkeyModule_SetCommandInfo(
    command: *mut c_void,
    info: *const ValkeyModuleCommandInfo,
) -> c_int {
    unsafe {
        if command.is_null() || info.is_null() || (*info).version.is_null() {
            return VALKEYMODULE_ERR;
        }
        let version = &*(*info).version;
        if version.version != 1
            || version.sizeof_keyspec < std::mem::size_of::<ValkeyModuleCommandKeySpec>()
        {
            return VALKEYMODULE_ERR;
        }

        let handle = &*(command as *mut ModuleCommandHandle);
        let key_specs = match copy_key_specs((*info).key_specs, version.sizeof_keyspec) {
            Some(specs) => specs,
            None => return VALKEYMODULE_ERR,
        };
        let (first_key, last_key, key_step) = legacy_key_range(&key_specs);
        let stored = with_active_registry(|registry| {
            registry.set_command_info(
                &handle.command_name,
                ModuleCommandInfo {
                    key_specs,
                    acl_categories: Vec::new(),
                    legacy_keys: Some((first_key, last_key, key_step)),
                    getkeys_api: false,
                    arity: ((*info).arity != 0).then_some((*info).arity),
                },
            )
        })
        .unwrap_or(false);
        if !stored {
            return VALKEYMODULE_ERR;
        }

        let Some(runtime) = runtime_from_opaque(handle.runtime_handle) else {
            return VALKEYMODULE_ERR;
        };
        let arity = ((*info).arity != 0).then_some((*info).arity);
        if runtime.update_module_command(&handle.command_name, arity, first_key, last_key, key_step)
        {
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: key_specs must be null or reference a BS_INVALID-terminated array whose
// elements are at least element_size bytes and follow the official v1 layout.
unsafe fn copy_key_specs(
    key_specs: *mut ValkeyModuleCommandKeySpec,
    element_size: size_t,
) -> Option<Vec<ModuleKeySpec>> {
    unsafe {
        if key_specs.is_null() {
            return Some(Vec::new());
        }
        let mut result = Vec::new();
        for index in 0..1024usize {
            let ptr = (key_specs as *const u8)
                .add(index.checked_mul(element_size)?)
                .cast::<ValkeyModuleCommandKeySpec>();
            let spec = &*ptr;
            if spec.begin_search_type == BS_INVALID {
                return Some(result);
            }
            result.push(copy_key_spec(spec)?);
        }
        None
    }
}

// SAFETY: spec must use the active union fields selected by its type tags, and
// keyword pointers must reference live NUL-terminated strings when non-null.
unsafe fn copy_key_spec(spec: &ValkeyModuleCommandKeySpec) -> Option<ModuleKeySpec> {
    unsafe {
        let begin_search = match spec.begin_search_type {
            BS_UNKNOWN => ModuleKeyBeginSearch::Unknown,
            BS_INDEX => ModuleKeyBeginSearch::Index {
                pos: spec.begin_search.index.pos,
            },
            BS_KEYWORD => {
                let keyword = spec.begin_search.keyword;
                if keyword.keyword.is_null() {
                    return None;
                }
                ModuleKeyBeginSearch::Keyword {
                    keyword: CStr::from_ptr(keyword.keyword).to_str().ok()?.to_string(),
                    start_from: keyword.start_from,
                }
            }
            _ => return None,
        };
        let find_keys = match spec.find_keys_type {
            FK_OMITTED => ModuleKeyFindKeys::Range {
                last_key: 0,
                key_step: 1,
                limit: 0,
            },
            FK_UNKNOWN => ModuleKeyFindKeys::Unknown,
            FK_RANGE => {
                let range = spec.find_keys.range;
                ModuleKeyFindKeys::Range {
                    last_key: range.last_key,
                    key_step: range.key_step,
                    limit: range.limit,
                }
            }
            FK_KEYNUM => {
                let key_num = spec.find_keys.key_num;
                ModuleKeyFindKeys::KeyNum {
                    key_num_idx: key_num.key_num_idx,
                    first_key: key_num.first_key,
                    key_step: key_num.key_step,
                }
            }
            _ => return None,
        };
        Some(ModuleKeySpec {
            flags: key_flags(spec.flags)?,
            begin_search,
            find_keys,
        })
    }
}

fn key_flags(flags: u64) -> Option<Vec<&'static str>> {
    if (flags & 0xf).count_ones() != 1 {
        return None;
    }
    if (flags & ((1 << 5) | (1 << 6) | (1 << 7))).count_ones() > 1 {
        return None;
    }
    let mapping = [
        (1 << 0, "RO"),
        (1 << 1, "RW"),
        (1 << 2, "OW"),
        (1 << 3, "RM"),
        (1 << 4, "access"),
        (1 << 5, "update"),
        (1 << 6, "insert"),
        (1 << 7, "delete"),
        (1 << 8, "not_key"),
        (1 << 9, "incomplete"),
        (1 << 10, "variable_flags"),
    ];
    Some(
        mapping
            .into_iter()
            .filter_map(|(bit, name)| (flags & bit != 0).then_some(name))
            .collect(),
    )
}

fn legacy_key_range(specs: &[ModuleKeySpec]) -> (i32, i32, i32) {
    let positions: Vec<i32> = specs
        .iter()
        .filter_map(|spec| match spec.begin_search {
            ModuleKeyBeginSearch::Index { pos } => Some(pos),
            ModuleKeyBeginSearch::Keyword { .. } | ModuleKeyBeginSearch::Unknown => None,
        })
        .collect();
    let Some(&first) = positions.first() else {
        return (0, 0, 0);
    };
    if positions.get(1).is_some_and(|second| *second == first + 1) {
        (first, first + 1, 1)
    } else {
        (first, first, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_module_key_flags_in_command_order() {
        assert_eq!(key_flags((1 << 1) | (1 << 5)), Some(vec!["RW", "update"]));
        assert_eq!(key_flags((1 << 0) | (1 << 4)), Some(vec!["RO", "access"]));
        assert_eq!(key_flags((1 << 0) | (1 << 1)), None);
    }

    #[test]
    fn derives_legacy_range_from_single_index_spec() {
        let specs = vec![ModuleKeySpec {
            flags: vec!["RW", "update"],
            begin_search: ModuleKeyBeginSearch::Index { pos: 1 },
            find_keys: ModuleKeyFindKeys::Range {
                last_key: 0,
                key_step: 1,
                limit: 0,
            },
        }];
        assert_eq!(legacy_key_range(&specs), (1, 1, 1));
    }
}
