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
    pub(crate) version: c_int,
    pub(crate) sizeof_historyentry: size_t,
    pub(crate) sizeof_keyspec: size_t,
    pub(crate) sizeof_arg: size_t,
}

#[repr(C)]
pub struct ValkeyModuleCommandInfo {
    pub(crate) version: *const ValkeyModuleCommandInfoVersion,
    pub(crate) summary: *const c_char,
    pub(crate) complexity: *const c_char,
    pub(crate) since: *const c_char,
    pub(crate) history: *mut c_void,
    pub(crate) tips: *const c_char,
    pub(crate) arity: c_int,
    pub(crate) key_specs: *mut ValkeyModuleCommandKeySpec,
    pub(crate) args: *mut c_void,
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
#[derive(Clone, Copy)]
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
#[derive(Clone, Copy)]
union FindKeysSpec {
    range: FindKeysRange,
    key_num: FindKeysKeyNum,
}

#[repr(C)]
#[derive(Clone, Copy)]
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
    // SAFETY: all ABI pointers and terminated arrays are live for this synchronous copy.
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
        let legacy_keys = (!(*info).key_specs.is_null()).then(|| legacy_key_range(&key_specs));
        let arguments = if (*info).args.is_null() {
            None
        } else {
            match super::command_args::copy_command_args((*info).args.cast(), version.sizeof_arg) {
                Some(arguments) => Some(arguments),
                None => return VALKEYMODULE_ERR,
            }
        };
        let update = ModuleCommandInfo {
            summary: copy_optional_string((*info).summary),
            since: copy_optional_string((*info).since),
            key_specs,
            acl_categories: Vec::new(),
            legacy_keys,
            getkeys_api: false,
            arity: ((*info).arity != 0).then_some((*info).arity),
        };
        let prepared = with_active_registry(|registry| {
            registry.prepare_command_metadata(&handle.command_name, update, arguments)
        })
        .flatten();
        let Some(prepared) = prepared else {
            crate::foundation::io::sys::os::set_errno(libc::EEXIST);
            return VALKEYMODULE_ERR;
        };
        let Some(runtime) = runtime_from_opaque(handle.runtime_handle) else {
            return VALKEYMODULE_ERR;
        };
        let (first_key, last_key, key_step) = prepared
            .info
            .as_ref()
            .and_then(|info| info.legacy_keys)
            .unwrap_or((0, 0, 0));
        if !runtime.update_module_command(
            &handle.command_name,
            ((*info).arity != 0).then_some((*info).arity),
            first_key,
            last_key,
            key_step,
        ) {
            return VALKEYMODULE_ERR;
        }
        with_active_registry(|registry| {
            registry.commit_command_metadata(&handle.command_name, prepared);
            VALKEYMODULE_OK
        })
        .unwrap_or(VALKEYMODULE_ERR)
    }
}

// SAFETY: value is null or points to a live NUL-terminated module string.
unsafe fn copy_optional_string(value: *const c_char) -> Option<String> {
    if value.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees a readable string for this synchronous copy.
    Some(
        unsafe { CStr::from_ptr(value) }
            .to_string_lossy()
            .into_owned(),
    )
}

// SAFETY: key_specs must be null or reference a BS_INVALID-terminated array whose
// elements are at least element_size bytes and follow the official v1 layout.
unsafe fn copy_key_specs(
    key_specs: *mut ValkeyModuleCommandKeySpec,
    element_size: size_t,
) -> Option<Vec<ModuleKeySpec>> {
    // SAFETY: every visited element and its active union fields follow the
    // caller's terminated-array contract; offsets are checked before access.
    unsafe {
        if key_specs.is_null() {
            return Some(Vec::new());
        }
        let mut result = Vec::new();
        for index in 0..1024usize {
            let offset = index.checked_mul(element_size)?;
            if offset > isize::MAX as usize {
                return None;
            }
            let spec = (key_specs as *const u8)
                .add(offset)
                .cast::<ValkeyModuleCommandKeySpec>();
            let spec = spec.read_unaligned();
            if spec.begin_search_type == BS_INVALID {
                return Some(result);
            }
            result.push(copy_key_spec(&spec)?);
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
    Some(key_flag_names(flags))
}

pub(super) fn key_flag_names(flags: u64) -> Vec<&'static str> {
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
    mapping
        .into_iter()
        .filter_map(|(bit, name)| (flags & bit != 0).then_some(name))
        .collect()
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
    if let Some(second) = first
        .checked_add(1)
        .filter(|second| positions.get(1) == Some(second))
    {
        (first, second, 1)
    } else {
        (first, first, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::CommandConn;
    use crate::engine::command::{CmdFlags, CommandArgs, CommandDescriptor};
    use crate::engine::extension::module::capability::{
        ModuleClientInfo, ModuleRuntime, ModuleRuntimeHandle,
    };
    use crate::engine::extension::module::{ModuleRef, ModuleRegistry};
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::Db;
    use crate::foundation::io::sys::os::{errno, set_errno};
    use bytes::Bytes;
    use std::ffi::CString;

    struct MetadataRuntime {
        updates: usize,
        arity: i32,
    }

    impl ModuleRuntime for MetadataRuntime {
        fn num_dbs(&self) -> usize {
            0
        }

        fn db_mut(&mut self, _db_idx: u32) -> Option<&mut Db> {
            panic!("metadata updates must not access a database");
        }

        fn flush_all_dbs(&mut self) {
            panic!("metadata updates must not flush databases");
        }

        fn uses_lfu(&self) -> bool {
            false
        }

        fn module_client_id(&self) -> u64 {
            0
        }

        fn module_client_info(&self, _client_id: u64) -> Option<ModuleClientInfo> {
            None
        }

        fn module_client_name(&self, _client_id: u64) -> Option<Bytes> {
            None
        }

        fn set_module_client_name(&mut self, _client_id: u64, _name: Bytes) -> bool {
            panic!("metadata updates must not rename clients");
        }

        fn list_max_listpack_size(&self) -> i64 {
            -2
        }

        fn lookup_command(&self, _name: &[u8]) -> Option<CommandDescriptor> {
            None
        }

        fn execute_command(
            &mut self,
            _name: &[u8],
            _conn: &mut dyn CommandConn,
            _args: &CommandArgs,
        ) -> Value {
            panic!("metadata updates must not execute commands");
        }

        fn register_module_command(
            &mut self,
            _name: &str,
            _arity: i32,
            _flags: CmdFlags,
            _first_key: i32,
            _last_key: i32,
            _key_step: i32,
        ) -> bool {
            panic!("metadata updates must not register commands");
        }

        fn update_module_command(
            &mut self,
            name: &str,
            arity: Option<i32>,
            first_key: i32,
            last_key: i32,
            key_step: i32,
        ) -> bool {
            assert_eq!(name, "metadata-probe.command");
            assert_eq!((first_key, last_key, key_step), (0, 0, 0));
            self.updates += 1;
            if let Some(arity) = arity {
                self.arity = arity;
            }
            true
        }

        fn publish_module_message(
            &mut self,
            _channel: Bytes,
            _message: Bytes,
            _is_shard: bool,
        ) -> Option<usize> {
            panic!("metadata updates must not publish messages");
        }

        fn propagate_module_command(&mut self, _frame: Vec<u8>) -> bool {
            panic!("metadata updates must not propagate commands");
        }
    }

    fn metadata_registry() -> ModuleRegistry {
        let mut registry = ModuleRegistry::new();
        registry
            .register(ModuleRef::new(
                "metadata-probe",
                std::ptr::null_mut(),
                1,
                true,
            ))
            .unwrap();
        registry
            .register_command("metadata-probe", "metadata-probe.command", None)
            .unwrap();
        assert!(registry.set_legacy_command_info("metadata-probe.command", (0, 0, 0), false, -2));
        registry
    }

    fn metadata_version() -> ValkeyModuleCommandInfoVersion {
        ValkeyModuleCommandInfoVersion {
            version: 1,
            sizeof_historyentry: 0,
            sizeof_keyspec: std::mem::size_of::<ValkeyModuleCommandKeySpec>(),
            sizeof_arg: std::mem::size_of::<super::super::command_args::ValkeyModuleCommandArg>(),
        }
    }

    fn empty_metadata(version: &ValkeyModuleCommandInfoVersion) -> ValkeyModuleCommandInfo {
        ValkeyModuleCommandInfo {
            version,
            summary: std::ptr::null(),
            complexity: std::ptr::null(),
            since: std::ptr::null(),
            history: std::ptr::null_mut(),
            tips: std::ptr::null(),
            arity: 0,
            key_specs: std::ptr::null_mut(),
            args: std::ptr::null_mut(),
        }
    }

    fn apply_metadata(
        registry: &mut ModuleRegistry,
        runtime: &mut MetadataRuntime,
        info: &ValkeyModuleCommandInfo,
    ) -> (c_int, c_int) {
        let mut runtime_handle = ModuleRuntimeHandle::new(runtime);
        let mut handle = ModuleCommandHandle {
            module_name: "metadata-probe".into(),
            command_name: "metadata-probe.command".into(),
            runtime_handle: (&mut runtime_handle as *mut ModuleRuntimeHandle).cast(),
        };
        let scope =
            super::super::active_registry::ActiveModuleRegistryScope::enter(registry).unwrap();
        set_errno(0);
        // SAFETY: fixtures retain the v1 info, its version, strings and terminated
        // arrays throughout this call. The handle and recording runtime are live
        // and exclusively accessed; the registry scope is restored on return.
        let result = unsafe {
            ValkeyModule_SetCommandInfo((&mut handle as *mut ModuleCommandHandle).cast(), info)
        };
        let error = errno();
        drop(scope);
        (result, error)
    }

    #[test]
    fn command_metadata_rejects_updates_after_argument_tree() {
        use super::super::command_args::test_support::end_arg;
        use super::super::command_args::ValkeyModuleCommandArg;

        for case in ["summary", "arity", "since", "empty", "args", "empty_args"] {
            let mut registry = metadata_registry();
            let mut runtime = MetadataRuntime {
                updates: 0,
                arity: -2,
            };
            let version = metadata_version();
            let name = CString::new("payload").unwrap();
            let mut args = vec![
                ValkeyModuleCommandArg {
                    name: name.as_ptr(),
                    ..end_arg()
                },
                end_arg(),
            ];
            let mut initial = empty_metadata(&version);
            initial.arity = -3;
            initial.args = args.as_mut_ptr().cast();
            assert_eq!(
                apply_metadata(&mut registry, &mut runtime, &initial).0,
                VALKEYMODULE_OK
            );
            let before = registry
                .lookup_cmd_func("metadata-probe.command")
                .unwrap()
                .clone();
            assert_eq!(before.arguments.as_ref().unwrap().len(), 1);
            drop(args);
            drop(name);

            let summary = CString::new("Replaced documentation.").unwrap();
            let since = CString::new("9.0.0").unwrap();
            let next_name = CString::new("replacement").unwrap();
            let mut next_args = vec![
                ValkeyModuleCommandArg {
                    name: next_name.as_ptr(),
                    ..end_arg()
                },
                end_arg(),
            ];
            let mut update = empty_metadata(&version);
            match case {
                "summary" => update.summary = summary.as_ptr(),
                "arity" => update.arity = -7,
                "since" => update.since = since.as_ptr(),
                "empty" => {}
                "args" => update.args = next_args.as_mut_ptr().cast(),
                "empty_args" => {
                    update.args = (&mut next_args[1] as *mut ValkeyModuleCommandArg).cast()
                }
                _ => unreachable!(),
            }
            let (result, error) = apply_metadata(&mut registry, &mut runtime, &update);
            assert_eq!((result, error), (VALKEYMODULE_ERR, libc::EEXIST), "{case}");
            assert_eq!(runtime.updates, 1, "{case}");
            assert_eq!(runtime.arity, -3, "{case}");
            let after = registry.lookup_cmd_func("metadata-probe.command").unwrap();
            assert_eq!(after.info, before.info, "{case}");
            assert_eq!(after.arguments, before.arguments, "{case}");
        }
    }

    #[test]
    fn command_metadata_allows_legacy_updates_before_first_tree() {
        use super::super::command_args::test_support::end_arg;
        use super::super::command_args::ValkeyModuleCommandArg;

        let mut registry = metadata_registry();
        let mut runtime = MetadataRuntime {
            updates: 0,
            arity: -2,
        };
        let mut version = metadata_version();
        version.sizeof_arg = 0;
        let mut legacy = empty_metadata(&version);
        legacy.arity = -4;
        assert_eq!(
            apply_metadata(&mut registry, &mut runtime, &legacy).0,
            VALKEYMODULE_OK
        );
        assert_eq!(runtime.arity, -4);
        assert_eq!(
            registry
                .lookup_cmd_func("metadata-probe.command")
                .unwrap()
                .arguments,
            None
        );
        version.sizeof_arg = std::mem::size_of::<ValkeyModuleCommandArg>();
        let name = CString::new("payload").unwrap();
        let mut args = vec![
            ValkeyModuleCommandArg {
                name: name.as_ptr(),
                ..end_arg()
            },
            end_arg(),
        ];
        let mut initial = empty_metadata(&version);
        initial.args = args.as_mut_ptr().cast();
        assert_eq!(
            apply_metadata(&mut registry, &mut runtime, &initial).0,
            VALKEYMODULE_OK
        );
        assert_eq!(runtime.updates, 2);
        assert_eq!(runtime.arity, -4);
        let stored = registry.lookup_cmd_func("metadata-probe.command").unwrap();
        assert_eq!(stored.arguments.as_ref().unwrap().len(), 1);
        assert_eq!(stored.info.as_ref().unwrap().arity, Some(-4));
    }

    #[test]
    fn optional_command_documentation_strings_are_owned_and_nullable() {
        use std::ffi::CString;

        let summary = CString::new("Appends a message.").unwrap();
        // SAFETY: summary remains readable until the synchronous copy returns.
        let copied = unsafe { copy_optional_string(summary.as_ptr()) };
        drop(summary);
        assert_eq!(copied.as_deref(), Some("Appends a message."));
        // SAFETY: null is explicitly supported by this optional-string copier.
        assert_eq!(unsafe { copy_optional_string(std::ptr::null()) }, None);
    }

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
