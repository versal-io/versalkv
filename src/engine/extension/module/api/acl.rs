//! ACL APIs exposed to module code.

#![allow(non_snake_case)]

use libc::{c_char, c_int, c_void, size_t};
use std::collections::HashSet;
use std::ffi::{CStr, CString};
use std::sync::{Mutex, OnceLock};

use super::super::types::{ValkeyModuleCtx, ValkeyModuleString, ValkeyModuleUser};
use super::super::{VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::active_registry::active_module_context;
use crate::engine::extension::acl::{
    acl_users, add_acl_log_entry, AclCommand, AclUser, ACL_DENIED_CHANNEL, ACL_DENIED_CMD,
    ACL_DENIED_DB, ACL_DENIED_KEY, ACL_LOG_CTX_MODULE, ACL_READ_PERMISSION, ACL_WRITE_PERMISSION,
};
use crate::engine::extension::module::registry::ModuleCommandHandle;
use crate::engine::extension::module::registry::ModuleCommandInfo;
use bytes::Bytes;

const ACL_LOG_CMD: c_int = 1;
const ACL_LOG_KEY: c_int = 2;
const ACL_LOG_CHANNEL: c_int = 3;
const ACL_LOG_DB: c_int = 4;

fn module_acl_categories() -> &'static Mutex<HashSet<String>> {
    static CATEGORIES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    CATEGORIES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn set_errno(value: i32) {
    crate::foundation::io::sys::os::set_errno(value);
}

// SAFETY: value is null or a live module string supplied by the C module ABI.
unsafe fn module_string_bytes(value: *mut ValkeyModuleString) -> Vec<u8> {
    unsafe {
        if value.is_null() {
            return Vec::new();
        }
        (*value).as_bytes().to_vec()
    }
}

// SAFETY: user is null or points to a module user allocated by this API.
unsafe fn user_ref<'a>(user: *mut ValkeyModuleUser) -> Option<&'a AclUser> {
    unsafe {
        if user.is_null() || (*user).user.is_null() {
            return None;
        }
        Some(&*((*user).user as *const AclUser))
    }
}

// SAFETY: user and its username are null or valid ABI-owned pointers.
unsafe fn user_name(user: *mut ValkeyModuleUser) -> String {
    unsafe {
        if user.is_null() || (*user).username.is_null() {
            return "default".to_string();
        }
        CStr::from_ptr((*user).username)
            .to_string_lossy()
            .into_owned()
    }
}

// SAFETY: The returned pointers own their boxed allocations and C strings.
unsafe fn make_user(name: &str, value: AclUser) -> *mut ValkeyModuleUser {
    let username = CString::new(name).unwrap_or_else(|_| CString::new("default").unwrap());
    Box::into_raw(Box::new(ValkeyModuleUser {
        user: Box::into_raw(Box::new(value)).cast(),
        username: username.into_raw(),
        is_module_user: 0,
    }))
}

// SAFETY: ctx is null or points to a live module context with valid internal fields.
unsafe fn current_name(ctx: *mut ValkeyModuleCtx) -> String {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() || (*(*ctx).internal).username.is_null() {
            return "default".to_string();
        }
        CStr::from_ptr((*(*ctx).internal).username)
            .to_string_lossy()
            .into_owned()
    }
}

// SAFETY: ctx is null or points to a live context whose username pointer is owned here.
unsafe fn set_context_name(ctx: *mut ValkeyModuleCtx, name: &str) {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() {
            return;
        }
        let old = (*(*ctx).internal).username;
        if !old.is_null() {
            let _ = CString::from_raw(old);
        }
        (*(*ctx).internal).username = CString::new(name)
            .unwrap_or_else(|_| CString::new("default").unwrap())
            .into_raw();
    }
}

// SAFETY: ctx is null or points to a live context whose auth-user pointer is owned here.
unsafe fn set_module_auth_user(ctx: *mut ValkeyModuleCtx, name: &str) {
    unsafe {
        if ctx.is_null() || (*ctx).internal.is_null() {
            return;
        }
        let old = (*(*ctx).internal).module_auth_user;
        if !old.is_null() {
            let _ = CString::from_raw(old);
        }
        (*(*ctx).internal).module_auth_user = CString::new(name)
            .unwrap_or_else(|_| CString::new("").unwrap())
            .into_raw();
    }
}

// SAFETY: name is null or a valid NUL-terminated C string supplied by module code.
pub unsafe extern "C" fn ValkeyModule_CreateModuleUser(
    name: *const c_char,
) -> *mut ValkeyModuleUser {
    unsafe {
        if name.is_null() {
            return std::ptr::null_mut();
        }
        let name = CStr::from_ptr(name).to_string_lossy().into_owned();
        let user = make_user(&name, AclUser::new_disabled());
        if !user.is_null() {
            (*user).is_module_user = 1;
        }
        user
    }
}

// SAFETY: user is null or an owned module-user pointer returned by this API.
pub unsafe extern "C" fn ValkeyModule_FreeModuleUser(user: *mut ValkeyModuleUser) {
    unsafe {
        if user.is_null() {
            return;
        }
        let username = user_name(user);
        if (*user).is_module_user != 0 {
            if let Some(ctx) = active_module_context() {
                if !ctx.is_null() && !(*ctx).internal.is_null() {
                    if let Some(runtime) =
                        crate::engine::extension::module::capability::runtime_from_opaque(
                            (*(*ctx).internal).runtime_handle,
                        )
                    {
                        runtime.deauthenticate_client(&username);
                    }
                }
            }
        }
        let user = Box::from_raw(user);
        if !user.user.is_null() {
            let _ = Box::from_raw(user.user as *mut AclUser);
        }
        if !user.username.is_null() {
            let _ = CString::from_raw(user.username);
        }
    }
}

// SAFETY: user is null or a live module-user pointer; acl is null or a valid C string.
pub unsafe extern "C" fn ValkeyModule_SetModuleUserACL(
    user: *mut ValkeyModuleUser,
    acl: *const c_char,
) -> c_int {
    unsafe {
        let (Some(user), false) = (user_ref_mut(user), acl.is_null()) else {
            return VALKEYMODULE_ERR;
        };
        let rule = CStr::from_ptr(acl).to_string_lossy();
        user.apply_rule(&rule)
            .map_or(VALKEYMODULE_ERR, |_| VALKEYMODULE_OK)
    }
}

// SAFETY: user is null or points to an exclusively owned module user.
unsafe fn user_ref_mut<'a>(user: *mut ValkeyModuleUser) -> Option<&'a mut AclUser> {
    unsafe {
        if user.is_null() || (*user).user.is_null() {
            return None;
        }
        Some(&mut *((*user).user as *mut AclUser))
    }
}

// SAFETY: ctx is null or a live module context owned by the current callback.
pub unsafe extern "C" fn ValkeyModule_GetCurrentUserName(
    ctx: *mut ValkeyModuleCtx,
) -> *mut ValkeyModuleString {
    unsafe {
        let name = current_name(ctx);
        super::super::auto_memory::create_auto_string(ctx, name.as_bytes())
    }
}

// SAFETY: name is null or a live module string supplied by module code.
pub unsafe extern "C" fn ValkeyModule_GetModuleUserFromUserName(
    name: *mut ValkeyModuleString,
) -> *mut ValkeyModuleUser {
    unsafe {
        let name = module_string_bytes(name);
        let name = String::from_utf8_lossy(&name).into_owned();
        let users = acl_users().lock().unwrap();
        users
            .get(&name)
            .cloned()
            .map(|user| make_user(&name, user))
            .unwrap_or(std::ptr::null_mut())
    }
}

// SAFETY: ctx and user are null or valid live module ABI objects.
pub unsafe extern "C" fn ValkeyModule_SetContextUser(
    ctx: *mut ValkeyModuleCtx,
    user: *const ValkeyModuleUser,
) {
    unsafe {
        if user.is_null() || (*user).username.is_null() {
            return;
        }
        let name = CStr::from_ptr((*user).username)
            .to_string_lossy()
            .into_owned();
        set_context_name(ctx, &name);
    }
}

// SAFETY: user and argv are validated pointers to live module ACL objects/strings.
pub unsafe extern "C" fn ValkeyModule_ACLCheckCommandPermissions(
    user: *mut ValkeyModuleUser,
    argv: *mut *mut ValkeyModuleString,
    argc: c_int,
) -> c_int {
    unsafe {
        let Some(user) = user_ref(user) else {
            return VALKEYMODULE_ERR;
        };
        if argv.is_null() || argc < 1 {
            return VALKEYMODULE_ERR;
        }
        let parts = (0..argc as usize)
            .map(|i| Bytes::from(module_string_bytes(*argv.add(i))))
            .collect::<Vec<_>>();
        let command_args = crate::engine::command::CommandArgs::from_parts(parts);
        let resolved =
            crate::engine::extension::module::capability::with_active_runtime(|runtime| {
                let descriptor = runtime.lookup_command(command_args.name.as_ref())?;
                let categories = runtime.command_acl_categories(descriptor.clone(), &command_args);
                Some((descriptor, categories))
            })
            .flatten();
        let Some((descriptor, acl_categories)) = resolved else {
            set_errno(libc::ENOENT);
            return VALKEYMODULE_ERR;
        };
        let command = AclCommand::new(descriptor.name.as_str(), &command_args.args, acl_categories);
        if user.can_execute_command_with_args(command) {
            VALKEYMODULE_OK
        } else {
            set_errno(libc::EACCES);
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: user and key are null or live module ABI objects supplied by the caller.
pub unsafe extern "C" fn ValkeyModule_ACLCheckKeyPermissions(
    user: *mut ValkeyModuleUser,
    key: *mut ValkeyModuleString,
    flags: c_int,
) -> c_int {
    unsafe {
        let Some(user) = user_ref(user) else {
            return VALKEYMODULE_ERR;
        };
        let key = module_string_bytes(key);
        let required = key_permission_bits(flags);
        c_int::from(!user.can_access_key_bits(&key, required))
    }
}

fn key_permission_bits(flags: c_int) -> u8 {
    let mut required = 0;
    if flags & (1 << 4) != 0 {
        required |= ACL_READ_PERMISSION;
    }
    if flags & ((1 << 5) | (1 << 6) | (1 << 7)) != 0 {
        required |= ACL_WRITE_PERMISSION;
    }
    required
}

// SAFETY: user and channel are null or live module ABI objects supplied by the caller.
pub unsafe extern "C" fn ValkeyModule_ACLCheckChannelPermissions(
    user: *mut ValkeyModuleUser,
    channel: *mut ValkeyModuleString,
    _flags: c_int,
) -> c_int {
    unsafe {
        let Some(user) = user_ref(user) else {
            return VALKEYMODULE_ERR;
        };
        c_int::from(!user.can_access_channel(&module_string_bytes(channel)))
    }
}

// SAFETY: user, argv, and denial_reason are validated ABI pointers for this call.
pub unsafe extern "C" fn ValkeyModule_ACLCheckPermissions(
    user: *mut ValkeyModuleUser,
    argv: *mut *mut ValkeyModuleString,
    argc: c_int,
    dbid: c_int,
    denial_reason: *mut c_int,
) -> c_int {
    unsafe {
        let Some(user) = user_ref(user) else {
            return VALKEYMODULE_ERR;
        };
        if argv.is_null() || argc < 1 || dbid < 0 {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let parts = (0..argc as usize)
            .map(|i| Bytes::from(module_string_bytes(*argv.add(i))))
            .collect::<Vec<_>>();
        let command_args = crate::engine::command::CommandArgs::from_parts(parts);
        let resolved =
            crate::engine::extension::module::capability::with_active_runtime(|runtime| {
                let descriptor = runtime.lookup_command(command_args.name.as_ref())?;
                let categories = runtime.command_acl_categories(descriptor.clone(), &command_args);
                Some((descriptor, categories, runtime.num_dbs()))
            })
            .flatten();
        let Some((descriptor, acl_categories, num_dbs)) = resolved else {
            // C VM_ACLCheckPermissions reports every invalid input, including
            // an unknown command, as EINVAL. ENOENT belongs to the older
            // VM_ACLCheckCommandPermissions API only.
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        };
        let name = descriptor.name.as_str();
        let args = &command_args.args;
        if dbid < 0 || dbid as usize >= num_dbs {
            set_errno(libc::EINVAL);
            return VALKEYMODULE_ERR;
        }
        let extracted_keys = descriptor.extract_keys(&command_args).unwrap_or_default();
        let db_req = crate::engine::extension::acl::compute_db_requirement(
            name,
            args,
            descriptor.flags & crate::engine::command::CMD_ALL_DBS != 0,
            crate::engine::extension::acl::should_restrict_cmd(
                name,
                acl_categories,
                !extracted_keys.is_empty(),
            ),
            dbid as i64,
            num_dbs,
        );
        let required = if descriptor.flags & crate::engine::command::CMD_WRITE != 0 {
            ACL_WRITE_PERMISSION
        } else {
            ACL_READ_PERMISSION
        };
        let key_requirements = extracted_keys
            .iter()
            .map(|key| (key.as_ref(), required))
            .collect::<Vec<_>>();
        let command = AclCommand::new(name, args, acl_categories);
        let denial = user.check_command_perm(command, &db_req, num_dbs, &key_requirements);
        if name.eq_ignore_ascii_case("publish") {
            if let Some(channel) = args.first() {
                if !user.can_access_channel(channel) {
                    set_errno(libc::EACCES);
                    if !denial_reason.is_null() {
                        *denial_reason = ACL_LOG_CHANNEL;
                    }
                    return VALKEYMODULE_ERR;
                }
            }
        }
        if let Some(reason) = denial {
            set_errno(libc::EACCES);
            if !denial_reason.is_null() {
                *denial_reason = match reason {
                    crate::engine::extension::acl::AclDenial::Db { .. } => ACL_LOG_DB,
                    crate::engine::extension::acl::AclDenial::Command => ACL_LOG_CMD,
                    crate::engine::extension::acl::AclDenial::Key(_) => ACL_LOG_KEY,
                };
            }
            VALKEYMODULE_ERR
        } else {
            VALKEYMODULE_OK
        }
    }
}

// SAFETY: user and key are validated pointers; key points to len bytes from the caller.
pub unsafe extern "C" fn ValkeyModule_ACLCheckKeyPrefixPermissions(
    user: *mut ValkeyModuleUser,
    key: *const c_char,
    len: size_t,
    flags: c_int,
) -> c_int {
    unsafe {
        let Some(user) = user_ref(user) else {
            return VALKEYMODULE_ERR;
        };
        if key.is_null() {
            return VALKEYMODULE_ERR;
        }
        let key = std::slice::from_raw_parts(key.cast::<u8>(), len);
        let required = key_permission_bits(flags);
        c_int::from(
            !user.root_selector.has_unrestricted_key_access(required)
                && !user.can_access_key_bits(key, required),
        )
    }
}

// SAFETY: ctx, user, and object are null or live module ABI objects for this call.
pub unsafe extern "C" fn ValkeyModule_ACLAddLogEntry(
    ctx: *mut ValkeyModuleCtx,
    user: *mut ValkeyModuleUser,
    object: *mut ValkeyModuleString,
    reason: c_int,
) {
    unsafe {
        let name = user_name(user);
        let object = String::from_utf8_lossy(&module_string_bytes(object)).into_owned();
        let reason = match reason {
            ACL_LOG_CMD => ACL_DENIED_CMD,
            ACL_LOG_KEY => ACL_DENIED_KEY,
            ACL_LOG_CHANNEL => ACL_DENIED_CHANNEL,
            ACL_LOG_DB => ACL_DENIED_DB,
            _ => crate::engine::extension::acl::ACL_DENIED_AUTH,
        };
        let _ = ctx;
        add_acl_log_entry(
            reason,
            ACL_LOG_CTX_MODULE,
            object,
            name,
            "module".to_string(),
        );
    }
}

// SAFETY: ctx and user are validated live module objects; callback pointers follow the ABI.
pub unsafe extern "C" fn ValkeyModule_AuthenticateClientWithUser(
    ctx: *mut ValkeyModuleCtx,
    user: *mut ValkeyModuleUser,
    callback: *mut c_void,
    privdata: *mut c_void,
    client_id: *mut u64,
) -> c_int {
    // SAFETY: the function validates nullable outputs before writing and the
    // caller contract keeps ctx/name and callback data live for this call.
    unsafe {
        if user.is_null() || (*user).username.is_null() {
            return VALKEYMODULE_ERR;
        }
        let name = user_name(user);
        set_context_name(ctx, &name);
        set_module_auth_user(ctx, &name);
        let client_id_value = (*ctx)
            .internal
            .as_ref()
            .and_then(|internal| {
                crate::engine::extension::module::capability::runtime_from_opaque(
                    internal.runtime_handle,
                )
            })
            .and_then(|runtime| runtime.authenticate_client(&name, callback, privdata));
        if let Some(client_id_value) = client_id_value {
            if !client_id.is_null() {
                *client_id = client_id_value;
            }
        }
        VALKEYMODULE_OK
    }
}

// SAFETY: ctx is live and name points to len bytes supplied by module code.
pub unsafe extern "C" fn ValkeyModule_AuthenticateClientWithACLUser(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
    len: size_t,
    callback: *mut c_void,
    privdata: *mut c_void,
    client_id: *mut u64,
) -> c_int {
    // SAFETY: name points to len readable bytes and ctx/callback/output pointers
    // follow the synchronous module authentication ABI contract.
    unsafe {
        if name.is_null() {
            return VALKEYMODULE_ERR;
        }
        let name = String::from_utf8_lossy(std::slice::from_raw_parts(name.cast::<u8>(), len));
        if acl_users().lock().unwrap().contains_key(name.as_ref()) {
            let name = name.into_owned();
            set_context_name(ctx, &name);
            set_module_auth_user(ctx, &name);
            let client_id_value = (*ctx)
                .internal
                .as_ref()
                .and_then(|internal| {
                    crate::engine::extension::module::capability::runtime_from_opaque(
                        internal.runtime_handle,
                    )
                })
                .and_then(|runtime| runtime.authenticate_client(&name, callback, privdata));
            if let Some(client_id_value) = client_id_value {
                if !client_id.is_null() {
                    *client_id = client_id_value;
                }
            }
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: command and categories are null or live ABI pointers owned by the module call.
pub unsafe extern "C" fn ValkeyModule_SetCommandACLCategories(
    command: *mut c_void,
    categories: *const c_char,
) -> c_int {
    unsafe {
        if command.is_null() || categories.is_null() {
            return VALKEYMODULE_ERR;
        }
        let handle = &*(command as *mut ModuleCommandHandle);
        let Some(ctx) = active_module_context() else {
            return VALKEYMODULE_ERR;
        };
        if (*ctx).internal.is_null() || (*(*ctx).internal).onload == 0 {
            return VALKEYMODULE_ERR;
        }
        let categories = CStr::from_ptr(categories)
            .to_string_lossy()
            .split_whitespace()
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>();
        let Some(runtime) = crate::engine::extension::module::capability::runtime_from_opaque(
            handle.runtime_handle,
        ) else {
            return VALKEYMODULE_ERR;
        };
        let info = ModuleCommandInfo {
            acl_categories: categories,
            key_specs: Vec::new(),
            legacy_keys: None,
            getkeys_api: false,
            arity: None,
        };
        for category in &info.acl_categories {
            crate::engine::extension::acl::register_module_acl_category(
                category,
                &handle.command_name,
            );
        }
        let updated = runtime.set_module_command_info(&handle.command_name, info);
        if updated {
            VALKEYMODULE_OK
        } else {
            VALKEYMODULE_ERR
        }
    }
}

// SAFETY: ctx is a live module context and name is null or a valid C string.
pub unsafe extern "C" fn ValkeyModule_AddACLCategory(
    ctx: *mut ValkeyModuleCtx,
    name: *const c_char,
) -> c_int {
    if ctx.is_null() || name.is_null() {
        return VALKEYMODULE_ERR;
    }
    // SAFETY: ctx is non-null and the ABI contract guarantees its internal pointer.
    let internal = unsafe { (*ctx).internal };
    // SAFETY: internal belongs to the validated live module context.
    if internal.is_null() || unsafe { (*internal).onload == 0 } {
        return VALKEYMODULE_ERR;
    }
    // SAFETY: name is a non-null NUL-terminated C string supplied by the module.
    let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        set_errno(libc::EINVAL);
        return VALKEYMODULE_ERR;
    }
    let builtin = crate::engine::extension::acl::is_builtin_acl_category(&name);
    let mut custom = module_acl_categories().lock().unwrap();
    if builtin || custom.contains(name.as_ref()) {
        set_errno(libc::EBUSY);
        return VALKEYMODULE_ERR;
    }
    let normalized = name.to_ascii_lowercase();
    let custom_limit =
        64usize.saturating_sub(crate::engine::extension::acl::BUILTIN_ACL_CATEGORY_NAMES.len());
    if custom.len() >= custom_limit {
        set_errno(libc::ENOMEM);
        return VALKEYMODULE_ERR;
    }
    custom.insert(normalized.clone());
    if let Some(registry) = super::active_registry::active_registry_ptr() {
        // SAFETY: ctx is live for this callback and registry is installed by
        // the synchronous OnLoad scope.
        let module_name = unsafe { super::ctx::get_module_name(ctx) };
        // SAFETY: the active registry remains exclusively borrowed for OnLoad.
        unsafe {
            (&mut *registry).add_acl_category(&module_name, &normalized);
        }
    }
    VALKEYMODULE_OK
}

pub(crate) fn remove_module_acl_category_all(category: &str) {
    module_acl_categories().lock().unwrap().remove(category);
    crate::engine::extension::acl::remove_module_acl_category_all(category);
}
