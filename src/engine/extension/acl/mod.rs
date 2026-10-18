//! - `command` — built-in category names and dynamic Module category matching

pub mod command;
mod default_user;
pub mod permissions;
pub mod selector;
pub mod user;

pub(crate) use user::{
    acl_users, compute_db_requirement, default_user_has_nopass, merge_acl_tokens,
    set_default_user_requirepass, AclDenial, AclUser, ACL_READ_PERMISSION, ACL_WRITE_PERMISSION,
};

pub(crate) use selector::{
    acl_log, add_acl_log_entry, context_str, now_ms, reason_str, reset_acl_log,
    set_acl_log_max_len, AclLogEntry, ACL_DENIED_AUTH, ACL_DENIED_CHANNEL, ACL_DENIED_CMD,
    ACL_DENIED_DB, ACL_DENIED_KEY, ACL_LOG_CTX_LUA, ACL_LOG_CTX_MODULE, ACL_LOG_CTX_MULTI,
    ACL_LOG_CTX_TOPLEVEL, ACL_STATS_AUTH, ACL_STATS_CHANNEL, ACL_STATS_CMD, ACL_STATS_DB,
    ACL_STATS_KEY,
};

pub(crate) use command::{
    is_builtin_acl_category, is_valid_sha256_hex, module_acl_categories,
    module_acl_category_commands, register_module_acl_category, remove_module_acl_category_all,
    should_restrict_cmd, AclCommand, BUILTIN_ACL_CATEGORY_NAMES,
};
