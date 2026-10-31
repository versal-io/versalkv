//! - `command` — built-in category names and dynamic Module category matching

pub mod command;
mod default_user;
mod file;
pub mod permissions;
mod role;
pub mod selector;
pub mod user;

pub(crate) use file::{parse_acl_declarations, parse_acl_file, replace_acl_state, AclFileState};
pub(crate) use role::{acl_roles, apply_role_rule, apply_role_rule_bytes, validate_role_name};

pub(crate) use user::{
    acl_users, compute_db_requirement, default_user_has_nopass, merge_acl_tokens,
    set_default_user_requirepass, AclDenial, AclUser, ACL_READ_PERMISSION, ACL_WRITE_PERMISSION,
};

pub(crate) use selector::{
    acl_log, add_acl_log_entry, context_str, now_ms, reason_str, reset_acl_log, reset_acl_stats,
    set_acl_log_max_len, AclLogEntry, ACL_DENIED_AUTH, ACL_DENIED_CHANNEL, ACL_DENIED_CMD,
    ACL_DENIED_DB, ACL_DENIED_KEY, ACL_DENIED_TLS_CERT, ACL_LOG_CTX_LUA, ACL_LOG_CTX_MODULE,
    ACL_LOG_CTX_MULTI, ACL_LOG_CTX_TOPLEVEL, ACL_STATS_AUTH, ACL_STATS_CHANNEL, ACL_STATS_CMD,
    ACL_STATS_DB, ACL_STATS_KEY,
};

pub(crate) use command::{
    is_builtin_acl_category, is_valid_sha256_hex, module_acl_categories,
    module_acl_category_commands, register_module_acl_category, remove_module_acl_category_all,
    should_restrict_cmd, AclCommand, BUILTIN_ACL_CATEGORY_NAMES,
};

/// Fingerprint effective rules independently of user insertion order.
/// Include each username so equal rules on distinct users do not cancel out.
pub(crate) fn digest() -> Result<String, &'static str> {
    use sha2::{Digest, Sha256};

    let users = acl_users()
        .lock()
        .map_err(|_| "ACL user registry is unavailable")?;
    let mut digest = [0u8; 32];
    for (name, user) in users.iter() {
        let user_digest = Sha256::digest([b"user ".as_slice(), &user.describe(name)].concat());
        for (combined, byte) in digest.iter_mut().zip(user_digest) {
            *combined ^= byte;
        }
    }
    let roles = acl_roles()
        .lock()
        .map_err(|_| "ACL role registry is unavailable")?;
    for (name, role) in roles.iter() {
        let role_digest =
            Sha256::digest([b"role ".as_slice(), &role.describe_role_bytes(name)].concat());
        for (combined, byte) in digest.iter_mut().zip(role_digest) {
            *combined ^= byte;
        }
    }
    Ok(hex::encode(digest))
}
