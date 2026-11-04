use crate::engine::command::{CommandArgs, CMD_ALL_DBS, CMD_NO_AUTH};
use crate::engine::extension::acl::{
    add_acl_log_entry, AclCommand, AclDenial, AclUser, ACL_DENIED_CHANNEL, ACL_DENIED_CMD,
    ACL_DENIED_DB, ACL_DENIED_KEY, ACL_LOG_CTX_MULTI, ACL_LOG_CTX_TOPLEVEL,
};
use crate::foundation::io::protocol::Value;

use super::super::super::ServerWorkerState;
use super::{CmdCallFlags, CmdPreconditions, CommandClientFacts, DispatchAction, DispatchOutcome};

struct AclRequest<'a> {
    facts: &'a CommandClientFacts,
    meta: &'a CmdPreconditions,
    args: &'a CommandArgs,
    call_flags: CmdCallFlags,
    log_context: i32,
}

impl<'a> AclRequest<'a> {
    fn new(
        facts: &'a CommandClientFacts,
        meta: &'a CmdPreconditions,
        args: &'a CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Self {
        let log_context = if facts.transaction {
            ACL_LOG_CTX_MULTI
        } else {
            ACL_LOG_CTX_TOPLEVEL
        };
        Self {
            facts,
            meta,
            args,
            call_flags,
            log_context,
        }
    }
}

impl ServerWorkerState {
    pub(crate) fn stage_acl_permissions(
        &mut self,
        facts: &CommandClientFacts,
        meta: &CmdPreconditions,
        args: &CommandArgs,
        call_flags: CmdCallFlags,
    ) -> Option<DispatchOutcome> {
        if (meta.flags & CMD_NO_AUTH) != 0 {
            return None;
        }
        let request = AclRequest::new(facts, meta, args, call_flags);
        let key_requirements = acl_key_requirements(meta, args);
        let users = crate::engine::extension::acl::acl_users().lock().unwrap();
        let user = users.get(&facts.username)?;
        if let Some(denial) = command_denial(user, &request, &key_requirements, self.ctx.num_dbs())
        {
            return Some(acl_denial_outcome(&request, denial, &key_requirements));
        }
        channel_denial_outcome(user, &request)
    }

    pub(super) fn acl_command_object_name(meta_name: &str, args: &CommandArgs) -> String {
        if !args.args.is_empty() {
            if let Ok(sub) = std::str::from_utf8(&args.args[0]) {
                let full_name = format!(
                    "{}|{}",
                    meta_name.to_ascii_lowercase(),
                    sub.to_ascii_lowercase()
                );
                if crate::framework::generated::commands::SUBCOMMAND_NAMES
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&full_name))
                {
                    return full_name;
                }
            }
        }
        meta_name.to_ascii_lowercase().to_string()
    }

    pub(super) fn acl_database_object_name(
        meta_name: &str,
        args: &CommandArgs,
        arg_index: Option<usize>,
    ) -> String {
        if let Some(i) = arg_index {
            if let Some(raw) = args.args.get(i) {
                if let Ok(s) = std::str::from_utf8(raw) {
                    return s.to_string();
                }
            }
        }
        Self::acl_command_object_name(meta_name, args)
    }
}

fn command_denial(
    user: &AclUser,
    request: &AclRequest<'_>,
    key_requirements: &[(&[u8], u8)],
    num_dbs: usize,
) -> Option<AclDenial> {
    let meta = request.meta;
    let db_id = if request.facts.transaction && !meta.name.as_str().eq_ignore_ascii_case("exec") {
        request.facts.transaction_db
    } else {
        request.facts.selected_db as i64
    };
    let restrict = crate::engine::extension::acl::should_restrict_cmd(
        meta.name.as_str(),
        meta.acl_categories,
        meta.first_key != 0 || meta.get_keys.is_some(),
    );
    let db = crate::engine::extension::acl::compute_db_requirement(
        meta.name.as_str(),
        &request.args.args,
        (meta.flags & CMD_ALL_DBS) != 0,
        restrict,
        db_id,
        num_dbs,
    );
    user.check_command_perm(
        AclCommand::new(meta.name.as_str(), &request.args.args, meta.acl_categories),
        &db,
        num_dbs,
        key_requirements,
    )
}

fn acl_key_requirements<'a>(meta: &CmdPreconditions, args: &'a CommandArgs) -> Vec<(&'a [u8], u8)> {
    if let Some(get_keys) = meta.get_keys {
        return get_keys(&args.args)
            .into_iter()
            .map(|(key, flags)| (key, key_flags_to_acl_bits(&flags)))
            .collect();
    }
    if meta.first_key == 0 {
        return Vec::new();
    }
    let first = (meta.first_key - 1) as isize;
    let last = if meta.last_key < 0 {
        args.args.len() as isize + meta.last_key as isize
    } else {
        meta.last_key as isize - 1
    };
    let step = meta.key_step.max(1) as isize;
    let required = if meta.key_flags.is_empty() {
        0
    } else {
        key_flags_to_acl_bits(meta.key_flags)
    };
    (first..=last)
        .step_by(step as usize)
        .filter_map(|index| args.args.get(index as usize))
        .map(|key| (key.as_ref(), required))
        .collect()
}

fn acl_denial_outcome(
    request: &AclRequest<'_>,
    denial: AclDenial,
    key_requirements: &[(&[u8], u8)],
) -> DispatchOutcome {
    let (reason, object, message) = match denial {
        AclDenial::Db { dbid, arg_index } => {
            let object = ServerWorkerState::acl_database_object_name(
                request.meta.name.as_str(),
                request.args,
                arg_index,
            );
            let db = arg_index
                .and_then(|index| request.args.args.get(index))
                .and_then(|raw| std::str::from_utf8(raw).ok())
                .map_or_else(|| dbid.to_string(), str::to_string);
            (
                ACL_DENIED_DB,
                object,
                format!(
                    "NOPERM User {} has no permissions to access database {db}",
                    request.facts.username
                ),
            )
        }
        AclDenial::Command => command_denial_details(request),
        AclDenial::Key(index) => key_denial_details(request, key_requirements, index),
        AclDenial::Channel(index) => {
            let object = request
                .args
                .args
                .get(index)
                .map(|channel| String::from_utf8_lossy(channel).into_owned())
                .unwrap_or_default();
            (
                ACL_DENIED_CHANNEL,
                object,
                format!(
                    "NOPERM User {} has no permissions to access a channel",
                    request.facts.username
                ),
            )
        }
    };
    let client_info = format!("addr={} cmd={object}", request.facts.peer_addr);
    add_acl_log_entry(
        reason,
        request.log_context,
        object,
        request.facts.username.clone(),
        client_info,
    );
    rejected(request.call_flags, message)
}

fn command_denial_details(request: &AclRequest<'_>) -> (i32, String, String) {
    let object =
        ServerWorkerState::acl_command_object_name(request.meta.name.as_str(), request.args);
    let message = format!(
        "NOPERM User {} has no permissions to run the '{}' command",
        request.facts.username, object
    );
    (ACL_DENIED_CMD, object, message)
}

fn key_denial_details(
    request: &AclRequest<'_>,
    key_requirements: &[(&[u8], u8)],
    index: usize,
) -> (i32, String, String) {
    let key = key_requirements
        .get(index)
        .map(|(key, _)| std::str::from_utf8(key).unwrap_or("<binary>"))
        .unwrap_or("<binary>")
        .to_string();
    let message = format!(
        "NOPERM User {} has no permissions to access a key",
        request.facts.username
    );
    (ACL_DENIED_KEY, key, message)
}

fn channel_denial_outcome(user: &AclUser, request: &AclRequest<'_>) -> Option<DispatchOutcome> {
    let command = request.meta.name.as_str();
    let is_channel_command = matches!(
        command.to_ascii_lowercase().as_str(),
        "publish" | "spublish" | "subscribe" | "ssubscribe" | "psubscribe"
    );
    if !is_channel_command || user.can_access_channel_for_command(command, &request.args.args) {
        return None;
    }
    let channel = request
        .args
        .args
        .first()
        .and_then(|value| std::str::from_utf8(value).ok())
        .unwrap_or("<binary>");
    let client_info = format!(
        "addr={} cmd={}",
        request.facts.peer_addr,
        command.to_ascii_lowercase()
    );
    add_acl_log_entry(
        ACL_DENIED_CHANNEL,
        request.log_context,
        channel.to_string(),
        request.facts.username.clone(),
        client_info,
    );
    Some(rejected(
        request.call_flags,
        format!(
            "NOPERM User {} has no permissions to access a channel",
            request.facts.username
        ),
    ))
}

fn rejected(call_flags: CmdCallFlags, message: String) -> DispatchOutcome {
    DispatchOutcome {
        action: DispatchAction::Rejected,
        call_flags,
        reject_reply: Some(Value::error(message)),
    }
}

/// Translate keyspec flag tokens to required ACL permission bits.
pub(super) fn key_flags_to_acl_bits(flags: &[&'static str]) -> u8 {
    crate::framework::spec::key_flags_to_acl_bits(flags)
}
