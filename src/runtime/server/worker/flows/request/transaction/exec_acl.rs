use super::super::super::ServerWorkerState;
use super::tx_db_object::tx_db_object_and_label;
use crate::engine::command::{CommandArgs, CMD_ALL_DBS, CMD_WRITE};
use crate::engine::extension::acl::{
    add_acl_log_entry, AclCommand, AclDenial, AclUser, ACL_DENIED_CHANNEL, ACL_DENIED_CMD,
    ACL_DENIED_DB, ACL_DENIED_KEY, ACL_LOG_CTX_MULTI,
};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::io::Token;
use crate::framework::CommandSpec;

pub(super) struct ExecAclRequest<'a> {
    pub(super) token: Token,
    pub(super) spec: &'a CommandSpec,
    pub(super) args: &'a CommandArgs,
    pub(super) username: &'a str,
    pub(super) client_info: &'a str,
}

struct ExecAclFailure {
    reason: i32,
    object: String,
    message: &'static str,
}

impl ServerWorkerState {
    pub(super) fn exec_check_acl(&self, request: ExecAclRequest<'_>) -> Option<Value> {
        let dbid = self.connection_registry[request.token].selected_db as i64;
        let num_dbs = self.ctx.num_dbs();
        let users = crate::engine::extension::acl::acl_users().lock().unwrap();
        let user = users.get(request.username)?;
        let failure = exec_acl_failure(&request, user, dbid, num_dbs);
        drop(users);
        let failure = failure?;
        add_acl_log_entry(
            failure.reason,
            ACL_LOG_CTX_MULTI,
            failure.object,
            request.username.to_string(),
            request.client_info.to_string(),
        );
        Some(Value::error(failure.message))
    }
}

fn exec_acl_failure(
    request: &ExecAclRequest<'_>,
    user: &AclUser,
    dbid: i64,
    num_dbs: usize,
) -> Option<ExecAclFailure> {
    let categories = super::super::dispatch::subcmd_flags::effective_acl_categories(
        request.spec.name.as_str(),
        request.spec.acl_categories,
        request.args.args.first().map(|arg| arg.as_ref()),
    );
    let has_keys = request.spec.first_key != 0;
    let db_req = crate::engine::extension::acl::compute_db_requirement(
        request.spec.name.as_str(),
        &request.args.args,
        (request.spec.flags & CMD_ALL_DBS) != 0,
        crate::engine::extension::acl::should_restrict_cmd(
            request.spec.name.as_str(),
            categories,
            has_keys,
        ),
        dbid,
        num_dbs,
    );
    let keys = exec_acl_key_requirements(request.spec, request.args);
    let command = AclCommand::new(request.spec.name.as_str(), &request.args.args, categories);
    user.check_command_perm(command, &db_req, num_dbs, &keys)
        .map(|denial| acl_denial_failure(request, denial, &keys))
        .or_else(|| channel_acl_failure(request, user))
}

fn exec_acl_key_requirements<'a>(spec: &CommandSpec, args: &'a CommandArgs) -> Vec<(&'a [u8], u8)> {
    if spec.first_key == 0 {
        return Vec::new();
    }
    let first = (spec.first_key - 1) as isize;
    let last = if spec.last_key < 0 {
        args.args.len() as isize + spec.last_key as isize
    } else {
        spec.last_key as isize - 1
    };
    let step = spec.key_step.max(1) as isize;
    let required = if (spec.flags & CMD_WRITE) != 0 {
        crate::engine::extension::acl::ACL_WRITE_PERMISSION
    } else {
        crate::engine::extension::acl::ACL_READ_PERMISSION
    };
    (first..=last)
        .step_by(step as usize)
        .filter_map(|index| args.args.get(index as usize))
        .map(|key| (key.as_ref(), required))
        .collect()
}

fn acl_denial_failure(
    request: &ExecAclRequest<'_>,
    denial: AclDenial,
    keys: &[(&[u8], u8)],
) -> ExecAclFailure {
    match denial {
        AclDenial::Db { dbid, arg_index } => {
            let (object, _) =
                tx_db_object_and_label(request.spec.name.as_str(), request.args, arg_index, dbid);
            ExecAclFailure {
                reason: ACL_DENIED_DB,
                object,
                message: "-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission",
            }
        }
        AclDenial::Command => ExecAclFailure {
            reason: ACL_DENIED_CMD,
            object: command_acl_object(request.spec, request.args),
            message: "-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission to execute the command or subcommand",
        },
        AclDenial::Key(index) => ExecAclFailure {
            reason: ACL_DENIED_KEY,
            object: keys
                .get(index)
                .map(|(key, _)| std::str::from_utf8(key).unwrap_or("<binary>"))
                .unwrap_or("<binary>")
                .to_string(),
            message: "-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission to touch the specified keys",
        },
    }
}

fn command_acl_object(spec: &CommandSpec, args: &CommandArgs) -> String {
    let Some(subcommand) = args
        .args
        .first()
        .and_then(|arg| std::str::from_utf8(arg).ok())
    else {
        return spec.name.as_str().to_ascii_lowercase();
    };
    let full_name = format!(
        "{}|{}",
        spec.name.as_str().to_ascii_lowercase(),
        subcommand.to_ascii_lowercase()
    );
    if crate::framework::generated::commands::SUBCOMMAND_NAMES
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&full_name))
    {
        full_name
    } else {
        spec.name.as_str().to_ascii_lowercase()
    }
}

fn channel_acl_failure(request: &ExecAclRequest<'_>, user: &AclUser) -> Option<ExecAclFailure> {
    let command = request.spec.name.as_str();
    let is_channel_command = [
        "publish",
        "spublish",
        "subscribe",
        "ssubscribe",
        "psubscribe",
    ]
    .iter()
    .any(|candidate| command.eq_ignore_ascii_case(candidate));
    if !is_channel_command || user.can_access_channel_for_command(command, &request.args.args) {
        return None;
    }
    let object = request
        .args
        .args
        .first()
        .and_then(|channel| std::str::from_utf8(channel).ok())
        .unwrap_or("<binary>")
        .to_string();
    Some(ExecAclFailure {
        reason: ACL_DENIED_CHANNEL,
        object,
        message: "-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission to access one of the channels used as arguments",
    })
}
