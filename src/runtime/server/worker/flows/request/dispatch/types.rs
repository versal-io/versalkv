use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use bytes::Bytes;
use std::net::SocketAddr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommandClientFacts {
    pub(crate) token: Token,
    pub(crate) selected_db: u32,
    pub(crate) peer_addr: SocketAddr,
    pub(crate) readonly: bool,
    pub(crate) asking: bool,
    pub(crate) monitoring: bool,
    pub(crate) transaction: bool,
    pub(crate) transaction_db: i64,
    pub(crate) username: String,
    pub(crate) import_source: bool,
    pub(crate) migration_control: bool,
    pub(crate) keyless_redirect: bool,
}

impl CommandClientFacts {
    pub(crate) fn capture(connection: &Connection) -> Self {
        Self {
            token: connection.token(),
            selected_db: connection.selected_db,
            peer_addr: connection.peer_addr,
            readonly: connection.readonly,
            asking: connection.asking,
            monitoring: connection.monitoring,
            transaction: connection.transaction.is_active(),
            transaction_db: connection
                .transaction
                .selected_db()
                .unwrap_or(i64::from(connection.selected_db)),
            username: connection.username.clone(),
            import_source: connection.import_source,
            migration_control: connection.slot_migration_role.is_some(),
            keyless_redirect: connection
                .capa_flags
                .iter()
                .any(|capability| capability.eq_ignore_ascii_case("redirect")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DispatchAction {
    Execute,
    Rejected,
    Postpone(PostponeReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostponeReason {
    ClientPause,
    ForklessKey,
    ReplicationThrottle,
    SlotMigration(crate::engine::distributed::cluster::migration::MigrationJobId),
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CmdCallFlags {
    #[allow(dead_code)]
    // TODO(dead_code): used in tests
    pub is_write: bool,
    pub client_asking: bool,
}

pub(crate) struct DispatchOutcome {
    pub action: DispatchAction,
    pub call_flags: CmdCallFlags,
    pub reject_reply: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct CmdPreconditions {
    pub name: crate::engine::command::CommandName,
    pub arity: i32,
    pub flags: u64,
    pub first_key: i32,
    pub last_key: i32,
    pub key_step: i32,
    /// Canonical ACL key-access tokens from the command keyspecs (empty when
    /// the spec carried no R/W requirement). Used by the generic key-permission
    /// check when no `get_keys` function is mounted.
    pub key_flags: &'static [&'static str],
    /// Generated ACL categories for the effective command or subcommand.
    pub acl_categories: &'static [&'static str],
    pub get_keys: Option<crate::framework::GetKeysFn>,
}

pub(super) struct PreflightRequest<'a> {
    pub(super) facts: CommandClientFacts,
    pub(super) meta: &'a CmdPreconditions,
    pub(super) args: &'a CommandArgs,
    pub(super) call_flags: CmdCallFlags,
}

pub(crate) struct CmdLogEntry {
    pub(crate) argv: Vec<Bytes>,
    pub(crate) duration_us: u64,
    pub(crate) peer_addr: SocketAddr,
    pub(crate) client_name: Option<Bytes>,
    pub(crate) req_bytes: usize,
    pub(crate) reply_bytes: usize,
    pub(crate) command_flags: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoopAction {
    Continue,
    Break,
}

#[cfg(test)]
mod client_facts_tests {
    use super::CommandClientFacts;
    use crate::engine::distributed::cluster::migration::{
        MigrationJobId, SlotMigrationConnectionRole,
    };
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use std::net::SocketAddr;

    #[test]
    fn captures_owned_preflight_client_state() {
        let peer_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 6380));
        let mut connection = Connection::new(Token(17), peer_addr, false);
        connection.selected_db = 3;
        connection.readonly = true;
        connection.asking = true;
        connection.monitoring = true;
        assert!(connection.transaction.begin(3, 0));
        connection.import_source = true;
        connection.capa_flags.push("redirect".to_string());
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Import(
            MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        ));

        let facts = CommandClientFacts::capture(&connection);
        connection.selected_db = 9;
        connection.asking = false;

        assert_eq!(facts.token, Token(17));
        assert_eq!(facts.selected_db, 3);
        assert_eq!(facts.peer_addr, peer_addr);
        assert!(facts.readonly);
        assert!(facts.asking);
        assert!(facts.monitoring);
        assert!(facts.transaction);
        assert!(facts.import_source);
        assert!(facts.migration_control);
        assert!(facts.keyless_redirect);
    }
}
