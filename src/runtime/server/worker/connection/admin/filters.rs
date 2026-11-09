use std::time::Instant;

use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::Connection;
use crate::runtime::server::worker::ServerWorkerState;

use super::helpers::{flag_char_for_conn, is_valid_flag, type_for_conn};

pub(super) fn user_exists(name: &str) -> bool {
    if name == "default" {
        return true;
    }
    let users = match crate::engine::extension::acl::acl_users().try_lock() {
        Ok(users) => users,
        Err(_) => return false,
    };
    users.contains_key(name)
}

fn conn_user(conn: &Connection) -> &str {
    &conn.username
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientTypeFilter {
    Normal,
    Master,
    Replica,
    Pubsub,
}

impl ClientTypeFilter {
    fn parse(value: String) -> Result<Self, Value> {
        match value.as_str() {
            "normal" => Ok(Self::Normal),
            "master" | "primary" => Ok(Self::Master),
            "replica" | "slave" => Ok(Self::Replica),
            "pubsub" => Ok(Self::Pubsub),
            _ => Err(Value::error(format!("ERR Unknown client type '{}'", value))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Master => "master",
            Self::Replica => "replica",
            Self::Pubsub => "pubsub",
        }
    }
}

#[derive(Default)]
struct ClientSelector {
    ids: Vec<usize>,
    name: Option<String>,
    flags: Option<String>,
    client_type: Option<ClientTypeFilter>,
    users: Vec<String>,
    capabilities: Vec<String>,
    db: Option<u32>,
    library_name: Option<String>,
    library_version: Option<String>,
    address: Option<String>,
    ips: Vec<String>,
    local_address: Option<String>,
}

#[derive(Default)]
pub(super) struct ClientFilters {
    include: ClientSelector,
    exclude: ClientSelector,
    max_age_seconds: Option<u64>,
    min_idle_seconds: Option<u64>,
    skip_current: Option<bool>,
}

impl ClientFilters {
    pub(super) fn parse(args: &CommandArgs) -> Result<Self, Value> {
        ClientFilterParser::new(args).parse()
    }

    pub(super) fn for_legacy_address(address: String) -> Self {
        Self {
            include: ClientSelector {
                address: Some(address),
                ..ClientSelector::default()
            },
            ..Self::default()
        }
    }

    fn selector_mut(&mut self, polarity: FilterPolarity) -> &mut ClientSelector {
        match polarity {
            FilterPolarity::Include => &mut self.include,
            FilterPolarity::Exclude => &mut self.exclude,
        }
    }

    fn matches(
        &self,
        conn: &Connection,
        context: ConnectionMatchContext,
        skip_current_default: bool,
    ) -> bool {
        if self.skip_current.unwrap_or(skip_current_default) && context.is_current {
            return false;
        }
        if !self.matches_time_limits(conn, context.now) {
            return false;
        }
        self.include.matches(conn, context.traits, MatchMode::All)
            && !self.exclude.matches(conn, context.traits, MatchMode::Any)
    }

    fn matches_time_limits(&self, conn: &Connection, now: Instant) -> bool {
        let age_matches = self
            .max_age_seconds
            .is_none_or(|minimum| now.duration_since(conn.create_time).as_secs() > minimum);
        let idle_matches = self
            .min_idle_seconds
            .is_none_or(|minimum| now.duration_since(conn.last_interaction).as_secs() >= minimum);
        age_matches && idle_matches
    }
}

#[derive(Clone, Copy)]
enum FilterPolarity {
    Include,
    Exclude,
}

#[derive(Clone, Copy)]
enum TextFilter {
    Name,
    Capability,
    LibraryName,
    LibraryVersion,
    Address,
    Ip,
    LocalAddress,
}

impl TextFilter {
    fn assign(self, selector: &mut ClientSelector, value: String) {
        match self {
            Self::Name => selector.name = Some(value),
            Self::Capability => selector.capabilities.push(value),
            Self::LibraryName => selector.library_name = Some(value),
            Self::LibraryVersion => selector.library_version = Some(value),
            Self::Address => selector.address = Some(value),
            Self::Ip => selector.ips.push(value),
            Self::LocalAddress => selector.local_address = Some(value),
        }
    }
}

#[derive(Clone, Copy)]
enum SelectorFilter {
    Id,
    Text(TextFilter),
    Flags,
    ClientType,
    User,
    Db,
}

const SELECTOR_FILTERS: &[(&str, SelectorFilter)] = &[
    ("id", SelectorFilter::Id),
    ("name", SelectorFilter::Text(TextFilter::Name)),
    ("capa", SelectorFilter::Text(TextFilter::Capability)),
    ("lib-name", SelectorFilter::Text(TextFilter::LibraryName)),
    ("lib-ver", SelectorFilter::Text(TextFilter::LibraryVersion)),
    ("addr", SelectorFilter::Text(TextFilter::Address)),
    ("ip", SelectorFilter::Text(TextFilter::Ip)),
    ("laddr", SelectorFilter::Text(TextFilter::LocalAddress)),
    ("flags", SelectorFilter::Flags),
    ("type", SelectorFilter::ClientType),
    ("user", SelectorFilter::User),
    ("db", SelectorFilter::Db),
];

#[derive(Clone, Copy)]
enum TimeFilter {
    MaxAge,
    Idle,
}

#[derive(Clone, Copy)]
enum FilterKey {
    Selector(FilterPolarity, SelectorFilter),
    Time(TimeFilter),
    SkipCurrent,
}

impl FilterKey {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "maxage" => return Some(Self::Time(TimeFilter::MaxAge)),
            "idle" => return Some(Self::Time(TimeFilter::Idle)),
            "skipme" => return Some(Self::SkipCurrent),
            _ => {}
        }

        let (polarity, name) = raw
            .strip_prefix("not-")
            .map_or((FilterPolarity::Include, raw), |name| {
                (FilterPolarity::Exclude, name)
            });
        SELECTOR_FILTERS
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, filter)| Self::Selector(polarity, *filter))
    }
}

struct ClientFilterParser<'a> {
    args: &'a CommandArgs,
    index: usize,
    filters: ClientFilters,
}

impl<'a> ClientFilterParser<'a> {
    fn new(args: &'a CommandArgs) -> Self {
        Self {
            args,
            index: 1,
            filters: ClientFilters::default(),
        }
    }

    fn parse(mut self) -> Result<ClientFilters, Value> {
        while self.index < self.args.args.len() {
            let raw = String::from_utf8_lossy(&self.args.args[self.index]).to_ascii_lowercase();
            self.index += 1;
            let key = FilterKey::parse(&raw).ok_or_else(syntax_error)?;
            self.parse_filter(key)?;
        }
        Ok(self.filters)
    }

    fn parse_filter(&mut self, key: FilterKey) -> Result<(), Value> {
        match key {
            FilterKey::Selector(polarity, filter) => self.parse_selector(polarity, filter),
            FilterKey::Time(filter) => self.parse_time(filter),
            FilterKey::SkipCurrent => self.parse_skip_current(),
        }
    }

    fn parse_selector(
        &mut self,
        polarity: FilterPolarity,
        filter: SelectorFilter,
    ) -> Result<(), Value> {
        match filter {
            SelectorFilter::Id => self.parse_ids(polarity),
            SelectorFilter::Text(filter) => self.parse_text(polarity, filter),
            SelectorFilter::Flags => self.parse_flags(polarity),
            SelectorFilter::ClientType => self.parse_client_type(polarity),
            SelectorFilter::User => self.parse_user(polarity),
            SelectorFilter::Db => self.parse_db(polarity),
        }
    }

    fn parse_ids(&mut self, polarity: FilterPolarity) -> Result<(), Value> {
        let start = self.index;
        while let Some(value) = self.args.args.get(self.index) {
            let Ok(id) = String::from_utf8_lossy(value).parse::<i64>() else {
                break;
            };
            if id < 1 {
                return Err(Value::error("ERR client id must be greater than 0"));
            }
            self.filters.selector_mut(polarity).ids.push(id as usize);
            self.index += 1;
        }
        if self.index == start {
            return Err(syntax_error());
        }
        Ok(())
    }

    fn parse_text(&mut self, polarity: FilterPolarity, filter: TextFilter) -> Result<(), Value> {
        let value = self.next_value()?;
        filter.assign(self.filters.selector_mut(polarity), value);
        Ok(())
    }

    fn parse_flags(&mut self, polarity: FilterPolarity) -> Result<(), Value> {
        let value = self.next_value()?;
        if !is_valid_flag(&value) {
            let description = match polarity {
                FilterPolarity::Include => "provided filter",
                FilterPolarity::Exclude => "NOT-FLAGS filter",
            };
            return Err(Value::error(format!(
                "ERR Unknown flags found in the {}: {}",
                description, value
            )));
        }
        self.filters.selector_mut(polarity).flags = Some(value);
        Ok(())
    }

    fn parse_client_type(&mut self, polarity: FilterPolarity) -> Result<(), Value> {
        let client_type = ClientTypeFilter::parse(self.next_value()?)?;
        self.filters.selector_mut(polarity).client_type = Some(client_type);
        Ok(())
    }

    fn parse_user(&mut self, polarity: FilterPolarity) -> Result<(), Value> {
        let user = self.next_value()?;
        if !user_exists(&user) {
            return Err(Value::error(format!("ERR No such user '{}'", user)));
        }
        self.filters.selector_mut(polarity).users.push(user);
        Ok(())
    }

    fn parse_db(&mut self, polarity: FilterPolarity) -> Result<(), Value> {
        let db = self
            .next_value()?
            .parse::<u32>()
            .map_err(|_| syntax_error())?;
        self.filters.selector_mut(polarity).db = Some(db);
        Ok(())
    }

    fn parse_time(&mut self, filter: TimeFilter) -> Result<(), Value> {
        let value = self
            .next_value()?
            .parse::<i64>()
            .map_err(|_| Value::error("ERR value is not an integer or out of range"))?;
        match filter {
            TimeFilter::MaxAge if value < 1 => {
                Err(Value::error("ERR maxage must be greater than 0"))
            }
            TimeFilter::Idle if value < 0 => {
                Err(Value::error("ERR idle must be greater than or equal to 0"))
            }
            TimeFilter::MaxAge => {
                self.filters.max_age_seconds = Some(value as u64);
                Ok(())
            }
            TimeFilter::Idle => {
                self.filters.min_idle_seconds = Some(value as u64);
                Ok(())
            }
        }
    }

    fn parse_skip_current(&mut self) -> Result<(), Value> {
        self.filters.skip_current = Some(match self.next_value()?.as_str() {
            "yes" => true,
            "no" => false,
            _ => return Err(syntax_error()),
        });
        Ok(())
    }

    fn next_value(&mut self) -> Result<String, Value> {
        let value = self.args.args.get(self.index).ok_or_else(syntax_error)?;
        self.index += 1;
        Ok(String::from_utf8_lossy(value).to_string())
    }
}

fn syntax_error() -> Value {
    Value::error("ERR syntax error")
}

#[derive(Clone, Copy)]
enum MatchMode {
    All,
    Any,
}

#[derive(Clone, Copy)]
struct ConnectionTraits {
    is_replica: bool,
    is_master_link: bool,
}

#[derive(Clone, Copy)]
struct ConnectionMatchContext {
    traits: ConnectionTraits,
    is_current: bool,
    now: Instant,
}

impl ClientSelector {
    fn matches(&self, conn: &Connection, traits: ConnectionTraits, mode: MatchMode) -> bool {
        let mut criteria = self
            .identity_criteria(conn)
            .into_iter()
            .chain(self.metadata_criteria(conn, traits, mode))
            .chain(self.network_criteria(conn, mode))
            .flatten();
        match mode {
            MatchMode::All => criteria.all(|matches| matches),
            MatchMode::Any => criteria.any(|matches| matches),
        }
    }

    fn identity_criteria(&self, conn: &Connection) -> [Option<bool>; 4] {
        [
            (!self.ids.is_empty()).then(|| self.ids.contains(&(conn.client_id() as usize))),
            self.name
                .as_ref()
                .map(|expected| connection_name_matches(conn, expected)),
            (!self.users.is_empty()).then(|| self.users.iter().any(|user| user == conn_user(conn))),
            self.db.map(|expected| expected == conn.selected_db),
        ]
    }

    fn metadata_criteria(
        &self,
        conn: &Connection,
        traits: ConnectionTraits,
        mode: MatchMode,
    ) -> [Option<bool>; 5] {
        [
            self.flags
                .as_ref()
                .map(|flags| flags_match(conn, traits, flags, mode)),
            self.client_type
                .map(|expected| client_type_matches(conn, traits, expected)),
            (!self.capabilities.is_empty()).then(|| capabilities_match(conn, &self.capabilities)),
            self.library_name
                .as_ref()
                .map(|expected| expected == &conn.lib_name),
            self.library_version
                .as_ref()
                .map(|expected| expected == &conn.lib_ver),
        ]
    }

    fn network_criteria(&self, conn: &Connection, mode: MatchMode) -> [Option<bool>; 3] {
        [
            self.address
                .as_ref()
                .map(|address| address_matches(conn, address, mode)),
            (!self.ips.is_empty()).then(|| {
                let peer_ip = conn.peer_addr.ip().to_string();
                self.ips.iter().any(|ip| ip == &peer_ip)
            }),
            self.local_address
                .as_ref()
                .map(|address| local_address_matches(address)),
        ]
    }
}

fn connection_name_matches(conn: &Connection, expected: &str) -> bool {
    conn.name.as_ref().map_or(expected.is_empty(), |name| {
        String::from_utf8_lossy(name) == expected
    })
}

fn flags_match(
    conn: &Connection,
    traits: ConnectionTraits,
    expected: &str,
    mode: MatchMode,
) -> bool {
    let actual = flag_char_for_conn(conn, traits.is_replica, traits.is_master_link);
    match mode {
        MatchMode::All => expected.chars().all(|flag| actual.contains(flag)),
        MatchMode::Any => expected.chars().any(|flag| actual.contains(flag)),
    }
}

fn client_type_matches(
    conn: &Connection,
    traits: ConnectionTraits,
    expected: ClientTypeFilter,
) -> bool {
    expected.as_str()
        == type_for_conn(
            conn.sub_channel_count,
            conn.psub_channel_count,
            conn.ssub_channel_count,
            traits.is_master_link,
            traits.is_replica,
        )
}

fn capabilities_match(conn: &Connection, expected: &[String]) -> bool {
    expected.iter().any(|capability| {
        capability.is_empty()
            || conn
                .capa_flags
                .iter()
                .any(|actual| actual.starts_with(capability))
    })
}

fn address_matches(conn: &Connection, expected: &str, mode: MatchMode) -> bool {
    let peer = conn.peer_addr.to_string();
    match mode {
        MatchMode::All => peer == expected,
        // Preserve the existing NOT-ADDR behavior; legacy positive addresses
        // must remain exact so malformed `<ip>:` input cannot match by prefix.
        MatchMode::Any => peer.contains(expected),
    }
}

fn local_address_matches(expected: &str) -> bool {
    "127.0.0.1:0".contains(expected)
}

impl ServerWorkerState {
    /// ACL check for CLIENT subcommands.
    /// Returns `Some(true)` if denied (error reply already pushed), `None` if allowed.
    pub(super) fn check_client_acl(
        &mut self,
        connection_token: usize,
        args: &CommandArgs,
        name_lower: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        let username = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .username
        .clone();
        let users = crate::engine::extension::acl::acl_users().lock().unwrap();
        if let Some(user) = users.get(&username) {
            let dbid = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .selected_db as i64;
            let num_dbs = self.ctx.num_dbs();
            let acl_categories = crate::framework::spec::effective_acl_categories(
                name_lower,
                crate::framework::generated::commands::CLIENT_META.acl_categories,
                args.args.first().map(|arg| arg.as_ref()),
            );
            let db_req = crate::engine::extension::acl::compute_db_requirement(
                name_lower,
                &args.args,
                false,
                crate::engine::extension::acl::should_restrict_cmd(
                    name_lower,
                    acl_categories,
                    false,
                ),
                dbid,
                num_dbs,
            );
            let command = crate::engine::extension::acl::AclCommand::new(
                name_lower,
                &args.args,
                acl_categories,
            );
            if !user.can_execute_command_with_args_db(command, &db_req, num_dbs) {
                drop(users);
                let cmd_name = if let Some(sub) = args.args.first() {
                    if let Ok(sub_str) = std::str::from_utf8(sub) {
                        format!("client|{}", sub_str.to_ascii_lowercase())
                    } else {
                        "client".to_string()
                    }
                } else {
                    "client".to_string()
                };
                self.ctx.end_cmd(&cmd_name, true);
                replies.push(if resp3 {
                    serialize_to_vec_resp3(&Value::error(format!(
                        "NOPERM User {} has no permissions to run the '{}' command",
                        username, cmd_name
                    )))
                } else {
                    serialize_to_vec_resp2(&Value::error(format!(
                        "NOPERM User {} has no permissions to run the '{}' command",
                        username, cmd_name
                    )))
                });
                return Some(true);
            }
        }
        drop(users);
        None
    }

    pub(super) fn conn_matches_filters(
        &self,
        conn: &Connection,
        filters: &ClientFilters,
        current_token: usize,
        skip_current_default: bool,
    ) -> bool {
        let current_client_id = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(current_token))
            .map(|current| current.client_id());
        let context = ConnectionMatchContext {
            traits: ConnectionTraits {
                is_replica: self.is_replica_connection(conn.token().0),
                is_master_link: self.is_master_link(conn.token().0),
            },
            is_current: current_client_id == Some(conn.client_id()),
            now: Instant::now(),
        };
        filters.matches(conn, context, skip_current_default)
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientFilters, ConnectionMatchContext, ConnectionTraits, MatchMode};
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use bytes::Bytes;
    use std::time::Instant;

    fn args(parts: &[&str]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect(),
        )
    }

    fn match_context() -> ConnectionMatchContext {
        ConnectionMatchContext {
            traits: ConnectionTraits {
                is_replica: false,
                is_master_link: false,
            },
            is_current: false,
            now: Instant::now(),
        }
    }

    #[test]
    fn parser_groups_positive_and_negative_selectors() {
        let filters = ClientFilters::parse(&args(&[
            "CLIENT", "LIST", "ID", "11", "12", "NOT-ID", "13", "TYPE", "primary", "NOT-TYPE",
            "slave",
        ]))
        .expect("parse CLIENT filters");

        assert_eq!(filters.include.ids, vec![11, 12]);
        assert_eq!(filters.exclude.ids, vec![13]);
        assert_eq!(filters.include.client_type.unwrap().as_str(), "master");
        assert_eq!(filters.exclude.client_type.unwrap().as_str(), "replica");
    }

    #[test]
    fn parser_preserves_numeric_validation_boundaries() {
        let zero_id = ClientFilters::parse(&args(&["CLIENT", "LIST", "ID", "0"]));
        assert_eq!(
            zero_id.err(),
            Some(Value::error("ERR client id must be greater than 0"))
        );

        let zero_maxage = ClientFilters::parse(&args(&["CLIENT", "LIST", "MAXAGE", "0"]));
        assert_eq!(
            zero_maxage.err(),
            Some(Value::error("ERR maxage must be greater than 0"))
        );

        let zero_idle = ClientFilters::parse(&args(&["CLIENT", "LIST", "IDLE", "0"]));
        assert_eq!(
            zero_idle.expect("zero idle is valid").min_idle_seconds,
            Some(0)
        );
    }

    #[test]
    fn negative_flags_reject_when_any_requested_flag_is_present() {
        let mut conn = Connection::new_fake();
        conn.monitoring = true;
        let mut filters = ClientFilters::default();
        filters.exclude.flags = Some("Ob".to_string());

        assert!(filters
            .exclude
            .matches(&conn, match_context().traits, MatchMode::Any));
        assert!(!filters.matches(&conn, match_context(), false));
    }

    #[test]
    fn positive_flags_require_every_requested_flag() {
        let mut conn = Connection::new_fake();
        conn.monitoring = true;
        conn.readonly = true;
        let mut filters = ClientFilters::default();
        filters.include.flags = Some("Or".to_string());
        assert!(filters.matches(&conn, match_context(), false));

        filters.include.flags = Some("Ob".to_string());
        assert!(!filters.matches(&conn, match_context(), false));
    }

    #[test]
    fn shard_only_subscriber_matches_pubsub_type() {
        let mut conn = Connection::new_fake();
        conn.ssub_channel_count = 1;
        let filters = ClientFilters::parse(&args(&["CLIENT", "LIST", "TYPE", "pubsub"]))
            .expect("parse pubsub filter");

        assert!(filters.matches(&conn, match_context(), false));
    }

    #[test]
    fn malformed_legacy_address_does_not_match_by_prefix() {
        let mut conn = Connection::new_fake();
        conn.peer_addr = "127.0.0.1:6379".parse().unwrap();
        let filters = ClientFilters::for_legacy_address("127.0.0.1:".to_string());

        assert!(!filters.matches(&conn, match_context(), false));
    }

    #[test]
    fn explicit_skipme_overrides_the_caller_default() {
        let conn = Connection::new_fake();
        let mut context = match_context();
        context.is_current = true;
        let mut filters = ClientFilters::default();

        assert!(!filters.matches(&conn, context, true));
        filters.skip_current = Some(false);
        assert!(filters.matches(&conn, context, true));
        filters.skip_current = Some(true);
        assert!(!filters.matches(&conn, context, false));
    }
}
