use std::collections::{HashMap, HashSet};

use crate::foundation::io::protocol::parser::parse_inline_args;

use super::state::SentinelRegistry;
use super::{format_sentinel_endpoint, split_sentinel_endpoint, InstanceRole};

const UNRECOGNIZED: &str = "Unrecognized sentinel configuration statement.";
const NO_SUCH_MASTER: &str = "No such master with specified name.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentinelConfigScriptKind {
    Notification,
    ClientReconfig,
}

pub trait SentinelConfigScriptValidator {
    fn validate_script(&self, kind: SentinelConfigScriptKind, path: &str) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub(super) struct SentinelConfigPlan {
    pub pre: Vec<PreMonitorDirective>,
    pub monitors: Vec<MonitorDirective>,
    pub post: Vec<PostMonitorDirective>,
}

#[derive(Debug, Clone)]
pub(super) enum PreMonitorDirective {
    MyId(String),
    CurrentEpoch(i64),
    DenyScriptsReconfig(bool),
    SentinelUser(String),
    SentinelPass(String),
    AnnounceIp(String),
    AnnouncePort(i32),
    ResolveHostnames(bool),
    AnnounceHostnames(bool),
}

#[derive(Debug, Clone)]
pub(super) struct MonitorDirective {
    pub name: String,
    pub addr: String,
    pub quorum: i64,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum MasterNumberField {
    DownAfterMs,
    FailoverTimeout,
    ParallelSyncs,
    PrimaryRebootDownAfter,
    ConfigEpoch,
    LeaderEpoch,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum MasterTextField {
    NotificationScript,
    ClientReconfigScript,
    AuthPass,
    AuthUser,
}

#[derive(Debug, Clone)]
pub(super) enum PostMonitorDirective {
    Number {
        name: String,
        field: MasterNumberField,
        value: i64,
    },
    Text {
        name: String,
        field: MasterTextField,
        value: String,
    },
    RenameCommand {
        name: String,
        source: String,
        target: String,
    },
    KnownReplica {
        name: String,
        addr: String,
    },
    KnownSentinel {
        name: String,
        addr: String,
        runid: String,
    },
    Ignore,
}

#[derive(Default)]
struct RawConfigPlan {
    pre: Vec<String>,
    monitors: Vec<String>,
    post: Vec<String>,
}

#[derive(Debug, Clone)]
enum ParsedPostDirective {
    Number {
        name: String,
        field: MasterNumberField,
        value: i64,
    },
    Text {
        name: String,
        field: MasterTextField,
        value: String,
    },
    RenameCommand {
        name: String,
        source: String,
        target: String,
    },
    KnownReplica {
        name: String,
        host: String,
        port: i64,
    },
    KnownSentinel {
        name: String,
        host: String,
        port: i64,
        runid: String,
    },
    Ignore,
}

impl SentinelConfigPlan {
    pub fn build(
        lines: &[String],
        registry: &SentinelRegistry,
        current_resolve_hostnames: bool,
        script_validator: &dyn SentinelConfigScriptValidator,
    ) -> Result<Self, String> {
        let raw = partition_directives(lines);
        let pre = parse_pre_stage(&raw.pre)?;
        let resolve_hostnames = prospective_resolve_hostnames(&pre, current_resolve_hostnames);
        let (monitors, masters) = parse_monitor_stage(&raw.monitors, registry, resolve_hostnames)?;
        let post = parse_post_stage(
            &raw.post,
            registry,
            &masters,
            resolve_hostnames,
            script_validator,
        )?;
        Ok(Self {
            pre,
            monitors,
            post,
        })
    }
}

fn partition_directives(lines: &[String]) -> RawConfigPlan {
    let mut plan = RawConfigPlan::default();
    for line in lines {
        let name = directive_name(line);
        if name == "monitor" {
            plan.monitors.push(line.clone());
        } else if is_pre_name(&name) {
            plan.pre.push(line.clone());
        } else {
            plan.post.push(line.clone());
        }
    }
    plan
}

fn directive_name(line: &str) -> String {
    line.split_ascii_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn parse_pre_stage(lines: &[String]) -> Result<Vec<PreMonitorDirective>, String> {
    lines
        .iter()
        .map(|line| {
            let args = tokenize(line)?;
            let name = directive_name(line);
            parse_pre(&name, &args)
        })
        .collect()
}

fn parse_monitor_stage(
    lines: &[String],
    registry: &SentinelRegistry,
    resolve_hostnames: bool,
) -> Result<(Vec<MonitorDirective>, HashSet<String>), String> {
    let mut masters: HashSet<String> = registry.master_names().cloned().collect();
    let mut monitors = Vec::with_capacity(lines.len());
    for line in lines {
        let monitor = parse_monitor(&tokenize(line)?, resolve_hostnames)?;
        if !masters.insert(monitor.name.clone()) {
            return Err("Duplicate master name.".to_string());
        }
        monitors.push(monitor);
    }
    Ok((monitors, masters))
}

fn parse_post_stage(
    lines: &[String],
    registry: &SentinelRegistry,
    masters: &HashSet<String>,
    resolve_hostnames: bool,
    script_validator: &dyn SentinelConfigScriptValidator,
) -> Result<Vec<PostMonitorDirective>, String> {
    let mut duplicates = PostDuplicateState::from_registry(registry);
    let mut directives = Vec::with_capacity(lines.len());
    for line in lines {
        let args = tokenize(line)?;
        let parsed = parse_post(&directive_name(line), &args)?;
        let directive = validate_post(parsed, masters, resolve_hostnames, script_validator)?;
        duplicates.insert(&directive)?;
        directives.push(directive);
    }
    Ok(directives)
}

fn tokenize(line: &str) -> Result<Vec<String>, String> {
    match parse_inline_args(line.as_bytes()) {
        Ok(arguments) => arguments
            .into_iter()
            .map(|arg| String::from_utf8(arg.to_vec()).map_err(|_| UNRECOGNIZED.to_string()))
            .collect(),
        Err(_) if all_quotes_are_escaped(line) => line
            .split_ascii_whitespace()
            .map(decode_legacy_token)
            .map(Ok)
            .collect(),
        Err(_) => Err(UNRECOGNIZED.to_string()),
    }
}

fn all_quotes_are_escaped(line: &str) -> bool {
    let bytes = line.as_bytes();
    bytes.contains(&b'"')
        && bytes.iter().enumerate().all(|(index, byte)| {
            if *byte != b'"' {
                return true;
            }
            let slash_count = bytes[..index]
                .iter()
                .rev()
                .take_while(|previous| **previous == b'\\')
                .count();
            slash_count % 2 == 1
        })
}

fn decode_legacy_token(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut chars = value.trim_matches('"').chars();
    while let Some(character) = chars.next() {
        if character == '\\' {
            if let Some(escaped) = chars.next() {
                decoded.push(escaped);
            }
        } else {
            decoded.push(character);
        }
    }
    decoded
}

fn parse_pre(name: &str, args: &[String]) -> Result<PreMonitorDirective, String> {
    match (name, args.len()) {
        ("myid", 2) => parse_myid(&args[1]),
        ("current-epoch", 2) => Ok(PreMonitorDirective::CurrentEpoch(parse_c_number(&args[1]))),
        ("deny-scripts-reconfig", 2) => parse_pre_bool(
            &args[1],
            "Please specify yes or no for the deny-scripts-reconfig options.",
            PreMonitorDirective::DenyScriptsReconfig,
        ),
        ("sentinel-user", 2) => Ok(PreMonitorDirective::SentinelUser(args[1].clone())),
        ("sentinel-pass", 2) => Ok(PreMonitorDirective::SentinelPass(args[1].clone())),
        ("announce-ip", 2) => Ok(PreMonitorDirective::AnnounceIp(args[1].clone())),
        ("announce-port", 2) => Ok(PreMonitorDirective::AnnouncePort(
            parse_c_number(&args[1]) as i32
        )),
        ("resolve-hostnames", 2) => parse_pre_bool(
            &args[1],
            "Please specify yes or no for the resolve-hostnames option.",
            PreMonitorDirective::ResolveHostnames,
        ),
        ("announce-hostnames", 2) => parse_pre_bool(
            &args[1],
            "Please specify yes or no for the announce-hostnames option.",
            PreMonitorDirective::AnnounceHostnames,
        ),
        _ => Err(UNRECOGNIZED.to_string()),
    }
}

fn parse_pre_bool(
    value: &str,
    error: &str,
    wrap: fn(bool) -> PreMonitorDirective,
) -> Result<PreMonitorDirective, String> {
    parse_bool(value, error).map(wrap)
}

fn is_pre_name(name: &str) -> bool {
    matches!(
        name,
        "myid"
            | "current-epoch"
            | "deny-scripts-reconfig"
            | "sentinel-user"
            | "sentinel-pass"
            | "announce-ip"
            | "announce-port"
            | "resolve-hostnames"
            | "announce-hostnames"
    )
}

fn parse_myid(value: &str) -> Result<PreMonitorDirective, String> {
    if value.len() != 40 {
        return Err("Malformed Sentinel id in myid option.".to_string());
    }
    Ok(PreMonitorDirective::MyId(value.to_string()))
}

fn parse_bool(value: &str, error: &str) -> Result<bool, String> {
    if value.eq_ignore_ascii_case("yes") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("no") {
        Ok(false)
    } else {
        Err(error.to_string())
    }
}

fn parse_monitor(args: &[String], resolve_hostnames: bool) -> Result<MonitorDirective, String> {
    if args.len() != 5 || !args[0].eq_ignore_ascii_case("monitor") {
        return Err(UNRECOGNIZED.to_string());
    }
    let quorum = parse_c_number(&args[4]);
    if quorum <= 0 {
        return Err("Quorum must be 1 or greater.".to_string());
    }
    Ok(MonitorDirective {
        name: args[1].clone(),
        addr: normalize_address(&args[2], parse_c_number(&args[3]), resolve_hostnames)?,
        quorum,
    })
}

fn parse_post(name: &str, args: &[String]) -> Result<ParsedPostDirective, String> {
    match (name, args.len()) {
        ("down-after-milliseconds", 3) => Ok(parsed_number(args, MasterNumberField::DownAfterMs)),
        ("failover-timeout", 3) => Ok(parsed_number(args, MasterNumberField::FailoverTimeout)),
        ("parallel-syncs", 3) => Ok(parsed_number(args, MasterNumberField::ParallelSyncs)),
        ("primary-reboot-down-after-period" | "master-reboot-down-after-period", 3) => Ok(
            parsed_number(args, MasterNumberField::PrimaryRebootDownAfter),
        ),
        ("config-epoch", 3) => Ok(parsed_number(args, MasterNumberField::ConfigEpoch)),
        ("leader-epoch", 3) => Ok(parsed_number(args, MasterNumberField::LeaderEpoch)),
        ("notification-script", 3) => Ok(parsed_text(args, MasterTextField::NotificationScript)),
        ("client-reconfig-script", 3) => {
            Ok(parsed_text(args, MasterTextField::ClientReconfigScript))
        }
        ("auth-pass", 3) => Ok(parsed_text(args, MasterTextField::AuthPass)),
        ("auth-user", 3) => Ok(parsed_text(args, MasterTextField::AuthUser)),
        ("rename-command", 4) => Ok(parsed_rename(args)),
        ("known-replica" | "known-slave", 4) => Ok(parsed_replica(args)),
        ("known-sentinel", 4) => Ok(ParsedPostDirective::Ignore),
        ("known-sentinel", 5) => Ok(parsed_sentinel(args)),
        _ => Err(UNRECOGNIZED.to_string()),
    }
}

fn parsed_number(args: &[String], field: MasterNumberField) -> ParsedPostDirective {
    ParsedPostDirective::Number {
        name: args[1].clone(),
        field,
        value: parse_c_number(&args[2]),
    }
}

fn parsed_text(args: &[String], field: MasterTextField) -> ParsedPostDirective {
    ParsedPostDirective::Text {
        name: args[1].clone(),
        field,
        value: args[2].clone(),
    }
}

fn parsed_rename(args: &[String]) -> ParsedPostDirective {
    ParsedPostDirective::RenameCommand {
        name: args[1].clone(),
        source: args[2].clone(),
        target: args[3].clone(),
    }
}

fn parsed_replica(args: &[String]) -> ParsedPostDirective {
    ParsedPostDirective::KnownReplica {
        name: args[1].clone(),
        host: args[2].clone(),
        port: parse_c_number(&args[3]),
    }
}

fn parsed_sentinel(args: &[String]) -> ParsedPostDirective {
    ParsedPostDirective::KnownSentinel {
        name: args[1].clone(),
        host: args[2].clone(),
        port: parse_c_number(&args[3]),
        runid: args[4].clone(),
    }
}

impl ParsedPostDirective {
    fn master_name(&self) -> Option<&str> {
        match self {
            Self::Number { name, .. }
            | Self::Text { name, .. }
            | Self::RenameCommand { name, .. }
            | Self::KnownReplica { name, .. }
            | Self::KnownSentinel { name, .. } => Some(name),
            Self::Ignore => None,
        }
    }
}

fn validate_post(
    parsed: ParsedPostDirective,
    masters: &HashSet<String>,
    resolve_hostnames: bool,
    script_validator: &dyn SentinelConfigScriptValidator,
) -> Result<PostMonitorDirective, String> {
    if parsed
        .master_name()
        .is_some_and(|name| !masters.contains(name))
    {
        return Err(NO_SUCH_MASTER.to_string());
    }
    match parsed {
        ParsedPostDirective::Number { name, field, value } => {
            validate_number(field, value)?;
            Ok(PostMonitorDirective::Number { name, field, value })
        }
        ParsedPostDirective::Text { name, field, value } => {
            validate_text(field, &value, script_validator)?;
            Ok(PostMonitorDirective::Text { name, field, value })
        }
        ParsedPostDirective::RenameCommand {
            name,
            source,
            target,
        } => Ok(PostMonitorDirective::RenameCommand {
            name,
            source,
            target,
        }),
        other => validate_known_instance(other, resolve_hostnames),
    }
}

fn validate_number(field: MasterNumberField, value: i64) -> Result<(), String> {
    if matches!(
        field,
        MasterNumberField::DownAfterMs | MasterNumberField::FailoverTimeout
    ) && value <= 0
    {
        return Err("negative or zero time parameter.".to_string());
    }
    if matches!(field, MasterNumberField::PrimaryRebootDownAfter) && value < 0 {
        return Err("negative time parameter.".to_string());
    }
    Ok(())
}

fn validate_text(
    field: MasterTextField,
    value: &str,
    validator: &dyn SentinelConfigScriptValidator,
) -> Result<(), String> {
    let kind = match field {
        MasterTextField::NotificationScript => SentinelConfigScriptKind::Notification,
        MasterTextField::ClientReconfigScript => SentinelConfigScriptKind::ClientReconfig,
        MasterTextField::AuthPass | MasterTextField::AuthUser => return Ok(()),
    };
    validator.validate_script(kind, value)
}

fn validate_known_instance(
    parsed: ParsedPostDirective,
    resolve_hostnames: bool,
) -> Result<PostMonitorDirective, String> {
    match parsed {
        ParsedPostDirective::KnownReplica { name, host, port } => {
            let addr = normalize_address(&host, port, resolve_hostnames)?;
            Ok(PostMonitorDirective::KnownReplica { name, addr })
        }
        ParsedPostDirective::KnownSentinel {
            name,
            host,
            port,
            runid,
        } => {
            let addr = normalize_address(&host, port, resolve_hostnames)?;
            Ok(PostMonitorDirective::KnownSentinel { name, addr, runid })
        }
        ParsedPostDirective::Ignore => Ok(PostMonitorDirective::Ignore),
        _ => unreachable!("number, text, and rename directives handled before known instances"),
    }
}

fn normalize_address(host: &str, port: i64, resolve_hostnames: bool) -> Result<String, String> {
    let port = u16::try_from(port).map_err(|_| "Invalid port number.".to_string())?;
    let addr = format_sentinel_endpoint(host, port);
    let is_literal = split_sentinel_endpoint(&addr)
        .is_some_and(|endpoint| endpoint.host.parse::<std::net::IpAddr>().is_ok());
    if !is_literal && !resolve_hostnames {
        Err("Can't resolve instance hostname.".to_string())
    } else {
        Ok(addr)
    }
}

fn parse_c_number(value: &str) -> i64 {
    let bytes = value.as_bytes();
    let (sign, start) = match bytes.first() {
        Some(b'-') => (-1_i64, 1),
        Some(b'+') => (1, 1),
        _ => (1, 0),
    };
    let end = bytes[start..]
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .map_or(bytes.len(), |offset| start + offset);
    if end == start {
        return 0;
    }
    let magnitude = value[start..end].parse::<i64>().unwrap_or(i64::MAX);
    magnitude.saturating_mul(sign)
}

fn prospective_resolve_hostnames(pre: &[PreMonitorDirective], initial: bool) -> bool {
    pre.iter()
        .fold(initial, |current, directive| match directive {
            PreMonitorDirective::ResolveHostnames(value) => *value,
            _ => current,
        })
}

struct PostDuplicateState {
    replicas: HashSet<(String, String)>,
    sentinels: HashSet<(String, String)>,
    renamed: HashMap<String, HashSet<String>>,
}

impl PostDuplicateState {
    fn from_registry(registry: &SentinelRegistry) -> Self {
        Self {
            replicas: existing_related(registry, InstanceRole::Replica),
            sentinels: existing_related(registry, InstanceRole::Sentinel),
            renamed: existing_renames(registry),
        }
    }

    fn insert(&mut self, directive: &PostMonitorDirective) -> Result<(), String> {
        match directive {
            PostMonitorDirective::KnownReplica { name, addr } => {
                if !self.replicas.insert((name.clone(), addr.clone())) {
                    return Err("Duplicate hostname and port for replica.".to_string());
                }
            }
            PostMonitorDirective::KnownSentinel { name, runid, .. } => {
                if !self.sentinels.insert((name.clone(), runid.clone())) {
                    return Err("Duplicate runid for sentinel.".to_string());
                }
            }
            PostMonitorDirective::RenameCommand { name, source, .. } => {
                let commands = self.renamed.entry(name.clone()).or_default();
                if !commands.insert(source.to_ascii_lowercase()) {
                    return Err("Same command renamed multiple times with rename-command.".into());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn existing_related(registry: &SentinelRegistry, role: InstanceRole) -> HashSet<(String, String)> {
    registry
        .masters()
        .flat_map(|(name, master)| {
            let ids = if role == InstanceRole::Replica {
                &master.replicas
            } else {
                &master.sentinels
            };
            ids.iter().filter_map(move |id| {
                registry.instance(*id).map(|instance| {
                    let key = if role == InstanceRole::Replica {
                        canonical_stored_address(&instance.addr)
                    } else {
                        instance.runid.clone()
                    };
                    (name.to_string(), key)
                })
            })
        })
        .collect()
}

fn canonical_stored_address(addr: &str) -> String {
    split_sentinel_endpoint(addr)
        .map(|endpoint| format_sentinel_endpoint(endpoint.host, endpoint.port))
        .unwrap_or_else(|| addr.to_string())
}

fn existing_renames(registry: &SentinelRegistry) -> HashMap<String, HashSet<String>> {
    registry
        .masters()
        .map(|(name, master)| {
            let commands = master
                .renamed_commands
                .keys()
                .map(|command| command.to_ascii_lowercase())
                .collect();
            (name.to_string(), commands)
        })
        .collect()
}
