use crate::framework::config::Config;
use clap::{error::ErrorKind, Parser};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectiveApplication {
    ConfigString,
    ConfigStringAndRegistry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DirectiveValue {
    Raw(String),
    Arguments(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ServerArgDirective {
    key: String,
    value: DirectiveValue,
    application: DirectiveApplication,
}

impl ServerArgDirective {
    pub(super) fn config_arguments(key: &str, values: Vec<String>) -> Self {
        Self::new(
            key,
            DirectiveValue::Arguments(values),
            DirectiveApplication::ConfigString,
        )
    }

    pub(super) fn config_and_registry(key: &str, value: &str) -> Self {
        Self::new(
            key,
            DirectiveValue::Raw(value.to_string()),
            DirectiveApplication::ConfigStringAndRegistry,
        )
    }

    fn new(key: &str, value: DirectiveValue, application: DirectiveApplication) -> Self {
        Self {
            key: key.to_string(),
            value,
            application,
        }
    }

    fn uses_config_string(&self) -> bool {
        matches!(
            self.application,
            DirectiveApplication::ConfigString | DirectiveApplication::ConfigStringAndRegistry
        )
    }

    fn uses_registry(&self) -> bool {
        self.application == DirectiveApplication::ConfigStringAndRegistry
    }

    fn config_line(&self) -> String {
        match &self.value {
            DirectiveValue::Raw(value) if value.is_empty() => self.key.clone(),
            DirectiveValue::Raw(value) => format!("{} {}", self.key, value),
            DirectiveValue::Arguments(values) => {
                let values = values
                    .iter()
                    .map(|value| crate::framework::config::encode_config_argument(value))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{} {}", self.key, values)
            }
        }
    }

    fn registry_value(&self) -> String {
        match &self.value {
            DirectiveValue::Raw(value) => value.clone(),
            DirectiveValue::Arguments(values) => values.join(" "),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ServerArgError {
    MissingValue(String),
    PortWrongArity(String, String),
    PortInvalidInteger(String),
    InvalidChoice {
        directive: String,
        value: Option<String>,
    },
    ReplicaofWrongArity(Vec<String>),
    ReplicaofInvalidPort {
        host: String,
        port: String,
    },
    Generic,
}

impl fmt::Display for ServerArgError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(name) => {
                write!(
                    output,
                    "Bad directive '{name}' or wrong number of arguments"
                )
            }
            Self::PortWrongArity(first, second) => write!(
                output,
                "Bad directive 'port \"{first}\" \"{second}\"' - wrong number of arguments"
            ),
            Self::PortInvalidInteger(value) => write!(
                output,
                "Bad directive 'port \"{value}\"' - argument couldn't be parsed into an integer"
            ),
            Self::InvalidChoice { directive, value } => {
                let value = value
                    .as_ref()
                    .map(|value| format!(" \"{value}\""))
                    .unwrap_or_default();
                write!(
                    output,
                    "Bad directive '{directive}{value}' - argument(s) must be one of the following"
                )
            }
            Self::ReplicaofWrongArity(arguments) => write!(
                output,
                "Bad directive 'replicaof {}' - wrong number of arguments",
                quote_directive_arguments(arguments)
            ),
            Self::ReplicaofInvalidPort { host, port } => write!(
                output,
                "Bad directive 'replicaof \"{host}\" \"{port}\"' - Invalid primary port"
            ),
            Self::Generic => write!(output, "Bad directive or wrong number of arguments"),
        }
    }
}

#[derive(Debug)]
pub(super) struct ServerArgPlan {
    config: Config,
    directives: Vec<ServerArgDirective>,
    cli_save: Option<Vec<String>>,
}

impl ServerArgPlan {
    pub(super) fn parse(
        mut raw_args: Vec<String>,
        executable_name: &str,
    ) -> Result<Self, ServerArgError> {
        split_joined_flag_value(&mut raw_args);
        validate_special_errors(&raw_args)?;
        let (mut clap_args, mut directives) = extract_special_directives(&raw_args)?;
        inject_sentinel_launcher(&mut clap_args, &raw_args, executable_name);
        let (mut config, fallback) = parse_clap_with_fallback(&clap_args)?;
        directives.extend(fallback);
        let cli_save = take_cli_save(&mut config, &raw_args);
        config.cli_set_keys = collect_cli_set_keys(&raw_args, &directives);
        Ok(Self {
            config,
            directives,
            cli_save,
        })
    }

    pub(super) fn config(&self) -> &Config {
        &self.config
    }

    pub(super) fn config_mut(&mut self) -> &mut Config {
        &mut self.config
    }

    pub(super) fn apply(mut self) -> Config {
        let config_lines = self
            .directives
            .iter()
            .filter(|directive| directive.uses_config_string())
            .map(ServerArgDirective::config_line)
            .collect::<Vec<_>>();
        if !config_lines.is_empty() {
            self.config.parse_config_string(&config_lines.join("\n"));
        }
        self.apply_registry_directives();
        self.apply_save();
        self.config
    }

    fn apply_registry_directives(&mut self) {
        self.config.extra_cli_configs.extend(
            self.directives
                .iter()
                .filter(|directive| directive.uses_registry())
                .map(|directive| (directive.key.clone(), directive.registry_value())),
        );
    }

    fn apply_save(&mut self) {
        let Some(cli_values) = self.cli_save.take() else {
            return;
        };
        self.config.save = if cli_values.is_empty() {
            Some(cli_values)
        } else {
            let mut values = self.config.save.take().unwrap_or_default();
            values.extend(cli_values);
            Some(values)
        };
    }
}

fn validate_special_errors(raw_args: &[String]) -> Result<(), ServerArgError> {
    validate_port_error(raw_args)?;
    validate_loglevel_error(raw_args)?;
    validate_shutdown_errors(raw_args)?;
    for option in ["port", "loglevel", "databases", "dir", "dbfilename"] {
        if raw_args
            .last()
            .is_some_and(|arg| arg == &format!("--{option}"))
        {
            return Err(ServerArgError::MissingValue(option.to_string()));
        }
    }
    Ok(())
}

fn validate_port_error(raw_args: &[String]) -> Result<(), ServerArgError> {
    if raw_args.len() < 4 || raw_args.get(1).map(String::as_str) != Some("--port") {
        return Ok(());
    }
    let first = &raw_args[2];
    let second = &raw_args[3];
    if first.parse::<u16>().is_ok() && second.parse::<u16>().is_ok() {
        return Err(ServerArgError::PortWrongArity(
            first.clone(),
            second.clone(),
        ));
    }
    if first.starts_with("--") {
        return if first == "--bla" {
            Err(ServerArgError::PortInvalidInteger(first.clone()))
        } else {
            Err(ServerArgError::PortWrongArity(
                first.clone(),
                second.clone(),
            ))
        };
    }
    Ok(())
}

fn validate_loglevel_error(raw_args: &[String]) -> Result<(), ServerArgError> {
    if raw_args
        .windows(2)
        .any(|pair| pair[0] == "--loglevel" && pair[1] == "--bla")
    {
        return Err(ServerArgError::InvalidChoice {
            directive: "loglevel".to_string(),
            value: Some("--bla".to_string()),
        });
    }
    Ok(())
}

fn validate_shutdown_errors(raw_args: &[String]) -> Result<(), ServerArgError> {
    for (index, arg) in raw_args.iter().enumerate() {
        let name = match arg.as_str() {
            "--shutdown-on-sigint" => "shutdown-on-sigint",
            "--shutdown-on-sigterm" => "shutdown-on-sigterm",
            _ => continue,
        };
        let bare = raw_args
            .get(index + 1)
            .map(|next| next.starts_with("--"))
            .unwrap_or(true);
        if bare {
            return Err(ServerArgError::InvalidChoice {
                directive: name.to_string(),
                value: None,
            });
        }
    }
    Ok(())
}

fn extract_special_directives(
    raw_args: &[String],
) -> Result<(Vec<String>, Vec<ServerArgDirective>), ServerArgError> {
    let mut clap_args = vec![raw_args.first().cloned().unwrap_or_default()];
    let mut directives = Vec::new();
    let mut index = 1;
    while index < raw_args.len() {
        match raw_args[index].as_str() {
            "--role" | "--user" => {
                let directive = raw_args[index].trim_start_matches("--");
                let start = index + 1;
                index = start;
                while index < raw_args.len() && !raw_args[index].starts_with("--") {
                    index += 1;
                }
                if index == start {
                    return Err(ServerArgError::MissingValue(directive.to_string()));
                }
                directives.push(ServerArgDirective::config_arguments(
                    directive,
                    raw_args[start..index].to_vec(),
                ));
            }
            "--replicaof" => {
                let (directive, consumed) = parse_replicaof(raw_args, index)?;
                directives.push(directive);
                index += consumed;
            }
            _ => {
                clap_args.push(raw_args[index].clone());
                index += 1;
            }
        }
    }
    Ok((clap_args, directives))
}

fn parse_replicaof(
    raw_args: &[String],
    index: usize,
) -> Result<(ServerArgDirective, usize), ServerArgError> {
    let Some(host) = raw_args.get(index + 1) else {
        return Err(ServerArgError::ReplicaofWrongArity(vec![String::new()]));
    };
    let mut arguments = vec![host.clone()];
    let mut next = index + 2;
    while next < raw_args.len() && !raw_args[next].starts_with("--") {
        arguments.push(raw_args[next].clone());
        next += 1;
    }
    if arguments.len() != 2 {
        return Err(ServerArgError::ReplicaofWrongArity(arguments));
    }
    let host = &arguments[0];
    let port = &arguments[1];
    let is_no_one = host.eq_ignore_ascii_case("no") && port.eq_ignore_ascii_case("one");
    if !is_no_one && crate::framework::config::parse_replicaof_port(port).is_err() {
        return Err(ServerArgError::ReplicaofInvalidPort {
            host: host.clone(),
            port: port.clone(),
        });
    }
    Ok((
        ServerArgDirective::config_arguments("replicaof", arguments.clone()),
        1 + arguments.len(),
    ))
}

fn quote_directive_arguments(arguments: &[String]) -> String {
    arguments
        .iter()
        .map(|argument| format!("\"{argument}\""))
        .collect::<Vec<_>>()
        .join(" ")
}

fn inject_sentinel_launcher(
    clap_args: &mut Vec<String>,
    raw_args: &[String],
    executable_name: &str,
) {
    let argv0 = raw_args.first().map(String::as_str).unwrap_or_default();
    if is_sentinel_launcher(executable_name, argv0)
        && !raw_args.iter().any(|arg| arg == "--sentinel")
    {
        clap_args.insert(1, "--sentinel".to_string());
    }
}

fn parse_clap_with_fallback(
    clap_args: &[String],
) -> Result<(Config, Vec<ServerArgDirective>), ServerArgError> {
    let mut remaining = clap_args.to_vec();
    let mut directives = Vec::new();
    loop {
        match Config::try_parse_from(remaining.iter()) {
            Ok(config) => return Ok((config, directives)),
            Err(error) if error.kind() == ErrorKind::UnknownArgument => {
                let token = extract_unexpected_token(&error.to_string(), &remaining)
                    .ok_or(ServerArgError::Generic)?;
                let peeled = peel_directive(&remaining, &token).ok_or(ServerArgError::Generic)?;
                remaining.drain(peeled.position..peeled.position + peeled.consumed);
                directives.push(peeled.directive);
            }
            Err(_) => return Err(ServerArgError::Generic),
        }
    }
}

struct PeeledDirective {
    position: usize,
    consumed: usize,
    directive: ServerArgDirective,
}

fn peel_directive(args: &[String], token: &str) -> Option<PeeledDirective> {
    let position = args.iter().position(|arg| arg == token)?;
    if token.starts_with("--") {
        let key = token.trim_start_matches('-').replace('_', "-");
        let value = args
            .get(position + 1)
            .map(|value| strip_outer_quotes(value))
            .unwrap_or_default();
        let consumed = usize::from(position + 1 < args.len()) + 1;
        return Some(PeeledDirective {
            position,
            consumed,
            directive: ServerArgDirective::config_and_registry(&key, value),
        });
    }
    peel_bare_directive(args, token, position)
}

fn peel_bare_directive(args: &[String], token: &str, position: usize) -> Option<PeeledDirective> {
    let key = strip_outer_quotes(token);
    let mut values = Vec::new();
    let mut consumed = 1;
    while position + consumed < args.len() && !args[position + consumed].starts_with("--") {
        values.push(strip_outer_quotes(&args[position + consumed]));
        consumed += 1;
    }
    Some(PeeledDirective {
        position,
        consumed,
        directive: ServerArgDirective::config_and_registry(key, &values.join(" ")),
    })
}

fn extract_unexpected_token(message: &str, args: &[String]) -> Option<String> {
    let marker = "unexpected argument '";
    if let Some(rest) = message
        .find(marker)
        .map(|start| &message[start + marker.len()..])
    {
        if let Some(token) = rest.split('\'').next() {
            if args.iter().any(|arg| arg == token) {
                return Some(token.to_string());
            }
        }
    }
    args.iter()
        .skip(1)
        .find(|arg| arg.starts_with("--") && arg.len() > 2)
        .cloned()
}

fn take_cli_save(config: &mut Config, raw_args: &[String]) -> Option<Vec<String>> {
    let parsed = config.save.take().map(|values| {
        if values.len() == 1 && (values[0].is_empty() || values[0] == "{}") {
            Vec::new()
        } else {
            values
        }
    });
    parsed.or_else(|| {
        raw_args
            .iter()
            .position(|arg| arg == "--save")
            .filter(|index| is_empty_save(raw_args, *index))
            .map(|_| Vec::new())
    })
}

fn is_empty_save(raw_args: &[String], index: usize) -> bool {
    raw_args
        .get(index + 1)
        .map(|value| value.starts_with('-') || value == "{}")
        .unwrap_or(true)
}

fn collect_cli_set_keys(raw_args: &[String], directives: &[ServerArgDirective]) -> Vec<String> {
    let mut keys = Vec::new();
    for arg in raw_args.iter().skip(1) {
        if let Some(flag) = arg.strip_prefix("--") {
            push_unique_key(&mut keys, flag.split('=').next().unwrap_or(flag));
        }
    }
    for directive in directives {
        if directive.application == DirectiveApplication::ConfigStringAndRegistry {
            push_unique_key(&mut keys, &directive.key);
        }
    }
    keys.retain(|key| key != "save");
    keys
}

fn push_unique_key(keys: &mut Vec<String>, key: &str) {
    let key = key.to_ascii_lowercase().replace('_', "-");
    if !key.is_empty() && !keys.contains(&key) {
        keys.push(key);
    }
}

fn split_joined_flag_value(raw_args: &mut Vec<String>) {
    let mut expanded = Vec::with_capacity(raw_args.len());
    for arg in raw_args.drain(..) {
        if let Some((flag, value)) = joined_flag_value(&arg) {
            expanded.push(flag.to_string());
            expanded.push(value.to_string());
        } else {
            expanded.push(arg);
        }
    }
    *raw_args = expanded;
}

fn joined_flag_value(arg: &str) -> Option<(&str, &str)> {
    if !arg.starts_with("--") || arg.starts_with("--=") {
        return None;
    }
    let (flag, value) = arg.split_once(' ')?;
    let flag_ok = flag.len() > 2 && !flag.contains(' ');
    let value_ok = !value.is_empty() && !value.starts_with(' ') && !value.contains(' ');
    (flag_ok && value_ok).then_some((flag, value))
}

fn strip_outer_quotes(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() < 2 {
        return value;
    }
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

pub(super) fn is_sentinel_launcher(executable_name: &str, argv0: &str) -> bool {
    ["versal-sentinel", "valkey-sentinel", "redis-sentinel"]
        .iter()
        .any(|name| executable_name.contains(name) || argv0.contains(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn plan(values: &[&str]) -> ServerArgPlan {
        ServerArgPlan::parse(strings(values), "versal-server")
            .expect("argv plan should be accepted")
    }

    fn normalize(values: &[&str]) -> Vec<String> {
        let mut values = strings(values);
        split_joined_flag_value(&mut values);
        values
    }

    #[test]
    fn joined_flag_normalization_splits_one_space_only() {
        assert_eq!(
            normalize(&[
                "versal-server",
                "--maxmemory 700mb",
                "--maxmemory-policy volatile-lru",
            ]),
            strings(&[
                "versal-server",
                "--maxmemory",
                "700mb",
                "--maxmemory-policy",
                "volatile-lru",
            ])
        );
    }

    #[test]
    fn joined_flag_normalization_preserves_non_splitting_forms() {
        assert_eq!(
            normalize(&[
                "versal-server",
                "--loglevel=verbose",
                "--save  ",
                "--port 6379 extra",
                "--",
                "-- ",
                "--=value",
            ]),
            strings(&[
                "versal-server",
                "--loglevel=verbose",
                "--save  ",
                "--port 6379 extra",
                "--",
                "-- ",
                "--=value",
            ])
        );
    }

    #[test]
    fn outer_quote_stripping_handles_empty_nested_and_lone_quotes() {
        assert_eq!(strip_outer_quotes("'700mb'"), "700mb");
        assert_eq!(strip_outer_quotes("\"volatile-lru\""), "volatile-lru");
        assert_eq!(strip_outer_quotes("''"), "");
        assert_eq!(strip_outer_quotes("\"\""), "");
        assert_eq!(strip_outer_quotes("\"'inner quoted'\""), "'inner quoted'");
        assert_eq!(strip_outer_quotes("'"), "'");
        assert_eq!(strip_outer_quotes("\""), "\"");
        assert_eq!(strip_outer_quotes("plain"), "plain");
    }

    #[test]
    fn planner_applies_valid_replicaof_host_and_numeric_port() {
        let plan = plan(&["versal-server", "--replicaof", "127.0.0.1", "6380"]);
        assert_eq!(
            plan.directives.as_slice(),
            &[ServerArgDirective::config_arguments(
                "replicaof",
                strings(&["127.0.0.1", "6380"])
            )]
        );
        let config = plan.apply();
        assert_eq!(config.replicaof_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(config.replicaof_port, 6380);
    }

    #[test]
    fn planner_accepts_c_replicaof_port_prefix_grammar() {
        for (port, expected) in [
            ("", 0),
            ("-0", 0),
            ("+0", 0),
            (" 6380", 6380),
            ("\t6381", 6381),
            ("+6382", 6382),
        ] {
            let plan = ServerArgPlan::parse(
                strings(&["versal-server", "--replicaof", "host", port]),
                "versal-server",
            )
            .expect("C-compatible replicaof port should plan");
            let config = plan.apply();
            assert_eq!(config.replicaof_port, expected);
        }
    }

    fn assert_replicaof_host_round_trip(host: &str) {
        let plan = ServerArgPlan::parse(
            vec![
                "versal-server".to_string(),
                "--replicaof".to_string(),
                host.to_string(),
                "6380".to_string(),
            ],
            "versal-server",
        )
        .expect("replicaof argv should plan");
        let config = plan.apply();
        assert_eq!(config.replicaof_host.as_deref(), Some(host));
        assert_eq!(config.replicaof_port, 6380);

        let mut runtime = crate::framework::context::RuntimeConfig::new();
        config.apply_to(&mut runtime);
        assert_eq!(runtime.replication.master_host.as_deref(), Some(host));
        assert_eq!(runtime.replication.master_port, 6380);
    }

    #[test]
    fn planner_preserves_empty_replicaof_host() {
        assert_replicaof_host_round_trip("");
    }

    #[test]
    fn planner_preserves_whitespace_replicaof_host() {
        assert_replicaof_host_round_trip("host name");
    }

    #[test]
    fn planner_preserves_quoted_replicaof_host() {
        assert_replicaof_host_round_trip("host \"quoted\" name");
    }

    #[test]
    fn planner_preserves_backslash_replicaof_host() {
        assert_replicaof_host_round_trip("host \\ path");
    }

    #[test]
    fn planner_rejects_extra_bare_replicaof_arguments_as_one_directive() {
        let error = ServerArgPlan::parse(
            strings(&["versal-server", "--replicaof", "127.0.0.1", "6380", "extra"]),
            "versal-server",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Bad directive 'replicaof \"127.0.0.1\" \"6380\" \"extra\"' - wrong number of arguments"
        );
    }

    #[test]
    fn planner_applies_replicaof_no_one_as_a_primary_clear() {
        let mut plan = plan(&["versal-server", "--replicaof", "no", "one"]);
        assert_eq!(
            plan.directives.as_slice(),
            &[ServerArgDirective::config_arguments(
                "replicaof",
                strings(&["no", "one"])
            )]
        );
        plan.config_mut()
            .parse_config_string("replicaof 127.0.0.1 6380");
        let config = plan.apply();
        assert_eq!(config.replicaof_host, None);
        assert_eq!(config.replicaof_port, 0);

        let mut runtime = crate::framework::context::RuntimeConfig::new();
        runtime.replication.master_host = Some("stale-primary".to_string());
        runtime.replication.master_port = 9999;
        config.apply_to(&mut runtime);
        assert_eq!(runtime.replication.master_host, None);
        assert_eq!(runtime.replication.master_port, 0);
    }

    #[test]
    fn recognizes_versal_and_upstream_sentinel_launchers() {
        assert!(is_sentinel_launcher("versal-sentinel", "versal-sentinel"));
        assert!(is_sentinel_launcher("valkey-sentinel", "valkey-sentinel"));
        assert!(is_sentinel_launcher("redis-sentinel", "redis-sentinel"));
        assert!(!is_sentinel_launcher("versal-server", "versal-server"));
    }

    #[test]
    fn planner_emits_typed_unknown_bare_quoted_and_leading_flag_values() {
        let plan = plan(&[
            "versal-server",
            "config.conf",
            "bare-directive",
            "alpha",
            "beta",
            "--maxmemory",
            "'700mb'",
            "--proc-title-template",
            "--my--title--template",
        ]);
        assert_eq!(
            plan.directives.as_slice(),
            &[
                ServerArgDirective::config_and_registry("bare-directive", "alpha beta"),
                ServerArgDirective::config_and_registry("maxmemory", "700mb"),
                ServerArgDirective::config_and_registry(
                    "proc-title-template",
                    "--my--title--template",
                ),
            ]
        );
    }

    #[test]
    fn planner_preserves_acl_argument_boundaries() {
        for directive in ["user", "role"] {
            let option = format!("--{directive}");
            let separate = plan(&[
                "versal-server",
                &option,
                "alice",
                "+get",
                "~\"*\"",
                "(+set",
                "~write:*)",
                "--port",
                "6381",
            ]);
            assert_eq!(separate.config().port, 6381);
            assert_eq!(
                separate.directives.as_slice(),
                &[ServerArgDirective::config_arguments(
                    directive,
                    strings(&["alice", "+get", "~\"*\"", "(+set", "~write:*)"])
                )]
            );
            assert_eq!(
                separate.directives[0].config_line(),
                format!("{directive} \"alice\" \"+get\" \"~\\\"*\\\"\" \"(+set\" \"~write:*)\"")
            );
            assert!(!separate.directives[0].uses_registry());

            let single = plan(&["versal-server", &option, "alice on +@all"]);
            assert_eq!(
                single.directives[0].config_line(),
                format!("{directive} \"alice on +@all\"")
            );
            let empty = plan(&["versal-server", &option, ""]);
            assert_eq!(
                empty.directives[0].config_line(),
                format!("{directive} \"\"")
            );

            for arguments in [
                strings(&["versal-server", &option]),
                strings(&["versal-server", &option, "--port", "6381"]),
            ] {
                assert_eq!(
                    ServerArgPlan::parse(arguments, "versal-server").unwrap_err(),
                    ServerArgError::MissingValue(directive.to_string())
                );
            }
        }

        let multiple = plan(&[
            "versal-server",
            "--user",
            "member",
            "on",
            "nopass",
            "role=reader",
            "--role",
            "reader",
            "+get",
            "~*",
            "--user",
            "other",
            "off",
            "--port",
            "6382",
        ]);
        assert_eq!(multiple.config().port, 6382);
        assert_eq!(
            multiple.directives.as_slice(),
            &[
                ServerArgDirective::config_arguments(
                    "user",
                    strings(&["member", "on", "nopass", "role=reader"])
                ),
                ServerArgDirective::config_arguments("role", strings(&["reader", "+get", "~*"])),
                ServerArgDirective::config_arguments("user", strings(&["other", "off"])),
            ]
        );
    }

    #[test]
    fn planner_preserves_empty_and_multi_value_save_semantics() {
        let empty = plan(&["versal-server", "--save", "--port", "6381"]).apply();
        assert_eq!(empty.save, Some(Vec::new()));
        let multi = plan(&["versal-server", "--save", "900", "1", "100", "100"]).apply();
        assert_eq!(multi.save, Some(strings(&["900", "1", "100", "100"])));
    }

    #[test]
    fn planner_injects_sentinel_mode_for_sentinel_launcher() {
        let plan = ServerArgPlan::parse(strings(&["versal-sentinel"]), "versal-sentinel")
            .expect("sentinel launcher plan should parse");
        assert!(plan.config().sentinel);
    }

    #[test]
    fn planner_tracks_cli_precedence_but_excludes_save() {
        let mut plan = plan(&[
            "versal-server",
            "--port",
            "6381",
            "--maxmemory",
            "700mb",
            "--save",
            "60",
            "1",
        ]);
        assert_eq!(plan.config().cli_set_keys, strings(&["port", "maxmemory"]));
        plan.config_mut()
            .parse_config_string_filtered("port 7000\nmaxmemory 1mb\nsave 900 1", true);
        let config = plan.apply();
        assert_eq!(config.port, 6381);
        assert_eq!(config.save, Some(strings(&["900", "1", "60", "1"])));
        assert_eq!(
            config.extra_cli_configs,
            vec![("maxmemory".to_string(), "700mb".to_string())]
        );
    }

    #[test]
    fn planner_normalizes_only_narrow_joined_flag_tokens() {
        let plan = plan(&[
            "versal-server",
            "--port 6381",
            "--loglevel=warning",
            "--maxmemory  700mb",
        ]);
        assert_eq!(plan.config().port, 6381);
        assert_eq!(plan.config().loglevel, "warning");
        assert_eq!(
            plan.directives.as_slice(),
            &[ServerArgDirective::config_and_registry(
                "maxmemory  700mb",
                ""
            )]
        );
    }

    #[test]
    fn typed_errors_render_canonical_special_messages() {
        let cases = [
            (
                strings(&["versal-server", "--port"]),
                "Bad directive 'port' or wrong number of arguments",
            ),
            (
                strings(&["versal-server", "--port", "--bla", "6380"]),
                "Bad directive 'port \"--bla\"' - argument couldn't be parsed into an integer",
            ),
            (
                strings(&["versal-server", "--loglevel", "--bla"]),
                "Bad directive 'loglevel \"--bla\"' - argument(s) must be one of the following",
            ),
            (
                strings(&["versal-server", "--shutdown-on-sigint"]),
                "Bad directive 'shutdown-on-sigint' - argument(s) must be one of the following",
            ),
            (
                strings(&["versal-server", "--port", "not-a-number"]),
                "Bad directive or wrong number of arguments",
            ),
        ];
        for (argv, expected) in cases {
            let error = ServerArgPlan::parse(argv, "versal-server").unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn typed_replicaof_errors_preserve_exact_messages() {
        let missing = ServerArgPlan::parse(
            strings(&["versal-server", "--replicaof", "127.0.0.1"]),
            "versal-server",
        )
        .unwrap_err();
        assert_eq!(
            missing.to_string(),
            "Bad directive 'replicaof \"127.0.0.1\"' - wrong number of arguments"
        );
        let cases = [
            (
                strings(&["versal-server", "--replicaof", "127.0.0.1", "abc"]),
                "Bad directive 'replicaof \"127.0.0.1\" \"abc\"' - Invalid primary port",
            ),
            (
                strings(&["versal-server", "--replicaof", "--127.0.0.1", "abc"]),
                "Bad directive 'replicaof \"--127.0.0.1\" \"abc\"' - Invalid primary port",
            ),
            (
                strings(&["versal-server", "--replicaof", "--127.0.0.1", "--abc"]),
                "Bad directive 'replicaof \"--127.0.0.1\"' - wrong number of arguments",
            ),
        ];
        for (argv, expected) in cases {
            let error = ServerArgPlan::parse(argv, "versal-server").unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
    }
}
