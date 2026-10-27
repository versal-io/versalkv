use std::str::FromStr;

use super::types::{KeyPattern, Ratio};

#[derive(Debug)]
pub(super) struct ParsedBenchmarkArgs {
    pub(super) options: Vec<BenchmarkOption>,
    pub(super) command: Vec<String>,
}

#[derive(Debug)]
pub(super) enum BenchmarkOption {
    Valkey(ValkeyOption),
    Memtier(MemtierOption),
}

#[derive(Debug)]
pub(super) enum ValkeyOption {
    Connection(ValkeyConnectionOption),
    Load(ValkeyLoadOption),
    Execution(ValkeyExecutionOption),
    Extended(ValkeyExtendedOption),
    Output(ValkeyOutputOption),
}

#[derive(Debug)]
pub(super) enum ValkeyConnectionOption {
    Host(String),
    Port(u16),
    UnixSocket(String),
    Password(String),
    User(String),
    Uri(ParsedUri),
}

#[derive(Debug)]
pub(super) enum ValkeyLoadOption {
    Clients(usize),
    Requests(u64),
    Keyspace(u64),
    Database(u32),
    DataSize(usize),
    Pipeline(usize),
    Tests(Vec<String>),
    Duration(u64),
}

#[derive(Debug)]
pub(super) enum ValkeyExecutionOption {
    Sequential,
    Loop,
    Idle,
    StdinLastArgument,
    Resp3,
    Warmup(u64),
    RequestsPerSecond(u64),
    EnableTracking,
}

#[derive(Debug)]
pub(super) enum ValkeyExtendedOption {
    NumFunctions(u32),
    NumKeysInFcall(u32),
    Threads(usize),
    Dataset(String),
    MaxDocuments(usize),
}

#[derive(Debug)]
pub(super) enum ValkeyOutputOption {
    Quiet,
    Csv,
}

#[derive(Debug)]
pub(super) enum MemtierOption {
    Run(MemtierRunOption),
    Key(MemtierKeyOption),
    Data(MemtierDataOption),
    Command(MemtierCommandOption),
    Output(MemtierOutputOption),
}

#[derive(Debug)]
pub(super) enum MemtierRunOption {
    Ratio(Ratio),
    Requests(u64),
    TestTime(u64),
    RunCount(u32),
    Clients(usize),
    Pipeline(usize),
    Protocol(MemtierProtocol),
    RateLimit(u64),
}

#[derive(Debug)]
pub(super) enum MemtierKeyOption {
    Pattern((KeyPattern, KeyPattern)),
    Prefix(String),
    Minimum(u64),
    Maximum(u64),
}

#[derive(Debug)]
pub(super) enum MemtierDataOption {
    Size(usize),
    Random,
    SizeRange((usize, usize)),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CommandTarget(usize);

impl CommandTarget {
    pub(super) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug)]
pub(super) enum MemtierCommandOption {
    Definition(Vec<String>),
    Ratio {
        target: CommandTarget,
        value: u32,
    },
    KeyPattern {
        target: CommandTarget,
        value: KeyPattern,
    },
}

#[derive(Debug)]
pub(super) enum MemtierOutputOption {
    JsonFile(String),
    Percentiles(Vec<f64>),
}

#[derive(Clone, Copy, Debug)]
pub(super) enum MemtierProtocol {
    Resp2,
    Resp3,
}

#[derive(Debug)]
pub(super) struct ParsedUri {
    pub(super) host: String,
    pub(super) port: Option<u16>,
    pub(super) credentials: UriCredentials,
}

#[derive(Debug)]
pub(super) enum UriCredentials {
    None,
    Password(String),
    UserPassword { user: String, password: String },
}

type ValueParser = fn(&str, &str) -> Result<BenchmarkOption, String>;
type DependentValueParser = fn(&str, &str) -> Result<DependentCommandValue, String>;

enum DependentCommandValue {
    Ratio(u32),
    KeyPattern(KeyPattern),
}

impl DependentCommandValue {
    fn bind(self, target: CommandTarget) -> BenchmarkOption {
        let option = match self {
            Self::Ratio(value) => MemtierCommandOption::Ratio { target, value },
            Self::KeyPattern(value) => MemtierCommandOption::KeyPattern { target, value },
        };
        BenchmarkOption::Memtier(MemtierOption::Command(option))
    }
}

#[derive(Clone, Copy)]
enum OptionParser {
    Flag(fn() -> BenchmarkOption),
    Value(ValueParser),
    DependentValue(DependentValueParser),
    IgnoredFlag,
    IgnoredValue,
}

#[derive(Clone, Copy)]
struct OptionSpec {
    name: &'static str,
    allow_equals: bool,
    parser: OptionParser,
}

impl OptionSpec {
    const fn flag(name: &'static str, parser: fn() -> BenchmarkOption) -> Self {
        Self {
            name,
            allow_equals: false,
            parser: OptionParser::Flag(parser),
        }
    }

    const fn value(name: &'static str, allow_equals: bool, parser: ValueParser) -> Self {
        Self {
            name,
            allow_equals,
            parser: OptionParser::Value(parser),
        }
    }

    const fn dependent_value(
        name: &'static str,
        allow_equals: bool,
        parser: DependentValueParser,
    ) -> Self {
        Self {
            name,
            allow_equals,
            parser: OptionParser::DependentValue(parser),
        }
    }

    const fn ignored_flag(name: &'static str) -> Self {
        Self {
            name,
            allow_equals: false,
            parser: OptionParser::IgnoredFlag,
        }
    }

    const fn ignored_value(name: &'static str) -> Self {
        Self {
            name,
            allow_equals: false,
            parser: OptionParser::IgnoredValue,
        }
    }
}

macro_rules! valkey {
    ($option:expr) => {
        BenchmarkOption::Valkey($option)
    };
}

macro_rules! memtier {
    ($option:expr) => {
        BenchmarkOption::Memtier($option)
    };
}

const VALKEY_OPTIONS: &[OptionSpec] = &[
    OptionSpec::value("-h", false, |_, v| {
        Ok(valkey!(ValkeyOption::Connection(
            ValkeyConnectionOption::Host(v.into())
        )))
    }),
    OptionSpec::value("-p", false, |n, v| {
        Ok(valkey!(ValkeyOption::Connection(
            ValkeyConnectionOption::Port(parse_number(n, v)?)
        )))
    }),
    OptionSpec::value("-s", false, |_, v| {
        Ok(valkey!(ValkeyOption::Connection(
            ValkeyConnectionOption::UnixSocket(v.into())
        )))
    }),
    OptionSpec::value("-a", false, |_, v| {
        Ok(valkey!(ValkeyOption::Connection(
            ValkeyConnectionOption::Password(v.into())
        )))
    }),
    OptionSpec::value("--user", false, |_, v| {
        Ok(valkey!(ValkeyOption::Connection(
            ValkeyConnectionOption::User(v.into())
        )))
    }),
    OptionSpec::value("-u", false, |_, v| {
        Ok(valkey!(ValkeyOption::Connection(
            ValkeyConnectionOption::Uri(parse_uri(v)?)
        )))
    }),
    OptionSpec::value("-c", false, |n, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::Clients(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("-n", false, |n, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::Requests(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("-r", false, |_, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::Keyspace(
            parse_keyspace(v)?
        ))))
    }),
    OptionSpec::value("--dbnum", false, |n, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::Database(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("-d", false, |n, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::DataSize(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("-P", false, |n, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::Pipeline(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("-t", false, |_, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::Tests(
            parse_tests(v)
        ))))
    }),
    OptionSpec::value("--duration", false, |n, v| {
        Ok(valkey!(ValkeyOption::Load(ValkeyLoadOption::Duration(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::flag("--sequential", || {
        valkey!(ValkeyOption::Execution(ValkeyExecutionOption::Sequential))
    }),
    OptionSpec::flag("-l", || {
        valkey!(ValkeyOption::Execution(ValkeyExecutionOption::Loop))
    }),
    OptionSpec::flag("-I", || {
        valkey!(ValkeyOption::Execution(ValkeyExecutionOption::Idle))
    }),
    OptionSpec::flag("-x", || {
        valkey!(ValkeyOption::Execution(
            ValkeyExecutionOption::StdinLastArgument
        ))
    }),
    OptionSpec::flag("-3", || {
        valkey!(ValkeyOption::Execution(ValkeyExecutionOption::Resp3))
    }),
    OptionSpec::value("--warmup", false, |n, v| {
        Ok(valkey!(ValkeyOption::Execution(
            ValkeyExecutionOption::Warmup(parse_number(n, v)?)
        )))
    }),
    OptionSpec::value("--rps", false, |n, v| {
        Ok(valkey!(ValkeyOption::Execution(
            ValkeyExecutionOption::RequestsPerSecond(parse_number(n, v)?)
        )))
    }),
    OptionSpec::flag("--enable-tracking", || {
        valkey!(ValkeyOption::Execution(
            ValkeyExecutionOption::EnableTracking
        ))
    }),
    OptionSpec::value("--num-functions", false, |n, v| {
        Ok(valkey!(ValkeyOption::Extended(
            ValkeyExtendedOption::NumFunctions(parse_number(n, v)?)
        )))
    }),
    OptionSpec::value("--num-keys-in-fcall", false, |n, v| {
        Ok(valkey!(ValkeyOption::Extended(
            ValkeyExtendedOption::NumKeysInFcall(parse_number(n, v)?)
        )))
    }),
    OptionSpec::value("--threads", false, |n, v| {
        Ok(valkey!(ValkeyOption::Extended(
            ValkeyExtendedOption::Threads(parse_number(n, v)?)
        )))
    }),
    OptionSpec::value("--dataset", false, |_, v| {
        Ok(valkey!(ValkeyOption::Extended(
            ValkeyExtendedOption::Dataset(v.into())
        )))
    }),
    OptionSpec::value("--maxdocs", false, |n, v| {
        Ok(valkey!(ValkeyOption::Extended(
            ValkeyExtendedOption::MaxDocuments(parse_number(n, v)?)
        )))
    }),
    OptionSpec::flag("-q", || {
        valkey!(ValkeyOption::Output(ValkeyOutputOption::Quiet))
    }),
    OptionSpec::flag("--csv", || {
        valkey!(ValkeyOption::Output(ValkeyOutputOption::Csv))
    }),
];

const MEMTIER_OPTIONS: &[OptionSpec] = &[
    OptionSpec::value("--ratio", true, |_, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::Ratio(
            parse_ratio(v)?
        ))))
    }),
    OptionSpec::value("--requests", true, |n, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::Requests(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--test-time", true, |n, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::TestTime(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--run-count", true, |n, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::RunCount(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--clients", true, |n, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::Clients(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--pipeline", true, |n, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::Pipeline(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--protocol", true, |_, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::Protocol(
            parse_protocol(v)?
        ))))
    }),
    OptionSpec::value("--rate-limiting", true, |n, v| {
        Ok(memtier!(MemtierOption::Run(MemtierRunOption::RateLimit(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--key-pattern", true, |_, v| {
        Ok(memtier!(MemtierOption::Key(MemtierKeyOption::Pattern(
            parse_key_pattern(v)?
        ))))
    }),
    OptionSpec::value("--key-prefix", true, |_, v| {
        Ok(memtier!(MemtierOption::Key(MemtierKeyOption::Prefix(
            v.into()
        ))))
    }),
    OptionSpec::value("--key-minimum", true, |n, v| {
        Ok(memtier!(MemtierOption::Key(MemtierKeyOption::Minimum(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--key-maximum", true, |n, v| {
        Ok(memtier!(MemtierOption::Key(MemtierKeyOption::Maximum(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::value("--data-size", true, |n, v| {
        Ok(memtier!(MemtierOption::Data(MemtierDataOption::Size(
            parse_number(n, v)?
        ))))
    }),
    OptionSpec::flag("--random-data", || {
        memtier!(MemtierOption::Data(MemtierDataOption::Random))
    }),
    OptionSpec::value("--data-size-range", true, |_, v| {
        Ok(memtier!(MemtierOption::Data(MemtierDataOption::SizeRange(
            parse_size_range(v)?
        ))))
    }),
    OptionSpec::value("--command", true, |_, v| {
        Ok(memtier!(MemtierOption::Command(
            MemtierCommandOption::Definition(parse_command_line(v)?)
        )))
    }),
    OptionSpec::dependent_value("--command-ratio", true, |n, v| {
        Ok(DependentCommandValue::Ratio(parse_number(n, v)?))
    }),
    OptionSpec::dependent_value("--command-key-pattern", true, |_, v| {
        Ok(DependentCommandValue::KeyPattern(parse_one_key_pattern(v)?))
    }),
    OptionSpec::value("--json-out-file", true, |_, v| {
        Ok(memtier!(MemtierOption::Output(
            MemtierOutputOption::JsonFile(v.into())
        )))
    }),
    OptionSpec::value("--print-percentiles", true, |_, v| {
        Ok(memtier!(MemtierOption::Output(
            MemtierOutputOption::Percentiles(parse_percentiles(v)?)
        )))
    }),
];

const COMPATIBILITY_OPTIONS: &[OptionSpec] = &[
    OptionSpec::ignored_flag("--tls"),
    OptionSpec::ignored_flag("--cluster"),
    OptionSpec::ignored_value("--cert"),
    OptionSpec::ignored_value("--key"),
    OptionSpec::ignored_value("--cacert"),
    OptionSpec::ignored_value("--tls-ciphers"),
    OptionSpec::ignored_value("--tls-ciphersuites"),
    OptionSpec::ignored_value("--precision"),
];

pub(super) fn parse_benchmark_options(
    raw_args: Vec<String>,
) -> Result<ParsedBenchmarkArgs, String> {
    let normalized = normalize_equals_options(raw_args)?;
    let mut cursor = TokenCursor::new(normalized);
    let mut sequence = ParsedOptionSequence::default();
    while let Some(token) = cursor.peek() {
        if token == "--" {
            cursor.advance();
            return Ok(sequence.finish(cursor.tail()));
        }
        if !token.starts_with('-') || token == "-" {
            return Ok(sequence.finish(cursor.tail()));
        }
        if let Some(option) = parse_next_option(&mut cursor, sequence.current_command())? {
            sequence.push(option);
        }
    }
    Ok(sequence.finish(Vec::new()))
}

#[derive(Default)]
struct ParsedOptionSequence {
    options: Vec<BenchmarkOption>,
    current_command: Option<CommandTarget>,
    command_count: usize,
}

impl ParsedOptionSequence {
    fn current_command(&self) -> Option<CommandTarget> {
        self.current_command
    }

    fn push(&mut self, option: BenchmarkOption) {
        if matches!(
            &option,
            BenchmarkOption::Memtier(MemtierOption::Command(MemtierCommandOption::Definition(_)))
        ) {
            self.current_command = Some(CommandTarget(self.command_count));
            self.command_count += 1;
        }
        self.options.push(option);
    }

    fn finish(self, command: Vec<String>) -> ParsedBenchmarkArgs {
        ParsedBenchmarkArgs {
            options: self.options,
            command,
        }
    }
}

fn parse_next_option(
    cursor: &mut TokenCursor,
    current_command: Option<CommandTarget>,
) -> Result<Option<BenchmarkOption>, String> {
    let name = cursor.take().expect("cursor was checked before parsing");
    let spec = find_option(&name).ok_or_else(|| format!("Unsupported option {name}"))?;
    match spec.parser {
        OptionParser::Flag(parser) => Ok(Some(parser())),
        OptionParser::Value(parser) => parser(&name, &required_value(cursor, &name)?).map(Some),
        OptionParser::DependentValue(parser) => {
            let value = parser(&name, &required_value(cursor, &name)?)?;
            let target =
                current_command.ok_or_else(|| format!("{name} requires a preceding --command"))?;
            Ok(Some(value.bind(target)))
        }
        OptionParser::IgnoredFlag => Ok(None),
        OptionParser::IgnoredValue => skip_required_value(cursor, &name).map(|()| None),
    }
}

fn required_value(cursor: &mut TokenCursor, option: &str) -> Result<String, String> {
    cursor
        .take()
        .ok_or_else(|| format!("Option {option} requires an argument"))
}

fn skip_required_value(cursor: &mut TokenCursor, option: &str) -> Result<(), String> {
    if cursor.peek().is_none() {
        return Err(format!("Option {option} requires an argument"));
    }
    cursor.advance();
    Ok(())
}

fn find_option(name: &str) -> Option<&'static OptionSpec> {
    [VALKEY_OPTIONS, MEMTIER_OPTIONS, COMPATIBILITY_OPTIONS]
        .into_iter()
        .flatten()
        .find(|spec| spec.name == name)
}

fn normalize_equals_options(raw_args: Vec<String>) -> Result<Vec<String>, String> {
    let mut normalized = Vec::with_capacity(raw_args.len());
    let mut passthrough = false;
    for arg in raw_args {
        if passthrough || arg == "--" {
            passthrough |= arg == "--";
            normalized.push(arg);
            continue;
        }
        push_normalized_argument(&mut normalized, arg)?;
    }
    Ok(normalized)
}

fn push_normalized_argument(normalized: &mut Vec<String>, arg: String) -> Result<(), String> {
    if let Some((name, value)) = arg.split_once('=') {
        if find_option(name).is_some_and(|spec| spec.allow_equals) {
            if value.is_empty() {
                return Err(format!("Option {name} requires an argument"));
            }
            normalized.extend([name.to_string(), value.to_string()]);
            return Ok(());
        }
    }
    normalized.push(arg);
    Ok(())
}

struct TokenCursor {
    args: Vec<String>,
    index: usize,
}

impl TokenCursor {
    fn new(args: Vec<String>) -> Self {
        Self { args, index: 1 }
    }

    fn peek(&self) -> Option<&str> {
        self.args.get(self.index).map(String::as_str)
    }

    fn advance(&mut self) {
        self.index += 1;
    }

    fn take(&mut self) -> Option<String> {
        let value = self.args.get(self.index)?.clone();
        self.index += 1;
        Some(value)
    }

    fn tail(&mut self) -> Vec<String> {
        let tail = self.args.split_off(self.index.min(self.args.len()));
        self.index = self.args.len();
        tail
    }
}

fn parse_number<T>(option: &str, value: &str) -> Result<T, String>
where
    T: FromStr,
{
    value
        .parse()
        .map_err(|_| format!("Invalid value for {option}: {value}"))
}

fn parse_keyspace(value: &str) -> Result<u64, String> {
    let number = value.trim_start_matches([' ', '\t', '\n', '\r', '\x0b', '\x0c']);
    let digits = number
        .strip_prefix('+')
        .or_else(|| number.strip_prefix('-'))
        .unwrap_or(number);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "Invalid value for -r: '{value}' is not a valid number"
        ));
    }
    number
        .parse::<i64>()
        .ok()
        .filter(|number| (0..=999_999_999_999).contains(number))
        .map(|number| number as u64)
        .ok_or_else(|| {
            "Invalid value for -r: keyspacelen must be between 0 and 999999999999".to_string()
        })
}

fn parse_tests(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|name| name.trim().to_ascii_uppercase())
        .filter(|name| !name.is_empty())
        .collect()
}

fn parse_ratio(input: &str) -> Result<Ratio, String> {
    let (writes, reads) = input
        .split_once(':')
        .ok_or_else(|| format!("Invalid ratio {input}, expected <writes>:<reads>"))?;
    let writes = writes
        .parse()
        .map_err(|_| format!("Invalid ratio write value: {writes}"))?;
    let reads = reads
        .parse()
        .map_err(|_| format!("Invalid ratio read value: {reads}"))?;
    if writes == 0 && reads == 0 {
        return Err("Invalid ratio 0:0".to_string());
    }
    Ok(Ratio { writes, reads })
}

fn parse_key_pattern(input: &str) -> Result<(KeyPattern, KeyPattern), String> {
    let (write, read) = input
        .split_once(':')
        .ok_or_else(|| format!("Invalid key pattern {input}, expected <write>:<read>"))?;
    Ok((parse_one_key_pattern(write)?, parse_one_key_pattern(read)?))
}

fn parse_one_key_pattern(input: &str) -> Result<KeyPattern, String> {
    match input {
        "R" => Ok(KeyPattern::Random),
        "S" => Ok(KeyPattern::Sequential),
        "P" => Ok(KeyPattern::Parallel),
        "G" | "Z" => Err(format!(
            "Key pattern {input} is not supported in this phase"
        )),
        _ => Err(format!("Invalid key pattern {input}")),
    }
}

fn parse_protocol(input: &str) -> Result<MemtierProtocol, String> {
    match input {
        "redis" | "resp2" => Ok(MemtierProtocol::Resp2),
        "resp3" => Ok(MemtierProtocol::Resp3),
        other => Err(format!("Unsupported Redis protocol {other}")),
    }
}

fn parse_percentiles(input: &str) -> Result<Vec<f64>, String> {
    input
        .split(',')
        .map(|item| {
            let percentile = item
                .parse::<f64>()
                .map_err(|_| format!("Invalid percentile {item}"))?;
            if !(0.0..=100.0).contains(&percentile) {
                return Err(format!("Invalid percentile {percentile}"));
            }
            Ok(percentile)
        })
        .collect()
}

fn parse_size_range(input: &str) -> Result<(usize, usize), String> {
    let (min, max) = input
        .split_once('-')
        .or_else(|| input.split_once(':'))
        .ok_or_else(|| format!("Invalid data size range {input}, expected <min>-<max>"))?;
    let min = min
        .parse()
        .map_err(|_| format!("Invalid data size range minimum: {min}"))?;
    let max = max
        .parse()
        .map_err(|_| format!("Invalid data size range maximum: {max}"))?;
    if max < min {
        return Err("--data-size-range maximum must be greater than or equal to minimum".into());
    }
    Ok((min, max))
}

fn parse_command_line(input: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '"' => in_quotes = !in_quotes,
            ch if ch.is_whitespace() && !in_quotes => push_word(&mut out, &mut current),
            _ => current.push(ch),
        }
    }
    finish_command_line(out, current, in_quotes, escaped)
}

fn push_word(out: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        out.push(std::mem::take(current));
    }
}

fn finish_command_line(
    mut out: Vec<String>,
    mut current: String,
    in_quotes: bool,
    escaped: bool,
) -> Result<Vec<String>, String> {
    if escaped {
        current.push('\\');
    }
    if in_quotes {
        return Err("Unterminated quote in --command".to_string());
    }
    push_word(&mut out, &mut current);
    if out.is_empty() {
        return Err("--command must not be empty".to_string());
    }
    Ok(out)
}

fn parse_uri(uri: &str) -> Result<ParsedUri, String> {
    let rest = uri
        .strip_prefix("valkey://")
        .or_else(|| uri.strip_prefix("redis://"))
        .ok_or_else(|| format!("Unsupported URI scheme in {uri}"))?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((userinfo, hostport)) => (Some(userinfo), hostport),
        None => (None, authority),
    };
    let credentials = parse_uri_credentials(userinfo);
    let (host, port) = match hostport.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse()
                .map_err(|_| format!("Invalid port in URI: {port}"))?;
            (host.to_string(), Some(port))
        }
        None => (hostport.to_string(), None),
    };
    Ok(ParsedUri {
        host,
        port,
        credentials,
    })
}

fn parse_uri_credentials(userinfo: Option<&str>) -> UriCredentials {
    match userinfo {
        Some(userinfo) => match userinfo.split_once(':') {
            Some((user, password)) => UriCredentials::UserPassword {
                user: percent_decode(user),
                password: percent_decode(password),
            },
            None => UriCredentials::Password(percent_decode(userinfo)),
        },
        None => UriCredentials::None,
    }
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(value) = u8::from_str_radix(hex, 16) {
                    out.push(value);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{COMPATIBILITY_OPTIONS, MEMTIER_OPTIONS, VALKEY_OPTIONS};

    #[test]
    fn option_names_are_unique_across_all_tables() {
        let mut names = HashSet::new();
        for spec in [VALKEY_OPTIONS, MEMTIER_OPTIONS, COMPATIBILITY_OPTIONS]
            .into_iter()
            .flatten()
        {
            assert!(names.insert(spec.name), "duplicate option {}", spec.name);
        }
    }
}
