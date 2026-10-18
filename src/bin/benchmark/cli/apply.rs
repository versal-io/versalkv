use super::option::{
    BenchmarkOption, CommandTarget, MemtierCommandOption, MemtierDataOption, MemtierKeyOption,
    MemtierOption, MemtierOutputOption, MemtierProtocol, MemtierRunOption, ParsedBenchmarkArgs,
    UriCredentials, ValkeyConnectionOption, ValkeyExecutionOption, ValkeyExtendedOption,
    ValkeyLoadOption, ValkeyOption, ValkeyOutputOption,
};
use super::types::{BenchConfig, BenchMode, KeyPattern, MemtierCommandSpec};

pub(super) fn apply_options(parsed: ParsedBenchmarkArgs) -> Result<BenchConfig, String> {
    let mut state = ApplyState::default();
    for option in parsed.options {
        state.apply(option)?;
    }
    state.config.command = parsed.command;
    state.validate()?;
    Ok(state.config)
}

#[derive(Default)]
struct ApplyState {
    config: BenchConfig,
    explicit_requests: bool,
    explicit_duration: bool,
}

impl ApplyState {
    fn apply(&mut self, option: BenchmarkOption) -> Result<(), String> {
        match option {
            BenchmarkOption::Valkey(option) => {
                self.apply_valkey(option);
                Ok(())
            }
            BenchmarkOption::Memtier(option) => self.apply_memtier(option),
        }
    }

    fn apply_valkey(&mut self, option: ValkeyOption) {
        match option {
            ValkeyOption::Connection(option) => apply_connection(&mut self.config, option),
            ValkeyOption::Load(option) => self.apply_load(option),
            ValkeyOption::Execution(option) => apply_execution(&mut self.config, option),
            ValkeyOption::Extended(option) => apply_extended(&mut self.config, option),
            ValkeyOption::Output(option) => apply_output(&mut self.config, option),
        }
    }

    fn apply_load(&mut self, option: ValkeyLoadOption) {
        match option {
            ValkeyLoadOption::Clients(value) => self.config.clients = value,
            ValkeyLoadOption::Requests(value) => {
                self.config.requests = value;
                self.explicit_requests = true;
            }
            ValkeyLoadOption::Keyspace(value) => self.config.keyspace_len = value,
            ValkeyLoadOption::Database(value) => self.config.dbnum = Some(value),
            ValkeyLoadOption::DataSize(value) => self.config.datasize = value,
            ValkeyLoadOption::Pipeline(value) => self.config.pipeline = value.max(1),
            ValkeyLoadOption::Tests(value) => self.config.tests = Some(value),
            ValkeyLoadOption::Duration(value) => {
                self.config.duration = Some(value);
                self.explicit_duration = true;
            }
        }
    }

    fn apply_memtier(&mut self, option: MemtierOption) -> Result<(), String> {
        self.config.mode = BenchMode::MemtierRedis;
        match option {
            MemtierOption::Run(option) => apply_memtier_run(&mut self.config, option),
            MemtierOption::Key(option) => apply_memtier_key(&mut self.config, option),
            MemtierOption::Data(option) => apply_memtier_data(&mut self.config, option),
            MemtierOption::Command(option) => self.apply_memtier_command(option)?,
            MemtierOption::Output(option) => apply_memtier_output(&mut self.config, option),
        }
        Ok(())
    }

    fn apply_memtier_command(&mut self, option: MemtierCommandOption) -> Result<(), String> {
        match option {
            MemtierCommandOption::Definition(argv) => {
                self.config.memtier.command_specs.push(MemtierCommandSpec {
                    argv,
                    ratio: 1,
                    key_pattern: KeyPattern::Sequential,
                });
            }
            MemtierCommandOption::Ratio { target, value } => {
                self.command_mut(target)?.ratio = value;
            }
            MemtierCommandOption::KeyPattern { target, value } => {
                self.command_mut(target)?.key_pattern = value;
            }
        }
        Ok(())
    }

    fn command_mut(&mut self, target: CommandTarget) -> Result<&mut MemtierCommandSpec, String> {
        self.config
            .memtier
            .command_specs
            .get_mut(target.index())
            .ok_or_else(|| "Internal benchmark option error: invalid command target".to_string())
    }

    fn validate(&self) -> Result<(), String> {
        if self.explicit_requests && self.explicit_duration {
            return Err("Options -n and --duration are mutually exclusive".to_string());
        }
        Ok(())
    }
}

fn apply_connection(config: &mut BenchConfig, option: ValkeyConnectionOption) {
    match option {
        ValkeyConnectionOption::Host(value) => config.host = value,
        ValkeyConnectionOption::Port(value) => config.port = value,
        ValkeyConnectionOption::UnixSocket(value) => config.unix_socket = Some(value),
        ValkeyConnectionOption::Password(value) => config.password = Some(value),
        ValkeyConnectionOption::User(value) => config.user = Some(value),
        ValkeyConnectionOption::Uri(uri) => {
            config.host = uri.host;
            if let Some(port) = uri.port {
                config.port = port;
            }
            apply_uri_credentials(config, uri.credentials);
        }
    }
}

fn apply_uri_credentials(config: &mut BenchConfig, credentials: UriCredentials) {
    match credentials {
        UriCredentials::None => {}
        UriCredentials::Password(password) => config.password = Some(password),
        UriCredentials::UserPassword { user, password } => {
            config.user = Some(user);
            config.password = Some(password);
        }
    }
}

fn apply_execution(config: &mut BenchConfig, option: ValkeyExecutionOption) {
    match option {
        ValkeyExecutionOption::Sequential => config.sequential = true,
        ValkeyExecutionOption::Loop => config.loop_mode = true,
        ValkeyExecutionOption::Idle => config.idle = true,
        ValkeyExecutionOption::StdinLastArgument => config.stdin_lastarg = true,
        ValkeyExecutionOption::Resp3 => config.resp3 = true,
        ValkeyExecutionOption::Warmup(value) => config.warmup = value,
        ValkeyExecutionOption::RequestsPerSecond(value) => config.rps = Some(value),
        ValkeyExecutionOption::EnableTracking => config.enable_tracking = true,
    }
}

fn apply_extended(config: &mut BenchConfig, option: ValkeyExtendedOption) {
    match option {
        ValkeyExtendedOption::NumFunctions(value) => config.num_functions = value,
        ValkeyExtendedOption::NumKeysInFcall(value) => config.num_keys_in_fcall = value,
        ValkeyExtendedOption::Threads(value) => config.threads = value.max(1),
        ValkeyExtendedOption::Dataset(value) => config.dataset = Some(value),
        ValkeyExtendedOption::MaxDocuments(value) => config.maxdocs = Some(value),
    }
}

fn apply_output(config: &mut BenchConfig, option: ValkeyOutputOption) {
    match option {
        ValkeyOutputOption::Quiet => config.quiet = true,
        ValkeyOutputOption::Csv => config.csv = true,
    }
}

fn apply_memtier_run(config: &mut BenchConfig, option: MemtierRunOption) {
    match option {
        MemtierRunOption::Ratio(value) => config.memtier.ratio = Some(value),
        MemtierRunOption::Requests(value) => config.memtier.requests = Some(value),
        MemtierRunOption::TestTime(value) => config.memtier.test_time_sec = Some(value),
        MemtierRunOption::RunCount(value) => config.memtier.run_count = value,
        MemtierRunOption::Clients(value) => config.clients = value,
        MemtierRunOption::Pipeline(value) => config.pipeline = value.max(1),
        MemtierRunOption::Protocol(MemtierProtocol::Resp2) => config.resp3 = false,
        MemtierRunOption::Protocol(MemtierProtocol::Resp3) => config.resp3 = true,
        MemtierRunOption::RateLimit(value) => {
            config.memtier.rate_limit_per_connection = Some(value);
        }
    }
}

fn apply_memtier_key(config: &mut BenchConfig, option: MemtierKeyOption) {
    match option {
        MemtierKeyOption::Pattern(value) => config.memtier.key_pattern = Some(value),
        MemtierKeyOption::Prefix(value) => config.memtier.key_prefix = value,
        MemtierKeyOption::Minimum(value) => config.memtier.key_minimum = value,
        MemtierKeyOption::Maximum(value) => config.memtier.key_maximum = value,
    }
}

fn apply_memtier_data(config: &mut BenchConfig, option: MemtierDataOption) {
    match option {
        MemtierDataOption::Size(value) => config.memtier.data_size = Some(value),
        MemtierDataOption::Random => config.memtier.random_data = true,
        MemtierDataOption::SizeRange(value) => config.memtier.data_size_range = Some(value),
    }
}

fn apply_memtier_output(config: &mut BenchConfig, option: MemtierOutputOption) {
    match option {
        MemtierOutputOption::JsonFile(value) => config.memtier.json_out_file = Some(value),
        MemtierOutputOption::Percentiles(value) => config.memtier.percentiles = value,
    }
}
