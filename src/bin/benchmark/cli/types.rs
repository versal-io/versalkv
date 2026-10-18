#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub mode: BenchMode,
    pub host: String,
    pub port: u16,
    pub unix_socket: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub requests: u64,
    pub clients: usize,
    pub datasize: usize,
    pub keyspace_len: u64,
    pub sequential: bool,
    pub pipeline: usize,
    pub tests: Option<Vec<String>>,
    pub quiet: bool,
    pub csv: bool,
    pub loop_mode: bool,
    pub idle: bool,
    pub duration: Option<u64>,
    pub warmup: u64,
    pub threads: usize,
    pub dbnum: Option<u32>,
    pub resp3: bool,
    pub stdin_lastarg: bool,
    pub dataset: Option<String>,
    pub maxdocs: Option<usize>,
    pub command: Vec<String>,
    pub rps: Option<u64>,
    pub enable_tracking: bool,
    pub num_functions: u32,
    pub num_keys_in_fcall: u32,
    pub memtier: MemtierConfig,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            mode: BenchMode::Valkey,
            host: "127.0.0.1".to_string(),
            port: 6379,
            unix_socket: None,
            user: None,
            password: None,
            requests: 100_000,
            clients: 50,
            datasize: 3,
            keyspace_len: 1,
            sequential: false,
            pipeline: 1,
            tests: None,
            quiet: false,
            csv: false,
            loop_mode: false,
            idle: false,
            duration: None,
            warmup: 0,
            threads: 1,
            dbnum: None,
            resp3: false,
            stdin_lastarg: false,
            dataset: None,
            maxdocs: None,
            command: Vec::new(),
            rps: None,
            enable_tracking: false,
            num_functions: 1,
            num_keys_in_fcall: 0,
            memtier: MemtierConfig {
                run_count: 1,
                key_prefix: "memtier-".to_string(),
                key_minimum: 0,
                key_maximum: 10_000_000,
                data_size: Some(32),
                percentiles: vec![50.0, 99.0, 99.9],
                ..Default::default()
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchMode {
    Valkey,
    MemtierRedis,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ratio {
    pub writes: u32,
    pub reads: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyPattern {
    Random,
    Sequential,
    Parallel,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemtierConfig {
    pub ratio: Option<Ratio>,
    pub requests: Option<u64>,
    pub test_time_sec: Option<u64>,
    pub run_count: u32,
    pub key_prefix: String,
    pub key_minimum: u64,
    pub key_maximum: u64,
    pub key_pattern: Option<(KeyPattern, KeyPattern)>,
    pub data_size: Option<usize>,
    pub random_data: bool,
    pub data_size_range: Option<(usize, usize)>,
    pub command_specs: Vec<MemtierCommandSpec>,
    pub json_out_file: Option<String>,
    pub percentiles: Vec<f64>,
    pub rate_limit_per_connection: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemtierCommandSpec {
    pub argv: Vec<String>,
    pub ratio: u32,
    pub key_pattern: KeyPattern,
}

pub struct FuzzerArgs {
    pub host: String,
    pub port: u16,
    pub num_clients: u32,
    pub total_commands: u64,
    pub num_keys: usize,
    pub loglevel: String,
    pub fuzz_flags: u32,
}
