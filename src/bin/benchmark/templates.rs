use std::collections::BTreeSet;

use super::cli::BenchConfig;
use super::dataset::Dataset;

#[derive(Clone, Debug)]
pub struct TestTemplate {
    pub name: String,
    pub sequence: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct CommandPlan {
    tests: Vec<TestTemplate>,
}

impl CommandPlan {
    pub fn from_config(config: &BenchConfig) -> Result<Self, String> {
        if config.idle {
            // Dataset validation still needs the raw template's field metadata.
            let tests = if config.dataset.is_some() {
                vec![TestTemplate {
                    name: "IDLE".to_string(),
                    sequence: vec![config.command.clone()],
                }]
            } else {
                Vec::new()
            };
            return Ok(Self { tests });
        }
        let mut tests = if config.command.is_empty() {
            select_default_tests(config)?
        } else {
            vec![TestTemplate {
                name: command_name(&config.command),
                sequence: parse_command_sequence(&config.command)?,
            }]
        };
        if config.command.is_empty() {
            append_function_tests(&mut tests, config);
        }
        if tests.is_empty() {
            return Err("No matching tests found".to_string());
        }
        Ok(Self { tests })
    }

    pub fn tests(&self) -> &[TestTemplate] {
        &self.tests
    }

    pub fn has_field_placeholders(&self) -> bool {
        self.tests.iter().any(|test| {
            test.sequence
                .iter()
                .flatten()
                .any(|arg| field_placeholder_name(arg).is_some())
        })
    }

    pub fn field_placeholders(&self) -> BTreeSet<&str> {
        let mut fields = BTreeSet::new();
        for arg in self
            .tests
            .iter()
            .flat_map(|test| test.sequence.iter().flatten())
        {
            if let Some(field) = field_placeholder_name(arg) {
                fields.insert(field);
            }
        }
        fields
    }
}

pub struct CommandGenerator<'a> {
    config: &'a BenchConfig,
    dataset: Option<&'a Dataset>,
    rng: FastRng,
    rand_int_sequential: u64,
    numbered_sequential: [u64; 9],
    generated: u64,
}

impl<'a> CommandGenerator<'a> {
    pub fn new(config: &'a BenchConfig, dataset: Option<&'a Dataset>) -> Self {
        Self {
            config,
            dataset,
            rng: FastRng::new(),
            rand_int_sequential: 0,
            numbered_sequential: [0; 9],
            generated: 0,
        }
    }

    pub fn build_sequence(&mut self, template: &TestTemplate) -> Result<Vec<Vec<Vec<u8>>>, String> {
        let mut commands = Vec::with_capacity(template.sequence.len());
        for command in &template.sequence {
            commands.push(self.build_command(command)?);
            self.generated += 1;
        }
        Ok(commands)
    }

    fn build_command(&mut self, template: &[String]) -> Result<Vec<Vec<u8>>, String> {
        let mut stable_values = [
            None::<String>,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ];
        let mut out = Vec::with_capacity(template.len());
        for arg in template {
            out.push(self.replace_arg(arg, &mut stable_values)?.into_bytes());
        }
        Ok(out)
    }

    fn replace_arg(
        &mut self,
        arg: &str,
        stable_values: &mut [Option<String>; 9],
    ) -> Result<String, String> {
        let mut result = arg.to_string();
        while let Some(pos) = result.find("{tag}") {
            let value = format!("{{{}}}", self.next_rand_int());
            result.replace_range(pos..pos + "{tag}".len(), &value);
        }
        while let Some(pos) = result.find("__rand_int__") {
            let value = self.next_rand_int();
            result.replace_range(pos..pos + "__rand_int__".len(), &value);
        }
        for slot in 1..=9 {
            let placeholder = format!("__rand_{}{}__", slot, ordinal_suffix(slot));
            if result.contains(&placeholder) {
                if stable_values[slot - 1].is_none() {
                    stable_values[slot - 1] = Some(self.next_numbered(slot - 1));
                }
                let value = stable_values[slot - 1].as_deref().unwrap_or_default();
                result = result.replace(&placeholder, value);
            }
        }
        if result.contains("__data__") {
            result = result.replace("__data__", &data_payload(self.config.datasize));
        }
        while let Some((start, end, field)) = locate_field_placeholder(&result) {
            let dataset = self
                .dataset
                .ok_or_else(|| "Field placeholder used without dataset".to_string())?;
            let value = dataset
                .get(self.generated as usize, field)
                .ok_or_else(|| format!("{field} not found in dataset fields"))?;
            result.replace_range(start..end, value);
        }
        Ok(result)
    }

    fn next_rand_int(&mut self) -> String {
        let value = if self.config.sequential {
            let value = self.rand_int_sequential % self.keyspace();
            self.rand_int_sequential = self.rand_int_sequential.wrapping_add(1);
            value
        } else {
            self.rng.next_u64() % self.keyspace()
        };
        format!("{value:012}")
    }

    fn next_numbered(&mut self, slot: usize) -> String {
        let value = if self.config.sequential {
            let value = self.numbered_sequential[slot] % self.keyspace();
            self.numbered_sequential[slot] = self.numbered_sequential[slot].wrapping_add(1);
            value
        } else {
            self.rng.next_u64() % self.keyspace()
        };
        format!("{value:012}")
    }

    fn keyspace(&self) -> u64 {
        self.config.keyspace_len.max(1)
    }
}

/// Append the FUNCTION_LOAD / FCALL benchmark templates when the user asked
/// for them via `-t`. These depend on runtime options (`--num-functions`,
/// `--num-keys-in-fcall`) so they cannot live in `DEFAULT_TESTS`.
fn append_function_tests(tests: &mut Vec<TestTemplate>, config: &BenchConfig) {
    let Some(requested) = config.tests.as_ref() else {
        return;
    };
    if test_is_selected(requested, "FUNCTION_LOAD") {
        let script = generate_function_script(config.num_functions, false);
        tests.push(TestTemplate {
            name: "FUNCTION LOAD".to_string(),
            sequence: vec![vec![
                "FUNCTION".to_string(),
                "LOAD".to_string(),
                "REPLACE".to_string(),
                script,
            ]],
        });
    }
    if test_is_selected(requested, "FCALL") {
        let num_keys = config.num_keys_in_fcall as usize;
        let mut argv = Vec::with_capacity(3 + num_keys);
        argv.push("FCALL".to_string());
        argv.push("foo1".to_string());
        argv.push(num_keys.to_string());
        for i in 0..num_keys {
            argv.push(format!("key{}", i + 1));
        }
        tests.push(TestTemplate {
            name: "FCALL".to_string(),
            sequence: vec![argv],
        });
    }
}

fn select_default_tests(config: &BenchConfig) -> Result<Vec<TestTemplate>, String> {
    let requested = config.tests.as_ref();
    let mut selected = Vec::new();
    for (name, command) in DEFAULT_TESTS {
        if requested
            .map(|names| test_is_selected(names, name))
            .unwrap_or(true)
        {
            selected.push(TestTemplate {
                name: (*name).to_string(),
                sequence: vec![split_words(command)],
            });
        }
    }
    Ok(selected)
}

/// Match the C `test_is_selected` behaviour, including the shorthand aliases:
/// - `ping` enables both PING_INLINE and PING_MBULK
/// - LRANGE enables LPUSH_LRANGE plus every LRANGE_* variant
fn test_is_selected(requested: &[String], candidate: &str) -> bool {
    for name in requested {
        let normalized = name.trim().to_ascii_uppercase();
        if normalized == candidate {
            return true;
        }
        if normalized == "PING" && matches!(candidate, "PING_INLINE" | "PING_MBULK") {
            return true;
        }
        if normalized == "LRANGE"
            && (candidate == "LPUSH_LRANGE" || candidate.starts_with("LRANGE_"))
        {
            return true;
        }
    }
    false
}

fn parse_command_sequence(args: &[String]) -> Result<Vec<Vec<String>>, String> {
    let mut sequence = Vec::new();
    let mut segments = args.split(|arg| arg == ";").peekable();
    let missing_command = || {
        "Invalid command sequence: a command is missing.\n\
         Use -n <requests> to set the total number of requests."
            .to_string()
    };
    while let Some(mut command) = segments.next() {
        // A trailing separator ends the last command without starting a new one.
        if command.is_empty() {
            if segments.peek().is_some() {
                return Err(missing_command());
            }
            continue;
        }
        let mut repeat = 1;
        while command
            .first()
            .is_some_and(|arg| matches!(arg.as_bytes().first(), Some(b'1'..=b'9')))
        {
            let digits: String = command[0]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            repeat = digits
                .parse::<usize>()
                .map_err(|_| "Invalid command repeat count".to_string())?;
            command = &command[1..];
        }
        if command.is_empty() {
            return Err(missing_command());
        }
        push_repeated(&mut sequence, command, repeat);
    }
    if sequence.is_empty() {
        return Err(missing_command());
    }
    Ok(sequence)
}

fn push_repeated(sequence: &mut Vec<Vec<String>>, command: &[String], repeat: usize) {
    if command.is_empty() {
        return;
    }
    for _ in 0..repeat {
        sequence.push(command.to_vec());
    }
}

fn command_name(args: &[String]) -> String {
    args.iter()
        .find(|arg| arg.as_str() != ";")
        .map(|arg| arg.to_ascii_uppercase())
        .unwrap_or_else(|| "COMMAND".to_string())
}

fn split_words(command: &str) -> Vec<String> {
    command
        .split_whitespace()
        .map(ToString::to_string)
        .collect()
}

fn data_payload(size: usize) -> String {
    "x".repeat(size)
}

fn ordinal_suffix(slot: usize) -> &'static str {
    match slot {
        1 => "st",
        2 => "nd",
        3 => "rd",
        _ => "th",
    }
}

fn locate_field_placeholder(input: &str) -> Option<(usize, usize, &str)> {
    let start = input.find("__field:")?;
    let rest = &input[start + "__field:".len()..];
    let end_offset = rest.find("__")?;
    let end = start + "__field:".len() + end_offset + "__".len();
    Some((start, end, &rest[..end_offset]))
}

fn field_placeholder_name(input: &str) -> Option<&str> {
    let (_, _, field) = locate_field_placeholder(input)?;
    Some(field)
}

struct FastRng(u64);

impl FastRng {
    fn new() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Generate a Lua function library script compatible with the C
/// `generateFunctionScript`. Used by the FUNCTION_LOAD / FCALL benchmarks.
pub fn generate_function_script(num_functions: u32, with_keys: bool) -> String {
    let mut buffer = String::from("#!lua name=benchlib\n");
    let count = num_functions.max(1);
    for n in (1..=count).rev() {
        if with_keys {
            buffer.push_str(&format!(
                "local function foo{n}(keys, args)\nreturn keys[0]\nend\n"
            ));
        } else {
            buffer.push_str(&format!("local function foo{n}()\nreturn 0\nend\n"));
        }
        buffer.push_str(&format!("server.register_function('foo{n}', foo{n})\n"));
    }
    buffer
}

const DEFAULT_TESTS: &[(&str, &str)] = &[
    ("PING_INLINE", "PING"),
    ("PING_MBULK", "PING"),
    ("SET", "SET key:__rand_int__ __data__"),
    ("GET", "GET key:__rand_int__"),
    ("INCR", "INCR counter:__rand_int__"),
    ("LPUSH", "LPUSH list:__rand_int__ __data__"),
    ("RPUSH", "RPUSH list:__rand_int__ __data__"),
    ("LPOP", "LPOP list:__rand_int__"),
    ("RPOP", "RPOP list:__rand_int__"),
    ("SADD", "SADD set:__rand_int__ __data__"),
    ("HSET", "HSET hash:__rand_int__ field __data__"),
    ("SPOP", "SPOP set:__rand_int__"),
    ("ZADD", "ZADD myzset __rand_1st__ ele:__rand_int__"),
    ("ZPOPMIN", "ZPOPMIN myzset"),
    ("LPUSH_LRANGE", "LPUSH lrange:__rand_int__ __data__"),
    ("LRANGE_100", "LRANGE lrange:__rand_int__ 0 99"),
    ("LRANGE_300", "LRANGE lrange:__rand_int__ 0 299"),
    ("LRANGE_500", "LRANGE lrange:__rand_int__ 0 499"),
    ("LRANGE_600", "LRANGE lrange:__rand_int__ 0 599"),
    (
        "MSET",
        "MSET key:__rand_int__ __data__ key:__rand_int__ __data__ key:__rand_int__ __data__ \
         key:__rand_int__ __data__ key:__rand_int__ __data__ key:__rand_int__ __data__ \
         key:__rand_int__ __data__ key:__rand_int__ __data__ key:__rand_int__ __data__ \
         key:__rand_int__ __data__",
    ),
    (
        "MGET",
        "MGET key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ \
         key:__rand_int__ key:__rand_int__ key:__rand_int__ key:__rand_int__ \
         key:__rand_int__ key:__rand_int__",
    ),
    ("XADD", "XADD mystream * myfield __data__"),
];

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn sequential_rand_int_advances_each_occurrence() {
        let mut cfg = BenchConfig::default();
        cfg.keyspace_len = 100;
        cfg.sequential = true;
        let template = TestTemplate {
            name: "SET".to_string(),
            sequence: vec![split_words("SET rain__rand_int__ rain__rand_int__")],
        };
        let mut gen = CommandGenerator::new(&cfg, None);
        let cmd = gen.build_sequence(&template).unwrap().remove(0);
        assert_eq!(
            String::from_utf8(cmd[1].clone()).unwrap(),
            "rain000000000000"
        );
        assert_eq!(
            String::from_utf8(cmd[2].clone()).unwrap(),
            "rain000000000001"
        );
    }

    #[test]
    fn numbered_placeholder_reuses_value_per_command() {
        let mut cfg = BenchConfig::default();
        cfg.keyspace_len = 100;
        cfg.sequential = true;
        let template = TestTemplate {
            name: "SET".to_string(),
            sequence: vec![split_words("SET rain__rand_1st__ rain__rand_1st__")],
        };
        let mut gen = CommandGenerator::new(&cfg, None);
        let cmd = gen.build_sequence(&template).unwrap().remove(0);
        assert_eq!(cmd[1], cmd[2]);
    }

    #[test]
    fn data_placeholder_has_configured_length() {
        let mut cfg = BenchConfig::default();
        cfg.datasize = 42;
        let template = TestTemplate {
            name: "SET".to_string(),
            sequence: vec![split_words("SET k value:__data__")],
        };
        let mut gen = CommandGenerator::new(&cfg, None);
        let cmd = gen.build_sequence(&template).unwrap().remove(0);
        assert_eq!(cmd[2].len(), 48);
    }

    #[test]
    fn command_sequence_expands_numeric_repeat() {
        let args = vec![
            "incr".to_string(),
            "foo".to_string(),
            ";".to_string(),
            "3".to_string(),
            "incr".to_string(),
            "bar".to_string(),
        ];
        let sequence = parse_command_sequence(&args).unwrap();
        assert_eq!(sequence.len(), 4);
        assert_eq!(sequence[0], vec!["incr", "foo"]);
        assert_eq!(sequence[1], vec!["incr", "bar"]);
    }

    #[test]
    fn cluster_tag_placeholder_keeps_hash_tag_shape() {
        let mut cfg = BenchConfig::default();
        cfg.keyspace_len = 100;
        cfg.sequential = true;
        let template = TestTemplate {
            name: "HSET".to_string(),
            sequence: vec![split_words("HSET doc:{tag}:__rand_int__ name item")],
        };
        let mut gen = CommandGenerator::new(&cfg, None);
        let cmd = gen.build_sequence(&template).unwrap().remove(0);
        let key = String::from_utf8(cmd[1].clone()).unwrap();
        assert_eq!(key, "doc:{000000000000}:000000000001");
    }
}
