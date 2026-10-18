use crate::cli::{MemtierCommandSpec, Ratio};
use crate::datagen::DataGenerator;
use crate::keygen::{KeyGenConfig, KeyGenerator};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    pub label: String,
    pub args: Vec<Vec<u8>>,
}

pub trait Workload {
    fn next_request(&mut self, request_id: u64) -> Request;
}

#[derive(Clone, Copy)]
enum RatioOp {
    Set,
    Get,
}

pub struct MemtierRatioWorkload {
    cycle: Vec<RatioOp>,
    write_keys: KeyGenerator,
    read_keys: KeyGenerator,
    data: DataGenerator,
}

impl MemtierRatioWorkload {
    pub fn new(
        ratio: Ratio,
        write_keys: KeyGenerator,
        read_keys: KeyGenerator,
        data: DataGenerator,
    ) -> Self {
        let mut cycle = Vec::with_capacity((ratio.writes + ratio.reads) as usize);
        cycle.extend((0..ratio.writes).map(|_| RatioOp::Set));
        cycle.extend((0..ratio.reads).map(|_| RatioOp::Get));
        if cycle.is_empty() {
            cycle.push(RatioOp::Get);
        }
        Self {
            cycle,
            write_keys,
            read_keys,
            data,
        }
    }
}

impl Workload for MemtierRatioWorkload {
    fn next_request(&mut self, request_id: u64) -> Request {
        let op = self.cycle[request_id as usize % self.cycle.len()];
        match op {
            RatioOp::Set => Request {
                label: "SET".to_string(),
                args: vec![
                    b"SET".to_vec(),
                    self.write_keys.next_key(),
                    self.data.next_value(),
                ],
            },
            RatioOp::Get => Request {
                label: "GET".to_string(),
                args: vec![b"GET".to_vec(), self.read_keys.next_key()],
            },
        }
    }
}

pub struct MemtierCommandWorkload {
    cycle: Vec<CommandCycleEntry>,
    keys: Vec<KeyGenerator>,
    data: DataGenerator,
}

#[derive(Clone)]
struct CommandCycleEntry {
    spec: MemtierCommandSpec,
    key_idx: usize,
}

impl MemtierCommandWorkload {
    pub fn new(
        specs: Vec<MemtierCommandSpec>,
        key_config: KeyGenConfig,
        seed_base: u64,
        data: DataGenerator,
    ) -> Result<Self, String> {
        let mut cycle = Vec::new();
        let mut keys = Vec::with_capacity(specs.len());
        for spec in specs {
            if spec.argv.is_empty() {
                return Err("Memtier command spec must not be empty".to_string());
            }
            let key_idx = keys.len();
            keys.push(KeyGenerator::new(
                KeyGenConfig {
                    prefix: key_config.prefix.clone(),
                    minimum: key_config.minimum,
                    maximum: key_config.maximum,
                    pattern: spec.key_pattern,
                    client_id: key_config.client_id,
                    total_clients: key_config.total_clients,
                },
                seed_base ^ key_idx as u64,
            ));
            cycle.extend((0..spec.ratio).map(|_| CommandCycleEntry {
                spec: spec.clone(),
                key_idx,
            }));
        }
        if cycle.is_empty() {
            return Err("Memtier command specs must not be empty".to_string());
        }
        Ok(Self { cycle, keys, data })
    }
}

impl Workload for MemtierCommandWorkload {
    fn next_request(&mut self, request_id: u64) -> Request {
        let entry = &self.cycle[request_id as usize % self.cycle.len()];
        let spec = &entry.spec;
        let label = spec.argv[0].to_ascii_uppercase();
        let mut args = Vec::with_capacity(spec.argv.len());
        let keys = &mut self.keys[entry.key_idx];
        for arg in &spec.argv {
            args.push(match arg.as_str() {
                "__key__" => keys.next_key(),
                "__data__" => self.data.next_value(),
                _ => arg.as_bytes().to_vec(),
            });
        }
        Request { label, args }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::KeyPattern;
    use crate::datagen::DataGenConfig;
    use crate::keygen::KeyGenConfig;

    fn test_key_cfg(pattern: KeyPattern) -> KeyGenConfig {
        KeyGenConfig {
            prefix: "memtier-".to_string(),
            minimum: 0,
            maximum: 10,
            pattern,
            client_id: 0,
            total_clients: 1,
        }
    }

    #[test]
    fn ratio_one_to_two_generates_set_get_get_cycle() {
        let mut workload = MemtierRatioWorkload::new(
            Ratio {
                writes: 1,
                reads: 2,
            },
            KeyGenerator::new(test_key_cfg(KeyPattern::Sequential), 1),
            KeyGenerator::new(test_key_cfg(KeyPattern::Sequential), 2),
            DataGenerator::new(DataGenConfig::fixed(3), 3),
        );
        assert_eq!(workload.next_request(0).label, "SET");
        assert_eq!(workload.next_request(1).label, "GET");
        assert_eq!(workload.next_request(2).label, "GET");
        assert_eq!(workload.next_request(3).label, "SET");
    }

    #[test]
    fn memtier_command_replaces_key_and_data_placeholders() {
        let spec = MemtierCommandSpec {
            argv: vec!["SET".into(), "__key__".into(), "__data__".into()],
            ratio: 1,
            key_pattern: KeyPattern::Sequential,
        };
        let mut workload = MemtierCommandWorkload::new(
            vec![spec],
            test_key_cfg(KeyPattern::Sequential),
            1,
            DataGenerator::new(DataGenConfig::fixed(5), 2),
        )
        .unwrap();
        let req = workload.next_request(0);
        assert_eq!(req.label, "SET");
        assert_eq!(req.args[0], b"SET".to_vec());
        assert_eq!(req.args[1], b"memtier-0".to_vec());
        assert_eq!(req.args[2], b"xxxxx".to_vec());
    }

    #[test]
    fn memtier_command_rejects_empty_spec_list() {
        let result = MemtierCommandWorkload::new(
            Vec::new(),
            test_key_cfg(KeyPattern::Sequential),
            1,
            DataGenerator::new(DataGenConfig::fixed(5), 2),
        );
        let Err(err) = result else {
            panic!("empty command spec list should be rejected");
        };
        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn memtier_command_rejects_empty_argv() {
        let spec = MemtierCommandSpec {
            argv: Vec::new(),
            ratio: 1,
            key_pattern: KeyPattern::Sequential,
        };
        let result = MemtierCommandWorkload::new(
            vec![spec],
            test_key_cfg(KeyPattern::Sequential),
            1,
            DataGenerator::new(DataGenConfig::fixed(5), 2),
        );
        let Err(err) = result else {
            panic!("empty command argv should be rejected");
        };
        assert!(err.contains("must not be empty"));
    }
}
