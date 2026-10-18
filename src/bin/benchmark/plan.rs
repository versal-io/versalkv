use std::time::Duration;

use crate::cli::{BenchConfig, BenchMode, Ratio};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkPlan {
    pub mode: BenchMode,
    pub run_count: u32,
    pub budget: RunBudget,
    pub pipeline: usize,
    pub clients: usize,
    pub threads: usize,
    pub workloads: Vec<WorkloadPlan>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunBudget {
    Requests(u64),
    Time(Duration),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkloadPlan {
    ValkeyTemplates,
    ValkeyCommandSequence,
    MemtierRatio(Ratio),
    MemtierCommands,
}

impl BenchmarkPlan {
    pub fn from_config(config: &BenchConfig) -> Result<Self, String> {
        match config.mode {
            BenchMode::Valkey => Ok(Self::from_valkey(config)),
            BenchMode::MemtierRedis => Self::from_memtier(config),
        }
    }

    fn from_valkey(config: &BenchConfig) -> Self {
        let budget = match config.duration {
            Some(duration_sec) => RunBudget::Time(Duration::from_secs(duration_sec)),
            None => RunBudget::Requests(config.requests),
        };
        let workload = if config.command.is_empty() {
            WorkloadPlan::ValkeyTemplates
        } else {
            WorkloadPlan::ValkeyCommandSequence
        };
        Self {
            mode: BenchMode::Valkey,
            run_count: 1,
            budget,
            pipeline: config.pipeline,
            clients: config.clients,
            threads: config.threads,
            workloads: vec![workload],
        }
    }

    fn from_memtier(config: &BenchConfig) -> Result<Self, String> {
        let memtier = &config.memtier;
        if memtier.requests.is_some() && memtier.test_time_sec.is_some() {
            return Err("--requests and --test-time are mutually exclusive".to_string());
        }
        let budget = if let Some(requests) = memtier.requests {
            RunBudget::Requests(requests)
        } else if let Some(test_time_sec) = memtier.test_time_sec {
            RunBudget::Time(Duration::from_secs(test_time_sec))
        } else {
            RunBudget::Requests(10_000)
        };
        let workload = if memtier.command_specs.is_empty() {
            WorkloadPlan::MemtierRatio(memtier.ratio.unwrap_or(Ratio {
                writes: 1,
                reads: 10,
            }))
        } else {
            WorkloadPlan::MemtierCommands
        };
        Ok(Self {
            mode: BenchMode::MemtierRedis,
            run_count: memtier.run_count.max(1),
            budget,
            pipeline: config.pipeline,
            clients: config.clients,
            threads: config.threads,
            workloads: vec![workload],
        })
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::cli::{BenchConfig, BenchMode, Ratio};

    #[test]
    fn valkey_mode_uses_existing_request_budget() {
        let mut cfg = BenchConfig::default();
        cfg.requests = 25;
        let plan = BenchmarkPlan::from_config(&cfg).unwrap();
        assert_eq!(plan.mode, BenchMode::Valkey);
        assert_eq!(plan.run_count, 1);
        assert_eq!(plan.budget, RunBudget::Requests(25));
    }

    #[test]
    fn memtier_requests_and_test_time_are_mutually_exclusive() {
        let mut cfg = BenchConfig::default();
        cfg.mode = BenchMode::MemtierRedis;
        cfg.memtier.requests = Some(10);
        cfg.memtier.test_time_sec = Some(1);
        let err = BenchmarkPlan::from_config(&cfg).unwrap_err();
        assert!(err.contains("--requests and --test-time are mutually exclusive"));
    }

    #[test]
    fn memtier_ratio_defaults_to_one_to_ten() {
        let mut cfg = BenchConfig::default();
        cfg.mode = BenchMode::MemtierRedis;
        let plan = BenchmarkPlan::from_config(&cfg).unwrap();
        assert_eq!(
            plan.workloads,
            vec![WorkloadPlan::MemtierRatio(Ratio {
                writes: 1,
                reads: 10
            })]
        );
    }
}
