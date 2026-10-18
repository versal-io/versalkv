#![forbid(unsafe_code)]

pub mod cli;
mod datagen;
mod dataset;
mod fuzzer;
mod keygen;
mod metrics;
mod output;
mod plan;
mod protocol;
mod runner;
pub mod scheduler;
mod templates;
mod transport;
pub mod workload;

fn main() {
    let raw_args: Vec<String> = std::env::args().collect();
    if let Some(args) = cli::parse_fuzzer_args(&raw_args) {
        let rc = fuzzer::run_fuzzer(
            &args.host,
            args.port,
            args.total_commands,
            args.num_clients,
            args.num_keys,
            &args.loglevel,
            args.fuzz_flags,
        );
        std::process::exit(rc);
    }

    if raw_args.iter().any(|arg| arg == "--help") {
        output::print_help();
        return;
    }
    if raw_args.iter().any(|arg| arg == "--version" || arg == "-v") {
        println!("versal-benchmark {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    let mut config = match cli::parse_args(raw_args) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };
    if let Err(err) = runner::append_stdin_lastarg(&mut config) {
        eprintln!("{err}");
        std::process::exit(1);
    }

    let benchmark_plan = match plan::BenchmarkPlan::from_config(&config) {
        Ok(plan) => plan,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let plan = match templates::CommandPlan::from_config(&config) {
        Ok(plan) => plan,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let dataset = match dataset::Dataset::from_config(&config, &plan) {
        Ok(dataset) => dataset,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let rc = match runner::run_benchmark(&config, &benchmark_plan, &plan, dataset.as_ref()) {
        Ok(runner::RunOutput::Legacy(stats)) => {
            output::print_results(&config, &stats);
            if let Some(path) = config.memtier.json_out_file.as_deref() {
                let summary = output::benchmark_summary_from_stats(&stats);
                if let Err(err) = output::write_json_summary(path, &summary) {
                    eprintln!("Error: {err}");
                    1
                } else {
                    0
                }
            } else {
                0
            }
        }
        Ok(runner::RunOutput::Summary(summary)) => output::print_summary_or_json(&config, &summary)
            .map(|()| 0)
            .unwrap_or_else(|err| {
                eprintln!("Error: {err}");
                1
            }),
        Err(err) => {
            eprintln!("Error: {err}");
            1
        }
    };
    std::process::exit(rc);
}
