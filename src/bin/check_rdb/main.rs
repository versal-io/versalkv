// versal-check-rdb - Valkey-compatible RDB file integrity checker in Rust

#![forbid(unsafe_code)]

use check::{scan_rdb, RdbCheckFailure, RdbCheckReport, RdbNotice};
use cli::{parse_args, RdbCheckOptions, RdbCliAction};
use constants::RDB_VERSION;
use state::{print_info, rdb_check_error, write_stats};
use std::fs;
use std::io;
use std::process::ExitCode;

mod check;
mod cli;
mod constants;
mod state;

fn print_usage(program: &str) {
    eprintln!(
        "Usage: {} <rdb-file> [--format table|info|csv] [--stats] [--output <file>]",
        program
    );
}

fn run(args: &[String], program: &str) -> ExitCode {
    match parse_args(args) {
        Ok(RdbCliAction::Version) => {
            println!("versal-check-rdb {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(RdbCliAction::Check(options)) => run_check(&options),
        Err(_) => {
            print_usage(program);
            ExitCode::FAILURE
        }
    }
}

fn run_check(options: &RdbCheckOptions) -> ExitCode {
    let data = match fs::read(&options.file) {
        Ok(data) => data,
        Err(error) => {
            eprintln!("Cannot open file {}: {}", options.file.display(), error);
            return ExitCode::FAILURE;
        }
    };
    println!("[info] Checking RDB file {}", options.file.display());
    match scan_rdb(&data) {
        Ok(report) => finish_success(options, report),
        Err(failure) => finish_failure(failure),
    }
}

fn finish_success(options: &RdbCheckOptions, mut report: RdbCheckReport) -> ExitCode {
    print_notices(&report.notices);
    if report.state.rdbver > RDB_VERSION {
        println!(
            "[offset {}] \\o/ RDB looks OK, but loading requires config 'rdb-version-check relaxed'",
            report.state.final_offset
        );
    } else {
        println!(
            "[offset {}] \\o/ RDB looks OK! \\o/",
            report.state.final_offset
        );
    }
    print_info(&report.state);
    match write_requested_stats(options, &mut report.state) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", error);
            ExitCode::FAILURE
        }
    }
}

fn finish_failure(failure: RdbCheckFailure) -> ExitCode {
    print_notices(&failure.notices);
    rdb_check_error(&failure);
    ExitCode::FAILURE
}

fn print_notices(notices: &[RdbNotice]) {
    for notice in notices {
        println!("[offset {}] {}", notice.offset, notice.message);
    }
}

fn write_requested_stats(options: &RdbCheckOptions, state: &mut state::RdbState) -> io::Result<()> {
    if !options.show_stats {
        return Ok(());
    }
    if let Some(path) = &options.output_file {
        let mut output = fs::File::create(path)?;
        write_stats(state, options.format, &mut output)
    } else {
        write_stats(state, options.format, &mut io::stdout())
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    run(&args[1..], &args[0])
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
