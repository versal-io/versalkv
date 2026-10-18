// versal-check-aof - Valkey-compatible AOF file integrity checker in Rust

#![forbid(unsafe_code)]

use cli::{parse_args, AofCliAction, AofCliError};
use std::process::ExitCode;

#[path = "check_aof/cli.rs"]
mod cli;
#[path = "check_aof/repair.rs"]
mod repair;
#[path = "check_aof/scan.rs"]
mod scan;
#[path = "check_aof/workflow.rs"]
mod workflow;

fn print_usage(program: &str) {
    eprintln!(
        "Usage: {} [--fix|--truncate-to-timestamp $timestamp] <file.manifest|file.aof>",
        program
    );
}

fn run(args: &[String], program: &str) -> ExitCode {
    match parse_args(args) {
        Ok(AofCliAction::Version) => {
            println!("versal-check-aof {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(AofCliAction::Check(options)) => match workflow::check_aof(&options) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                error.report();
                ExitCode::FAILURE
            }
        },
        Err(AofCliError::InvalidTimestamp) => {
            eprintln!("Invalid timestamp, aborting...");
            ExitCode::FAILURE
        }
        Err(AofCliError::PathTooLong) => {
            eprintln!("Error: filepath is too long (exceeds PATH_MAX)");
            print_usage(program);
            ExitCode::FAILURE
        }
        Err(AofCliError::Usage) => {
            print_usage(program);
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    run(&args[1..], &args[0])
}

#[cfg(test)]
#[path = "check_aof/tests.rs"]
mod tests;
