use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::stats::FuzzerStats;
use super::worker::fuzz_worker;
use super::{log_msg, LogLevel, DEFAULT_NUM_KEYS};

pub fn run_fuzzer(
    host: &str,
    port: u16,
    total_commands: u64,
    num_clients: u32,
    num_keys: usize,
    log_level: &str,
    _fuzz_flags: u32,
) -> i32 {
    let _ = num_keys;
    let _ = DEFAULT_NUM_KEYS;
    let level = LogLevel::from_str(log_level);

    let stats = Arc::new(FuzzerStats::default());
    let abort = Arc::new(AtomicU64::new(0));

    let commands_per_client = total_commands / num_clients as u64;
    let commands_per_client = if commands_per_client < 1 {
        1
    } else {
        commands_per_client
    };

    println!(
        "Running fuzzer with {} clients, {} commands per client ({} total commands)",
        num_clients, commands_per_client, total_commands
    );

    let start_time = Instant::now();
    let progress_abort = Arc::clone(&abort);
    let progress_stats = Arc::clone(&stats);
    let progress_total = total_commands;
    let progress_handle = std::thread::spawn(move || {
        while progress_abort.load(Ordering::Relaxed) == 0 {
            std::thread::sleep(Duration::from_secs(1));
            let sent = progress_stats.sent();
            let elapsed = start_time.elapsed().as_secs_f64();
            let rps = if elapsed > 0.0 {
                sent as f64 / elapsed
            } else {
                0.0
            };
            let pct = if progress_total > 0 {
                (sent as f64 * 100.0) / progress_total as f64
            } else {
                0.0
            };
            print!(
                "\rProgress: {}/{} commands ({:.1}%), {:.1} rps",
                sent, progress_total, pct, rps
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
            if sent >= progress_total {
                break;
            }
        }
        println!();
    });

    let mut handles = Vec::new();
    for tid in 0..num_clients {
        let h = host.to_string();
        let s = Arc::clone(&stats);
        let a = Arc::clone(&abort);
        let handle = std::thread::Builder::new()
            .name(format!("fuzzer-{}", tid))
            .spawn(move || fuzz_worker(h, port, commands_per_client, tid as usize + 1, s, a, level))
            .expect("failed to spawn fuzzer thread");
        handles.push(handle);
    }

    let mut failed = 0;
    for handle in handles {
        match handle.join() {
            Ok((errors, sent)) => {
                if (level as usize) >= (LogLevel::Debug as usize) && errors.total > 0 {
                    log_msg(LogLevel::Error, level, "=== ERROR SUMMARY ===");
                    log_msg(
                        LogLevel::Error,
                        level,
                        &format!("Total errors: {}", errors.total),
                    );
                    for (i, e) in errors.errors.iter().enumerate() {
                        log_msg(
                            LogLevel::Error,
                            level,
                            &format!("Error {}: {}", i + 1, e.message),
                        );
                        log_msg(LogLevel::Error, level, &format!("  Command: {}", e.command));
                    }
                }
                if sent < commands_per_client {
                    failed += 1;
                }
            }
            Err(_) => failed += 1,
        }
    }

    abort.store(1, Ordering::Relaxed);
    let _ = progress_handle.join();

    let final_sent = stats.sent();
    let final_success = stats.success();
    let final_errs = stats.errs();
    let final_malformed = stats.malformed();
    let final_total_err = stats.total_errs();

    log_msg(LogLevel::Info, level, "=== FINAL STATISTICS ===");
    log_msg(
        LogLevel::Info,
        level,
        &format!("Total commands sent: {}", final_sent),
    );
    log_msg(
        LogLevel::Info,
        level,
        &format!("Success replies: {}", final_success),
    );
    log_msg(
        LogLevel::Info,
        level,
        &format!("Error replies: {}", final_errs),
    );
    log_msg(
        LogLevel::Info,
        level,
        &format!("Malformed replies: {}", final_malformed),
    );
    log_msg(
        LogLevel::Info,
        level,
        &format!("Total errors: {}", final_total_err),
    );
    let success_rate = if final_sent > 0 {
        (final_success as f64 * 100.0) / final_sent as f64
    } else {
        0.0
    };
    log_msg(
        LogLevel::Info,
        level,
        &format!("Success rate: {:.2}%", success_rate),
    );

    log_msg(
        LogLevel::Info,
        level,
        &format!(
            "All client threads completed. {} succeeded, {} failed.",
            num_clients - failed,
            failed
        ),
    );

    if final_malformed > 0 {
        1
    } else {
        0
    }
}
