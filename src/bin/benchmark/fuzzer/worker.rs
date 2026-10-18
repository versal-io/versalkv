use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::commands::{get_command_templates, KeyGenerator};
use super::connection::{connect_to_server, reconnect_with_backoff};
use super::mutator::{cmd_to_string, generate_command};
use super::protocol::{read_reply, serialize_resp, ReplyError, ReplyType};
use super::rng::FastRng;
use super::stats::{ErrorList, FuzzerStats};
use super::{log_msg, LogLevel, DEFAULT_NUM_KEYS, RECONNECT_BASE_DELAY_MS, RECONNECT_MAX_ATTEMPTS};

pub(super) fn fuzz_worker(
    host: String,
    port: u16,
    commands_per_client: u64,
    thread_id: usize,
    stats: Arc<FuzzerStats>,
    abort: Arc<AtomicU64>,
    log_level: LogLevel,
) -> (ErrorList, u64) {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
        ^ (thread_id as u64).wrapping_mul(0x9E3779B97F4A7C15)
        ^ std::process::id() as u64;
    let mut rng = FastRng::new(seed);
    let mut key_gen = KeyGenerator::new(&mut rng, DEFAULT_NUM_KEYS);
    let templates = get_command_templates();
    let mut error_list = ErrorList::default();

    let mut stream = match connect_to_server(&host, port) {
        Ok(s) => s,
        Err(e) => {
            log_msg(
                LogLevel::Error,
                log_level,
                &format!("[Thread {}] Failed to connect: {}", thread_id, e),
            );
            return (error_list, 0);
        }
    };

    let mut read_buf = vec![0u8; 65536];
    let mut read_pos = 0;
    let mut commands_sent = 0u64;

    while commands_sent < commands_per_client && abort.load(Ordering::Relaxed) == 0 {
        let cmd = generate_command(&templates, &mut rng, &mut key_gen, false);
        let cmd_str = if (log_level as usize) >= (LogLevel::Debug as usize) {
            cmd_to_string(&cmd)
        } else {
            String::new()
        };
        if (log_level as usize) >= (LogLevel::Debug as usize) {
            log_msg(
                LogLevel::Debug,
                log_level,
                &format!("[Thread {}] -> {}", thread_id, cmd_str),
            );
        }

        let data = serialize_resp(&cmd);
        if let Err(e) = stream.write_all(&data) {
            log_msg(
                LogLevel::Info,
                log_level,
                &format!("[Thread {}] Connection error: {}", thread_id, e),
            );
            if let Some(new_stream) =
                reconnect_with_backoff(&host, port, RECONNECT_MAX_ATTEMPTS, RECONNECT_BASE_DELAY_MS)
            {
                stream = new_stream;
                read_pos = 0;
                continue;
            } else {
                log_msg(
                    LogLevel::Error,
                    log_level,
                    &format!("[Thread {}] Reconnection failed", thread_id),
                );
                break;
            }
        }

        match read_reply(&mut stream, &mut read_buf, &mut read_pos) {
            Ok(reply) => match reply.reply_type {
                ReplyType::Error => {
                    let err_str = String::from_utf8_lossy(&reply.data);
                    let err_start = if err_str.starts_with('-') { 1 } else { 0 };
                    let err_msg = &err_str[err_start..];
                    let is_syntax = err_msg.contains("syntax error")
                        || err_msg.contains("wrong number of arguments")
                        || err_msg.contains("unknown command")
                        || err_msg.contains("unknown subcommand");
                    if is_syntax {
                        error_list.add(
                            format!("Command error: {}", err_msg.lines().next().unwrap_or("")),
                            cmd_str,
                        );
                        stats.incr_err();
                        stats.incr_total_err();
                    } else {
                        stats.incr_success();
                    }
                }
                _ => stats.incr_success(),
            },
            Err(ReplyError::Malformed) => {
                log_msg(
                    LogLevel::Error,
                    log_level,
                    &format!("[Thread {}] Malformed reply detected!", thread_id),
                );
                stats.incr_malformed();
                stats.incr_total_err();
                error_list.add("Malformed reply from server".to_string(), cmd_str);
                if let Some(new_stream) = reconnect_with_backoff(
                    &host,
                    port,
                    RECONNECT_MAX_ATTEMPTS,
                    RECONNECT_BASE_DELAY_MS,
                ) {
                    stream = new_stream;
                    read_pos = 0;
                } else {
                    break;
                }
            }
            Err(ReplyError::Io) => {
                log_msg(
                    LogLevel::Info,
                    log_level,
                    &format!("[Thread {}] Connection I/O error", thread_id),
                );
                if let Some(new_stream) = reconnect_with_backoff(
                    &host,
                    port,
                    RECONNECT_MAX_ATTEMPTS,
                    RECONNECT_BASE_DELAY_MS,
                ) {
                    stream = new_stream;
                    read_pos = 0;
                } else {
                    break;
                }
            }
        }

        commands_sent += 1;
        stats.incr_sent();
    }

    (error_list, commands_sent)
}
