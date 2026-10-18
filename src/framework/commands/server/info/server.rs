use crate::foundation::io::sys::time::ustime;
use crate::foundation::memory::util_version as version;
use crate::framework::context::ServerContext;
use std::env::consts;
use std::process;

use super::util::emit_line;

pub(super) fn gen_server_section(ctx: &ServerContext) -> String {
    let mut s = String::new();
    let stats = ctx.stats();
    let compat = ctx.config().server.extended_redis_compatibility;
    s.push_str("# Server\r\n");
    emit_line(&mut s, "redis_version", version::REDIS_VERSION);
    emit_line(&mut s, "server_name", version::SERVER_NAME);
    emit_line(&mut s, "valkey_version", version::VALKEY_VERSION);
    emit_line(&mut s, "redis_git_sha1", version::REDIS_GIT_SHA1);
    emit_line(&mut s, "redis_git_dirty", 0);
    emit_line(
        &mut s,
        "redis_build_id",
        "0000000000000000000000000000000000000000",
    );
    if compat {
        emit_line(&mut s, "redis_mode", version::SERVER_MODE);
    } else {
        emit_line(&mut s, "server_mode", version::SERVER_MODE);
    }
    emit_line(&mut s, "os", format!("{} {}", consts::OS, consts::ARCH));
    emit_line(&mut s, "arch_bits", version::ARCH_BITS);
    emit_line(&mut s, "monotonic_clock", "POSIX clock_gettime");
    emit_line(&mut s, "multiplexing_api", "epoll");
    emit_line(&mut s, "gcc_version", "0.0.0");
    emit_line(&mut s, "process_id", process::id());
    emit_line(
        &mut s,
        "process_supervised",
        if ctx.config().server.supervised_active {
            ctx.config().server.supervised.as_str()
        } else {
            "no"
        },
    );
    emit_line(&mut s, "run_id", &stats.run_id);
    emit_line(&mut s, "tcp_port", stats.tcp_port);
    emit_line(&mut s, "server_time_usec", ustime());
    emit_line(&mut s, "uptime_in_seconds", stats.uptime_seconds());
    emit_line(&mut s, "uptime_in_days", stats.uptime_seconds() / 86400);
    emit_line(&mut s, "hz", ctx.config().server.hz);
    emit_line(&mut s, "configured_hz", ctx.config().server.hz);
    emit_line(&mut s, "clients_hz", stats.clients_hz);
    emit_line(&mut s, "lru_clock", 0);
    emit_line(&mut s, "executable", "versal-server");
    emit_line(&mut s, "config_file", "");
    emit_line(
        &mut s,
        "io_threads_active",
        usize::from(ctx.io_thread_info().active_threads > 1),
    );
    emit_line(
        &mut s,
        "listener0",
        format!("name=tcp,bind=*,bind=::,port={}", stats.tcp_port),
    );
    s
}

#[cfg(test)]
mod tests {
    use super::gen_server_section;
    use crate::framework::context::{IoThreadInfo, ServerContext};

    #[test]
    fn server_section_reports_live_io_thread_activity() {
        let mut ctx = ServerContext::new(1);
        ctx.set_io_thread_info(IoThreadInfo {
            configured_threads: 5,
            active_threads: 5,
            ..IoThreadInfo::default()
        });

        let section = gen_server_section(&ctx);

        assert!(section.contains("io_threads_active:1\r\n"), "{section}");
    }

    #[test]
    fn server_section_reports_the_effective_client_cron_rate() {
        let mut ctx = ServerContext::new(1);
        ctx.stats_mut().clients_hz = 73;

        let section = gen_server_section(&ctx);

        assert!(section.contains("hz:10\r\n"), "{section}");
        assert!(section.contains("configured_hz:10\r\n"), "{section}");
        assert!(section.contains("clients_hz:73\r\n"), "{section}");
    }
}
