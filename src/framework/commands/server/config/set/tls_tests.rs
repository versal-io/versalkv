use super::{apply_config_set_one, commit_tls_log_config, config_set};
use crate::engine::command::CommandArgs;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::Connection;
use crate::framework::context::ServerContext;
use crate::framework::observability::slowlog::SlowLog;
use bytes::Bytes;

fn seed_logs(ctx: &mut ServerContext) {
    for parameter in [
        "commandlog-execution-slower-than",
        "commandlog-request-larger-than",
        "commandlog-reply-larger-than",
    ] {
        apply_config_set_one(ctx, parameter, "10", b"10").unwrap();
    }
    for parameter in [
        "commandlog-slow-execution-max-len",
        "commandlog-large-request-max-len",
        "commandlog-large-reply-max-len",
    ] {
        apply_config_set_one(ctx, parameter, "4", b"4").unwrap();
    }
    for index in 0..3 {
        let argument = Bytes::from(index.to_string());
        ctx.slowlog_mut()
            .push(vec![argument.clone()], 20, String::new(), None);
        let commandlog = ctx.commandlog_mut();
        for log in [
            &mut commandlog.slow,
            &mut commandlog.large_request,
            &mut commandlog.large_reply,
        ] {
            log.push(vec![argument.clone()], 20, String::new(), None);
        }
    }
}

fn assert_log(log: &SlowLog, threshold: i64, capacity: i64, ids: &[u64]) {
    assert_eq!(log.threshold_us, threshold);
    assert_eq!(log.max_len, capacity);
    assert_eq!(
        log.get(-1).iter().map(|entry| entry.id).collect::<Vec<_>>(),
        ids
    );
}

fn assert_logs(ctx: &ServerContext, threshold: i64, capacity: i64, ids: &[u64]) {
    for log in [
        ctx.slowlog(),
        &ctx.commandlog().slow,
        &ctx.commandlog().large_request,
        &ctx.commandlog().large_reply,
    ] {
        assert_log(log, threshold, capacity, ids);
    }
}

fn tls_log_batch(tls_value: &'static [u8]) -> CommandArgs {
    CommandArgs::from_parts(vec![
        Bytes::from_static(b"CONFIG"),
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"slowlog-log-slower-than"),
        Bytes::from_static(b"100"),
        Bytes::from_static(b"slowlog-max-len"),
        Bytes::from_static(b"1"),
        Bytes::from_static(b"commandlog-request-larger-than"),
        Bytes::from_static(b"100"),
        Bytes::from_static(b"commandlog-large-request-max-len"),
        Bytes::from_static(b"1"),
        Bytes::from_static(b"commandlog-reply-larger-than"),
        Bytes::from_static(b"100"),
        Bytes::from_static(b"commandlog-large-reply-max-len"),
        Bytes::from_static(b"1"),
        Bytes::from_static(b"tls-auth-clients"),
        Bytes::from_static(tls_value),
    ])
}

#[test]
fn tls_log_batch_defers_owner_changes_until_successful_runtime_commit() {
    let mut ctx = ServerContext::new(1);
    let mut conn = Connection::new_fake();
    seed_logs(&mut ctx);

    assert_eq!(
        config_set(&mut conn, &tls_log_batch(b"no"), &mut ctx),
        Value::Null
    );
    assert!(ctx.take_tls_reconfigure_request().is_some());
    assert_eq!(ctx.config().server.commandlog_slow_threshold, 100);
    assert_eq!(ctx.config().server.commandlog_slow_max_len, 1);
    assert_logs(&ctx, 10, 4, &[2, 1, 0]);

    commit_tls_log_config(&mut ctx);
    assert_logs(&ctx, 100, 1, &[2]);
}

#[test]
fn tls_log_batch_validation_failure_preserves_owners_and_history() {
    let mut ctx = ServerContext::new(1);
    let mut conn = Connection::new_fake();
    seed_logs(&mut ctx);

    assert!(matches!(
        config_set(&mut conn, &tls_log_batch(b"invalid"), &mut ctx),
        Value::Error(_)
    ));
    assert!(ctx.take_tls_reconfigure_request().is_none());
    assert_eq!(ctx.config().server.commandlog_slow_threshold, 10);
    assert_eq!(ctx.config().server.commandlog_slow_max_len, 4);
    assert_logs(&ctx, 10, 4, &[2, 1, 0]);
}

#[test]
fn tls_log_batch_runtime_rollback_preserves_owners_and_history() {
    let mut ctx = ServerContext::new(1);
    let mut conn = Connection::new_fake();
    seed_logs(&mut ctx);

    assert_eq!(
        config_set(&mut conn, &tls_log_batch(b"no"), &mut ctx),
        Value::Null
    );
    let request = ctx.take_tls_reconfigure_request().unwrap();
    *ctx.config_mut() = request.previous_config;
    assert_logs(&ctx, 10, 4, &[2, 1, 0]);
    assert_eq!(ctx.config().server.commandlog_slow_threshold, 10);
    assert_eq!(ctx.config().server.commandlog_slow_max_len, 4);
}
