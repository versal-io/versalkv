use crate::framework::context::ServerContext;

pub(super) fn config_set_stats(
    ctx: &mut ServerContext,
    param: &str,
    _val_trimmed: &str,
    _val_bytes: &[u8],
) -> Result<(), String> {
    match param {
        "slowlog-log-slower-than"
        | "slowlog-max-len"
        | "commandlog-execution-slower-than"
        | "commandlog-slow-execution-max-len" => {
            let threshold = ctx.config().server.commandlog_slow_threshold;
            let max_len = ctx.config().server.commandlog_slow_max_len;
            ctx.slowlog_mut().reconfigure(threshold, max_len);
            ctx.commandlog_mut().slow.reconfigure(threshold, max_len);
        }
        "commandlog-request-larger-than" | "commandlog-large-request-max-len" => {
            let threshold = ctx.config().server.large_request_threshold;
            let max_len = ctx.config().server.large_request_max_len;
            ctx.commandlog_mut()
                .large_request
                .reconfigure(threshold, max_len);
        }
        "commandlog-reply-larger-than" | "commandlog-large-reply-max-len" => {
            let threshold = ctx.config().server.large_reply_threshold;
            let max_len = ctx.config().server.large_reply_max_len;
            ctx.commandlog_mut()
                .large_reply
                .reconfigure(threshold, max_len);
        }
        _ => unreachable!(),
    }
    Ok(())
}
