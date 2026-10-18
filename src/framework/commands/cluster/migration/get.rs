use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use bytes::Bytes;

pub(crate) fn cluster_getslotmigrations(args: &[Bytes], ctx: &mut ServerContext) -> Value {
    if !args.is_empty() {
        return crate::framework::CommandError::wrong_arity("cluster|getslotmigrations");
    }
    let max_len = ctx.config().server.cluster_slot_migration_log_max_len as usize;
    let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
        return Value::error("ERR This instance has cluster support disabled");
    };
    state.trim_migration_history(max_len);
    Value::Array(
        state
            .migrations
            .views()
            .into_iter()
            .map(|job| {
                Value::map(vec![
                    (
                        Value::bulk("name"),
                        Value::bulk(job.id.as_str().to_string()),
                    ),
                    (
                        Value::bulk("operation"),
                        Value::bulk(job.operation.as_str()),
                    ),
                    (
                        Value::bulk("slot_ranges"),
                        Value::bulk(job.ranges.to_string()),
                    ),
                    (
                        Value::bulk("source_node"),
                        Value::bulk(job.source.as_str().to_string()),
                    ),
                    (
                        Value::bulk("target_node"),
                        Value::bulk(job.target.as_str().to_string()),
                    ),
                    (Value::bulk("create_time"), Value::integer(job.create_time)),
                    (
                        Value::bulk("last_update_time"),
                        Value::integer(job.last_update_time),
                    ),
                    (
                        Value::bulk("last_ack_time"),
                        Value::integer(job.last_ack_time),
                    ),
                    (Value::bulk("state"), Value::bulk(job.phase.as_str())),
                    (
                        Value::bulk("message"),
                        Value::bulk(job.message.unwrap_or_default()),
                    ),
                    (Value::bulk("cow_size"), Value::integer(job.cow_size as i64)),
                    (
                        Value::bulk("remaining_repl_size"),
                        Value::integer(job.remaining_repl_size as i64),
                    ),
                ])
            })
            .collect(),
    )
}
