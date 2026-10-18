use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::observability::slot_metrics::{SlotMetric, SlotOrder, SlotStatsRow};
use crate::framework::CommandError;
use bytes::Bytes;

pub(super) fn cluster_slot_stats(args: &[Bytes], ctx: &ServerContext) -> Value {
    let state = match ctx.cluster_state().as_deref() {
        Some(state) => state,
        None => return Value::error("ERR This instance has cluster support disabled"),
    };
    let topology = state.projection().management_topology();
    let owner_slots = topology.local_shard_slots();

    let key_counts = (0..16384)
        .map(|slot| ctx.dbs().count_keys_in_slot(slot) as u64)
        .collect::<Vec<_>>();
    let rows = if args.len() == 3 && args[0].eq_ignore_ascii_case(b"slotsrange") {
        let start_slot = match parse_slot(&args[1]) {
            Ok(slot) => slot,
            Err(reply) => return reply,
        };
        let end_slot = match parse_slot(&args[2]) {
            Ok(slot) => slot,
            Err(reply) => return reply,
        };
        if start_slot > end_slot {
            return Value::error(format!(
                "ERR Start slot number {} is greater than end slot number {}",
                start_slot, end_slot
            ));
        }
        ctx.slot_metrics()
            .query_range(start_slot, end_slot, &owner_slots, &key_counts)
    } else if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"orderby") {
        let metric = match parse_metric(&args[1], ctx.config().server.cluster_slot_stats_enabled) {
            Ok(metric) => metric,
            Err(reply) => return reply,
        };
        let (order, limit) = match parse_order_options(&args[2..]) {
            Ok(options) => options,
            Err(reply) => return reply,
        };
        ctx.slot_metrics()
            .query_ordered(metric, order, limit, &owner_slots, &key_counts)
    } else {
        return CommandError::syntax_error();
    };
    Value::Array(
        rows.into_iter()
            .map(|row| render_row(row, ctx.config().server.cluster_slot_stats_enabled))
            .collect(),
    )
}

fn parse_metric(raw: &[u8], advanced_enabled: bool) -> Result<SlotMetric, Value> {
    if raw.eq_ignore_ascii_case(b"key-count") {
        return Ok(SlotMetric::KeyCount);
    }
    let metric = if raw.eq_ignore_ascii_case(b"cpu-usec") {
        Some(SlotMetric::CpuUsec)
    } else if raw.eq_ignore_ascii_case(b"network-bytes-in") {
        Some(SlotMetric::NetworkBytesIn)
    } else if raw.eq_ignore_ascii_case(b"network-bytes-out") {
        Some(SlotMetric::NetworkBytesOut)
    } else {
        None
    };
    metric
        .filter(|_| advanced_enabled)
        .ok_or_else(|| Value::error("ERR Unrecognized sort metric for ORDERBY."))
}

fn parse_order_options(args: &[Bytes]) -> Result<(SlotOrder, usize), Value> {
    let mut order = SlotOrder::Desc;
    let mut limit = 16384usize;
    let (mut limit_seen, mut order_seen) = (false, false);
    let mut index = 0;
    while index < args.len() {
        if args[index].eq_ignore_ascii_case(b"limit") && index + 1 < args.len() {
            if limit_seen {
                return Err(Value::error(
                    "ERR Multiple filters of the same type are disallowed.",
                ));
            }
            limit = parse_limit(&args[index + 1])?;
            limit_seen = true;
            index += 2;
        } else if args[index].eq_ignore_ascii_case(b"asc")
            || args[index].eq_ignore_ascii_case(b"desc")
        {
            if order_seen {
                return Err(Value::error(
                    "ERR Multiple filters of the same type are disallowed.",
                ));
            }
            order = if args[index].eq_ignore_ascii_case(b"asc") {
                SlotOrder::Asc
            } else {
                SlotOrder::Desc
            };
            order_seen = true;
            index += 1;
        } else {
            return Err(CommandError::syntax_error());
        }
    }
    Ok((order, limit))
}

fn parse_limit(raw: &[u8]) -> Result<usize, Value> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|limit| (1..=16384).contains(limit))
        .ok_or_else(|| {
            Value::error("ERR Limit has to lie in between 1 and 16384 (maximum number of slots).")
        })
}

fn render_row(row: SlotStatsRow, advanced_enabled: bool) -> Value {
    let mut stats = vec![(
        Value::bulk("key-count"),
        Value::integer(as_resp_integer(row.key_count)),
    )];
    if advanced_enabled {
        stats.extend([
            (
                Value::bulk("cpu-usec"),
                Value::integer(as_resp_integer(row.cpu_usec)),
            ),
            (
                Value::bulk("network-bytes-in"),
                Value::integer(as_resp_integer(row.network_bytes_in)),
            ),
            (
                Value::bulk("network-bytes-out"),
                Value::integer(as_resp_integer(row.network_bytes_out)),
            ),
        ]);
    }
    Value::Array(vec![Value::integer(row.slot as i64), Value::map(stats)])
}

fn as_resp_integer(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn parse_slot(raw: &[u8]) -> Result<usize, Value> {
    match std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
    {
        Some(slot) if (0..16384).contains(&slot) => Ok(slot as usize),
        _ => Err(Value::error("ERR Invalid slot")),
    }
}

#[cfg(test)]
#[path = "slot_stats_tests.rs"]
mod tests;
