use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::CommandConn;
use bytes::Bytes;
use command_macros::command;

#[command("LATENCY")]
pub fn latency_cmd(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("latency");
    }
    let subcmd = match std::str::from_utf8(&args.args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return CommandError::unknown(&args.args[0]),
    };
    match subcmd.as_str() {
        "latest" => {
            let mut result = Vec::new();
            for (name, ts, latest, max, sum, cnt) in ctx.latency().latest() {
                result.push(Value::Array(vec![
                    Value::bulk(Bytes::from(name.into_bytes())),
                    Value::integer(ts),
                    Value::integer(latest),
                    Value::integer(max),
                    Value::integer(sum),
                    Value::integer(cnt),
                ]));
            }
            Value::Array(result)
        }
        "history" => {
            if args.args.len() != 2 {
                return Value::error("ERR latency history requires an event name");
            }
            let event = match std::str::from_utf8(&args.args[1]) {
                Ok(s) => s,
                Err(_) => return Value::error("ERR invalid event name"),
            };
            let mut result = Vec::new();
            for (ts, lat) in ctx.latency().history(event) {
                result.push(Value::Array(vec![
                    Value::integer(ts),
                    Value::integer(lat),
                ]));
            }
            Value::Array(result)
        }
        "reset" => {
            let mut resets = 0i64;
            if args.args.len() >= 2 {
                for arg in &args.args[1..] {
                    if let Ok(name) = std::str::from_utf8(arg) {
                        resets += ctx.latency_mut().reset(Some(name)) as i64;
                    }
                }
            } else {
                resets = ctx.latency_mut().reset(None) as i64;
            }
            Value::integer(resets)
        }
        "graph" => {
            if args.args.len() != 2 {
                return Value::error("ERR latency graph requires an event name");
            }
            let event = match std::str::from_utf8(&args.args[1]) {
                Ok(s) => s,
                Err(_) => return Value::error("ERR invalid event name"),
            };
            let graph = ctx.latency().graph(event);
            Value::bulk(Bytes::from(graph.into_bytes()))
        }
        "doctor" => {
            let report = ctx.latency().doctor();
            Value::bulk(Bytes::from(report.into_bytes()))
        }
        "histogram" => {
            let mut result = Vec::new();
            if args.args.len() == 1 {
                let mut names: Vec<(&String, &u64)> =
                    ctx.stats_mut().latency_histos.iter().collect();
                names.sort_by(|a, b| a.0.cmp(b.0));
                for (name, &calls) in names {
                    result.push(Value::bulk(Bytes::from(name.clone().into_bytes())));
                    result.push(build_histo_entry(calls));
                }
            } else {
                let mut matched: Vec<(String, u64)> = Vec::new();
                for arg in &args.args[1..] {
                    let name = match std::str::from_utf8(arg) {
                        Ok(s) => s.to_ascii_lowercase(),
                        Err(_) => continue,
                    };
                    let prefix = format!("{}|", name);
                    for (k, &v) in &ctx.stats_mut().latency_histos {
                        if (k == &name || k.starts_with(&prefix))
                            && !matched.iter().any(|(ek, _)| ek == k)
                        {
                            matched.push((k.clone(), v));
                        }
                    }
                }
                matched.sort_by(|a, b| a.0.cmp(&b.0));
                for (name, calls) in matched {
                    result.push(Value::bulk(Bytes::from(name.into_bytes())));
                    result.push(build_histo_entry(calls));
                }
            }
            Value::Array(result)
        }
        "help" => {
            if args.args.len() > 1 {
                return Value::error(
                    "ERR wrong number of arguments for 'latency|help' command"
                );
            }
            let help = [
                "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands:",
                "LATEST",
                "HISTORY <event>",
                "RESET [<event> ...]",
                "GRAPH <event>",
                "DOCTOR",
                "HISTOGRAM [<command> ...]",
                "HELP",
            ]
            .join("\r\n");
            Value::bulk(Bytes::from(help))
        }
        _ => CommandError::syntax_error(),
    }
}

fn build_histo_entry(calls: u64) -> Value {
    let mut histo_map = Vec::new();
    histo_map.push(Value::bulk(Bytes::from_static(b"calls")));
    histo_map.push(Value::integer(calls as i64));
    histo_map.push(Value::bulk(Bytes::from_static(b"histogram_usec")));
    let mut buckets = Vec::new();
    if calls > 0 {
        buckets.push(Value::integer(1));
        buckets.push(Value::integer(calls as i64));
    }
    histo_map.push(Value::Array(buckets));
    Value::Array(histo_map)
}
