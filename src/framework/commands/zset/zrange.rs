use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::CommandOutcome;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::{parse_lex_bound, parse_score_range, score_value};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ZRangeMode {
    Rank {
        start: i64,
        stop: i64,
    },
    Score {
        min: (f64, bool),
        max: (f64, bool),
    },
    Lex {
        min: (Bytes, bool),
        max: (Bytes, bool),
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ZRangeLimit {
    pub offset: i64,
    pub count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeKind {
    Rank,
    Score,
    Lex,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ZRangeRequest {
    pub key: Bytes,
    pub mode: ZRangeMode,
    pub reverse: bool,
    pub limit: Option<ZRangeLimit>,
    pub with_scores: bool,
}

struct RangeParseSpec {
    key_index: usize,
    first_bound: usize,
    options_start: usize,
    initial_kind: RangeKind,
    reverse: bool,
    allow_mode_options: bool,
    allow_with_scores: bool,
    allow_limit: bool,
}

impl ZRangeRequest {
    pub(super) fn parse(args: &CommandArgs) -> Result<Self, Value> {
        if args.args.len() < 3 {
            return Err(CommandError::syntax_error());
        }
        Self::parse_with(
            args,
            RangeParseSpec {
                key_index: 0,
                first_bound: 1,
                options_start: 3,
                initial_kind: RangeKind::Rank,
                reverse: false,
                allow_mode_options: true,
                allow_with_scores: true,
                allow_limit: true,
            },
        )
    }

    fn parse_with(args: &CommandArgs, spec: RangeParseSpec) -> Result<Self, Value> {
        let (kind, reverse, limit, with_scores, lex_seen) = parse_range_options(args, &spec)?;
        if limit.is_some_and(|limit| limit.count >= 0) && kind == RangeKind::Rank {
            return Err(CommandError::syntax_error());
        }
        if with_scores && lex_seen {
            return Err(CommandError::syntax_error());
        }
        let mode = parse_range_mode(args, spec.first_bound, kind, reverse)?;
        Ok(Self {
            key: args.args[spec.key_index].clone(),
            mode,
            reverse,
            limit,
            with_scores,
        })
    }
}

fn parse_range_options(
    args: &CommandArgs,
    spec: &RangeParseSpec,
) -> Result<(RangeKind, bool, Option<ZRangeLimit>, bool, bool), Value> {
    let mut kind = spec.initial_kind;
    let mut reverse = spec.reverse;
    let mut limit = None;
    let mut with_scores = false;
    let mut lex_seen = kind == RangeKind::Lex;
    let mut index = spec.options_start;
    while index < args.args.len() {
        let option = &args.args[index];
        if option.eq_ignore_ascii_case(b"byscore") && spec.allow_mode_options {
            kind = RangeKind::Score;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"bylex") && spec.allow_mode_options {
            lex_seen = true;
            if kind != RangeKind::Score {
                kind = RangeKind::Lex;
            }
            index += 1;
        } else if option.eq_ignore_ascii_case(b"rev") && spec.allow_mode_options {
            reverse = true;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"withscores") && spec.allow_with_scores {
            with_scores = true;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"limit") && spec.allow_limit {
            limit = Some(parse_limit(args, index)?);
            index += 3;
        } else {
            return Err(CommandError::syntax_error());
        }
    }
    Ok((kind, reverse, limit, with_scores, lex_seen))
}

fn parse_limit(args: &CommandArgs, index: usize) -> Result<ZRangeLimit, Value> {
    if index + 2 >= args.args.len() {
        return Err(CommandError::syntax_error());
    }
    Ok(ZRangeLimit {
        offset: parse_integer(&args.args[index + 1])?,
        count: parse_integer(&args.args[index + 2])?,
    })
}

fn parse_integer(value: &[u8]) -> Result<i64, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))
}

fn parse_range_mode(
    args: &CommandArgs,
    first: usize,
    kind: RangeKind,
    reverse: bool,
) -> Result<ZRangeMode, Value> {
    match kind {
        RangeKind::Rank => Ok(ZRangeMode::Rank {
            start: parse_integer(&args.args[first])?,
            stop: parse_integer(&args.args[first + 1])?,
        }),
        RangeKind::Score => parse_score_mode(args, first, reverse),
        RangeKind::Lex => parse_lex_mode(args, first, reverse),
    }
}

fn parse_score_mode(args: &CommandArgs, first: usize, reverse: bool) -> Result<ZRangeMode, Value> {
    let left = parse_score_range(&args.args[first])
        .ok_or_else(|| Value::error("ERR min or max is not a float"))?;
    let right = parse_score_range(&args.args[first + 1])
        .ok_or_else(|| Value::error("ERR min or max is not a float"))?;
    let (min, max) = if reverse {
        (right, left)
    } else {
        (left, right)
    };
    Ok(ZRangeMode::Score { min, max })
}

fn parse_lex_mode(args: &CommandArgs, first: usize, reverse: bool) -> Result<ZRangeMode, Value> {
    let left = parse_lex_bound(&args.args[first])
        .ok_or_else(|| Value::error("ERR min or max not valid string range item"))?;
    let right = parse_lex_bound(&args.args[first + 1])
        .ok_or_else(|| Value::error("ERR min or max not valid string range item"))?;
    let (min, max) = if reverse {
        (right, left)
    } else {
        (left, right)
    };
    Ok(ZRangeMode::Lex { min, max })
}

pub(super) fn collect_range_entries(zset: &Robj, request: &ZRangeRequest) -> Vec<(Bytes, f64)> {
    match &request.mode {
        ZRangeMode::Rank { start, stop } => zset.zset_range_by_rank(*start, *stop, request.reverse),
        ZRangeMode::Score { min, max } => {
            let limit = request.limit.unwrap_or(ZRangeLimit {
                offset: 0,
                count: -1,
            });
            zset.zset_range_by_score(*min, *max, limit.offset, limit.count, request.reverse)
        }
        ZRangeMode::Lex { min, max } => {
            let entries = zset.zset_range_by_lex(&min.0, min.1, &max.0, max.1, request.reverse);
            apply_lex_limit(entries, request.limit)
        }
    }
}

fn apply_lex_limit(entries: Vec<(Bytes, f64)>, limit: Option<ZRangeLimit>) -> Vec<(Bytes, f64)> {
    let Some(limit) = limit else { return entries };
    if limit.count < 0 {
        return entries;
    }
    let len = entries.len() as i64;
    let start = limit.offset.max(0);
    let end = (start + limit.count).min(len);
    if start >= len {
        Vec::new()
    } else {
        entries[start as usize..end as usize].to_vec()
    }
}

fn format_range_entries(entries: &[(Bytes, f64)], with_scores: bool, resp3: bool) -> Value {
    let mut output = Vec::new();
    for (member, score) in entries {
        if with_scores && resp3 {
            output.push(Value::Array(vec![
                Value::bulk(member.clone()),
                score_value(*score, true),
            ]));
        } else {
            output.push(Value::bulk(member.clone()));
            if with_scores {
                output.push(score_value(*score, false));
            }
        }
    }
    Value::Array(output)
}

#[command("ZRANGE", runtime)]
pub fn zrange(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match ZRangeRequest::parse(args) {
        Ok(request) => request,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let zset = match db.get_zset_object(&request.key) {
        Ok(None) => return CommandOutcome::reply(Value::Array(Vec::new())),
        Ok(Some(zset)) => zset,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    CommandOutcome::reply(format_range_entries(
        &collect_range_entries(zset, &request),
        request.with_scores,
        resp3,
    ))
}

impl ZRangeRequest {
    pub(super) fn parse_store(args: &CommandArgs) -> Result<Self, Value> {
        if args.args.len() < 4 {
            return Err(CommandError::syntax_error());
        }
        Self::parse_with(
            args,
            RangeParseSpec {
                key_index: 1,
                first_bound: 2,
                options_start: 4,
                initial_kind: RangeKind::Rank,
                reverse: false,
                allow_mode_options: true,
                allow_with_scores: false,
                allow_limit: true,
            },
        )
    }

    pub(super) fn parse_legacy(
        args: &CommandArgs,
        kind: RangeKind,
        reverse: bool,
        allow_with_scores: bool,
    ) -> Result<Self, Value> {
        if args.args.len() < 3 {
            return Err(CommandError::syntax_error());
        }
        Self::parse_with(
            args,
            RangeParseSpec {
                key_index: 0,
                first_bound: 1,
                options_start: 3,
                initial_kind: kind,
                reverse,
                allow_mode_options: false,
                allow_with_scores,
                allow_limit: kind != RangeKind::Rank,
            },
        )
    }
}

pub(super) fn execute_zrange_request(
    runtime: &mut dyn CommandRuntime,
    request: &ZRangeRequest,
) -> CommandOutcome {
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let zset = match db.get_zset_object(&request.key) {
        Ok(None) => return CommandOutcome::reply(Value::Array(Vec::new())),
        Ok(Some(zset)) => zset,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let entries = collect_range_entries(zset, request);
    CommandOutcome::reply(format_range_entries(&entries, request.with_scores, resp3))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&'static [u8]]) -> CommandArgs {
        let mut argv = vec![Bytes::from_static(b"ZRANGE")];
        argv.extend(parts.iter().map(|part| Bytes::from_static(part)));
        CommandArgs::from_parts(argv)
    }

    #[test]
    fn request_parses_score_mode_reverse_limit_and_output() {
        let request = ZRangeRequest::parse(&args(&[
            b"scores",
            b"+inf",
            b"-inf",
            b"BYSCORE",
            b"REV",
            b"LIMIT",
            b"2",
            b"3",
            b"WITHSCORES",
        ]))
        .unwrap();

        assert!(matches!(request.mode, ZRangeMode::Score { .. }));
        assert_eq!(
            request.limit,
            Some(ZRangeLimit {
                offset: 2,
                count: 3
            })
        );
        assert!(request.reverse);
        assert!(request.with_scores);
    }
}
