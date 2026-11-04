use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::CommandOutcome;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::foundation::io::store::structure::zset::LexBound;
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
        min: LexBound,
        max: LexBound,
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
    pub should_require_key: bool,
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
    allow_xx: bool,
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
                allow_xx: true,
            },
        )
    }

    fn parse_with(args: &CommandArgs, spec: RangeParseSpec) -> Result<Self, Value> {
        let (kind, reverse, limit, with_scores, lex_seen, should_require_key) =
            parse_range_options(args, &spec)?;
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
            should_require_key,
        })
    }
}

fn parse_range_options(
    args: &CommandArgs,
    spec: &RangeParseSpec,
) -> Result<(RangeKind, bool, Option<ZRangeLimit>, bool, bool, bool), Value> {
    let mut kind = spec.initial_kind;
    let mut reverse = spec.reverse;
    let mut limit = None;
    let mut with_scores = false;
    let mut lex_seen = kind == RangeKind::Lex;
    let mut should_require_key = false;
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
        } else if option.eq_ignore_ascii_case(b"xx") && spec.allow_xx {
            should_require_key = true;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"limit") && spec.allow_limit {
            limit = Some(parse_limit(args, index)?);
            index += 3;
        } else {
            return Err(CommandError::syntax_error());
        }
    }
    Ok((
        kind,
        reverse,
        limit,
        with_scores,
        lex_seen,
        should_require_key,
    ))
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
            let limit = request.limit.unwrap_or(ZRangeLimit {
                offset: 0,
                count: -1,
            });
            zset.zset_range_by_lex_bounds_with_limit(
                min.clone(),
                max.clone(),
                limit.offset,
                limit.count,
                request.reverse,
            )
        }
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
        Ok(None) if request.should_require_key => return CommandOutcome::reply(Value::NullArray),
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
                allow_xx: false,
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
                allow_xx: true,
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
        Ok(None) if request.should_require_key => return CommandOutcome::reply(Value::NullArray),
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
    use crate::foundation::io::store::structure::zset::{ZSetLp, ZSetSl};

    fn args(parts: &[&'static [u8]]) -> CommandArgs {
        let mut argv = vec![Bytes::from_static(b"ZRANGE")];
        argv.extend(parts.iter().map(|part| Bytes::from_static(part)));
        CommandArgs::from_parts(argv)
    }

    #[test]
    fn score_bounds_accept_c_numeric_syntax_and_reject_trailing_data() {
        for (input, expected) in [
            (b"0x1.8".as_slice(), Some((1.5, true))),
            (b"( \t0x1p1", Some((2.0, false))),
            (b"\x0b3", Some((3.0, true))),
            (b"-INFINITY", Some((f64::NEG_INFINITY, true))),
            (b"3 ", None),
            (b"0x1tail", None),
            (b"NaN", None),
            (b"(", None),
            (b"", None),
        ] {
            assert_eq!(parse_score_range(input), expected, "{input:?}");
        }
    }

    #[test]
    fn score_range_negative_offsets_are_empty_in_both_encodings() {
        for mut zset in [
            Robj::ZSetListpack(ZSetLp::new()),
            Robj::ZSetSkiplist(ZSetSl::new()),
        ] {
            zset.zset_add(b"a", 1.0);
            for reverse in [false, true] {
                for offset in [-1, i64::MIN, i64::MAX] {
                    assert!(zset
                        .zset_range_by_score((0.0, true), (2.0, true), offset, -1, reverse)
                        .is_empty());
                }
            }
        }
    }

    #[test]
    fn lex_ranges_follow_score_order_with_limits_in_both_encodings() {
        for mut zset in [
            Robj::ZSetListpack(ZSetLp::new()),
            Robj::ZSetSkiplist(ZSetSl::new()),
        ] {
            for (member, score) in [(b"b", 0.0), (b"d", 0.0), (b"a", 1.0), (b"c", 1.0), (b"e", 2.0)] {
                zset.zset_add(member, score);
            }
            assert_eq!(zset.zset_count_by_lex(b"a", true, b"c", true), 1);
            assert_eq!(zset.zset_count_by_lex(b"c", true, b"+", true), 4);
            for (min, max, reverse, offset, count, expected) in [
                (b"[a".as_slice(), b"[c".as_slice(), false, 0, -1, vec!["b"]),
                (b"[c", b"+", false, 0, -1, vec!["d", "a", "c", "e"]),
                (b"-", b"[c", true, 0, -1, vec!["c", "a", "d", "b"]),
                (b"(b", b"(e", true, 0, -1, vec!["c"]),
                (b"[a", b"[c", false, 2, -1, vec!["a", "c"]),
                (b"(b", b"(e", true, 2, -1, vec!["d"]),
                (b"-", b"+", false, 1, -1, vec!["d", "a", "c", "e"]),
                (b"-", b"+", false, -1, 3, vec![]),
                (b"-", b"+", true, -1, -1, vec![]),
                (b"-", b"+", false, i64::MAX, i64::MAX, vec![]),
                (b"-", b"+", false, 1, i64::MAX, vec!["d", "a", "c", "e"]),
                (b"-", b"+", false, 0, 0, vec![]),
                (b"[d", b"[c", false, 0, -1, vec![]),
                (b"-", b"[a", true, 0, -1, vec![]),
            ] {
                let request = ZRangeRequest {
                    key: Bytes::from_static(b"z"),
                    mode: ZRangeMode::Lex {
                        min: parse_lex_bound(min).unwrap(),
                        max: parse_lex_bound(max).unwrap(),
                    },
                    reverse,
                    limit: Some(ZRangeLimit { offset, count }),
                    with_scores: false,
                    should_require_key: false,
                };
                let actual: Vec<_> = collect_range_entries(&zset, &request)
                    .into_iter()
                    .map(|(member, _)| String::from_utf8(member.to_vec()).unwrap())
                    .collect();
                assert_eq!(actual, expected, "{min:?} {max:?} {reverse} {offset} {count}");
            }
        }
    }

    #[test]
    fn lex_ranges_distinguish_literal_sign_members_from_infinity_sentinels() {
        for mut zset in [
            Robj::ZSetListpack(ZSetLp::new()),
            Robj::ZSetSkiplist(ZSetSl::new()),
        ] {
            zset.zset_add(b"-", 0.0);
            zset.zset_add(b"+", 0.0);
            for (bound, expected) in [(b"[-".as_slice(), b"-"), (b"[+", b"+")] {
                let request = ZRangeRequest {
                    key: Bytes::from_static(b"z"),
                    mode: ZRangeMode::Lex {
                        min: parse_lex_bound(bound).unwrap(),
                        max: parse_lex_bound(bound).unwrap(),
                    },
                    reverse: false,
                    limit: None,
                    with_scores: false,
                    should_require_key: false,
                };
                let members: Vec<_> = collect_range_entries(&zset, &request)
                    .into_iter()
                    .map(|(member, _)| member)
                    .collect();

                assert_eq!(members, vec![Bytes::copy_from_slice(expected)]);
            }
        }
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
