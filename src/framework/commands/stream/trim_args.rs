use super::{parse_signed_integer, parse_stream_id_strict, parse_xadd_id};
use crate::engine::core::Value;
use crate::foundation::io::store::structure::stream::{StreamID, ValkeyStream};
use crate::framework::CommandError;
use bytes::Bytes;

#[derive(Clone, Copy)]
enum TrimStrategy {
    MaxLen(usize),
    MinId(StreamID),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(raw: &[&[u8]]) -> Vec<Bytes> {
        raw.iter()
            .map(|part| Bytes::copy_from_slice(part))
            .collect()
    }

    #[test]
    fn add_trim_scanner_and_replay_policy_preserve_target_validation() {
        let raw = parts(&[
            b"s",
            b"LIMIT",
            b"0",
            b"NOMKSTREAM",
            b"MINID",
            b"~",
            b"2-0",
            b"0-*",
            b"f",
            b"v",
        ]);
        let parsed = parse_add_trim_args(&raw, true, false).unwrap();
        assert!(parsed.no_mkstream);
        assert_eq!(parsed.id_index, 7);
        assert_eq!(parsed.limit, Some(0));
        assert_eq!(parsed.id, Some((true, true, StreamID::zero())));
        for (raw, error) in [
            (
                parts(&[b"s", b"MAXLEN", b"-1"]),
                "ERR The MAXLEN argument must be >= 0.",
            ),
            (
                parts(&[b"s", b"MAXLEN", b"+1"]),
                "ERR value is not an integer or out of range",
            ),
            (
                parts(&[b"s", b"MINID", b"+"]),
                "ERR Invalid stream ID specified as stream command argument",
            ),
            (
                parts(&[b"s", b"MAXLEN", b"1", b"MAXLEN", b"2"]),
                "ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
            ),
            (
                parts(&[b"s", b"MAXLEN", b"1", b"LIMIT", b"0"]),
                "ERR syntax error, LIMIT cannot be used without the special ~ option",
            ),
            (
                parts(&[b"s", b"MAXLEN", b"~", b"1", b"LIMIT", b"-1"]),
                "ERR The LIMIT argument must be >= 0.",
            ),
        ] {
            assert_eq!(
                parse_add_trim_args(&raw, false, false).err(),
                Some(Value::error(error))
            );
        }
        let raw = parts(&[b"s", b"MAXLEN", b"1", b"LIMIT", b"0"]);
        assert_eq!(
            parse_add_trim_args(&raw, false, true).unwrap().limit,
            Some(0)
        );
        let raw = parts(&[b"s", b"MAXLEN", b"~", b"1", b"LIMIT", b"-1"]);
        assert_eq!(
            parse_add_trim_args(&raw, false, true).err(),
            Some(Value::error("ERR The LIMIT argument must be >= 0."))
        );
    }

    #[test]
    fn approximate_rewrite_keeps_physical_minid_edge_and_one_append_command() {
        let mut stream = ValkeyStream::new();
        for ms in 1..=3 {
            stream
                .append(StreamID::new(ms, 0), vec![], true, false, 0)
                .unwrap();
        }
        stream.trim_by_id(&StreamID::new(2, 0), false);
        let raw = parts(&[
            b"s", b"LIMIT", b"30", b"MINID", b"~", b"2-0", b"*", b"f", b"v",
        ]);
        let parsed = parse_add_trim_args(&raw, true, false).unwrap();
        assert_eq!(
            parsed.propagation(b"XADD", &raw, &stream, Some(StreamID::new(4, 0))),
            parts(&[b"XADD", b"s", b"MINID", b"=", b"1-0", b"4-0", b"f", b"v"])
        );
        let raw = parts(&[b"s", b"MINID", b"2-0"]);
        let parsed = parse_add_trim_args(&raw, false, false).unwrap();
        assert_eq!(
            parsed.propagation(b"XTRIM", &raw, &stream, None),
            parts(&[b"XTRIM", b"s", b"MINID", b"2-0"])
        );
        stream.trim_by_length(0, false);
        let raw = parts(&[b"s", b"MINID", b"~", b"2-0", b"LIMIT", b"0"]);
        let parsed = parse_add_trim_args(&raw, false, false).unwrap();
        let rewritten = parsed.propagation(b"XTRIM", &raw, &stream, None);
        assert_eq!(rewritten[4], Bytes::from(StreamID::max().fmt_id()));
        assert_eq!(rewritten.len(), 5);
    }
}

#[derive(Default)]
pub(crate) struct AddTrimArgs {
    strategy: Option<TrimStrategy>,
    approx: bool,
    threshold_index: usize,
    limit_indices: Vec<usize>,
    limit: Option<usize>,
    pub no_mkstream: bool,
    pub id_index: usize,
    pub id: Option<(bool, bool, StreamID)>,
}

fn parse_nonnegative(raw: &[u8], name: &str) -> Result<usize, Value> {
    let value = parse_signed_integer(raw)
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
    if value < 0 {
        return Err(Value::error(format!(
            "ERR The {name} argument must be >= 0."
        )));
    }
    Ok(value as usize)
}

pub(crate) fn parse_add_trim_args(
    args: &[Bytes],
    is_xadd: bool,
    must_obey: bool,
) -> Result<AddTrimArgs, Value> {
    let mut parsed = AddTrimArgs::default();
    let mut index = 1;
    while index < args.len() {
        let raw = &args[index];
        let more = args.len() - index - 1;
        if (raw.eq_ignore_ascii_case(b"MAXLEN") || raw.eq_ignore_ascii_case(b"MINID")) && more != 0
        {
            if parsed.strategy.is_some() {
                return Err(Value::error("ERR syntax error, MAXLEN and MINID options at the same time are not compatible"));
            }
            let is_maxlen = raw.eq_ignore_ascii_case(b"MAXLEN");
            index += 1;
            if more >= 2 && (args[index] == b"~".as_slice() || args[index] == b"=".as_slice()) {
                parsed.approx = args[index] == b"~".as_slice();
                index += 1;
            }
            parsed.threshold_index = index;
            parsed.strategy = Some(if is_maxlen {
                TrimStrategy::MaxLen(parse_nonnegative(&args[index], "MAXLEN")?)
            } else {
                TrimStrategy::MinId(parse_stream_id_strict(&args[index])?)
            });
        } else if raw.eq_ignore_ascii_case(b"LIMIT") && more != 0 {
            parsed.limit_indices.push(index);
            index += 1;
            parsed.limit = Some(parse_nonnegative(&args[index], "LIMIT")?);
        } else if is_xadd && raw.eq_ignore_ascii_case(b"NOMKSTREAM") {
            parsed.no_mkstream = true;
        } else if is_xadd {
            parsed.id_index = index;
            parsed.id = Some(parse_xadd_id(raw)?);
            break;
        } else {
            return Err(CommandError::syntax_error());
        }
        index += 1;
    }
    if parsed.limit.is_some_and(|limit| limit != 0) && parsed.strategy.is_none() {
        return Err(Value::error(
            "ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
        ));
    }
    if !is_xadd && parsed.strategy.is_none() {
        return Err(Value::error(
            "ERR syntax error, XTRIM must be called with a trimming strategy",
        ));
    }
    if must_obey {
        parsed.limit = Some(0);
    } else if !parsed.limit_indices.is_empty() && !parsed.approx {
        return Err(Value::error(
            "ERR syntax error, LIMIT cannot be used without the special ~ option",
        ));
    }
    Ok(parsed)
}

impl AddTrimArgs {
    pub fn trim(&self, stream: &mut ValkeyStream, entries: usize, bytes: usize) -> usize {
        match self.strategy {
            Some(TrimStrategy::MaxLen(count)) => stream.trim_by_length_limit_with_node_limits(
                count,
                self.approx,
                self.limit,
                entries,
                bytes,
            ),
            Some(TrimStrategy::MinId(minid)) => stream.trim_by_id_limit_with_node_limits(
                &minid,
                self.approx,
                self.limit,
                entries,
                bytes,
            ),
            None => 0,
        }
    }

    pub fn propagation(
        &self,
        command: &'static [u8],
        args: &[Bytes],
        stream: &ValkeyStream,
        new_id: Option<StreamID>,
    ) -> Vec<Bytes> {
        let mut parts = args.to_vec();
        if self.approx {
            parts[self.threshold_index - 1] = Bytes::from_static(b"=");
            parts[self.threshold_index] = match self.strategy {
                Some(TrimStrategy::MaxLen(_)) => Bytes::from(stream.len().to_string()),
                Some(TrimStrategy::MinId(_)) => Bytes::from(stream.physical_first_id().fmt_id()),
                None => unreachable!(),
            };
        }
        if let Some(id) = new_id {
            if self
                .id
                .is_some_and(|(use_id, seq_auto, _)| !use_id || seq_auto)
            {
                parts[self.id_index] = Bytes::from(id.fmt_id());
            }
        }
        if self.approx {
            if let Some(index) = self.limit_indices.last() {
                parts.drain(*index..*index + 2);
            }
        }
        parts.insert(0, Bytes::from_static(command));
        parts
    }
}
