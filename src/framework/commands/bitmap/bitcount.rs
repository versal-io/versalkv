use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BitRangeUnit {
    Byte,
    Bit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BitRange {
    pub start: i64,
    pub end: Option<i64>,
    pub unit: BitRangeUnit,
}

impl BitRange {
    pub(super) fn resolve(self, total: i64) -> Option<(usize, usize)> {
        let end = self.end.unwrap_or(total - 1);
        if self.start < 0 && end < 0 && self.start > end {
            return None;
        }
        let start = normalize_bound(self.start, total);
        let end = normalize_bound(end, total).min(total - 1);
        if total == 0 || start > end {
            None
        } else {
            Some((start as usize, end as usize))
        }
    }
}

fn normalize_bound(value: i64, total: i64) -> i64 {
    if value < 0 {
        (total + value).max(0)
    } else {
        value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BitCountRequest {
    key: Bytes,
    range: Option<BitRange>,
}

impl BitCountRequest {
    fn parse(args: &CommandArgs) -> Result<Self, Value> {
        if args.args.is_empty() {
            return Err(CommandError::wrong_arity("bitcount"));
        }
        if args.args.len() > 4 {
            return Err(CommandError::syntax_error());
        }
        let range = if args.args.len() == 1 {
            None
        } else {
            Some(BitRange {
                start: parse_integer(&args.args[1])?,
                end: args.args.get(2).map(|value| parse_integer(value)).transpose()?,
                unit: parse_unit(args.args.get(3))?,
            })
        };
        Ok(Self {
            key: args.args[0].clone(),
            range,
        })
    }
}

fn parse_integer(value: &[u8]) -> Result<i64, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))
}

fn parse_unit(value: Option<&Bytes>) -> Result<BitRangeUnit, Value> {
    match value {
        None => Ok(BitRangeUnit::Byte),
        Some(value) if value.eq_ignore_ascii_case(b"BYTE") => Ok(BitRangeUnit::Byte),
        Some(value) if value.eq_ignore_ascii_case(b"BIT") => Ok(BitRangeUnit::Bit),
        Some(_) => Err(CommandError::syntax_error()),
    }
}

fn popcount(data: &[u8]) -> u64 {
    data.iter().map(|byte| byte.count_ones() as u64).sum()
}

fn popcount_range(data: &[u8], start_bit: usize, end_bit: usize) -> u64 {
    if data.is_empty() || start_bit > end_bit {
        return 0;
    }
    let start_byte = start_bit / 8;
    if start_byte >= data.len() {
        return 0;
    }
    let end_byte = (end_bit / 8).min(data.len() - 1);
    (start_byte..=end_byte)
        .map(|index| masked_byte(data[index], index, start_bit, end_bit))
        .map(|byte| byte.count_ones() as u64)
        .sum()
}

fn masked_byte(byte: u8, index: usize, start_bit: usize, end_bit: usize) -> u8 {
    let byte_start = index * 8;
    let first = start_bit.saturating_sub(byte_start).min(8);
    let last = end_bit.saturating_sub(byte_start).min(7);
    if first > last {
        return 0;
    }
    let width = last - first + 1;
    let mask = if width == 8 {
        u8::MAX
    } else {
        ((1u16 << width) - 1) as u8
    };
    (byte >> (7 - last)) & mask
}

#[command("BITCOUNT", runtime)]
pub fn bitcount(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(bitcount_reply(runtime, args))
}

fn bitcount_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let request = match BitCountRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let db = match runtime.db_mut() {
        Some(db) => db,
        None => return CommandError::db_index_out_of_range(),
    };
    let bytes = match db.get_string(&request.key) {
        Err(_) => return CommandError::wrong_type(),
        Ok(None) => return Value::integer(0),
        Ok(Some(value)) => value,
    };
    Value::integer(count_bits(bytes, request.range) as i64)
}

fn count_bits(bytes: &[u8], range: Option<BitRange>) -> u64 {
    let Some(range) = range else { return popcount(bytes) };
    let total = match range.unit {
        BitRangeUnit::Byte => bytes.len() as i64,
        BitRangeUnit::Bit => (bytes.len() * 8) as i64,
    };
    let Some((start, end)) = range.resolve(total) else {
        return 0;
    };
    match range.unit {
        BitRangeUnit::Byte => popcount(&bytes[start..=end]),
        BitRangeUnit::Bit => popcount_range(bytes, start, end),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parses_typed_bit_range() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"BITCOUNT"),
            Bytes::from_static(b"bits"),
            Bytes::from_static(b"-4"),
            Bytes::from_static(b"-1"),
            Bytes::from_static(b"BIT"),
        ]);

        let request = BitCountRequest::parse(&args).unwrap();
        assert_eq!(
            request.range,
            Some(BitRange {
                start: -4,
                end: Some(-1),
                unit: BitRangeUnit::Bit,
            })
        );
    }

    #[test]
    fn bit_range_counts_partial_first_and_last_bytes() {
        assert_eq!(popcount_range(&[0b1111_0000, 0b0000_1111], 2, 13), 4);
    }
}
