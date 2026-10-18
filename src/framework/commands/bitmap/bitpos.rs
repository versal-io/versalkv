use super::bitcount::{BitRange, BitRangeUnit};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BitPosRequest {
    key: Bytes,
    bit: u8,
    range: Option<BitRange>,
}

impl BitPosRequest {
    fn parse(args: &CommandArgs) -> Result<Self, Value> {
        if args.args.len() < 2 {
            return Err(CommandError::wrong_arity("bitpos"));
        }
        if args.args.len() > 5 {
            return Err(CommandError::syntax_error());
        }
        let bit = match parse_integer(&args.args[1])? {
            0 => 0,
            1 => 1,
            _ => return Err(Value::error("ERR bit is not an integer or out of range")),
        };
        let range = if args.args.len() < 3 {
            None
        } else {
            let unit = parse_unit(args.args.get(4))?;
            Some(BitRange {
                start: parse_integer(&args.args[2])?,
                end: args.args.get(3).map(|value| parse_integer(value)).transpose()?,
                unit,
            })
        };
        Ok(Self {
            key: args.args[0].clone(),
            bit,
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

fn first_matching_bit(data: &[u8], bit: u8) -> Option<usize> {
    let skip = if bit == 1 { 0 } else { u8::MAX };
    data.iter().enumerate().find_map(|(index, byte)| {
        (*byte != skip).then(|| {
            let within = if bit == 1 {
                byte.leading_zeros()
            } else {
                (!byte).leading_zeros()
            };
            index * 8 + within as usize
        })
    })
}

#[derive(Clone, Copy)]
struct BitWindow {
    start: usize,
    end: usize,
    end_given: bool,
}

fn resolve_window(byte_len: usize, range: BitRange) -> Option<BitWindow> {
    let total = match range.unit {
        BitRangeUnit::Byte => byte_len as i64,
        BitRangeUnit::Bit => (byte_len * 8) as i64,
    };
    if total == 0 {
        return None;
    }
    let mut start = normalize_bound(range.start, total);
    let mut end = normalize_bound(range.end.unwrap_or(total - 1), total);
    match range.unit {
        BitRangeUnit::Byte => start = start.min(total - 1),
        BitRangeUnit::Bit => start = start.min(total),
    }
    end = end.min(total - 1);
    if start > end {
        return None;
    }
    let (start, end) = match range.unit {
        BitRangeUnit::Byte => (start as usize * 8, end as usize * 8 + 7),
        BitRangeUnit::Bit => (start as usize, end as usize),
    };
    Some(BitWindow {
        start,
        end,
        end_given: range.end.is_some(),
    })
}

fn normalize_bound(value: i64, total: i64) -> i64 {
    if value < 0 {
        (total + value).max(0)
    } else {
        value
    }
}

fn find_in_window(data: &[u8], bit: u8, window: BitWindow) -> Option<usize> {
    let first_byte = window.start / 8;
    let last_byte = window.end / 8;
    for (index, byte) in data
        .iter()
        .enumerate()
        .take(last_byte + 1)
        .skip(first_byte)
    {
        let start = if index == first_byte { window.start % 8 } else { 0 };
        let end = if index == last_byte { window.end % 8 } else { 7 };
        let masked = mask_search_byte(*byte, bit, start, end);
        if let Some(position) = first_matching_bit(&[masked], bit) {
            if position <= end {
                return Some(index * 8 + position);
            }
        }
    }
    None
}

fn mask_search_byte(byte: u8, bit: u8, start: usize, end: usize) -> u8 {
    let before = if start == 0 { 0 } else { u8::MAX << (8 - start) };
    let after = if end == 7 { 0 } else { (1u8 << (7 - end)) - 1 };
    if bit == 1 {
        byte & !(before | after)
    } else {
        byte | before | after
    }
}

#[command("BITPOS", runtime)]
pub fn bitpos(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(bitpos_reply(runtime, args))
}

fn bitpos_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let request = match BitPosRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let db = match runtime.db_mut() {
        Some(db) => db,
        None => return CommandError::db_index_out_of_range(),
    };
    let bytes = match db.get_string(&request.key) {
        Err(_) => return CommandError::wrong_type(),
        Ok(None) => return Value::integer(if request.bit == 0 { 0 } else { -1 }),
        Ok(Some(value)) => value,
    };
    Value::integer(execute_bitpos(bytes, &request))
}

fn execute_bitpos(bytes: &[u8], request: &BitPosRequest) -> i64 {
    let Some(range) = request.range else {
        return first_matching_bit(bytes, request.bit)
            .map(|position| position as i64)
            .unwrap_or_else(|| if request.bit == 0 { (bytes.len() * 8) as i64 } else { -1 });
    };
    let Some(window) = resolve_window(bytes.len(), range) else {
        return -1;
    };
    find_in_window(bytes, request.bit, window)
        .map(|position| position as i64)
        .unwrap_or_else(|| {
            if request.bit == 0 && !window.end_given {
                (bytes.len() * 8) as i64
            } else {
                -1
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parses_typed_byte_range() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"BITPOS"),
            Bytes::from_static(b"bits"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"8"),
            Bytes::from_static(b"BYTE"),
        ]);

        let request = BitPosRequest::parse(&args).unwrap();
        assert_eq!(request.bit, 1);
        assert_eq!(
            request.range,
            Some(BitRange {
                start: 2,
                end: Some(8),
                unit: BitRangeUnit::Byte,
            })
        );
    }

    #[test]
    fn bounded_zero_search_does_not_return_implicit_trailing_zero() {
        let request = BitPosRequest {
            key: Bytes::new(),
            bit: 0,
            range: Some(BitRange {
                start: 0,
                end: Some(0),
                unit: BitRangeUnit::Byte,
            }),
        };
        assert_eq!(execute_bitpos(&[u8::MAX], &request), -1);
    }
}
