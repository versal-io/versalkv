use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq)]
enum OverflowType {
    Wrap,
    Sat,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BitfieldEncoding {
    signed: bool,
    bits: u8,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BitfieldOperation {
    Get {
        encoding: BitfieldEncoding,
        offset: u64,
    },
    Set {
        encoding: BitfieldEncoding,
        offset: u64,
        value: i64,
        overflow: OverflowType,
    },
    IncrBy {
        encoding: BitfieldEncoding,
        offset: u64,
        increment: i64,
        overflow: OverflowType,
    },
}

struct BitfieldRequest {
    key: Bytes,
    operations: Vec<BitfieldOperation>,
}

impl BitfieldRequest {
    fn parse(args: &CommandArgs, writable: bool) -> Result<Self, Value> {
        if args.args.is_empty() {
            return Err(CommandError::wrong_arity(if writable {
                "bitfield"
            } else {
                "bitfield_ro"
            }));
        }
        let mut operations = Vec::new();
        let mut overflow = OverflowType::Wrap;
        let mut index = 1;
        while index < args.args.len() {
            if args.args[index].eq_ignore_ascii_case(b"overflow") {
                overflow = parse_overflow(args, index)?;
                index += 2;
                continue;
            }
            let (operation, next) = parse_operation(args, index, writable, overflow)?;
            operations.push(operation);
            index = next;
        }
        Ok(Self {
            key: args.args[0].clone(),
            operations,
        })
    }
}

fn parse_overflow(args: &CommandArgs, index: usize) -> Result<OverflowType, Value> {
    if index + 1 >= args.args.len() {
        return Err(CommandError::syntax_error());
    }
    if args.args[index + 1].eq_ignore_ascii_case(b"wrap") {
        Ok(OverflowType::Wrap)
    } else if args.args[index + 1].eq_ignore_ascii_case(b"sat") {
        Ok(OverflowType::Sat)
    } else if args.args[index + 1].eq_ignore_ascii_case(b"fail") {
        Ok(OverflowType::Fail)
    } else {
        Err(CommandError::syntax_error())
    }
}

fn parse_operation(
    args: &CommandArgs,
    index: usize,
    writable: bool,
    overflow: OverflowType,
) -> Result<(BitfieldOperation, usize), Value> {
    let name = &args.args[index];
    if !writable && !name.eq_ignore_ascii_case(b"get") {
        return Err(Value::error(
            "ERR BITFIELD_RO only supports the GET subcommand",
        ));
    }
    if index + 2 >= args.args.len() {
        return Err(CommandError::syntax_error());
    }
    let (signed, bits) = parse_bitfield_type(&args.args[index + 1])?;
    let encoding = BitfieldEncoding { signed, bits };
    let offset = parse_offset(&args.args[index + 2], bits)?;
    validate_field_end(offset, bits)?;
    if name.eq_ignore_ascii_case(b"get") {
        return Ok((BitfieldOperation::Get { encoding, offset }, index + 3));
    }
    if index + 3 >= args.args.len() {
        return Err(CommandError::syntax_error());
    }
    let value = args.parse::<i64>(index + 3)?;
    if name.eq_ignore_ascii_case(b"set") {
        Ok((BitfieldOperation::Set { encoding, offset, value, overflow }, index + 4))
    } else if name.eq_ignore_ascii_case(b"incrby") {
        Ok((BitfieldOperation::IncrBy { encoding, offset, increment: value, overflow }, index + 4))
    } else {
        Err(CommandError::syntax_error())
    }
}

fn validate_field_end(offset: u64, bits: u8) -> Result<(), Value> {
    let end = offset
        .checked_add(bits as u64)
        .ok_or_else(|| Value::error("ERR bit offset is not an integer or out of range"))?;
    if end.div_ceil(8) as usize > MAX_STRING_SIZE {
        return Err(Value::error("ERR string exceeds maximum allowed size"));
    }
    Ok(())
}

fn parse_bitfield_type(s: &[u8]) -> Result<(bool, u8), Value> {
    if s.len() < 2 {
        return Err(Value::error(
            "ERR Invalid bitfield type. Use i<N> or u<N> where N is the number of bits (1-64).",
        ));
    }
    let signed =
        match s[0] {
            b'i' => true,
            b'u' => false,
            _ => return Err(Value::error(
                "ERR Invalid bitfield type. Use i<N> or u<N> where N is the number of bits (1-64).",
            )),
        };
    let bits_str =
        match std::str::from_utf8(&s[1..]) {
            Ok(v) => v,
            Err(_) => return Err(Value::error(
                "ERR Invalid bitfield type. Use i<N> or u<N> where N is the number of bits (1-64).",
            )),
        };
    let bits: u8 =
        match bits_str.parse() {
            Ok(n) => n,
            Err(_) => return Err(Value::error(
                "ERR Invalid bitfield type. Use i<N> or u<N> where N is the number of bits (1-64).",
            )),
        };
    if !(1..=64).contains(&bits) {
        return Err(Value::error(
            "ERR Invalid bitfield type. Use i<N> or u<N> where N is the number of bits (1-64).",
        ));
    }
    Ok((signed, bits))
}

fn parse_offset(s: &[u8], bits: u8) -> Result<u64, Value> {
    if s.is_empty() {
        return Err(Value::error("ERR Invalid bitfield offset"));
    }
    let (raw, multiplier) = if s[0] == b'#' {
        (&s[1..], bits as i64)
    } else {
        (s, 1)
    };
    let value = parse_nonnegative_offset(raw)?;
    value
        .checked_mul(multiplier)
        .map(|value| value as u64)
        .ok_or_else(invalid_offset)
}

fn parse_nonnegative_offset(value: &[u8]) -> Result<i64, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(invalid_offset)
}

fn invalid_offset() -> Value {
    Value::error("ERR bit offset is not an integer or out of range")
}

fn read_bits(buf: &[u8], bit_offset: u64, bits: u8, signed: bool) -> i64 {
    let mut value: u64 = 0;
    for i in 0..bits {
        let pos = bit_offset + i as u64;
        let byte_idx = (pos / 8) as usize;
        if byte_idx < buf.len() {
            let bit_idx = 7 - (pos % 8) as u8;
            let bit = ((buf[byte_idx] >> bit_idx) & 1) as u64;
            value = (value << 1) | bit;
        } else {
            value <<= 1;
        }
    }
    if signed && bits > 0 && bits < 64 {
        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            value |= !((1u64 << bits) - 1);
        }
    }
    value as i64
}

fn write_bits(buf: &mut [u8], bit_offset: u64, bits: u8, value: i64) {
    let uv = value as u64;
    for i in 0..bits {
        let pos = bit_offset + i as u64;
        let byte_idx = (pos / 8) as usize;
        let bit_idx = 7 - (pos % 8) as u8;
        let bit = ((uv >> (bits - 1 - i)) & 1) as u8;
        buf[byte_idx] = (buf[byte_idx] & !(1 << bit_idx)) | (bit << bit_idx);
    }
}

fn apply_overflow(value: i64, bits: u8, signed: bool, overflow: OverflowType) -> Result<i64, ()> {
    match overflow {
        OverflowType::Wrap => Ok(wrap_value(value, bits, signed)),
        OverflowType::Sat => Ok(saturate_value(value, bits, signed)),
        OverflowType::Fail => validate_value(value, bits, signed),
    }
}

fn wrap_value(value: i64, bits: u8, signed: bool) -> i64 {
    let mask = bit_mask(bits);
    let mut wrapped = (value as u64) & mask;
    if signed && bits < 64 && wrapped & (1u64 << (bits - 1)) != 0 {
        wrapped |= !mask;
    }
    wrapped as i64
}

fn saturate_value(value: i64, bits: u8, signed: bool) -> i64 {
    if signed {
        let (min, max) = signed_bounds(bits);
        value.clamp(min, max)
    } else if value < 0 {
        0
    } else {
        (value as u64).min(bit_mask(bits)) as i64
    }
}

fn validate_value(value: i64, bits: u8, signed: bool) -> Result<i64, ()> {
    let valid = if signed {
        let (min, max) = signed_bounds(bits);
        (min..=max).contains(&value)
    } else {
        value >= 0 && (value as u64) <= bit_mask(bits)
    };
    valid.then_some(value).ok_or(())
}

fn signed_bounds(bits: u8) -> (i64, i64) {
    if bits == 64 {
        (i64::MIN, i64::MAX)
    } else {
        (-(1i64 << (bits - 1)), (1i64 << (bits - 1)) - 1)
    }
}

fn bit_mask(bits: u8) -> u64 {
    if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    }
}

struct BitfieldExecution {
    buffer: Vec<u8>,
    key_new: bool,
    modified: bool,
    extended: bool,
    results: Vec<Value>,
}

impl BitfieldExecution {
    fn apply(&mut self, operation: BitfieldOperation) {
        match operation {
            BitfieldOperation::Get { encoding, offset } => self.apply_get(encoding, offset),
            BitfieldOperation::Set {
                encoding,
                offset,
                value,
                overflow,
            } => self.apply_set(encoding, offset, value, overflow),
            BitfieldOperation::IncrBy {
                encoding,
                offset,
                increment,
                overflow,
            } => self.apply_increment(encoding, offset, increment, overflow),
        }
    }

    fn apply_get(&mut self, encoding: BitfieldEncoding, offset: u64) {
        let value = self.read(encoding, offset);
        self.results.push(Value::integer(value));
    }

    fn apply_set(
        &mut self,
        encoding: BitfieldEncoding,
        offset: u64,
        value: i64,
        overflow: OverflowType,
    ) {
        let Ok(final_value) = apply_overflow(value, encoding.bits, encoding.signed, overflow) else {
            self.results.push(Value::null());
            return;
        };
        let old_value = self.read(encoding, offset);
        self.extend(encoding, offset);
        if self.key_new || self.extended || old_value != value {
            write_bits(&mut self.buffer, offset, encoding.bits, final_value);
            self.modified = true;
        }
        self.results.push(Value::integer(old_value));
    }

    fn apply_increment(
        &mut self,
        encoding: BitfieldEncoding,
        offset: u64,
        increment: i64,
        overflow: OverflowType,
    ) {
        let current = self.read(encoding, offset);
        let (value, arithmetic_overflow) = current.overflowing_add(increment);
        if arithmetic_overflow && overflow == OverflowType::Fail {
            self.results.push(Value::null());
            return;
        }
        let Ok(value) = apply_overflow(value, encoding.bits, encoding.signed, overflow) else {
            self.results.push(Value::null());
            return;
        };
        self.extend(encoding, offset);
        write_bits(&mut self.buffer, offset, encoding.bits, value);
        self.modified = true;
        self.results.push(Value::integer(value));
    }

    fn read(&self, encoding: BitfieldEncoding, offset: u64) -> i64 {
        if offset >= (self.buffer.len() as u64) * 8 {
            0
        } else {
            read_bits(&self.buffer, offset, encoding.bits, encoding.signed)
        }
    }

    fn extend(&mut self, encoding: BitfieldEncoding, offset: u64) {
        let required = (offset + encoding.bits as u64).div_ceil(8) as usize;
        if self.buffer.len() < required {
            self.buffer.resize(required, 0);
            self.extended = true;
        }
    }
}

/// BITFIELD and BITFIELD_RO share a parse-first execution pipeline.
pub(super) fn bitfield_impl(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    writable: bool,
) -> CommandOutcome {
    let request = match BitfieldRequest::parse(args, writable) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_index = runtime.selected_db();
    let execution = match execute_bitfield_request(runtime, &request) {
        Ok(execution) => execution,
        Err(error) => return CommandOutcome::reply(error),
    };
    finish_bitfield(runtime, db_index, request, execution)
}

fn execute_bitfield_request(
    runtime: &mut dyn CommandRuntime,
    request: &BitfieldRequest,
) -> Result<BitfieldExecution, Value> {
    let db = runtime
        .db_mut()
        .ok_or_else(CommandError::db_index_out_of_range)?;
    let (buffer, key_new) = match db.get_string(&request.key) {
        Err(_) => return Err(CommandError::wrong_type()),
        Ok(None) => (Vec::new(), true),
        Ok(Some(value)) => (value.to_vec(), false),
    };
    let mut execution = BitfieldExecution {
        buffer,
        key_new,
        modified: false,
        extended: false,
        results: Vec::new(),
    };
    for operation in &request.operations {
        execution.apply(*operation);
    }
    Ok(execution)
}

fn finish_bitfield(
    runtime: &mut dyn CommandRuntime,
    db_index: u32,
    request: BitfieldRequest,
    execution: BitfieldExecution,
) -> CommandOutcome {
    let mut effects = CommandEffects::default();
    if execution.modified {
        if let Some(db) = runtime.db_mut() {
            db.set(
                request.key.to_vec(),
                Robj::string_raw(Bytes::from(execution.buffer)),
            );
        }
        effects.changed_keys.push((db_index, request.key.clone()));
        effects.events.push(CoreEvent {
            kind: "setbit",
            key: request.key,
        });
    } else {
        effects.skip_propagate = true;
    }
    CommandOutcome {
        reply: Value::Array(execution.results),
        effects,
    }
}

fn bitfield_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    if args.is_empty() {
        return Vec::new();
    }
    let mut readonly = true;
    let mut i = 1usize; //  0 = key 1
    while i < args.len() {
        let remargs = args.len() - i - 1;
        let tok = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        if tok == "GET" && remargs >= 2 {
            i += 2;
        } else if (tok == "SET" || tok == "INCRBY") && remargs >= 3 {
            readonly = false;
            break;
        } else if tok == "OVERFLOW" && remargs >= 1 {
            i += 1;
        } else {
            readonly = false; //
            break;
        }
        i += 1;
    }
    let flags = if readonly {
        vec!["RO", "access"]
    } else {
        vec!["RW", "access", "update"]
    };
    vec![(args[0].as_ref(), flags)]
}

#[command("BITFIELD", runtime, get_keys = bitfield_get_keys)]
pub fn bitfield(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    bitfield_impl(runtime, args, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&'static [u8]]) -> CommandArgs {
        let mut argv = vec![Bytes::from_static(b"BITFIELD")];
        argv.extend(parts.iter().map(|part| Bytes::from_static(part)));
        CommandArgs::from_parts(argv)
    }

    #[test]
    fn request_attaches_overflow_policy_to_following_write_operations() {
        let request = BitfieldRequest::parse(
            &args(&[
                b"bits",
                b"OVERFLOW",
                b"SAT",
                b"INCRBY",
                b"i8",
                b"#2",
                b"3",
                b"GET",
                b"u4",
                b"0",
            ]),
            true,
        )
        .unwrap();

        assert!(matches!(
            request.operations[0],
            BitfieldOperation::IncrBy {
                overflow: OverflowType::Sat,
                ..
            }
        ));
        assert!(matches!(request.operations[1], BitfieldOperation::Get { .. }));
    }
}
