use super::LongDouble;
use rustc_apfloat::{Float, Round, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericParseError;

pub fn parse_long_double(input: &[u8]) -> Result<LongDouble, NumericParseError> {
    parse_float(input)
}

pub(super) fn parse_float<F: Float>(input: &[u8]) -> Result<F, NumericParseError> {
    if input.is_empty() || input.len() >= 5 * 1024 || !input.is_ascii() {
        return Err(NumericParseError);
    }
    let mut index = usize::from(matches!(input[0], b'+' | b'-'));
    let negative = input[0] == b'-';
    let unsigned = &input[index..];
    if unsigned.eq_ignore_ascii_case(b"inf") || unsigned.eq_ignore_ascii_case(b"infinity") {
        return Ok(if negative { -F::INFINITY } else { F::INFINITY });
    }
    let is_hex = unsigned.starts_with(b"0x") || unsigned.starts_with(b"0X");
    if is_hex {
        index += 2;
    }
    let mut has_digit = false;
    let mut has_nonzero_digit = false;
    let mut has_point = false;
    while let Some(&byte) = input.get(index) {
        let is_digit = if is_hex {
            byte.is_ascii_hexdigit()
        } else {
            byte.is_ascii_digit()
        };
        if is_digit {
            has_digit = true;
            has_nonzero_digit |= byte != b'0';
        } else if byte == b'.' && !has_point {
            has_point = true;
        } else {
            break;
        }
        index += 1;
    }
    if !has_digit {
        return Err(NumericParseError);
    }
    let has_exponent = index < input.len();
    if has_exponent {
        let expected = if is_hex { b'p' } else { b'e' };
        if input[index].to_ascii_lowercase() != expected {
            return Err(NumericParseError);
        }
        index += 1;
        if input
            .get(index)
            .is_some_and(|byte| matches!(byte, b'+' | b'-'))
        {
            index += 1;
        }
        let start = index;
        while input.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if start == index || index != input.len() {
            return Err(NumericParseError);
        }
    }
    if !has_nonzero_digit {
        return Ok(if negative { -F::ZERO } else { F::ZERO });
    }
    let text = std::str::from_utf8(input).map_err(|_| NumericParseError)?;
    let normalized;
    let text = if is_hex && !has_exponent {
        normalized = format!("{text}p0");
        normalized.as_str()
    } else {
        text
    };
    let parsed = F::from_str_r(text, Round::NearestTiesToEven).map_err(|_| NumericParseError)?;
    if parsed.value.is_nan()
        || parsed.value.is_infinite()
        || parsed
            .status
            .intersects(Status::INVALID_OP | Status::OVERFLOW)
        || parsed.value.is_zero()
    {
        return Err(NumericParseError);
    }
    Ok(parsed.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustc_apfloat::ieee::{Double, Quad, X87DoubleExtended};

    fn check_syntax<F: Float>() {
        for (text, expected) in [
            ("+.5", "0.5"),
            ("01", "1"),
            ("0x1.8", "1.5"),
            ("0X1", "1"),
            ("-0x1.8P+1", "-3"),
            ("1e+2", "100"),
            (".25", "0.25"),
        ] {
            assert_eq!(
                parse_float::<F>(text.as_bytes()).unwrap().to_bits(),
                F::from_str_r(expected, Round::NearestTiesToEven)
                    .unwrap()
                    .value
                    .to_bits(),
                "{text}"
            );
        }
        for text in [
            "", "+", "-", ".", "1e", "1e+", "0x1p", "0x1p-", "0x", "0x.", "1.2.3", "1tail", " 1",
            "1 ", "1\0", "nan", "-NaN", "NaN(1)",
        ] {
            assert!(parse_float::<F>(text.as_bytes()).is_err(), "{text:?}");
        }
        assert!(parse_float::<F>(&[0xff]).is_err());
        for text in ["inf", "+INFINITY", "-Inf", "-infinity"] {
            let parsed = parse_float::<F>(text.as_bytes()).unwrap();
            assert!(parsed.is_infinite());
            assert_eq!(parsed.is_negative(), text.starts_with('-'));
        }
        for text in ["0e999999999999", "-0e-999999999999", "0x0p999999999999"] {
            let parsed = parse_float::<F>(text.as_bytes()).unwrap();
            assert!(parsed.is_zero());
            assert_eq!(parsed.is_negative(), text.starts_with('-'));
        }
        let longest = vec![b'0'; 5119];
        assert!(parse_float::<F>(&longest).unwrap().is_zero());
        assert!(parse_float::<F>(&vec![b'0'; 5120]).is_err());
    }

    fn check_range<F: Float>(smallest_exponent: i32, largest: &str) {
        let smallest = format!("0x1p{smallest_exponent}");
        assert!(!parse_float::<F>(smallest.as_bytes()).unwrap().is_zero());
        let below = format!("0x1p{}", smallest_exponent - 1);
        assert!(parse_float::<F>(below.as_bytes()).is_err());
        assert!(parse_float::<F>(largest.as_bytes()).unwrap().is_finite());
        assert!(parse_float::<F>(b"1e9999").is_err());
        assert!(parse_float::<F>(b"1e-9999").is_err());
    }

    #[test]
    fn decimal_double_parser_syntax_and_range() {
        check_syntax::<Double>();
        check_range::<Double>(-1074, "0x1.fffffffffffffp1023");
        assert!(parse_float::<Double>(b"1e-4965").is_err());
    }

    #[test]
    fn decimal_quad_parser_syntax_and_range() {
        check_syntax::<Quad>();
        check_range::<Quad>(-16494, "0x1.ffffffffffffffffffffffffffffp16383");
        assert!(!parse_float::<Quad>(b"1e-4965").unwrap().is_zero());
    }

    #[test]
    fn decimal_x87_parser_syntax_and_range() {
        check_syntax::<X87DoubleExtended>();
        check_range::<X87DoubleExtended>(-16445, "0x1.fffffffffffffffep16383");
    }
}
