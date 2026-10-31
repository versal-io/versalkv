use super::LongDouble;
use num_bigint::BigUint;
use rustc_apfloat::Float;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericFormatError;

pub fn format_long_double_human(value: LongDouble) -> Result<String, NumericFormatError> {
    format_float(value)
}

pub(super) fn format_float<F: Float>(value: F) -> Result<String, NumericFormatError> {
    if value.is_nan() {
        return Err(NumericFormatError);
    }
    if value.is_infinite() {
        return Ok(if value.is_negative() { "-inf" } else { "inf" }.to_owned());
    }
    if value.is_zero() {
        return Ok("0".to_owned());
    }
    let bits = value.to_bits();
    let (significand, exponent) = match F::BITS {
        64 => {
            let exponent = ((bits >> 52) & 0x7ff) as i32;
            let fraction = bits & ((1u128 << 52) - 1);
            (
                fraction | (u128::from(exponent != 0) << 52),
                exponent.max(1) - 1023 - 52,
            )
        }
        128 => {
            let exponent = ((bits >> 112) & 0x7fff) as i32;
            let fraction = bits & ((1u128 << 112) - 1);
            (
                fraction | (u128::from(exponent != 0) << 112),
                exponent.max(1) - 16383 - 112,
            )
        }
        80 => {
            let exponent = ((bits >> 64) & 0x7fff) as i32;
            (bits & u128::from(u64::MAX), exponent.max(1) - 16383 - 63)
        }
        _ => return Err(NumericFormatError),
    };
    let scaled = BigUint::from(significand) * BigUint::from(100_000_000_000_000_000u64);
    let mut rounded = if exponent >= 0 {
        scaled << exponent as usize
    } else {
        let shift = (-exponent) as usize;
        let quotient = &scaled >> shift;
        let remainder = &scaled - (&quotient << shift);
        let denominator = BigUint::from(1u8) << shift;
        let twice_remainder = remainder << 1usize;
        if twice_remainder > denominator || (twice_remainder == denominator && quotient.bit(0)) {
            quotient + BigUint::from(1u8)
        } else {
            quotient
        }
    }
    .to_str_radix(10);
    if rounded == "0" {
        return Ok(rounded);
    }
    if rounded.len() <= 17 {
        rounded = format!("{}{}", "0".repeat(18 - rounded.len()), rounded);
    }
    rounded.insert(rounded.len() - 17, '.');
    while rounded.ends_with('0') {
        rounded.pop();
    }
    if rounded.ends_with('.') {
        rounded.pop();
    }
    if value.is_negative() {
        rounded.insert(0, '-');
    }
    Ok(rounded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::memory::numeric::parse::parse_float;
    use rustc_apfloat::ieee::{Double, Quad, X87DoubleExtended};

    fn check_format<F: Float>(maximum_digits: usize) {
        for (input, expected) in [
            ("0", "0"),
            ("-0", "0"),
            ("1.5", "1.5"),
            ("-10", "-10"),
            ("0x1p-18", "0.00000381469726562"),
            ("0x3p-18", "0.00001144409179688"),
            ("-0x3p-18", "-0.00001144409179688"),
            ("1e-20", "0"),
            ("-1e-20", "0"),
            ("1000", "1000"),
        ] {
            let value = parse_float::<F>(input.as_bytes()).unwrap();
            assert_eq!(format_float(value).unwrap(), expected, "{input}");
        }
        assert_eq!(format_float(F::INFINITY).unwrap(), "inf");
        assert_eq!(format_float(-F::INFINITY).unwrap(), "-inf");
        assert!(format_float(F::NAN).is_err());
        let maximum = format_float(F::largest()).unwrap();
        assert_eq!(maximum.len(), maximum_digits);
        assert!(maximum.bytes().all(|byte| byte.is_ascii_digit()));
        let smallest = F::SMALLEST;
        assert!(!smallest.is_zero());
        assert_eq!(format_float(smallest).unwrap(), "0");
    }

    #[test]
    fn decimal_double_formatter_fixed_rounding() {
        check_format::<Double>(309);
        assert_eq!(
            format_float(parse_float::<Double>(b"5e-18").unwrap()).unwrap(),
            "0.00000000000000001"
        );
    }

    #[test]
    fn decimal_quad_formatter_fixed_rounding() {
        check_format::<Quad>(4933);
        assert_eq!(
            format_float(parse_float::<Quad>(b"5e-18").unwrap()).unwrap(),
            "0"
        );
        assert_eq!(
            format_float(parse_float::<Quad>(b"0x1.ffffffffffffffffffffffffffffp-1").unwrap())
                .unwrap(),
            "1"
        );
        assert_eq!(
            format_float(parse_float::<Quad>(b"9007199254740993").unwrap()).unwrap(),
            "9007199254740993"
        );
    }

    #[test]
    fn decimal_x87_formatter_fixed_rounding() {
        check_format::<X87DoubleExtended>(4933);
        assert_eq!(
            format_float(parse_float::<X87DoubleExtended>(b"0x1.fffffffffffffffep-1").unwrap())
                .unwrap(),
            "1"
        );
        assert_eq!(
            format_float(parse_float::<X87DoubleExtended>(b"9007199254740993").unwrap()).unwrap(),
            "9007199254740993"
        );
    }
}
