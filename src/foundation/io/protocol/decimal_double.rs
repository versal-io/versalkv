use bytes::Bytes;

/// Validated single-line decimal text, preserved without numeric conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalDoubleText(Bytes);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidDecimalDouble;

impl DecimalDoubleText {
    pub fn new(text: impl Into<Bytes>) -> Result<Self, InvalidDecimalDouble> {
        let text = text.into();
        if matches!(text.as_ref(), b"inf" | b"-inf") {
            return Ok(Self(text));
        }
        let mut index = usize::from(text.first() == Some(&b'-'));
        let mut has_digit = false;
        let mut has_point = false;
        while let Some(&byte) = text.get(index) {
            if byte.is_ascii_digit() {
                has_digit = true;
            } else if byte == b'.' && !has_point {
                has_point = true;
            } else {
                break;
            }
            index += 1;
        }
        if !has_digit {
            return Err(InvalidDecimalDouble);
        }
        if text
            .get(index)
            .is_some_and(|byte| matches!(byte, b'e' | b'E'))
        {
            index += 1;
            if text
                .get(index)
                .is_some_and(|byte| matches!(byte, b'+' | b'-'))
            {
                index += 1;
            }
            let start = index;
            while text.get(index).is_some_and(u8::is_ascii_digit) {
                index += 1;
            }
            if index == start {
                return Err(InvalidDecimalDouble);
            }
        }
        if index != text.len() {
            return Err(InvalidDecimalDouble);
        }
        Ok(Self(text))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_double_text_validates_without_changing_payload() {
        for text in [
            "0",
            "-0",
            "123456789012345678901",
            ".5",
            "1.",
            "-inf",
            "inf",
            "1e4932",
            "-1.25E-20",
        ] {
            let value = DecimalDoubleText::new(text.to_owned()).unwrap();
            assert_eq!(value.as_bytes(), text.as_bytes());
            assert_eq!(value.clone().bytes(), Bytes::from(text.to_owned()));
        }
        for text in [
            "", "-", "+1", ".", "1.2.3", "nan", "Infinity", "0x1", "1e", "1e+", "1e-2x", " 1",
            "1 ", "1\0", "1\r\n+OK",
        ] {
            assert!(DecimalDoubleText::new(text.to_owned()).is_err(), "{text:?}");
        }
        assert!(DecimalDoubleText::new(vec![0xff]).is_err());
    }
}
