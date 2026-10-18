command_macros::portable_command_support!();

pub fn string_match(pattern: &[u8], value: &[u8], nocase: bool) -> bool {
    let mut pattern_index = 0;
    let mut value_index = 0;
    while pattern_index < pattern.len() && value_index < value.len() {
        match pattern[pattern_index] {
            b'*' => {
                while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
                    pattern_index += 1;
                }
                if pattern_index == pattern.len() {
                    return true;
                }
                while value_index < value.len() {
                    if string_match(&pattern[pattern_index..], &value[value_index..], nocase) {
                        return true;
                    }
                    value_index += 1;
                }
                return false;
            }
            b'?' => {
                value_index += 1;
                pattern_index += 1;
            }
            b'[' => {
                pattern_index += 1;
                let mut negate = false;
                if pattern_index < pattern.len() && pattern[pattern_index] == b'^' {
                    negate = true;
                    pattern_index += 1;
                }
                let mut matched = false;
                while pattern_index < pattern.len() && pattern[pattern_index] != b']' {
                    if pattern_index + 2 < pattern.len() && pattern[pattern_index + 1] == b'-' {
                        let start = pattern[pattern_index];
                        let end = pattern[pattern_index + 2];
                        let candidate = value[value_index];
                        let (candidate, start, end) = if nocase {
                            (
                                candidate.to_ascii_lowercase(),
                                start.to_ascii_lowercase(),
                                end.to_ascii_lowercase(),
                            )
                        } else {
                            (candidate, start, end)
                        };
                        if (candidate >= start && candidate <= end)
                            || (candidate >= end && candidate <= start)
                        {
                            matched = true;
                        }
                        pattern_index += 3;
                    } else {
                        let candidate = value[value_index];
                        let pattern_byte = pattern[pattern_index];
                        let (candidate, pattern_byte) = if nocase {
                            (
                                candidate.to_ascii_lowercase(),
                                pattern_byte.to_ascii_lowercase(),
                            )
                        } else {
                            (candidate, pattern_byte)
                        };
                        if candidate == pattern_byte {
                            matched = true;
                        }
                        pattern_index += 1;
                    }
                }
                if negate {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                value_index += 1;
                if pattern_index < pattern.len() {
                    pattern_index += 1;
                }
            }
            _ => {
                let candidate = value[value_index];
                let pattern_byte = pattern[pattern_index];
                let (candidate, pattern_byte) = if nocase {
                    (
                        candidate.to_ascii_lowercase(),
                        pattern_byte.to_ascii_lowercase(),
                    )
                } else {
                    (candidate, pattern_byte)
                };
                if candidate != pattern_byte {
                    return false;
                }
                value_index += 1;
                pattern_index += 1;
            }
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len() && value_index == value.len()
}
