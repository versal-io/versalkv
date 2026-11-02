command_macros::portable_command_support!();

pub fn string_match(pattern: &[u8], value: &[u8], nocase: bool) -> bool {
    let mut skip_longer_matches = false;
    string_match_impl(pattern, value, nocase, &mut skip_longer_matches, 0)
}

fn string_match_impl(
    pattern: &[u8],
    value: &[u8],
    nocase: bool,
    skip_longer_matches: &mut bool,
    nesting: usize,
) -> bool {
    if nesting > 1000 {
        return false;
    }
    let mut pattern_index = 0;
    let mut value_index = 0;
    while pattern_index < pattern.len() && value_index <= value.len() {
        match pattern[pattern_index] {
            b'*' => {
                while pattern_index + 1 < pattern.len() && pattern[pattern_index + 1] == b'*' {
                    pattern_index += 1;
                }
                if pattern_index + 1 == pattern.len() {
                    return true;
                }
                let mut candidate = value_index;
                loop {
                    if string_match_impl(
                        &pattern[pattern_index + 1..],
                        &value[candidate..],
                        nocase,
                        skip_longer_matches,
                        nesting + 1,
                    ) {
                        return true;
                    }
                    if *skip_longer_matches {
                        return false;
                    }
                    if candidate >= value.len() {
                        break;
                    }
                    candidate += 1;
                }
                *skip_longer_matches = true;
                return false;
            }
            b'?' => {
                if value_index >= value.len() {
                    return false;
                }
                value_index += 1;
                pattern_index += 1;
            }
            b'[' => {
                if value_index >= value.len() {
                    return false;
                }
                pattern_index += 1;
                let mut negate = false;
                if pattern_index < pattern.len() && pattern[pattern_index] == b'^' {
                    negate = true;
                    pattern_index += 1;
                }
                let mut matched = false;
                while pattern_index < pattern.len() && pattern[pattern_index] != b']' {
                    if pattern[pattern_index] == b'\\' && pattern_index + 1 < pattern.len() {
                        pattern_index += 1;
                        if pattern[pattern_index] == value[value_index] {
                            matched = true;
                        }
                        pattern_index += 1;
                    } else if pattern_index + 2 < pattern.len()
                        && pattern[pattern_index + 1] == b'-'
                    {
                        let mut start = pattern[pattern_index];
                        let mut end = pattern[pattern_index + 2];
                        let mut candidate = value[value_index];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        if nocase {
                            start = start.to_ascii_lowercase();
                            end = end.to_ascii_lowercase();
                            candidate = candidate.to_ascii_lowercase();
                        }
                        matched |= (start..=end).contains(&candidate);
                        pattern_index += 3;
                    } else {
                        if bytes_equal(pattern[pattern_index], value[value_index], nocase) {
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
            b'\\' if pattern_index + 1 < pattern.len() => {
                pattern_index += 1;
                if value_index >= value.len()
                    || !bytes_equal(pattern[pattern_index], value[value_index], nocase)
                {
                    return false;
                }
                value_index += 1;
                pattern_index += 1;
            }
            pattern_byte => {
                if value_index >= value.len()
                    || !bytes_equal(pattern_byte, value[value_index], nocase)
                {
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

fn bytes_equal(left: u8, right: u8, nocase: bool) -> bool {
    if nocase {
        left.eq_ignore_ascii_case(&right)
    } else {
        left == right
    }
}

#[cfg(test)]
mod tests {
    use super::string_match;

    #[test]
    fn character_class_range_can_end_at_closing_bracket() {
        assert!(string_match(b"[a-]", b"^", false));
        assert!(string_match(b"[a-]x", b"^", false));
    }

    #[test]
    fn escaped_character_class_bytes_remain_case_sensitive_in_nocase_mode() {
        assert!(string_match(br"[\m]", b"m", true));
        assert!(!string_match(br"[\M]", b"m", true));
        assert!(string_match(b"[M]", b"m", true));
    }
}
