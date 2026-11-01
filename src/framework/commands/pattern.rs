command_macros::portable_command_support!();

pub fn string_match(pattern: &[u8], value: &[u8], nocase: bool) -> bool {
    string_match_inner(pattern, value, nocase, &mut false, 0)
}

fn string_match_inner(
    pattern: &[u8],
    value: &[u8],
    nocase: bool,
    should_skip_longer_matches: &mut bool,
    depth: usize,
) -> bool {
    // Match the reference server's protection against abusive nesting.
    if depth > 1000 {
        return false;
    }
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
                    if string_match_inner(
                        &pattern[pattern_index..],
                        &value[value_index..],
                        nocase,
                        should_skip_longer_matches,
                        depth + 1,
                    ) {
                        return true;
                    }
                    if *should_skip_longer_matches {
                        return false;
                    }
                    value_index += 1;
                }
                // This suffix failed at every remaining input position. An
                // earlier star cannot make it succeed by consuming more bytes.
                *should_skip_longer_matches = true;
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

#[cfg(test)]
mod tests {

    #[test]
    fn repeated_stars_prune_an_impossible_suffix() {
        let mut pattern = b"*a".repeat(40);
        pattern.push(b'b');
        assert!(!super::string_match(&pattern, &[b'a'; 80], false));
        assert!(!super::string_match(&pattern, &[b'A'; 80], true));

        let mut matching_value = vec![b'a'; 80];
        matching_value.push(b'b');
        assert!(super::string_match(&pattern, &matching_value, false));
    }

    #[test]
    fn deeply_nested_stars_are_rejected_without_exhausting_the_stack() {
        assert!(!super::string_match(
            &b"*?".repeat(50_000),
            &vec![b'a'; 50_000],
            false
        ));
    }

    #[test]
    fn backtracking_preserves_alternative_matches() {
        for (pattern, value, expected) in [
            (b"*ab*ac".as_slice(), b"ababac".as_slice(), true),
            (b"*ab*ac", b"ababab", false),
            (b"a**?c", b"abbc", true),
            (b"*a*b", b"aab", true),
            (b"*a*b", b"aac", false),
        ] {
            assert_eq!(super::string_match(pattern, value, false), expected);
        }
    }
}
