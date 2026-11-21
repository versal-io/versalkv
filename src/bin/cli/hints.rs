use super::{repl::parse_inline_command, RespValue};

#[derive(Clone, Copy)]
enum ArgumentKind {
    Value,
    Integer,
    Double,
    Token,
    OneOf,
    Block,
}

struct Argument {
    kind: ArgumentKind,
    name: Vec<u8>,
    display_text: Option<Vec<u8>>,
    token: Option<Vec<u8>>,
    children: Vec<Argument>,
    is_optional: bool,
    is_multiple: bool,
    has_multiple_token: bool,
    matched: usize,
    is_token_matched: bool,
    is_name_matched: bool,
    is_all_matched: bool,
}

struct CommandDoc {
    words: Vec<Vec<u8>>,
    arguments: Vec<Argument>,
}

fn reply_bytes(value: &RespValue) -> Result<&[u8], String> {
    match value {
        RespValue::Bulk(bytes) => Ok(c_string_bytes(bytes)),
        RespValue::SimpleString(text) => Ok(c_string_bytes(text.as_bytes())),
        _ => Err("Invalid string in COMMAND DOCS".to_string()),
    }
}

fn c_string_bytes(bytes: &[u8]) -> &[u8] {
    // The C hint engine copies metadata with sdsnew and compares C strings.
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    &bytes[..end]
}

fn map_entries(value: &RespValue) -> Result<Vec<(&[u8], &RespValue)>, String> {
    match value {
        RespValue::Map(entries) => entries
            .iter()
            .map(|(key, value)| Ok((reply_bytes(key)?, value)))
            .collect(),
        RespValue::Array(entries) if entries.len() % 2 == 0 => entries
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| Ok((reply_bytes(&pair[0])?, &pair[1])))
            .collect(),
        RespValue::Error(error) => Err(error.clone()),
        _ => Err("Invalid map in COMMAND DOCS".to_string()),
    }
}

fn parse_arguments(value: &RespValue) -> Result<Vec<Argument>, String> {
    let RespValue::Array(arguments) = value else {
        return Err("Invalid arguments in COMMAND DOCS".to_string());
    };
    arguments.iter().map(parse_argument).collect()
}

fn parse_argument(value: &RespValue) -> Result<Argument, String> {
    let mut argument = Argument {
        kind: ArgumentKind::Value,
        name: Vec::new(),
        display_text: None,
        token: None,
        children: Vec::new(),
        is_optional: false,
        is_multiple: false,
        has_multiple_token: false,
        matched: 0,
        is_token_matched: false,
        is_name_matched: false,
        is_all_matched: false,
    };
    for (key, value) in map_entries(value)? {
        match key {
            b"name" => argument.name = reply_bytes(value)?.to_vec(),
            b"display_text" => argument.display_text = Some(reply_bytes(value)?.to_vec()),
            b"token" => argument.token = Some(reply_bytes(value)?.to_vec()),
            b"type" => {
                argument.kind = match reply_bytes(value)? {
                    b"integer" | b"unix-time" => ArgumentKind::Integer,
                    b"double" => ArgumentKind::Double,
                    b"pure-token" => ArgumentKind::Token,
                    b"oneof" => ArgumentKind::OneOf,
                    b"block" => ArgumentKind::Block,
                    _ => ArgumentKind::Value,
                };
            }
            b"arguments" => argument.children = parse_arguments(value)?,
            b"flags" => {
                let (RespValue::Array(flags) | RespValue::Set(flags)) = value else {
                    return Err("Invalid argument flags in COMMAND DOCS".to_string());
                };
                for flag in flags {
                    match reply_bytes(flag)? {
                        b"optional" => argument.is_optional = true,
                        b"multiple" => argument.is_multiple = true,
                        b"multiple_token" => argument.has_multiple_token = true,
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(argument)
}

fn parse_commands(
    value: &RespValue,
    parent: &[Vec<u8>],
    commands: &mut Vec<CommandDoc>,
) -> Result<(), String> {
    for (name, specs) in map_entries(value)? {
        let mut words = if name.contains(&b'|') {
            Vec::new()
        } else {
            parent.to_vec()
        };
        words.extend(name.split(|byte| *byte == b'|').map(<[u8]>::to_vec));
        let mut arguments = Vec::new();
        for (key, value) in map_entries(specs)? {
            match key {
                b"arguments" => arguments = parse_arguments(value)?,
                b"subcommands" => parse_commands(value, &words, commands)?,
                _ => {}
            }
        }
        commands.push(CommandDoc { words, arguments });
    }
    Ok(())
}

fn normalize_inline_input(input: &str) -> Option<String> {
    // Adapt SDS escapes and line separators to the shared quoted-token parser.
    let bytes = input.as_bytes();
    let mut normalized = Vec::with_capacity(bytes.len());
    let mut quote = None;
    let mut is_word_start = true;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        match (quote, byte, next) {
            (Some(b'"'), b'\\', Some(escaped)) => {
                match escaped {
                    b'a' => normalized.extend_from_slice(b"\\x07"),
                    b'b' => normalized.extend_from_slice(b"\\x08"),
                    _ => normalized.extend_from_slice(&[byte, escaped]),
                }
                index += 2;
                continue;
            }
            (Some(b'\''), b'\\', Some(b'\'')) => {
                normalized.extend_from_slice(b"'\"\\x27\"'");
                index += 2;
                continue;
            }
            (Some(current), byte, _) if current == byte => {
                quote = None;
                normalized.push(byte);
            }
            (None, b'\'' | b'"', _) => {
                quote = Some(byte);
                is_word_start = false;
                normalized.push(byte);
            }
            (None, b' ' | b'\t' | b'\n' | b'\r', _) => {
                is_word_start = true;
                normalized.push(b' ');
            }
            (None, b'\x0b' | b'\x0c', _) if is_word_start => normalized.push(b' '),
            _ => {
                is_word_start = false;
                normalized.push(byte);
            }
        }
        index += 1;
    }
    String::from_utf8(normalized).ok()
}

pub(super) fn completed_words(input: &str) -> Option<Vec<Vec<u8>>> {
    let normalized = normalize_inline_input(input)?;
    let mut words = parse_inline_command(&normalized).ok()?;
    for word in &mut words {
        word.truncate(c_string_bytes(word).len());
    }
    if !input
        .as_bytes()
        .last()
        .is_some_and(|byte| is_c_whitespace(*byte))
    {
        words.pop();
    }
    Some(words)
}

fn is_c_whitespace(byte: u8) -> bool {
    byte.is_ascii_whitespace() || byte == b'\x0b'
}

pub(super) fn hint_for_input(docs: &RespValue, input: &str) -> Result<Option<Vec<u8>>, String> {
    let Some(words) = completed_words(input) else {
        return Ok(None);
    };
    let mut commands = Vec::new();
    parse_commands(docs, &[], &mut commands)?;
    let command = commands
        .iter_mut()
        .filter(|command| {
            command.words.len() <= words.len()
                && command
                    .words
                    .iter()
                    .zip(&words)
                    .all(|(expected, actual)| expected.eq_ignore_ascii_case(actual))
        })
        .max_by_key(|command| command.words.len());
    let Some(command) = command else {
        return Ok(None);
    };
    let arguments = &words[command.words.len()..];
    let mut hint = Vec::new();
    if match_arguments(arguments, &mut command.arguments) == arguments.len() {
        add_arguments_hint(&mut hint, &mut command.arguments, b" ");
    }
    Ok(Some(hint))
}

fn clear_matches(argument: &mut Argument) {
    argument.matched = 0;
    argument.is_token_matched = false;
    argument.is_name_matched = false;
    argument.is_all_matched = false;
    for child in &mut argument.children {
        clear_matches(child);
    }
}

fn match_arguments(words: &[Vec<u8>], arguments: &mut [Argument]) -> usize {
    let mut word_index = 0;
    let mut argument_index = 0;
    while word_index < words.len() && argument_index < arguments.len() {
        let matched = if arguments[argument_index].is_optional {
            let end = arguments[argument_index..]
                .iter()
                .position(|argument| !argument.is_optional)
                .map_or(arguments.len(), |offset| argument_index + offset);
            let matched =
                match_optional_arguments(&words[word_index..], &mut arguments[argument_index..end]);
            argument_index = end;
            matched
        } else {
            let matched = match_argument(&words[word_index..], &mut arguments[argument_index]);
            if matched == 0 {
                return 0;
            }
            argument_index += 1;
            matched
        };
        word_index += matched;
    }
    word_index
}

fn match_optional_arguments(words: &[Vec<u8>], arguments: &mut [Argument]) -> usize {
    let mut word_index = 0;
    let mut previous: Option<usize> = None;
    while word_index < words.len() {
        let next = arguments
            .iter_mut()
            .enumerate()
            .find_map(|(index, argument)| {
                if argument.matched != 0 {
                    return None;
                }
                let matched = match_argument(&words[word_index..], argument);
                (matched != 0).then_some((index, matched))
            });
        let Some((index, matched)) = next else {
            break;
        };
        if let Some(previous) = previous {
            arguments[previous].is_all_matched = true;
        }
        previous = Some(index);
        word_index += matched;
    }
    word_index
}

fn match_argument(words: &[Vec<u8>], argument: &mut Argument) -> usize {
    let mut matched = match_argument_once(words, argument);
    if !argument.is_multiple {
        return matched;
    }
    while argument.is_all_matched && matched < words.len() {
        clear_matches(argument);
        let next = if argument.token.is_some() && !argument.has_multiple_token {
            let next = match_argument_body(&words[matched..], argument);
            argument.is_token_matched = argument.matched != 0;
            next
        } else {
            match_argument_once(&words[matched..], argument)
        };
        if next == 0 {
            break;
        }
        matched += next;
    }
    argument.is_all_matched = false;
    matched
}

fn match_argument_once(words: &[Vec<u8>], argument: &mut Argument) -> usize {
    let Some(word) = words.first() else {
        return 0;
    };
    let mut body = words;
    if let Some(token) = &argument.token {
        if !token.eq_ignore_ascii_case(word) {
            return 0;
        }
        argument.is_token_matched = true;
        argument.matched = 1;
        if matches!(argument.kind, ArgumentKind::Token) {
            argument.is_all_matched = true;
            return 1;
        }
        if words.len() == 1 {
            return 1;
        }
        body = &words[1..];
    }
    if match_argument_body(body, argument) == 0 {
        return 0;
    }
    argument.matched
}

fn match_argument_body(words: &[Vec<u8>], argument: &mut Argument) -> usize {
    let Some(word) = words.first() else {
        return 0;
    };
    match argument.kind {
        ArgumentKind::Block => {
            argument.matched += match_arguments(words, &mut argument.children);
            argument.is_all_matched = argument.children.iter().all(|child| child.is_all_matched);
        }
        ArgumentKind::OneOf => {
            for child in &mut argument.children {
                if match_argument(words, child) != 0 {
                    argument.matched += child.matched;
                    argument.is_all_matched = child.is_all_matched;
                    break;
                }
            }
        }
        kind => {
            let is_valid = match kind {
                ArgumentKind::Integer => numeric_prefix(word, false),
                ArgumentKind::Double => numeric_prefix(word, true),
                _ => true,
            };
            if is_valid {
                argument.matched += 1;
                argument.is_name_matched = true;
                argument.is_all_matched = true;
            } else {
                argument.matched = 0;
                argument.is_name_matched = false;
            }
        }
    }
    argument.matched
}

fn numeric_prefix(word: &[u8], is_double: bool) -> bool {
    // C uses sscanf rather than strict whole-word numeric validation for hints.
    let start = word
        .iter()
        .position(|byte| !is_c_whitespace(*byte))
        .unwrap_or(word.len());
    let mut number = &word[start..];
    if matches!(number.first(), Some(b'+' | b'-')) {
        number = &number[1..];
    }
    if is_double
        && (number
            .get(..3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"inf"))
            || number
                .get(..3)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"nan")))
    {
        return true;
    }
    number.first().is_some_and(u8::is_ascii_digit)
        || (is_double
            && number.first() == Some(&b'.')
            && number.get(1).is_some_and(u8::is_ascii_digit))
}

fn add_separator(hint: &mut Vec<u8>, previous_len: &mut usize, separator: &[u8], is_last: bool) {
    if hint.len() > *previous_len && !is_last {
        hint.extend_from_slice(separator);
        *previous_len = hint.len();
    }
}

fn add_arguments_hint(hint: &mut Vec<u8>, arguments: &mut [Argument], separator: &[u8]) {
    let mut previous_len = hint.len();
    let mut index = 0;
    let count = arguments.len();
    while index < count {
        if !arguments[index].is_optional {
            add_argument_hint(hint, &mut arguments[index]);
            add_separator(hint, &mut previous_len, separator, index == count - 1);
            index += 1;
            continue;
        }
        let end = arguments[index..]
            .iter()
            .position(|argument| !argument.is_optional)
            .map_or(count, |offset| index + offset);
        let mut incomplete = None;
        for (next, argument) in arguments.iter_mut().enumerate().take(end).skip(index) {
            if argument.matched != 0 && !argument.is_all_matched {
                add_argument_hint(hint, argument);
                add_separator(hint, &mut previous_len, separator, index == count - 1);
                incomplete = Some(next);
            }
        }
        if end == count || arguments[end].matched == 0 {
            for (next, argument) in arguments.iter_mut().enumerate().take(end).skip(index) {
                if incomplete != Some(next) {
                    add_argument_hint(hint, argument);
                    add_separator(hint, &mut previous_len, separator, next == count - 1);
                }
            }
        }
        index = end;
    }
}

fn add_text(hint: &mut Vec<u8>, text: &[u8]) {
    hint.extend_from_slice(if text.is_empty() { b"\"\"" } else { text });
}

fn add_argument_body_hint(hint: &mut Vec<u8>, argument: &mut Argument) {
    match argument.kind {
        ArgumentKind::OneOf if argument.matched == 0 => {
            add_arguments_hint(hint, &mut argument.children, b"|");
        }
        ArgumentKind::OneOf => {
            for child in &mut argument.children {
                if child.matched != 0 {
                    add_argument_hint(hint, child);
                }
            }
        }
        ArgumentKind::Block => add_arguments_hint(hint, &mut argument.children, b" "),
        ArgumentKind::Token => {}
        _ if !argument.is_name_matched => {
            add_text(
                hint,
                argument.display_text.as_deref().unwrap_or(&argument.name),
            );
        }
        _ => {}
    }
}

fn add_repeated_hint(hint: &mut Vec<u8>, argument: &mut Argument) {
    if !argument.is_multiple {
        return;
    }
    // Repetitions always describe a fresh complete unit, not its matched prefix.
    clear_matches(argument);
    if !hint.is_empty() {
        hint.push(b' ');
    }
    hint.push(b'[');
    if argument.has_multiple_token {
        if let Some(token) = &argument.token {
            add_text(hint, token);
            if !matches!(argument.kind, ArgumentKind::Token) {
                hint.push(b' ');
            }
        }
    }
    add_argument_body_hint(hint, argument);
    hint.extend_from_slice(b" ...]");
}

fn add_argument_hint(hint: &mut Vec<u8>, argument: &mut Argument) {
    if argument.is_all_matched {
        return;
    }
    let is_optional_unmatched = argument.is_optional && argument.matched == 0;
    if is_optional_unmatched {
        hint.push(b'[');
    }
    if !argument.is_token_matched {
        if let Some(token) = &argument.token {
            add_text(hint, token);
            if !matches!(argument.kind, ArgumentKind::Token) {
                hint.push(b' ');
            }
        }
    }
    add_argument_body_hint(hint, argument);
    add_repeated_hint(hint, argument);
    if is_optional_unmatched {
        hint.push(b']');
    }
}

#[cfg(test)]
mod tests {
    use super::hint_for_input;
    use crate::RespValue;

    fn text(value: &str) -> RespValue {
        RespValue::Bulk(value.as_bytes().to_vec())
    }

    fn fields(items: Vec<(&str, RespValue)>) -> RespValue {
        RespValue::Map(
            items
                .into_iter()
                .map(|(key, value)| (text(key), value))
                .collect(),
        )
    }

    fn argument(
        kind: &str,
        name: &str,
        token: Option<&str>,
        children: Vec<RespValue>,
        flags: &[&str],
    ) -> RespValue {
        let mut items = vec![("type", text(kind)), ("name", text(name))];
        if let Some(token) = token {
            items.push(("token", text(token)));
        }
        if !children.is_empty() {
            items.push(("arguments", RespValue::Array(children)));
        }
        if !flags.is_empty() {
            items.push((
                "flags",
                RespValue::Set(
                    flags
                        .iter()
                        .map(|flag| RespValue::SimpleString((*flag).to_string()))
                        .collect(),
                ),
            ));
        }
        fields(items)
    }

    fn token(value: &str) -> RespValue {
        argument("pure-token", "mode", Some(value), vec![], &[])
    }

    fn value(name: &str) -> RespValue {
        argument("string", name, None, vec![], &[])
    }

    fn document(arguments: Vec<RespValue>) -> RespValue {
        fields(vec![(
            "control",
            fields(vec![("arguments", RespValue::Array(arguments))]),
        )])
    }

    fn check(docs: &RespValue, cases: &[(&str, Option<&str>)]) {
        for (input, expected) in cases {
            assert_eq!(
                hint_for_input(docs, input).unwrap().as_deref(),
                expected.map(str::as_bytes),
                "{input:?}"
            );
        }
    }

    #[test]
    fn matches_only_completed_words_and_rejects_unknown_inputs() {
        let docs = document(vec![value("key"), value("payload")]);
        check(
            &docs,
            &[
                ("", None),
                ("control", None),
                ("missing ", None),
                ("control ", Some("key payload")),
                ("control\n", Some("key payload")),
                ("control\r\nkey\t", Some("payload")),
                ("\x0bcontrol \x0c key ", Some("payload")),
                ("CoNtRoL unfinished", Some("key payload")),
                ("control \"a b\" ", Some("payload")),
                ("control \"\\xff\" ", Some("payload")),
                ("control \"\" ", Some("payload")),
                ("control key payload ", Some("")),
                ("control key payload excess ", Some("")),
                ("control \"unfinished", None),
            ],
        );
        check(
            &document(vec![token("it's"), value("next")]),
            &[("control 'it\\'s' ", Some("next"))],
        );
        check(
            &document(vec![token("\x08\x07"), value("next")]),
            &[("control \"\\b\\a\" ", Some("next"))],
        );
    }

    #[test]
    fn selects_longest_subcommand_prefix_in_both_resp_versions() {
        let docs = fields(vec![(
            "control",
            fields(vec![
                ("arguments", RespValue::Array(vec![value("subcommand")])),
                (
                    "subcommands",
                    fields(vec![(
                        "control|action",
                        fields(vec![(
                            "arguments",
                            RespValue::Array(vec![value("destination")]),
                        )]),
                    )]),
                ),
            ]),
        )]);
        check(
            &docs,
            &[
                ("control ", Some("subcommand")),
                ("control action", Some("subcommand")),
                ("CONTROL AcTiOn ", Some("destination")),
                ("control action target ", Some("")),
            ],
        );
        let resp2 = RespValue::Array(vec![
            text("control"),
            RespValue::Array(vec![
                text("arguments"),
                RespValue::Array(vec![RespValue::Array(vec![
                    text("name"),
                    text("entry"),
                    text("type"),
                    text("key"),
                ])]),
            ]),
        ]);
        check(&resp2, &[("control ", Some("entry"))]);
        let flat = fields(vec![(
            "control|action",
            fields(vec![(
                "arguments",
                RespValue::Array(vec![value("destination")]),
            )]),
        )]);
        check(&flat, &[("control action ", Some("destination"))]);
    }

    #[test]
    fn renders_oneof_and_partial_blocks() {
        let docs = document(vec![argument(
            "oneof",
            "choice",
            None,
            vec![
                token("STOP"),
                argument(
                    "block",
                    "route",
                    Some("ROUTE"),
                    vec![value("host"), value("port")],
                    &[],
                ),
            ],
            &[],
        )]);
        check(
            &docs,
            &[
                ("control ", Some("STOP|ROUTE host port")),
                ("control route ", Some("host port")),
                ("control route server ", Some("port")),
                ("control route server 1 ", Some("")),
                ("control stop ", Some("")),
                ("control unknown ", Some("")),
            ],
        );
    }

    #[test]
    fn renders_repeated_choices_tokens_and_partial_blocks() {
        let choices = document(vec![argument(
            "oneof",
            "mode",
            None,
            vec![token("FIRST"), token("SECOND")],
            &["multiple"],
        )]);
        check(
            &choices,
            &[
                ("control ", Some("FIRST|SECOND [FIRST|SECOND ...]")),
                ("control first ", Some("[FIRST|SECOND ...]")),
                ("control first second ", Some("[FIRST|SECOND ...]")),
                ("control first wrong ", Some("")),
            ],
        );
        for (flags, repeated, input) in [
            (vec!["multiple"], "[item ...]", "control ITEMS one two "),
            (
                vec!["multiple", "multiple_token"],
                "[ITEMS item ...]",
                "control ITEMS one ITEMS two ",
            ),
        ] {
            let docs = document(vec![argument(
                "string",
                "item",
                Some("ITEMS"),
                vec![],
                &flags,
            )]);
            check(&docs, &[(input, Some(repeated))]);
        }
        let pairs = document(vec![argument(
            "block",
            "pair",
            None,
            vec![value("key"), value("value")],
            &["multiple"],
        )]);
        check(
            &pairs,
            &[
                ("control ", Some("key value [key value ...]")),
                ("control k v ", Some("[key value ...]")),
                ("control k v k2 ", Some("value [key value ...]")),
            ],
        );
    }

    #[test]
    fn matches_optional_groups_in_any_order_and_closes_them_at_required_arguments() {
        let docs = document(vec![
            argument("integer", "count", Some("COUNT"), vec![], &["optional"]),
            argument("string", "label", Some("LABEL"), vec![], &["optional"]),
            token("END"),
            value("destination"),
        ]);
        check(
            &docs,
            &[
                (
                    "control ",
                    Some("[COUNT count] [LABEL label] END destination"),
                ),
                ("control LABEL text COUNT ", Some("count END destination")),
                ("control COUNT 2 LABEL ", Some("label END destination")),
                ("control COUNT 2 ", Some("[LABEL label] END destination")),
                ("control END ", Some("destination")),
                ("control END target ", Some("")),
                ("control END target COUNT 2 ", Some("")),
            ],
        );
        let repeated = document(vec![
            argument(
                "integer",
                "number",
                Some("ITEMS"),
                vec![],
                &["optional", "multiple"],
            ),
            argument("pure-token", "done", Some("DONE"), vec![], &["optional"]),
        ]);
        check(&repeated, &[("control ITEMS 1 DONE ", Some(""))]);
    }

    #[test]
    fn preserves_binary_display_text_empty_tokens_and_metadata_errors() {
        let docs = document(vec![fields(vec![
            ("name", text("internal")),
            ("type", text("string")),
            ("token", text("")),
            ("display_text", RespValue::Bulk(vec![0xff, b'x', 0, b'y'])),
        ])]);
        assert_eq!(
            hint_for_input(&docs, "control ").unwrap(),
            Some(vec![b'"', b'"', b' ', 0xff, b'x'])
        );
        assert_eq!(
            hint_for_input(&docs, "control \"\" ").unwrap(),
            Some(vec![0xff, b'x'])
        );
        let binary_token = document(vec![
            fields(vec![
                ("type", text("pure-token")),
                ("token", RespValue::Bulk(vec![0xff])),
            ]),
            value("next"),
        ]);
        check(&binary_token, &[("control \"\\xff\" ", Some("next"))]);
        check(
            &binary_token,
            &[("control \"\\xff\\x00ignored\" ", Some("next"))],
        );
        check(
            &document(vec![fields(vec![
                ("name", text("")),
                ("type", text("string")),
            ])]),
            &[("control ", Some("\"\""))],
        );
        assert!(hint_for_input(&RespValue::Array(vec![text("control")]), "control ").is_err());
        let malformed = document(vec![fields(vec![
            ("name", RespValue::Integer(7)),
            ("type", text("string")),
        ])]);
        assert!(hint_for_input(&malformed, "control ").is_err());
        assert!(hint_for_input(
            &RespValue::Error("NOPERM metadata denied".to_string()),
            "control "
        )
        .is_err());
    }

    #[test]
    fn numeric_arguments_follow_c_prefix_matching() {
        for kind in ["integer", "unix-time", "double"] {
            let docs = document(vec![
                argument(kind, "number", None, vec![], &[]),
                value("next"),
            ]);
            check(
                &docs,
                &[
                    ("control -12suffix ", Some("next")),
                    ("control wrong ", Some("")),
                ],
            );
        }
        let docs = document(vec![
            argument("double", "number", None, vec![], &[]),
            value("next"),
        ]);
        check(
            &docs,
            &[
                ("control .5suffix ", Some("next")),
                ("control inf ", Some("next")),
                ("control nan ", Some("next")),
                ("control + ", Some("")),
            ],
        );
    }
}
