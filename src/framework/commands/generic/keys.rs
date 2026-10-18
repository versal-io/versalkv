use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("KEYS", runtime)]
pub fn keys(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let no_lazy_expire = runtime.suppress_lazy_expire();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    CommandOutcome::reply(keys_reply(db, &args.args[0], no_lazy_expire))
}

fn keys_reply(db: &mut Db, pattern: &[u8], no_lazy_expire: bool) -> Value {
    db.set_no_lazy_expire(no_lazy_expire);

    let match_all = pattern == b"*";
    let mut out: Vec<Value> = Vec::new();
    db.for_each_entry(|key, _val, _expire| {
        if match_all || glob_match(pattern, key) {
            out.push(Value::bulk(key.to_vec()));
        }
    });
    db.set_no_lazy_expire(false);
    Value::Array(out)
}

fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut skip_longer_matches = false;
    glob_match_impl(pattern, string, &mut skip_longer_matches, 0)
}

fn glob_match_impl(
    pattern: &[u8],
    string: &[u8],
    skip_longer_matches: &mut bool,
    nesting: usize,
) -> bool {
    if nesting > 1000 {
        return false;
    }

    let mut p = 0usize;
    let mut s = 0usize;
    while p < pattern.len() && s <= string.len() {
        match pattern[p] {
            b'*' => {
                while p + 1 < pattern.len() && pattern[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pattern.len() {
                    return true;
                }
                let mut i = s;
                loop {
                    if glob_match_impl(
                        &pattern[p + 1..],
                        &string[i..],
                        skip_longer_matches,
                        nesting + 1,
                    ) {
                        return true;
                    }
                    if *skip_longer_matches {
                        return false;
                    }
                    if i >= string.len() {
                        break;
                    }
                    i += 1;
                }
                *skip_longer_matches = true;
                return false;
            }
            b'?' => {
                if s >= string.len() {
                    return false;
                }
                s += 1;
                p += 1;
            }
            b'[' => {
                if s >= string.len() {
                    return false;
                }
                p += 1;
                let mut negate = false;
                if p < pattern.len() && pattern[p] == b'^' {
                    negate = true;
                    p += 1;
                }
                let mut matched = false;
                while p < pattern.len() && pattern[p] != b']' {
                    if pattern[p] == b'\\' && p + 1 < pattern.len() {
                        p += 1;
                        if pattern[p] == string[s] {
                            matched = true;
                        }
                        p += 1;
                    } else if p + 2 < pattern.len()
                        && pattern[p + 1] == b'-'
                        && pattern[p + 2] != b']'
                    {
                        let (mut lo, mut hi) = (pattern[p], pattern[p + 2]);
                        if lo > hi {
                            std::mem::swap(&mut lo, &mut hi);
                        }
                        if string[s] >= lo && string[s] <= hi {
                            matched = true;
                        }
                        p += 3;
                    } else {
                        if pattern[p] == string[s] {
                            matched = true;
                        }
                        p += 1;
                    }
                }
                if p < pattern.len() {
                    p += 1; //  ']'
                }
                if negate {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                s += 1;
            }
            b'\\' if p + 1 < pattern.len() => {
                p += 1;
                if s >= string.len() || pattern[p] != string[s] {
                    return false;
                }
                p += 1;
                s += 1;
            }
            c => {
                if s >= string.len() || c != string[s] {
                    return false;
                }
                p += 1;
                s += 1;
            }
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len() && s == string.len()
}

#[cfg(test)]
mod tests {
    use super::{glob_match, keys_reply};
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::{now_ms, Db};
    use crate::foundation::io::store::Robj;

    #[test]
    fn keys_hides_expired_entries_without_deleting_them() {
        let mut db = Db::new();
        db.set(b"expired".to_vec(), Robj::string("value"));
        db.set_expire(b"expired", now_ms().saturating_sub(1));

        assert_eq!(keys_reply(&mut db, b"*", false), Value::Array(Vec::new()));
        assert!(
            db.get_allow_expired(b"expired").is_some(),
            "KEYS must not physically delete logically expired entries"
        );
    }

    #[test]
    fn glob_match_prunes_failed_star_suffixes() {
        assert!(!glob_match(
            b"a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
    }

    #[test]
    fn glob_match_caps_deep_star_question_nesting() {
        let mut pattern = Vec::new();
        for _ in 0..50_000 {
            pattern.extend_from_slice(b"*?");
        }
        let string = vec![b'a'; 50_000];

        assert!(!glob_match(&pattern, &string));
    }
}
