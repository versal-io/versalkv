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
        if match_all
            || crate::framework::commands::pattern::string_match(pattern, key, false)
        {
            out.push(Value::bulk(key.to_vec()));
        }
    });
    db.set_no_lazy_expire(false);
    Value::Array(out)
}

#[cfg(test)]
mod tests {
    use super::keys_reply;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::{now_ms, Db};
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::pattern::string_match;

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
        assert!(!string_match(
            b"a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            false,
        ));
    }

    #[test]
    fn glob_match_caps_deep_star_question_nesting() {
        let mut pattern = Vec::new();
        for _ in 0..50_000 {
            pattern.extend_from_slice(b"*?");
        }
        let string = vec![b'a'; 50_000];

        assert!(!string_match(&pattern, &string, false));
    }
}
