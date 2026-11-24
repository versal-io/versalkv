use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::dict::Dict;
use crate::foundation::io::store::structure::set::ValkeySet;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;
use std::collections::HashSet;

#[command("SUNIONSTORE", runtime)]
pub fn sunionstore(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let (max_intset, max_lp_entries, max_lp_value) = runtime.set_encoding_limits();
    let db_idx = runtime.selected_db();
    let dst_key = args.args[0].clone();
    let reply = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        let dst_key = &args.args[0];
        let src_keys = &args.args[1..];

        let mut result = Dict::new();
        let mut merged_sources = HashSet::new();
        let mut membership: Option<HashSet<Bytes>> = None;

        for key in src_keys {
            match db.get_set(key) {
                Ok(Some(s)) => {
                    // Validate every source before skipping one already merged.
                    if s.data.is_empty() || !merged_sources.insert(key.clone()) {
                        continue;
                    }
                    if result.is_empty() {
                        result = Dict::with_capacity(s.data.len());
                        for member in s.data.keys() {
                            #[cfg(test)]
                            tests::record_member_insert();
                            result.insert(member.clone(), ());
                        }
                    } else {
                        // Build the collision-resistant membership index only when
                        // another distinct nonempty source needs to be merged.
                        let membership =
                            membership.get_or_insert_with(|| result.keys().cloned().collect());
                        for member in s.data.keys() {
                            if membership.insert(member.clone()) {
                                #[cfg(test)]
                                tests::record_member_insert();
                                result.insert(member.clone(), ());
                            }
                        }
                    }
                }
                Ok(None) => continue,
                Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            }
        }

        let count = result.len() as i64;

        if result.is_empty() {
            db.del(dst_key);
        } else {
            db.set_slice(
                dst_key,
                Robj::SetHashtable(ValkeySet::from_dict_with_thresholds(
                    result,
                    max_intset,
                    max_lp_entries,
                    max_lp_value,
                )),
            );
        }

        Value::integer(count)
    };
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, dst_key.clone())],
            events: vec![CoreEvent {
                kind: "sunionstore",
                key: dst_key,
            }],
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::structure::set::{IntSet, SetEncoding, SetLp};
    use crate::foundation::memory::value::SetOps;
    use crate::framework::commands::command_runtime::TestCommandRuntime;
    use bytes::Bytes;
    use std::cell::Cell;
    use std::collections::HashSet;

    thread_local! {
        static MEMBER_INSERTS: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn record_member_insert() {
        MEMBER_INSERTS.with(|count| count.set(count.get() + 1));
    }

    fn command(parts: &[&[u8]]) -> CommandArgs {
        let mut argv = vec![Bytes::from_static(b"SUNIONSTORE")];
        argv.extend(parts.iter().map(|part| Bytes::copy_from_slice(part)));
        CommandArgs::from_parts(argv)
    }

    fn source(kind: usize, values: &[&[u8]]) -> Robj {
        let mut result = match kind {
            0 => Robj::SetHashtable(ValkeySet::new()),
            1 => Robj::SetIntset(IntSet::new()),
            _ => Robj::SetListpack(SetLp::new()),
        };
        for value in values {
            assert!(result.set_add(value, 512));
        }
        result
    }

    #[test]
    fn overlapping_colliding_sources_insert_each_result_member_once() {
        let members: Vec<_> = (0..32usize)
            .map(|value| {
                let mut member = Vec::new();
                for bit in 0..5 {
                    member.extend_from_slice(if value & (1 << bit) == 0 {
                        b"Aa"
                    } else {
                        b"B@"
                    });
                }
                member
            })
            .collect();
        let hash =
            crate::foundation::io::store::structure::configurable_hash::hash_bytes(&members[0]);
        assert!(members.iter().all(|member| {
            crate::foundation::io::store::structure::configurable_hash::hash_bytes(member) == hash
        }));
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let values: Vec<_> = members.iter().map(Vec::as_slice).collect();
        let object = source(0, &values);
        let db = runtime.db_mut().unwrap();
        db.set(b"first".to_vec(), object.clone());
        db.set(b"second".to_vec(), object);
        let mut parts = vec![b"dst".as_slice()];
        parts.extend(std::iter::repeat_n(b"first".as_slice(), 16));
        parts.extend(std::iter::repeat_n(b"second".as_slice(), 16));
        let args = command(&parts);
        MEMBER_INSERTS.with(|count| count.set(0));
        let result = sunionstore(&mut runtime, &args);
        assert_eq!(result.reply, Value::integer(32));
        assert_eq!(MEMBER_INSERTS.with(Cell::get), 32);
        assert_eq!(runtime.db_mut().unwrap().get(b"dst").unwrap().set_len(), 32);
    }

    #[test]
    fn union_store_preserves_aliases_encoding_expiry_and_effects() {
        for first_kind in 0..3 {
            for second_kind in 0..3 {
                for destination in [b"first".as_slice(), b"second", b"dst"] {
                    let mut runtime = TestCommandRuntime::new(1, 1_000);
                    let db = runtime.db_mut().unwrap();
                    db.set(b"first".to_vec(), source(first_kind, &[b"1", b"2"]));
                    db.set(b"second".to_vec(), source(second_kind, &[b"2", b"3"]));
                    if destination == b"dst" {
                        db.set(destination.to_vec(), Robj::string("old"));
                    }
                    db.set_expire(destination, u64::MAX);
                    let result = sunionstore(
                        &mut runtime,
                        &command(&[destination, b"missing", b"first", b"second", b"first"]),
                    );
                    assert_eq!(result.reply, Value::integer(3));
                    assert_eq!(
                        result.effects.changed_keys,
                        vec![(0, Bytes::copy_from_slice(destination))]
                    );
                    assert_eq!(result.effects.events[0].kind, "sunionstore");
                    assert_eq!(result.effects.events[0].key, destination);
                    let db = runtime.db_mut().unwrap();
                    assert_eq!(db.get_expire(destination), None);
                    let Robj::SetHashtable(set) = db.get(destination).unwrap() else {
                        panic!("Expected a set");
                    };
                    assert_eq!(set.encoding, SetEncoding::Intset);
                    assert_eq!(set.data.len(), 3);
                    assert!([b"1", b"2", b"3"]
                        .iter()
                        .all(|member| set.contains(*member)));
                }
            }
        }
    }

    #[test]
    fn membership_index_tracks_new_members_from_later_sources() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(b"first".to_vec(), source(0, &[b"1", b"2"]));
        db.set(b"second".to_vec(), source(0, &[b"2", b"3"]));
        db.set(b"third".to_vec(), source(0, &[b"3", b"4"]));
        let args = command(&[b"dst", b"first", b"second", b"third", b"second"]);
        MEMBER_INSERTS.with(|count| count.set(0));
        let result = sunionstore(&mut runtime, &args);
        assert_eq!(result.reply, Value::integer(4));
        assert_eq!(MEMBER_INSERTS.with(Cell::get), 4);
        assert_eq!(runtime.db_mut().unwrap().get(b"dst").unwrap().set_len(), 4);
    }

    #[test]
    fn union_store_wrong_type_preserves_destination_and_expiry() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(b"first".to_vec(), source(0, &[b"1"]));
        db.set(b"dst".to_vec(), Robj::string("old"));
        db.set(b"wrong".to_vec(), Robj::string("value"));
        db.set_expire(b"dst", u64::MAX);
        let result = sunionstore(&mut runtime, &command(&[b"dst", b"first", b"wrong"]));
        assert_eq!(result.reply, CommandError::wrong_type());
        assert!(result.effects.changed_keys.is_empty());
        assert!(result.effects.events.is_empty());
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get_expire(b"dst"), Some(u64::MAX));
        assert_eq!(db.get(b"dst").unwrap().as_bytes(), b"old");
    }

    #[test]
    fn empty_union_removes_destination_and_expiry() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(b"dst".to_vec(), Robj::string("old"));
        db.set_expire(b"dst", u64::MAX);
        let result = sunionstore(&mut runtime, &command(&[b"dst", b"missing"]));
        assert_eq!(result.reply, Value::integer(0));
        let db = runtime.db_mut().unwrap();
        assert!(db.get(b"dst").is_none());
        assert_eq!(db.get_expire(b"dst"), None);
    }

    #[test]
    fn direct_dictionary_constructor_matches_set_encoding_thresholds() {
        for values in [
            vec![b"1".as_slice(), b"-2"],
            vec![b"1".as_slice(), b"abc"],
            vec![b"abc".as_slice(), b"0123456789"],
            vec![],
        ] {
            for (max_intset, max_entries, max_value) in [(0, 0, 0), (2, 4, 8), (1, 2, 2)] {
                let members: HashSet<_> = values
                    .iter()
                    .map(|value| Bytes::copy_from_slice(value))
                    .collect();
                let expected = ValkeySet::from_hashset_with_thresholds(
                    members.clone(),
                    max_intset,
                    max_entries,
                    max_value,
                );
                let mut dictionary = Dict::with_capacity(members.len());
                for member in members {
                    dictionary.insert(member, ());
                }
                assert_eq!(
                    ValkeySet::from_dict_with_thresholds(
                        dictionary,
                        max_intset,
                        max_entries,
                        max_value
                    ),
                    expected
                );
            }
        }
    }
}
