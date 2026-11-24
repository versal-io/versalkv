use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::structure::set::SetLp;
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::foundation::io::store::structure::zset::ZSetLp;
use crate::foundation::io::store::Robj;
use crate::foundation::memory::value::SetOps;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;
use std::collections::HashSet;

#[command(
    "ZINTERCARD",
    runtime,
    get_keys = crate::framework::commands::key_extractors::numkeys_ro_get_keys
)]
pub fn zintercard(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(zintercard_reply(runtime, args))
}

fn zintercard_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::syntax_error();
    }
    let numkeys = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => return Value::error("ERR at least 1 input key is needed for 'zintercard' command"),
    };
    if numkeys > args.args.len() - 1 {
        return CommandError::syntax_error();
    }
    let mut limit = 0usize;
    let mut i = 1 + numkeys;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"limit") {
            if i + 1 >= args.args.len() {
                return CommandError::syntax_error();
            }
            let limit_val = match std::str::from_utf8(&args.args[i + 1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => n,
                None => return Value::error("ERR LIMIT value is not an integer or out of range"),
            };
            if limit_val < 0 {
                return Value::error("ERR LIMIT can't be negative");
            }
            limit = limit_val as usize;
            i += 2;
        } else {
            return CommandError::syntax_error();
        }
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let keys = &args.args[1..=numkeys];
    let mut smallest_index = 0;
    let mut smallest_len = usize::MAX;
    // Mutable lookups preserve input-order expiry and missing/type precedence.
    for (index, key) in keys.iter().enumerate() {
        let Some(source) = db.get(key) else {
            return Value::integer(0);
        };
        let len = match source {
            Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_) => source.zset_len(),
            Robj::SetHashtable(_) | Robj::SetIntset(_) | Robj::SetListpack(_) => source.set_len(),
            _ => return CommandError::wrong_type(),
        };
        if len < smallest_len {
            smallest_index = index;
            smallest_len = len;
        }
    }
    // No store mutations occur after validation, so all source borrows can coexist.
    let Some(mut sources) = keys
        .iter()
        .map(|key| db.get_allow_expired(key))
        .collect::<Option<Vec<_>>>()
    else {
        return Value::integer(0);
    };
    let smallest = sources.swap_remove(smallest_index);
    let count = match smallest {
        Robj::ZSetSkiplist(zset) => count_intersection(zset.keys(), &sources, limit),
        Robj::SetHashtable(set) => count_intersection(set.data.keys(), &sources, limit),
        Robj::SetIntset(set) => {
            count_intersection(set.iter().map(|value| value.to_string()), &sources, limit)
        }
        // Candidate enumeration is separate from lazy indexes on other compact sources.
        Robj::ZSetListpack(zset) => count_intersection(
            zset.all_pairs().into_iter().map(|(member, _)| member),
            &sources,
            limit,
        ),
        Robj::SetListpack(set) => {
            count_intersection(set.all_members().into_iter(), &sources, limit)
        }
        _ => return CommandError::wrong_type(),
    };
    Value::integer(count as i64)
}

fn count_intersection(
    members: impl Iterator<Item = impl AsRef<[u8]>>,
    sources: &[&Robj],
    limit: usize,
) -> usize {
    let mut sources: Vec<_> = sources
        .iter()
        .map(|source| MembershipSource::new(source))
        .collect();
    let mut count = 0;
    for member in members {
        if sources
            .iter_mut()
            .all(|source| source.contains(member.as_ref()))
        {
            count += 1;
            if limit > 0 && count >= limit {
                break;
            }
        }
    }
    count
}

enum MembershipSource<'a> {
    Borrowed(&'a Robj),
    Compact {
        source: CompactSource<'a>,
        membership: CompactMembership,
    },
}

enum CompactSource<'a> {
    Set(&'a SetLp),
    SortedSet(&'a ZSetLp),
}

enum CompactMembership {
    Unprobed,
    Probed,
    Indexed(HashSet<Vec<u8>>),
}

impl<'a> MembershipSource<'a> {
    fn new(source: &'a Robj) -> Self {
        let source = match source {
            Robj::ZSetListpack(zset) => CompactSource::SortedSet(zset),
            Robj::SetListpack(set) => CompactSource::Set(set),
            _ => return Self::Borrowed(source),
        };
        Self::Compact {
            source,
            membership: CompactMembership::Unprobed,
        }
    }

    fn contains(&mut self, member: &[u8]) -> bool {
        match self {
            Self::Borrowed(source) => match source {
                Robj::ZSetSkiplist(zset) => zset.dict().contains_key(member),
                Robj::SetHashtable(_) => source.set_contains(member),
                Robj::SetIntset(set) => {
                    int_encodable(member).is_some_and(|value| set.contains(value))
                }
                _ => false,
            },
            Self::Compact { source, membership } => match membership {
                CompactMembership::Unprobed => {
                    let is_present = source.contains(member);
                    *membership = CompactMembership::Probed;
                    is_present
                }
                // A second probe bounds each source to one direct query and one index build.
                CompactMembership::Probed => {
                    let index = source.member_index();
                    let is_present = index.contains(member);
                    *membership = CompactMembership::Indexed(index);
                    is_present
                }
                CompactMembership::Indexed(index) => index.contains(member),
            },
        }
    }
}

impl CompactSource<'_> {
    fn contains(&self, member: &[u8]) -> bool {
        #[cfg(test)]
        tests::record_compact_scan(false);
        match self {
            Self::Set(set) => set.contains(member),
            Self::SortedSet(zset) => zset.contains_member(member),
        }
    }

    fn member_index(&self) -> HashSet<Vec<u8>> {
        #[cfg(test)]
        tests::record_compact_scan(true);
        match self {
            Self::Set(set) => set.all_members().into_iter().collect(),
            Self::SortedSet(zset) => zset
                .all_pairs()
                .into_iter()
                .map(|(member, _)| member)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::codec::{decoder_compact, types};
    use crate::foundation::io::persistence::load_robj;
    use crate::foundation::io::store::structure::set::{IntSet, SetLp, ValkeySet};
    use crate::foundation::io::store::structure::zset::{ZSetLp, ZSetSl};
    use crate::foundation::memory::value::{SetOps, ZSetOps};
    use crate::framework::commands::command_runtime::TestCommandRuntime;
    use bytes::Bytes;
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct CompactWork {
        // Counts query/build API entries, not physical listpack traversals.
        scans: usize,
        index_builds: usize,
    }

    thread_local! {
        static COMPACT_WORK: Cell<CompactWork> = const {
            Cell::new(CompactWork { scans: 0, index_builds: 0 })
        };
    }

    pub(super) fn record_compact_scan(is_index: bool) {
        COMPACT_WORK.with(|work| {
            let mut current = work.get();
            current.scans += 1;
            current.index_builds += usize::from(is_index);
            work.set(current);
        });
    }

    fn measure_compact_work<T>(run: impl FnOnce() -> T) -> (T, CompactWork) {
        COMPACT_WORK.with(|work| work.set(CompactWork::default()));
        let result = run();
        let work = COMPACT_WORK.with(|work| work.replace(CompactWork::default()));
        (result, work)
    }

    fn loaded_compact_source(pairs: &[(&[u8], &[u8])], is_saturated: bool) -> Robj {
        let items: Vec<_> = pairs
            .iter()
            .flat_map(|(member, score)| [member.to_vec(), score.to_vec()])
            .collect();
        let mut listpack = decoder_compact::listpack_encode(&items);
        if is_saturated {
            listpack[4..6].copy_from_slice(&u16::MAX.to_le_bytes());
        }
        let mut encoded = Vec::new();
        types::save_len(&mut encoded, listpack.len() as u64);
        encoded.extend_from_slice(&listpack);
        let mut pos = 0;
        let object = load_robj(&encoded, &mut pos, types::RDB_TYPE_ZSET_LISTPACK).unwrap();
        assert_eq!(pos, encoded.len());
        let Robj::ZSetListpack(zset) = &object else {
            panic!("Expected a physically compact sorted set");
        };
        assert_eq!(zset.raw_bytes(), listpack);
        assert_eq!(zset.len(), pairs.len());
        object
    }

    #[test]
    fn restored_score_single_candidate_membership() {
        for member in [b"x".as_slice(), b"", b"\0\xff", b"1"] {
            let compact = loaded_compact_source(&[(member, b" 1"), (b"y", b"2")], false);
            assert_eq!(compact.zset_score(member), None);
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let db = runtime.db_mut().unwrap();
            db.set(b"small".to_vec(), source(4, &[member]));
            db.set(b"compact".to_vec(), compact);
            for keys in [[b"small".as_slice(), b"compact"], [b"compact", b"small"]] {
                for limit in [None, Some(b"0".as_slice()), Some(b"1".as_slice())] {
                    let mut parts = vec![b"2".as_slice(), keys[0], keys[1]];
                    if let Some(limit) = limit {
                        parts.extend([b"LIMIT".as_slice(), limit]);
                    }
                    let (result, work) =
                        measure_compact_work(|| zintercard_reply(&mut runtime, &command(&parts)));
                    assert_eq!(
                        result,
                        Value::integer(1),
                        "member: {member:?}, keys: {keys:?}, limit: {limit:?}"
                    );
                    assert_eq!(
                        work,
                        CompactWork {
                            scans: 1,
                            index_builds: 0
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn restored_score_first_match_limit_one_avoids_index() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(b"small".to_vec(), source(4, &[b"x"]));
        db.set(
            b"compact".to_vec(),
            loaded_compact_source(&[(b"x", b" 1"), (b"y", b"2")], false),
        );
        let (result, work) = measure_compact_work(|| {
            zintercard_reply(
                &mut runtime,
                &command(&[b"2", b"small", b"compact", b"LIMIT", b"1"]),
            )
        });
        assert_eq!(result, Value::integer(1));
        assert_eq!(
            work,
            CompactWork {
                scans: 1,
                index_builds: 0
            }
        );
    }

    #[test]
    fn restored_score_membership_survives_index_transition() {
        let compact = loaded_compact_source(
            &[
                (b"x", b" 1"),
                (b"y", b" 2"),
                (b"z", b" 3"),
                (b"spare", b"4"),
            ],
            false,
        );
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(b"small".to_vec(), source(4, &[b"x", b"y", b"z"]));
        db.set(b"compact".to_vec(), compact);
        for (limit, expected, scans, index_builds) in [
            (b"0".as_slice(), 3, 2, 1),
            (b"1", 1, 1, 0),
            (b"10", 3, 2, 1),
        ] {
            let (result, work) = measure_compact_work(|| {
                zintercard_reply(
                    &mut runtime,
                    &command(&[b"2", b"small", b"compact", b"LIMIT", limit]),
                )
            });
            assert_eq!(result, Value::integer(expected), "limit: {limit:?}");
            assert_eq!(
                work,
                CompactWork {
                    scans,
                    index_builds
                }
            );
        }
    }

    #[test]
    fn restored_score_compact_smallest_preserves_membership() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(
            b"compact".to_vec(),
            loaded_compact_source(&[(b"x", b" 1"), (b"y", b"2")], false),
        );
        db.set(b"large".to_vec(), source(4, &[b"x", b"y", b"z"]));
        for (limit, expected) in [(b"0".as_slice(), 2), (b"1", 1)] {
            for keys in [[b"compact".as_slice(), b"large"], [b"large", b"compact"]] {
                let (result, work) = measure_compact_work(|| {
                    zintercard_reply(
                        &mut runtime,
                        &command(&[b"2", keys[0], keys[1], b"LIMIT", limit]),
                    )
                });
                assert_eq!(result, Value::integer(expected));
                assert_eq!(work, CompactWork::default());
            }
        }
    }

    #[test]
    fn saturated_compact_zset_query_entries_are_bounded() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(
            b"compact".to_vec(),
            loaded_compact_source(&[(b"x", b"1"), (b"y", b"2"), (b"z", b"3")], true),
        );
        db.set(b"small".to_vec(), source(4, &[b"x", b"y"]));
        for (limit, expected, scans, index_builds) in [(b"0".as_slice(), 2, 2, 1), (b"1", 1, 1, 0)]
        {
            let (result, work) = measure_compact_work(|| {
                zintercard_reply(
                    &mut runtime,
                    &command(&[b"2", b"small", b"compact", b"LIMIT", limit]),
                )
            });
            assert_eq!(result, Value::integer(expected));
            assert_eq!(
                work,
                CompactWork {
                    scans,
                    index_builds
                }
            );
            let Robj::ZSetListpack(zset) = runtime.db_mut().unwrap().get(b"compact").unwrap()
            else {
                panic!("Expected a physically compact sorted set");
            };
            assert_eq!(&zset.raw_bytes()[4..6], &u16::MAX.to_le_bytes());
        }
    }

    fn numbered_source(kind: usize, len: usize) -> Robj {
        let mut object = source(kind, &[]);
        for value in 0..len {
            let member = value.to_string();
            match &mut object {
                Robj::ZSetListpack(zset) => assert!(zset
                    .add_with_threshold(member.as_bytes(), value as f64, 2_048, 64)
                    .unwrap()),
                Robj::SetListpack(set) => assert!(set
                    .add_with_threshold(member.as_bytes(), 2_048, 64)
                    .unwrap()),
                Robj::SetIntset(set) => assert!(SetOps::add(set, member.as_bytes()).unwrap()),
                Robj::SetHashtable(set) => assert!(SetOps::add(set, member.as_bytes()).unwrap()),
                _ => assert!(object.zset_add(member.as_bytes(), value as f64)),
            }
        }
        object
    }

    fn assert_compact_scan_bound(kind: usize) {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(b"small".to_vec(), numbered_source(4, 1_000));
        db.set(b"compact".to_vec(), numbered_source(kind, 1_001));
        for limit in [b"0".as_slice(), b"1010"] {
            let (result, work) = measure_compact_work(|| {
                zintercard_reply(
                    &mut runtime,
                    &command(&[b"2", b"small", b"compact", b"LIMIT", limit]),
                )
            });
            assert_eq!(result, Value::integer(1_000));
            assert!(
                work.scans <= 2,
                "kind: {kind}, limit: {limit:?}, work: {work:?}"
            );
            assert_eq!(work.index_builds, 1, "work: {work:?}");
        }
    }

    #[test]
    fn compact_zset_scan_work_is_bounded() {
        assert_compact_scan_bound(0);
    }

    #[test]
    fn compact_set_scan_work_is_bounded() {
        assert_compact_scan_bound(3);
    }

    #[test]
    fn compact_indexes_are_reused_for_repeated_sources() {
        for kind in [0, 3] {
            let compact = numbered_source(kind, 33);
            let sources = [&compact, &compact];
            let (result, work) = measure_compact_work(|| {
                count_intersection((0..32).map(|value| value.to_string()), &sources, 0)
            });
            assert_eq!(result, 32);
            assert_eq!(work.index_builds, 2, "kind: {kind}, work: {work:?}");
            assert_eq!(work.scans, 4, "kind: {kind}, work: {work:?}");
        }
    }

    #[test]
    fn compact_first_match_limit_one_avoids_indexes() {
        for kind in [0, 3] {
            let compact = numbered_source(kind, 33);
            let (result, work) = measure_compact_work(|| {
                count_intersection([b"0".as_slice(), b"1"].into_iter(), &[&compact], 1)
            });
            assert_eq!(result, 1);
            assert_eq!(
                work,
                CompactWork {
                    scans: 1,
                    index_builds: 0
                }
            );
        }
    }

    #[test]
    fn compact_sources_rejected_by_earlier_membership_are_not_scanned() {
        let reject = source(4, &[b"not-present"]);
        for kind in [0, 3] {
            let compact = numbered_source(kind, 33);
            let (result, work) = measure_compact_work(|| {
                count_intersection(
                    [b"0".as_slice(), b"1", b"2"].into_iter(),
                    &[&reject, &compact],
                    0,
                )
            });
            assert_eq!(result, 0);
            assert_eq!(work, CompactWork::default());
        }
    }

    #[test]
    fn noncompact_sources_never_build_indexes() {
        for kind in [1, 2, 4] {
            let object = numbered_source(kind, 33);
            let (result, work) = measure_compact_work(|| {
                count_intersection((0..32).map(|value| value.to_string()), &[&object], 0)
            });
            assert_eq!(result, 32);
            assert_eq!(work, CompactWork::default());
        }
    }

    fn source(kind: usize, members: &[&[u8]]) -> Robj {
        let mut object = match kind {
            0 => Robj::ZSetListpack(ZSetLp::new()),
            1 => Robj::ZSetSkiplist(ZSetSl::new()),
            2 => Robj::SetIntset(IntSet::new()),
            3 => Robj::SetListpack(SetLp::new()),
            4 => Robj::SetHashtable(ValkeySet::new()),
            _ => unreachable!(),
        };
        for member in members {
            match &mut object {
                Robj::SetIntset(set) => assert!(SetOps::add(set, member).unwrap()),
                Robj::SetListpack(set) => assert!(SetOps::add(set, member).unwrap()),
                Robj::SetHashtable(set) => assert!(SetOps::add(set, member).unwrap()),
                _ => assert!(object.zset_add(member, 0.0)),
            }
        }
        object
    }

    fn command(parts: &[&[u8]]) -> CommandArgs {
        let mut argv = vec![Bytes::from_static(b"ZINTERCARD")];
        argv.extend(parts.iter().map(|part| Bytes::copy_from_slice(part)));
        CommandArgs::from_parts(argv)
    }

    #[test]
    fn numkeys_max_returns_syntax_error() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let count = usize::MAX.to_string();
        assert_eq!(
            zintercard(&mut runtime, &command(&[count.as_bytes(), b"key"])).reply,
            CommandError::syntax_error()
        );
    }

    #[test]
    fn numkeys_max_minus_one_returns_syntax_error() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let count = (usize::MAX - 1).to_string();
        for parts in [
            vec![count.as_bytes(), b"key".as_slice()],
            vec![count.as_bytes(), b"key", b"LIMIT", b"-1"],
        ] {
            assert_eq!(
                zintercard(&mut runtime, &command(&parts)).reply,
                CommandError::syntax_error()
            );
        }
    }

    #[test]
    fn numkeys_parse_errors_preserve_existing_reply() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let out_of_range = (usize::MAX as u128 + 1).to_string();
        for count in [
            out_of_range.as_bytes(),
            b"0",
            b"-1",
            b"invalid",
            b"",
            b"\xff",
        ] {
            assert_eq!(
                zintercard(&mut runtime, &command(&[count, b"key"])).reply,
                Value::error("ERR at least 1 input key is needed for 'zintercard' command"),
                "count: {count:?}"
            );
        }
    }

    #[test]
    fn numkeys_length_boundaries_preserve_validation_order() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        for parts in [
            vec![],
            vec![b"invalid".as_slice()],
            vec![b"2".as_slice(), b"key"],
            vec![b"1".as_slice(), b"key", b"LIMIT"],
        ] {
            assert_eq!(
                zintercard(&mut runtime, &command(&parts)).reply,
                CommandError::syntax_error(),
                "parts: {parts:?}"
            );
        }
        assert_eq!(
            zintercard(&mut runtime, &command(&[b"1", b"key"])).reply,
            Value::integer(0)
        );
    }

    #[test]
    fn all_physical_encodings_support_intersection_and_limits() {
        for first_kind in 0..5 {
            for second_kind in 0..5 {
                let mut runtime = TestCommandRuntime::new(1, 1_000);
                let db = runtime.db_mut().unwrap();
                db.set(b"first".to_vec(), source(first_kind, &[b"1", b"2", b"3"]));
                db.set(
                    b"second".to_vec(),
                    source(second_kind, &[b"2", b"3", b"4", b"5"]),
                );
                for (limit, expected) in [(b"0".as_slice(), 2), (b"1", 1), (b"2", 2), (b"10", 2)] {
                    for keys in [
                        [b"first".as_slice(), b"second".as_slice()],
                        [b"second", b"first"],
                    ] {
                        let args = command(&[b"2", keys[0], keys[1], b"LIMIT", limit]);
                        assert_eq!(
                            zintercard_reply(&mut runtime, &args),
                            Value::integer(expected),
                            "kinds: {first_kind}, {second_kind}, keys: {keys:?}, limit: {limit:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn physical_intset_does_not_match_noncanonical_integer_text() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let db = runtime.db_mut().unwrap();
        db.set(
            b"ints".to_vec(),
            source(
                2,
                &[b"0", b"1", b"-9223372036854775808", b"9223372036854775807"],
            ),
        );
        db.set(b"aliases".to_vec(), source(1, &[b"-0", b"+1", b"01"]));
        assert_eq!(
            zintercard_reply(&mut runtime, &command(&[b"2", b"aliases", b"ints"])),
            Value::integer(0)
        );
        runtime.db_mut().unwrap().set(
            b"exact".to_vec(),
            source(
                0,
                &[
                    b"0",
                    b"1",
                    b"-9223372036854775808",
                    b"9223372036854775807",
                    b"9223372036854775808",
                ],
            ),
        );
        assert_eq!(
            zintercard_reply(&mut runtime, &command(&[b"2", b"exact", b"ints"])),
            Value::integer(4)
        );
    }

    #[test]
    fn empty_source_still_validates_later_input_types() {
        for kind in 0..5 {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let db = runtime.db_mut().unwrap();
            db.set(b"empty".to_vec(), source(kind, &[]));
            db.set(b"wrong".to_vec(), Robj::string("value"));
            assert_eq!(
                zintercard_reply(&mut runtime, &command(&[b"2", b"empty", b"wrong"])),
                CommandError::wrong_type()
            );
            assert_eq!(
                zintercard_reply(&mut runtime, &command(&[b"1", b"empty"])),
                Value::integer(0)
            );
        }
    }
}
