use proptest::prelude::*;
use versal::foundation::io::persistence::{self, RdbStreamDecoder, StreamStep};
use versal::foundation::io::store::kv_engine::DbSet;
use versal::foundation::io::store::Robj;

proptest! {
    // For any dataset and any chunk boundary, streaming decode yields the same
    // DbSet as the one-shot buffer decode, and always reaches Finished.
    #[test]
    fn streaming_matches_buffer_decode(
        vals in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..64),
            0..40,
        ),
        chunk in 1usize..4096,
    ) {
        let mut src = DbSet::new(16);
        for (i, v) in vals.iter().enumerate() {
            let k = format!("k{i}").into_bytes();
            src.get_mut(0).unwrap().set(k, Robj::string(v.clone()));
        }
        let bytes = persistence::serialize_all(&src, 0);

        // One-shot reference.
        let mut expected = DbSet::new(16);
        let (loaded_ref, _consumed, _expired, _funcs) =
            persistence::load_rdb_bytes_checked_with_functions(&bytes, &mut expected, true)
                .unwrap();

        // Streaming: feed arbitrary-width chunks.
        let mut d = RdbStreamDecoder::new(true);
        let mut got = DbSet::new(16);
        let mut step = StreamStep::NeedMore;
        for c in bytes.chunks(chunk) {
            d.feed(c);
            step = d.drive(&mut got, 0).unwrap();
        }
        prop_assert_eq!(step, StreamStep::Finished);
        prop_assert_eq!(d.loaded(), loaded_ref);
        for (i, v) in vals.iter().enumerate() {
            let k = format!("k{i}").into_bytes();
            prop_assert_eq!(
                got.get(0).unwrap().get_no_expire(&k).map(|r| r.as_bytes().to_vec()),
                Some(v.clone())
            );
        }
    }
}
