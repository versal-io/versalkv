use super::types::{
    RDB_TYPE_HASH, RDB_TYPE_HASH_2, RDB_TYPE_HASH_LISTPACK, RDB_TYPE_HASH_ZIPLIST,
    RDB_TYPE_HASH_ZIPMAP, RDB_TYPE_LIST, RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2,
    RDB_TYPE_LIST_ZIPLIST, RDB_TYPE_SET, RDB_TYPE_SET_INTSET, RDB_TYPE_SET_LISTPACK,
    RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_STREAM_LISTPACKS_2, RDB_TYPE_STREAM_LISTPACKS_3,
    RDB_TYPE_STRING, RDB_TYPE_ZSET, RDB_TYPE_ZSET_2, RDB_TYPE_ZSET_LISTPACK, RDB_TYPE_ZSET_ZIPLIST,
};
use super::{hash, list, set, stream, string, zset, CodecLoad, LoadFn};

pub(super) fn build_registry() -> [Option<LoadFn>; 256] {
    let mut r: [Option<LoadFn>; 256] = [None; 256];
    macro_rules! reg {
        ($codec:ty, $($tb:expr),+ $(,)?) => {
            $( r[$tb as usize] = Some(<$codec as CodecLoad>::load_dispatch); )+
        };
    }
    reg!(string::StringCodec, RDB_TYPE_STRING);
    reg!(
        list::ListCodec,
        RDB_TYPE_LIST,
        RDB_TYPE_LIST_ZIPLIST,
        RDB_TYPE_LIST_QUICKLIST,
        RDB_TYPE_LIST_QUICKLIST_2
    );
    reg!(
        set::SetCodec,
        RDB_TYPE_SET,
        RDB_TYPE_SET_INTSET,
        RDB_TYPE_SET_LISTPACK
    );
    reg!(
        hash::HashCodec,
        RDB_TYPE_HASH,
        RDB_TYPE_HASH_2,
        RDB_TYPE_HASH_ZIPLIST,
        RDB_TYPE_HASH_LISTPACK,
        RDB_TYPE_HASH_ZIPMAP
    );
    reg!(
        zset::ZSetCodec,
        RDB_TYPE_ZSET,
        RDB_TYPE_ZSET_2,
        RDB_TYPE_ZSET_ZIPLIST,
        RDB_TYPE_ZSET_LISTPACK
    );
    reg!(
        stream::StreamCodec,
        RDB_TYPE_STREAM_LISTPACKS,
        RDB_TYPE_STREAM_LISTPACKS_2,
        RDB_TYPE_STREAM_LISTPACKS_3
    );
    r
}
