#[derive(Clone)]
pub struct EncodingConfig {
    pub zset_max_ziplist_entries: i64,
    pub zset_max_ziplist_value: i64,
    pub list_max_listpack_size: i64,
    pub list_compress_depth: i64,
    pub hash_max_listpack_entries: i64,
    pub hash_max_listpack_value: i64,
    pub set_max_intset_entries: i64,
    pub set_max_listpack_entries: i64,
    pub set_max_listpack_value: i64,
    pub stream_node_max_entries: i64,
    pub stream_node_max_bytes: i64,
}

impl EncodingConfig {
    pub fn new() -> Self {
        Self {
            zset_max_ziplist_entries: 128,
            zset_max_ziplist_value: 64,
            list_max_listpack_size: -2,
            list_compress_depth: 0,
            hash_max_listpack_entries: 512,
            hash_max_listpack_value: 64,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            stream_node_max_entries: 100,
            stream_node_max_bytes: 4096,
        }
    }
}
