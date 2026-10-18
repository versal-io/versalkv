#![no_main]

use libfuzzer_sys::fuzz_target;
use versal::foundation::io::persistence::load_rdb_bytes_checked;
use versal::foundation::io::store::kv_engine::DbSet;

fuzz_target!(|data: &[u8]| {
    let mut dbs = DbSet::new(16);
    let _ = load_rdb_bytes_checked(data, &mut dbs, true);
});
