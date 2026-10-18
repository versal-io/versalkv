pub trait Snapshot {
    type Error;
    fn save(
        &mut self,
        dbs: &crate::foundation::io::store::kv_engine::DbSet,
    ) -> Result<Vec<u8>, Self::Error>;
    fn load(
        &mut self,
        data: &[u8],
        dbs: &mut crate::foundation::io::store::kv_engine::DbSet,
    ) -> Result<(usize, usize), Self::Error>;
    fn save_to_file(
        &mut self,
        path: &str,
        dbs: &crate::foundation::io::store::kv_engine::DbSet,
    ) -> Result<(), Self::Error>;
}

pub trait Journal {
    type Error;
    fn append(&mut self, entry: &[u8]) -> Result<(), Self::Error>;
    fn replay(&mut self) -> Result<(), Self::Error>;
    fn rewrite(&mut self) -> Result<(), Self::Error>;
    fn truncate(&mut self, size: u64) -> Result<(), Self::Error>;
}
