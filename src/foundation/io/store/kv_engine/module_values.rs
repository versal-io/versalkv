use super::db::Db;
use std::ffi::c_void;

impl Db {
    pub fn set_module_value(&mut self, key: Vec<u8>, type_id: u64, value: *mut c_void) {
        self.module_values.insert(key, (type_id, value));
    }

    pub fn get_module_value(&self, key: &[u8]) -> Option<&(u64, *mut c_void)> {
        self.module_values.get(key)
    }

    pub fn remove_module_value(&mut self, key: &[u8]) -> bool {
        self.module_values.remove(key).is_some()
    }

    pub fn is_module_key(&self, key: &[u8]) -> bool {
        self.module_values.contains_key(key)
    }
}
