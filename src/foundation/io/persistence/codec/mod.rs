pub mod decoder;
pub mod decoder_compact;
mod dispatch;
mod hash;
mod list;
pub(crate) mod lzf;
mod registry;
mod set;
mod stream;
mod string;
pub mod types;
mod zset;

use crate::foundation::io::store::Robj;

pub(crate) use dispatch::load_robj_with_hash_field_expires_detailed;
pub use dispatch::{
    load_robj, load_robj_with_hash_field_expires, load_typed_object,
    load_typed_object_with_hash_field_expires, load_typed_object_with_hash_field_expires_detailed,
    save_robj, save_robj_with_hash_field_expires, type_byte, type_byte_with_hash_field_expires,
    ObjectLoadError,
};

pub trait RdbWriter {
    fn write_len(&mut self, len: u64);
    fn write_string(&mut self, s: &[u8]);
    fn write_binary_double(&mut self, val: f64);
    fn write_millisecond_time(&mut self, t: i64);
    fn write_raw(&mut self, bytes: &[u8]);
}

pub(crate) type LoadFn = fn(&[u8], &mut usize, u8) -> Option<Robj>;

pub(crate) trait CodecLoad {
    fn load_dispatch(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Robj>;
}
