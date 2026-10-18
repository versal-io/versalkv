use super::CodecLoad;
use crate::foundation::io::persistence::codec::decoder;
use crate::foundation::io::persistence::codec::RdbWriter;
use crate::foundation::io::store::Robj;
use bytes::Bytes;

pub struct StringCodec;

impl CodecLoad for StringCodec {
    fn load_dispatch(buf: &[u8], pos: &mut usize, _type_byte: u8) -> Option<Robj> {
        let v = decoder::load_string(buf, pos)?;
        Some(Robj::string(Bytes::from(v)))
    }
}

impl StringCodec {
    pub fn save<W: RdbWriter>(val: &[u8], w: &mut W) {
        w.write_string(val);
    }
}
