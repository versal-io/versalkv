use bytes::Bytes;

use crate::engine::command::{CmdFlags, DispatchGroup};

pub type GetKeysFn = for<'a> fn(&'a [Bytes]) -> Vec<(&'a [u8], Vec<&'static str>)>;

pub struct CommandMeta {
    pub name: &'static str,
    pub arity: i32,
    pub flags: CmdFlags,
    pub dispatch_group: DispatchGroup,
    pub first_key: i32,
    pub last_key: i32,
    pub key_step: i32,
    pub container: Option<&'static str>,
    pub key_flags: &'static [&'static str],
    pub acl_categories: &'static [&'static str],
    pub get_keys: Option<GetKeysFn>,
}
