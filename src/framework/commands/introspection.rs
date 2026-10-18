use crate::engine::command::{CommandArgs, CommandError};
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

command_macros::portable_command_support!();

pub trait CommandCatalog {
    fn count(&self) -> usize;
    fn all_info(&self) -> Value;
    fn list(&self, args: &[Bytes]) -> Value;
    fn docs(&self, names: &[Bytes]) -> Value;
    fn info(&self, names: &[Bytes]) -> Value;
    fn getkeys(&self, args: &[Bytes], include_flags: bool) -> Value;
}

pub fn command_reply(catalog: &dyn CommandCatalog, args: &CommandArgs) -> Value {
    let Some(subcommand) = args.args.first() else {
        return catalog.all_info();
    };
    let Ok(subcommand) = std::str::from_utf8(subcommand) else {
        return CommandError::syntax_error();
    };
    let rest = &args.args[1..];
    match subcommand.to_ascii_lowercase().as_str() {
        "count" => Value::integer(catalog.count() as i64),
        "list" => catalog.list(rest),
        "docs" => catalog.docs(rest),
        "info" => catalog.info(rest),
        "getkeys" => catalog.getkeys(rest, false),
        "getkeysandflags" => catalog.getkeys(rest, true),
        "help" => Value::bulk(Bytes::from_static(
            b"COMMAND <subcommand> [<arg> [value] [opt] ...]. See https://valkey.io for details.",
        )),
        _ => CommandError::syntax_error(),
    }
}
