use crate::engine::command::CommandArgs;

/// ACL-DB-denial object name (for the log) and database label (for the error
/// message). Mirrors C `addACLLogEntry`: the object is `argv[argpos]` when a
/// specific db argument is at fault, else the command full name; the error
/// message always names the offending database id.
pub(super) fn tx_db_object_and_label(
    cmd_name: &str,
    args: &CommandArgs,
    arg_index: Option<usize>,
    dbid: i64,
) -> (String, String) {
    let db_label = arg_index
        .and_then(|i| args.args.get(i))
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .map_or_else(|| dbid.to_string(), str::to_string);
    let object = if arg_index.is_some() {
        db_label.clone()
    } else {
        cmd_name.to_ascii_lowercase()
    };
    (object, db_label)
}
