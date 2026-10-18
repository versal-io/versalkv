use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;
use command_macros::command;

mod clients;
mod cluster;
mod dispatch;
mod memory;
mod persistence;
mod replication;
mod server;
mod stats;
mod util;

#[command("INFO")]
pub fn info(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let is_sentinel = ctx.sentinel_state().sentinel_mode;
    let mut sections: Vec<String> = Vec::new();

    if args.args.is_empty() {
        for sec in dispatch::default_sections(is_sentinel) {
            sections.push(util::normalize_section(sec));
        }
    } else {
        for arg in &args.args {
            let s = match std::str::from_utf8(arg) {
                Ok(s) => s,
                Err(_) => return Value::error("ERR section name is not valid"),
            };
            let norm = util::normalize_section(s);
            if norm == "all" || norm == "everything" {
                sections.clear();
                for sec in dispatch::all_sections(is_sentinel) {
                    sections.push(util::normalize_section(sec));
                }
                break;
            }
            if norm == "default" {
                sections.clear();
                for sec in dispatch::default_sections(is_sentinel) {
                    sections.push(util::normalize_section(sec));
                }
                break;
            }
            if !sections.contains(&norm) {
                sections.push(norm);
            }
        }
    }

    let mut out = String::new();
    for sec in &sections {
        let chunk = dispatch::gen_one(ctx, sec);
        if !chunk.is_empty() {
            if !out.is_empty() {
                out.push_str("\r\n");
            }
            out.push_str(&chunk);
        }
    }
    Value::bulk(out.into_bytes())
}
