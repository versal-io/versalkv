use crate::check::RdbCheckFailure;
use crate::cli::RdbOutputFormat;
use std::io::{self, Write};
use versal::foundation::io::store::kv_engine::now_ms;
use versal::foundation::memory::value::Type;

pub const STATS_TYPE_NAMES: [&str; 6] = ["string", "list", "set", "zset", "hash", "stream"];

pub fn type_index(value_type: Type) -> usize {
    match value_type {
        Type::String => 0,
        Type::List => 1,
        Type::Set => 2,
        Type::ZSet => 3,
        Type::Hash => 4,
        Type::Stream => 5,
    }
}

pub fn state_now_ms() -> i64 {
    now_ms() as i64
}

#[derive(Debug)]
pub struct RdbState {
    pub keys: u64,
    pub expires: u64,
    pub already_expired: u64,
    pub functions: u64,
    pub lua_scripts: u64,
    pub databases: u64,
    pub rdbver: u64,
    pub final_offset: u64,
    pub type_counts: Vec<[u64; 6]>,
}

impl RdbState {
    pub fn new() -> Self {
        Self {
            keys: 0,
            expires: 0,
            already_expired: 0,
            functions: 0,
            lua_scripts: 0,
            databases: 0,
            rdbver: 0,
            final_offset: 0,
            type_counts: vec![[0; 6]],
        }
    }

    pub fn ensure_db(&mut self, database: u64) {
        if database as usize >= self.type_counts.len() {
            self.type_counts.resize(database as usize + 1, [0; 6]);
        }
    }
}

pub fn print_info(state: &RdbState) {
    println!("[info] {} keys read", state.keys);
    println!("[info] {} expires", state.expires);
    println!("[info] {} already expired", state.already_expired);
    println!("[info] {} functions", state.functions);
    if state.lua_scripts > 0 {
        println!("[info] {} lua scripts read", state.lua_scripts);
    }
}

pub fn rdb_check_error(failure: &RdbCheckFailure) {
    println!("--- RDB ERROR DETECTED ---");
    println!("[offset {}] {}", failure.offset, failure.message);
    println!("[additional info] While doing: {}", failure.phase.as_str());
    if failure.state.keys > 0 {
        println!("[info] {} keys read", failure.state.keys);
    }
}

pub fn write_stats(
    state: &mut RdbState,
    format: RdbOutputFormat,
    writer: &mut dyn Write,
) -> io::Result<()> {
    for database in 0..=state.databases {
        state.ensure_db(database);
        write_database_stats(state, database, format, writer)?;
    }
    Ok(())
}

fn write_database_stats(
    state: &RdbState,
    database: u64,
    format: RdbOutputFormat,
    writer: &mut dyn Write,
) -> io::Result<()> {
    for (index, name) in STATS_TYPE_NAMES.iter().enumerate() {
        let total = state.type_counts[database as usize][index];
        match format {
            RdbOutputFormat::Info => {
                writeln!(
                    writer,
                    "[info] db.{}.type.{}.keys.total:{}",
                    database, name, total
                )?;
            }
            RdbOutputFormat::Csv => {
                writeln!(writer, "db.{}.type.{}.keys.total,{}", database, name, total)?;
            }
            RdbOutputFormat::Table => {
                writeln!(
                    writer,
                    "db.{}.type.{}.keys.total\t{}",
                    database, name, total
                )?;
            }
        }
    }
    Ok(())
}
