use std::collections::HashSet;

use bytes::Bytes;
use mlua::prelude::*;

use crate::engine::extension::script::{ScriptError, ScriptRunCtx};
use crate::engine::host::HostDispatcher;

const MAX_FORMAT_DEPTH: usize = 16;
const MAX_FORMAT_TABLE_ENTRIES: usize = 1024;
const MAX_DEPTH_MESSAGE: &str = "<max recursion level reached! Nested table?>";
const FORMAT_WORK_LIMIT_MESSAGE: &[u8] = b"<format work limit reached>";
#[cfg(test)]
const MAX_DEBUGGER_FORMAT_BYTES: usize = 1024 * 1024;

pub(crate) struct LuaFormatOutput {
    pub(crate) bytes: Bytes,
    pub(crate) truncated: bool,
}

pub(crate) fn eval_fragment(
    fragment: &[u8],
    run: &mut ScriptRunCtx,
    dispatcher: &mut HostDispatcher<'_>,
    output_limit: usize,
) -> LuaFormatOutput {
    let result = super::runtime::with_lua_runtime(run.lua_enable_insecure_api, |runtime| {
        let lua = &runtime.lua;

        let mut expression = Vec::with_capacity(b"return ".len() + fragment.len());
        expression.extend_from_slice(b"return ");
        expression.extend_from_slice(fragment);
        let function = match lua
            .load(expression.as_slice())
            .set_name("@ldb_eval")
            .into_function()
        {
            Ok(function) => function,
            Err(_) => match lua.load(fragment).set_name("@ldb_eval").into_function() {
                Ok(function) => function,
                Err(error) => {
                    return Ok(format_lua_display_log(
                        b"Error compiling code: ",
                        &error,
                        output_limit,
                    ));
                }
            },
        };

        match super::runtime::with_lua_host(lua, run, dispatcher, || function.call::<LuaValue>(()))
        {
            Ok(value) => format_lua_value_log(&value, b"<retval> ", output_limit)
                .map_err(|error| ScriptError::Runtime(error.to_string())),
            Err(error) => Ok(format_lua_display_log(b"<error> ", &error, output_limit)),
        }
    });

    result.unwrap_or_else(|error| format_lua_display_log(b"<error> ", &error, output_limit))
}

pub(crate) fn bind_eval_globals(
    enable_insecure_api: bool,
    keys: &[Bytes],
    args: &[Bytes],
) -> Result<(), ScriptError> {
    super::runtime::with_lua_runtime(enable_insecure_api, |runtime| {
        super::runtime::bind_eval_arguments(&runtime.lua, keys, args)
            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
        Ok(())
    })
}

pub(crate) fn format_eval_global(
    name: &str,
    enable_insecure_api: bool,
    output_limit: usize,
) -> Result<LuaFormatOutput, ScriptError> {
    super::runtime::with_lua_runtime(enable_insecure_api, |runtime| {
        let value = runtime
            .lua
            .globals()
            .raw_get::<LuaValue>(name)
            .map_err(|error| ScriptError::Runtime(error.to_string()))?;
        format_lua_value_with_limit(&value, output_limit)
            .map_err(|error| ScriptError::Runtime(error.to_string()))
    })
}

fn format_lua_value_log(
    value: &LuaValue,
    prefix: &[u8],
    limit: usize,
) -> LuaResult<LuaFormatOutput> {
    let mut output = LuaFormatWriter::new(limit);
    let mut work = LuaFormatWork::default();
    output.push(prefix);
    format_lua_value_at_depth(value, 0, &mut HashSet::new(), &mut work, &mut output)?;
    Ok(output.finish())
}

fn format_lua_display_log(
    prefix: &[u8],
    value: &impl std::fmt::Display,
    limit: usize,
) -> LuaFormatOutput {
    let mut output = LuaFormatWriter::new(limit);
    output.push(prefix);
    output.push_display(value);
    output.finish()
}

#[cfg(test)]
fn format_lua_value(value: &LuaValue) -> LuaResult<String> {
    Ok(String::from_utf8_lossy(
        &format_lua_value_with_limit(value, MAX_DEBUGGER_FORMAT_BYTES)?.bytes,
    )
    .into_owned())
}

fn format_lua_value_with_limit(value: &LuaValue, limit: usize) -> LuaResult<LuaFormatOutput> {
    let mut output = LuaFormatWriter::new(limit);
    let mut work = LuaFormatWork::default();
    format_lua_value_at_depth(value, 0, &mut HashSet::new(), &mut work, &mut output)?;
    Ok(output.finish())
}

fn format_lua_value_at_depth(
    value: &LuaValue,
    depth: usize,
    active_tables: &mut HashSet<usize>,
    work: &mut LuaFormatWork,
    output: &mut LuaFormatWriter,
) -> LuaResult<()> {
    if output.is_full() {
        output.mark_truncated();
        return Ok(());
    }
    if depth >= MAX_FORMAT_DEPTH {
        output.push(MAX_DEPTH_MESSAGE.as_bytes());
        return Ok(());
    }
    match value {
        LuaValue::Nil => output.push(b"nil"),
        LuaValue::Boolean(value) => output.push_display(value),
        LuaValue::Integer(value) => output.push_display(value),
        LuaValue::Number(value) => output.push_display(value),
        LuaValue::String(value) => output.push(value.as_bytes().as_ref()),
        LuaValue::Table(table) => {
            return format_lua_table(table, depth, active_tables, work, output);
        }
        LuaValue::Function(_) => output.push(b"<function>"),
        LuaValue::UserData(_) => output.push(b"<userdata>"),
        LuaValue::Thread(_) => output.push(b"<thread>"),
        LuaValue::LightUserData(_) => output.push(b"<light-userdata>"),
        LuaValue::Error(error) => {
            output.push(b"<error: ");
            output.push_display(error);
            output.push(b">");
        }
        _ => output.push(b"<unknown-lua-type>"),
    }
    Ok(())
}

fn format_lua_table(
    table: &LuaTable,
    depth: usize,
    active_tables: &mut HashSet<usize>,
    work: &mut LuaFormatWork,
    output: &mut LuaFormatWriter,
) -> LuaResult<()> {
    let table_pointer = table.to_pointer() as usize;
    if !active_tables.insert(table_pointer) {
        output.push(b"<cycle>");
        return Ok(());
    }

    let result = (|| {
        let raw_len = table.raw_len();
        let mut entries = Vec::new();
        let mut max_array_index = 0;
        let mut is_array = true;
        let mut is_work_limited = false;
        for pair in table.clone().pairs::<LuaValue, LuaValue>() {
            if !work.consume_entry() {
                is_work_limited = true;
                break;
            }
            let (key, value) = pair?;
            match table_array_index(&key) {
                Some(index) if index > 0 => max_array_index = max_array_index.max(index),
                _ => is_array = false,
            }
            entries.push((key, value));
        }
        is_array &= max_array_index == entries.len()
            && (raw_len == entries.len() || (is_work_limited && raw_len >= entries.len()));

        output.push(b"{");
        if is_array {
            for index in 1..=entries.len() {
                if output.is_full() {
                    break;
                }
                let value = table.raw_get::<LuaValue>(index)?;
                format_lua_value_at_depth(&value, depth + 1, active_tables, work, output)?;
                output.push(b"; ");
            }
        } else {
            let entry_budget = output.remaining();
            let mut rendered_len = 0;
            let mut rendered_entries = Vec::new();
            for (key, value) in entries {
                if rendered_len >= entry_budget {
                    break;
                }
                let mut entry = LuaFormatWriter::new(entry_budget - rendered_len);
                entry.push(b"[");
                format_lua_value_at_depth(&key, depth + 1, active_tables, work, &mut entry)?;
                entry.push(b"]=");
                format_lua_value_at_depth(&value, depth + 1, active_tables, work, &mut entry)?;
                entry.push(b"; ");
                let entry = entry.finish();
                if entry.truncated {
                    output.mark_truncated();
                }
                rendered_len += entry.bytes.len();
                rendered_entries.push(entry.bytes);
            }
            rendered_entries.sort_unstable();
            for entry in rendered_entries {
                output.push(&entry);
            }
        }
        if is_work_limited {
            output.force_suffix(FORMAT_WORK_LIMIT_MESSAGE);
            if !output.is_full() {
                output.push(b"}");
            }
        } else {
            output.push(b"}");
        }
        Ok(())
    })();

    active_tables.remove(&table_pointer);
    result
}

#[derive(Default)]
struct LuaFormatWork {
    entries: usize,
}

impl LuaFormatWork {
    fn consume_entry(&mut self) -> bool {
        if self.entries >= MAX_FORMAT_TABLE_ENTRIES {
            return false;
        }
        self.entries += 1;
        true
    }
}

struct LuaFormatWriter {
    bytes: Vec<u8>,
    limit: usize,
    truncated: bool,
}

impl LuaFormatWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(256)),
            limit,
            truncated: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        let count = bytes.len().min(self.remaining());
        self.bytes.extend_from_slice(&bytes[..count]);
        self.truncated |= count < bytes.len();
    }

    fn force_suffix(&mut self, suffix: &[u8]) {
        if suffix.len() > self.limit {
            self.bytes.clear();
            self.bytes.extend_from_slice(&suffix[..self.limit]);
        } else {
            self.bytes.truncate(self.limit - suffix.len());
            self.bytes.extend_from_slice(suffix);
        }
        self.truncated = true;
    }

    fn push_display(&mut self, value: impl std::fmt::Display) {
        use std::fmt::Write;

        let _ = write!(self, "{value}");
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.bytes.len())
    }

    fn is_full(&self) -> bool {
        self.remaining() == 0
    }

    fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn finish(self) -> LuaFormatOutput {
        LuaFormatOutput {
            bytes: Bytes::from(self.bytes),
            truncated: self.truncated,
        }
    }
}

impl std::fmt::Write for LuaFormatWriter {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.push(value.as_bytes());
        Ok(())
    }
}

fn table_array_index(value: &LuaValue) -> Option<usize> {
    match value {
        LuaValue::Integer(value) => usize::try_from(*value).ok(),
        LuaValue::Number(value) if value.is_finite() && value.fract() == 0.0 && *value >= 0.0 => {
            Some(*value as usize)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use mlua::prelude::*;

    use super::{
        bind_eval_globals, eval_fragment, format_eval_global, format_lua_value,
        format_lua_value_with_limit, MAX_DEBUGGER_FORMAT_BYTES,
    };
    use crate::engine::extension::script::{ScriptError, ScriptRunCtx};
    use crate::engine::host::{HostError, HostReply, HostRequest};

    const FORMAT_WORK_ENTRY_LIMIT: usize = 1024;
    const FORMAT_WORK_LIMIT_MESSAGE: &[u8] = b"<format work limit reached>";

    fn reject_host_request(_: HostRequest<'_>) -> Result<HostReply, HostError> {
        Err(HostError::Invariant("unexpected debugger host request"))
    }

    #[test]
    fn debugger_value_formatter_distinguishes_arrays_maps_and_cycles() {
        let lua = Lua::new();
        let array = lua
            .load("return {'first', true, 3}")
            .eval::<LuaValue>()
            .unwrap();
        assert_eq!(format_lua_value(&array).unwrap(), "{first; true; 3; }");

        let map = lua.create_table().unwrap();
        map.raw_set("name", "value").unwrap();
        assert_eq!(
            format_lua_value(&LuaValue::Table(map)).unwrap(),
            "{[name]=value; }"
        );

        let mixed = lua.create_table().unwrap();
        mixed.raw_set(1, "first").unwrap();
        mixed.raw_set("name", "value").unwrap();
        assert_eq!(
            format_lua_value(&LuaValue::Table(mixed)).unwrap(),
            "{[1]=first; [name]=value; }"
        );

        let cycle = lua.create_table().unwrap();
        cycle.raw_set("self", cycle.clone()).unwrap();
        assert_eq!(
            format_lua_value(&LuaValue::Table(cycle)).unwrap(),
            "{[self]=<cycle>; }"
        );
    }

    #[test]
    fn debugger_value_formatter_stops_at_byte_budget() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        for index in 1..=1024 {
            table.raw_set(index, "0123456789").unwrap();
        }

        let formatted = format_lua_value_with_limit(&LuaValue::Table(table), 64).unwrap();

        assert_eq!(formatted.bytes.len(), 64);
        assert!(formatted.truncated);
    }

    #[test]
    fn debugger_value_formatter_stops_at_work_entry_budget() {
        const OUTPUT_LIMIT: usize = 128 * 1024;

        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        for index in 1..=FORMAT_WORK_ENTRY_LIMIT + 1 {
            table.raw_set(index, format!("entry-{index}")).unwrap();
        }
        let entry_beyond_budget = format!("entry-{}", FORMAT_WORK_ENTRY_LIMIT + 1);

        let value = LuaValue::Table(table);
        let formatted = format_lua_value_with_limit(&value, OUTPUT_LIMIT).unwrap();

        assert!(formatted
            .bytes
            .windows(FORMAT_WORK_LIMIT_MESSAGE.len())
            .any(|part| part == FORMAT_WORK_LIMIT_MESSAGE));
        assert!(!formatted
            .bytes
            .windows(entry_beyond_budget.len())
            .any(|part| part == entry_beyond_budget.as_bytes()));

        let formatted = format_lua_value_with_limit(&value, 256).unwrap();

        assert!(formatted
            .bytes
            .windows(FORMAT_WORK_LIMIT_MESSAGE.len())
            .any(|part| part == FORMAT_WORK_LIMIT_MESSAGE));
        assert!(formatted.bytes.len() <= 256);
        assert!(formatted.truncated);
    }

    #[test]
    fn debugger_value_formatter_preserves_binary_prefix_at_byte_budget() {
        let lua = Lua::new();
        let value = LuaValue::String(lua.create_string([0xc3, 0xa9, 0xc3, 0xa9]).unwrap());

        let formatted = format_lua_value_with_limit(&value, 3).unwrap();

        assert_eq!(formatted.bytes, &[0xc3, 0xa9, 0xc3][..]);
        assert!(formatted.truncated);
    }

    #[test]
    fn debugger_eval_reports_the_statement_compile_error() {
        let mut run = ScriptRunCtx::new_eval_detached(false, true, true, false);
        let mut dispatcher = reject_host_request;
        bind_eval_globals(
            false,
            &[Bytes::from_static(b"key")],
            &[Bytes::from_static(b"arg")],
        )
        .unwrap();

        let log = eval_fragment(
            b"local =",
            &mut run,
            &mut dispatcher,
            MAX_DEBUGGER_FORMAT_BYTES,
        );

        assert!(log.bytes.starts_with(b"Error compiling code: "));
        assert!(log
            .bytes
            .windows(b"ldb_eval".len())
            .any(|part| part == b"ldb_eval"));
    }

    #[test]
    fn debugger_global_formatter_reads_the_current_lua_global() {
        bind_eval_globals(
            false,
            &[Bytes::from_static(b"original")],
            &[Bytes::from_static(b"arg")],
        )
        .unwrap();
        super::super::runtime::with_lua_runtime(false, |runtime| {
            let keys = runtime
                .lua
                .globals()
                .raw_get::<LuaTable>("KEYS")
                .map_err(|error| ScriptError::Runtime(error.to_string()))?;
            keys.raw_set(1, "changed")
                .map_err(|error| ScriptError::Runtime(error.to_string()))
        })
        .unwrap();

        assert_eq!(
            format_eval_global("KEYS", false, MAX_DEBUGGER_FORMAT_BYTES)
                .unwrap()
                .bytes,
            "{changed; }"
        );
    }
}
