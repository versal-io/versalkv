use crate::framework::context::ServerContext;

use super::super::parse_mem_unit_unsigned;

pub(super) fn config_set_memory(
    ctx: &mut ServerContext,
    param: &str,
    val_trimmed: &str,
    _val_bytes: &[u8],
) -> Result<(), String> {
    match param {
        "maxmemory" => {
            let Some(n) = parse_mem_unit_unsigned(val_trimmed) else {
                return Err("ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value".to_string());
            };
            ctx.config_mut().server.maxmemory = n;
            ctx.set_needs_eviction(n > 0);
            if ctx.config().server.maxmemory_scripts < 0 {
                ctx.schedule_script_eviction();
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
