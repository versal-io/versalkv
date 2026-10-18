use super::geohash_util::*;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

pub fn georadius_generic(
    args: &CommandArgs,
    runtime: &mut dyn CommandRuntime,
    lon: f64,
    lat: f64,
    radius_raw: f64,
    unit_idx: usize,
    opt_start: usize,
    allow_store: bool,
) -> Value {
    let conversion = match unit_to_meters(&args.args[unit_idx]) {
        Some(f) => f,
        None => return Value::error("ERR unsupported unit provided. please use M, KM, FT, MI"),
    };
    let radius_meters = radius_raw * conversion;
    let err_name = if allow_store {
        "STORE option in GEORADIUS"
    } else {
        "GEORADIUS_RO"
    };
    let (opts, storekey) = match parse_radius_optional_args(args, opt_start, allow_store, err_name)
    {
        Ok(v) => v,
        Err(e) => return e,
    };

    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = &args.args[0];
    let map = match db.get_zset(key) {
        Ok(None) => {
            if let Some((skey, _)) = &storekey {
                db.del(skey);
                return Value::integer(0);
            }
            return Value::Array(Vec::new());
        }
        Ok(Some(m)) => m,
        Err(_) => return CommandError::wrong_type(),
    };

    let shape = GeoShape::Circular {
        lon,
        lat,
        radius_meters,
    };
    let results = search_and_process(map.dict(), &shape, &opts, conversion);

    if let Some((skey, storedist)) = storekey {
        use crate::foundation::io::store::structure::zset::ZSetSl;
        use crate::foundation::io::store::structure::hashtable::HashTable;
        use crate::foundation::io::store::Robj;
        let mut ht = HashTable::new();
        for gp in &results {
            let score = if storedist { gp.dist } else { gp.score };
            ht.insert(gp.member.clone(), score);
        }
        if ht.is_empty() {
            db.del(&skey);
        } else {
            let zset = ZSetSl::from_dict(ht);
            db.set(skey.to_vec(), Robj::ZSetSkiplist(zset));
        }
        return Value::integer(results.len() as i64);
    }

    format_search_results(&results, &opts)
}

#[command(
    "GEORADIUS",
    runtime,
    get_keys = crate::framework::commands::key_extractors::georadius_get_keys
)]
pub fn georadius(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(georadius_reply(runtime, args))
}

fn georadius_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 5 {
        return CommandError::wrong_arity("georadius");
    }
    let lon = match parse_double(&args.args[1]) {
        Some(v) => v,
        None => return Value::error("ERR invalid longitude"),
    };
    let lat = match parse_double(&args.args[2]) {
        Some(v) => v,
        None => return Value::error("ERR invalid latitude"),
    };
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return Value::error(format!(
            "ERR invalid longitude,latitude pair {:.6},{:.6}",
            lon, lat
        ));
    }
    let radius_raw = match parse_double(&args.args[3]) {
        Some(v) if v >= 0.0 => v,
        _ => return Value::error("ERR radius cannot be negative"),
    };
    georadius_generic(
        args,
        runtime,
        lon,
        lat,
        radius_raw,
        4,
        5,
        true,
    )
}
