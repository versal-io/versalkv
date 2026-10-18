use crate::framework::commands::geo::geohash_util::{
    format_distance, geo_search, GeoPoint, GeoShape,
};
use crate::framework::CommandError;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::hashtable::HashTable;
use bytes::Bytes;

#[derive(Clone, Copy, PartialEq)]
pub enum SortOrder {
    None,
    Asc,
    Desc,
}

#[derive(Clone, Copy)]
pub struct SearchResultOptions {
    pub withdist: bool,
    pub withhash: bool,
    pub withcoord: bool,
    pub sort: SortOrder,
    pub count: Option<i64>,
    pub any: bool,
}

impl Default for SearchResultOptions {
    fn default() -> Self {
        SearchResultOptions {
            withdist: false,
            withhash: false,
            withcoord: false,
            sort: SortOrder::None,
            count: None,
            any: false,
        }
    }
}

fn fmt_coord_value(v: f64) -> Value {
    let b = if v.is_infinite() {
        if v > 0.0 {
            Bytes::from_static(b"inf")
        } else {
            Bytes::from_static(b"-inf")
        }
    } else if v == v.trunc() && v.abs() < 1e17 {
        Bytes::from(format!("{}", v as i64))
    } else {
        Bytes::from(
            format!("{:.17}", v)
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string(),
        )
    };
    Value::bulk(b)
}

pub fn format_search_results(points: &[GeoPoint], opts: &SearchResultOptions) -> Value {
    let mut option_len = 0;
    if opts.withdist {
        option_len += 1;
    }
    if opts.withhash {
        option_len += 1;
    }
    if opts.withcoord {
        option_len += 1;
    }
    let mut out = Vec::with_capacity(points.len());
    for gp in points {
        if option_len > 0 {
            let mut entry = Vec::with_capacity(1 + option_len);
            entry.push(Value::bulk(gp.member.clone()));
            if opts.withdist {
                entry.push(Value::bulk(Bytes::from(format_distance(gp.dist))));
            }
            if opts.withhash {
                entry.push(Value::integer(gp.score as i64));
            }
            if opts.withcoord {
                entry.push(Value::Array(vec![
                    fmt_coord_value(gp.lon),
                    fmt_coord_value(gp.lat),
                ]));
            }
            out.push(Value::Array(entry));
        } else {
            out.push(Value::bulk(gp.member.clone()));
        }
    }
    Value::Array(out)
}

pub fn search_and_process(
    zset: &HashTable<f64>,
    shape: &GeoShape,
    opts: &SearchResultOptions,
    conversion: f64,
) -> Vec<GeoPoint> {
    let limit = if opts.any {
        opts.count.unwrap_or(0) as usize
    } else {
        0
    };
    let mut results = geo_search(zset, shape, limit);
    let sort = if opts.count.is_some() && opts.sort == SortOrder::None && !opts.any {
        SortOrder::Asc
    } else {
        opts.sort
    };
    match sort {
        SortOrder::Asc => {
            results.sort_by(|a, b| {
                a.dist
                    .partial_cmp(&b.dist)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        SortOrder::Desc => {
            results.sort_by(|a, b| {
                b.dist
                    .partial_cmp(&a.dist)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        SortOrder::None => {}
    }
    if let Some(c) = opts.count {
        let c = c as usize;
        if !opts.any && c < results.len() {
            results.truncate(c);
        }
    }
    for gp in &mut results {
        gp.dist /= conversion;
    }
    results
}

pub fn parse_radius_optional_args(
    args: &crate::framework::CommandArgs,
    start: usize,
    allow_store: bool,
    cmd_name_for_store_err: &str,
) -> Result<(SearchResultOptions, Option<(Bytes, bool)>), Value> {
    let mut opts = SearchResultOptions::default();
    let mut storekey: Option<(Bytes, bool)> = None;
    let mut i = start;
    while i < args.args.len() {
        let arg = &args.args[i];
        if arg.eq_ignore_ascii_case(b"withdist") {
            opts.withdist = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"withhash") {
            opts.withhash = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"withcoord") {
            opts.withcoord = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"any") {
            opts.any = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"asc") {
            opts.sort = SortOrder::Asc;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"desc") {
            opts.sort = SortOrder::Desc;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"count") && i + 1 < args.args.len() {
            let c = match std::str::from_utf8(&args.args[i + 1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) if n > 0 => n,
                Some(_) => return Err(Value::error("ERR COUNT must be > 0")),
                None => return Err(Value::error("ERR value is not an integer or out of range")),
            };
            opts.count = Some(c);
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"store") && i + 1 < args.args.len() && allow_store {
            if storekey.is_some() {
                return Err(CommandError::syntax_error());
            }
            storekey = Some((args.args[i + 1].clone(), false));
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"storedist") && i + 1 < args.args.len() && allow_store
        {
            if storekey.is_some() {
                return Err(CommandError::syntax_error());
            }
            storekey = Some((args.args[i + 1].clone(), true));
            i += 2;
        } else {
            return Err(CommandError::syntax_error());
        }
    }
    if opts.any && opts.count.is_none() {
        return Err(Value::error("ERR the ANY argument requires COUNT argument"));
    }
    if storekey.is_some() && (opts.withdist || opts.withhash || opts.withcoord) {
        return Err(Value::error(format!(
            "ERR {} is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            cmd_name_for_store_err
        )));
    }
    Ok((opts, storekey))
}
