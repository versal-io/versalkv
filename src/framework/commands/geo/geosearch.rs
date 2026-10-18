use super::geohash_util::*;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[derive(Debug, Clone, Copy, PartialEq)]
enum GeoOrigin {
    Member { offset: usize },
    Coordinates { lon: f64, lat: f64 },
    ShapeCentroid,
}

#[derive(Debug, Clone, PartialEq)]
enum GeoShapeSpec {
    Radius {
        radius_meters: f64,
        conversion: f64,
    },
    Box {
        width_meters: f64,
        height_meters: f64,
        conversion: f64,
    },
    Polygon {
        vertices: Vec<(f64, f64)>,
    },
}

struct GeoSearchRequest {
    origin: GeoOrigin,
    shape: GeoShapeSpec,
    opts: SearchResultOptions,
    storedist: bool,
}

#[derive(Default)]
struct GeoSearchBuilder {
    origin: Option<GeoOrigin>,
    shape: Option<GeoShapeSpec>,
    opts: SearchResultOptions,
    storedist: bool,
}

impl GeoSearchRequest {
    fn parse(args: &CommandArgs, start: usize, is_store: bool) -> Result<Self, Value> {
        let mut builder = GeoSearchBuilder::default();
        let mut index = start;
        while index < args.args.len() {
            index = builder.parse_next(args, index, is_store)?;
        }
        builder.finish()
    }

    fn shape(&self) -> GeoShape {
        let (lon, lat) = match self.origin {
            GeoOrigin::Coordinates { lon, lat } => (lon, lat),
            GeoOrigin::Member { .. } | GeoOrigin::ShapeCentroid => (0.0, 0.0),
        };
        match &self.shape {
            GeoShapeSpec::Radius { radius_meters, .. } => GeoShape::Circular {
                lon,
                lat,
                radius_meters: *radius_meters,
            },
            GeoShapeSpec::Box {
                width_meters,
                height_meters,
                ..
            } => GeoShape::Rectangle {
                lon,
                lat,
                width_meters: *width_meters,
                height_meters: *height_meters,
            },
            GeoShapeSpec::Polygon { vertices } => make_polygon_shape(vertices.clone()),
        }
    }

    fn conversion(&self) -> f64 {
        match self.shape {
            GeoShapeSpec::Radius { conversion, .. }
            | GeoShapeSpec::Box { conversion, .. } => conversion,
            GeoShapeSpec::Polygon { .. } => 1.0,
        }
    }
}

impl GeoSearchBuilder {
    fn parse_next(
        &mut self,
        args: &CommandArgs,
        index: usize,
        is_store: bool,
    ) -> Result<usize, Value> {
        if let Some(next) = self.parse_result_option(args, index, is_store) {
            return Ok(next);
        }
        if let Some(next) = self.parse_count(args, index)? {
            return Ok(next);
        }
        if let Some(next) = self.parse_origin(args, index)? {
            return Ok(next);
        }
        if let Some(next) = self.parse_shape(args, index)? {
            return Ok(next);
        }
        Err(CommandError::syntax_error())
    }

    fn parse_result_option(
        &mut self,
        args: &CommandArgs,
        index: usize,
        is_store: bool,
    ) -> Option<usize> {
        let arg = &args.args[index];
        if arg.eq_ignore_ascii_case(b"withdist") && !is_store {
            self.opts.withdist = true;
        } else if arg.eq_ignore_ascii_case(b"withhash") && !is_store {
            self.opts.withhash = true;
        } else if arg.eq_ignore_ascii_case(b"withcoord") && !is_store {
            self.opts.withcoord = true;
        } else if arg.eq_ignore_ascii_case(b"any") {
            self.opts.any = true;
        } else if arg.eq_ignore_ascii_case(b"asc") {
            self.opts.sort = SortOrder::Asc;
        } else if arg.eq_ignore_ascii_case(b"desc") {
            self.opts.sort = SortOrder::Desc;
        } else if arg.eq_ignore_ascii_case(b"storedist") && is_store {
            self.storedist = true;
        } else {
            return None;
        }
        Some(index + 1)
    }

    fn parse_count(&mut self, args: &CommandArgs, index: usize) -> Result<Option<usize>, Value> {
        if !args.args[index].eq_ignore_ascii_case(b"count") || index + 1 >= args.args.len() {
            return Ok(None);
        }
        let count = std::str::from_utf8(&args.args[index + 1])
            .ok()
            .and_then(|value| value.parse::<i64>().ok());
        self.opts.count = Some(match count {
            Some(value) if value > 0 => value,
            Some(_) => return Err(Value::error("ERR COUNT must be > 0")),
            None => return Err(Value::error("ERR value is not an integer or out of range")),
        });
        Ok(Some(index + 2))
    }

    fn parse_origin(&mut self, args: &CommandArgs, index: usize) -> Result<Option<usize>, Value> {
        let arg = &args.args[index];
        if arg.eq_ignore_ascii_case(b"frommember") && index + 1 < args.args.len() {
            if !matches!(self.origin, None | Some(GeoOrigin::Member { .. })) {
                return Ok(None);
            }
            self.origin = Some(GeoOrigin::Member { offset: index + 1 });
            return Ok(Some(index + 2));
        }
        if !arg.eq_ignore_ascii_case(b"fromlonlat") || index + 2 >= args.args.len() {
            return Ok(None);
        }
        if !matches!(self.origin, None | Some(GeoOrigin::Coordinates { .. })) {
            return Ok(None);
        }
        let lon = parse_coordinate(&args.args[index + 1], "longitude")?;
        let lat = parse_coordinate(&args.args[index + 2], "latitude")?;
        validate_coordinates(lon, lat)?;
        self.origin = Some(GeoOrigin::Coordinates { lon, lat });
        Ok(Some(index + 3))
    }

    fn parse_shape(&mut self, args: &CommandArgs, index: usize) -> Result<Option<usize>, Value> {
        let arg = &args.args[index];
        if arg.eq_ignore_ascii_case(b"byradius") && index + 2 < args.args.len() {
            if !matches!(self.shape, None | Some(GeoShapeSpec::Radius { .. })) {
                return Ok(None);
            }
            let radius_raw = match parse_double(&args.args[index + 1]) {
                Some(v) if v >= 0.0 => v,
                _ => return Err(Value::error("ERR radius cannot be negative")),
            };
            let conversion = parse_unit(&args.args[index + 2])?;
            self.shape = Some(GeoShapeSpec::Radius {
                radius_meters: radius_raw * conversion,
                conversion,
            });
            return Ok(Some(index + 3));
        }
        if arg.eq_ignore_ascii_case(b"bybox") && index + 3 < args.args.len() {
            if !matches!(self.shape, None | Some(GeoShapeSpec::Box { .. })) {
                return Ok(None);
            }
            let width = match parse_double(&args.args[index + 1]) {
                Some(v) if v >= 0.0 => v,
                _ => return Err(Value::error("ERR width cannot be negative")),
            };
            let height = match parse_double(&args.args[index + 2]) {
                Some(v) if v >= 0.0 => v,
                _ => return Err(Value::error("ERR height cannot be negative")),
            };
            let conversion = parse_unit(&args.args[index + 3])?;
            self.shape = Some(GeoShapeSpec::Box {
                width_meters: width * conversion,
                height_meters: height * conversion,
                conversion,
            });
            return Ok(Some(index + 4));
        }
        self.parse_polygon(args, index)
    }

    fn parse_polygon(&mut self, args: &CommandArgs, index: usize) -> Result<Option<usize>, Value> {
        if !args.args[index].eq_ignore_ascii_case(b"bypolygon")
            || index + 1 >= args.args.len()
            || !matches!(self.origin, None | Some(GeoOrigin::ShapeCentroid))
            || !matches!(self.shape, None | Some(GeoShapeSpec::Polygon { .. }))
        {
            return Ok(None);
        }
        let count = parse_polygon_count(&args.args[index + 1])?;
        if args.args.len() - (index + 2) < count * 2 {
            return Err(polygon_arity_error());
        }
        let mut vertices = Vec::with_capacity(count);
        for vertex in 0..count {
            let lon = parse_coordinate(&args.args[index + 2 + vertex * 2], "longitude")?;
            let lat = parse_coordinate(&args.args[index + 3 + vertex * 2], "latitude")?;
            validate_coordinates(lon, lat)?;
            vertices.push((lon, lat));
        }
        self.origin = Some(GeoOrigin::ShapeCentroid);
        self.shape = Some(GeoShapeSpec::Polygon { vertices });
        Ok(Some(index + 2 + count * 2))
    }

    fn finish(self) -> Result<GeoSearchRequest, Value> {
        if self.opts.any && self.opts.count.is_none() {
            return Err(Value::error("ERR the ANY argument requires COUNT argument"));
        }
        let origin = self.origin.ok_or_else(|| {
            Value::error(
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
            )
        })?;
        let shape = self.shape.ok_or_else(|| {
            Value::error(
                "ERR exactly one of BYRADIUS, BYBOX and BYPOLYGON can be specified for GEOSEARCH",
            )
        })?;
        Ok(GeoSearchRequest {
            origin,
            shape,
            opts: self.opts,
            storedist: self.storedist,
        })
    }
}

fn parse_coordinate(value: &[u8], name: &str) -> Result<f64, Value> {
    parse_double(value).ok_or_else(|| Value::error(format!("ERR invalid {name}")))
}

fn validate_coordinates(lon: f64, lat: f64) -> Result<(), Value> {
    if (GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon) && (GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return Ok(());
    }
    Err(Value::error(format!(
        "ERR invalid longitude,latitude pair {:.6},{:.6}",
        lon, lat
    )))
}

fn parse_unit(value: &[u8]) -> Result<f64, Value> {
    unit_to_meters(value).ok_or_else(|| {
        Value::error("ERR unsupported unit provided. please use M, KM, FT, MI")
    })
}

fn parse_polygon_count(value: &[u8]) -> Result<usize, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|count| *count >= 3)
        .map(|count| count as usize)
        .ok_or_else(polygon_arity_error)
}

fn polygon_arity_error() -> Value {
    Value::error("ERR GEOSEARCH BYPOLYGON must have at least 3 vertices")
}

pub struct GeoSearchInvocation {
    pub db_index: u32,
    pub key_idx: usize,
    pub opt_start: usize,
    pub is_store: bool,
    pub dest_key: Option<bytes::Bytes>,
}

pub fn geosearch_generic(
    args: &CommandArgs,
    runtime: &mut dyn CommandRuntime,
    invocation: GeoSearchInvocation,
) -> Value {
    let request = match GeoSearchRequest::parse(args, invocation.opt_start, invocation.is_store) {
        Ok(request) => request,
        Err(e) => return e,
    };
    let shape = match resolve_search_shape(&request, args, runtime, &invocation) {
        Ok(shape) => shape,
        Err(reply) => return reply,
    };
    execute_geosearch(&request, args, runtime, &invocation, &shape)
}

fn resolve_search_shape(
    request: &GeoSearchRequest,
    args: &CommandArgs,
    runtime: &mut dyn CommandRuntime,
    invocation: &GeoSearchInvocation,
) -> Result<GeoShape, Value> {
    let mut shape = request.shape();
    if let GeoOrigin::Member { offset } = request.origin {
        let db = match runtime.db_mut_at(invocation.db_index) {
            Some(d) => d,
            None => return Err(CommandError::db_index_out_of_range()),
        };
        let score = match db.get_zset(&args.args[invocation.key_idx]) {
            Ok(None) => {
                if let Some(dkey) = &invocation.dest_key {
                    db.del(dkey);
                    return Err(Value::integer(0));
                }
                return Err(Value::Array(Vec::new()));
            }
            Ok(Some(m)) => match m.dict().get(&args.args[offset]) {
                Some(&s) => s,
                None => {
                    let member = String::from_utf8_lossy(&args.args[offset]);
                    return Err(Value::error(format!("ERR member {} does not exist", member)));
                }
            },
            Err(_) => return Err(CommandError::wrong_type()),
        };
        let (lon, lat) = geohash_decode(score as u64)
            .ok_or_else(|| Value::error("ERR could not decode requested zset member"))?;
        set_shape_origin(&mut shape, lon, lat);
    }
    Ok(shape)
}

fn execute_geosearch(
    request: &GeoSearchRequest,
    args: &CommandArgs,
    runtime: &mut dyn CommandRuntime,
    invocation: &GeoSearchInvocation,
    shape: &GeoShape,
) -> Value {
    let db = match runtime.db_mut_at(invocation.db_index) {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = &args.args[invocation.key_idx];
    let map = match db.get_zset(key) {
        Ok(None) => {
            if let Some(dkey) = &invocation.dest_key {
                db.del(dkey);
                return Value::integer(0);
            }
            return Value::Array(Vec::new());
        }
        Ok(Some(m)) => m,
        Err(_) => return CommandError::wrong_type(),
    };
    let results = search_and_process(map.dict(), shape, &request.opts, request.conversion());
    if let Some(dest_key) = &invocation.dest_key {
        return store_geosearch_results(db, dest_key, &results, request.storedist);
    }
    format_search_results(&results, &request.opts)
}

fn set_shape_origin(shape: &mut GeoShape, lon: f64, lat: f64) {
    match shape {
        GeoShape::Circular {
            lon: shape_lon,
            lat: shape_lat,
            ..
        }
        | GeoShape::Rectangle {
            lon: shape_lon,
            lat: shape_lat,
            ..
        } => {
            *shape_lon = lon;
            *shape_lat = lat;
        }
        GeoShape::Polygon { .. } => {}
    }
}

fn store_geosearch_results(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    dest_key: &[u8],
    results: &[GeoPoint],
    storedist: bool,
) -> Value {
    use crate::foundation::io::store::structure::hashtable::HashTable;
    use crate::foundation::io::store::structure::zset::ZSetSl;
    use crate::foundation::io::store::Robj;
    let mut values = HashTable::new();
    for point in results {
        let score = if storedist { point.dist } else { point.score };
        values.insert(point.member.clone(), score);
    }
    if values.is_empty() {
        db.del(dest_key);
    } else {
        db.set(dest_key.to_vec(), Robj::ZSetSkiplist(ZSetSl::from_dict(values)));
    }
    Value::integer(results.len() as i64)
}

#[command("GEOSEARCH", runtime)]
pub fn geosearch(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("geosearch"));
    }
    let db_index = runtime.selected_db();
    CommandOutcome::reply(geosearch_generic(
        args,
        runtime,
        GeoSearchInvocation {
            db_index,
            key_idx: 0,
            opt_start: 1,
            is_store: false,
            dest_key: None,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn args(parts: &[&'static [u8]]) -> CommandArgs {
        let mut argv = vec![Bytes::from_static(b"GEOSEARCH")];
        argv.extend(parts.iter().map(|part| Bytes::from_static(part)));
        CommandArgs::from_parts(argv)
    }

    #[test]
    fn request_parses_typed_coordinate_origin_and_radius_shape() {
        let request = GeoSearchRequest::parse(
            &args(&[b"places", b"FROMLONLAT", b"1", b"2", b"BYRADIUS", b"3", b"km"]),
            1,
            false,
        )
        .unwrap();

        assert!(matches!(
            request.origin,
            GeoOrigin::Coordinates { lon: 1.0, lat: 2.0 }
        ));
        assert!(matches!(
            request.shape,
            GeoShapeSpec::Radius {
                radius_meters: 3000.0,
                conversion: 1000.0,
            }
        ));
    }

    #[test]
    fn polygon_request_rejects_a_separate_member_origin() {
        let result = GeoSearchRequest::parse(
            &args(&[
                b"places",
                b"FROMMEMBER",
                b"a",
                b"BYPOLYGON",
                b"3",
                b"0",
                b"0",
                b"1",
                b"0",
                b"0",
                b"1",
            ]),
            1,
            false,
        );

        assert!(result.is_err());
    }
}
