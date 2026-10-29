
use crate::foundation::io::store::Robj;

use super::{
    GeoHashBits, GeoHashRadius, GeoPoint, GeoShape, GEO_LAT_MAX, GEO_LAT_MIN,
    GEO_LONG_MAX, GEO_LONG_MIN,
};
use super::codec::{
    geohash_align52_bits, geohash_decode, geohash_decode_area, geohash_encode_step,
    geohash_estimate_steps_by_radius, geohash_neighbors,
};

pub(super) const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;

pub fn deg_rad(deg: f64) -> f64 {
    deg * std::f64::consts::PI / 180.0
}

pub fn rad_deg(rad: f64) -> f64 {
    rad * 180.0 / std::f64::consts::PI
}

fn geohash_get_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2d) - deg_rad(lat1d)).abs()
}

pub fn geohash_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1r = deg_rad(lon1);
    let lat1r = deg_rad(lat1);
    let lon2r = deg_rad(lon2);
    let v = ((lon2r - lon1r) / 2.0).sin();
    const GEO_EPSILON: f64 = 1e-15;
    if v.abs() <= GEO_EPSILON {
        return geohash_get_lat_distance(lat1, lat2);
    }
    let lat2r = deg_rad(lat2);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

fn bounding_box_for_shape(shape: &GeoShape) -> (f64, f64, f64, f64) {
    match shape {
        GeoShape::Circular {
            lon,
            lat,
            radius_meters,
        } => {
            let height = *radius_meters;
            let width = *radius_meters;
            let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
            let long_delta_top =
                rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat + lat_delta).cos());
            let long_delta_bottom =
                rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat - lat_delta).cos());
            let southern = *lat < 0.0;
            let (min_lon, max_lon) = if southern {
                (lon - long_delta_bottom, lon + long_delta_bottom)
            } else {
                (lon - long_delta_top, lon + long_delta_top)
            };
            (min_lon, lat - lat_delta, max_lon, lat + lat_delta)
        }
        GeoShape::Rectangle {
            lon,
            lat,
            width_meters,
            height_meters,
        } => {
            let height = height_meters / 2.0;
            let width = width_meters / 2.0;
            let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
            let long_delta_top =
                rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat + lat_delta).cos());
            let long_delta_bottom =
                rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat - lat_delta).cos());
            let southern = *lat < 0.0;
            let (min_lon, max_lon) = if southern {
                (lon - long_delta_bottom, lon + long_delta_bottom)
            } else {
                (lon - long_delta_top, lon + long_delta_top)
            };
            (min_lon, lat - lat_delta, max_lon, lat + lat_delta)
        }
        GeoShape::Polygon { vertices, .. } => {
            let mut min_lon = GEO_LONG_MAX;
            let mut max_lon = GEO_LONG_MIN;
            let mut min_lat = GEO_LAT_MAX;
            let mut max_lat = GEO_LAT_MIN;
            for (plon, plat) in vertices {
                if *plon < min_lon {
                    min_lon = *plon;
                }
                if *plon > max_lon {
                    max_lon = *plon;
                }
                if *plat < min_lat {
                    min_lat = *plat;
                }
                if *plat > max_lat {
                    max_lat = *plat;
                }
            }
            (min_lon, min_lat, max_lon, max_lat)
        }
    }
}

fn point_in_polygon(lon: f64, lat: f64, vertices: &[(f64, f64)]) -> bool {
    let n = vertices.len();
    if n < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = vertices[i];
        let (xj, yj) = vertices[j];
        if ((yi > lat) != (yj > lat)) && (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi) {
            inside = !inside;
        }
        j = i;
    }
    inside
}

pub fn point_within_shape(shape: &GeoShape, lon: f64, lat: f64) -> Option<f64> {
    match shape {
        GeoShape::Circular {
            lon: clon,
            lat: clat,
            radius_meters,
        } => {
            let d = geohash_distance(*clon, *clat, lon, lat);
            if d <= *radius_meters {
                Some(d)
            } else {
                None
            }
        }
        GeoShape::Rectangle {
            lon: clon,
            lat: clat,
            width_meters,
            height_meters,
        } => {
            let lat_d = geohash_get_lat_distance(lat, *clat);
            if lat_d > height_meters / 2.0 {
                return None;
            }
            let lon_d = geohash_distance(lon, lat, *clon, lat);
            if lon_d > width_meters / 2.0 {
                return None;
            }
            Some(geohash_distance(*clon, *clat, lon, lat))
        }
        GeoShape::Polygon {
            vertices,
            centroid_lon,
            centroid_lat,
        } => {
            if point_in_polygon(lon, lat, vertices) {
                Some(geohash_distance(*centroid_lon, *centroid_lat, lon, lat))
            } else {
                None
            }
        }
    }
}

pub(super) fn calculate_areas_by_shape(shape: &GeoShape) -> GeoHashRadius {
    let (min_lon, min_lat, max_lon, max_lat) = bounding_box_for_shape(shape);
    let (clon, clat) = match shape {
        GeoShape::Circular { lon, lat, .. }
        | GeoShape::Rectangle { lon, lat, .. }
        | GeoShape::Polygon {
            centroid_lon: lon,
            centroid_lat: lat,
            ..
        } => (*lon, *lat),
    };

    let radius_meters = {
        let d1 = geohash_distance(clon, clat, min_lon, max_lat);
        let d2 = geohash_distance(clon, clat, max_lon, max_lat);
        let d3 = geohash_distance(clon, clat, min_lon, min_lat);
        let d4 = geohash_distance(clon, clat, max_lon, min_lat);
        d1.max(d2).max(d3).max(d4)
    };

    let mut steps = geohash_estimate_steps_by_radius(radius_meters, clat);
    let mut hash =
        geohash_encode_step(clon, clat, steps).unwrap_or(GeoHashBits { bits: 0, step: 0 });
    let mut neighbors = geohash_neighbors(&hash);
    let mut area = geohash_decode_area(&hash);

    if steps > 1 {
        let north = geohash_decode_area(&neighbors.north);
        let south = geohash_decode_area(&neighbors.south);
        let east = geohash_decode_area(&neighbors.east);
        let west = geohash_decode_area(&neighbors.west);
        let mut decrease = false;
        if north.latitude.1 < max_lat {
            decrease = true;
        }
        if south.latitude.0 > min_lat {
            decrease = true;
        }
        if east.longitude.1 < max_lon {
            decrease = true;
        }
        if west.longitude.0 > min_lon {
            decrease = true;
        }
        if decrease {
            steps -= 1;
            hash =
                geohash_encode_step(clon, clat, steps).unwrap_or(GeoHashBits { bits: 0, step: 0 });
            neighbors = geohash_neighbors(&hash);
            area = geohash_decode_area(&hash);
        }
    }

    if steps >= 2 {
        if area.latitude.0 < min_lat {
            neighbors.south = GeoHashBits::default();
            neighbors.south_west = GeoHashBits::default();
            neighbors.south_east = GeoHashBits::default();
        }
        if area.latitude.1 > max_lat {
            neighbors.north = GeoHashBits::default();
            neighbors.north_east = GeoHashBits::default();
            neighbors.north_west = GeoHashBits::default();
        }
        if area.longitude.0 < min_lon {
            neighbors.west = GeoHashBits::default();
            neighbors.south_west = GeoHashBits::default();
            neighbors.north_west = GeoHashBits::default();
        }
        if area.longitude.1 > max_lon {
            neighbors.east = GeoHashBits::default();
            neighbors.south_east = GeoHashBits::default();
            neighbors.north_east = GeoHashBits::default();
        }
    }

    GeoHashRadius { hash, neighbors }
}

pub fn geo_search(zset: &Robj, shape: &GeoShape, limit: usize) -> Vec<GeoPoint> {
    let radius = calculate_areas_by_shape(shape);

    let boxes: [GeoHashBits; 9] = [
        radius.hash,
        radius.neighbors.north,
        radius.neighbors.south,
        radius.neighbors.east,
        radius.neighbors.west,
        radius.neighbors.north_east,
        radius.neighbors.north_west,
        radius.neighbors.south_east,
        radius.neighbors.south_west,
    ];

    let mut results: Vec<GeoPoint> = Vec::new();

    for i in 0..9 {
        if boxes[i].is_zero() {
            continue;
        }
        if i > 0 && boxes[i].bits == boxes[i - 1].bits && boxes[i].step == boxes[i - 1].step {
            continue;
        }

        let min_hash = boxes[i];
        let mut max_hash = boxes[i];
        max_hash.bits = max_hash.bits.wrapping_add(1);
        let min_score = geohash_align52_bits(&min_hash) as f64;
        let max_score = if max_hash.bits < min_hash.bits {
            f64::INFINITY
        } else {
            geohash_align52_bits(&max_hash) as f64
        };

        for (member, score) in zset.zset_range_by_score(
            (min_score, true), (max_score, false), 0, -1, false,
        ) {
            let bits = score as u64;
            let Some((lon, lat)) = geohash_decode(bits) else {
                continue;
            };
            if let Some(dist) = point_within_shape(shape, lon, lat) {
                results.push(GeoPoint {
                    member,
                    dist,
                    score,
                    lon,
                    lat,
                });
                if limit > 0 && results.len() >= limit {
                    return results;
                }
            }
        }
    }

    results
}
