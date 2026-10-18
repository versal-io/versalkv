
use bytes::Bytes;
use crate::foundation::io::store::structure::hashtable::HashTable;

#[path = "geohash_results.rs"]
mod geohash_results;

pub use geohash_results::{
    format_search_results, parse_radius_optional_args, search_and_process, SearchResultOptions,
    SortOrder,
};

pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;
pub const GEO_STEP_MAX: u8 = 26;
pub(crate) const MERCATOR_MAX: f64 = 20037726.37;

#[path = "geohash_codec.rs"]
pub(super) mod codec;
#[path = "geohash_ops.rs"]
pub(super) mod ops;

#[derive(Clone, Copy, Default)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

impl GeoHashBits {
    pub(crate) fn is_zero(&self) -> bool {
        self.bits == 0 && self.step == 0
    }
}

#[derive(Clone, Copy)]
pub struct GeoHashArea {
    pub longitude: (f64, f64),
    pub latitude: (f64, f64),
}

#[derive(Clone, Copy, Default)]
pub struct GeoHashNeighbors {
    pub north: GeoHashBits,
    pub east: GeoHashBits,
    pub west: GeoHashBits,
    pub south: GeoHashBits,
    pub south_east: GeoHashBits,
    pub south_west: GeoHashBits,
    pub north_east: GeoHashBits,
    pub north_west: GeoHashBits,
}

#[derive(Clone, Copy)]
pub struct GeoHashRadius {
    pub hash: GeoHashBits,
    pub neighbors: GeoHashNeighbors,
}

pub enum GeoShape {
    Circular {
        lon: f64,
        lat: f64,
        radius_meters: f64,
    },
    Rectangle {
        lon: f64,
        lat: f64,
        width_meters: f64,
        height_meters: f64,
    },
    Polygon {
        vertices: Vec<(f64, f64)>,
        centroid_lon: f64,
        centroid_lat: f64,
    },
}

pub struct GeoPoint {
    pub member: Bytes,
    pub dist: f64,
    pub score: f64,
    pub lon: f64,
    pub lat: f64,
}

pub use codec::{geohash_decode, geohash_encode, interleave64};
pub use ops::{geo_search, geohash_distance};
pub(crate) use ops::{deg_rad, rad_deg};

pub fn unit_to_meters(unit: &[u8]) -> Option<f64> {
    match unit {
        b"m" | b"M" => Some(1.0),
        b"km" | b"KM" => Some(1000.0),
        b"ft" | b"FT" => Some(0.3048),
        b"mi" | b"MI" => Some(1609.34),
        _ => None,
    }
}

pub fn geohash_to_base32(bits: u64) -> Option<String> {
    let (lon, lat) = geohash_decode(bits)?;

    let long_range_min = -180.0;
    let long_range_max = 180.0;
    let lat_range_min = -90.0;
    let lat_range_max = 90.0;

    if lon < long_range_min || lon > long_range_max || lat < lat_range_min || lat > lat_range_max {
        return None;
    }

    let lat_offset = (lat - lat_range_min) / (lat_range_max - lat_range_min);
    let lon_offset = (lon - long_range_min) / (long_range_max - long_range_min);

    let step = 26u8;
    let scale = 1u64 << step;
    let lat_bits = (lat_offset * scale as f64) as u32;
    let lon_bits = (lon_offset * scale as f64) as u32;
    let hash_bits = interleave64(lat_bits, lon_bits);

    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut buf = [0u8; 11];
    for (i, b) in buf.iter_mut().enumerate() {
        let idx = if i == 10 {
            0
        } else {
            ((hash_bits >> (52 - ((i + 1) * 5))) & 0x1f) as usize
        };
        *b = ALPHABET[idx];
    }
    Some(
        std::str::from_utf8(&buf)
            .expect("geohash alphabet is ASCII")
            .to_string(),
    )
}

pub fn format_distance(d: f64) -> String {
    format!("{:.4}", d)
}

pub fn parse_double(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    s.parse::<f64>().ok().filter(|f| !f.is_nan())
}

fn polygon_centroid(vertices: &[(f64, f64)]) -> (f64, f64) {
    let mut x = 0.0f64;
    let mut y = 0.0f64;
    let mut z = 0.0f64;
    for (lon, lat) in vertices {
        let lon_r = deg_rad(*lon);
        let lat_r = deg_rad(*lat);
        x += lat_r.cos() * lon_r.cos();
        y += lat_r.cos() * lon_r.sin();
        z += lat_r.sin();
    }
    let central_lon = y.atan2(x);
    let central_hyp = (x * x + y * y).sqrt();
    let central_lat = z.atan2(central_hyp);
    (rad_deg(central_lon), rad_deg(central_lat))
}

pub fn make_polygon_shape(vertices: Vec<(f64, f64)>) -> GeoShape {
    let (clon, clat) = polygon_centroid(&vertices);
    GeoShape::Polygon {
        vertices,
        centroid_lon: clon,
        centroid_lat: clat,
    }
}

// Need these for codec & ops to find GeoHashBits structs through super:: path
// when they use `#[path]` inclusion. All types are above as pub.
#[allow(dead_code)]
fn _ensure_imports(_: &HashTable<f64>, _b: Bytes) {}
#[allow(dead_code)]
fn _unused(_: impl FnOnce() -> HashTable<()>) {
    // Force HashTable and Bytes to be used to silence unused warnings
    // (callers outside via pub use already use them, but the above imports are
    // needed for the type definitions used by submodules.)
}
command_macros::portable_command_support!();
