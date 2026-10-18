
use super::{
    GeoHashArea, GeoHashBits, GeoHashNeighbors, GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX,
    GEO_LONG_MIN, GEO_STEP_MAX, MERCATOR_MAX,
};

pub fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];

    let mut x = xlo as u64;
    let mut y = ylo as u64;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];
    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];
    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];
    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];
    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    x | (y << 1)
}

pub(super) fn deinterleave64(interleaved: u64) -> (u32, u32) {
    const B: [u64; 6] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];
    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];
    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];
    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];
    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];
    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    (x as u32, y as u32)
}

fn geohash_move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let mut x = hash.bits & 0xaaaaaaaaaaaaaaaau64;
    let y = hash.bits & 0x5555555555555555u64;
    let zz = 0x5555555555555555u64 >> (64 - hash.step as u32 * 2);

    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }
    x &= 0xaaaaaaaaaaaaaaaau64 >> (64 - hash.step as u32 * 2);
    hash.bits = x | y;
}

fn geohash_move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let x = hash.bits & 0xaaaaaaaaaaaaaaaau64;
    let mut y = hash.bits & 0x5555555555555555u64;
    let zz = 0xaaaaaaaaaaaaaaaau64 >> (64 - hash.step as u32 * 2);

    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555555555555555u64 >> (64 - hash.step as u32 * 2);
    hash.bits = x | y;
}

pub(super) fn geohash_neighbors(hash: &GeoHashBits) -> GeoHashNeighbors {
    let mut n = GeoHashNeighbors {
        east: *hash,
        west: *hash,
        north: *hash,
        south: *hash,
        south_east: *hash,
        south_west: *hash,
        north_east: *hash,
        north_west: *hash,
    };

    geohash_move_x(&mut n.east, 1);
    geohash_move_x(&mut n.west, -1);
    geohash_move_y(&mut n.south, -1);
    geohash_move_y(&mut n.north, 1);
    geohash_move_x(&mut n.north_west, -1);
    geohash_move_y(&mut n.north_west, 1);
    geohash_move_x(&mut n.north_east, 1);
    geohash_move_y(&mut n.north_east, 1);
    geohash_move_x(&mut n.south_east, 1);
    geohash_move_y(&mut n.south_east, -1);
    geohash_move_x(&mut n.south_west, -1);
    geohash_move_y(&mut n.south_west, -1);
    n
}

pub(super) fn geohash_encode_step(lon: f64, lat: f64, step: u8) -> Option<GeoHashBits> {
    if step == 0 || step > 32 {
        return None;
    }
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
        return None;
    }
    let lat_offset = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    let lon_offset = (lon - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);
    let scale = 1u64 << step;
    let lat_bits = (lat_offset * scale as f64) as u32;
    let lon_bits = (lon_offset * scale as f64) as u32;
    let bits = interleave64(lat_bits, lon_bits);
    Some(GeoHashBits { bits, step })
}

pub fn geohash_encode(lon: f64, lat: f64) -> Option<u64> {
    let h = geohash_encode_step(lon, lat, GEO_STEP_MAX)?;
    Some(geohash_align52_bits(&h))
}

pub(super) fn geohash_decode_area(hash: &GeoHashBits) -> GeoHashArea {
    let step = hash.step;
    let (ilat, ilon) = deinterleave64(hash.bits);
    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let lon_scale = GEO_LONG_MAX - GEO_LONG_MIN;
    let denom = 1u64 << step;

    let lat_min = GEO_LAT_MIN + (ilat as f64 / denom as f64) * lat_scale;
    let lat_max = GEO_LAT_MIN + ((ilat as u64 + 1) as f64 / denom as f64) * lat_scale;
    let lon_min = GEO_LONG_MIN + (ilon as f64 / denom as f64) * lon_scale;
    let lon_max = GEO_LONG_MIN + ((ilon as u64 + 1) as f64 / denom as f64) * lon_scale;

    GeoHashArea {
        longitude: (lon_min, lon_max),
        latitude: (lat_min, lat_max),
    }
}

pub fn geohash_decode(bits: u64) -> Option<(f64, f64)> {
    let hash = GeoHashBits {
        bits: bits >> (52 - GEO_STEP_MAX as u32 * 2),
        step: GEO_STEP_MAX,
    };
    let area = geohash_decode_area(&hash);
    let lon = (area.longitude.0 + area.longitude.1) / 2.0;
    let lat = (area.latitude.0 + area.latitude.1) / 2.0;
    Some((lon, lat))
}

pub fn geohash_align52_bits(hash: &GeoHashBits) -> u64 {
    hash.bits << (52 - hash.step as u32 * 2)
}

pub(super) fn geohash_estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return 26;
    }
    let mut range_m = range_meters;
    let mut step = 1i32;
    while range_m < MERCATOR_MAX {
        range_m *= 2.0;
        step += 1;
    }
    step -= 2;
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}
