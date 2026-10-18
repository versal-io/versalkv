// Store internals that do not receive host time still need a positive, stable
// clock value. Command expiration uses the explicit time owned by EmbeddedRuntime.
const FALLBACK_UNIX_TIME: i64 = 1_700_000_000;

pub fn mstime() -> i64 {
    FALLBACK_UNIX_TIME * 1_000
}

pub fn unixtime() -> i64 {
    FALLBACK_UNIX_TIME
}
