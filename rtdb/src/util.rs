use std::time;

/// Returns the current system time in nanoseconds since Unix epoch as an i64.
pub fn new_timestamp() -> i64 {
    time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64
}
