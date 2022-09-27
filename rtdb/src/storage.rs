use std::fmt::Debug;
use std::str::FromStr;

pub mod series;
pub mod field;
mod float_field;
pub mod field_block;
pub mod field_index;


// TODO: use a default path, e.g. /var/lib/rtdb/data
const DEFAULT_DATA_DIR: &str = "data";