


pub mod series;
pub mod field;
mod float_field;
pub mod field_block;
pub mod field_index;
mod field_float;
pub mod block_bool;
pub mod block_storage;
pub mod block_manager;


// TODO: use a default path, e.g. /var/lib/rtdb/data
const DEFAULT_DATA_DIR: &str = "data";