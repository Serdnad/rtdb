pub mod series;
pub mod field;
pub mod field_block;
pub mod field_index;
pub mod block_bool;
pub mod storage_block;
pub mod block_manager;


// TODO: use a default path, e.g. /var/lib/rtdb/data
const DEFAULT_DATA_DIR: &str = "data";