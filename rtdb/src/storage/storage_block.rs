use crate::storage::block_bool::{deserialize_bools, serialize_bools, serialize_floats};
use crate::wire_protocol::DataType;

pub enum StorageBlock {
    Bool(Vec<Option<bool>>),
    Float64(Vec<Option<f64>>),
}

impl StorageBlock {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            StorageBlock::Bool(values) => { serialize_bools(values) }
            StorageBlock::Float64(values) => { serialize_floats(values) }
        }
    }

    // TODO: takes either a file handler and an offset, or a byte slice (prob latter), and does the thingy
    //  oh also needs a type...
    pub fn deserialize_from(buffer: &[u8], data_type: DataType) -> StorageBlock {
        match data_type {
            DataType::Float => { todo!() }
            DataType::Bool => { StorageBlock::Bool(deserialize_bools(buffer)) }
            DataType::Timestamp => { todo!() }
        }
    }
}
