// TODO: a lot of this code, particularly the parsing, will very easily cause panics if
//  the input is not perfect.

use byteorder::ReadBytesExt;
use crate::execution::ExecutionResult;
use crate::wire_protocol::insert::parse_insert_result;
use crate::wire_protocol::query::{ByteReader, parse_query_result};

pub mod query;
pub mod insert;


#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[repr(u8)]
pub enum DataType {
    Float = 0,
    Bool = 1,
}

impl std::convert::TryFrom<u8> for DataType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DataType::Float),
            1 => Ok(DataType::Bool),
            e => {
                dbg!(e);
                Err(())
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, serde::Serialize)]
pub struct FieldDescription {
    pub name: String,
    pub data_type: DataType,
}

impl FieldDescription {
    // pub fn new(name: &str, data_type: &) {
    //
    // }
}

pub fn parse_result(buffer: &mut Vec<u8>) -> ExecutionResult {
    let mut cursor = ByteReader::new(&buffer);
    match cursor.read_u8().unwrap() {
        1 => {
            let result = parse_query_result(&mut cursor);
            ExecutionResult::Query(result)
        }
        2 => {
            let result = parse_insert_result(&mut cursor);
            ExecutionResult::Insert(result)
        }
        _ => panic!("Not supported")
    }
}