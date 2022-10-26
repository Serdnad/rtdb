// TODO: a lot of this code, particularly the parsing, will very easily cause panics if
//  the input is not perfect.

use std::io::{Write};
use byteorder::ReadBytesExt;
use tokio::net::TcpStream;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

use crate::execution::{ClientQueryResult, ExecutionResult, InsertionResult};
use crate::wire_protocol::insert::{build_insert_result, parse_insert_result};
use crate::wire_protocol::query::{build_query_result, ByteReader, parse_query_result};

pub mod query;
pub mod insert;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[repr(u8)]
pub enum DataType {
    Float = 0,
    Bool = 1,
    Timestamp = 2,
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

/// Serialize an execution result into a byte vector that is ready to be sent back to the client,
/// using a format custom to our database.
/// TODO: the function name is perhaps a little on the nose (or well, verbose)
pub async fn build_response<T>(result: &ExecutionResult, out: &mut T)
    where
        T: AsyncWrite + Unpin + Send
{
    match result {
        ExecutionResult::Query(query_result) => build_query_result(query_result, out).await,
        ExecutionResult::Insert(insert_result) => build_insert_result(insert_result, out).await,
    };
}

// TODO: move to client library and rename
pub enum ClientExecutionResult {
    Query(ClientQueryResult),
    Insert(InsertionResult),
}

// TODO: move to client library
pub fn parse_result(buffer: &mut Vec<u8>) -> ClientExecutionResult {
    let mut cursor = ByteReader::new(&buffer);
    match cursor.read_u8().unwrap() {
        1 => {
            let result = parse_query_result(&mut cursor);
            ClientExecutionResult::Query(result)
        }
        2 => {
            let result = parse_insert_result(&mut cursor);
            ClientExecutionResult::Insert(result)
        }
        _ => panic!("Not supported")
    }
}

/// Pushes a string onto a buffer, prefixing it with the string's length as a u16
#[inline]
async fn push_str<T>(buffer: &mut T, str: &str)
    where
        T: AsyncWrite + Unpin + Send
{
    let len = str.len() as u16;
    buffer.write(&len.to_be_bytes()).await;
    buffer.write(&str.as_bytes()).await;
}