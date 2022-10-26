use std::io;
use std::io::{Read, Write};
use std::str::from_utf8;

use byteorder::{BigEndian, ReadBytesExt};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ClientRecordCollection, DataRow, DataValue};
use crate::execution::{ClientQueryResult, QueryResult};
use crate::wire_protocol::{DataType, FieldDescription, push_str};

pub type ByteReader<'a> = io::Cursor<&'a [u8]>;

// TODO: technically, this module really only needs to have the code to construct a query response.
//  the actual query is constructed by the client, so maybe we should take that out of here?
//  is there any benefit to that code being inside of the actual database codebase, or is it just
//  bloat? plus it'd be kind of nice to have a small and lean reference implementation for porting
//  to other languages (which will also be a pain, but oh well.
//  I think I'll keep it together for now because it's easier to test that way, but then I'll extract
//  the client stuff to a new client lib.

/// Append multiple field descriptions to a buffer, prefixed by the number of fields being printed
/// as a u8.
///
/// A field description is formatted as:
/// [DATA_TYPE] [NAME]
/// u8          PStr(u8)
#[inline]
async fn write_field_descriptions<T>(mut buffer: T, fields: &Vec<FieldDescription>)
    where
        T: AsyncWrite + Unpin + Send
{
    buffer.write(&(fields.len() as u8).to_be_bytes()).await;
    for field in fields {
        let t = (&field.data_type).clone() as u8;
        buffer.write(&t.to_be_bytes()).await;
        push_str(&mut buffer, &field.name).await;
    }
}

#[inline]
fn parse_field_description(buffer: &mut ByteReader) -> Result<(DataType, String), ()> {
    let data_type = DataType::try_from(buffer.read_u8().unwrap()).unwrap();
    let len = buffer.read_u16::<BigEndian>().unwrap();

    let mut name_buf = vec![0; len as usize];
    buffer.read_exact(&mut name_buf);
    let name = from_utf8(&name_buf).unwrap();

    Ok((data_type, name.to_owned()))
}


#[inline]
fn parse_field_descriptions(buffer: &mut ByteReader) -> Result<Vec<FieldDescription>, ()> {
    let n = buffer.read_u8().unwrap();

    let mut fields = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let (data_type, name) = parse_field_description(buffer).unwrap();
        let f = FieldDescription { name, data_type };
        fields.push(f);
    }

    Ok(fields)
}

// TODO: generalizing this might be a pain... but oh well
// TODO: figure out what to do about null values
// #[inline]
// fn write_data_row(buffer: &mut Vec<u8>, row: &DataRow, _fields: &Vec<FieldDescription>) {
//     let time = row.time;
//     buffer.extend(time.to_be_bytes());
//
//     for (_i, entry) in row.elements.iter().enumerate() {
//         buffer.extend(&entry.to_be_bytes()); // TODO: handle None properly
//     };
// }

#[inline]
fn parse_data_row(buffer: &mut ByteReader, fields: &Vec<FieldDescription>) -> DataRow {
    let mut values = Vec::with_capacity(fields.len()); // TODO: don't alloc per row...

    dbg!(&buffer);

    let time = buffer.read_i64::<BigEndian>().unwrap();
    for field in fields {
        let value = match field.data_type {
            // TODO: handle "null" values
            DataType::Timestamp => DataValue::Timestamp(buffer.read_i64::<BigEndian>().unwrap()),
            DataType::Float => DataValue::from(buffer.read_f64::<BigEndian>().unwrap()),
            DataType::Bool => DataValue::Bool(buffer.read_u8().unwrap() == 1),
        };
        values.push(value);
    }

    DataRow { time, elements: values }
}

pub async fn build_query_result<T>(result: &QueryResult, mut out: &mut T)
    where
        T: AsyncWrite + Unpin + Send
{
    let fields = &result.records.fields;

    out.write(&1u8.to_be_bytes()).await; // TODO: replace with constant?
    write_field_descriptions(&mut out, &fields).await;

    out.write(&(result.count as u32).to_be_bytes()).await;
    for elem in &result.records.elements {
        out.write(&elem.to_be_bytes()).await; // TODO: handle None properly
    }
}

pub fn parse_query_result(mut buffer: &mut ByteReader) -> ClientQueryResult {
    let fields = parse_field_descriptions(&mut buffer).unwrap();
    let field_count = fields.len();

    let count = buffer.read_u32::<BigEndian>().unwrap();
    let mut rows = Vec::with_capacity(field_count * count as usize);
    for _ in 0..count {
        rows.push(parse_data_row(&mut buffer, &fields));
    }

    ClientQueryResult {
        count: count as usize,
        records: ClientRecordCollection {
            fields,
            rows,
        },
    }
}

/// Approximate a buffer size to reduce allocations, which are the biggest cost in serializing and
/// deserializing query results.
#[inline]
fn estimate_mem(fields: &Vec<FieldDescription>, row_count: usize) -> usize {
    let mem_estimate = fields.iter().map(|field| {
        let data_size = match field.data_type {
            DataType::Timestamp => 8,
            DataType::Float => 8,
            DataType::Bool => 1,
        } as usize;

        // 32 comes from nowhere, fyi
        32 + field.name.len() + (8 + data_size) * row_count
    }).reduce(|acc, x| acc + x).unwrap_or(0);

    mem_estimate
}

// TODO: write response parsers


// -
#[cfg(test)]
mod tests {
    use std::str::from_utf8;
    use crate::{ClientRecordCollection, DataValue};
    use crate::{DataRow, RecordCollection};
    use crate::execution::QueryResult;
    use crate::wire_protocol::{ClientExecutionResult, DataType, FieldDescription, parse_result};
    use crate::wire_protocol::query::{build_query_result, ByteReader, parse_field_descriptions, write_field_descriptions};

    /// Serialize a query result and parse it back.
    #[tokio::test]
    async fn query_response() {
        let result = QueryResult {
            count: 1,
            records: RecordCollection {
                fields: vec![FieldDescription { name: String::from("field1"), data_type: DataType::Float },
                             FieldDescription { name: String::from("field2"), data_type: DataType::Float }],
                elements: vec![DataValue::Timestamp(12345678912345), DataValue::from(123.0), DataValue::from(123.01)],
            },
        };

        // dbg!(&result);
        let mut buf = vec![];
        build_query_result(&result, &mut buf).await;
        dbg!(&buf);

        let result = parse_result(&mut buf);
        match result {
            ClientExecutionResult::Query(result) => {
                assert_eq!(result.count, 1);
                assert_eq!(result.records, ClientRecordCollection {
                    fields: vec![FieldDescription { name: String::from("field1"), data_type: DataType::Float },
                                 FieldDescription { name: String::from("field2"), data_type: DataType::Float }],
                    rows: vec![DataRow {
                        time: 12345678912345,
                        elements: vec![DataValue::from(123.0), DataValue::from(123.01)],
                    }],
                });
            }
            _ => assert!(false)
        }
    }

    #[tokio::test]
    async fn field_descs() {
        let mut buffer = vec![];
        write_field_descriptions(&mut buffer, &vec![
            FieldDescription { data_type: DataType::Float, name: String::from("field1") },
            FieldDescription { data_type: DataType::Float, name: String::from("field2") },
        ]).await;

        dbg!(&buffer);

        let mut cursor = ByteReader::new(&buffer);
        let fields = parse_field_descriptions(&mut cursor).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], FieldDescription { data_type: DataType::Float, name: String::from("field1") });
        assert_eq!(fields[1], FieldDescription { data_type: DataType::Float, name: String::from("field2") });
    }

    // #[test]
    // fn parse_field_descs() {
    //     let mut buffer = vec![2];
    //     write_field_descriptions(&mut buffer, &vec![
    //         FieldDescription { data_type: DataType::Float, name: String::from("field1") },
    //         FieldDescription { data_type: DataType::Float, name: String::from("field2") },
    //     ]);
    //
    //     let mut cursor = ByteReader::new(&buffer);
    //     let fields = parse_field_descriptions(&mut cursor).unwrap();
    //     assert_eq!(fields.len(), 2);
    //     assert_eq!(fields[0], FieldDescription { data_type: DataType::Bool, name: String::from("field1") });
    //     assert_eq!(fields[1], FieldDescription { data_type: DataType::Float, name: String::from("field2") });
    // }

    // #[test]
    // fn gen_data_row() {
    //     let row = DataRow { time: 12345678912345, elements: vec![DataValue::from(123.0), DataValue::from(123.01)] };
    //
    //     let mut buffer = vec![];
    //     write_data_row(&mut buffer, &row, &vec![]);
    //
    //     let mut cursor = ByteReader::new(&buffer);
    //     assert_eq!(cursor.read_i64::<BigEndian>().unwrap(), 12345678912345);
    //     assert_eq!(cursor.read_f64::<BigEndian>().unwrap(), 123.0);
    //     assert_eq!(cursor.read_f64::<BigEndian>().unwrap(), 123.01);
    // }

    // #[test]
    // fn parses_data_row() {
    //     let row = DataRow { time: 12345678912345, elements: vec![DataValue::from(123.0), DataValue::from(123.01)] };
    //
    //     let mut buffer = vec![];
    //     write_data_row(&mut buffer, &row, &vec![]);
    //
    //     let mut cursor = ByteReader::new(&buffer);
    //     let parsed_row = parse_data_row(&mut cursor, &vec![
    //         FieldDescription { name: String::from("field1"), data_type: DataType::Float },
    //         FieldDescription { name: String::from("field2"), data_type: DataType::Float },
    //     ]);
    //
    //     assert_eq!(parsed_row, row);
    // }

// TODO: test like ^ but with Some (null values) for both float and bool
}