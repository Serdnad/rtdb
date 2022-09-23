use std::io;
use std::io::{Read, Write};
use std::str::from_utf8;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tokio::io::AsyncWriteExt;

use crate::execution::QueryResult;
use crate::network::ACTION_QUERY;
use crate::storage::series::{DataRow, RecordCollection};
use crate::wire_protocol::{DataType, Field};

pub type ByteReader<'a> = io::Cursor<&'a [u8]>;

// TODO: technically, this module really only needs to have the code to construct a query response.
//  the actual query is constructed by the client, so maybe we should take that out of here?
//  is there any benefit to that code being inside of the actual database codebase, or is it just
//  bloat? plus it'd be kind of nice to have a small and lean reference implementation for porting
//  to other languages (which will also be a pain, but oh well.
//  I think I'll keep it together for now because it's easier to test that way, but then I'll extract
//  the client stuff to a new client lib.

/// returns a buffer containing a query command, with a textual query.
/// a query command is formatted as follows:

/// ```markdown
/// [action]  [query]
/// u8        ucsd str
/// ```
#[inline]
pub fn build_query_command(query: &str) -> Vec<u8> {
    let len = query.len() as u16;

    // add 3 bytes for action and query length
    let mut buffer = Vec::with_capacity((len + 3) as usize);

    buffer.push(ACTION_QUERY);
    buffer.extend(len.to_be_bytes());
    buffer.extend(query.as_bytes());
    buffer
}

/// A query response is formatted as follows:
/// ```markdown
/// [N_ROWS] [COLS_SUMMARY] [ROWS]
/// u32      col_summaries  []data_row
///
/// [COL_SUMMARIES]:
/// [N_SUMMARIES] [SUMMARIES]
/// u8            []col_summary
///
/// [COL_SUMMARY]:
/// [COL_TYPE]   [COL_NAME]
/// u8           UCSD Str(u8)
/// ```
// pub fn build_query_response(result: QueryResult) {}


/// Append a field description to a buffer.
///
/// A field description is formatted as:
/// [DATA_TYPE] [NAME]
/// u8          PStr(u8)
#[inline]
fn write_field_description(mut buffer: &mut Vec<u8>, name: &str, data_type: DataType) {
    buffer.push(data_type as u8);
    buffer.push(name.len() as u8);
    buffer.extend(name.as_bytes());
}

/// Append multiple field descriptions to a buffer, prefixed by the number of fields being printed
/// as a u8.
fn write_field_descriptions(mut buffer: &mut Vec<u8>, fields: Vec<Field>) {
    buffer.push(fields.len() as u8);
    for field in fields {
        write_field_description(&mut buffer, &field.name, field.data_type)
    }
}

// fn build_field_summaries(fields: Vec<Field>) -> Vec<u8> {
//     let mut buffer = Vec::with_capacity((2 + name.len()));
//
//     buffer.push(data_type as u8);
//     buffer.push(name.len() as u8);
//     buffer.extend(name.as_bytes());
//     buffer
// }

fn parse_field_description(buffer: &mut ByteReader) -> Result<(DataType, String), ()> {
    let data_type = DataType::try_from(buffer.read_u8().unwrap()).unwrap();
    let len = buffer.read_u8().unwrap();

    let mut name_buf = vec![0; len as usize];
    buffer.read_exact(&mut name_buf);
    let name = from_utf8(&name_buf).unwrap();

    Ok((data_type, name.to_owned()))
}


fn parse_field_descriptions(buffer: &mut ByteReader) -> Result<Vec<Field>, ()> {
    let n = buffer.read_u8().unwrap();

    let mut fields = vec![];
    for _ in 0..n {
        let (data_type, name) = parse_field_description(buffer).unwrap();
        let f = Field { name, data_type };
        fields.push(f);
    }

    Ok(fields)
}

// TODO
// pub fn parse_query_response()

// TODO: generalizing this might be a pain... but oh well
// TODO: figure out what to do about null values
fn write_data_row(mut buffer: &mut Vec<u8>, row: &DataRow) {
    let time = row.time;
    buffer.extend(time.to_be_bytes());

    for entry in &row.elements {
        buffer.extend(entry.unwrap().to_be_bytes());
    }
}

fn parse_data_row(buffer: &mut ByteReader, fields: &Vec<Field>) -> DataRow {
    let mut values = vec![];

    let time = buffer.read_i64::<BigEndian>().unwrap();
    for field in fields {
        let value = match field.data_type {
            DataType::Float => Some(buffer.read_f64::<BigEndian>().unwrap()),
            DataType::Bool => Some(-1.0), // TODO
        };
        values.push(value);
    }

    DataRow { time, elements: values }
}

pub fn build_query_result(result: &QueryResult) -> Vec<u8> {
    let mut buffer = vec![];

    buffer.extend((result.count as u32).to_be_bytes());

    // TODO: gotta refactor this uh oh
    let fields = result.records.fields.iter().map(|f| Field {
        name: f.to_owned(),
        data_type: DataType::Float,
    }).collect();
    write_field_descriptions(&mut buffer, fields);

    for row in &result.records.rows {
        write_data_row(&mut buffer, row);
    }

    buffer
}

pub fn parse_query_result(mut buffer: &mut ByteReader) -> QueryResult {
    let count = buffer.read_u32::<BigEndian>().unwrap();
    let fields = parse_field_descriptions(&mut buffer).unwrap();

    let mut rows = Vec::with_capacity(count as usize);
    for _ in 0..count {
        rows.push(parse_data_row(&mut buffer, &fields));
    }

    QueryResult {
        count: count as usize,
        records: RecordCollection {
            fields: fields.iter().map(|f| f.name.to_owned()).collect(),
            rows,
        },
    }
}

// TODO: write response parsers


/// Returns a buffer containing an INSERT command, with a textual query
// #[inline]
// pub fn build_insert_command(insertion: &str) -> Vec<u8> {
//     let len = insertion.len() as u16;
//
//     // add 3 bytes for action and query length
//     let mut buffer = Vec::with_capacity((len + 3) as usize);
//
//     buffer.push(ACTION_INSERT);
//     buffer.extend(len.to_be_bytes());
//     buffer.extend(insertion.as_bytes());
//     buffer
// }

// -
#[cfg(test)]
mod tests {
    use std::str::from_utf8;

    use byteorder::{BigEndian, ReadBytesExt};

    use crate::execution::QueryResult;
    use crate::network::ACTION_QUERY;
    use crate::storage::series::{DataRow, RecordCollection};
    use crate::wire_protocol::{DataType, Field};
    use crate::wire_protocol::query::{build_query_command, build_query_result, ByteReader, parse_data_row, parse_field_description, parse_field_descriptions, parse_query_result, write_data_row, write_field_description, write_field_descriptions};

    /// Serialize a query result and parse it back.
    #[test]
    fn query_response() {
        let records = RecordCollection {
            fields: vec![String::from("field1"), String::from("field2")],
            rows: vec![DataRow {
                time: 12345678912345,
                elements: vec![Some(123.0), Some(123.01)],
            }],
        };
        let result = QueryResult {
            count: 1,
            records: RecordCollection {
                fields: vec![String::from("field1"), String::from("field2")],
                rows: vec![DataRow {
                    time: 12345678912345,
                    elements: vec![Some(123.0), Some(123.01)],
                }],
            },
        };

        let mut buffer = build_query_result(&result);

        let mut cursor = ByteReader::new(&buffer);
        let result = parse_query_result(&mut cursor);
        assert_eq!(result.count, 1);
        assert_eq!(result.records, records);
    }

    #[test]
    fn query_cmd() {
        let mut cmd = build_query_command("SELECT test_series");
        assert_eq!(cmd.len(), 21);
        assert_eq!(cmd[0], ACTION_QUERY);
        assert_eq!(cmd[1..3], [0, 18]);
        assert_eq!(from_utf8(&cmd[3..]).unwrap(), "SELECT test_series");
    }

    #[test]
    fn gen_field_desc() {
        let mut buffer = vec![];
        let col_summary = write_field_description(&mut buffer, "field1", DataType::Float);
        assert_eq!(buffer[0], 0);
        assert_eq!(buffer[1], 6);
        assert_eq!(from_utf8(&buffer[2..]).unwrap(), "field1");
    }

    #[test]
    fn parse_field_desc() {
        let mut buffer = vec![];
        let mut col_summary = write_field_description(&mut buffer, "field1", DataType::Float);

        let mut cursor = ByteReader::new(&buffer);
        let col_summary = parse_field_description(&mut cursor).unwrap();
        assert_eq!(col_summary.0, DataType::Float);
        assert_eq!(col_summary.1, "field1");
    }

    #[test]
    fn parse_field_descs() {
        let mut buffer = vec![2];
        write_field_description(&mut buffer, "field1", DataType::Float);
        write_field_description(&mut buffer, "field2", DataType::Float);

        let mut cursor = ByteReader::new(&buffer);
        let fields = parse_field_descriptions(&mut cursor).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], Field { data_type: DataType::Float, name: String::from("field1") });
        assert_eq!(fields[1], Field { data_type: DataType::Float, name: String::from("field2") });
    }

    #[test]
    fn gen_field_descs() {
        let mut buffer = vec![];
        write_field_descriptions(&mut buffer, vec![
            Field { data_type: DataType::Float, name: String::from("field1") },
            Field { data_type: DataType::Float, name: String::from("field2") },
        ]);

        let mut cursor = ByteReader::new(&buffer);
        let fields = parse_field_descriptions(&mut cursor).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], Field { data_type: DataType::Float, name: String::from("field1") });
        assert_eq!(fields[1], Field { data_type: DataType::Float, name: String::from("field2") });
    }

    #[test]
    fn gen_data_row() {
        let row = DataRow { time: 12345678912345, elements: vec![Some(123.0), Some(123.01)] };

        let mut buffer = vec![];
        write_data_row(&mut buffer, &row);

        let mut cursor = ByteReader::new(&buffer);
        assert_eq!(cursor.read_i64::<BigEndian>().unwrap(), 12345678912345);
        assert_eq!(cursor.read_f64::<BigEndian>().unwrap(), 123.0);
        assert_eq!(cursor.read_f64::<BigEndian>().unwrap(), 123.01);
    }

    #[test]
    fn parses_data_row() {
        let row = DataRow { time: 12345678912345, elements: vec![Some(123.0), Some(123.01)] };

        let mut buffer = vec![];
        write_data_row(&mut buffer, &row);

        let mut cursor = ByteReader::new(&buffer);
        let parsed_row = parse_data_row(&mut cursor, &vec![
            Field { name: String::from("field1"), data_type: DataType::Float },
            Field { name: String::from("field2"), data_type: DataType::Float },
        ]);

        assert_eq!(parsed_row, row);
    }
}