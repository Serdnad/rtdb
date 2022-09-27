use std::io;
use std::io::Read;
use std::str::from_utf8;

use byteorder::{BigEndian, ReadBytesExt};
use tokio::time;

use crate::{DataRow, DataValue, RecordCollection};
use crate::execution::{ExecutionResult, QueryResult};
use crate::wire_protocol::{DataType, FieldDescription};
use crate::wire_protocol::insert::build_insert_result;

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


// TODO: move
pub fn build_response(result: &ExecutionResult) -> Vec<u8> {
    let buffer = match result {
        ExecutionResult::Query(query_result) => {
            build_query_result(query_result)
        }
        ExecutionResult::Insert(insert_result) => {
            build_insert_result(insert_result)
        }
    };

    buffer
}


// TODO: move
/// Pushes a string onto a buffer, prefixing it with the string's length as a u16
fn push_str(buffer: &mut Vec<u8>, str: &str) {
    // buffer.push(str.len() as u8);
    let len = str.len() as u16;
    buffer.extend(len.to_be_bytes());
    buffer.extend(str.as_bytes());
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
    push_str(&mut buffer, name);
}

/// Append multiple field descriptions to a buffer, prefixed by the number of fields being printed
/// as a u8.
fn write_field_descriptions(mut buffer: &mut Vec<u8>, fields: &Vec<FieldDescription>) {
    buffer.push(fields.len() as u8);
    for field in fields {
        write_field_description(&mut buffer, &field.name, field.data_type.clone())
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

// TODO
// pub fn parse_query_response()

// TODO: generalizing this might be a pain... but oh well
// TODO: figure out what to do about null values
#[inline]
fn write_data_row(buffer: &mut Vec<u8>, row: &DataRow, fields: &Vec<FieldDescription>) {
    let time = row.time;
    buffer.extend(time.to_be_bytes());

    for (i, entry) in row.elements.iter().enumerate() {
        let b = match entry {
            None => match fields[i].data_type {
                DataType::Float => DataValue::from(f64::NAN), // TODO: actual null value (maybe specialization of NaN
                DataType::Bool => DataValue::from(false), // tODO
            },
            Some(v) => *v
        };

        buffer.extend((&entry).unwrap_or(b).to_be_bytes()); // TODO: handle None properly
    }
}

#[inline]
fn parse_data_row(buffer: &mut ByteReader, fields: &Vec<FieldDescription>) -> DataRow {
    let mut values = Vec::with_capacity(fields.len());

    let time = buffer.read_i64::<BigEndian>().unwrap();
    for field in fields {
        let value = match field.data_type {
            DataType::Float => Some(DataValue::from(buffer.read_f64::<BigEndian>().unwrap())),
            DataType::Bool => Some(DataValue::Bool(buffer.read_u8().unwrap() == 1)),
        };
        values.push(value);
    }

    DataRow { time, elements: values }
}

pub fn build_query_result(result: &QueryResult) -> Vec<u8> {
    let fields = &result.records.fields;

    let mut buffer = Vec::with_capacity(estimate_mem(&fields, result.count));
    buffer.push(1); // TODO: this says that it's a query result. move this out of this function (?).
    write_field_descriptions(&mut buffer, &fields);

    buffer.extend((result.count as u32).to_be_bytes());
    for row in &result.records.rows {
        write_data_row(&mut buffer, row, &fields);
    }
    buffer
}

pub fn parse_query_result(mut buffer: &mut ByteReader) -> QueryResult {
    let fields = parse_field_descriptions(&mut buffer).unwrap();

    let count = buffer.read_u32::<BigEndian>().unwrap();
    let mut rows = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let s = time::Instant::now();
        rows.push(parse_data_row(&mut buffer, &fields));
        let elapsed = s.elapsed();

        // println!("{}ns", elapsed.as_nanos());
    }

    QueryResult {
        count: count as usize,
        records: RecordCollection {
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

    use byteorder::{BigEndian, ReadBytesExt};

    use crate::DataValue;
    use crate::execution::{ExecutionResult, QueryResult};
    use crate::{DataRow, RecordCollection};
    use crate::wire_protocol::{DataType, FieldDescription, parse_result};
    use crate::wire_protocol::query::{build_query_result, ByteReader, parse_data_row, parse_field_description, parse_field_descriptions, write_data_row, write_field_description};

    /// Serialize a query result and parse it back.
    #[test]
    fn query_response() {
        let records = RecordCollection {
            fields: vec![FieldDescription { name: String::from("field1"), data_type: DataType::Float },
                         FieldDescription { name: String::from("field2"), data_type: DataType::Float }],
            rows: vec![DataRow {
                time: 12345678912345,
                elements: vec![Some(DataValue::from(123.0)), Some(DataValue::from(123.01))],
            }],
        };
        let result = QueryResult {
            count: 1,
            records: RecordCollection {
                fields: vec![FieldDescription { name: String::from("field1"), data_type: DataType::Float },
                             FieldDescription { name: String::from("field2"), data_type: DataType::Float }],
                rows: vec![DataRow {
                    time: 12345678912345,
                    elements: vec![Some(DataValue::from(123.0)), Some(DataValue::from(123.01))],
                }],
            },
        };

        dbg!(&result);
        let mut buffer = build_query_result(&result);
        let result = parse_result(&mut buffer);
        match result {
            ExecutionResult::Query(result) => {
                assert_eq!(result.count, 1);
                assert_eq!(result.records, records);
            }
            _ => assert!(false)
        }
    }

    #[test]
    fn gen_field_desc() {
        let mut buffer = vec![];
        let _col_summary = write_field_description(&mut buffer, "field1", DataType::Float);
        assert_eq!(buffer[0], 0);
        assert_eq!(buffer[2], 6);
        assert_eq!(from_utf8(&buffer[3..]).unwrap(), "field1");
    }

    #[test]
    fn parse_field_desc() {
        let mut buffer = vec![];
        let _col_summary = write_field_description(&mut buffer, "field1", DataType::Float);

        let mut cursor = ByteReader::new(&buffer);
        let col_summary = parse_field_description(&mut cursor).unwrap();
        assert_eq!(col_summary.0, DataType::Float);
        assert_eq!(col_summary.1, "field1");
    }

    #[test]
    fn parse_field_descs() {
        let mut buffer = vec![2];
        write_field_description(&mut buffer, "field1", DataType::Bool);
        write_field_description(&mut buffer, "field2", DataType::Float);

        let mut cursor = ByteReader::new(&buffer);
        let fields = parse_field_descriptions(&mut cursor).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], FieldDescription { data_type: DataType::Bool, name: String::from("field1") });
        assert_eq!(fields[1], FieldDescription { data_type: DataType::Float, name: String::from("field2") });
    }

    // #[test]
    // fn gen_field_descs() {
    //     let mut buffer = vec![];
    //     write_field_descriptions(&mut buffer, vec![
    //         Field { data_type: DataType::Float, name: String::from("field1") },
    //         Field { data_type: DataType::Float, name: String::from("field2") },
    //     ]);
    //
    //     let mut cursor = ByteReader::new(&buffer);
    //     let fields = parse_field_descriptions(&mut cursor).unwrap();
    //     assert_eq!(fields.len(), 2);
    //     assert_eq!(fields[0], Field { data_type: DataType::Float, name: String::from("field1") });
    //     assert_eq!(fields[1], Field { data_type: DataType::Float, name: String::from("field2") });
    // }

    #[test]
    fn gen_data_row() {
        let row = DataRow { time: 12345678912345, elements: vec![Some(DataValue::from(123.0)), Some(DataValue::from(123.01))] };

        let mut buffer = vec![];
        write_data_row(&mut buffer, &row, &vec![]);

        let mut cursor = ByteReader::new(&buffer);
        assert_eq!(cursor.read_i64::<BigEndian>().unwrap(), 12345678912345);
        assert_eq!(cursor.read_f64::<BigEndian>().unwrap(), 123.0);
        assert_eq!(cursor.read_f64::<BigEndian>().unwrap(), 123.01);
    }

    #[test]
    fn parses_data_row() {
        let row = DataRow { time: 12345678912345, elements: vec![Some(DataValue::from(123.0)), Some(DataValue::from(123.01))] };

        let mut buffer = vec![];
        write_data_row(&mut buffer, &row, &vec![]);

        let mut cursor = ByteReader::new(&buffer);
        let parsed_row = parse_data_row(&mut cursor, &vec![
            FieldDescription { name: String::from("field1"), data_type: DataType::Float },
            FieldDescription { name: String::from("field2"), data_type: DataType::Float },
        ]);

        assert_eq!(parsed_row, row);
    }

    // TODO: test like ^ but with Some (null values) for both float and bool
}