use std::str::from_utf8;
use crate::DataValue;

use crate::lang::util::{advance_whitespace, parse_ascii, parse_identifier, parse_timestamp};
use crate::storage::series::SeriesEntry;
use crate::util::new_timestamp;

/// A valid entry to be inserted into a series.
#[derive(Debug, PartialEq)]
pub struct Insertion {
    pub series: String,
    pub entry: SeriesEntry,
}

// TODO: generalize to strings
/// Attempt to parse a value, starting from s at the given index.
///
/// A value may be a bool, in the form of an unqouted "true" or "false", or a float.
/// TODO: support parsing strings
fn parse_value<'a>(s: &'a [u8], index: &'a mut usize) -> Result<DataValue, String> {
    if s[*index..].starts_with(b"true") {
        *index += 4;
        return Ok(DataValue::Bool(true));
    } else if s[*index..].starts_with(b"false") {
        *index += 5;
        return Ok(DataValue::Bool(false));
    }


    if let Ok((val, len)) = fast_float::parse_partial::<f64, _>(from_utf8(&s[*index..]).unwrap()) {
        *index += len;
        return Ok(DataValue::Float(val))
    }

    Err(format!("failed to parse a value at pos: {}", index))
}

// TODO: return option
#[inline]
fn parse_fields<'a>(s: &'a [u8], index: &'a mut usize, entry: &mut SeriesEntry) {
    while *index < s.len() {
        advance_whitespace(s, index);
        let (success, field) = parse_identifier(s, index);
        if !success {
            break;
        }

        entry.fields.push(from_utf8(field).unwrap().to_owned());

        advance_whitespace(s, index);
        parse_ascii("=", s, index);
        advance_whitespace(s, index);

        match parse_value(s, index) {
            Err(e) => panic!("{}", e),
            Ok(value) => {
                entry.values.push(value);
                *index += 1;
            }
        }
    }
}

/// Attempt to parse an insert statement.
/// TODO: return Result, and handle invalid statements
pub fn parse_insert(raw_query: &mut str) -> Insertion {
    raw_query.make_ascii_lowercase();

    let mut entry = SeriesEntry { values: vec![], fields: vec![], time: 0 };

    let input = raw_query.as_bytes();
    let mut index: usize = 7; // start after "INSERT "

    advance_whitespace(input, &mut index);

    let (_, series) = parse_identifier(input, &mut index);
    let series = from_utf8(series).unwrap().to_owned();
    advance_whitespace(input, &mut index);
    parse_fields(input, &mut index, &mut entry);
    advance_whitespace(input, &mut index);

    if index >= input.len() {
        entry.time = new_timestamp();
        return Insertion { series, entry };
    }

    entry.time = match parse_timestamp(input, &mut index) {
        Some(t) => t,
        None => new_timestamp(),
    };

    Insertion { series, entry }
}

#[cfg(test)]
mod tests {
    use crate::DataValue;
    use crate::lang::insert::{parse_fields, parse_insert};
    use crate::lang::util::parse_identifier;
    use crate::storage::series::SeriesEntry;

    #[test]
    fn parses_insert() {
        let mut query = String::from("INSERT test_series,field1=1.0");
        let entry = parse_insert(&mut query);
        dbg!(entry);

        let mut query = String::from("INSERT test_series,value1=0.5,value2=1 1663644227213092171");
        let entry = parse_insert(&mut query);
        dbg!(entry);
    }

    #[test]
    fn parses_fields() {
        let mut index = 0;
        let mut entry = SeriesEntry { fields: vec![], values: vec![], time: 0 };
        parse_fields(b"field1=1", &mut index, &mut entry);
        assert_eq!(entry.fields[0], String::from("field1"));
        assert_eq!(entry.values[0], DataValue::from(1.0));
        assert_eq!(entry.time, 0);

        let mut index = 0;
        let mut entry = SeriesEntry { fields: vec![], values: vec![], time: 0 };
        parse_fields(b"field1=1.0", &mut index, &mut entry);
        assert_eq!(entry.fields[0], String::from("field1"));
        assert_eq!(entry.values[0], DataValue::from(1.0));
        assert_eq!(entry.time, 0);

        let mut index = 0;
        let mut entry = SeriesEntry { fields: vec![], values: vec![], time: 0 };
        parse_fields(b"field1=1.0,field2=3.01", &mut index, &mut entry);
        assert_eq!(entry.fields, vec![String::from("field1"), String::from("field2")]);
        assert_eq!(entry.values, vec![DataValue::from(1.0), DataValue::from(3.01)]);
        assert_eq!(entry.time, 0);

        let mut index = 0;
        let mut entry = SeriesEntry { fields: vec![], values: vec![], time: 0 };
        parse_fields(b"field1=1.0, field2=true", &mut index, &mut entry);
        assert_eq!(entry.fields, vec![String::from("field1"), String::from("field2")]);
        assert_eq!(entry.values, vec![DataValue::from(1.0), DataValue::from(true)]);
        assert_eq!(entry.time, 0);
    }

    #[test]
    fn parses_identifier() {
        let mut index = 0;

        let (parsed, ident) = parse_identifier(b"test_series,value1=1", &mut index);
        assert_eq!(parsed, true);
        assert_eq!(ident, b"test_series");
        assert_eq!(index, 12);

        let (parsed, ident) = parse_identifier(b"test_series,value1=1", &mut index);
        assert_eq!(parsed, true);
        assert_eq!(ident, b"value1");
        assert_eq!(index, 19);

        let (parsed, _ident) = parse_identifier(b"test_series,value1=1 12345", &mut index);
        assert_eq!(parsed, false);
        assert_eq!(index, 19);

        let mut index = 0;
        let (parsed, ident) = parse_identifier(b"1identifiers_cannot_start_with_number", &mut index);
        assert_eq!(parsed, false);
        assert_eq!(index, 0);

        let mut index = 0;
        let (parsed, ident) = parse_identifier(b"name-with_ch4rs", &mut index);
        assert_eq!(parsed, true);
        assert_eq!(ident, b"name-with_ch4rs");
        assert_eq!(index, 16);
    }
}