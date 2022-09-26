use std::str::from_utf8;
use crate::DataValue;

use crate::lang::util::{advance_whitespace, parse_ascii, parse_timestamp};
use crate::storage::series::SeriesEntry;
use crate::util::new_timestamp;

/// A valid entry to be inserted into a series.
#[derive(Debug, PartialEq)]
pub struct Insertion {
    pub series: String,
    pub entry: SeriesEntry,
}

// TODO: generalize to booleans and strings
// TODO: return option
fn parse_value<'a>(s: &'a [u8], index: &'a mut usize) -> DataValue {
    if s[*index..].starts_with(b"true") {
        *index += 4;
        return DataValue::Bool(true);
    } else if s[*index..].starts_with(b"false") {
        *index += 5;
        return DataValue::Bool(false);
    }

    let (val, len) = fast_float::parse_partial::<f64, _>(from_utf8(&s[*index..]).unwrap()).unwrap();
    *index += len;
    DataValue::Float(val)
}

/// Attempts to parse an identifier. Identifiers must begin with an alphabetic character.
/// This function assumes that the input has already been converted to lowercase.
/// This function increases index by the length of the parsed identifier.
/// TODO: if we change the break condition from not certain characters to support ranges of ascii, we can reuse this
///     + we should move to new file
#[inline]
fn parse_identifier<'a>(s: &'a [u8], index: &'a mut usize) -> (bool, &'a [u8]) {
    let mut i = 0;

    let first_char = s[*index];
    if first_char < 0x61 || first_char > 0x7A { // test a-z
        return (false, b"");
    }

    for &c in &s[*index..] {
        i += 1;
        if !c.is_ascii_alphanumeric() && c != b'_' && c != b'-' {
            break;
        }
    }

    *index += i;
    (i > 0, &s[*index - i..*index - 1])
}

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

        let value = parse_value(s, index);
        entry.values.push(value); // TODO: support other values

        *index += 1;
    }
}

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

    // TODO: we should tweak things so this check isn't necessary...
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
    use crate::lang::insert::{parse_fields, parse_identifier, parse_insert};
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
    }
}