use std::str::from_utf8;

use nom::{AsBytes, ParseTo};

use crate::lang::SelectQuery;

/// Parse a full SELECT query.
pub fn parse_select(raw_query: &mut str) -> SelectQuery {
    raw_query.make_ascii_lowercase();

    let mut index: usize = 0;
    let input = raw_query.as_bytes();

    index = 6;
    advance_whitespace(input, &mut index);

    // parse series name
    let start_index = index;
    while index < input.len() && input[index] != b'[' && input[index] != b' ' {
        index += 1;
    }

    // I wonder if we could avoid this copy, since under the hood this is basically memcpy
    let series_name = from_utf8(&input[start_index..index]).unwrap();

    let mut query = SelectQuery { series: series_name, fields: vec![], start: None, end: None };
    if index == input.len() {
        return query;
    }

    advance_whitespace(input, &mut index);
    parse_fields(input, &mut index, &mut query.fields);
    advance_whitespace(input, &mut index);
    parse_time_range(input, &mut index, &mut query);

    // TODO: parse WHERE

    // TODO: parse GROUP BY

    // dbg!(&query);

    query
}

// TODO: move this somewhere else
#[inline]
fn advance_whitespace(s: &[u8], mut index: &mut usize) {
    let mut i = *index;
    while i < s.len() && s[i] == b' ' {
        i += 1;
    }

    *index = i;
}

fn parse_fields<'a>(s: &'a [u8], mut index: &mut usize, fields: &mut Vec<&'a str>) {
    let mut i = *index;

    if s[i] == b'[' {
        i += 1;
        advance_whitespace(s, &mut i);

        let mut start_index = i;
        while i < s.len() {
            match s[i] {
                b',' => {
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(field);

                    i += 1;
                    advance_whitespace(s, &mut i);
                    start_index = i;
                }
                b']' => {
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(field);
                    i += 1;
                    break;
                }
                b' ' => { // handle trailing whitespace
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(field);

                    advance_whitespace(s, &mut i);

                    if s[i] == b',' {
                        i += 1;
                        advance_whitespace(s, &mut i);
                    } else if s[i] == b']' {
                        break;
                    }

                    start_index = i;
                }
                _ => i += 1,
            }
        }
    }

    *index = i;
}

/// Attempt to parse an ascii literal, and advance the index accordingly if successful.
/// TODO: move
#[inline]
fn parse_ascii(tag: &'static str, s: &[u8], index: &mut usize) -> bool {
    match s[*index..].starts_with(tag.as_bytes()) {
        true => {
            *index += tag.len();
            true
        }
        false => false,
    }
}

/// TODO: make this more sophisticated in what kind of timestamps it can accept
// TODO: move this somewhere else
#[inline]
fn parse_timestamp(s: &[u8], index: &mut usize) -> Option<i64> {
    let mut i = 0;
    for char in &s[*index..] {
        match char.is_ascii_digit() {
            true => i += 1,
            false => break
        }
    }

    match from_utf8(&s[*index..*index + i]).unwrap().parse() {
        Ok(time) => {
            *index += i;
            Some(time)
        }
        Err(_) => None
    }
}


fn parse_time_range<'a>(mut s: &'a [u8], mut index: &mut usize, query: &mut SelectQuery<'a>) {
    if parse_ascii("after", &s, &mut index) {
        advance_whitespace(s, &mut index);
        query.start = parse_timestamp(s, &mut index);
    }

    advance_whitespace(s, &mut index);
    if parse_ascii("before", &s, &mut index) {
        advance_whitespace(s, &mut index);
        query.end = parse_timestamp(s, &mut index);
    }
}

#[cfg(test)]
mod tests {
    use crate::lang::query::{parse_select, parse_time_range};
    use crate::lang::SelectQuery;

    #[test]
    fn time_range() {
        let mut input = "after 1663226470079106890".as_bytes();
        let mut query = SelectQuery { series: "test_series", fields: vec![], start: None, end: None };
        let mut index = 0;

        parse_time_range(&mut input, &mut index, &mut query)
    }

    #[test]
    fn select_query() {
        let mut input = String::from("SELECT test_series");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", fields: vec![], start: None, end: None });
        dbg!(&query);

        let mut input = String::from("SELECT test_series[value1, value2]");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", fields: vec!["value1", "value2"], start: None, end: None });
        dbg!(&query);

        let mut input = String::from("SELECT test_series[value1, value2] AFTER 1663226470079106890");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", fields: vec!["value1", "value2"], start: Some(1663226470079106890), end: None });
        dbg!(&query);

        let mut input = String::from("SELECT test_series[value1, value2] BEFORE 1663226470079106895");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", fields: vec!["value1", "value2"], start: None, end: Some(1663226470079106895) });
        dbg!(&query);

        let mut input = String::from("SELECT test_series[value1, value2] AFTER 1663226470079106890 BEFORE 1663226470079106895");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", fields: vec!["value1", "value2"], start: Some(1663226470079106890), end: Some(1663226470079106895) });
        dbg!(&query);
    }
}