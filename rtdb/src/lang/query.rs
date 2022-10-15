use std::str::from_utf8;

use crate::lang::{Aggregation, FieldSelection, SelectQuery};
use crate::lang::util::{advance_whitespace, parse_ascii, parse_identifier, parse_timestamp};

/// Parse a full SELECT query.
pub fn parse_select(raw_query: &mut str) -> SelectQuery {
    raw_query.make_ascii_lowercase();

    let mut index: usize = 0;
    let input = raw_query.as_bytes();

    index = 6;
    advance_whitespace(input, &mut index);

    // parse series name
    // let (success, a) = parse_identifier(&input, &mut index);
    let start_index = index;
    while index < input.len() && input[index] != b'[' && input[index] != b' ' {
        index += 1;
    }

    // I wonder if we could avoid this copy, since under the hood this is basically memcpy
    //  TODO: actually, if we switch to parse_identifier, we probably can
    let series_name = from_utf8(&input[start_index..index]).unwrap();

    let mut query = SelectQuery { series: series_name, fields: vec![], start: None, end: None };
    if index == input.len() {
        return query;
    }

    advance_whitespace(input, &mut index);
    parse_fields2(input, &mut index, &mut query.fields);
    advance_whitespace(input, &mut index);
    parse_time_range(input, &mut index, &mut query);

    // TODO: parse WHERE

    // TODO: parse GROUP BY

    // dbg!(&query);

    query
}

// TODO: move this somewhere else

fn parse_fields<'a>(s: &'a [u8], index: &mut usize, fields: &mut Vec<FieldSelection<'a>>) {
    let mut i = *index;

    if s[i] == b'[' {
        i += 1;
        advance_whitespace(s, &mut i);

        let mut start_index = i;
        while i < s.len() {
            match s[i] {
                b',' => {
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(FieldSelection { name: field, aggregator: Aggregation::None });

                    i += 1;
                    advance_whitespace(s, &mut i);
                    start_index = i;
                }
                b']' => {
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(FieldSelection { name: field, aggregator: Aggregation::None });
                    i += 1;
                    break;
                }
                b' ' => { // handle trailing whitespace
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(FieldSelection { name: field, aggregator: Aggregation::None });

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

fn parse_fields2<'a>(s: &'a [u8], index: &mut usize, fields: &mut Vec<FieldSelection<'a>>) {
    if !parse_ascii("[", s, index) {
        return;
    }

    let mut i = *index;
    while i < s.len() {
        advance_whitespace(s, &mut i);
        match s[i] {
            b',' => {
                i += 1;
            }
            b']' => {
                i += 1;
                break;
            }
            _ => {
                if let Some(selection) = parse_field_selection(s, &mut i) {
                    fields.push(selection);
                }
            }
        }
    }

    *index = i;
}

#[inline]
fn parse_field_selection<'a>(s: &'a [u8], index: &mut usize) -> Option<FieldSelection<'a>> {
    let aggregator =
        if parse_ascii("last(", s, index) {
            Aggregation::Last
        } else if parse_ascii("mean(", s, index) {
            Aggregation::Mean
        } else if parse_ascii("max(", s, index) {
            Aggregation::Max
        } else if parse_ascii("min(", s, index) {
            Aggregation::Min
        } else {
            Aggregation::None
        };


    let (ok, ident) = parse_identifier(s, index);
    if !ok {
        return None;
    }

    if aggregator != Aggregation::None {
        parse_ascii(")", s, index);
    }

    Some(FieldSelection { name: ident, aggregator })
}

/// Parses a time range from one of the following formats:
///
/// ```markdown
/// AFTER <timestamp>
/// BEFORE <timestamp>
/// AFTER <timestamp> BEFORE <timestamp>
/// ```
/// and updates the given query.
fn parse_time_range<'a>(s: &'a [u8], mut index: &mut usize, query: &mut SelectQuery<'a>) {
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
    use crate::lang::{Aggregation, FieldSelection, SelectQuery};

    #[test]
    fn time_range() {
        let mut input = "after 1663226470079106890".as_bytes();
        let mut query = SelectQuery { series: "test_series", fields: vec![], start: None, end: None };
        let mut index = 0;

        parse_time_range(&mut input, &mut index, &mut query)
    }

    #[test]
    fn select_query_simple() {
        let mut input = String::from("SELECT test_series");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", fields: vec![], start: None, end: None });

        let mut input = String::from("SELECT test_series[value1, value2]");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![FieldSelection { name: "value1", aggregator: Aggregation::None },
                         FieldSelection { name: "value2", aggregator: Aggregation::None }],
            start: None,
            end: None,
        });
    }

    #[test]
    fn select_query_timestamps() {
        let mut input = String::from("SELECT test_series AFTER 1663226470079106890");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![],
            start: Some(1663226470079106890),
            end: None,
        });

        let mut input = String::from("SELECT test_series[value1, value2] AFTER 1663226470079106890");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![FieldSelection { name: "value1", aggregator: Aggregation::None },
                         FieldSelection { name: "value2", aggregator: Aggregation::None }],
            start: Some(1663226470079106890),
            end: None,
        });

        let mut input = String::from("SELECT test_series[value1, value2] BEFORE 1663226470079106895");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![FieldSelection { name: "value1", aggregator: Aggregation::None },
                         FieldSelection { name: "value2", aggregator: Aggregation::None }],
            start: None,
            end: Some(1663226470079106895),
        });

        let mut input = String::from("SELECT test_series[value1, value2] AFTER 1663226470079106890 BEFORE 1663226470079106895");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![FieldSelection { name: "value1", aggregator: Aggregation::None },
                         FieldSelection { name: "value2", aggregator: Aggregation::None }],
            start: Some(1663226470079106890),
            end: Some(1663226470079106895),
        });
    }

    #[test]
    fn select_query_aggregator() {
        let mut input = String::from("SELECT test_series");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", fields: vec![], start: None, end: None });


        let mut input = String::from("SELECT test_series[last(value1), value2]");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![FieldSelection { name: "value1", aggregator: Aggregation::Last },
                         FieldSelection { name: "value2", aggregator: Aggregation::None }],
            start: None,
            end: None,
        });


        let mut input = String::from("SELECT test_series[ value1 ,  mean(value2) ] ");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![FieldSelection { name: "value1", aggregator: Aggregation::None },
                         FieldSelection { name: "value2", aggregator: Aggregation::Mean }],
            start: None,
            end: None,
        });


        let mut input = String::from("SELECT test_series[min(value1), max(value2), mean(value3)]");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            fields: vec![FieldSelection { name: "value1", aggregator: Aggregation::Min },
                         FieldSelection { name: "value2", aggregator: Aggregation::Max },
                         FieldSelection { name: "value3", aggregator: Aggregation::Mean }],
            start: None,
            end: None,
        });
    }
}