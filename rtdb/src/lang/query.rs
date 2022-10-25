use std::str::from_utf8;

use crate::lang::{Aggregation, SelectExpression, Selection, SelectQuery};

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

    let mut query = SelectQuery { series: series_name, selections: vec![], start: None, end: None };
    if index == input.len() {
        return query;
    }

    advance_whitespace(input, &mut index);
    parse_fields(input, &mut index, &mut query.selections);
    advance_whitespace(input, &mut index);
    parse_time_range(input, &mut index, &mut query);

    // TODO: parse WHERE

    // TODO: parse GROUP BY

    // dbg!(&query);

    query
}

fn parse_fields<'a>(s: &'a [u8], index: &mut usize, fields: &mut Vec<Selection<'a>>) {
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
fn parse_field_selection<'a>(s: &'a [u8], index: &mut usize) -> Option<Selection<'a>> {
    let aggregator =
        if parse_ascii("last(", s, index) {
            Some(Aggregation::Last)
        } else if parse_ascii("mean(", s, index) {
            Some(Aggregation::Mean)
        } else if parse_ascii("max(", s, index) {
            Some(Aggregation::Max)
        } else if parse_ascii("min(", s, index) {
            Some(Aggregation::Min)
        } else {
            None
        };


    let (ok, ident) = parse_identifier(s, index);
    if !ok {
        return None;
    }

    let selection = match aggregator {
        Some(aggregator) => {
            parse_ascii(")", s, index);
            Selection::Expression(Box::new(SelectExpression { expression: Selection::Field(ident), aggregator }))
        }
        None => Selection::Field(ident)
    };

    Some(selection)
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
    use crate::lang::{Aggregation, SelectExpression, Selection, SelectQuery};

    #[test]
    fn time_range() {
        let mut input = "after 1663226470079106890".as_bytes();
        let mut query = SelectQuery { series: "test_series", selections: vec![], start: None, end: None };
        let mut index = 0;

        parse_time_range(&mut input, &mut index, &mut query)
    }

    #[test]
    fn select_query_simple() {
        let mut input = String::from("SELECT test_series");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", selections: vec![], start: None, end: None });

        let mut input = String::from("SELECT test_series[value1, value2]");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            selections: vec![Selection::Field("value1"),
                             Selection::Field("value2")],
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
            selections: vec![],
            start: Some(1663226470079106890),
            end: None,
        });

        let mut input = String::from("SELECT test_series[value1, value2] AFTER 1663226470079106890");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            selections: vec![Selection::Field("value1"),
                             Selection::Field("value2")],
            start: Some(1663226470079106890),
            end: None,
        });

        let mut input = String::from("SELECT test_series[value1, value2] BEFORE 1663226470079106895");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            selections: vec![Selection::Field("value1"),
                             Selection::Field("value2")],
            start: None,
            end: Some(
                1663226470079106895),
        });

        let mut input = String::from("SELECT test_series[value1, value2] AFTER 1663226470079106890 BEFORE 1663226470079106895");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            selections: vec![Selection::Field("value1"),
                             Selection::Field("value2")],
            start: Some(1663226470079106890),
            end: Some(1663226470079106895),
        });
    }

    #[test]
    fn select_query_aggregator() {
        let mut input = String::from("SELECT test_series");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery { series: "test_series", selections: vec![], start: None, end: None });


        let mut input = String::from("SELECT test_series[last(value1), value2]");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            selections: vec![Selection::Expression(Box::new(SelectExpression {
                expression: Selection::Field("value1"),
                aggregator: Aggregation::Last,
            })), Selection::Field("value2")],
            start: None,
            end: None,
        });


        let mut input = String::from("SELECT test_series[ value1 ,  mean(value2) ] ");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            selections: vec![Selection::Field("value1"),
                             Selection::Expression(Box::new(SelectExpression {
                                 expression: Selection::Field("value2"),
                                 aggregator: Aggregation::Mean,
                             }))],
            start: None,
            end: None,
        });


        let mut input = String::from("SELECT test_series[min(value1), max(value2), mean(value3)]");
        let query = parse_select(&mut input);
        assert_eq!(query, SelectQuery {
            series: "test_series",
            selections: vec![Selection::Expression(Box::new(SelectExpression {
                expression: Selection::Field("value1"),
                aggregator: Aggregation::Min,
            })),
                             Selection::Expression(Box::new(SelectExpression {
                                 expression: Selection::Field("value2"),
                                 aggregator: Aggregation::Max,
                             })),
                             Selection::Expression(Box::new(SelectExpression {
                                 expression: Selection::Field("value3"),
                                 aggregator: Aggregation::Mean,
                             }))],
            start: None,
            end: None,
        });
    }
}