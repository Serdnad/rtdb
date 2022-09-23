use std::collections::HashMap;

use nom::{AsChar, Compare, InputLength, InputTake, InputTakeAtPosition, IResult, Parser};
use nom::branch::alt;
use nom::bytes::complete::*;
use nom::character::complete::{space0, space1};
use nom::character::is_alphanumeric;
use nom::combinator::opt;
use nom::error::ParseError;
use nom::multi::many0;
use nom::sequence::{preceded, terminated};
use crate::lang::insert::Insertion;

use crate::storage::series::SeriesEntry;


mod util;
pub mod query;
pub mod insert;

#[derive(Debug, PartialEq)]
pub enum Action<'a> {
    Select(SelectQuery<'a>),
    Insert(Insertion),
}

#[derive(Debug, PartialEq)]
pub struct SelectQuery<'a> {
    pub series: &'a str,
    pub fields: Vec<&'a str>,

    pub start: Option<i64>,
    pub end: Option<i64>,

    // TODO: filters, group by
}


// TODO: move to different file
// #[derive(Debug, PartialEq)]
// pub struct Insertion<'a> {
//     pub series: &'a str,
//     pub values: HashMap<String, f64>,
//     pub time: i64,
// }

// TODO: move to different file
// impl Into<SeriesEntry> for Insertion<'_> {
//     fn into(self) -> SeriesEntry {
//         SeriesEntry {
//             values: self.values,
//             time: self.time,
//         }
//     }
// }

pub fn parse(query: &mut String) -> Result<Action, &str> {
    query.make_ascii_lowercase();
    let (rem, action) = parse_action(query).unwrap();
    // TODO: check rem, and replace unwrap
    Ok(action)
}

fn parse_action(input: &str) -> IResult<&str, Action> {
    let (rem, action) = alt((tag("select"), tag("insert")))(input)?;
    let action = match action.as_ref() {
        "select" => {
            let (rem, _) = space1(rem)?;
            let (rem, query) = select_query(rem)?;

            Action::Select(query)
        }
        // "insert" => Action::Insert(Insertion {
        //     series: "",
        //     values: Default::default(),
        //     time: 0,
        // }),
        _ => panic!("asd"), // TODO
    };

    Ok((rem, action))
}

fn select_query(input: &str) -> IResult<&str, SelectQuery> {
    let (rem, query) = selection(input)?;

    Ok((rem, query))
}

///
/// Examples:
/// - [field1]
/// - [field1, field2]
fn fields_selection(input: &str) -> IResult<&str, Vec<&str>> {
    let (rem, inner) = preceded(tag("["), terminated(take_until("]"), tag("]")))(input)?;

    let (last, mut fields) = many0(terminated(take_until(","), opt(tag(","))))(inner)?;
    // TODO: is there a cleaner way of doing this, without handling the last one specially?
    if last != "" {
        let (last, _) = space0(last)?;
        fields.push(last);
    }

    // TODO: this might be faster if it were done as part of the initial parsing instead
    for field in fields.iter_mut() {
        *field = field.trim()
    }

    Ok((rem, fields))
}

fn series_selection(input: &str) -> IResult<&str, &str> {
    take_while1(|c| is_alphanumeric(c as u8) || c == '_' || c == '-')(input)
}

fn selection(input: &str) -> IResult<&str, SelectQuery> {
    let (rem, series) = series_selection(input)?;
    let (rem, fields) = preceded(space0, opt(fields_selection))(rem)?;

    Ok((rem, SelectQuery {
        series,
        fields: fields.unwrap_or(vec![]),
        start: None,
        end: None,
    }))
}

pub fn trim<I, O, E: ParseError<I>, F>(
    mut inner: F,
) -> impl FnMut(I) -> IResult<I, O, E>
    where
        F: Parser<I, O, E>,
        I: InputTakeAtPosition,
        <I as InputTakeAtPosition>::Item: AsChar + Clone,
{
    move |input: I| {
        let (input, _) = space0(input)?;
        let (input, cap) = inner.parse(input)?;
        let (input, _) = space0(input)?;

        Ok((input, cap))
    }
}

#[cfg(test)]
mod tests {
    use nom::bytes::complete::{tag, take_until};
    use nom::error::Error;

    use crate::lang::{Action, fields_selection, parse_action, selection, series_selection, trim};

    #[test]
    fn trim_parser() {
        let s1 = "text";
        let s2 = "  text";
        let s3 = "text  ";
        let s4 = " text ";
        let s5 = "te xt";
        let s6 = " te xt ";

        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s1).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s2).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s3).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s4).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("te"))(s5).unwrap(), ("xt", "te"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("te"))(s6).unwrap(), ("xt ", "te"));

        dbg!(trim::<&str, &str, Error<_>, _>(take_until("]"))(" asd ]"));
    }

    // #[test]
    // fn parse_query_action() {
    //     let s1 = "SELECT blah";
    //     let s2 = "select blah";
    //     let s3 = "sEleCt blah";
    //     let s4 = "INSERT blah";
    //     let s5 = "insert blah";
    //     let s6 = "inSeRt blah";
    //
    //     assert_eq!(action(s1), Ok((" blah", Action::Select)));
    //     assert_eq!(action(s2), Ok((" blah", Action::Select)));
    //     assert_eq!(action(s3), Ok((" blah", Action::Select)));
    //     assert_eq!(action(s4), Ok((" blah", Action::Insert)));
    //     assert_eq!(action(s5), Ok((" blah", Action::Insert)));
    //     assert_eq!(action(s6), Ok((" blah", Action::Insert)));
    // }


    #[test]
    fn parse_series_selection() {
        let s1 = "test_series";
        let s2 = "test_series[field1]";
        let s3 = "test_series[field1, field2]";

        assert_eq!(series_selection(s1), Ok(("", "test_series")));
        assert_eq!(series_selection(s2), Ok(("[field1]", "test_series")));
        assert_eq!(series_selection(s3), Ok(("[field1, field2]", "test_series")));
    }

    #[test]
    fn parse_fields() {
        let s1 = "[]";
        let s2 = "[field1]";
        let s3 = "[field1,field2]";
        let s4 = "[field1, field2]";
        let s5 = "[field1, field2, field3]";
        let s6 = "[  field1,  field2 ,field3 ]";

        assert_eq!(fields_selection(s2), Ok(("", vec!["field1"])));
        assert_eq!(fields_selection(s3), Ok(("", vec!["field1", "field2"])));
        assert_eq!(fields_selection(s4), Ok(("", vec!["field1", "field2"])));
        assert_eq!(fields_selection(s5), Ok(("", vec!["field1", "field2", "field3"])));
        assert_eq!(fields_selection(s6), Ok(("", vec!["field1", "field2", "field3"])));
    }

    #[test]
    fn parse_series_with_fields() {
        let s1 = "test_series";
        let s2 = "test_series[field1]";
        let s3 = "test_series[field1, field2]";
        let s4 = "test_series [field1, field2] AFTER now()-5m";

        let (rem, q) = selection(s1).unwrap();
        assert_eq!(rem, "");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, Vec::<String>::new());

        let (rem, q) = selection(s2).unwrap();
        assert_eq!(rem, "");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, vec!["field1"]);

        let (rem, q) = selection(s3).unwrap();
        assert_eq!(rem, "");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, vec!["field1", "field2"]);

        let (rem, q) = selection(s4).unwrap();
        assert_eq!(rem, " AFTER now()-5m");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, vec!["field1", "field2"]);
    }

    #[test]
    fn parse_basic_select() {
        let s1 = "select test_series";
        let s2 = "select test_series[field1]";
        let s3 = "select test_series[field1, field2]";

        let (rem, action) = parse_action(s1).unwrap();
        let query = if let Action::Select(query) = action { query } else { panic!() };
        assert_eq!(query.series, "test_series");
        assert_eq!(query.fields, Vec::<String>::new());

        let (rem, action) = parse_action(s2).unwrap();
        let query = if let Action::Select(query) = action { query } else { panic!() };
        assert_eq!(query.series, "test_series");
        assert_eq!(query.fields, vec!["field1"]);

        let (rem, action) = parse_action(s3).unwrap();
        let query = if let Action::Select(query) = action { query } else { panic!() };
        assert_eq!(query.series, "test_series");
        assert_eq!(query.fields, vec!["field1", "field2"]);
    }
}

fn parse2(query: &str) {
    // if query.starts_with("")
// match query. { }
}

#[cfg(test)]
mod tests2 {
    use nom::bytes::complete::{tag, take_until};
    use nom::error::Error;

    use crate::lang::{Action, fields_selection, parse_action, selection, series_selection, trim};

    #[test]
    fn trim_parser() {
        let s1 = "text";
        let s2 = "  text";
        let s3 = "text  ";
        let s4 = " text ";
        let s5 = "te xt";
        let s6 = " te xt ";

        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s1).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s2).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s3).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("text"))(s4).unwrap(), ("", "text"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("te"))(s5).unwrap(), ("xt", "te"));
        assert_eq!(trim::<&str, &str, Error<_>, _>(tag("te"))(s6).unwrap(), ("xt ", "te"));

        dbg!(trim::<&str, &str, Error<_>, _>(take_until("]"))(" asd ]"));
    }

    #[test]
    fn parse_query_action() {
        let s1 = "SELECT blah";
    }


    #[test]
    fn parse_series_selection() {
        let s1 = "test_series";
        let s2 = "test_series[field1]";
        let s3 = "test_series[field1, field2]";

        assert_eq!(series_selection(s1), Ok(("", "test_series")));
        assert_eq!(series_selection(s2), Ok(("[field1]", "test_series")));
        assert_eq!(series_selection(s3), Ok(("[field1, field2]", "test_series")));
    }

    #[test]
    fn parse_fields() {
        let s1 = "[]";
        let s2 = "[field1]";
        let s3 = "[field1,field2]";
        let s4 = "[field1, field2]";
        let s5 = "[field1, field2, field3]";
        let s6 = "[  field1,  field2 ,field3 ]";

        assert_eq!(fields_selection(s2), Ok(("", vec!["field1"])));
        assert_eq!(fields_selection(s3), Ok(("", vec!["field1", "field2"])));
        assert_eq!(fields_selection(s4), Ok(("", vec!["field1", "field2"])));
        assert_eq!(fields_selection(s5), Ok(("", vec!["field1", "field2", "field3"])));
        assert_eq!(fields_selection(s6), Ok(("", vec!["field1", "field2", "field3"])));
    }

    #[test]
    fn parse_series_with_fields() {
        let s1 = "test_series";
        let s2 = "test_series[field1]";
        let s3 = "test_series[field1, field2]";
        let s4 = "test_series [field1, field2] AFTER now()-5m";

        let (rem, q) = selection(s1).unwrap();
        assert_eq!(rem, "");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, Vec::<String>::new());

        let (rem, q) = selection(s2).unwrap();
        assert_eq!(rem, "");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, vec!["field1"]);

        let (rem, q) = selection(s3).unwrap();
        assert_eq!(rem, "");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, vec!["field1", "field2"]);

        let (rem, q) = selection(s4).unwrap();
        assert_eq!(rem, " AFTER now()-5m");
        assert_eq!(q.series, "test_series");
        assert_eq!(q.fields, vec!["field1", "field2"]);
    }

    #[test]
    fn parse_basic_select() {
        let s1 = "select test_series";
        let s2 = "select test_series[field1]";
        let s3 = "select test_series[field1, field2]";

        let (rem, action) = parse_action(s1).unwrap();
        let query = if let Action::Select(query) = action { query } else { panic!() };
        assert_eq!(query.series, "test_series");
        assert_eq!(query.fields, Vec::<String>::new());

        let (rem, action) = parse_action(s2).unwrap();
        let query = if let Action::Select(query) = action { query } else { panic!() };
        assert_eq!(query.series, "test_series");
        assert_eq!(query.fields, vec!["field1"]);

        let (rem, action) = parse_action(s3).unwrap();
        let query = if let Action::Select(query) = action { query } else { panic!() };
        assert_eq!(query.series, "test_series");
        assert_eq!(query.fields, vec!["field1", "field2"]);
    }
}
