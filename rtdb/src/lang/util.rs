use std::str::{from_utf8, from_utf8_unchecked};
use crate::util::new_timestamp;

#[inline]
pub fn advance_whitespace(s: &[u8], index: &mut usize) {
    let mut i = *index;
    while i < s.len() && s[i].is_ascii_whitespace() {
        i += 1;
    }

    *index = i;
}

/// Attempt to parse an ascii literal, and advance the index accordingly if successful.
#[inline]
pub fn parse_ascii(tag: &'static str, s: &[u8], index: &mut usize) -> bool {
    match s[*index..].starts_with(tag.as_bytes()) {
        true => {
            *index += tag.len();
            true
        }
        false => false,
    }
}

/// Attempts to parse an identifier. Identifiers must begin with an alphabetic character, and
/// afterwards may only container alphanumeric characters, '-', or '_'. On success, increases index
/// by the length of the parsed identifier.
///
/// This function assumes that the input has already been converted to lowercase.
#[inline]
pub fn parse_identifier<'a>(s: &'a [u8], index: &mut usize) -> (bool, &'a str) {
    let mut i = 0;

    let first_char = s[*index];
    if first_char < 0x61 || first_char > 0x7A { // test a-z
        return (false, "");
    }
    // i += 1;

    for &c in &s[*index..] {
        if !c.is_ascii_alphanumeric() && c != b'_' && c != b'-' {
            break;
        }
        i += 1;
    }

    *index += i;
    unsafe { (i > 0, from_utf8_unchecked(&s[*index - i..*index])) }
}

/// Attempts to parse a timestamp starting from index.
///
/// Currently accepts the following formats:
/// - nanoseconds from Unix epoch
/// - now()
/// - TODO: support additional formats
#[inline]
pub fn parse_timestamp(s: &[u8], index: &mut usize) -> Option<i64> {
    // now()
    if parse_ascii("now()", s, index) {
        return Some(new_timestamp());
    }

    // all others
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

#[cfg(test)]
mod tests {
    use crate::lang::util::{advance_whitespace, parse_ascii, parse_identifier, parse_timestamp};
    use crate::util::new_timestamp;

    #[test]
    fn advances_whitespace() {
        let mut index = 0;

        advance_whitespace(b"a test", &mut index);
        assert_eq!(index, 0);

        advance_whitespace(b"   test", &mut index);
        assert_eq!(index, 3);

        advance_whitespace(b"   \ttest", &mut index);
        assert_eq!(index, 4);

        advance_whitespace(b"    \ntest", &mut index);
        assert_eq!(index, 5);

        advance_whitespace(b"   test", &mut index);
        assert_eq!(index, 5);

        advance_whitespace(b"      ", &mut index);
        assert_eq!(index, 6);
    }

    #[test]
    fn parses_ascii() {
        let mut index = 0;

        assert_eq!(parse_ascii("test", b"a test", &mut index), false);
        assert_eq!(index, 0);

        assert_eq!(parse_ascii("a", b"a test", &mut index), true);
        assert_eq!(index, 1);

        assert_eq!(parse_ascii(" test ", b"a test", &mut index), false);
        assert_eq!(index, 1);
    }

    #[test]
    fn parses_timestamps() {
        let mut index = 0;
        assert_eq!(parse_timestamp(b"", &mut index), None);

        let mut index = 0;
        assert_eq!(parse_timestamp(b"1665877689000000", &mut index).unwrap(), 1665877689000000i64);

        let mut index = 0;
        assert!(parse_timestamp(b"now()", &mut index).unwrap() > new_timestamp() - 1_000_000);
    }

    #[test]
    fn parses_identifier() {
        let mut index = 0;

        let (parsed, ident) = parse_identifier(b"test_series,value1=1", &mut index);
        assert_eq!(parsed, true);
        assert_eq!(ident, "test_series");
        assert_eq!(index, 11);

        index += 1; // step past comma
        let (parsed, ident) = parse_identifier(b"test_series,value1=1", &mut index);
        assert_eq!(parsed, true);
        assert_eq!(ident, "value1");
        assert_eq!(index, 18);

        let (parsed, _ident) = parse_identifier(b"test_series,value1=1 12345", &mut index);
        assert_eq!(parsed, false);
        assert_eq!(index, 18);

        let mut index = 0;
        let (parsed, _ident) = parse_identifier(b"1identifiers_cannot_start_with_number", &mut index);
        assert_eq!(parsed, false);
        assert_eq!(index, 0);

        let mut index = 0;
        let (parsed, ident) = parse_identifier(b"name-with_ch4rs", &mut index);
        assert_eq!(parsed, true);
        assert_eq!(ident, "name-with_ch4rs");
        assert_eq!(index, 15);

        // TODO: try with non utf8
    }
}