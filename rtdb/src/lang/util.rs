use std::str::from_utf8;

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
/// afterwards may only container alphanumeric characters, '-', or '_'.
///
/// This function assumes that the input has already been converted to lowercase.
/// This function increases index by the length of the parsed identifier.
#[inline]
pub fn parse_identifier<'a>(s: &'a [u8], index: &'a mut usize) -> (bool, &'a [u8]) {
    let mut i = 0;

    let first_char = s[*index];
    if first_char < 0x61 || first_char > 0x7A { // test a-z
        return (false, b"");
    }
    i += 1;

    for &c in &s[*index..] {
        if !c.is_ascii_alphanumeric() && c != b'_' && c != b'-' {
            break;
        }
        i += 1;
    }

    *index += i;
    (i > 0, &s[*index - i..*index - 1])
}

/// Attempts to parse a timestamp starting from index.
///
/// Currently accepts the following formats:
/// - nanoseconds from Unix epoch
/// - TODO: support additional formats
#[inline]
pub fn parse_timestamp(s: &[u8], index: &mut usize) -> Option<i64> {
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
    use crate::lang::util::{advance_whitespace, parse_ascii};

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

        assert_eq!(parse_ascii("test", b"a test", &mut index), false);
        assert_eq!(index, 0);

        assert_eq!(parse_ascii("a", b"a test", &mut index), true);
        assert_eq!(index, 1);

        assert_eq!(parse_ascii(" test ", b"a test", &mut index), false);
        assert_eq!(index, 1);
    }
}