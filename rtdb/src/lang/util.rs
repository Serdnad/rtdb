use std::str::from_utf8;

#[inline]
pub fn advance_whitespace(s: &[u8], index: &mut usize) {
    let mut i = *index;
    while i < s.len() && s[i] == b' ' {
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

/// TODO: make this more sophisticated in what kind of timestamps it can accept
// TODO: move this somewhere else
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
    // TODO: tabs, newlines, and carriage returns
    fn advances_whitespace() {
        let mut index = 0;

        advance_whitespace(b"a test", &mut index);
        assert_eq!(index, 0);

        advance_whitespace(b"   test", &mut index);
        assert_eq!(index, 3);

        advance_whitespace(b"test", &mut index);
        assert_eq!(index, 3);

        advance_whitespace(b"     ", &mut index);
        assert_eq!(index, 5);
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