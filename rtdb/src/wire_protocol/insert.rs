use byteorder::ReadBytesExt;
use crate::execution::InsertionResult;
use crate::wire_protocol::query::ByteReader;

/// Returns a byte slice containing the result of an insertion.
pub fn build_insert_result(result: &InsertionResult) -> Vec<u8> {
    // add 3 bytes for action and query length
    // let mut buffer = Vec::with_capacity(2);
    //
    // buffer.push(2);
    // buffer.push(result.success as u8);
    // buffer

    // TODO: define enum or constants
    vec![2, result.success as u8]
}

pub fn parse_insert_result(buffer: &mut ByteReader) -> InsertionResult {
    let success = buffer.read_u8().unwrap() == 1;
    InsertionResult { success }
}

#[cfg(test)]
mod tests {
    use crate::execution::InsertionResult;
    use crate::wire_protocol::insert::build_insert_result;

    #[test]
    fn builds_insert_result() {
        assert_eq!(build_insert_result(&InsertionResult { success: false }), vec![2, 0]);
        assert_eq!(build_insert_result(&InsertionResult { success: true }), vec![2, 1]);
    }
}