use byteorder::ReadBytesExt;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use crate::execution::InsertionResult;
use crate::wire_protocol::query::ByteReader;

/// Returns a byte slice containing the result of an insertion.
/// TODO: on error, return a string describing the error
pub async fn build_insert_result<T>(result: &InsertionResult, out: &mut T)
    where
        T: AsyncWrite + Unpin + Send
{
// TODO: define enum or constants, instead of magic numbers
    out.write(&vec![2, result.success as u8]).await;
}

// TODO: move to client
pub fn parse_insert_result(buffer: &mut ByteReader) -> InsertionResult {
    let success = buffer.read_u8().unwrap() == 1;
    InsertionResult { success }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt, BufWriter};
    use crate::execution::InsertionResult;
    use crate::wire_protocol::insert::build_insert_result;

    #[tokio::test]
    async fn builds_insert_result() {
        let mut buf = vec![];
        build_insert_result(&InsertionResult { success: false }, &mut buf).await;
        assert_eq!(buf, vec![2, 0]);
    }
}