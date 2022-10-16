use std::io::{Error, Read, Write};
use std::net::TcpStream;

use byteorder::{BigEndian, ReadBytesExt};
pub use rtdb::DataValue;

pub use rtdb::execution::{ExecutionResult, InsertionResult, QueryResult};
pub use rtdb::execution::ClientQueryResult;
pub use rtdb::wire_protocol::DataType;
pub use rtdb::wire_protocol::{ClientExecutionResult, parse_result};

pub struct Client {
    stream: TcpStream,
}

/// A rtdb client, which can issue queries to and insert data into a database.
impl Client {
    /// Creates a new rtdb client for the database available at `endpoint`.
    pub fn new(endpoint: &str) -> Result<Client, Error> {
        let connect = std::net::TcpStream::connect(endpoint);
        match connect {
            Err(err) => Err(err),
            Ok(stream) => {
                Ok(Client { stream })
            }
        }
    }

    pub fn execute(&mut self, query: &str) -> ClientExecutionResult {
        let len = query.len() as u16;
        let mut buffer = Vec::with_capacity((2 + len) as usize);
        buffer.write_all(&len.to_be_bytes());
        buffer.write_all(query.as_bytes());

        self.stream.write_all(&buffer).unwrap();
        self.stream.flush().unwrap();

        read_from_stream(&mut self.stream)
    }
}

// TODO: generalize this, and we can probably optimize it a fair bit too, but that'll involve
//  tweaking the way we serialize responses probably.
fn read_from_stream(stream: &mut TcpStream) -> ClientExecutionResult {
    let buf_len = stream.read_u64::<BigEndian>().unwrap();
    let mut response = vec![0; buf_len as usize];
    stream.read_exact(&mut response).unwrap();

    parse_result(&mut response)
}


#[cfg(test)]
mod tests {
    use crate::Client;

    #[test]
    fn compile() {}

    #[tokio::test]
    async fn creates_client() {
        let mut client = Client::new("127.0.0.1:2345").unwrap();
        client.execute("SELECT test_series");
    }
}