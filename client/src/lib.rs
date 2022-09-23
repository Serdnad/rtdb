use std::io::{Error, Read, Write};
use std::net::TcpStream;

pub use rtdb::execution::QueryResult;
use rtdb::wire_protocol::query::*;

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

    pub fn query(&mut self, query: &str) -> QueryResult {
        let msg = build_query_command(query);
        self.stream.write_all(&msg).unwrap();
        self.stream.flush().unwrap();

        read_from_stream(&mut self.stream)
    }
}

// TODO: generalize this, and we can probably optimize it a fair bit too, but that'll involve
//  tweaking the way we serialize responses probably.
fn read_from_stream(stream: &mut TcpStream) -> QueryResult {
    let mut response = vec![];

    let mut buffer = vec![0; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                response.write_all(&buffer[..n]).expect("ruh roh");

                if n < 4096 {
                    break;
                }
            }
            Err(e) => {
                eprintln!("{}", e);
                break;
            }
        }
    }

    let mut cursor = ByteReader::new(&response);
    let query_result = parse_query_result(&mut cursor);
    query_result
}

#[cfg(test)]
mod tests {
    use crate::Client;

    #[test]
    fn compile() {}

    #[tokio::test]
    async fn creates_client() {
        let mut client = Client::new("127.0.0.1:2345").unwrap();
        client.query("SELECT test_series");
    }
}