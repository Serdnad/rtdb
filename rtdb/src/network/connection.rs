use std::time;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::lang::Action;
use crate::lang::insert::parse_insert;
use crate::lang::query::parse_select;
use crate::network::read_string;
use crate::network::server::ENGINE;
use crate::wire_protocol::build_response;

/// A database connection.
///
/// In addition to a TCP stream, this includes the state of the connection.
pub struct Connection {
    pub live: bool,
    pub authenticated: bool,
    pub stream: TcpStream,
}

/// A managed TCP stream connected to a cli.
impl Connection {
    pub fn from(stream: TcpStream) -> Connection {
        Connection {
            live: false,
            authenticated: false,
            stream,
        }
    }

    /// Start the main handle loop for a connection.
    ///
    /// This involves:
    /// 1. listening for and parsing a command.
    /// 2. executing a corresponding action.
    /// 3. serializing and writing back the results.
    pub async fn start_handle_loop(&mut self) {
        loop {
            let mut msg = match read_string(&mut self.stream).await {
                None => break,
                Some(msg) => msg
            };

            let start = time::Instant::now();


            msg.make_ascii_lowercase();
            let action =
                if msg.starts_with("select") {
                    Action::Select(parse_select(&mut msg))
                } else if msg.starts_with("insert") {
                    Action::Insert(parse_insert(&mut msg))
                } else {
                    panic!("Could not parse!") // tODO: error handling
                };

            let engine = ENGINE.read().await;
            let result = engine.execute(action);

            let elapsed1 = start.elapsed();


            // --
            // --
            // --
            // -- HEY! So, the issue that we left off at, is that we don't actually
            //      know the length of the response, which we were previously sending as the very
            //      first part of the response...
            // --
            // --



            let mut buf = BufWriter::new(&mut self.stream);
            build_response(&result, &mut buf).await;
            buf.flush().await;

            let elapsed = start.elapsed();
            println!("exec: {}us", elapsed1.as_micros());

            // let len = response.len();
            self.stream.write(&len.to_be_bytes()).await;
            self.stream.write_all(&response).await;
            self.stream.flush().await;

            println!("post_serialization: {}us", elapsed.as_micros());
        }
    }
}

/// ConnectionPool manages a fixed size pool of live TCP connections from clients.
pub struct ConnectionPool {
    active_connections: u16,
}

impl ConnectionPool {
    pub fn new() -> ConnectionPool {
        ConnectionPool {
            active_connections: 0,
        }
    }

    pub fn add(&mut self, stream: TcpStream) {
        self.active_connections += 1;

        tokio::spawn(async move {
            let mut connection = Connection::from(stream);
            connection.start_handle_loop().await;
        });
    }
}