use std::io::ErrorKind::UnexpectedEof;
use std::time;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::execution::ExecutionResult;
use crate::lang::Action;
use crate::lang::query::parse_select;
use crate::network::read_string;
use crate::network::{ACTION_QUERY, ACTION_INSERT};
use crate::server::ENGINE;
use crate::wire_protocol::query::build_query_result;

/// A database connection.
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
            let data = self.stream.read_u8().await;
            let action_byte = match data {
                Ok(action) => action,
                Err(err) => {
                    if err.kind() == UnexpectedEof {
                        break;
                        // TODO: close this connection and clean up resources
                    }

                    dbg!(err);
                    break;
                }
            };

            match action_byte {
                ACTION_QUERY => {
                    let mut msg = read_string(&mut self.stream).await;
                    // dbg!(&msg);
                    //
                    // println!("RUN THE QUERY");


                    let start = time::Instant::now();
                    let select = parse_select(&mut msg);

                    let engine = ENGINE.read().await;
                    let result = engine.execute(Action::Select(select));

                    let elapsed = start.elapsed();
                    println!("{}us", elapsed.as_micros());

                    match result {
                        ExecutionResult::Query(result) => {
                            let response = build_query_result(&result);
                            dbg!(&response.len());
                            self.stream.write_all(&response).await;
                            self.stream.flush().await;
                        }
                        ExecutionResult::Insert(_) => {}
                    }

                    // dbg!(result);

                    // let serialized_result = serde_json::to_string(&result).unwrap();
                    // dbg!(serialized_result);


                }
                ACTION_INSERT => {
                    let msg = read_string(&mut self.stream).await;
                    dbg!(msg);

                    println!("RUN THE INSERT");
                }
                _ => { println!("UNSUPPORTED"); }
            }
        }
    }
}

/// ConnectionPool manages a fixed size pool of live TCP connections from clients.
pub struct ConnectionPool {
    connections: Vec<Connection>,
}

impl ConnectionPool {
    pub fn new() -> ConnectionPool {
        ConnectionPool {
            connections: vec![]
        }
    }

    pub fn add(&mut self, stream: TcpStream) {
        let mut connection = Connection::from(stream);
        // self.connections.push(connection);

        // TODO: investigate tokio::spawn vs tokio::task::spawn
        tokio::spawn(async move {
            connection.start_handle_loop().await;
        });
    }
}