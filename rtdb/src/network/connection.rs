use std::io::ErrorKind::UnexpectedEof;
use std::os::linux::raw::stat;
use std::time;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::execution::{ExecutionResult, QueryResult};
use crate::lang::Action;
use crate::lang::insert::parse_insert;
use crate::lang::query::parse_select;
use crate::network::read_string;
use crate::network::{ACTION_QUERY, ACTION_INSERT};
use crate::server::ENGINE;
use crate::storage::series::{DataRow, RecordCollection};
use crate::wire_protocol::query::build_query_result;

/// A database connection.
/// In addition to a TCP stream, this includes the state of the connection.
pub struct Connection {
    pub live: bool,
    pub authenticated: bool,
    pub stream: TcpStream,
}

// TODO: move, probably to lang? or execution?
// pub fn parse_statement<'a>(mut statement: &'a mut String) -> Action<'a> {
//     statement.make_ascii_lowercase();
//
//     if statement.starts_with("select") {
//         Action::Select(parse_select(&mut statement))
//     } else if statement.starts_with("insert") {
//         Action::Insert(parse_insert(&mut statement))
//     } else {
//         panic!("Could not parse!") // tODO: error handling
//     }
// }

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
            // let msg_length = self.stream.read_u8().await.unwrap();
            // let input_length = match data {
            //     Ok(action) => action,
            //     Err(err) => {
            //         if err.kind() == UnexpectedEof {
            //             break;
            //             // TODO: close this connection and clean up resources
            //         }
            //
            //         dbg!(err);
            //         break;
            //     }
            // };

            // match action_byte {
            //     ACTION_QUERY => {
            let mut msg = read_string(&mut self.stream).await;

            // let result = QueryResult{ count: 0, records: RecordCollection { fields: vec![String::from("test")], rows: vec![DataRow{ time: 0, elements: vec![] }] } };
            // let response = build_query_result(&result);
            // self.stream.write_all(&response).await;
            // self.stream.flush().await;
            // return;
            // dbg!(&msg);
            //
            // println!("RUN THE QUERY");


            let start = time::Instant::now();

            // let action = parse_statement(&mut msg);
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

            match result {
                ExecutionResult::Query(result) => {
                    let response = build_query_result(&result);

                    let elapsed = start.elapsed();
                    println!("{}us", elapsed.as_micros());

                    // dbg!(&response.len());
                    let len = response.len();
                    let mut buf = Vec::with_capacity(2 + len);
                    buf.write_all(&len.to_be_bytes()).await;
                    buf.write_all(&response).await;

                    dbg!(&buf.len());
                    self.stream.write_all(&buf).await;
                    self.stream.flush().await;
                }
                ExecutionResult::Insert(_) => {}
            }

            // dbg!(result);

            // let serialized_result = serde_json::to_string(&result).unwrap();
            // dbg!(serialized_result);
            //     }
            //     ACTION_INSERT => {
            //         let msg = read_string(&mut self.stream).await;
            //         dbg!(msg);
            //
            //         println!("RUN THE INSERT");
            //     }
            //     _ => { println!("UNSUPPORTED"); }
            // }
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