use std::str::from_utf8;

use rustyline::error::ReadlineError::Eof;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use rtdb::wire_protocol;

#[tokio::main]
async fn main() {
    let mut connection = tokio::net::TcpStream::connect("127.0.0.1:2345").await.unwrap();

    let mut rl = rustyline::Editor::<()>::new().unwrap();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }


    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                let cmd = wire_protocol::build_query_command(&line);

                connection.write_all(&cmd).await;
                connection.flush().await;

                let mut s = vec![];
                connection.read(&mut s);

                dbg!(from_utf8(&s).unwrap());
            }
            Err(Eof) => {
                break;
            }
            Err(err) => {
                dbg!(err);
                println!("No input");
            }
        }
    }

    rl.save_history("history.txt");
}
