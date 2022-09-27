use std::time;

use rustyline::error::ReadlineError::{Eof, Interrupted};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

use rtdb_client::{Client, ExecutionResult};
use crate::table::to_table;

mod table;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    let mut client = Client::new("127.0.0.1:2345").unwrap();

    let mut rl = rustyline::Editor::<()>::new().unwrap();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line);

                let start = time::Instant::now();
                let result = client.execute(&line);
                let elapsed = start.elapsed();

                match result {
                    ExecutionResult::Query(data) => {
                        println!("{}", to_table(&data));
                    }
                    ExecutionResult::Insert(_) => {}
                }
                println!("{}us", elapsed.as_micros());
            }
            Err(Eof) => break,
            Err(Interrupted) => break,
            Err(err) => println!("Error: {}", err),
        }
    }

    rl.save_history("history.txt");
}