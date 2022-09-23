use std::time;

use rustyline::error::ReadlineError::{Eof, Interrupted};

use rtdb_client::{Client, QueryResult};

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
                let query_result = client.query(&line);
                let elapsed = start.elapsed();

                println!("{}", to_table(&query_result));
                println!("{}us", elapsed.as_micros());
            }
            Err(Eof) => break,
            Err(Interrupted) => break,
            Err(err) => println!("Error: {}", err),
        }
    }

    rl.save_history("history.txt");
}

fn to_table(data: &QueryResult) -> String {
    let mut s = String::from("│ ");
    s.push_str(&data.records.fields.iter().map(|f| f.to_owned()).collect::<Vec<_>>().join(" | "));
    s.push_str(" │\n");

    for row in &data.records.rows[0..20] {
        for (i, &elem) in row.elements.iter().enumerate() {
            let val_s = match elem {
                None => String::from(""),
                Some(val) => val.to_string()
            };

            let len = &data.records.fields[i].len();
            s.push_str(&format!("{: >8}", val_s));
        }
        s.push_str("\n");
    }

    s
}