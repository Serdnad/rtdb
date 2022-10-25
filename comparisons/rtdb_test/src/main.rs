mod influx;

use std::{thread, time};
use std::time::Duration;
use postgres::NoTls;

fn main() {
    let mut rtdb_client = rtdb_client::Client::new("127.0.0.1:2345").unwrap();
    let mut client = postgres::Client::connect("host=localhost user=postgres password=letmepass", NoTls).unwrap();

    // dirty concurrency performance test
    // for _ in 0..8 {
    //     thread::spawn(|| {
    //         let start = time::Instant::now();
    //
    //         let mut client = rtdb_client::Client::new("127.0.0.1:2345").unwrap();
    //         rtdb_test_reads(&mut client);
    //
    //         let elapsed = start.elapsed();
    //         println!("elapsed: {}ms", elapsed.as_millis());
    //     });
    // }
    //
    // thread::sleep(Duration::new(20, 0))

    // test_postgres_reads(&mut client);
    test_postgres_inserts(&mut client);


    // rtdb_test_reads(&mut rtdb_client);
    rtdb_test_inserts(&mut rtdb_client);

    // test_postgres()
}


fn time(name: &str, iterations: usize, func: &mut dyn FnMut()) {
    let start = time::Instant::now();

    for _ in 0..iterations {
        func();
    }

    let elapsed = start.elapsed();
    println!("[{}] {} iters: {}ms", name, iterations, elapsed.as_millis());
}

fn rtdb_test_reads(client: &mut rtdb_client::Client) {
    time("RTDB read", 1000, &mut || {
        let r = client.execute("SELECT test_series");
    });
}

fn rtdb_test_inserts(client: &mut rtdb_client::Client) {
    time("RTDB write", 20_001, &mut || {
        let _ = client.execute("INSERT test_series field1=123.0,field2=-321.0,field3=true");
    });
}

fn test_postgres_reads(client: &mut postgres::Client) {
    time("pg read", 1_000, &mut || {
        let _ = client.query("SELECT * FROM playground", &[]).unwrap();
    });
}

fn test_postgres_inserts(client: &mut postgres::Client) {
    time("pg write", 20_001, &mut || {
        let _ = client.query("INSERT INTO playground (field1, field2, field3) VALUES (123.0, -321.0, true)", &[]).unwrap();
    });
}