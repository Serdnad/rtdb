// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;
//
// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;


use std::thread::Thread;
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

    test_postgres_reads(&mut client);
    // test_postgres_inserts(&mut client);


    rtdb_test_reads(&mut rtdb_client);
    // rtdb_test_inserts(&mut rtdb_client);

    // test_postgres()
}


fn rtdb_test_reads(client: &mut rtdb_client::Client) {
    let start = time::Instant::now();

    let N = 1000;
    for _ in 0..N {
        let r = client.execute("SELECT test_series");
    }

    let elapsed = start.elapsed();
    println!("Read {} records: {}ms", N, elapsed.as_millis());
}

fn rtdb_test_inserts(client: &mut rtdb_client::Client) {
    let start = time::Instant::now();

    let N = 15001;
    for _ in 0..N {
        let _ = client.execute("INSERT test_series field1=123.0,field2=-321.0");
    }

    let elapsed = start.elapsed();
    println!("Insert {} records: {}ms", N, elapsed.as_millis());
}

fn test_postgres_reads(client: &mut postgres::Client) {
    let start = time::Instant::now();

    let N = 1000;
    for _ in 0..N {
        let _ = client.query("SELECT * FROM playground", &[]).unwrap();
    }

    let elapsed = start.elapsed();
    println!("Insert {} records: {}ms", N, elapsed.as_millis());
}


fn test_postgres_inserts(client: &mut postgres::Client) {
    let start = time::Instant::now();

    let N = 1000001;
    for _ in 0..N {
        let _ = client.query("INSERT INTO playground (field1, field2) VALUES (123.0, -321.0)", &[]).unwrap();
    }

    let elapsed = start.elapsed();
    println!("Insert {} records: {}ms", N, elapsed.as_millis());
}