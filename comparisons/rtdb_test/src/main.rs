// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;
//
// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;


use std::time;

fn main() {
    let mut client = rtdb_client::Client::new("127.0.0.1:2345").unwrap();
    // let mut client = postgres::Client::connect("host=localhost user=postgres", NoTls).unwrap();


    // test_postgres_reads(&mut client);


    rtdb_test_reads(&mut client);
    // rtdb_test_inserts(&mut client);

    // test_postgres()
}


fn rtdb_test_reads(client: &mut rtdb_client::Client) {
    let start = time::Instant::now();

    let N = 5000;
    for _ in 0..N {
        let r = client.execute("SELECT test_series");
        // match r {
        //     ExecutionResult::Query(q) => { dbg!(q.count); },
        //     ExecutionResult::Insert(_) => {}
        // }
    }

    let elapsed = start.elapsed();
    println!("Read {} records: {}ms", N, elapsed.as_millis());
}

fn rtdb_test_inserts(client: &mut rtdb_client::Client) {
    let start = time::Instant::now();

    let N = 10001;
    for _ in 0..N {
        let _ = client.execute("INSERT test_series,field1=123.0,field2=-321.0");
    }

    let elapsed = start.elapsed();
    println!("Insert {} records: {}ms", N, elapsed.as_millis());
}

fn test_postgres_reads(client: &mut postgres::Client) {
    let start = time::Instant::now();

    let N = 10000;
    for _ in 0..N {
        let _ = client.query("SELECT * FROM playground", &[]).unwrap();
    }

    let elapsed = start.elapsed();
    println!("Insert {} records: {}ms", N, elapsed.as_millis());
}