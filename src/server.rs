use std::collections::HashMap;
use std::net::SocketAddr;
use axum::http::StatusCode;
use axum::{Json, Router};
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::routing::get;
use log::info;
use tokio::time;
use crate::execution::ExecutionEngine;
use crate::lang::{Action, parse};
use crate::lang::query::parse_select;

pub struct HttpServer {}

impl HttpServer {
    pub async fn start() {
        let app = Router::new()
            // .route("/insert", get(query))
            .route("/query", get(query))
            .route("/", get(root));

        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        info!("Running HTTP Server on 127.0.0.1:3000");
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

// TODO: version

async fn root() -> &'static str {
    "Hello, World!"
}

async fn query(Query(mut params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let start = time::Instant::now();
    let mut query = params.get_mut("query").unwrap();
    // println!("{}", &query);
    // let res = parse(&mut query).unwrap();
    let q = parse_select(&mut query);


    let result = ExecutionEngine::execute(Action::Select(q));

    let elapsed = start.elapsed();
    println!("{}us", elapsed.as_micros());


    (StatusCode::OK, Json(result))
}