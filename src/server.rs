use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::Router;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use csv::Writer;
use lazy_static::lazy_static;
use log::info;
use tokio::time;

use crate::execution::{ExecutionEngine, ExecutionResult};
use crate::lang::Action;
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
    let select = parse_select(&mut query);

    let mut engine = ENGINE.write().unwrap();
    let result = engine.execute(Action::Select(select));

    let elapsed = start.elapsed();
    // println!("{}us", elapsed.as_micros());

    (StatusCode::OK, serde_json::to_string(&result).unwrap())
}

// TODO: move
lazy_static! {
    static ref ENGINE: Arc<RwLock<ExecutionEngine<'static>>> = {
        Arc::new(RwLock::new(ExecutionEngine::new()))
    };
}