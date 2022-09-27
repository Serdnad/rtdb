// TODO: reassess whether we even want to ship an HTTP restish server out of the box. The performance
//  on it isn't great, and it really shouldn't be used for anything other than the occasional ad hoc
//  query - for which the CLI client is still a better choice.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock};

use axum::Router;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use log::info;
use once_cell::sync::Lazy;
use tokio::time;

use crate::execution::{ExecutionEngine};
use crate::lang::Action;
use crate::lang::insert::parse_insert;
use crate::lang::query::parse_select;

pub struct HttpServer {}

impl HttpServer {
    pub async fn start() {
        let app = Router::new()
            .route("/insert", post(insert))
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

    // TOOD: move parsing into execution engine?
    let select = parse_select(&mut query);

    let engine = ENGINE.read().await;
    let result = engine.execute(Action::Select(select));

    let elapsed = start.elapsed();
    println!("{}us", elapsed.as_micros());

    (StatusCode::OK, serde_json::to_string(&result).unwrap())
}


async fn insert(Query(mut params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let start = time::Instant::now();
    let mut query = params.get_mut("query").unwrap();
    let insertion = parse_insert(&mut query);

    let engine = ENGINE.write().await;
    let result = engine.execute(Action::Insert(insertion));

    let elapsed = start.elapsed();
    println!("{}us", elapsed.as_micros());

    (StatusCode::OK, serde_json::to_string(&result).unwrap())
}

// TODO: move
pub static ENGINE: Lazy<Arc<RwLock<ExecutionEngine>>> = Lazy::new(|| {
    Arc::new(RwLock::new(ExecutionEngine::new()))
});
