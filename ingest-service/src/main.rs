use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::{env, net::SocketAddr, sync::Arc, time::Duration};
use tradepulse_common::Tick;
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    producer: Arc<FutureProducer>,
    topic: &'static str,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let bootstrap = env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string());
    let bind = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8085".to_string());

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("failed to create kafka producer");

    let state = AppState {
        producer: Arc::new(producer),
        topic: "ticks.v1",
    };

    let app = Router::new()
        .route("/health", post(|| async { StatusCode::OK }))
        .route("/ticks", post(post_tick))
        .with_state(state);

    let addr: SocketAddr = bind.parse().expect("BIND_ADDR invalid, expected ip:port");
    info!("ingest-service listening on http://{addr} (kafka: {bootstrap})");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn post_tick(
    State(state): State<AppState>,
    Json(tick): Json<Tick>,
) -> Result<StatusCode, (StatusCode, String)> {
    let payload = serde_json::to_string(&tick)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid tick json: {e}")))?;

    // Key by symbol to preserve ordering per symbol partition
    let key = tick.symbol.clone();

    let record = FutureRecord::to(state.topic)
        .payload(&payload)
        .key(&key);

    match state
        .producer
        .send(record, Duration::from_secs(2))
        .await
    {
        Ok(delivery) => {
            info!("produced tick key={key} delivery={delivery:?}");
            Ok(StatusCode::ACCEPTED)
        }
        Err((e, _msg)) => {
            warn!("kafka produce failed: {e}");
            Err((StatusCode::BAD_GATEWAY, format!("kafka produce failed: {e}")))
        }
    }
}
