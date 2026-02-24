use anyhow::Result;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use std::{collections::HashMap, env, time::Duration};
use tradepulse_common::{Signal, Tick};
use tracing::{info, warn};

fn kafka_client_config() -> rdkafka::ClientConfig {
    use std::env;
    let mut cfg = rdkafka::ClientConfig::new();

    // required
    cfg.set(
        "bootstrap.servers",
        &env::var("KAFKA_BOOTSTRAP").expect("KAFKA_BOOTSTRAP missing"),
    );

    // cloud-required knobs (if set)
    if let Ok(v) = env::var("KAFKA_SECURITY_PROTOCOL") {
        cfg.set("security.protocol", &v); // e.g. SASL_SSL
    }
    if let Ok(v) = env::var("KAFKA_SASL_MECHANISM") {
        cfg.set("sasl.mechanisms", &v); // IMPORTANT: plural
    }
    if let Ok(v) = env::var("KAFKA_SASL_USERNAME") {
        cfg.set("sasl.username", &v);
    }
    if let Ok(v) = env::var("KAFKA_SASL_PASSWORD") {
        cfg.set("sasl.password", &v);
    }

    // sane defaults for cloud TLS
    // cfg.set("ssl.endpoint.identification.algorithm", "https");

    // optional: if you ever need a custom CA bundle
    if let Ok(v) = env::var("KAFKA_SSL_CA_LOCATION") {
        cfg.set("ssl.ca.location", &v);
    }

    // debugging (optional)
    // cfg.set("debug", "broker,security,protocol");

    cfg
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let bootstrap = env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string());
    let group_id = env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "aggregator.v1".to_string());

    let in_topic = "ticks.v1";
    let out_topic = "signals.v1";

    let consumer: StreamConsumer = kafka_client_config()
        .set("group.id", &group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("failed to create kafka consumer");


    consumer.subscribe(&[in_topic])?;

    let producer: FutureProducer = kafka_client_config()
        .set("message.timeout.ms", "5000")
        .create()
        .expect("failed to create kafka producer");

    info!("aggregator-service up (kafka: {bootstrap}) consuming {in_topic} -> producing {out_topic} group={group_id}");

    // in-memory state per symbol (good enough for MVP)
    let mut last_price: HashMap<String, f64> = HashMap::new();

    loop {
        let msg = consumer.recv().await?;

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s,
            _ => {
                warn!("skipping message with empty/invalid payload");
                // commit offset so we don't poison-loop on garbage
                consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async)?;
                continue;
            }
        };

        let tick: Tick = match serde_json::from_str(payload) {
            Ok(t) => t,
            Err(e) => {
                warn!("bad tick json, skipping: {e} payload={payload}");
                consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async)?;
                continue;
            }
        };

        let prev = last_price.get(&tick.symbol).copied();
        let pct = prev.map(|p| ((tick.price - p) / p) * 100.0);

        last_price.insert(tick.symbol.clone(), tick.price);

        let signal = Signal {
            event_id: tick.event_id,
            symbol: tick.symbol.clone(),
            price: tick.price,
            prev_price: prev,
            pct_change: pct,
            ts: tick.ts,
            source: "aggregator".to_string(),
        };
        let key = signal.symbol.clone();
        let out_payload = serde_json::to_string(&signal)?;

        // Produce FIRST, commit ONLY after produce succeeds => at-least-once
        match producer
            .send(
                FutureRecord::to(out_topic).key(&key).payload(&out_payload),
                Duration::from_secs(2),
            )
            .await
        {
            Ok(delivery) => {
                info!("signal produced key={key} delivery={delivery:?} pct_change={:?}", signal.pct_change);
                consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async)?;
            }
            Err((e, _)) => {
                // Do NOT commit. We'll retry by re-processing this message.
                warn!("produce failed, will retry (no commit): {e}");
            }
        }
    }
}
