use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use std::{collections::HashMap, env, time::Duration};
use tradepulse_common::{Alert, Signal};
use tracing::{info, warn};
use uuid::Uuid;

const OUT_TOPIC: &str = "alerts.v1";
const DLQ_TOPIC: &str = "alerts.dlq.v1";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let bootstrap = env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string());
    let group_id = env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "alert.v1".to_string());

    let threshold_pct: f64 = env::var("ALERT_THRESHOLD_PCT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.20);

    let throttle_secs: i64 = env::var("ALERT_THROTTLE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&["signals.v1"])?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("message.timeout.ms", "5000")
        .create()?;

    info!("alert-service up (kafka: {bootstrap}) consuming signals.v1 -> producing {OUT_TOPIC} (dlq: {DLQ_TOPIC}) group={group_id} threshold={threshold_pct}% throttle={throttle_secs}s");

    // throttle per symbol
    let mut last_alert_at: HashMap<String, DateTime<Utc>> = HashMap::new();

    loop {
        let msg = consumer.recv().await?;

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s,
            _ => {
                warn!("empty/invalid payload, sending to DLQ");
                send_dlq(&producer, "signals.v1", msg.key(), "<empty>").await;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        };

        let signal: Signal = match serde_json::from_str(payload) {
            Ok(s) => s,
            Err(e) => {
                warn!("bad signal json, DLQ it: {e}");
                send_dlq(&producer, "signals.v1", msg.key(), payload).await;
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        };

        let pct = match signal.pct_change {
            Some(p) => p,
            None => {
                // first tick per symbol => no pct_change
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        };

        if pct.abs() < threshold_pct {
            consumer.commit_message(&msg, CommitMode::Async)?;
            continue;
        }

        // throttle
        let now = Utc::now();
        if let Some(last) = last_alert_at.get(&signal.symbol) {
            if now.signed_duration_since(*last) < ChronoDuration::seconds(throttle_secs) {
                consumer.commit_message(&msg, CommitMode::Async)?;
                continue;
            }
        }

        let alert = Alert {
            alert_id: Uuid::new_v4(),
            event_id: signal.event_id,
            symbol: signal.symbol.clone(),
            rule: format!("abs(pct_change) >= {threshold_pct}%"),
            pct_change: pct,
            price: signal.price,
            ts: signal.ts,
            source: "alert-service".to_string(),
        };

        let key = alert.symbol.clone();
        let out_payload = serde_json::to_string(&alert)?;

        match producer
            .send(
                FutureRecord::to(OUT_TOPIC).key(&key).payload(&out_payload),
                Duration::from_secs(2),
            )
            .await
        {
            Ok(delivery) => {
                last_alert_at.insert(alert.symbol.clone(), now);
                info!("ALERT produced key={key} delivery={delivery:?} pct_change={}", alert.pct_change);
                consumer.commit_message(&msg, CommitMode::Async)?;
            }
            Err((e, _)) => {
                warn!("alert produce failed, will retry (no commit): {e}");
            }
        }
    }
}

async fn send_dlq(producer: &FutureProducer, from_topic: &str, key: Option<&[u8]>, payload: &str) {
    // Keep DLQ payload dead simple and readable
    let wrapper = serde_json::json!({
        "from_topic": from_topic,
        "payload": payload,
        "ts": Utc::now().to_rfc3339(),
    })
    .to_string();

    let k = key.and_then(|b| std::str::from_utf8(b).ok()).unwrap_or("dlq");

    let _ = producer
        .send(
            FutureRecord::to(DLQ_TOPIC).key(k).payload(&wrapper),
            Duration::from_secs(2),
        )
        .await;
}
