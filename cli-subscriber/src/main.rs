use anyhow::Result;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use std::env;
use tradepulse_common::Alert;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let bootstrap = env::var("KAFKA_BOOTSTRAP").expect("KAFKA_BOOTSTRAP missing");

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &bootstrap);

    // force cloud security
    cfg.set(
        "security.protocol",
        &env::var("KAFKA_SECURITY_PROTOCOL").unwrap_or_else(|_| "SASL_SSL".into()),
    );
    cfg.set(
        "sasl.mechanism",
        &env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-256".into()),
    );
    cfg.set(
        "sasl.username",
        &env::var("KAFKA_SASL_USERNAME").expect("KAFKA_SASL_USERNAME missing"),
    );
    cfg.set(
        "sasl.password",
        &env::var("KAFKA_SASL_PASSWORD").expect("KAFKA_SASL_PASSWORD missing"),
    );

    // recommended for cloud
    cfg.set("enable.idempotence", "true"); // producer safety
    cfg.set("message.timeout.ms", "5000");
    cfg.set("socket.timeout.ms", "10000");
    cfg.set("session.timeout.ms", "45000");

    let group_id = env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "cli-subscriber.v1".to_string());
    let offset = env::var("KAFKA_OFFSET").unwrap_or_else(|_| "earliest".to_string());


    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", &group_id)
        .set(
            "auto.offset.reset",
            if offset == "earliest" { "earliest" } else { "latest" },
        )
        .create()?;

    consumer.subscribe(&["alerts.v1"])?;

    info!("cli-subscriber up (kafka: {bootstrap}) tailing alerts.v1 group={group_id} offset={offset}");
    println!("--- TradePulse Alerts (CTRL+C to stop) ---");

    loop {
        let msg = consumer.recv().await?;
        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s,
            _ => continue,
        };

        if let Ok(alert) = serde_json::from_str::<Alert>(payload) {
            // Clean, recruiter-readable line
            println!(
                "[{}] {} ALERT | pct_change={:.4}% | price={:.2} | rule={} | event_id={}",
                alert.ts, alert.symbol, alert.pct_change, alert.price, alert.rule, alert.event_id
            );
        } else {
            // If someone breaks schema, still show something
            println!("[raw] {payload}");
        }
    }
}
