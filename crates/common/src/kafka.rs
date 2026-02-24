use rdkafka::ClientConfig;
use std::env;

pub fn kafka_config() -> ClientConfig {
    let mut cfg = ClientConfig::new();
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &bootstrap)
    .set("message.timeout.ms", "5000")
    .set("api.version.request", "true");

    if security_protocol != "PLAINTEXT" {
        cfg.set("security.protocol", &security_protocol)
        .set("ssl.endpoint.identification.algorithm", "https");

        // only set SASL bits if provided
        if !sasl_mechanism.is_empty() {
            cfg.set("sasl.mechanism", &sasl_mechanism);
        }
        if !sasl_username.is_empty() {
            cfg.set("sasl.username", &sasl_username);
        }
        if !sasl_password.is_empty() {
            cfg.set("sasl.password", &sasl_password);
        }
    }
    let bootstrap = env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "localhost:9092".to_string());
    cfg.set("bootstrap.servers", &bootstrap);

    // sane defaults
    cfg.set("message.timeout.ms", "5000");
    cfg.set("enable.idempotence", "true");

    // Optional SASL/TLS (cloud)
    if let Ok(proto) = env::var("KAFKA_SECURITY_PROTOCOL") {
        // ex: SASL_SSL
        cfg.set("security.protocol", &proto);
    }
    if let Ok(mech) = env::var("KAFKA_SASL_MECHANISM") {
        // ex: SCRAM-SHA-256
        cfg.set("sasl.mechanism", &mech);
    }
    if let Ok(user) = env::var("KAFKA_SASL_USERNAME") {
        cfg.set("sasl.username", &user);
    }
    if let Ok(pass) = env::var("KAFKA_SASL_PASSWORD") {
        cfg.set("sasl.password", &pass);
    }

    cfg
}
