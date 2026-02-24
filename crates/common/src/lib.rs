use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub event_id: Uuid,
    pub symbol: String,
    pub price: f64,
    pub volume: i64,
    pub ts: DateTime<Utc>,
    pub source: String,
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub event_id: Uuid,
    pub symbol: String,
    pub price: f64,
    pub prev_price: Option<f64>,
    pub pct_change: Option<f64>,
    pub ts: DateTime<Utc>,
    pub source: String, // "aggregator"
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub alert_id: Uuid,
    pub event_id: Uuid,
    pub symbol: String,
    pub rule: String,
    pub pct_change: f64,
    pub price: f64,
    pub ts: DateTime<Utc>,
    pub source: String, // "alert-service"
}


