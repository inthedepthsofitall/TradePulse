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

// later: Signal, Alert, etc.
