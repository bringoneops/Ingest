use async_trait::async_trait;
use ingest_core::{config::VenueConfig, event::NormalizedEvent, error::IngestError};
use tokio::sync::mpsc::Sender;

pub mod spec {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct AdapterSpec {
        pub name: String,
        pub endpoints: Vec<String>,
    }
}

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn connect(&self, cfg: VenueConfig, tx: Sender<NormalizedEvent>) -> Result<(), IngestError>;
}

/// A helper macro that implements Adapter for empty structs for prototyping.
#[macro_export]
macro_rules! simple_adapter {
    ($t:ty, $name:expr) => {
        #[async_trait::async_trait]
        impl Adapter for $t {
            async fn connect(
                &self,
                _cfg: ingest_core::config::VenueConfig,
                tx: tokio::sync::mpsc::Sender<ingest_core::event::NormalizedEvent>,
            ) -> Result<(), ingest_core::error::IngestError> {
                let evt = ingest_core::event::NormalizedEvent {
                    venue: $name.to_string(),
                    symbol: "DUMMY".into(),
                    timestamp: chrono::Utc::now(),
                    payload: serde_json::json!({"hello": "world"}),
                };
                tx.send(evt).await.map_err(|e| ingest_core::error::IngestError::Validation(e.to_string()))
            }
        }
    };
}

pub mod binance {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct BinanceSpec {
        pub name: String,
        pub endpoints: Vec<String>,
    }

    pub fn spec() -> BinanceSpec {
        toml::from_str(include_str!("../specs/binance_spot.toml")).expect("valid spec")
    }

    pub struct BinanceAdapter;
    simple_adapter!(BinanceAdapter, "binance");
}
