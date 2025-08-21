use async_trait::async_trait;
use futures_util::StreamExt;
use ingest_core::{
    canonical_symbol,
    config::VenueConfig,
    event::NormalizedEvent,
    error::IngestError,
};
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
    use chrono::{DateTime, Utc};
    use serde::Deserialize;
    use tokio_tungstenite::connect_async;

    #[derive(Debug, Deserialize)]
    pub struct BinanceSpec {
        pub name: String,
        pub endpoints: Vec<String>,
    }

    pub fn spec() -> BinanceSpec {
        toml::from_str(include_str!("../specs/binance_spot.toml")).expect("valid spec")
    }

    /// Adapter implementation for streaming data from Binance.
    pub struct BinanceAdapter;

    #[async_trait]
    impl Adapter for BinanceAdapter {
        async fn connect(
            &self,
            cfg: VenueConfig,
            tx: Sender<NormalizedEvent>,
        ) -> Result<(), IngestError> {
            let spec = spec();

            // Determine streams per symbol (trade channel only for now).
            // e.g. btcusdt@trade/ethusdt@trade
            let streams: Vec<String> = cfg
                .symbols
                .iter()
                .map(|s| format!("{}@trade", s.to_lowercase()))
                .collect();
            if streams.is_empty() {
                return Ok(());
            }

            // For simplicity we use the first endpoint. Multiple venues can
            // spawn additional adapter tasks with different cfg.name values.
            let base = spec
                .endpoints
                .get(0)
                .cloned()
                .unwrap_or_else(|| "wss://stream.binance.com:9443/stream".to_string());
            let url = if streams.len() == 1 {
                format!("{}/{}", base.trim_end_matches('/'), streams[0])
            } else {
                format!(
                    "{}/stream?streams={}",
                    base.trim_end_matches('/'),
                    streams.join("/")
                )
            };

            let (ws_stream, _) =
                connect_async(&url).await.map_err(|e| IngestError::Validation(e.to_string()))?;
            let (_, mut read) = ws_stream.split();

            while let Some(msg) = read.next().await {
                let msg = msg.map_err(|e| IngestError::Validation(e.to_string()))?;
                if !msg.is_text() {
                    continue;
                }
                let text = msg.into_text().map_err(|e| IngestError::Validation(e.to_string()))?;
                let value: serde_json::Value = serde_json::from_str(&text)?;

                // Combined stream messages include a `data` field; otherwise the
                // trade payload is at the top level.
                let (payload, symbol, ts) = if let Some(data) = value.get("data") {
                    let sym = data
                        .get("s")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let t_ms = data
                        .get("E")
                        .and_then(|v| v.as_i64())
                        .unwrap_or_else(|| Utc::now().timestamp_millis());
                    (data.clone(), sym, t_ms)
                } else {
                    let sym = value
                        .get("s")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let t_ms = value
                        .get("E")
                        .and_then(|v| v.as_i64())
                        .unwrap_or_else(|| Utc::now().timestamp_millis());
                    (value, sym, t_ms)
                };

                let ts = DateTime::<Utc>::from_timestamp_millis(ts)
                    .unwrap_or_else(|| Utc::now());

                let event = NormalizedEvent {
                    venue: cfg.name.clone(),
                    symbol: canonical_symbol(&symbol),
                    timestamp: ts,
                    payload,
                };
                // Ignore send failures (receiver closed).
                let _ = tx.send(event).await;
            }

            Ok(())
        }
    }
}
