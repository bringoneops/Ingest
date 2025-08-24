use async_trait::async_trait;
use futures_util::StreamExt;
use ingest_core::{
    canonical_symbol, config::VenueConfig, error::IngestError, event::NormalizedEvent,
};
use tokio::sync::mpsc::Sender;

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn connect(
        &self,
        cfg: VenueConfig,
        tx: Sender<NormalizedEvent>,
    ) -> Result<(), IngestError>;
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
    use tokio_tungstenite::connect_async;
    use reqwest::Client;

    /// Adapter implementation for streaming data from Binance.
    pub struct BinanceAdapter;

    async fn discover_symbols(cfg: &VenueConfig) -> Result<Vec<String>, IngestError> {
        let disc = cfg.discovery.clone().unwrap_or_default();
        if !disc.enabled {
            return Ok(Vec::new());
        }
        let base = cfg
            .rest_base
            .clone()
            .ok_or_else(|| IngestError::Validation("rest_base required".into()))?;
        let url = format!("{}/exchangeInfo", base.trim_end_matches('/'));
        let client = Client::new();
        let resp: serde_json::Value = client
            .get(&url)
            .send()
            .await
            .map_err(|e| IngestError::Validation(e.to_string()))?
            .json()
            .await
            .map_err(|e| IngestError::Validation(e.to_string()))?;
        let mut symbols = Vec::new();
        let include_re = if disc.quote_whitelist.is_empty() {
            None
        } else {
            Some(disc.quote_whitelist)
        };
        let blacklist = disc.symbol_blacklist;

        if let Some(arr) = resp.get("symbols").and_then(|v| v.as_array()) {
            for sym in arr {
                let symbol = sym
                    .get("symbol")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let quote = sym
                    .get("quoteAsset")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let status = sym
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if status != "TRADING" {
                    continue;
                }
                if let Some(list) = &include_re {
                    if !list.iter().any(|q| q == quote) {
                        continue;
                    }
                }
                if blacklist.iter().any(|s| s == &symbol) {
                    continue;
                }
                symbols.push(symbol);
            }
        }
        Ok(symbols)
    }

    fn build_streams(cfg: &VenueConfig, symbols: &[String]) -> Vec<String> {
        let mut streams = Vec::new();
        if cfg.channels.trades {
            streams.extend(
                symbols
                    .iter()
                    .map(|s| format!("{}@trade", s.to_lowercase())),
            );
        }
        if let Some(ticker) = &cfg.channels.ticker {
            if ticker.enabled {
                match ticker.mode.as_deref() {
                    Some("!ticker@arr") => streams.push("!ticker@arr".to_string()),
                    _ => streams.extend(
                        symbols
                            .iter()
                            .map(|s| format!("{}@ticker", s.to_lowercase())),
                    ),
                }
            }
        }
        streams
    }

    #[async_trait]
    impl Adapter for BinanceAdapter {
        async fn connect(
            &self,
            cfg: VenueConfig,
            tx: Sender<NormalizedEvent>,
        ) -> Result<(), IngestError> {
            let mut symbols = cfg.symbols.clone();
            if symbols.is_empty() {
                symbols = discover_symbols(&cfg).await?;
            }
            let streams = build_streams(&cfg, &symbols);
            if streams.is_empty() {
                return Ok(());
            }

            // Use ws_base from config or default to public endpoint.
            let base = cfg
                .ws_base
                .clone()
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

            let (ws_stream, _) = connect_async(&url)
                .await
                .map_err(|e| IngestError::Validation(e.to_string()))?;
            let (_, mut read) = ws_stream.split();

            while let Some(msg) = read.next().await {
                let msg = msg.map_err(|e| IngestError::Validation(e.to_string()))?;
                if !msg.is_text() {
                    continue;
                }
                let text = msg
                    .into_text()
                    .map_err(|e| IngestError::Validation(e.to_string()))?;
                let value: serde_json::Value = serde_json::from_str(&text)?;

                // Combined stream messages include a `data` field. For aggregated
                // streams `data` may be an array.
                if let Some(data) = value.get("data") {
                    if let Some(arr) = data.as_array() {
                        for item in arr {
                            process_payload(item.clone(), &cfg, &tx).await?;
                        }
                    } else {
                        process_payload(data.clone(), &cfg, &tx).await?;
                    }
                } else {
                    process_payload(value, &cfg, &tx).await?;
                }
            }

            Ok(())
        }
    }

    async fn process_payload(
        payload: serde_json::Value,
        cfg: &VenueConfig,
        tx: &Sender<NormalizedEvent>,
        ) -> Result<(), IngestError> {
        let symbol = payload
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let t_ms = payload
            .get("E")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        let ts = DateTime::<Utc>::from_timestamp_millis(t_ms).unwrap_or_else(|| Utc::now());
        let event = NormalizedEvent {
            venue: cfg.name.clone(),
            symbol: canonical_symbol(&symbol),
            timestamp: ts,
            payload,
        };
        let _ = tx.send(event).await;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn base_cfg() -> VenueConfig {
            VenueConfig {
                name: "binance".into(),
                symbols: vec!["BTCUSDT".into()],
                discover: false,
                ws_base: None,
                rest_base: None,
                channels: ingest_core::config::ChannelConfig {
                    trades: true,
                    ticker: Some(ingest_core::config::TickerConfig {
                        enabled: true,
                        mode: None,
                    }),
                },
                discovery: None,
            }
        }

        #[test]
        fn build_trade_and_ticker_streams() {
            let cfg = base_cfg();
            let streams = build_streams(&cfg, &cfg.symbols);
            assert!(streams.contains(&"btcusdt@trade".to_string()));
            assert!(streams.contains(&"btcusdt@ticker".to_string()));
        }

        #[test]
        fn build_aggregate_ticker_stream() {
            let mut cfg = base_cfg();
            cfg.channels.ticker = Some(ingest_core::config::TickerConfig {
                enabled: true,
                mode: Some("!ticker@arr".into()),
            });
            let streams = build_streams(&cfg, &cfg.symbols);
            assert!(streams.contains(&"!ticker@arr".to_string()));
        }
    }
}
