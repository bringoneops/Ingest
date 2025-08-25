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
    use reqwest::Client;
    use tokio_tungstenite::connect_async;

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
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| IngestError::Validation(format!("{}: {}", url, e)))?;
        let resp = resp.error_for_status().map_err(|e| {
            let status = e
                .status()
                .map(|s| s.as_u16().to_string())
                .unwrap_or_else(|| "unknown".into());
            let url = e
                .url()
                .map(|u| u.to_string())
                .unwrap_or_else(|| url.clone());
            IngestError::Validation(format!("request to {} failed with status {}", url, status))
        })?;
        let resp: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| IngestError::Validation(format!("{}: {}", url, e)))?;
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
                let status = sym.get("status").and_then(|v| v.as_str()).unwrap_or("");
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
                if let Err(e) = discover_symbols(&cfg).await.map(|s| symbols = s) {
                    tracing::warn!(
                        "symbol discovery failed for {}: {}. Provide a `symbols` list in config to disable discovery",
                        cfg.name,
                        e
                    );
                }
            }
            if symbols.is_empty() {
                return Ok(());
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
        use ingest_core::config::DiscoveryConfig;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

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

        async fn start_mock_server(status: u16) -> (String, tokio::task::JoinHandle<()>) {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let handle = tokio::spawn(async move {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let mut buf = [0u8; 1024];
                    let _ = socket.read(&mut buf).await;
                    let reason = match status {
                        404 => "Not Found",
                        451 => "Unavailable For Legal Reasons",
                        _ => "Error",
                    };
                    let response = format!(
                        "HTTP/1.1 {} {}\r\nContent-Length: 0\r\n\r\n",
                        status, reason
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            });
            (format!("http://{}", addr), handle)
        }

        #[tokio::test]
        async fn discover_symbols_reports_404() {
            let (base, handle) = start_mock_server(404).await;
            let mut cfg = base_cfg();
            cfg.rest_base = Some(base.clone());
            cfg.discovery = Some(DiscoveryConfig {
                enabled: true,
                quote_whitelist: vec![],
                symbol_blacklist: vec![],
            });
            let err = discover_symbols(&cfg).await.unwrap_err();
            match err {
                IngestError::Validation(msg) => {
                    assert!(msg.contains("404"));
                    assert!(msg.contains(&format!("{}/exchangeInfo", base.trim_end_matches('/'))));
                }
                _ => panic!("expected validation error"),
            }
            handle.abort();
        }

        #[tokio::test]
        async fn discover_symbols_reports_451() {
            let (base, handle) = start_mock_server(451).await;
            let mut cfg = base_cfg();
            cfg.rest_base = Some(base.clone());
            cfg.discovery = Some(DiscoveryConfig {
                enabled: true,
                quote_whitelist: vec![],
                symbol_blacklist: vec![],
            });
            let err = discover_symbols(&cfg).await.unwrap_err();
            match err {
                IngestError::Validation(msg) => {
                    assert!(msg.contains("451"));
                    assert!(msg.contains(&format!("{}/exchangeInfo", base.trim_end_matches('/'))));
                }
                _ => panic!("expected validation error"),
            }
            handle.abort();
        }
    }
}
