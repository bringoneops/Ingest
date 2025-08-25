pub mod event {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct NormalizedEvent {
        pub venue: String,
        pub symbol: String,
        pub timestamp: DateTime<Utc>,
        pub payload: serde_json::Value,
    }
}

pub mod config {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Config {
        pub venues: Vec<VenueConfig>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct VenueConfig {
        pub name: String,
        pub symbols: Vec<String>,
        #[serde(default)]
        pub discover: bool,
        #[serde(default)]
        pub ws_base: Option<String>,
        #[serde(default)]
        pub rest_base: Option<String>,
        #[serde(default)]
        pub http_timeout_secs: Option<u64>,
        #[serde(default)]
        pub channels: ChannelConfig,
        #[serde(default)]
        pub discovery: Option<DiscoveryConfig>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    pub struct DiscoveryConfig {
        #[serde(default)]
        pub enabled: bool,
        #[serde(default)]
        pub quote_whitelist: Vec<String>,
        #[serde(default)]
        pub symbol_blacklist: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct ChannelConfig {
        #[serde(default = "default_trades")]
        pub trades: bool,
        #[serde(default)]
        pub ticker: Option<TickerConfig>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TickerConfig {
        pub enabled: bool,
        #[serde(default)]
        pub mode: Option<String>,
    }

    const fn default_trades() -> bool {
        true
    }

    impl Default for ChannelConfig {
        fn default() -> Self {
            Self {
                trades: default_trades(),
                ticker: None,
            }
        }
    }

    impl Default for TickerConfig {
        fn default() -> Self {
            Self {
                enabled: false,
                mode: None,
            }
        }
    }

    impl Config {
        /// Parse configuration from TOML, supporting both the simple `[[venues]]`
        /// format and the more advanced `[venue.<name>]` style used by
        /// `config/binance.toml`.
        pub fn from_str(data: &str) -> Result<Self, toml::de::Error> {
            // First attempt to deserialize using the simple struct format.
            if let Ok(cfg) = toml::from_str::<Config>(data) {
                return Ok(cfg);
            }

            // Fallback to parsing `[venue.*]` tables manually.
            let value: toml::Value = toml::from_str(data)?;
            let global_discovery: DiscoveryConfig = value
                .get("discovery")
                .cloned()
                .map(|v| v.try_into().unwrap_or_default())
                .unwrap_or_default();
            if let Some(table) = value.get("venue").and_then(|v| v.as_table()) {
                let mut venues = Vec::new();
                for (name, cfg) in table {
                    if cfg
                        .get("enabled")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    {
                        let mut discover = false;
                        let symbols = match cfg.get("symbols") {
                            Some(toml::Value::Array(arr)) => arr
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect(),
                            Some(toml::Value::String(s)) if s == "ALL" => {
                                discover = true;
                                Vec::new()
                            }
                            _ => Vec::new(),
                        };
                        let ws_base = cfg
                            .get("ws_base")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let rest_base = cfg
                            .get("rest_base")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let http_timeout_secs = cfg
                            .get("http_timeout_secs")
                            .and_then(|v| v.as_integer())
                            .map(|v| v as u64);
                        let channels: ChannelConfig = cfg
                            .get("channels")
                            .cloned()
                            .map(|v| v.try_into().unwrap_or_default())
                            .unwrap_or_default();
                        let discovery: Option<DiscoveryConfig> = cfg
                            .get("discovery")
                            .cloned()
                            .map(|v| v.try_into().unwrap_or_default())
                            .or_else(|| {
                                if discover {
                                    Some(global_discovery.clone())
                                } else {
                                    None
                                }
                            });

                        venues.push(VenueConfig {
                            name: name.clone(),
                            symbols,
                            discover,
                            ws_base,
                            rest_base,
                            http_timeout_secs,
                            channels,
                            discovery,
                        });
                    }
                }
                return Ok(Config { venues });
            }

            Ok(Config { venues: Vec::new() })
        }
    }
}

pub mod error {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum IngestError {
        #[error("validation failed: {0}")]
        Validation(String),
        #[error("io error: {0}")]
        Io(#[from] std::io::Error),
        #[error("serde error: {0}")]
        Serde(#[from] serde_json::Error),
    }
}

/// Canonicalize a symbol to our uppercase format.
pub fn canonical_symbol(input: &str) -> String {
    input.to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::{canonical_symbol, config::Config};

    #[test]
    fn symbol_uppercase() {
        assert_eq!(canonical_symbol("btcusdt"), "BTCUSDT");
    }

    #[test]
    fn parse_simple_config() {
        let data = "[[venues]]\nname=\"binance\"\nsymbols=[\"BTCUSDT\"]\n";
        let cfg = Config::from_str(data).unwrap();
        assert_eq!(cfg.venues.len(), 1);
        assert_eq!(cfg.venues[0].name, "binance");
    }

    #[test]
    fn parse_binance_style_config() {
        let data = r#"
[venue.binance_spot]
enabled = true
ws_base = "wss://example"
rest_base = "https://rest.example"
symbols = ["BTCUSDT", "ETHUSDT"]

[venue.binance_spot.channels]
trades = true

[venue.binance_usdm]
enabled = false
"#;
        let cfg = Config::from_str(data).unwrap();
        assert_eq!(cfg.venues.len(), 1);
        assert_eq!(cfg.venues[0].name, "binance_spot");
        assert_eq!(cfg.venues[0].ws_base.as_deref(), Some("wss://example"));
        assert!(cfg.venues[0].channels.trades);
        assert!(!cfg.venues[0].discover);
    }

    #[test]
    fn parse_all_symbols_as_discovery() {
        let data = r#"
[venue.binance_spot]
enabled = true
symbols = "ALL"
"#;
        let cfg = Config::from_str(data).unwrap();
        assert_eq!(cfg.venues.len(), 1);
        assert!(cfg.venues[0].discover);
        assert!(cfg.venues[0].symbols.is_empty());
    }
}
