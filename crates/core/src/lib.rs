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
    use super::canonical_symbol;

    #[test]
    fn symbol_uppercase() {
        assert_eq!(canonical_symbol("btcusdt"), "BTCUSDT");
    }
}
