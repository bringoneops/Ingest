use chrono::Utc;
use ingest_core::{event::NormalizedEvent, error::IngestError, canonical_symbol};

pub fn normalize(venue: &str, symbol: &str, raw: &str) -> Result<NormalizedEvent, IngestError> {
    let payload: serde_json::Value = serde_json::from_str(raw)?;
    Ok(NormalizedEvent {
        venue: venue.to_string(),
        symbol: canonical_symbol(symbol),
        timestamp: Utc::now(),
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_normalize() {
        let evt = normalize("binance", "btcusdt", "{\"p\":1}").unwrap();
        assert_eq!(evt.symbol, "BTCUSDT");
    }

    #[test]
    fn golden_replay() {
        let data = include_str!("../../../golden/binance_spot_trades.jsonl");
        for line in data.lines() {
            let evt = normalize("binance", "btcusdt", line).unwrap();
            assert_eq!(evt.venue, "binance");
        }
    }
}
