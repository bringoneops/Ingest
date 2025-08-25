use ingest_core::event::NormalizedEvent;
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};

pub struct EventBus {
    tx: broadcast::Sender<NormalizedEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn publisher(&self) -> EventPublisher {
        EventPublisher { tx: self.tx.clone() }
    }

    pub fn subscribe(&self) -> EventConsumer {
        EventConsumer { rx: self.tx.subscribe() }
    }

    /// Subscribe to events as an asynchronous stream.
    pub fn subscribe_stream(&self) -> impl Stream<Item = NormalizedEvent> {
        BroadcastStream::new(self.tx.subscribe()).filter_map(|res| res.ok())
    }
}

#[derive(Clone)]
pub struct EventPublisher {
    tx: broadcast::Sender<NormalizedEvent>,
}

impl EventPublisher {
    pub fn publish(&self, event: NormalizedEvent) {
        let _ = self.tx.send(event);
    }
}

pub struct EventConsumer {
    rx: broadcast::Receiver<NormalizedEvent>,
}

impl EventConsumer {
    pub async fn recv(&mut self) -> Option<NormalizedEvent> {
        self.rx.recv().await.ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn queue_roundtrip() {
        let bus = EventBus::new(1);
        let pubr = bus.publisher();
        let mut stream = bus.subscribe_stream();
        pubr.publish(NormalizedEvent {
            venue: "x".into(),
            symbol: "y".into(),
            timestamp: Utc::now(),
            payload: serde_json::json!({}),
        });
        assert!(stream.next().await.is_some());
    }
}
