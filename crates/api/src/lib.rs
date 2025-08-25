use crossbeam_queue::ArrayQueue;
use ingest_core::event::NormalizedEvent;
use std::sync::Arc;

#[derive(Clone)]
pub struct EventBus {
    queue: Arc<ArrayQueue<NormalizedEvent>>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(capacity)),
        }
    }

    pub fn publisher(&self) -> EventPublisher {
        EventPublisher {
            queue: self.queue.clone(),
        }
    }

    pub fn subscribe(&self) -> EventConsumer {
        EventConsumer {
            queue: self.queue.clone(),
        }
    }
}

#[derive(Clone)]
pub struct EventPublisher {
    queue: Arc<ArrayQueue<NormalizedEvent>>,
}

impl EventPublisher {
    pub fn publish(&self, event: NormalizedEvent) {
        if self.queue.push(event.clone()).is_err() {
            // drop oldest
            let _ = self.queue.pop();
            let _ = self.queue.push(event);
        }
    }
}

pub struct EventConsumer {
    queue: Arc<ArrayQueue<NormalizedEvent>>,
}

impl EventConsumer {
    pub fn poll(&self) -> Option<NormalizedEvent> {
        self.queue.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn queue_roundtrip() {
        let bus = EventBus::new(1);
        let pubr = bus.publisher();
        let cons = bus.subscribe();
        pubr.publish(NormalizedEvent {
            venue: "x".into(),
            symbol: "y".into(),
            timestamp: Utc::now(),
            payload: serde_json::json!({}),
        });
        assert!(cons.poll().is_some());
    }
}
