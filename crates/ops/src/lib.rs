use api::EventBus;
use async_stream::stream;
use axum::{
    extract::State,
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use futures_core::stream::Stream;
use prometheus::{Encoder, IntCounter, Registry, TextEncoder};
use std::{convert::Infallible, net::SocketAddr, time::Duration};

pub struct OpsServer {
    pub registry: Registry,
    pub requests: IntCounter,
    bus: EventBus,
}

impl OpsServer {
    pub fn new(bus: EventBus) -> Self {
        let registry = Registry::new();
        let requests = IntCounter::new("requests_total", "total requests").unwrap();
        registry.register(Box::new(requests.clone())).unwrap();
        Self {
            registry,
            requests,
            bus,
        }
    }

    pub async fn run(self, addr: SocketAddr) {
        let registry = self.registry.clone();
        let bus = self.bus.clone();
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/ready", get(|| async { "ready" }))
            .route("/metrics", get(move || metrics(registry.clone())))
            .route("/events", get(events))
            .with_state(bus);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}

async fn metrics(registry: Registry) -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let mf = registry.gather();
    encoder.encode(&mf, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn events(State(bus): State<EventBus>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let consumer = bus.subscribe();
    let stream = stream! {
        loop {
            if let Some(evt) = consumer.poll() {
                let data = serde_json::to_string(&evt).unwrap();
                yield Ok(Event::default().data(data));
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    Sse::new(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::EventBus;

    #[tokio::test]
    async fn health_check() {
        let bus = EventBus::new(1);
        let server = OpsServer::new(bus);
        tokio::spawn(server.run("127.0.0.1:3001".parse().unwrap()));
        // give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let body = reqwest::get("http://127.0.0.1:3001/health")
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "ok");
    }
}
