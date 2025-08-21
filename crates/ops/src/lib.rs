use axum::{routing::get, Router};
use prometheus::{Encoder, TextEncoder, Registry, IntCounter};
use std::net::SocketAddr;

pub struct OpsServer {
    pub registry: Registry,
    pub requests: IntCounter,
}

impl OpsServer {
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests = IntCounter::new("requests_total", "total requests").unwrap();
        registry.register(Box::new(requests.clone())).unwrap();
        Self { registry, requests }
    }

    pub async fn run(self, addr: SocketAddr) {
        let registry = self.registry.clone();
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/ready", get(|| async { "ready" }))
            .route("/metrics", get(move || metrics(registry.clone())));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_check() {
        let server = OpsServer::new();
        tokio::spawn(server.run("127.0.0.1:3001".parse().unwrap()));
        // give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let body = reqwest::get("http://127.0.0.1:3001/health").await.unwrap().text().await.unwrap();
        assert_eq!(body, "ok");
    }
}
