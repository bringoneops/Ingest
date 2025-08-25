use std::{env, fs, net::SocketAddr};

use agents::{binance::BinanceAdapter, Adapter};
use api::EventBus;
use ingest_core::config::Config;
use ops::OpsServer;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg_path = env::args().nth(1).expect("config path required");
    let data = fs::read_to_string(cfg_path)?;
    let cfg = Config::from_str(&data)?;

    let bus = EventBus::new(1024);
    let publisher = bus.publisher();

    let ops = OpsServer::new(bus.clone());
    let ops_addr: SocketAddr = "127.0.0.1:3000".parse().unwrap();
    let ops_handle = tokio::spawn(ops.run(ops_addr));

    let (tx, mut rx) = mpsc::channel(100);
    let forward_handle = tokio::spawn(async move {
        while let Some(evt) = rx.recv().await {
            publisher.publish(evt);
        }
    });

    for venue in cfg.venues {
        let tx = tx.clone();
        tokio::spawn(async move {
            let adapter = BinanceAdapter;
            if let Err(e) = adapter.connect(venue, tx).await {
                eprintln!("adapter error: {e}");
            }
        });
    }

    let _ = tokio::join!(ops_handle, forward_handle);
    Ok(())
}
