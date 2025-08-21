use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{BufRead, BufReader};
use pipeline;

#[derive(Parser)]
#[command(name = "devtools")]
#[command(about = "Developer tools for the ingestion engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scaffold a new adapter from spec
    Scaffold { spec: String },
    /// Replay a golden data pack
    Replay { file: String },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Scaffold { spec } => {
            let text = std::fs::read_to_string(spec)?;
            println!("// adapter spec\n{}", text);
        }
        Commands::Replay { file } => {
            let f = File::open(file)?;
            let reader = BufReader::new(f);
            for line in reader.lines() {
                let raw = line?;
                let evt = pipeline::normalize("binance", "TEST", &raw)?;
                println!("{}", serde_json::to_string(&evt)?);
            }
        }
    }
    Ok(())
}
