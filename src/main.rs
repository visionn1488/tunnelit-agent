mod config;
mod local_proxy;
mod protocol;
mod tunnel_client;

use clap::Parser;
use config::AgentConfig;
use tokio::time::{sleep, Duration};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;
use url::Url;

#[derive(Parser, Debug)]
#[command(author, version, about = "tunnelit agent — manage all your tunnels from the web dashboard", long_about = None)]
struct Args {
    /// Relay WebSocket URL (e.g. wss://ws.ezbchat.fun/ws or ws://127.0.0.1:9090/ws)
    #[arg(short, long)]
    relay: Option<String>,

    /// Secret agent token (optional, if not provided will generate a web claim link)
    #[arg(short, long)]
    token: Option<String>,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let args = Args::parse();
    let config = AgentConfig::load();

    let raw_token = args.token.clone().or_else(|| config.token.clone());
    let token = match raw_token {
        Some(ref t) if !t.trim().is_empty() && t != "YOUR_TOKEN_HERE" => Some(t.trim().to_string()),
        _ => None,
    };

    if let Some(ref tok) = token {
        let mut cfg = config.clone();
        cfg.token = Some(tok.clone());
        cfg.save();
    }

    let relay_str = args
        .relay
        .or(config.relay)
        .unwrap_or_else(|| "wss://ws.ezbchat.fun/ws".to_string());

    let relay_url = match Url::parse(&relay_str) {
        Ok(url) => url,
        Err(e) => {
            error!("Invalid relay URL '{}': {}", relay_str, e);
            std::process::exit(1);
        }
    };

    println!("========================================");
    println!("          tunnelit-agent                ");
    println!("========================================");
    println!("Relay: {}", relay_url);
    println!("Config file: {:?}", AgentConfig::config_path());
    if token.is_some() {
        println!("Auth: Token found");
    } else {
        println!("Auth: No token (web claim link will be requested)");
    }
    println!("========================================");

    let mut current_token = token;

    loop {
        let mut client = tunnel_client::TunnelClient::new(
            relay_url.clone(),
            current_token.clone(),
        );

        info!("Starting client connection...");
        if let Err(e) = client.run().await {
            error!("Connection error: {}", e);
        }

        // Check if client obtained a token during claim
        let updated_cfg = AgentConfig::load();
        if updated_cfg.token.is_some() {
            current_token = updated_cfg.token;
        }

        info!("Disconnected. Reconnecting in 3 seconds...");
        sleep(Duration::from_secs(3)).await;
    }
}
