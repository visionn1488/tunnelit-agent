mod local_proxy;
mod protocol;
mod tunnel_client;

use clap::Parser;
use tokio::time::{sleep, Duration};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;
use url::Url;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    relay: String,

    #[arg(short, long)]
    local: u16,

    #[arg(short, long)]
    proto: String,

    #[arg(long)]
    remote_port: Option<u16>,

    #[arg(long)]
    subdomain: Option<String>,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let args = Args::parse();
    
    let relay_url = match Url::parse(&args.relay) {
        Ok(url) => url,
        Err(e) => {
            error!("Invalid relay URL: {}", e);
            std::process::exit(1);
        }
    };

    println!("========================================");
    println!("          tunnelit-agent                ");
    println!("========================================");
    println!("Relay: {}", relay_url);
    println!("Local Port: {}", args.local);
    println!("Protocol: {}", args.proto);
    if let Some(rp) = args.remote_port {
        println!("Requested Remote Port: {}", rp);
    }
    if let Some(sd) = &args.subdomain {
        println!("Requested Subdomain: {}", sd);
    }
    println!("========================================");

    loop {
        let client = tunnel_client::TunnelClient::new(
            relay_url.clone(),
            args.local,
            args.proto.clone(),
            args.remote_port,
            args.subdomain.clone(),
        );

        info!("Starting client...");
        if let Err(e) = client.run().await {
            error!("Client error: {}", e);
        }

        info!("Disconnected. Reconnecting in 3 seconds...");
        sleep(Duration::from_secs(3)).await;
    }
}
