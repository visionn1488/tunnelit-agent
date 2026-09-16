use crate::config::AgentConfig;
use crate::local_proxy::LocalConnections;
use crate::protocol::{AgentMessage, RelayMessage};
use base64::{engine::general_purpose::STANDARD as b64, Engine};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};
use url::Url;

pub struct TunnelClient {
    relay_url: Url,
    token: Option<String>,
    hostname: String,
}

impl TunnelClient {
    pub fn new(relay_url: Url, token: Option<String>) -> Self {
        let hostname = std::env::var("USER")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "my-device".to_string());

        Self {
            relay_url,
            token,
            hostname,
        }
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to relay at {}", self.relay_url);
        let (ws_stream, _) = connect_async(self.relay_url.clone()).await?;
        info!("WebSocket connected to relay");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<AgentMessage>();

        let local_connections = LocalConnections::new();

        // Forward internal messages to WebSocket
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Ok(json) = serde_json::to_string(&msg) {
                    if ws_sender.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Keep connection alive with periodic pings every 15s (prevents reverse proxy / Caddy idle timeouts)
        let ping_tx = tx.clone();
        let ping_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
            loop {
                interval.tick().await;
                if ping_tx.send(AgentMessage::Ping).is_err() {
                    break;
                }
            }
        });

        // Authenticate or request Claim
        if let Some(token) = &self.token {
            tx.send(AgentMessage::Auth {
                token: token.clone(),
                hostname: self.hostname.clone(),
            })?;
        } else {
            tx.send(AgentMessage::RequestClaim {
                hostname: self.hostname.clone(),
            })?;
        }

        while let Some(msg_result) = ws_receiver.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str::<RelayMessage>(&text) {
                        Ok(RelayMessage::ClaimReady { code, claim_url, token }) => {
                            if let Some(ref t) = token {
                                self.token = Some(t.clone());
                                let mut cfg = AgentConfig::load();
                                cfg.token = Some(t.clone());
                                cfg.save();
                            }
                            let web_host = self.relay_url.host_str().unwrap_or("localhost");
                            let display_host = if web_host.contains("ws.") {
                                web_host.replace("ws.", "cabinet.")
                            } else if web_host.contains("ezbchat.fun") {
                                "cabinet.ezbchat.fun".to_string()
                            } else {
                                web_host.to_string()
                            };
                            let full_url = if claim_url.starts_with("http") {
                                claim_url
                            } else {
                                format!("https://{}{}", display_host, claim_url)
                            };

                            println!("\n╔════════════════════════════════════════════════════════════════════════════╗");
                            println!("║                                                                            ║");
                            println!("║   🔗 LINK YOUR DEVICE TO YOUR ACCOUNT:                                     ║");
                            println!("║                                                                            ║");
                            println!("║      {}", full_url);
                            println!("║                                                                            ║");
                            println!("║   (Or visit your dashboard and enter code: {})                     ║", code);
                            println!("║                                                                            ║");
                            println!("╚════════════════════════════════════════════════════════════════════════════╝\n");
                            println!("Waiting for confirmation in browser...");
                        }
                        Ok(RelayMessage::AuthSuccess { name, token, .. }) => {
                            self.token = Some(token.clone());
                            let mut cfg = AgentConfig::load();
                            cfg.token = Some(token);
                            cfg.save();

                            println!("\n==================================================");
                            println!("  ✅ Connected and Authenticated as '{}'", name);
                            println!("  🌐 Manage your tunnels in the web dashboard!");
                            println!("==================================================\n");
                        }
                        Ok(RelayMessage::SyncTunnels { tunnels }) => {
                            local_connections.sync_tunnels(tunnels.clone()).await;

                            println!("--- Active Tunnels ({} configured) ---", tunnels.len());
                            for t in &tunnels {
                                if t.enabled {
                                    println!(
                                        "  🟢 [{}] {} -> 127.0.0.1:{} (Public Port: {})",
                                        t.protocol.to_uppercase(),
                                        t.name,
                                        t.local_port,
                                        t.public_port
                                    );
                                } else {
                                    println!("  ⚪ [{}] {} (Paused)", t.protocol.to_uppercase(), t.name);
                                }
                            }
                            println!("--------------------------------------");
                        }
                        Ok(RelayMessage::StartTunnel { tunnel }) => {
                            println!(
                                "\n[+] Tunnel Activated: {} [{}] 127.0.0.1:{} -> Public :{}",
                                tunnel.name,
                                tunnel.protocol.to_uppercase(),
                                tunnel.local_port,
                                tunnel.public_port
                            );
                            local_connections.start_tunnel(tunnel).await;
                        }
                        Ok(RelayMessage::StopTunnel { tunnel_id }) => {
                            println!("\n[-] Tunnel Deactivated: {}", tunnel_id);
                            local_connections.stop_tunnel(&tunnel_id).await;
                        }
                        Ok(RelayMessage::NewConnection { tunnel_id, conn_id }) => {
                            local_connections
                                .handle_new_connection(tunnel_id, conn_id, tx.clone())
                                .await;
                        }
                        Ok(RelayMessage::Data { conn_id, payload }) => {
                            if let Ok(data) = b64.decode(payload) {
                                local_connections.route_data(conn_id, data).await;
                            }
                        }
                        Ok(RelayMessage::CloseConnection { conn_id }) => {
                            local_connections.close_connection(&conn_id).await;
                        }
                        Ok(RelayMessage::Error { message }) => {
                            error!("Relay error: {}", message);
                            if message.contains("Invalid") || message.contains("expired") {
                                warn!("Clearing saved token due to authentication failure");
                                let mut cfg = AgentConfig::load();
                                cfg.token = None;
                                cfg.save();
                                self.token = None;
                            }
                        }
                        Ok(RelayMessage::Pong) => {}
                        Err(e) => {
                            warn!("Failed to parse RelayMessage: {} | {}", e, text);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("Relay closed connection");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        ping_handle.abort();
        Ok(())
    }
}

