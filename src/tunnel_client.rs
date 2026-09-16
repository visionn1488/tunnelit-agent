use crate::local_proxy::LocalConnections;
use crate::protocol::{AgentMessage, RelayMessage};
use base64::{engine::general_purpose::STANDARD as b64, Engine};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};
use url::Url;

pub struct TunnelClient {
    relay_url: Url,
    local_port: u16,
    protocol: String,
    preferred_port: Option<u16>,
    subdomain: Option<String>,
}

impl TunnelClient {
    pub fn new(
        relay_url: Url,
        local_port: u16,
        protocol: String,
        preferred_port: Option<u16>,
        subdomain: Option<String>,
    ) -> Self {
        Self {
            relay_url,
            local_port,
            protocol,
            preferred_port,
            subdomain,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to relay at {}", self.relay_url);
        let (ws_stream, _) = connect_async(self.relay_url.clone()).await?;
        info!("Connected to relay");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<AgentMessage>();

        let mut local_connections = LocalConnections::new();
        
        // Spawn task to forward messages from tx channel to websocket
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match serde_json::to_string(&msg) {
                    Ok(json) => {
                        if let Err(e) = ws_sender.send(Message::Text(json)).await {
                            error!("Failed to send message to relay: {}", e);
                            break;
                        }
                    }
                    Err(e) => error!("Failed to serialize message: {}", e),
                }
            }
        });

        // Send CreateTunnel
        let create_tunnel = AgentMessage::CreateTunnel {
            local_port: self.local_port,
            protocol: self.protocol.clone(),
            preferred_port: self.preferred_port,
        };
        tx.send(create_tunnel)?;

        while let Some(msg_result) = ws_receiver.next().await {
            match msg_result {
                Ok(Message::Text(text)) => match serde_json::from_str::<RelayMessage>(&text) {
                    Ok(RelayMessage::TunnelCreated { tunnel_id, public_port }) => {
                        info!(
                            "Tunnel created! public port: {}, id: {}",
                            public_port, tunnel_id
                        );
                        if let Some(desired_name) = &self.subdomain {
                            tx.send(AgentMessage::RequestSubdomain {
                                tunnel_id,
                                desired_name: desired_name.clone(),
                            })?;
                        }
                    }
                    Ok(RelayMessage::NewConnection { tunnel_id: _, conn_id }) => {
                        info!("New connection: {}", conn_id);
                        if self.protocol == "tcp" {
                            local_connections.spawn_tcp_connection(conn_id, self.local_port, tx.clone());
                        } else if self.protocol == "udp" {
                            local_connections.spawn_udp_connection(conn_id, self.local_port, tx.clone());
                        }
                    }
                    Ok(RelayMessage::Data { conn_id, payload }) => {
                        match b64.decode(payload) {
                            Ok(data) => {
                                local_connections.route_data(conn_id, data).await;
                            }
                            Err(e) => error!("Failed to decode payload for {}: {}", conn_id, e),
                        }
                    }
                    Ok(RelayMessage::CloseConnection { conn_id }) => {
                        info!("Relay requested to close connection: {}", conn_id);
                        local_connections.close_connection(&conn_id);
                    }
                    Ok(RelayMessage::SubdomainAssigned { tunnel_id: _, subdomain }) => {
                        info!("Subdomain assigned: {}", subdomain);
                    }
                    Ok(RelayMessage::Error { message }) => {
                        error!("Relay error: {}", message);
                    }
                    Err(e) => error!("Failed to parse RelayMessage: {} | {}", e, text),
                },
                Ok(Message::Close(_)) => {
                    info!("Relay closed connection");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {} // Ignore other message types (Ping, Pong, Binary)
            }
        }

        Ok(())
    }
}
