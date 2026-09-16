use crate::protocol::{AgentMessage, TunnelConfig};
use base64::{engine::general_purpose::STANDARD as b64, Engine};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct LocalConnections {
    tunnels: Arc<RwLock<HashMap<Uuid, TunnelConfig>>>,
    connections: Arc<RwLock<HashMap<Uuid, mpsc::UnboundedSender<Vec<u8>>>>>,
}

impl LocalConnections {
    pub fn new() -> Self {
        Self {
            tunnels: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn sync_tunnels(&self, list: Vec<TunnelConfig>) {
        let mut map = self.tunnels.write().await;
        map.clear();
        for t in list {
            if t.enabled {
                map.insert(t.id, t);
            }
        }
    }

    pub async fn start_tunnel(&self, tunnel: TunnelConfig) {
        let mut map = self.tunnels.write().await;
        map.insert(tunnel.id, tunnel);
    }

    pub async fn stop_tunnel(&self, tunnel_id: &Uuid) {
        let mut map = self.tunnels.write().await;
        map.remove(tunnel_id);
    }

    pub async fn route_data(&self, conn_id: Uuid, data: Vec<u8>) {
        let map = self.connections.read().await;
        if let Some(sender) = map.get(&conn_id) {
            let _ = sender.send(data);
        } else {
            debug!("Connection {} not found for data routing", conn_id);
        }
    }

    pub async fn close_connection(&self, conn_id: &Uuid) {
        let mut map = self.connections.write().await;
        map.remove(conn_id);
        debug!("Closed local proxy connection {}", conn_id);
    }

    pub async fn handle_new_connection(
        &self,
        tunnel_id: Uuid,
        conn_id: Uuid,
        ws_sender: mpsc::UnboundedSender<AgentMessage>,
    ) {
        let tunnel_opt = {
            let map = self.tunnels.read().await;
            map.get(&tunnel_id).cloned()
        };

        let tunnel = match tunnel_opt {
            Some(t) => t,
            None => {
                warn!("Received connection for unknown or disabled tunnel {}", tunnel_id);
                let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                return;
            }
        };

        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        {
            let mut conns = self.connections.write().await;
            conns.insert(conn_id, tx);
        }

        let conns_clone = self.connections.clone();

        if tunnel.protocol.to_lowercase() == "tcp" {
            tokio::spawn(async move {
                Self::run_tcp_stream(conn_id, tunnel.local_port, rx, ws_sender, conns_clone).await;
            });
        } else {
            tokio::spawn(async move {
                Self::run_udp_stream(conn_id, tunnel.local_port, rx, ws_sender, conns_clone).await;
            });
        }
    }

    async fn run_tcp_stream(
        conn_id: Uuid,
        local_port: u16,
        mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
        ws_sender: mpsc::UnboundedSender<AgentMessage>,
        connections: Arc<RwLock<HashMap<Uuid, mpsc::UnboundedSender<Vec<u8>>>>>,
    ) {
        match TcpStream::connect(format!("127.0.0.1:{}", local_port)).await {
            Ok(stream) => {
                info!("Forwarding connection {} to 127.0.0.1:{}", conn_id, local_port);
                let (mut read_half, mut write_half) = stream.into_split();

                let ws_clone = ws_sender.clone();
                let read_task = async move {
                    let mut buf = vec![0u8; 8192];
                    loop {
                        match read_half.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let payload = b64.encode(&buf[..n]);
                                if ws_clone.send(AgentMessage::Data { conn_id, payload }).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                debug!("Local read error on connection {}: {}", conn_id, e);
                                break;
                            }
                        }
                    }
                };

                let write_task = async move {
                    while let Some(data) = rx.recv().await {
                        if write_half.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                };

                tokio::select! {
                    _ = read_task => {},
                    _ = write_task => {},
                }

                let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                let mut conns = connections.write().await;
                conns.remove(&conn_id);
            }
            Err(e) => {
                error!("Failed to connect to local port {}: {}", local_port, e);
                let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                let mut conns = connections.write().await;
                conns.remove(&conn_id);
            }
        }
    }

    async fn run_udp_stream(
        conn_id: Uuid,
        local_port: u16,
        mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
        ws_sender: mpsc::UnboundedSender<AgentMessage>,
        connections: Arc<RwLock<HashMap<Uuid, mpsc::UnboundedSender<Vec<u8>>>>>,
    ) {
        match UdpSocket::bind("127.0.0.1:0").await {
            Ok(socket) => {
                if let Err(e) = socket.connect(format!("127.0.0.1:{}", local_port)).await {
                    error!("UDP connect error to local port {}: {}", local_port, e);
                    let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                    return;
                }

                let socket = Arc::new(socket);
                let socket_read = socket.clone();
                let socket_write = socket.clone();

                let ws_clone = ws_sender.clone();
                let read_task = async move {
                    let mut buf = vec![0u8; 65536];
                    loop {
                        match socket_read.recv(&mut buf).await {
                            Ok(n) => {
                                let payload = b64.encode(&buf[..n]);
                                if ws_clone.send(AgentMessage::Data { conn_id, payload }).is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                };

                let write_task = async move {
                    while let Some(data) = rx.recv().await {
                        if socket_write.send(&data).await.is_err() {
                            break;
                        }
                    }
                };

                tokio::select! {
                    _ = read_task => {},
                    _ = write_task => {},
                }

                let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                let mut conns = connections.write().await;
                conns.remove(&conn_id);
            }
            Err(e) => {
                error!("Failed to bind local UDP socket: {}", e);
                let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                let mut conns = connections.write().await;
                conns.remove(&conn_id);
            }
        }
    }
}
