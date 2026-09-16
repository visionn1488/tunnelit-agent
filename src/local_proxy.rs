use crate::protocol::AgentMessage;
use base64::{engine::general_purpose::STANDARD as b64, Engine};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use uuid::Uuid;

pub struct LocalConnections {
    connections: HashMap<Uuid, mpsc::UnboundedSender<Vec<u8>>>,
}

impl LocalConnections {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    pub async fn route_data(&self, conn_id: Uuid, data: Vec<u8>) {
        if let Some(sender) = self.connections.get(&conn_id) {
            if let Err(e) = sender.send(data) {
                error!("Failed to route data to connection {}: {}", conn_id, e);
            }
        } else {
            debug!("Connection {} not found for routing", conn_id);
        }
    }

    pub fn close_connection(&mut self, conn_id: &Uuid) {
        self.connections.remove(conn_id);
        debug!("Closed connection {}", conn_id);
    }

    pub fn spawn_tcp_connection(
        &mut self,
        conn_id: Uuid,
        local_port: u16,
        ws_sender: mpsc::UnboundedSender<AgentMessage>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        self.connections.insert(conn_id, tx);

        tokio::spawn(async move {
            match TcpStream::connect(format!("127.0.0.1:{}", local_port)).await {
                Ok(stream) => {
                    info!("TCP Connected to local port {}", local_port);
                    let (mut read_half, mut write_half) = stream.into_split();

                    // Read from local TCP, send to relay
                    let ws_sender_clone = ws_sender.clone();
                    let read_task = async move {
                        let mut buf = vec![0; 8192];
                        loop {
                            match read_half.read(&mut buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    let payload = b64.encode(&buf[..n]);
                                    if ws_sender_clone
                                        .send(AgentMessage::Data { conn_id, payload })
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Local TCP read error: {}", e);
                                    break;
                                }
                            }
                        }
                    };

                    // Read from relay (rx), write to local TCP
                    let write_task = async move {
                        while let Some(data) = rx.recv().await {
                            if let Err(e) = write_half.write_all(&data).await {
                                error!("Local TCP write error: {}", e);
                                break;
                            }
                        }
                    };

                    tokio::select! {
                        _ = read_task => {},
                        _ = write_task => {},
                    }

                    let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                    info!("TCP Connection {} finished", conn_id);
                }
                Err(e) => {
                    error!("Failed to connect to local TCP port {}: {}", local_port, e);
                    let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                }
            }
        });
    }

    pub fn spawn_udp_connection(
        &mut self,
        conn_id: Uuid,
        local_port: u16,
        ws_sender: mpsc::UnboundedSender<AgentMessage>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        self.connections.insert(conn_id, tx);

        tokio::spawn(async move {
            match UdpSocket::bind("127.0.0.1:0").await {
                Ok(socket) => {
                    if let Err(e) = socket.connect(format!("127.0.0.1:{}", local_port)).await {
                        error!("Failed to connect UDP to local port {}: {}", local_port, e);
                        let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                        return;
                    }

                    info!("UDP Connected to local port {}", local_port);
                    let socket = std::sync::Arc::new(socket);
                    let socket_read = socket.clone();
                    let socket_write = socket.clone();

                    // Read from local UDP, send to relay
                    let ws_sender_clone = ws_sender.clone();
                    let read_task = async move {
                        let mut buf = vec![0; 65536];
                        loop {
                            match socket_read.recv(&mut buf).await {
                                Ok(n) => {
                                    let payload = b64.encode(&buf[..n]);
                                    if ws_sender_clone
                                        .send(AgentMessage::Data { conn_id, payload })
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Local UDP read error: {}", e);
                                    break;
                                }
                            }
                        }
                    };

                    // Read from relay (rx), write to local UDP
                    let write_task = async move {
                        while let Some(data) = rx.recv().await {
                            if let Err(e) = socket_write.send(&data).await {
                                error!("Local UDP write error: {}", e);
                                break;
                            }
                        }
                    };

                    tokio::select! {
                        _ = read_task => {},
                        _ = write_task => {},
                    }

                    let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                    info!("UDP Connection {} finished", conn_id);
                }
                Err(e) => {
                    error!("Failed to bind local UDP socket: {}", e);
                    let _ = ws_sender.send(AgentMessage::CloseConnection { conn_id });
                }
            }
        });
    }
}
