use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use async_channel::Sender;
use log::debug;
use tokio::net::{TcpListener, TcpStream};

use crate::{ClientRegisterCommand, Configuration, OperationReturn, SystemRegisterCommand};

pub struct TCPConnector {
    hmac_client_key: [u8; 32],
    tcp_locations: HashSet<SocketAddr>,
    self_rank: u8,
    request_client_msg_handle_tx: Sender<(ClientRegisterCommand, Sender<OperationReturn>)>,
    request_system_msg_handle_tx: Sender<SystemRegisterCommand>,
}

pub async fn get_listener(config: &Configuration) -> TcpListener {
    let self_rank = config.public.self_rank;
    let (host, port) = config.public.tcp_locations.get(self_rank as usize).unwrap();
    TcpListener::bind(format!("{}:{}", host, port).to_string())
        .await
        .unwrap()
}

impl TCPConnector {
    pub fn new(
        config: &Configuration,
        request_client_msg_handle_tx: Sender<(ClientRegisterCommand, Sender<OperationReturn>)>,
        request_system_msg_handle_tx: Sender<SystemRegisterCommand>,
    ) -> Self {
        TCPConnector {
            hmac_client_key: config.hmac_client_key,
            tcp_locations: HashSet::from_iter(
                config
                    .public
                    .tcp_locations
                    .iter()
                    .map(|(host, port)| format!("{}:{}", host, port).to_string().parse().unwrap()),
            ),
            self_rank: config.public.self_rank,
            request_client_msg_handle_tx,
            request_system_msg_handle_tx,
        }
    }

    pub async fn run(self: Arc<Self>, listener: TcpListener) {
        let run_handle = tokio::spawn(async move {
            loop {
                if let Ok((socket, addr)) = listener.accept().await {
                    debug!("TCPConnector new connection from {:?}", addr);
                    // check if addr is one of tcp location of other processes
                    let me = self.clone();
                    if self.tcp_locations.contains(&addr) {
                        tokio::spawn(async move {
                            me.system_message_handle(socket).await;
                        });
                    } else {
                        tokio::spawn(async move {
                            me.client_message_handle(socket).await;
                        });
                    }
                }
            }
        });

        if let Err(e) = run_handle.await {
            debug!("Error in TCPConnector main loop: {:?}", e);
        }
    }

    pub async fn system_message_handle(&self, socket: TcpStream) {
        loop {
            unimplemented!();
            let system_msg: SystemRegisterCommand;
            self.request_system_msg_handle_tx
                .send(system_msg)
                .await
                .unwrap();
        }
    }

    pub async fn client_message_handle(&self, socket: TcpStream) {
        unimplemented!();
        // let client_msg : ClientRegisterCommand;
        // let (result_tx, result_rx) = bounded(1);
        // self.request_client_msg_handle_tx.send((client_msg, result_tx)).await.unwrap();
    }
}
