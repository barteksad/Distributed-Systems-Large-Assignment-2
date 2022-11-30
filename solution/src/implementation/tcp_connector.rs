use std::{collections::HashSet, net::SocketAddr, sync::Arc, marker::PhantomData};

use async_channel::{Sender, bounded};
use log::debug;
use tokio::net::{TcpListener, TcpStream};

use crate::{ClientRegisterCommand, OperationReturn, SystemRegisterCommand};

struct TCPConnector {
    hmac_client_key: [u8; 32],
    tcp_locations: HashSet<SocketAddr>,
    self_rank: u8,
    request_client_msg_handle_tx: Sender<(ClientRegisterCommand, Sender<OperationReturn>)>,
    request_system_msg_handle_tx: Sender<SystemRegisterCommand>,
}

impl TCPConnector {
    pub async fn run(self: Arc<Self>, listener : TcpListener) {
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
    }

    pub async fn system_message_handle(&self, socket: TcpStream) {
        loop {
            unimplemented!();
            let system_msg : SystemRegisterCommand;
            self.request_system_msg_handle_tx.send(system_msg).await.unwrap();
        }
    }

    pub async fn client_message_handle(&self, socket: TcpStream) {
        unimplemented!();
        // let client_msg : ClientRegisterCommand;
        // let (result_tx, result_rx) = bounded(1);
        // self.request_client_msg_handle_tx.send((client_msg, result_tx)).await.unwrap();
    }
}