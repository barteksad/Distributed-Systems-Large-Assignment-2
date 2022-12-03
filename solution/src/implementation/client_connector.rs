use std::collections::VecDeque;
use std::sync::Arc;

use async_channel::{bounded, unbounded, Receiver, Sender};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::{
    serialize_register_command, Configuration, RegisterClient, RegisterCommand,
    SystemRegisterCommand,
};

use crate::register_client_public::{Broadcast, Send};

use super::utils::stubborn_send;

static MAX_NOT_SEND_MSG_COUNT: usize = 256; 

struct Connection {
    host: String,
    port: u16,
    msg_queue: Receiver<Vec<u8>>,
    recover_rx: Receiver<()>,
}

impl Connection {
    fn new(
        host: String,
        port: u16,
        msg_queue: Receiver<Vec<u8>>,
        recover_rx: Receiver<()>,
    ) -> Self {
        Connection {
            host,
            port,
            msg_queue,
            recover_rx,
        }
    }
    async fn run(&mut self) {
        let peer_address = format!("{}:{}", self.host, self.port);
        let mut not_send : VecDeque<Vec<u8>> = VecDeque::with_capacity(256);
        loop {
            if let Ok(stream) = TcpStream::connect(&peer_address).await {
                'send_loop: loop {
                    while !not_send.is_empty() {
                        if stubborn_send(&stream, not_send.front().unwrap()).await {
                            not_send.pop_front();
                        } else {
                            break 'send_loop;
                        }
                    }

                    if let Ok(msg) = self.msg_queue.recv().await {
                        if !stubborn_send(&stream, &msg).await {
                            not_send.push_back(msg);
                            break 'send_loop;
                        }
                    }
                }
            }

            'recover_wait_loop: loop {
                tokio::select! {
                    _ = self.recover_rx.recv() => {
                        break 'recover_wait_loop;
                    },
                    Ok(msg) = self.msg_queue.recv() => {
                        not_send.push_back(msg);
                        if not_send.len() > MAX_NOT_SEND_MSG_COUNT {
                            not_send.pop_front();
                        }
                    }
                };
            }
        }
    }
}

pub struct ClientConnector {
    self_rank: u8,
    hmac_system_key: [u8; 64],
    msg_txs: Vec<Sender<Vec<u8>>>,
    recover_txs: Vec<Sender<()>>,
    request_system_msg_handle_tx: Sender<SystemRegisterCommand>,
    system_recovered_rx: Receiver<u8>,
    connection_handles: Vec<JoinHandle<()>>,
}

impl ClientConnector {
    pub fn new(
        config: &Configuration,
        request_system_msg_handle_tx: Sender<SystemRegisterCommand>,
        system_recovered_rx: Receiver<u8>,
    ) -> Self {
        let n_connections = (config.public.tcp_locations.len() - 1) as usize;
        let mut msg_txs = Vec::with_capacity(n_connections);
        let mut recover_txs = Vec::with_capacity(n_connections);
        let mut connection_handles = Vec::with_capacity(n_connections);

        for process in 0..n_connections {
            if process == (config.public.self_rank - 1) as usize {
                continue;
            }

            let (host, port) = config.public.tcp_locations.get(process).unwrap().clone();
            let (msg_tx, msg_rx) = unbounded();
            let (recover_tx, recover_rx) = bounded(1);
            msg_txs.push(msg_tx);
            recover_txs.push(recover_tx);
            let mut connection = Connection {
                host,
                port,
                msg_queue: msg_rx,
                recover_rx,
            };
            connection_handles.push(tokio::spawn(async move {
                connection.run().await;
            }));
        }

        ClientConnector {
            self_rank: config.public.self_rank - 1,
            hmac_system_key: config.hmac_system_key,
            msg_txs,
            recover_txs,
            request_system_msg_handle_tx,
            system_recovered_rx,
            connection_handles,
        }
    }

    fn process2index(&self, process: u8) -> usize {
        if process < self.self_rank {
            return process as usize;
        } else {
            return (process - 1) as usize;
        }
    }

    pub async fn run(&self) {
        loop {
            let process = self.system_recovered_rx.recv().await.unwrap();
            self.recover_txs
                .get(self.process2index(process))
                .unwrap()
                .send(())
                .await
                .unwrap();
        }
    }
}

#[async_trait::async_trait]
impl RegisterClient for ClientConnector {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send) {
        if msg.target == self.self_rank {
            self.request_system_msg_handle_tx
                .send(Arc::try_unwrap(msg.cmd).unwrap())
                .await
                .unwrap();
            return;
        }

        let process = self.process2index(msg.target);
        let mut buff: Vec<u8> = Vec::new();
        serialize_register_command(
            &RegisterCommand::System(Arc::try_unwrap(msg.cmd).expect("Error unwraping msg Send")),
            &mut buff,
            &self.hmac_system_key,
        )
        .await
        .expect("Error serializing register command!");
        self.msg_txs.get(process).unwrap().send(buff).await.unwrap();
    }

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast) {
        let cmd = Arc::try_unwrap(msg.cmd).expect("Error unwraping msg Broadcast");
        self.request_system_msg_handle_tx
            .send(cmd.clone())
            .await
            .unwrap();
        let mut buff: Vec<u8> = Vec::new();
        serialize_register_command(
            &RegisterCommand::System(cmd),
            &mut buff,
            &self.hmac_system_key,
        )
        .await
        .expect("Error serializing register command!");

        for tx in self.msg_txs.iter() {
            tx.send(buff.clone()).await.unwrap();
        }
    }
}
