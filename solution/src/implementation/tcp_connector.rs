use std::sync::Arc;

use async_channel::{bounded, Sender};
use log::debug;
use tokio::net::{TcpListener, TcpStream, tcp::OwnedWriteHalf};

use crate::{
    deserialize_register_command, ClientRegisterCommand, ClientRegisterCommandContent,
    Configuration, OperationReturn, RegisterCommand, SectorIdx, StatusCode, SystemRegisterCommand,
    MAGIC_NUMBER,
};

use super::utils::{add_hmac_tag, stubborn_send};

pub struct TCPConnector {
    hmac_client_key: [u8; 32],
    hmac_system_key: [u8; 64],
    n_sectors: SectorIdx,
    request_client_msg_handle_tx: Sender<(ClientRegisterCommand, Sender<OperationReturn>)>,
    request_system_msg_handle_tx: Sender<SystemRegisterCommand>,
    system_recovered_tx: Sender<u8>,
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
        system_recovered_tx: Sender<u8>,
    ) -> Self {
        TCPConnector {
            hmac_client_key: config.hmac_client_key,
            hmac_system_key: config.hmac_system_key,
            n_sectors: config.public.n_sectors,
            request_client_msg_handle_tx,
            request_system_msg_handle_tx,
            system_recovered_tx,
        }
    }

    pub async fn run(self: Arc<Self>, listener: TcpListener) {
        loop {
            if let Ok((mut socket, addr)) = listener.accept().await {
                debug!("TCPConnector new connection from {:?}", addr);
                let me = self.clone();
                tokio::spawn(async move {
                    match deserialize_register_command(
                        &mut socket,
                        &me.hmac_system_key,
                        &me.hmac_client_key,
                    )
                    .await
                    {
                        Ok((RegisterCommand::Client(client_msg), hmac_valid)) => {
                            me.client_message_handle(socket, client_msg, hmac_valid)
                                .await;
                        }
                        Ok((RegisterCommand::System(system_msg), hmac_valid)) => {
                            me.system_message_handle(socket, system_msg, hmac_valid)
                                .await;
                        }
                        Err(e) => {
                            debug!("Error in TcpListener during new connection: {:?}", e);
                        }
                    }
                });
            }
        }
    }

    async fn system_message_handle(
        &self,
        socket: TcpStream,
        mut system_msg: SystemRegisterCommand,
        mut hmac_valid: bool,
    ) {
        self.system_recovered_tx.send(system_msg.header.process_identifier).await.unwrap();
        
        let peer_address = socket.peer_addr();
        let (read_half, _) = socket.into_split();
        let mut read_buff = tokio::io::BufReader::new(read_half);

        loop {
            // TODO check sector index and process rank
            if hmac_valid {
                self.request_system_msg_handle_tx
                    .send(system_msg)
                    .await
                    .unwrap();
            } else {
                debug!(
                    "Invalid hmac in system message from: {:?}",
                    peer_address
                );
            }

            if let Ok((RegisterCommand::System(new_system_msg), new_hmac_valid)) =
                deserialize_register_command(
                    &mut read_buff,
                    &self.hmac_system_key,
                    &self.hmac_client_key,
                )
                .await
            {
                system_msg = new_system_msg;
                hmac_valid = new_hmac_valid;
                debug!("New system message from: {:?}", peer_address);
            } else {
                return;
            }
        }
    }

    async fn client_message_handle(
        &self,
        socket: TcpStream,
        mut client_msg: ClientRegisterCommand,
        mut hmac_valid: bool,
    ) {
        let (read_half, mut write_half) = socket.into_split();
        let mut read_buff = tokio::io::BufReader::new(read_half);

        loop {
            let mut op_return: Option<OperationReturn> = None;
            let request_identifier = client_msg.header.request_identifier;
            let msg_type: u8 = match client_msg.content {
                ClientRegisterCommandContent::Read => 0x41,
                ClientRegisterCommandContent::Write { .. } => 0x42,
            };
            let status_code = match (hmac_valid, client_msg.header.sector_idx < self.n_sectors) {
                (false, _) => StatusCode::AuthFailure,
                (_, false) => StatusCode::InvalidSectorIndex,
                _ => StatusCode::Ok,
            };

            if status_code == StatusCode::Ok {
                let (result_tx, result_rx) = bounded(2);
                self.request_client_msg_handle_tx
                    .send((client_msg, result_tx))
                    .await
                    .unwrap();

                if let Ok(received_op_return) = result_rx.recv().await {
                    op_return = Some(received_op_return);
                }

                assert!(op_return.is_some());
            }

            self.send_client_response(
                &mut write_half,
                status_code,
                op_return,
                msg_type,
                request_identifier,
            ).await;

            if let Ok((RegisterCommand::Client(new_client_msg), new_hmac_valid)) =
                deserialize_register_command(
                    &mut read_buff,
                    &self.hmac_system_key,
                    &self.hmac_client_key,
                )
                .await
            {
                client_msg = new_client_msg;
                hmac_valid = new_hmac_valid;
                debug!("New client message from: {:?}", write_half.peer_addr());
            } else {
                return;
            }
        }
    }

    async fn send_client_response(
        &self,
        socket: &mut OwnedWriteHalf, 
        status_code: StatusCode,
        op_return: Option<OperationReturn>,
        msg_type: u8,
        request_identifier: u64,
    ) {
        let mut header_buff = Vec::<u8>::with_capacity(16);
        let (mut content_buff, content) = match op_return {
            None => (vec![0u8; 0], None),
            Some(OperationReturn::Read(read_ret)) => {
                (Vec::<u8>::with_capacity(4096), Some(read_ret))
            }
            Some(OperationReturn::Write) => (Vec::<u8>::with_capacity(0), None),
        };

        header_buff.extend(&MAGIC_NUMBER);
        header_buff.extend([0u8, 0u8]); // Padding
        header_buff.push(status_code as u8);
        header_buff.push(msg_type);
        header_buff.extend(request_identifier.to_be_bytes());
        if let Some(reat_ret) = content {
            content_buff.extend(reat_ret.read_data.0);
        };
        let hmac_tag = add_hmac_tag(&header_buff, &content_buff, &self.hmac_client_key);
        let data = [header_buff, content_buff, hmac_tag].concat();
        stubborn_send(socket, &data[..]).await;
    }
}
