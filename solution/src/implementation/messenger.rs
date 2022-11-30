use std::sync::Arc;

use async_channel::{Sender, Receiver};
use log::debug;

use crate::{ClientRegisterCommand, SystemRegisterCommand, OperationReturn};


struct Messenger {
    self_rank: u8,
    client_msg_tx: Sender<(ClientRegisterCommand, Sender<OperationReturn>)>,
    system_msg_tx: Sender<SystemRegisterCommand>,
    // request_client_msg: Receiver<()>,
    // request_system_msg: Receiver<()>,
    request_client_msg_handle_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
    request_system_msg_handle_rx: Receiver<SystemRegisterCommand>,
}

impl Messenger {
    pub async fn run(self: &Arc<Self>) {
        tokio::join!(
            self.run_client_handle(),
            self.run_system_handle(),
        );
    }

    async fn run_system_handle(self: &Arc<Self>) {
        let me = self.clone();
        let system_handle = tokio::spawn(async move {
            // loop {
            //     match me.request_system_msg.recv().await {
            //         Ok(_) => break,
            //         Err(e) => {
            //             debug!("Error in Messenger system handle request_system_msg.recv: {:?}", e);
            //             continue;
            //         },
            //     }
            // }

            loop {
                match me.request_system_msg_handle_rx.recv().await {
                    Ok(system_msg) => {
                        if let Err(e) = me.system_msg_tx.send(system_msg).await {
                            debug!("Error in Messenger system_msg_tx.send: {:?}", e);
                        }
                        break;
                    },
                    Err(e) => {
                        debug!("Error in Messenger system handle request_system_msg_handle.recv: {:?}", e);
                        continue;
                    },
                }
            }
        });

        if let Err(e) = system_handle.await {
            debug!("Error in Messenger system_handle.await: {:?}", e);
        }
    }

    async fn run_client_handle(self: &Arc<Self>) {
        let me = self.clone();
        let client_handle = tokio::spawn(async move {
            loop {
                // loop {
                //     match me.request_client_msg.recv().await {
                //         Ok(_) => break,
                //         Err(e) => {
                //             debug!("Error in Messenger client handle request_client_msg.recv: {:?}", e);
                //             continue;
                //         },
                //     }
                // }

                loop {
                    match me.request_client_msg_handle_rx.recv().await {
                        Ok((client_msg, handle_tx)) => {
                            // let (result_tx, result_rx) = bounded::<OperationReturn>(1);
                            if let Err(e) = me.client_msg_tx.send((client_msg, handle_tx)).await {
                                debug!("Error in Messenger client_msg_tx.send: {:?}", e);
                            }
                            break;
                        }
                        Err(e) => {
                            debug!("Error in Messenger client handle request_client_msg_handle.recv: {:?}", e);
                            continue;
                        }
                    }
                }

            }
        });

        if let Err(e) = client_handle.await {
            debug!("Error in Messenger client_handle.await: {:?}", e);
        }
    }
}