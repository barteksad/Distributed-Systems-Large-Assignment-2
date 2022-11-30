use std::sync::Arc;
use async_channel::{Sender, Receiver};
use log::debug;
use uuid::Uuid;
use std::future::Future;
use std::pin::Pin;

// use crate::{StableStorage, RegisterClient, SectorsManager, AtomicRegister, ClientRegisterCommand, OperationSuccess, SystemRegisterCommand};
use crate::atomic_register_public::*;
use crate::sectors_manager_public::*;
use crate::register_client_public::RegisterClient;
use crate::stable_storage_public::*;
use crate::domain::*;

struct ARWorker {
    self_ident: u8,
    self_id: Uuid,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    client_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
    system_rx: Receiver<SystemRegisterCommand>,
    client_cmd_finished: Sender<()>,
    system_cmd_finished: Sender<()>,
}

#[async_trait::async_trait]
impl AtomicRegister for ARWorker {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                + Send
                + Sync,
        >,
    ) {
        unimplemented!();
    }

    /// Handle a system command.
    ///
    /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
    /// and ACK messages in the (N,N)-AtomicRegister algorithm.
    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        unimplemented!();
    }
}

impl ARWorker {
    pub fn new(
        self_ident: u8,
        self_id: Uuid,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
        client_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
        system_rx: Receiver<SystemRegisterCommand>,
        client_cmd_finished: Sender<()>,
        system_cmd_finished: Sender<()>,
    ) -> Self {
        unimplemented!();
    }
    pub async fn run() {
        unimplemented!();
    }

    async fn run_client_handle(self: &Arc<Self>) {
        let me = self.clone();

        let client_handle = tokio::spawn(async move {
            loop {
                match me.client_cmd_finished.send(()).await {
                    Ok(_) => break,
                    Err(e) => {
                        debug!("Error in ARWorker request_client_msg.send: {:?}", e);
                        continue;
                    },
                }
            }

            loop {
                match me.client_rx.recv().await {
                    Ok((client_msg, result_tx)) => {
                        let success_callback = |operation_success: OperationSuccess| {
                            result_tx.send(operation_success.op_return)
                        };

                        me.client_command(client_msg, success_callback).await
                    },
                    Err(e) => {
                        debug!("Error in ARWorker client_rx.recv: {:?}", e);
                        continue;
                    },
                }
            }

        });
    }
}
