use async_channel::{Receiver, Sender};
use log::debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

// use crate::{StableStorage, RegisterClient, SectorsManager, AtomicRegister, ClientRegisterCommand, OperationSuccess, SystemRegisterCommand};
use crate::atomic_register_public::*;
use crate::domain::*;
use crate::register_client_public::RegisterClient;
use crate::sectors_manager_public::*;
use crate::stable_storage_public::*;

struct AtomicRegisterInstance;

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
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
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

    async fn run_client_handle(
        &mut self,
        success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        >,
    ) {
        loop {
            loop {
                match self.client_cmd_finished.send(()).await {
                    Ok(_) => break,
                    Err(e) => {
                        debug!("Error in ARWorker request_client_msg.send: {:?}", e);
                        continue;
                    }
                }
            }

            loop {
                match self.client_rx.recv().await {
                    Ok((client_msg, result_tx)) => {
                        let success_callback: Box<
                            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                                + Send
                                + Sync,
                        > = Box::new(move |operation_success: OperationSuccess| {
                            Box::pin(async move { 
                                if let Err(e) = result_tx.send(operation_success.op_return).await {
                                    
                                }
                                () 
                            })
                        });

                        self.client_command(client_msg, success_callback).await
                    }
                    Err(e) => {
                        debug!("Error in ARWorker client_rx.recv: {:?}", e);
                        continue;
                    }
                }
            }
        }
    }
}
