use async_channel::{Receiver, Sender};
use log::debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

use crate::atomic_register_public::*;
use crate::domain::*;
use crate::register_client_public::RegisterClient;
use crate::sectors_manager_public::*;
use crate::stable_storage_public::*;

struct AtomicRegisterInstance {
    self_ident: u8,
    self_id: Uuid,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
}

pub struct ARWorker {
    self_id: Uuid,
    ar_instance: AtomicRegisterInstance,
    client_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
    system_rx: Receiver<SystemRegisterCommand>,
    client_msg_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
    system_msg_rx: Receiver<SystemRegisterCommand>,
    client_msg_finished_tx: Sender<Uuid>,
    system_msg_finished_tx: Sender<SectorIdx>,
}

#[async_trait::async_trait]
impl AtomicRegister for AtomicRegisterInstance {
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
        client_msg_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
        system_msg_rx: Receiver<SystemRegisterCommand>,
        client_msg_finished_tx: Sender<Uuid>,
        system_msg_finished_tx: Sender<SectorIdx>,
    ) -> Self {
        unimplemented!();
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Ok((client_msg, result_tx)) = self.client_rx.recv() => {
                    self.handle_client_command(client_msg, result_tx).await;
                    self.client_msg_finished_tx.send(self.self_id).await.unwrap();
                }
                Err(e) = self.client_rx.recv() => {
                    debug!("Error in ARWorker client_rx.recv: {:?}", e);
                }
                Ok(system_msg) = self.system_rx.recv() => {
                    let sector_idx = system_msg.header.sector_idx;
                    self.ar_instance.system_command(system_msg).await;
                    self.system_msg_finished_tx.send(sector_idx).await.unwrap();
                }
                Err(e) = self.system_rx.recv() => {
                    debug!("Error in ARWorker system_rx.recv: {:?}", e);
                }
            }
        }
    }

    async fn handle_client_command(
        &mut self,
        client_msg: ClientRegisterCommand,
        result_tx: Sender<OperationReturn>,
    ) {
        let success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        > = Box::new(move |operation_success: OperationSuccess| {
            Box::pin(async move {
                if let Err(e) = result_tx.send(operation_success.op_return).await {
                    debug!("Error in ARWorker result_tx.send: {:?}", e);
                }
            })
        });

        self.ar_instance.client_command(client_msg, success_callback).await
    }
}
