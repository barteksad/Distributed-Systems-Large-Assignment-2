use async_channel::{Receiver, Sender};
use log::debug;
use std::collections::HashMap;
use std::collections::HashSet;
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

    readlist: HashMap<u8, (u64, u8, SectorVec)>,
    acklist: HashSet<u8>,
}

impl AtomicRegisterInstance {
    fn new(
        self_ident: u8,
        self_id: Uuid,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Self {
        AtomicRegisterInstance {
            self_ident,
            self_id,
            metadata,
            register_client,
            sectors_manager,
            processes_count,
        }
    }
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

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        let rid_maybe: Vec<u8> = self
            .metadata
            .get(&"rid".to_string())
            .await
            .unwrap_or(bincode::serialize(&(0 as usize)).unwrap());
        let rid: usize = bincode::deserialize::<usize>(&rid_maybe)
            .expect("Error reading rid from StableStorage")
            + 1;
    }
}

pub struct ARWorker {
    self_id: Uuid,
    ar: Box<dyn AtomicRegister>,
    client_msg_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
    system_msg_rx: Receiver<SystemRegisterCommand>,
    client_msg_finished_tx: Sender<Uuid>,
    system_msg_finished_tx: Sender<SectorIdx>,
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
        let ar = AtomicRegisterInstance::new(
            self_ident,
            self_id,
            metadata,
            register_client,
            sectors_manager,
            processes_count,
        );

        ARWorker {
            self_id,
            ar,
            client_msg_rx,
            system_msg_rx,
            client_msg_finished_tx,
            system_msg_finished_tx,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Ok((client_msg, result_tx)) = self.client_msg_rx.recv() => {
                    self.handle_client_command(client_msg, result_tx).await;
                    self.client_msg_finished_tx.send(self.self_id).await.unwrap();
                }
                Err(e) = self.client_msg_rx.recv() => {
                    debug!("Error in ARWorker client_rx.recv: {:?}", e);
                }
                Ok(system_msg) = self.system_msg_rx.recv() => {
                    let sector_idx = system_msg.header.sector_idx;
                    self.ar.system_command(system_msg).await;
                    self.system_msg_finished_tx.send(sector_idx).await.unwrap();
                }
                Err(e) = self.system_msg_rx.recv() => {
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

        self.ar.client_command(client_msg, success_callback).await
    }
}
