use std::{
    collections::HashMap,
    sync::Arc,
};

use async_channel::{bounded, unbounded, Receiver, Sender};
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::{
    implementation::{arworker::ARWorker, storage_data::build_stable_storage},
    ClientRegisterCommand, Configuration, OperationReturn, SectorIdx, SystemRegisterCommand,
    SystemRegisterCommandContent::{Ack, ReadProc, Value, WriteProc}, RegisterClient,
};

use super::{
    client_connector::ClientConnector, sector_storage::SectorStorage, tcp_connector::TCPConnector,
};

static ARWORKER_COUNT: u8 = 16;

pub struct RunManager {
    ready_for_client: Vec<Uuid>,
    ready_for_system: Vec<Uuid>,
    sector2rw: HashMap<SectorIdx, (usize, Uuid)>,
    // uuid2sector: HashMap<Uuid, SectorIdx>,
    request_client_msg_handle_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
    request_system_msg_handle_rx: Receiver<SystemRegisterCommand>,
    uuid2client_msg_tx: HashMap<Uuid, Sender<(ClientRegisterCommand, Sender<OperationReturn>)>>,
    uuid2system_msg_tx: HashMap<Uuid, Sender<SystemRegisterCommand>>,
    client_msg_finished_rx: Receiver<Uuid>,
    system_msg_finished_rx: Receiver<SectorIdx>,
}

impl RunManager {
    pub async fn new(config: Configuration, tcp_listener: TcpListener) -> Self {
        let (request_client_msg_handle_tx, request_client_msg_handle_rx) = bounded(1);
        let (request_system_msg_handle_tx, request_system_msg_handle_rx) = unbounded();
        let (system_recovered_tx, system_recovered_rx) = bounded(1);
        let tcp_connector = Arc::new(TCPConnector::new(
            &config,
            request_client_msg_handle_tx,
            request_system_msg_handle_tx.clone(),
            system_recovered_tx
        ));
        tokio::spawn(async move { tcp_connector.run(tcp_listener).await });

        let sectors_manager = Arc::new(SectorStorage {});
        let register_client = Arc::new(ClientConnector::new(&config, request_system_msg_handle_tx, system_recovered_rx));
        
        let (client_msg_finished_tx, client_msg_finished_rx) = unbounded::<Uuid>();
        let (system_msg_finished_tx, system_msg_finished_rx) = unbounded::<SectorIdx>();

        let mut ready_for_client = Vec::new();
        let mut ready_for_system = Vec::new();

        let mut uuid2client_msg_tx = HashMap::new();
        let mut uuid2system_msg_tx = HashMap::new();

        for i in 0..ARWORKER_COUNT {
            let uuid = Uuid::from_u128((config.public.self_rank * ARWORKER_COUNT + i) as u128);

            let mut worker_path = config.public.storage_dir.clone();
            worker_path.push(format!("stable-storage-{}", i));
            tokio::fs::create_dir_all(&worker_path).await.unwrap();
            let stable_storage = build_stable_storage(worker_path).await;

            ready_for_client.push(uuid);
            ready_for_system.push(uuid);

            let (client_msg_tx, client_msg_rx) =
                bounded::<(ClientRegisterCommand, Sender<OperationReturn>)>(1);
            let (system_msg_tx, system_msg_rx) = unbounded::<SystemRegisterCommand>();
            
            uuid2client_msg_tx.insert(uuid, client_msg_tx);
            uuid2system_msg_tx.insert(uuid, system_msg_tx);

            let mut arworker = ARWorker::new(
                config.public.self_rank,
                uuid,
                stable_storage,
                register_client.clone() as Arc<dyn RegisterClient>,
                sectors_manager.clone(),
                config.public.tcp_locations.len() as u8,
                client_msg_rx.clone(),
                system_msg_rx.clone(),
                client_msg_finished_tx.clone(),
                system_msg_finished_tx.clone(),
            );
            tokio::spawn(async move { arworker.run().await });
        }

        // tokio::spawn(async move { sectors_manager.run().await });
        tokio::spawn(async move { register_client.run().await });

        RunManager {
            ready_for_client,
            ready_for_system,
            sector2rw: HashMap::new(),
            // uuid2sector: HashMap::new(),
            request_client_msg_handle_rx,
            request_system_msg_handle_rx,
            uuid2client_msg_tx,
            uuid2system_msg_tx,
            client_msg_finished_rx,
            system_msg_finished_rx,
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                // Send new client message to be handeled if there is ARWorker ready to do so
                Ok((client_msg, result_tx)) = self.request_client_msg_handle_rx.recv(), if !self.ready_for_client.is_empty() => {
                    assert!(!self.ready_for_client.is_empty());
                    let uuid = self.ready_for_client.pop().unwrap();
                    // self.uuid2sector.insert(uuid, client_msg.header.sector_idx);
                    self.uuid2client_msg_tx.get_mut(&uuid).unwrap().send((client_msg, result_tx)).await.unwrap();
                }
                // Client message handling finished
                Ok(uuid) = self.client_msg_finished_rx.recv() => {
                    // assert!(self.uuid2sector.remove(&uuid).is_some());
                    self.ready_for_client.push(uuid);
                }
                // Send new system message to be handeled if there is ARWorker ready to do so
                Ok(system_msg) = self.request_system_msg_handle_rx.recv(), if !self.ready_for_system.is_empty() => {
                    assert!(!self.ready_for_system.is_empty());
                    let sector_idx = system_msg.header.sector_idx;
                    match system_msg.content {
                        Value { .. } | Ack => {
                            let uuid = &system_msg.header.msg_ident;
                            self.uuid2system_msg_tx.get(uuid).unwrap().send(system_msg).await.unwrap();
                        }
                        ReadProc | WriteProc { .. } => {    
                            if let Some((rw_count, uuid)) = self.sector2rw.get_mut(&sector_idx) {
                                *rw_count += 1;
                                self.uuid2system_msg_tx.get_mut(uuid).unwrap().send(system_msg).await.unwrap();
                            } else {
                                let uuid = self.ready_for_system.pop().unwrap();
                                self.sector2rw.insert(sector_idx, (1, uuid));
                                self.uuid2system_msg_tx.get_mut(&uuid).unwrap().send(system_msg).await.unwrap();
                            }
                        }
                    }
                }
                Ok(sector_idx) = self.system_msg_finished_rx.recv() => {
                    let (rw_count, uuid) = self.sector2rw.get_mut(&sector_idx).unwrap();
                    *rw_count -= 1;
                    if *rw_count == 0 {
                        self.ready_for_system.push(*uuid);
                        self.sector2rw.remove(&sector_idx);
                    }
                }
            }
        }
    }
}
