use std::{sync::Arc, collections::{HashMap, LinkedList}};

use async_channel::{Receiver, Sender, bounded, unbounded};
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::{ClientRegisterCommand, SystemRegisterCommand, OperationReturn, Configuration, SectorIdx, implementation::{arworker::ARWorker, storage_data::{StorageData, build_stable_storage}}};

use super::{sector_storage::SectorStorage, client_connector::ClientConnector, tcp_connector::TCPConnector};

static ARWORKER_COUNT: usize = 16;

struct RunManager {
    ready_for_client: LinkedList<Uuid>,
    ready_for_system: LinkedList<Uuid>,
    sector2rw: HashMap<SectorIdx, usize>,
    uuid2sector: HashMap<Uuid, SectorIdx>
}

impl RunManager {
    pub async fn new(
        config: Configuration,
        tcp_listener: TcpListener,
    ) -> Self {
        let (request_client_msg_handle_tx, request_client_msg_handle_rx) = bounded(1);
        let (request_system_msg_handle_tx, request_system_msg_handle_rx) = unbounded();
        let tcp_connector = Arc::new(TCPConnector::new(&config, request_client_msg_handle_tx, request_system_msg_handle_tx));
        
        let sector_manager = Arc::new(SectorStorage{});
        let register_client = Arc::new(ClientConnector{});

        let (client_tx, client_rx) = bounded::<(ClientRegisterCommand, Sender<OperationReturn>)>(1);
        let (client_msg_finished_tx, client_msg_finished_rx) = unbounded::<Uuid>();
        let (system_msg_finished_tx, system_msg_finished_rx) = unbounded::<Uuid>();
        
        let mut ready_for_client = LinkedList::new();
        let mut ready_for_system = LinkedList::new();

        for i in 0..ARWORKER_COUNT {
            let uuid = Uuid::new_v4();

            let mut worker_path = config.public.storage_dir;
            worker_path.push(format!("stable-storage-{}", i))
            tokio::fs::create_dir_all(worker_path).await.unwrap();
            let stable_storage = build_stable_storage(worker_path);


            ready_for_client.push_back(uuid);
            ready_for_system.push_back(uuid);

            let arworker = ARWorker::new(
                config.public.self_rank,
                uuid, 
                metadata, 
                register_client, 
                sectors_manager, 
                processes_count, 
                client_msg_rx, 
                system_msg_rx, 
                client_msg_finished_tx, 
                system_msg_finished_tx,
            );
        }
        unimplemented!();
    }
}