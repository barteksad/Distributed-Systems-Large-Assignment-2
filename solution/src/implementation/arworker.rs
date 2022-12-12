use async_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::marker::Send;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

use crate::domain;
use crate::AtomicRegister;
use crate::Broadcast;
use crate::ClientCommandHeader;
use crate::ClientRegisterCommand;
use crate::ClientRegisterCommandContent;
use crate::OperationReturn;
use crate::OperationSuccess;
use crate::RegisterClient;
use crate::SectorIdx;
use crate::SectorVec;
use crate::SectorsManager;
use crate::StableStorage;
use crate::SystemCommandHeader;
use crate::SystemRegisterCommand;
use crate::SystemRegisterCommandContent;

pub struct AtomicRegisterInstance {
    self_ident: u8,
    self_id: Uuid,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,

    readlist: HashMap<u8, (u64, u8, SectorVec)>,
    reading: bool,
    writing: bool,
    acklist: HashSet<u8>,
    writeval: Option<SectorVec>,
    readval: Option<SectorVec>,
    write_phase: bool,
    success_callback: Option<
        Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    >,
    request_identifier: Option<u64>,
}

impl AtomicRegisterInstance {
    pub async fn new(
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
            readlist: HashMap::new(),
            acklist: HashSet::new(),
            reading: false,
            writing: false,
            writeval: None,
            readval: None,
            write_phase: false,
            success_callback: None,
            request_identifier: None,
        }
    }

    async fn get_rid(&mut self) -> u64 {
        let rid_maybe: Vec<u8> = self
            .metadata
            .get(&"rid".to_string())
            .await
            .unwrap_or(bincode::serialize(&(0 as u64)).unwrap());
        bincode::deserialize::<u64>(&rid_maybe).expect("Error reading rid from StableStorage")
    }

    async fn store_rid(&mut self, rid: u64) {
        let data = bincode::serialize(&rid).unwrap();
        self.metadata
            .put(&"rid".to_string(), &data)
            .await
            .expect("Error storing rid in StableStorage");
    }

    async fn client_read(&mut self, header: ClientCommandHeader) {
        let rid = self.get_rid().await + 1;
        self.store_rid(rid).await;
        self.readlist.clear();
        self.acklist.clear();
        self.reading = true;

        let sector_idx = header.sector_idx;
        let system_msg = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: self.self_id,
                read_ident: rid,
                sector_idx,
            },
            content: SystemRegisterCommandContent::ReadProc,
        };

        self.register_client
            .broadcast(Broadcast {
                cmd: Arc::new(system_msg),
            })
            .await;
    }

    async fn client_write(&mut self, header: ClientCommandHeader, data: SectorVec) {
        let rid = self.get_rid().await + 1;
        self.writeval = Some(data);
        self.readlist.clear();
        self.acklist.clear();
        self.writing = true;
        self.store_rid(rid).await;

        let sector_idx = header.sector_idx;
        let system_msg = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: self.self_id,
                read_ident: rid,
                sector_idx: sector_idx,
            },
            content: SystemRegisterCommandContent::ReadProc,
        };

        self.register_client
            .broadcast(Broadcast {
                cmd: Arc::new(system_msg),
            })
            .await;
    }

    async fn system_read_proc(&self, header: SystemCommandHeader) {
        let (ts, wr) = self.sectors_manager.read_metadata(header.sector_idx).await;
        let val = self.sectors_manager.read_data(header.sector_idx).await;
        let system_msg = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: header.msg_ident,
                read_ident: header.read_ident,
                sector_idx: header.sector_idx,
            },
            content: SystemRegisterCommandContent::Value {
                timestamp: ts,
                write_rank: wr,
                sector_data: val,
            },
        };

        self.register_client
            .send(crate::Send {
                cmd: Arc::new(system_msg),
                target: header.process_identifier,
            })
            .await;
    }

    async fn system_write_proc(
        &mut self,
        header: SystemCommandHeader,
        ts: u64,
        wr: u8,
        v: SectorVec,
    ) {
        let (curr_ts, curr_wr) = self.sectors_manager.read_metadata(header.sector_idx).await;

        if (ts, wr) > (curr_ts, curr_wr) {
            self.sectors_manager
                .write(header.sector_idx, &(v, ts, wr))
                .await;
        }

        let system_msg = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: header.msg_ident,
                read_ident: header.read_ident,
                sector_idx: header.sector_idx,
            },
            content: SystemRegisterCommandContent::Ack,
        };

        self.register_client
            .send(crate::Send {
                cmd: Arc::new(system_msg),
                target: header.process_identifier,
            })
            .await;
    }

    async fn system_value(&mut self, header: SystemCommandHeader, ts: u64, wr: u8, v: SectorVec) {
        let rid = self.get_rid().await;
        if !(rid == header.read_ident && !self.write_phase) {
            return;
        }

        self.readlist.insert(header.process_identifier, (ts, wr, v));
        // readlist[self] should be received by broadcasting VALUES to ourselves
        if !self.readlist.contains_key(&self.self_ident) {
            return;
        }
        // >= not > beacuse we set readlist[self] also by receiving VALUE from ourselves
        if self.readlist.len() as u8 >= (self.processes_count / 2) + self.processes_count % 2
            && (self.reading || self.writing)
        {
            let mut sorted: Vec<(u64, u8, SectorVec)> =
                self.readlist.drain().map(|(_, v)| v).collect();
            sorted.sort_by(|(lsh_ts, lhs_wr, _), (rhs_ts, rhs_wr, _)| {
                (lsh_ts, lhs_wr).cmp(&(rhs_ts, rhs_wr))
            });

            let (maxts, rr, new_readval) = sorted.pop().unwrap();
            self.readval = Some(new_readval);
            self.readlist.clear();
            self.acklist.clear();
            self.write_phase = true;

            let header = SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: header.msg_ident,
                read_ident: rid,
                sector_idx: header.sector_idx,
            };
            let content = match self.reading {
                true => SystemRegisterCommandContent::WriteProc {
                    timestamp: maxts,
                    write_rank: rr,
                    data_to_write: self
                        .readval
                        .clone()
                        .expect("Error in algorithm logic, writeval not set"),
                },
                false => {
                    // Do not store(ts, wr, val) here because it may cause race condition, instead store it when received broadcasted WRITE_PROC
                    SystemRegisterCommandContent::WriteProc {
                        timestamp: maxts + 1,
                        write_rank: self.self_ident,
                        data_to_write: self
                            .writeval
                            .clone()
                            .expect("Error in algorithm logic, writeval not set"),
                    }
                }
            };

            let system_msg = SystemRegisterCommand { header, content };

            self.register_client
                .broadcast(Broadcast {
                    cmd: Arc::new(system_msg),
                })
                .await;
        }
    }

    async fn system_ack(&mut self, header: SystemCommandHeader) {
        let rid = self.get_rid().await;
        if !(rid == header.read_ident && self.write_phase) {
            return;
        }

        self.acklist.insert(header.process_identifier);

        if !self.acklist.contains(&self.self_ident) {
            return;
        }

        if self.acklist.len() as u8 >= (self.processes_count / 2) + self.processes_count % 2
            && (self.reading || self.writing)
        {
            self.acklist.clear();
            self.write_phase = false;
            let op_return = match self.reading {
                true => {
                    self.reading = false;
                    OperationReturn::Read(domain::ReadReturn {
                        read_data: self
                            .readval
                            .take()
                            .expect("Error in algorithm logic, readval not set"),
                    })
                }
                false => {
                    self.writing = false;
                    OperationReturn::Write
                }
            };

            let op_success = OperationSuccess {
                request_identifier: self
                    .request_identifier
                    .take()
                    .expect("Error in algorithm logic, request_identifier not set"),
                op_return,
            };
            self.success_callback
                .take()
                .expect("Error in algorithm logic, success_callback not set")(
                op_success
            )
            .await;
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
        self.success_callback = Some(success_callback);
        self.request_identifier = Some(cmd.header.request_identifier);

        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.client_read(cmd.header).await;
            }
            ClientRegisterCommandContent::Write { data } => {
                self.client_write(cmd.header, data).await;
            }
        };
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => {
                self.system_read_proc(cmd.header).await;
            }
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                self.system_value(cmd.header, timestamp, write_rank, sector_data)
                    .await;
            }
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => {
                self.system_write_proc(cmd.header, timestamp, write_rank, data_to_write)
                    .await;
            }
            SystemRegisterCommandContent::Ack => {
                self.system_ack(cmd.header).await;
            }
        }
    }
}

pub struct ARWorker {
    self_id: Uuid,
    ar: Box<dyn AtomicRegister>,
}

impl ARWorker {
    pub async fn new(
        self_ident: u8,
        self_id: Uuid,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Self {
        let ar = Box::new(
            AtomicRegisterInstance::new(
                self_ident,
                self_id,
                metadata,
                register_client,
                sectors_manager,
                processes_count,
            )
            .await,
        );

        ARWorker { self_id, ar }
    }

    pub async fn run(
        mut self,
        client_msg_rx: Receiver<(ClientRegisterCommand, Sender<OperationReturn>)>,
        system_msg_rx: Receiver<SystemRegisterCommand>,
        client_msg_finished_tx: Sender<Uuid>,
        system_msg_finished_tx: Sender<SectorIdx>,
    ) {
        let client_msg_rx = client_msg_rx.recv();
        let system_msg_rx = system_msg_rx.recv();
        tokio::pin!(client_msg_rx);
        tokio::pin!(system_msg_rx);
        loop {
                tokio::select! {
                Ok((client_msg, result_tx)) = &mut client_msg_rx => {
                    self.handle_client_command(client_msg, result_tx, client_msg_finished_tx.clone()).await;
                }
                Ok(system_msg) = &mut system_msg_rx => {
                    let sector_idx = system_msg.header.sector_idx;
                    self.ar.system_command(system_msg).await;
                    system_msg_finished_tx.send(sector_idx).await.unwrap();
                }
            }
        }
    }

    async fn handle_client_command(
        &mut self,
        client_msg: ClientRegisterCommand,
        result_tx: Sender<OperationReturn>,
        client_msg_finished_tx: Sender<Uuid>,
    ) {
        let id = self.self_id.clone();
        let success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        > = Box::new(move |operation_success: OperationSuccess| {
            Box::pin(async move {
                result_tx.send(operation_success.op_return).await.unwrap();
                client_msg_finished_tx.send(id).await.unwrap();
            })
        });

        self.ar.client_command(client_msg, success_callback).await
    }
}
