use std::convert::TryInto;

use assignment_2_solution::{
    run_register_process, serialize_register_command, ClientRegisterCommand, Configuration,
    PublicConfiguration, RegisterCommand, SectorVec, StatusCode, MAGIC_NUMBER,
};
use async_channel::{Sender, Receiver};
use hmac::{Hmac, Mac};
use log::info;
use rand::Rng;
use sha2::Sha256;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Duration;

pub const HMAC_TAG_SIZE: usize = 32;

pub struct RegisterResponse {
    pub header: RegisterResponseHeader,
    pub content: RegisterResponseContent,
    pub hmac_tag: [u8; HMAC_TAG_SIZE],
}

impl RegisterResponse {
    fn msg_type(&self) -> u8 {
        match self.content {
            RegisterResponseContent::Read(_) => 0x41,
            RegisterResponseContent::Write => 0x42,
        }
    }
}

pub struct RegisterResponseHeader {
    pub status_code: StatusCode,
    pub request_identifier: u64,
}

pub enum RegisterResponseContent {
    Read(SectorVec),
    Write,
}

pub struct TestProcessesConfig {
    hmac_client_key: Vec<u8>,
    hmac_system_key: Vec<u8>,
    storage_dirs: Vec<TempDir>,
    tcp_locations: Vec<(String, u16)>,
    starts: Vec<(Sender<()>, Receiver<()>)>,
    kills: Vec<(Sender<()>, Receiver<()>)>, 
}

impl TestProcessesConfig {
    pub const N_SECTORS: u64 = 65536;

    pub fn new(processes_count: usize, port_range_start: u16) -> Self {
        TestProcessesConfig {
            hmac_client_key: (0..32)
                .map(|_| rand::thread_rng().gen_range(0..255))
                .collect(),
            hmac_system_key: (0..64)
                .map(|_| rand::thread_rng().gen_range(0..255))
                .collect(),
            storage_dirs: (0..processes_count)
                .map(|_| tempfile::tempdir().unwrap())
                .collect(),
            tcp_locations: (0..processes_count)
                .map(|idx| ("localhost".to_string(), port_range_start + idx as u16))
                .collect(),
            starts: (0..processes_count).map(|_| async_channel::bounded(1)).collect(),
            kills: (0..processes_count).map(|_| async_channel::bounded(1)).collect(),
        }
    }

    fn config(&self, proc_idx: usize) -> Configuration {
        Configuration {
            public: PublicConfiguration {
                storage_dir: self
                    .storage_dirs
                    .get(proc_idx)
                    .unwrap()
                    .path()
                    .to_path_buf(),
                tcp_locations: self.tcp_locations.clone(),
                self_rank: (proc_idx + 1) as u8,
                n_sectors: TestProcessesConfig::N_SECTORS,
            },
            hmac_system_key: self.hmac_system_key.clone().try_into().unwrap(),
            hmac_client_key: self.hmac_client_key.clone().try_into().unwrap(),
        }
    }

    pub async fn init(&self) {
        let processes_count = self.storage_dirs.len();
        for idx in 0..processes_count {
            let start_rx = self.starts.get(idx).unwrap().1.clone();
            let kill_rx = self.kills.get(idx).unwrap().1.clone();
            let config =self.config(idx); 
            tokio::spawn(async move {
                loop {
                    _ = start_rx.recv().await;
                    let config_cloned = Configuration {
                        public: PublicConfiguration {
                            tcp_locations: config.public.tcp_locations.clone(),
                            self_rank: config.public.self_rank,
                            n_sectors: config.public.n_sectors,
                            storage_dir: config.public.storage_dir.clone(),
                        },
                        hmac_system_key: config.hmac_system_key,
                        hmac_client_key: config.hmac_client_key,
                    };
                    tokio::select! {
                        _ = kill_rx.recv() => {
                            info!("Process {} killed", idx);
                            break;
                        }
                        _ = run_register_process(config_cloned) => {
                            info!("Process {} exited", idx);
                            break;
                        }
                    }
                }
            });
        }
        // wait_for_tcp_listen().await;
    }

    pub async fn start(&self, proc_idx: usize) {
        let start_tx = &self.starts.get(proc_idx).unwrap().0;
        start_tx.send(()).await.unwrap();
    }

    pub async fn kill(&self, proc_idx: usize) {
        let kill_tx = &self.kills.get(proc_idx).unwrap().0;
        kill_tx.send(()).await.unwrap();
    }

    pub async fn send_cmd(&self, register_cmd: &RegisterCommand, stream: &mut TcpStream) {
        let mut data = Vec::new();
        serialize_register_command(register_cmd, &mut data, &self.hmac_client_key)
            .await
            .unwrap();

        stream.write_all(&data).await.unwrap();
    }

    pub async fn connect(&self, proc_idx: usize) -> TcpStream {
        let location = self.tcp_locations.get(proc_idx).unwrap();
        TcpStream::connect((location.0.as_str(), location.1))
            .await
            .expect("Could not connect to TCP port")
    }

    pub async fn read_response(&self, stream: &mut TcpStream) -> Result<RegisterResponse, String> {
        let mut buf = [0; 8];
        stream.read_exact(&mut buf).await.unwrap();
        if &buf[0..4] != MAGIC_NUMBER.as_ref() {
            return Err("Invalid magic number".to_string());
        }
        let status_code = try_to_status_code(*buf.get(6).unwrap());
        let msg_type = *buf.get(7).unwrap();
        if status_code.is_none() {
            return Err("Invalid status code".to_string());
        }

        let status_code = status_code.unwrap();
        stream.read_exact(&mut buf).await.unwrap();
        let request_number = u64::from_be_bytes(buf);
        let header = RegisterResponseHeader {
            status_code,
            request_identifier: request_number,
        };
        let mut hmac_tag = [0; HMAC_TAG_SIZE];
        match msg_type {
            66 => {
                stream.read_exact(&mut hmac_tag).await.unwrap();
                Ok(RegisterResponse {
                    header,
                    content: RegisterResponseContent::Write,
                    hmac_tag,
                })
            }
            65 => {
                let mut sector = vec![0; 4096];
                stream.read_exact(&mut sector).await.unwrap();
                stream.read_exact(&mut hmac_tag).await.unwrap();
                Ok(RegisterResponse {
                    header,
                    content: RegisterResponseContent::Read(SectorVec(sector)),
                    hmac_tag,
                })
            }
            _ => Err(format!("Invalid message type: {}", msg_type)),
        }
    }

    pub fn assert_response_header(&self, response: &RegisterResponse, cmd: &ClientRegisterCommand) {
        assert_eq!(
            response.header.request_identifier,
            cmd.header.request_identifier
        );
        assert!(self.hmac_tag_is_ok(response));
    }

    fn hmac_tag_is_ok(&self, response: &RegisterResponse) -> bool {
        let msg_type = response.msg_type();
        let mut data = vec![];
        data.extend_from_slice(MAGIC_NUMBER.as_ref());
        data.extend(&[0, 0, response.header.status_code as u8, msg_type]);
        data.extend(&response.header.request_identifier.to_be_bytes());
        match &response.content {
            RegisterResponseContent::Read(SectorVec(sector)) => data.extend(sector),
            RegisterResponseContent::Write => {}
        }

        let mut mac = HmacSha256::new_from_slice(&self.hmac_client_key).unwrap();
        mac.update(&data);
        mac.verify_slice(&response.hmac_tag).is_ok()
    }
}

fn try_to_status_code(byte: u8) -> Option<StatusCode> {
    match byte {
        0 => Some(StatusCode::Ok),
        1 => Some(StatusCode::AuthFailure),
        2 => Some(StatusCode::InvalidSectorIndex),
        _ => None,
    }
}

async fn wait_for_tcp_listen() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

pub type HmacSha256 = Hmac<Sha256>;
