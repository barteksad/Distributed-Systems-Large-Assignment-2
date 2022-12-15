use std::env;

use assignment_2_solution::{Configuration, PublicConfiguration, run_register_process};
use log::debug;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    env_logger::init();
    let n_clients: u16 = env::args().nth(1).unwrap().parse().unwrap();
    let self_rank = env::args().nth(2).unwrap().parse().unwrap();
    let storage_dir = tempdir().unwrap();
    let tcp_port = 12345;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: (0..n_clients).map(|p| ("127.0.0.1".to_string(), p + tcp_port)).collect(),
            self_rank,
            n_sectors: 2_u64.pow(21),
            storage_dir: storage_dir.into_path(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key: [0; 32],
    };
    debug!("Starting process {:?}", self_rank);
    tokio::spawn(run_register_process(config)).await.unwrap();
}