use std::time::Duration;

use my_test_utils::utils::TestProcessesConfig;


#[tokio::main]
async fn main() {
    env_logger::init();
    let n_processes = 16;
    let port_range_start = 12345;
    let n_clients = 16;

    let config = TestProcessesConfig::new(n_processes, port_range_start);
    config.init().await;
    for i in 0..n_processes {
        config.start(i).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    tokio::time::sleep(Duration::from_secs(100000)).await;
}