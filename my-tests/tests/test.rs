use std::time::Duration;

use assignment_2_solution::{ClientRegisterCommandContent, ClientCommandHeader, ClientRegisterCommand, RegisterCommand, SectorVec};
use ntest::timeout;
use my_test_utils::utils::{TestProcessesConfig, RegisterResponseContent};

#[tokio::test]
#[timeout(4000)]
async fn crash_recover() {
    let n_processes = 4;
    let port_range_start = 10000;
    let n_clients = 16;

    let config = TestProcessesConfig::new(n_processes, port_range_start);
    config.init().await;
    for i in 0..n_processes {
        config.start(i).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }

    for (i, stream) in streams.iter_mut().enumerate() {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: i.try_into().unwrap(),
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![if i % 2 == 0 { 1 } else { 254 }; 4096]),
                    },
                }),
                stream,
            )
            .await;
    }

    for stream in &mut streams {
        config.read_response(stream).await.unwrap();
    }

    for i in 0..n_processes {
        config.kill(i).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..n_processes {
        config.start(i).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }

    config
    .send_cmd(
        &RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: n_clients,
                sector_idx: 0,
            },
            content: ClientRegisterCommandContent::Read,
        }),
        &mut streams[0],
    )
    .await;
    let response = config.read_response(&mut streams[0]).await.unwrap();

    match response.content {
        RegisterResponseContent::Read(SectorVec(sector)) => {
            assert!(sector == vec![1; 4096] || sector == vec![254; 4096]);
        }
        _ => panic!("Expected read response"),
    }
}
