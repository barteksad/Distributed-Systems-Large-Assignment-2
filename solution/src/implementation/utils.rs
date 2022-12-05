use std::time::Duration;

use hmac::{Hmac, Mac};
use log::debug;
use sha2::Sha256;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::tcp::OwnedWriteHalf,
    time::{sleep, Instant},
};
use uuid::Uuid;

use crate::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand,
    SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent,
    MAGIC_NUMBER,
};

type HmacSha256 = Hmac<Sha256>;

static N_SEND_TRIES: usize = 5;
static NEXT_SEND_DELAY: Duration = Duration::from_millis(5000);

pub fn add_hmac_tag(header_buff: &Vec<u8>, content_buff: &Vec<u8>, hmac_key: &[u8; 32]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(header_buff);
    mac.update(content_buff);
    mac.finalize().into_bytes().to_vec()
}

pub async fn stubborn_send(socket: &OwnedWriteHalf, data: &[u8]) -> bool {
    let mut wrote = 0;
    let sleep = sleep(NEXT_SEND_DELAY);
    tokio::pin!(sleep);

    for _ in 0..N_SEND_TRIES {
        tokio::select! {
            _ = socket.writable() => {
                match socket.try_write(&data[wrote..]) {
                    Ok(n) => {
                        wrote += n;
                        if wrote == data.len() {
                            return true;
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    Err(e) => {
                        debug!("Error in socket.try_write to: {:?}, {:?}", socket.peer_addr(), e);
                        return false;
                    }
                }
            },
            _ = &mut sleep => {
                sleep.as_mut().reset(Instant::now() + NEXT_SEND_DELAY);
            },
        }
    }

    return false;
}

pub async fn detect_and_deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), std::io::Error> {
    loop {
        let mut buff = vec![0u8; MAGIC_NUMBER.len()];

        while buff != MAGIC_NUMBER {
            data.read_exact(&mut buff).await?;
            if buff == MAGIC_NUMBER {
                break;
            };
        }

        data.read_exact(&mut buff).await?;

        match try_to_msg_type(buff[3]) {
            Some(code) if code == MessageCode::Read || code == MessageCode::Write => {
                let mut mac = HmacSha256::new_from_slice(hmac_client_key).unwrap();
                mac.update(&MAGIC_NUMBER);
                mac.update(&buff);
                match deserialize_client_command(data, mac, code).await {
                    Ok(command) => return Ok(command),
                    Err(DeserializeError::IoError(e)) => return Err(e),
                    Err(DeserializeError::Other(what)) => {
                        debug!("Error in deserialize_client_command from: {:?}", what);
                        continue;
                    }
                }
            }
            Some(code) => {
                let mut mac = HmacSha256::new_from_slice(hmac_system_key).unwrap();
                mac.update(&MAGIC_NUMBER);
                mac.update(&buff);
                let process_identifier = buff[2];
                match deserialize_system_command(data, mac, code, process_identifier).await {
                    Ok(command) => return Ok(command),
                    Err(DeserializeError::IoError(e)) => return Err(e),
                    Err(DeserializeError::Other(what)) => {
                        debug!("Error in deserialize_system_command from: {:?}", what);
                        continue;
                    }
                }
            }
            None => continue,
        }
    }
}

async fn deserialize_client_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    mut mac: HmacSha256,
    code: MessageCode,
) -> Result<(RegisterCommand, bool), DeserializeError> {
    let mut buff = match code {
        MessageCode::Read => vec![0u8; 8 + 8],
        MessageCode::Write => vec![0u8; 8 + 8 + 4096],
        _ => unreachable!(),
    };

    data.read_exact(&mut buff)
        .await
        .map_err(|e| DeserializeError::IoError(e))?;
    mac.update(&buff);
    // Check before deserialize to read exact amonut of bytes
    let hmac_valid = check_hmac_valid(mac, data)
        .await
        .map_err(|e| DeserializeError::IoError(e))?;

    let request_identifier = u64::from_be_bytes(buff[0..8].try_into().or(Err(
        DeserializeError::Other("invalid request_identifier".to_string()),
    ))?);
    let sector_idx = u64::from_be_bytes(buff[8..16].try_into().or(Err(
        DeserializeError::Other("invalid sector_idx".to_string()),
    ))?);

    let header = ClientCommandHeader {
        request_identifier,
        sector_idx,
    };
    let content = match code {
        MessageCode::Read => ClientRegisterCommandContent::Read,
        MessageCode::Write => ClientRegisterCommandContent::Write {
            data: SectorVec(buff[16..].to_vec()),
        },
        _ => unreachable!(),
    };
    let client_msg = ClientRegisterCommand { header, content };

    Ok((RegisterCommand::Client(client_msg), hmac_valid))
}

async fn deserialize_system_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    mut mac: HmacSha256,
    code: MessageCode,
    process_identifier: u8,
) -> Result<(RegisterCommand, bool), DeserializeError> {
    let mut buff = match code {
        MessageCode::ReadProc | MessageCode::ACK => {
            vec![0u8; 32]
        }
        MessageCode::VALUE | MessageCode::WriteProc => {
            vec![0u8; 32 + 4096]
        }
        _ => unreachable!(),
    };

    data.read_exact(&mut buff)
        .await
        .map_err(|e| DeserializeError::IoError(e))?;
    mac.update(&buff);
    // Check before deserialize to read exact amonut of bytes
    let hmac_valid = check_hmac_valid(mac, data)
        .await
        .map_err(|e| DeserializeError::IoError(e))?;

    let msg_ident = Uuid::from_slice(&buff[0..16]).or(Err(DeserializeError::Other(
        "invalid msg_ident".to_string(),
    )))?;
    let read_ident = u64::from_be_bytes(buff[16..24].try_into().or(Err(
        DeserializeError::Other("invalid read_ident".to_string()),
    ))?);
    let sector_idx = u64::from_be_bytes(buff[24..32].try_into().or(Err(
        DeserializeError::Other("invalid sector_idx".to_string()),
    ))?);

    let header = SystemCommandHeader {
        process_identifier,
        msg_ident,
        read_ident,
        sector_idx,
    };
    let content = match code {
        MessageCode::ReadProc => SystemRegisterCommandContent::ReadProc,
        MessageCode::VALUE => SystemRegisterCommandContent::Value {
            timestamp: u64::from_be_bytes(buff[32..40].try_into().or(Err(
                DeserializeError::Other("invalid timestamp".to_string()),
            ))?),
            write_rank: buff[47],
            sector_data: SectorVec(buff[48..].to_vec()),
        },
        MessageCode::WriteProc => SystemRegisterCommandContent::WriteProc {
            timestamp: u64::from_be_bytes(buff[32..40].try_into().or(Err(
                DeserializeError::Other("invalid timestamp".to_string()),
            ))?),
            write_rank: buff[47],
            data_to_write: SectorVec(buff[48..].to_vec()),
        },
        MessageCode::ACK => SystemRegisterCommandContent::Ack,
        _ => unreachable!(),
    };

    let system_msg = SystemRegisterCommand { header, content };

    Ok((RegisterCommand::System(system_msg), hmac_valid))
}

fn try_to_msg_type(value: u8) -> Option<MessageCode> {
    match value {
        0x01 => Some(MessageCode::Read),
        0x02 => Some(MessageCode::Write),
        0x03 => Some(MessageCode::ReadProc),
        0x04 => Some(MessageCode::VALUE),
        0x05 => Some(MessageCode::WriteProc),
        0x06 => Some(MessageCode::ACK),
        _ => None,
    }
}

async fn check_hmac_valid(
    mat: HmacSha256,
    data: &mut (dyn AsyncRead + Send + Unpin),
) -> Result<bool, std::io::Error> {
    let mut buff = vec![0u8; 32];
    data.read_exact(&mut buff).await?;
    let hmac = mat.finalize().into_bytes().to_vec();
    Ok(hmac == buff)
}

#[derive(Debug)]
enum DeserializeError {
    IoError(std::io::Error),
    Other(String),
}

#[repr(u8)]
#[derive(PartialEq, Eq)]
enum MessageCode {
    Read = 0x01,
    Write,
    ReadProc,
    VALUE,
    WriteProc,
    ACK,
}
