use std::{time::Duration, io};

use hmac::{Hmac, Mac};
use log::debug;
use sha2::Sha256;
use tokio::{net::TcpStream, time::{Instant, sleep}};

type HmacSha256 = Hmac<Sha256>;

static N_SEND_TRIES: usize = 5;
static NEXT_SEND_DELAY: Duration = Duration::from_millis(1000); 

pub fn add_hmac_tag(header_buff: &Vec<u8>, content_buff: &Vec<u8>, hmac_key: &[u8; 32]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(header_buff);
    mac.update(content_buff);
    mac.finalize().into_bytes().to_vec()
}

pub async fn stubborn_send(socket: &TcpStream, data: &[u8]) -> bool  {
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
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
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