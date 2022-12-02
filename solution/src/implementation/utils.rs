use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::{net::TcpStream, io::AsyncWriteExt};

type HmacSha256 = Hmac<Sha256>;

pub fn add_hmac_tag(header_buff: &Vec<u8>, content_buff: &Vec<u8>, hmac_key: &[u8; 32]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(header_buff);
    mac.update(content_buff);
    mac.finalize().into_bytes().to_vec()
}

pub async fn stubborn_send(mut socket: TcpStream, data: &[u8])  {
    socket.write_all(data).await;
} 