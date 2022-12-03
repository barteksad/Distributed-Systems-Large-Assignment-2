use crate::RegisterClient;

use crate::register_client_public::{Send, Broadcast};
pub struct ClientConnector;

#[async_trait::async_trait]
impl RegisterClient for ClientConnector {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send) {

    }

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast) {
        
    }
}