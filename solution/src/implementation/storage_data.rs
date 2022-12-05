use std::path::PathBuf;

use sha2::{Sha256, Digest};
use tokio::io::AsyncWriteExt;

use crate::StableStorage;

struct StorageData {
    root_storage_dir: PathBuf,
}

fn get_key_hash(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    format!("{:X}", hasher.finalize())
}

#[async_trait::async_trait]
impl StableStorage for StorageData {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.bytes().len() > 255 {
            return Err("Maximum key size is 255 bytes".to_string())
        }
        
        if value.len() > 65535 {
            return Err("Maximum value size is 65535 bytes".to_string())
        }

        let hash = get_key_hash(key);
        let tmp_filepath = self.root_storage_dir.join(format!("tmp-{}", hash));

        let mut tmp_file = tokio::fs::File::create(tmp_filepath.clone())
            .await
            .unwrap();
        tmp_file
            .write_all(value)
            .await
            .unwrap();
        tmp_file
            .sync_data()
            .await
            .unwrap();
        std::mem::drop(tmp_file);
        match tokio::fs::rename(
            tmp_filepath,
            self.root_storage_dir.join(hash)
        )
            .await {
                Ok(_) => {
                    let dstdir = tokio::fs::File::open(&self.root_storage_dir).await.unwrap();
                    tokio::fs::File::sync_data(&dstdir).await.unwrap();
                },
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
                _ => panic!(),
            }
        Ok(())
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let hash = get_key_hash(key);
        match tokio::fs::read(self.root_storage_dir.join(hash)).await {
            Ok(value) => Some(value),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            _ => panic!(),
        }
    }

    async fn remove(&mut self, key: &str) -> bool {
        let hash = get_key_hash(key);
        match tokio::fs::remove_file(self.root_storage_dir.join(hash)).await {
            Ok(_) => {
                let dstdir = tokio::fs::File::open(self.root_storage_dir.clone()).await.unwrap();
                tokio::fs::File::sync_data(&dstdir).await.unwrap();
                true
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            _ => panic!(),
        }
    }
}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    Box::new(StorageData {
        root_storage_dir,
    })
}
