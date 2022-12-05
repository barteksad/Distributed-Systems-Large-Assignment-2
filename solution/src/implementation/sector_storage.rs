use std::{collections::HashMap, path::PathBuf, sync::Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{SectorIdx, SectorVec, SectorsManager};

pub struct SectorStorage {
    path: PathBuf,
    metadata: Mutex<HashMap<SectorIdx, (u64, u8)>>,
}

impl SectorStorage {
    pub async fn new(path: PathBuf) -> Self {
        let mut metadata = HashMap::new();
        let mut dir = tokio::fs::read_dir(&path)
            .await
            .expect("SectorStorage folder doesnt exists!");
        while let Ok(Some(file)) = dir.next_entry().await {
            let file_name = file.file_name();
            let file_name = file_name.to_str().expect("Invalid file name");
            let file_name = file_name.split('.').collect::<Vec<&str>>();
            let sector_idx = file_name[0]
                .parse::<SectorIdx>()
                .expect("Invalid sector_idx");
            let timestamp = file_name[1].parse::<u64>().expect("Invalid timestamp");
            let write_rank = file_name[2].parse::<u8>().expect("Invalid write_rank");
            metadata.insert(sector_idx, (timestamp, write_rank));
        }

        SectorStorage {
            path,
            metadata: Mutex::new(metadata),
        }
    }
}

#[async_trait::async_trait]
impl SectorsManager for SectorStorage {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let mut maybe_file_path = None;
        {
            let lock = self.metadata.lock().unwrap();
            let sector_data = lock.get(&idx);
            maybe_file_path = match sector_data {
                Some((timestamp, wr)) => {
                    Some(self.path.join(format!("{}.{}.{}", idx, *timestamp, *wr)))
                }
                None => None,
            };
        }

        match maybe_file_path {
            Some(file_path) => {
                let mut file = tokio::fs::File::open(&file_path)
                    .await
                    .expect("File doesnt exists!");
                let mut data = vec![0; 4096];
                file.read_exact(&mut data)
                    .await
                    .expect("File is too small!");
                SectorVec(data)
            }
            None => SectorVec(vec![0; 4096]),
        }
    }

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        if let Some((timestamp, wr)) = self.metadata.lock().unwrap().get(&idx) {
            (*timestamp, *wr)
        } else {
            (0, 0)
        }
    }

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let (data, timestamp, wr) = sector;
        let mut maybe_file_path = None;
        {
            let lock = self.metadata.lock().unwrap();
            let sector_data = lock.get(&idx);
            maybe_file_path = match sector_data {
                Some((timestamp, wr)) => {
                    Some(self.path.join(format!("{}.{}.{}", idx, *timestamp, *wr)))
                }
                None => None,
            };
        }

        if let Some(file_path) = maybe_file_path {
            tokio::fs::remove_file(&file_path)
                .await
                .expect("File doesnt exists!");
            let dstdir = tokio::fs::File::open(&self.path).await.unwrap();
            tokio::fs::File::sync_data(&dstdir)
                .await
                .expect("Error syncing data!");
        }

        let file_name = self.path.join(format!("{}.{}.{}", idx, timestamp, wr));
        let tmp_file_name = self.path.join(format!("tmp.{}.{}.{}", idx, timestamp, wr));
        let mut file = tokio::fs::File::create(&tmp_file_name)
            .await
            .expect("File doesnt exists!");
        file.write_all(&data.0)
            .await
            .expect("Error writing to file!");
        file.sync_data().await.expect("Error writing to file!");
        std::mem::drop(file);
        match tokio::fs::rename(tmp_file_name, file_name).await {
            Ok(_) => {
                let dstdir = tokio::fs::File::open(&self.path).await.unwrap();
                tokio::fs::File::sync_data(&dstdir)
                    .await
                    .expect("Error syncing data!");
            }
            Err(e) => {
                println!("Error renaming file: {}", e);
            }
        }
        self.metadata.lock().unwrap().insert(idx, (*timestamp, *wr));
    }
}
