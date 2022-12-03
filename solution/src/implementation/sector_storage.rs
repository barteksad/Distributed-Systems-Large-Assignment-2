use crate::{SectorsManager, SectorIdx, SectorVec};


pub struct SectorStorage;

#[async_trait::async_trait]
impl SectorsManager for SectorStorage {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        unimplemented!();
    }

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8){
        unimplemented!();
    }

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)){
        unimplemented!();
    }

}