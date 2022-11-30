use crate::SectorsManager;


pub struct SectorStorage;

impl SectorsManager for SectorStorage {
    fn read_data<'life0,'async_trait>(&'life0 self,idx:crate::SectorIdx) ->  core::pin::Pin<Box<dyn core::future::Future<Output = crate::SectorVec> + core::marker::Send+'async_trait> >where 'life0:'async_trait,Self:'async_trait {
        unimplemented!();
    }

    fn read_metadata<'life0,'async_trait>(&'life0 self,idx:crate::SectorIdx) ->  core::pin::Pin<Box<dyn core::future::Future<Output = (u64,u8)> + core::marker::Send+'async_trait> >where 'life0:'async_trait,Self:'async_trait {
        unimplemented!();
    }

    fn write<'life0,'life1,'async_trait>(&'life0 self,idx:crate::SectorIdx,sector: &'life1(crate::SectorVec,u64,u8)) ->  core::pin::Pin<Box<dyn core::future::Future<Output = ()> + core::marker::Send+'async_trait> >where 'life0:'async_trait,'life1:'async_trait,Self:'async_trait {
        unimplemented!();
    }
}