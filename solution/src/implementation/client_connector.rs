use crate::RegisterClient;


pub struct ClientConnector;

impl RegisterClient for ClientConnector {
    fn send<'life0,'async_trait>(&'life0 self,msg:crate::Send) ->  core::pin::Pin<Box<dyn core::future::Future<Output = ()> + core::marker::Send+'async_trait> >where 'life0:'async_trait,Self:'async_trait {
        unimplemented!();
    }

    fn broadcast<'life0,'async_trait>(&'life0 self,msg:crate::Broadcast) ->  core::pin::Pin<Box<dyn core::future::Future<Output = ()> + core::marker::Send+'async_trait> >where 'life0:'async_trait,Self:'async_trait {
        unimplemented!();
    }
}