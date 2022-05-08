use super::super::keeper;
use crate::keeper::keeper_service_server::KeeperService;
use async_trait::async_trait;
use tonic::Response;

pub struct KeeperRPCReceiver {}

#[async_trait] // VERY IMPORTANT !!!=
impl KeeperService for KeeperRPCReceiver {
    async fn ping(
        &self,
        request: tonic::Request<crate::keeper::Heartbeat>,
    ) -> Result<tonic::Response<crate::keeper::HeartbeatResponse>, tonic::Status> {
        let msg_body = keeper::HeartbeatResponse { value: true };
        Ok(Response::new(msg_body))
    }
}

impl KeeperRPCReceiver {
    pub fn new() -> Self {
        Self {}
    }
}
