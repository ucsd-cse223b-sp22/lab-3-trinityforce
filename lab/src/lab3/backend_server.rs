use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tribbler::rpc;
use tribbler::rpc::trib_storage_server::TribStorage;
use tribbler::storage;
use tribbler::storage::Storage;

pub struct BackendServer {
    addr: String,
    store: Box<dyn Storage>,
}

impl BackendServer {
    pub fn new(addr: String, store_param: Box<dyn Storage>) -> Self {
        Self {
            addr,
            store: store_param,
        }
    }
}

#[async_trait] // VERY IMPORTANT !!!=
impl TribStorage for BackendServer {
    async fn get(
        &self,
        request: Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::Value>, tonic::Status> {
        let key_cpy = request.into_inner().key.to_string();
        let key_str = key_cpy.as_str();
        // println!("server getting {}", key_cpy);
        let option_value = match self.store.get(&key_str).await {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("key error"));
            }
        };
        let value = match option_value {
            Some(res) => res,
            None => {
                return Err(Status::unavailable("key error"));
            }
        };
        let msg_body = rpc::Value { value: value };
        Ok(Response::new(msg_body))
    }

    async fn set(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let request_inner = request.into_inner();
        let key = request_inner.key.to_string();
        let value = request_inner.value.to_string();
        // println!("backend server {} setting {}, {}", self.addr, key, value);

        let flag = match self
            .store
            .set(&storage::KeyValue {
                key: key,
                value: value,
            })
            .await
        {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("set error"));
            }
        };
        let msg_body = rpc::Bool { value: flag };
        Ok(Response::new(msg_body))
    }

    async fn keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let request_inner = request.into_inner();
        let prefix = request_inner.prefix.to_string();
        let suffix = request_inner.suffix.to_string();
        let list = match self
            .store
            .keys(&storage::Pattern {
                prefix: prefix,
                suffix: suffix,
            })
            .await
        {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("keys retrieve error"));
            }
        };
        let storage::List(vec) = list;
        let msg_body = rpc::StringList { list: vec };
        Ok(Response::new(msg_body))
    }

    async fn list_get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let request_inner = request.into_inner();
        let key_cpy = request_inner.key.to_string();
        let key_str = key_cpy.as_str();
        let list = match self.store.list_get(key_str).await {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("list get error"));
            }
        };
        let storage::List(vec) = list;
        // println!("server getting {:?}", vec);
        let msg_body = rpc::StringList { list: vec };
        Ok(Response::new(msg_body))
    }

    async fn list_set(
        &self,
        request: tonic::Request<rpc::KeyValueList>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let request_inner = request.into_inner();
        let key = request_inner.key.to_string();
        let list_val = request_inner.list.clone();
        let flag = match self
            .store
            .list_set(&storage::KeyValueList {
                key: key,
                list: list_val,
            })
            .await
        {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("list set error"));
            }
        };
        let msg_body = rpc::Bool { value: flag };
        Ok(Response::new(msg_body))
    }

    async fn list_append(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let request_inner = request.into_inner();
        let key = request_inner.key.to_string();
        let value = request_inner.value.to_string();
        let flag = match self
            .store
            .list_append(&storage::KeyValue {
                key: key,
                value: value,
            })
            .await
        {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("list append error"));
            }
        };
        let msg_body = rpc::Bool { value: flag };
        Ok(Response::new(msg_body))
    }

    async fn list_remove(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::ListRemoveResponse>, tonic::Status> {
        let request_inner = request.into_inner();
        let key = request_inner.key.to_string();
        let value = request_inner.value.to_string();
        let flag = match self
            .store
            .list_remove(&storage::KeyValue {
                key: key,
                value: value,
            })
            .await
        {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("list remove error"));
            }
        };
        let msg_body = rpc::ListRemoveResponse { removed: flag };
        Ok(Response::new(msg_body))
    }

    async fn list_keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let request_inner = request.into_inner();
        let prefix = request_inner.prefix.to_string();
        let suffix = request_inner.suffix.to_string();
        let list = match self
            .store
            .list_keys(&storage::Pattern {
                prefix: prefix,
                suffix: suffix,
            })
            .await
        {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("list keys error"));
            }
        };
        let storage::List(vec) = list;
        let msg_body = rpc::StringList { list: vec };
        Ok(Response::new(msg_body))
    }

    async fn clock(
        &self,
        request: tonic::Request<rpc::Clock>,
    ) -> Result<tonic::Response<rpc::Clock>, tonic::Status> {
        let request_inner = request.into_inner();
        let timestamp = request_inner.timestamp;
        let ret_ts = match self.store.clock(timestamp).await {
            Ok(res) => res,
            Err(_) => {
                return Err(Status::unavailable("clock error"));
            }
        };
        // println!(
        //     "{} server clock inc to: {} in request of {}",
        //     self.addr, ret_ts, timestamp
        // );
        let msg_body = rpc::Clock { timestamp: ret_ts };
        Ok(Response::new(msg_body))
    }
}
