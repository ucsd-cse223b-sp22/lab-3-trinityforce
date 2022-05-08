use tonic::transport::Channel;
use tribbler::rpc::trib_storage_client::TribStorageClient;
#[derive(Debug, Default)]
pub struct StorageClient {
    pub addr: String,
    pub chan: Option<Channel>,
}

// use tonic::transport::Endpoint;
use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::storage;

impl StorageClient {
    pub fn new(addr: &str, chan: Option<Channel>) -> Self {
        Self {
            addr: addr.to_string(),
            chan: chan.clone(),
        }
    }

    async fn connect(&self) -> Result<TribStorageClient<Channel>, tonic::transport::Error> {
        let formatted_addr = format!("http://{}", self.addr);
        let client = TribStorageClient::connect(formatted_addr).await?;
        return Ok(client);
    }
}

use async_trait::async_trait;
#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .get(rpc::Key {
                key: key.to_string(),
            })
            .await;
        match r {
            Ok(resp) => Ok(Some(resp.into_inner().value)),
            Err(e) => {
                if e.message() == "key error" {
                    return Ok(None);
                }
                return Err(Box::new(e));
            }
        }
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .set(rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match r.into_inner().value {
            value => Ok(value),
        }
    }

    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .keys(rpc::Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        let list = r.into_inner().list;
        return Ok(storage::List(list));
    }
}

#[async_trait]
impl storage::KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .list_get(rpc::Key {
                key: key.to_string(),
            })
            .await?;
        let list = r.into_inner().list;
        return Ok(storage::List(list));
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .list_append(rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match r.into_inner().value {
            value => Ok(value),
        }
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .list_remove(rpc::KeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        match r.into_inner().removed {
            value => Ok(value),
        }
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .list_keys(rpc::Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;
        let list = r.into_inner().list;
        return Ok(storage::List(list));
    }
}

#[async_trait]
impl storage::Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let mut client = match &self.chan {
            Some(chan) => TribStorageClient::new(chan.clone()),
            None => self.connect().await?,
        };

        let r = client
            .clock(rpc::Clock {
                timestamp: at_least,
            })
            .await?;
        match r.into_inner().timestamp {
            value => Ok(value),
        }
    }
}
