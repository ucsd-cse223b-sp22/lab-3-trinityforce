use std::collections::HashMap;
use std::sync::Arc;

use super::bin_client::update_channel_cache;
use super::client::StorageClient;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tribbler::err::TribResult;
use tribbler::storage;

pub struct BinPrefixAdapter {
    pub addr: String,
    pub bin: String,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
}

impl BinPrefixAdapter {
    pub fn new(
        addr: &str,
        bin: &str,
        channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    ) -> Self {
        Self {
            addr: addr.to_string(),
            bin: bin.to_string(),
            channel_cache: channel_cache.clone(),
        }
    }
}

use async_trait::async_trait;
#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyString for BinPrefixAdapter {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_key = format!("{}::{}", self.bin, key);
        return storage_client.get(wrapped_key.as_str()).await;
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_key = format!("{}::{}", self.bin, kv.key);
        return storage_client
            .set(&storage::KeyValue {
                key: wrapped_key.to_string(),
                value: kv.value.to_string(),
            })
            .await;
    }

    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_prefix = format!("{}::{}", self.bin, p.prefix);
        let resp_list = storage_client
            .keys(&storage::Pattern {
                prefix: wrapped_prefix,
                suffix: p.suffix.to_string(),
            })
            .await?
            .0;
        let mut ret_val = vec![];
        for element in resp_list {
            let element_str = element.as_str();
            let extracted_key = &element_str[self.bin.len() + 2..element.len()];
            if extracted_key.ends_with(&p.suffix) {
                ret_val.push(extracted_key.to_string());
            }
        }
        return Ok(storage::List(ret_val));
    }
}

#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyList for BinPrefixAdapter {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_key = format!("{}::{}", self.bin, key);
        return storage_client.list_get(wrapped_key.as_str()).await;
    }

    async fn list_set(&self, kl: &storage::KeyValueList) -> TribResult<bool> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_key = format!("{}::{}", self.bin, kl.key);
        return storage_client
            .list_set(&storage::KeyValueList {
                key: wrapped_key.to_string(),
                list: kl.list.clone(),
            })
            .await;
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_key = format!("{}::{}", self.bin, kv.key);
        return storage_client
            .list_append(&storage::KeyValue {
                key: wrapped_key.to_string(),
                value: kv.value.to_string(),
            })
            .await;
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_key = format!("{}::{}", self.bin, kv.key);
        return storage_client
            .list_remove(&storage::KeyValue {
                key: wrapped_key,
                value: kv.value.to_string(),
            })
            .await;
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        let wrapped_prefix = format!("{}::{}", self.bin, p.prefix);
        let resp_list = storage_client
            .list_keys(&storage::Pattern {
                prefix: wrapped_prefix,
                suffix: p.suffix.to_string(),
            })
            .await?
            .0;
        let mut ret_val = vec![];
        for element in resp_list {
            let element_str = element.as_str();
            let extracted_key = &element_str[self.bin.len() + 2..];
            if extracted_key.ends_with(&p.suffix) {
                ret_val.push(extracted_key.to_string());
            }
        }
        return Ok(storage::List(ret_val));
    }
}

#[async_trait]
impl storage::Storage for BinPrefixAdapter {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.addr.clone()).await?;
        let storage_client = StorageClient::new(&self.addr, Some(chan));
        return storage_client.clock(at_least).await;
    }
}
