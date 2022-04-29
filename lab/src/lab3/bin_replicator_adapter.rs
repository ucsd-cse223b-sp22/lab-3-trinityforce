use super::client::StorageClient;
use tribbler::err::TribResult;
use tribbler::storage;

pub struct BinReplicatorAdapter {
    pub hash_index: u32,
    pub backs: Vec<String>,
    pub bin: String,
}

impl BinReplicatorAdapter {
    pub fn new(addr: &str, bin: &str) -> Self {
        Self {
            addr: addr.to_string(),
            bin: bin.to_string(),
        }
    }
}

use async_trait::async_trait;
#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyString for BinReplicatorAdapter {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        todo!();
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        todo!();
    }

    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        todo!();
    }
}

#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyList for BinReplicatorAdapter {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        todo!();
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        todo!();
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        todo!();
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        todo!();
    }
}

#[async_trait]
impl storage::Storage for BinReplicatorAdapter {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        todo!();
    }
}
