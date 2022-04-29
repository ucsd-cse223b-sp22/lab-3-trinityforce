use super::bin_prefix_adapter::BinPrefixAdapter;
use super::client::StorageClient;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use tribbler::storage::{BinStorage, Storage};

#[derive(Debug, Default)]
pub struct BinStorageClient {
    pub backs: RwLock<Vec<String>>,
}

impl BinStorageClient {
    pub fn new(backs: Vec<String>) -> Self {
        Self {
            backs: RwLock::new(backs.clone()),
        }
    }
}

use async_trait::async_trait;
#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> tribbler::err::TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        let hash_res = hasher.finish();
        let backs = self.backs.read().map_err(|e| e.to_string())?;
        let len = backs.len() as u64;
        let ind = (hash_res % len) as usize;
        let target_back_addr = backs[ind].as_str();
        let storage_bin_prefix_adapter = BinPrefixAdapter::new(target_back_addr, name);
        // println!("{}", target_back_addr);
        Ok(Box::new(storage_bin_prefix_adapter))
    }
}
