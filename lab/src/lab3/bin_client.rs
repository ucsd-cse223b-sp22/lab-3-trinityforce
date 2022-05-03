use super::bin_replicator_adapter::BinReplicatorAdapter;
use super::client::StorageClient;
use super::constants::SCAN_INTERVAL_CONSTANT;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tribbler::err::TribResult;
use tribbler::storage::{BinStorage, KeyString, Storage};

#[derive(Debug, Default)]
pub struct BinStorageClient {
    pub backs: Vec<String>,
    back_status_mut: RwLock<Vec<bool>>,
    last_scan_ts: RwLock<u64>,
}

impl BinStorageClient {
    pub fn new(backs: Vec<String>) -> Self {
        let mut back_status = vec![];
        for i in 0..backs.len() {
            back_status.push(false);
        }
        Self {
            backs: backs.clone(),
            back_status_mut: RwLock::new(back_status),
            last_scan_ts: RwLock::new(0),
        }
    }
}

use async_trait::async_trait;
#[async_trait]
pub trait BackStatusScanner {
    async fn scan_backs_status(&self);
}

#[async_trait]
impl BackStatusScanner for BinStorageClient {
    async fn scan_backs_status(&self) {
        let mut cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let ts_reader = self.last_scan_ts.read().await;
        if *ts_reader + SCAN_INTERVAL_CONSTANT > cur_time {
            return;
        }
        drop(ts_reader);
        let ts_writer = self.last_scan_ts.write().await;
        cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if *ts_writer + SCAN_INTERVAL_CONSTANT > cur_time {
            return;
        }
        // scan + update
        let mut back_status = self.back_status_mut.write().await;
        for i in 0..self.backs.len() {
            let client = StorageClient::new(self.backs[i].as_str());
            let get_res = client.get("DUMMY").await;
            if get_res.is_err() {
                (*back_status)[i] = false;
            } else {
                (*back_status)[i] = true;
            }
        }
    }
}

#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> tribbler::err::TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        self.scan_backs_status().await;
        name.hash(&mut hasher);
        let hash_res = hasher.finish();
        let backs = self.backs.clone();
        let len = backs.len() as u64;
        let ind = (hash_res % len) as u32;
        let mut back_status = self.back_status_mut.read().await;
        let back_status_copy = (*back_status).clone();
        let storage_bin_prefix_adapter =
            BinReplicatorAdapter::new(ind, backs.clone(), name, back_status_copy);
        // println!("{}", target_back_addr);
        Ok(Box::new(storage_bin_prefix_adapter))
    }
}
