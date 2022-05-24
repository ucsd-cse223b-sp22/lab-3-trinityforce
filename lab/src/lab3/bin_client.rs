use super::bin_replicator_adapter::BinReplicatorAdapter;
use super::client::StorageClient;
use super::constants::SCAN_INTERVAL_CONSTANT;
use super::lock_client::{self, LockClient};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tribbler::err::TribResult;
use tribbler::storage::{BinStorage, KeyString, Storage};

#[derive(Debug, Default)]
pub struct BinStorageClient {
    pub backs: Vec<String>,
    back_status_mut: RwLock<Vec<bool>>,
    last_scan_ts: RwLock<u64>,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    lock_client: Arc<LockClient>,
}

impl BinStorageClient {
    pub fn new(backs: Vec<String>) -> Self {
        let mut back_status = vec![];
        for _ in 0..backs.len() {
            back_status.push(false);
        }
        Self {
            backs: backs.clone(),
            back_status_mut: RwLock::new(back_status),
            last_scan_ts: RwLock::new(0),
            channel_cache: Arc::new(RwLock::new(HashMap::new())),
            lock_client: Arc<LockClient>::new(),
        }
    }

    pub fn new_with_channel(
        backs: &Vec<String>,
        channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    ) -> Self {
        let mut back_status = vec![];
        for _ in 0..backs.len() {
            back_status.push(false);
        }
        Self {
            backs: backs.clone(),
            back_status_mut: RwLock::new(back_status),
            last_scan_ts: RwLock::new(0),
            channel_cache,
            lock_client: Arc<LockClient>::new(),
        }
    }

    pub fn update_lock_client(&mut self, lock_client: Arc<LockClient>) {
        self.lock_client = lock_client;
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
            let chan_res =
                update_channel_cache(self.channel_cache.clone(), self.backs[i].clone()).await;
            if chan_res.is_err() {
                (*back_status)[i] = false;
                continue;
            }
            let client = StorageClient::new(self.backs[i].as_str(), Some(chan_res.unwrap()));
            let get_res = client.get("DUMMY").await;
            if get_res.is_err() {
                (*back_status)[i] = false;
            } else {
                (*back_status)[i] = true;
            }
        }
    }
}

pub(crate) async fn update_channel_cache(
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    back_addr: String,
) -> TribResult<Channel> {
    let back_addr = format!("http://{}", back_addr);
    let channel_cache_read = channel_cache.read().await;
    let res = (*channel_cache_read).get(&back_addr);
    if let Some(chan) = res {
        return Ok(chan.clone());
    }
    drop(channel_cache_read);

    let mut channel_cache_write = channel_cache.write().await;
    let res = (*channel_cache_write).get(&back_addr);
    if let Some(chan) = res {
        Ok(chan.clone())
    } else {
        let chan = Endpoint::from_shared(back_addr.clone())?.connect().await?;
        (*channel_cache_write).insert(back_addr, chan.clone());
        Ok(chan)
    }
}

impl BinStorageClient {
    // This function is provided for keeper to use, so it do not need to scan
    pub fn bin_with_backs(
        &self,
        name: &str,
        backs_status: &Vec<bool>,
    ) -> tribbler::err::TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        let hash_res = hasher.finish();
        let len = backs_status.len() as u64;
        let ind = (hash_res % len) as u32;
        let storage_bin_replicator_adapter = BinReplicatorAdapter::new(
            ind,
            self.backs.clone(),
            name,
            backs_status.clone(),
            self.channel_cache.clone(),
            self.lock_client.clone(),
        );
        // println!("{}", target_back_addr);
        Ok(Box::new(storage_bin_replicator_adapter))
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
        let back_status = self.back_status_mut.read().await;
        let back_status_copy = (*back_status).clone();
        let storage_bin_replicator_adapter = BinReplicatorAdapter::new(
            ind,
            backs.clone(),
            name,
            back_status_copy,
            self.channel_cache.clone(),
            self.lock_client.clone(),
        );
        // println!("{}", target_back_addr);
        Ok(Box::new(storage_bin_replicator_adapter))
    }
}
