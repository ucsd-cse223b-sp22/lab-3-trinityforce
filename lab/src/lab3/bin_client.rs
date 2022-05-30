use crate::lockserver::PingRequest;

use super::super::lockserver::lock_service_client::LockServiceClient;
use super::bin_replicator_adapter::BinReplicatorAdapter;
use super::client::StorageClient;
use super::constants::{
    DEFAULT_LOCK_SERVERS_STARTING_PORT, DEFAULT_NUM_LOCK_SERVERS, LIST_LOG_KEYWORD,
    LOCK_SERVERS_STARTING_PORT_KEY, NUM_LOCK_SERVERS_KEY, SCAN_INTERVAL_CONSTANT, STR_LOG_KEYWORD,
    TRANS_LOG_LIST_PREFIX, TRANS_LOG_STR_PREFIX,
};
use super::lock_client::{self, LockClient};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tribbler::err::TribResult;
use tribbler::storage::{BinStorage, KeyString, KeyValue, Storage};
extern crate dotenv;
use dotenv::dotenv;
use std::env;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct transaction_log {
    transaction_id: String,
    transaction_key: String,
    old_value: Vec<String>,
}

pub struct transaction_client {
    transaction_id: String,
    transaction_num: RwLock<u64>,
    lock_client: Arc<LockClient>,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    bin_storage: Arc<BinStorageClient>,
}

pub fn split_string(s: String) -> (String, String) {
    let splits = s.split("::").collect::<Vec<&str>>();
    if splits.len() <= 2 {
        panic!("Split Error");
    }
    return (splits[0].to_string(), splits[1].to_string());
}

impl transaction_client {
    pub fn new(
        lock_client: Arc<LockClient>,
        transaction_id: String,
        channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
        bin_storage: Arc<BinStorageClient>,
    ) -> Self {
        Self {
            transaction_id,
            transaction_num: RwLock::<u64>::new(0),
            lock_client,
            channel_cache,
            bin_storage,
        }
    }

    pub async fn transaction_start(
        &mut self,
        read_keys_map: HashMap<String, Vec<String>>,
        write_keys_map: HashMap<String, Vec<String>>,
    ) -> TribResult<String> {
        let mut write_keys = vec![];
        let mut read_keys = vec![];
        for (bin, key_list) in write_keys_map.iter() {
            for key in key_list.iter() {
                write_keys.push(format!("{}::{}", bin, key));
            }
        }
        for (bin, key_list) in read_keys_map.iter() {
            for key in key_list.iter() {
                read_keys.push(format!("{}::{}", bin, key))
            }
        }
        self.lock_client
            .acquire_locks(read_keys, write_keys)
            .await?;
        let mut trans_num = self.transaction_num.write().await;
        let trans_key = trans_num.to_string();
        (*trans_num) = (*trans_num) + 1;
        drop(trans_num);
        for (bin, key_list) in write_keys_map.iter() {
            let client = self.bin_storage.bin_with_locks(bin).await?;
            for raw_key in key_list.iter() {
                let (prefix, key) = split_string(raw_key.to_string());
                let mut trans_log = transaction_log {
                    transaction_id: self.transaction_id.to_string(),
                    transaction_key: trans_key.to_string(),
                    old_value: vec![],
                };
                if prefix == STR_LOG_KEYWORD {
                    let old_value = client.get(&key).await?;
                    if old_value.is_none() {
                        trans_log.old_value.push("".to_string());
                        client
                            .set(&KeyValue {
                                key: format!("{}{}", TRANS_LOG_STR_PREFIX, key),
                                value: serde_json::to_string(&trans_log)?,
                            })
                            .await?;
                    } else {
                        trans_log.old_value.push(old_value.unwrap());
                        client
                            .set(&KeyValue {
                                key: format!("{}{}", TRANS_LOG_STR_PREFIX, key),
                                value: serde_json::to_string(&trans_log)?,
                            })
                            .await?;
                    }
                } else if prefix == LIST_LOG_KEYWORD {
                    let mut old_value = client.list_get(&key).await?.0;
                    trans_log.old_value.append(&mut old_value);
                    client
                        .set(&KeyValue {
                            key: format!("{}{}", TRANS_LOG_LIST_PREFIX, key),
                            value: serde_json::to_string(&trans_log)?,
                        })
                        .await?;
                }
            }
        }
        Ok(trans_key)
    }

    pub async fn transaction_end(
        &self,
        trans_key: String,
        read_keys_map: HashMap<String, Vec<String>>,
        write_keys_map: HashMap<String, Vec<String>>,
    ) -> TribResult<()> {
        let mut write_keys = vec![];
        let mut read_keys = vec![];
        for (bin, key_list) in write_keys_map.iter() {
            for key in key_list.iter() {
                write_keys.push(format!("{}::{}", bin, key));
            }
        }
        for (bin, key_list) in read_keys_map.iter() {
            for key in key_list.iter() {
                read_keys.push(format!("{}::{}", bin, key))
            }
        }
        let client = self.bin_storage.bin(&self.transaction_id).await?;
        client
            .set(&KeyValue {
                key: trans_key.to_string(),
                value: "True".to_string(),
            })
            .await?;
        self.lock_client
            .release_locks(read_keys, write_keys)
            .await?;
        Ok(())
    }
}

pub(crate) fn init_lock_servers_addresses() -> Vec<String> {
    dotenv::from_filename("config.env").ok();
    let mut start_port = DEFAULT_LOCK_SERVERS_STARTING_PORT;
    let mut num_servers = DEFAULT_NUM_LOCK_SERVERS;
    for (key, value) in env::vars() {
        if key == LOCK_SERVERS_STARTING_PORT_KEY {
            start_port = value.parse::<usize>().unwrap();
        } else if key == NUM_LOCK_SERVERS_KEY {
            num_servers = value.parse::<usize>().unwrap();
        }
    }
    let mut lock_addrs = vec![];
    for i in 0..num_servers {
        let lock_addr = format!("127.0.0.1:{}", start_port + i);
        lock_addrs.push(lock_addr);
    }
    return lock_addrs;
}

#[derive(Debug, Default)]
pub struct LockServerPinger {
    lock_addrs: Vec<String>,
}

impl LockServerPinger {
    pub fn new() -> Self {
        let lock_addrs = init_lock_servers_addresses();
        Self {
            lock_addrs: lock_addrs.clone(),
        }
    }

    pub async fn ping_test(&self) -> TribResult<()> {
        for addr in self.lock_addrs.clone() {
            let chan = Endpoint::from_shared(format!("http://{}", addr))?
                .connect()
                .await?;
            let mut client = LockServiceClient::new(chan);
            let response = client
                .ping(PingRequest {
                    client_id: "bin_client".to_string(),
                })
                .await?;
            if !response.into_inner().flag {
                panic!("ping returns failure");
            }
        }
        Ok(())
    }
}

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
        let lock_addrs = init_lock_servers_addresses();
        Self {
            backs: backs.clone(),
            back_status_mut: RwLock::new(back_status),
            last_scan_ts: RwLock::new(0),
            channel_cache: Arc::new(RwLock::new(HashMap::new())),
            lock_client: Arc::new(LockClient::new(lock_addrs, false)),
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
        let lock_addrs = init_lock_servers_addresses();
        Self {
            backs: backs.clone(),
            back_status_mut: RwLock::new(back_status),
            last_scan_ts: RwLock::new(0),
            channel_cache,
            lock_client: Arc::new(LockClient::new(lock_addrs, false)),
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
    pub async fn bin_with_locks(&self, name: &str) -> tribbler::err::TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        self.scan_backs_status().await;
        name.hash(&mut hasher);
        let hash_res = hasher.finish();
        let backs = self.backs.clone();
        let len = backs.len() as u64;
        let ind = (hash_res % len) as u32;
        let back_status = self.back_status_mut.read().await;
        let back_status_copy = (*back_status).clone();
        let mut storage_bin_replicator_adapter = BinReplicatorAdapter::new(
            ind,
            backs.clone(),
            name,
            back_status_copy,
            self.channel_cache.clone(),
            self.lock_client.clone(),
        );
        storage_bin_replicator_adapter.with_lock = true;
        // println!("{}", target_back_addr);
        Ok(Box::new(storage_bin_replicator_adapter))
    }
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
