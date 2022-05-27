use crate::lockserver::AcquireLocksInfo;
use crate::lockserver::ReleaseLocksInfo;

use super::super::lockserver::lock_service_client::LockServiceClient;
use super::bin_client::update_channel_cache;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct LockClient {
    client_id: String,
    read_held_cache: RwLock<HashSet<String>>,
    write_held_cache: RwLock<HashSet<String>>,
    locks_addrs: Vec<String>,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    is_keeper: bool,
}

// use tonic::transport::Endpoint;
use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::storage;

impl LockClient {
    pub fn new(locks_addrs: Vec<String>, is_keeper: bool) -> Self {
        let uuid = Uuid::new_v4();
        let mut keeper_prefix = "";
        if is_keeper {
            keeper_prefix = "keeper-";
        }
        Self {
            client_id: format!("{}{}", keeper_prefix, uuid),
            read_held_cache: RwLock::new(HashSet::new()),
            write_held_cache: RwLock::new(HashSet::new()),
            channel_cache: Arc::new(RwLock::new(HashMap::new())),
            locks_addrs: locks_addrs.clone(),
            is_keeper: is_keeper,
        }
    }

    fn get_hash_index(&self, key: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_res = hasher.finish();
        let len = self.locks_addrs.len() as u64;
        let ind = (hash_res % len) as u32;
        return ind;
    }

    async fn acquire_locks_with_server_index(
        &self,
        ind: usize,
        read_keys: Vec<String>,
        write_keys: Vec<String>,
    ) -> TribResult<()> {
        let chan_res =
            update_channel_cache(self.channel_cache.clone(), self.locks_addrs[ind].clone()).await;
        if chan_res.is_err() {
            panic!("Connection failed");
        }
        let mut client = LockServiceClient::new(chan_res.unwrap());
        println!("r {:?} w {:?}", read_keys, write_keys);
        client
            .acquire(AcquireLocksInfo {
                client_id: self.client_id.to_string(),
                read_keys,
                write_keys,
                is_keeper: self.is_keeper,
            })
            .await?;
        Ok(())
    }

    async fn release_locks_with_server_index(
        &self,
        ind: usize,
        read_keys: Vec<String>,
        write_keys: Vec<String>,
    ) -> TribResult<()> {
        let chan_res =
            update_channel_cache(self.channel_cache.clone(), self.locks_addrs[ind].clone()).await;
        if chan_res.is_err() {
            panic!("Connection failed");
        }
        let mut client = LockServiceClient::new(chan_res.unwrap());
        client
            .release(ReleaseLocksInfo {
                client_id: self.client_id.to_string(),
                read_keys,
                write_keys,
            })
            .await?;
        Ok(())
    }

    pub async fn acquire_locks(
        &self,
        read_keys: Vec<String>,
        write_keys: Vec<String>,
    ) -> TribResult<()> {
        let client_id = format!("{}", self.client_id);

        let read_held_cache = self.read_held_cache.read().await;
        let write_held_cache = self.write_held_cache.read().await;

        let mut read_bins: HashMap<usize, Vec<String>> = HashMap::new();
        let mut write_bins: HashMap<usize, Vec<String>> = HashMap::new();
        let mut bins: HashSet<usize> = HashSet::new();
        for key in read_keys.iter() {
            if !read_held_cache.contains(key) {
                let ind = self.get_hash_index(key) as usize;
                if !read_bins.contains_key(&ind) {
                    read_bins.insert(ind, vec![]);
                }
                bins.insert(ind);
                read_bins.get_mut(&ind).unwrap().push(key.to_string());
            }
        }
        for key in write_keys.iter() {
            if !write_held_cache.contains(key) {
                let ind = self.get_hash_index(key) as usize;
                if !write_bins.contains_key(&ind) {
                    write_bins.insert(ind, vec![]);
                }
                bins.insert(ind);
                write_bins.get_mut(&ind).unwrap().push(key.to_string());
            }
        }
        drop(read_held_cache);
        drop(write_held_cache);
        for ind in bins {
            let mut purified_read_keys = vec![];
            let mut purified_write_keys = vec![];
            let read_keys_option = read_bins.get(&ind);
            if read_keys_option.is_some() {
                purified_read_keys = read_keys_option.unwrap().clone();
            }
            let write_keys_option = write_bins.get(&ind);
            if write_keys_option.is_some() {
                purified_write_keys = write_keys_option.unwrap().clone();
            }
            self.acquire_locks_with_server_index(ind, purified_read_keys, purified_write_keys)
                .await?;
        }
        let mut read_held_cache = self.read_held_cache.write().await;
        let mut write_held_cache = self.write_held_cache.write().await;

        for key in read_keys.iter() {
            // println!("read_held_cache trying to insert key: {}", key);
            if !read_held_cache.contains(key) {
                //println!("read_held_cache inserting key: {}", key);
                read_held_cache.insert(key.to_string());
            }
        }
        for key in write_keys.iter() {
            // println!("write_held_cache trying to insert key: {}", key);
            if !write_held_cache.contains(key) {
                // println!("write_held_cache inserting key: {}", key);
                write_held_cache.insert(key.to_string());
            }
        }
        Ok(())
    }

    async fn release_all_locks(&self) -> TribResult<()> {
        let read_held_cache = self.read_held_cache.read().await;
        let write_held_cache = self.write_held_cache.read().await;
        let mut read_keys = vec![];
        let mut write_keys = vec![];
        for key in read_held_cache.iter() {
            read_keys.push(key.to_string());
        }
        for key in write_held_cache.iter() {
            write_keys.push(key.to_string());
        }
        drop(read_held_cache);
        drop(write_held_cache);
        self.release_locks(read_keys, write_keys).await?;
        Ok(())
    }

    pub async fn release_locks(
        &self,
        read_keys: Vec<String>,
        write_keys: Vec<String>,
    ) -> TribResult<()> {
        let client_id = format!("{}", self.client_id);

        let read_held_cache = self.read_held_cache.read().await;
        let write_held_cache = self.write_held_cache.read().await;

        let mut read_bins: HashMap<usize, Vec<String>> = HashMap::new();
        let mut write_bins: HashMap<usize, Vec<String>> = HashMap::new();
        let mut bins: HashSet<usize> = HashSet::new();
        for key in read_keys.iter() {
            if read_held_cache.contains(key) {
                let ind = self.get_hash_index(key) as usize;
                if !read_bins.contains_key(&ind) {
                    read_bins.insert(ind, vec![]);
                }
                bins.insert(ind);
                read_bins.get_mut(&ind).unwrap().push(key.to_string());
            }
        }
        for key in write_keys.iter() {
            if write_held_cache.contains(key) {
                let ind = self.get_hash_index(key) as usize;
                if !write_bins.contains_key(&ind) {
                    write_bins.insert(ind, vec![]);
                }
                bins.insert(ind);
                write_bins.get_mut(&ind).unwrap().push(key.to_string());
            }
        }
        drop(read_held_cache);
        drop(write_held_cache);
        for ind in bins {
            let mut purified_read_keys = vec![];
            let mut purified_write_keys = vec![];
            let read_keys_option = read_bins.get(&ind);
            if read_keys_option.is_some() {
                purified_read_keys = read_keys_option.unwrap().clone();
            }
            let write_keys_option = write_bins.get(&ind);
            if write_keys_option.is_some() {
                purified_write_keys = write_keys_option.unwrap().clone();
            }
            self.release_locks_with_server_index(ind, purified_read_keys, purified_write_keys)
                .await?;
        }
        let mut read_held_cache = self.read_held_cache.write().await;
        let mut write_held_cache = self.write_held_cache.write().await;

        for key in read_keys.iter() {
            if read_held_cache.contains(key) {
                read_held_cache.remove(key);
            }
        }
        for key in write_keys.iter() {
            // println!("trying to remove key: {}", key);
            if write_held_cache.contains(key) {
                // println!("remove key {} from cache", key);
                write_held_cache.remove(key);
            }
        }
        Ok(())
    }
}
