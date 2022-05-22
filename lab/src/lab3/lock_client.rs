use super::bin_client::update_channel_cache;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct LockClient {
    client_id: Uuid,
    read_held_cache: RwLock<HashSet<String>>,
    write_held_cache: RwLock<HashSet<String>>,
    locks_addrs: Vec<String>,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
}

// use tonic::transport::Endpoint;
use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::storage;

impl LockClient {
    pub fn new(locks_addrs: Vec<String>) -> Self {
        let id = Uuid::new_v4();
        Self {
            client_id: Uuid::new_v4(),
            read_held_cache: RwLock::new(HashSet::new()),
            write_held_cache: RwLock::new(HashSet::new()),
            channel_cache: Arc::new(RwLock::new(HashMap::new())),
            locks_addrs: locks_addrs.clone(),
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
            panic!();
        }
        Ok(())
    }

    async fn acquire_locks(
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
        for key in read_keys {
            if !read_held_cache.contains(&key) {
                let ind = self.get_hash_index(&key) as usize;
                if !read_bins.contains_key(&ind) {
                    read_bins.insert(ind, vec![]);
                }
                bins.insert(ind);
                read_bins.get_mut(&ind).unwrap().push(key);
            }
        }
        for key in write_keys {
            if !write_held_cache.contains(&key) {
                let ind = self.get_hash_index(&key) as usize;
                if !write_bins.contains_key(&ind) {
                    write_bins.insert(ind, vec![]);
                }
                bins.insert(ind);
                write_bins.get_mut(&ind).unwrap().push(key);
            }
        }
        for ind in bins {
            let mut read_keys = vec![];
            let mut write_keys = vec![];
            let read_keys_option = read_bins.get(&ind);
            if read_keys_option.is_some() {
                read_keys = read_keys_option.unwrap().clone();
            }
            let write_keys_option = write_bins.get(&ind);
            if write_keys_option.is_some() {
                write_keys = write_keys_option.unwrap().clone();
            }
            self.acquire_locks_with_server_index(ind, read_keys, write_keys)
                .await;
        }
        Ok(())
    }

    async fn release_all_locks(
        &self,
        read_keys: Vec<String>,
        write_keys: Vec<String>,
    ) -> TribResult<()> {
        Ok(())
    }

    async fn release_locks() -> TribResult<()> {
        Ok(())
    }
}
