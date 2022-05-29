use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::{HashMap, HashSet};
use super::lock_client::LockClient;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct transaction_log {
    transaction_id: String,
    transaction_key: String,
    old_value: Vec<String>,
    bin_storage: Arc<BinStorageClient>,
}

pub struct transaction_client {
    transaction_id: String,
    transaction_num: RwLock<u64>,
    lock_client: Arc<LockClient>,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
}

impl transaction_client {
    pub fn new(lock_client: Arc<LockClient>, transaction_id: String, channel_cache: Arc<RwLock<HashMap<String, Channel>>>) -> Self {
        Self {
            transaction_id,
            transaction_num: RwLock<u64>::new(0),
            lock_client,
            channel_cache,
        }
    }

    pub async fn transaction_start(keys_map: HashMap<String, Vec<String>>) {
        let bin_clients = vec![];
        for (bin, key_list) in keys_map.iter() {

        }
    }
}
