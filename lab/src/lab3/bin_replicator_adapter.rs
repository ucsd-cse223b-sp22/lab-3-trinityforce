use super::bin_client::update_channel_cache;
use super::bin_prefix_adapter::BinPrefixAdapter;
use super::client::StorageClient;
use super::constants::{
    APPEND_ACTION, KEYS_PREFIX, LIST_KEYS_PREFIX, LIST_LOG_PREFIX, REMOVE_ACTION, STR_LOG_PREFIX,
    VALIDATION_BIT_KEY,
};
use super::lock_client::LockClient;
use serde::{Deserialize, Serialize};
use std::cmp::{self, Ordering};
use std::collections::{HashMap, HashSet};
use std::error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tribbler::err::TribResult;
use tribbler::storage::{self, KeyList, KeyString, Storage};

// Change the alias to `Box<error::Error>`.
#[derive(Debug, Clone)]
struct NotEnoughServers;

impl fmt::Display for NotEnoughServers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "all servers are down")
    }
}

impl error::Error for NotEnoughServers {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SortableLogRecord {
    pub wrapped_string: String,
    pub clock_id: u64,
    pub action: String, // const APPEND_ACTION stands for append, REMOVE_ACTION stands for remove.
}

pub struct BinReplicatorAdapter {
    pub hash_index: u32,
    pub backs: Vec<String>,
    pub bin: String,
    back_status: Vec<bool>,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    lock_client: Arc<LockClient>,
    pub with_lock: bool,
}

impl BinReplicatorAdapter {
    pub fn new(
        hash_index: u32,
        backs: Vec<String>,
        bin: &str,
        back_status: Vec<bool>,
        channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
        lock_client: Arc<LockClient>,
    ) -> Self {
        Self {
            hash_index,
            backs: backs.clone(),
            bin: bin.to_string(),
            back_status: back_status.clone(),
            channel_cache,
            lock_client,
            with_lock: false,
        }
    }

    pub fn acquire_lock(&mut self) {
        self.with_lock = true;
    }

    pub fn release_lock(&mut self) {
        self.with_lock = false;
    }
}

use async_trait::async_trait;
#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyString for BinReplicatorAdapter {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let wrapped_key = format!("{}{}", STR_LOG_PREFIX, key);
        let mut read_keys = vec![];
        read_keys.push(wrapped_key.to_string());
        if !self.with_lock {
            self.lock_client
                .acquire_locks(self.lockkey_decorator(read_keys.clone()), vec![])
                .await?;
        }

        // Get ther first alive and valid bin.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_read_replicas_access_new().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        // Get all logs
        let result_str = self
            .get_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
            )
            .await;

        if !self.with_lock {
            self.lock_client
                .release_locks(self.lockkey_decorator(read_keys), vec![])
                .await?;
        }
        return result_str;
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let wrapped_key = format!("{}{}", STR_LOG_PREFIX, kv.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let mut write_keys = vec![];
        write_keys.push(wrapped_key.to_string());
        // println!(
        //     "set acquire {:?}",
        //     self.lockkey_decorator(write_keys.clone())
        // );
        if !self.with_lock {
            self.lock_client
                .acquire_locks(vec![], self.lockkey_decorator(write_keys.clone()))
                .await?;
        }

        // Try to append the entry in primary and secondary
        let result = self
            .set_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kv,
            )
            .await;
        // println!(
        //     "set release {:?}",
        //     self.lockkey_decorator(write_keys.clone())
        // );
        if !self.with_lock {
            self.lock_client
                .release_locks(vec![], self.lockkey_decorator(write_keys))
                .await?;
        }
        result
    }

    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let wrapped_prefx = format!("{}{}", STR_LOG_PREFIX, p.prefix);

        let (primary_adapter_option, secondary_adapter_option) =
            self.get_read_replicas_access_new().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let mut read_keys = vec![];
        read_keys.push(format!("{}{}", KEYS_PREFIX, self.bin.to_string()));
        if !self.with_lock {
            self.lock_client
                .acquire_locks(read_keys.clone(), vec![])
                .await?;
        }

        let potential_keys = self
            .keys_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &storage::Pattern {
                    prefix: wrapped_prefx.to_string(),
                    suffix: p.suffix.to_string(),
                },
            )
            .await;
        if let Err(e) = potential_keys {
            if !self.with_lock {
                self.lock_client.release_locks(read_keys, vec![]).await?;
            }
            return Err(e);
        }
        if !self.with_lock {
            self.lock_client.release_locks(read_keys, vec![]).await?;
        }
        let true_keys = potential_keys.unwrap();

        return Ok(storage::List(true_keys));
    }
}

#[async_trait]
pub trait BinReplicatorHelper {
    fn lockkey_decorator(&self, keys: Vec<String>) -> Vec<String>;
    async fn get_read_replicas_access(&self) -> Option<BinPrefixAdapter>; // starting from returning the first living valid machine
    async fn get_write_replicas_access(
        &self,
    ) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>); // return two first trues from back_status
    async fn get_sorted_log_struct(
        &self,
        bin_prefix_adapter: &BinPrefixAdapter,
        wrapped_key: &str,
    ) -> TribResult<Vec<SortableLogRecord>>; // Get all logs. Sort and dedup.
    async fn append_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
    ) -> TribResult<bool>;
    async fn remove_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
    ) -> TribResult<u32>;
    async fn set_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
    ) -> TribResult<bool>;
    async fn get_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
    ) -> TribResult<Option<String>>;
    async fn get_list_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
    ) -> TribResult<storage::List>;
    async fn set_list_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kl: &storage::KeyValueList,
    ) -> TribResult<bool>;
    async fn keys_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        p: &storage::Pattern,
    ) -> TribResult<Vec<String>>;
    async fn list_keys_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        p: &storage::Pattern,
    ) -> TribResult<Vec<String>>;
    async fn get_read_replicas_access_new(
        &self,
    ) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>);
}

#[async_trait]
impl BinReplicatorHelper for BinReplicatorAdapter {
    fn lockkey_decorator(&self, wrap_keys: Vec<String>) -> Vec<String> {
        let mut ret_vec = vec![];
        for key in wrap_keys {
            let bin_wrapped_key = format!("{}::{}", self.bin, key);
            ret_vec.push(bin_wrapped_key);
        }
        return ret_vec;
    }

    async fn get_read_replicas_access(&self) -> Option<BinPrefixAdapter> {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = start + backs.len();
        for i in start..end {
            let backend_index = i % backs.len();
            if self.back_status[backend_index] == false {
                continue;
            }
            let backend_addr = &backs[backend_index];
            let chan_res =
                update_channel_cache(self.channel_cache.clone(), backend_addr.clone()).await;
            if chan_res.is_err() {
                // println!("Channel get failed!");
                continue;
            }
            let pinger = StorageClient::new(backend_addr, Some(chan_res.unwrap()));
            let resp = pinger.get(VALIDATION_BIT_KEY).await;
            if resp.is_err() {
                continue;
            }
            let validation = resp.unwrap();
            if validation == None {
                continue;
            }
            return Some(BinPrefixAdapter::new(
                &backend_addr,
                &self.bin.to_string(),
                self.channel_cache.clone(),
            ));
        }
        return None;
    }
    async fn get_write_replicas_access(
        &self,
    ) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>) {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = start + backs.len();
        let mut primary_adapter_option = None;
        let mut secondary_adapter_option = None;
        for i in start..end {
            // start scanning the primary replica
            let primary_backend_index = i % backs.len();
            if self.back_status[primary_backend_index] == false {
                continue;
            }
            let primary_backend_addr = &backs[primary_backend_index];
            let primary_chan_res =
                update_channel_cache(self.channel_cache.clone(), primary_backend_addr.clone())
                    .await;
            if !primary_chan_res.is_err() {
                let primary_pinger =
                    StorageClient::new(primary_backend_addr, Some(primary_chan_res.unwrap()));
                let primary_resp = primary_pinger.get("DUMMY").await;
                if primary_resp.is_err() {
                    primary_adapter_option = None;
                } else {
                    primary_adapter_option = Some(BinPrefixAdapter::new(
                        &primary_backend_addr,
                        &self.bin.to_string(),
                        self.channel_cache.clone(),
                    ));
                }
            } else {
                // println!("Channel get failed!");
            }

            // start scanning the secondary replica
            for j in i + 1..end {
                let secondary_backend_index = j % backs.len();
                if self.back_status[secondary_backend_index] == false {
                    continue;
                }
                let secondary_backend_addr = &backs[secondary_backend_index];
                let secondary_chan_res = update_channel_cache(
                    self.channel_cache.clone(),
                    secondary_backend_addr.clone(),
                )
                .await;

                if !secondary_chan_res.is_err() {
                    let secondary_pinger = StorageClient::new(
                        secondary_backend_addr,
                        Some(secondary_chan_res.unwrap()),
                    );
                    let secondary_resp = secondary_pinger.get("DUMMY").await;
                    if secondary_resp.is_err() {
                        secondary_adapter_option = None;
                    } else {
                        secondary_adapter_option = Some(BinPrefixAdapter::new(
                            &secondary_backend_addr,
                            &self.bin.to_string(),
                            self.channel_cache.clone(),
                        ));
                    }
                } else {
                    // println!("Channel get failed!");
                }
                break;
            }
            break;
        }
        return (primary_adapter_option, secondary_adapter_option);
    }

    async fn get_sorted_log_struct(
        &self,
        bin_prefix_adapter: &BinPrefixAdapter,
        wrapped_key: &str,
    ) -> TribResult<Vec<SortableLogRecord>> {
        let logs_string = bin_prefix_adapter.list_get(&wrapped_key).await?.0;
        let mut logs_struct = vec![];
        for element in logs_string {
            let log_entry: SortableLogRecord = serde_json::from_str(&element).unwrap();
            logs_struct.push(log_entry);
        }
        logs_struct.sort_unstable_by(|a, b| {
            if a.clock_id < b.clock_id {
                return Ordering::Less;
            } else if a.clock_id > b.clock_id {
                return Ordering::Greater;
            }
            return Ordering::Equal;
        });
        logs_struct.dedup_by(|a, b| a.clock_id == b.clock_id); // Is it necessary to dedup?
        Ok(logs_struct)
    }

    async fn append_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
    ) -> TribResult<bool> {
        let new_kv = &storage::KeyValue {
            key: wrapped_key.to_string(),
            value: kv.value.to_string(),
        };
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            // println!(
            //     "{},{}",
            //     primary_bin_prefix_adapter.addr, secondary_bin_prefix_adapter.addr
            // );
            primary_bin_prefix_adapter.list_append(new_kv).await;
            secondary_bin_prefix_adapter.list_append(new_kv).await;
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            primary_bin_prefix_adapter.list_append(new_kv).await;
        } else {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            secondary_bin_prefix_adapter.list_append(new_kv).await;
        }
        Ok(true)
    }

    async fn set_list_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kl: &storage::KeyValueList,
    ) -> TribResult<bool> {
        let new_kl = &storage::KeyValueList {
            key: wrapped_key.to_string(),
            list: kl.list.clone(),
        };
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            // println!(
            //     "{},{}",
            //     primary_bin_prefix_adapter.addr, secondary_bin_prefix_adapter.addr
            // );
            primary_bin_prefix_adapter.list_set(new_kl).await;
            secondary_bin_prefix_adapter.list_set(new_kl).await;
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            primary_bin_prefix_adapter.list_set(new_kl).await;
        } else {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            secondary_bin_prefix_adapter.list_set(new_kl).await;
        }
        Ok(true)
    }

    async fn remove_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
    ) -> TribResult<u32> {
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let result1 = primary_bin_prefix_adapter.list_remove(kv).await;
            let result2 = secondary_bin_prefix_adapter.list_remove(kv).await;
            if result1.is_err() {
                return result2;
            }
            return result1;
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let result = primary_bin_prefix_adapter.list_remove(kv).await;
            return result;
        } else {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let result = secondary_bin_prefix_adapter.list_remove(kv).await;
            return result;
        }
    }

    async fn set_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
    ) -> TribResult<bool> {
        let new_kv = &storage::KeyValue {
            key: wrapped_key.to_string(),
            value: kv.value.to_string(),
        };
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            primary_bin_prefix_adapter.set(new_kv).await;
            secondary_bin_prefix_adapter.set(new_kv).await;
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            primary_bin_prefix_adapter.set(new_kv).await;
        } else if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            secondary_bin_prefix_adapter.set(new_kv).await;
        }
        Ok(true)
    }

    async fn get_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
    ) -> TribResult<Option<String>> {
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.get(wrapped_key).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap());
            }
            let vec_secondary = secondary_bin_prefix_adapter.get(wrapped_key).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap());
            }
            return Ok(None);
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.get(wrapped_key).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap());
            }
            return Ok(None);
        } else if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_secondary = secondary_bin_prefix_adapter.get(wrapped_key).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap());
            }
            return Ok(None);
        }
        return Ok(None);
    }

    async fn get_list_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
    ) -> TribResult<storage::List> {
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap());
            }
            let vec_secondary = secondary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap());
            }
            return Ok(storage::List(vec![] as Vec<String>));
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap());
            }
            return Ok(storage::List(vec![] as Vec<String>));
        } else if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_secondary = secondary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap());
            }
            return Ok(storage::List(vec![] as Vec<String>));
        }
        return Ok(storage::List(vec![] as Vec<String>));
    }

    async fn keys_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        p: &storage::Pattern,
    ) -> TribResult<Vec<String>> {
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.keys(p).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap().0);
            }
            let vec_secondary = secondary_bin_prefix_adapter.keys(p).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.keys(p).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        } else if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_secondary = secondary_bin_prefix_adapter.keys(p).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        }
        return Ok(vec![] as Vec<String>);
    }

    async fn list_keys_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        p: &storage::Pattern,
    ) -> TribResult<Vec<String>> {
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.list_keys(p).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap().0);
            }
            let vec_secondary = secondary_bin_prefix_adapter.list_keys(p).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.list_keys(p).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        } else if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_secondary = secondary_bin_prefix_adapter.list_keys(p).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        }
        return Ok(vec![] as Vec<String>);
    }

    async fn get_read_replicas_access_new(
        &self,
    ) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>) {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = start + backs.len();
        let mut primary_adapter_option = None;
        let mut secondary_adapter_option = None;
        for i in start..end {
            // start scanning the primary replica
            let primary_backend_index = i % backs.len();
            if self.back_status[primary_backend_index] == false {
                continue;
            }
            let primary_backend_addr = &backs[primary_backend_index];
            let primary_chan_res =
                update_channel_cache(self.channel_cache.clone(), primary_backend_addr.clone())
                    .await;
            if !primary_chan_res.is_err() {
                let primary_pinger =
                    StorageClient::new(primary_backend_addr, Some(primary_chan_res.unwrap()));
                let primary_resp = primary_pinger.get(VALIDATION_BIT_KEY).await;
                if primary_resp.is_err() || primary_resp.unwrap().is_none() {
                    primary_adapter_option = None;
                } else {
                    primary_adapter_option = Some(BinPrefixAdapter::new(
                        &primary_backend_addr,
                        &self.bin.to_string(),
                        self.channel_cache.clone(),
                    ));
                }
            } else {
                // println!("Channel get failed!");
            }

            // start scanning the secondary replica
            for j in i + 1..end {
                let secondary_backend_index = j % backs.len();
                if self.back_status[secondary_backend_index] == false {
                    continue;
                }
                let secondary_backend_addr = &backs[secondary_backend_index];
                let secondary_chan_res = update_channel_cache(
                    self.channel_cache.clone(),
                    secondary_backend_addr.clone(),
                )
                .await;

                if !secondary_chan_res.is_err() {
                    let secondary_pinger = StorageClient::new(
                        secondary_backend_addr,
                        Some(secondary_chan_res.unwrap()),
                    );
                    let secondary_resp = secondary_pinger.get(VALIDATION_BIT_KEY).await;
                    if secondary_resp.is_err() || secondary_resp.unwrap().is_none() {
                        secondary_adapter_option = None;
                    } else {
                        secondary_adapter_option = Some(BinPrefixAdapter::new(
                            &secondary_backend_addr,
                            &self.bin.to_string(),
                            self.channel_cache.clone(),
                        ));
                    }
                } else {
                    // println!("Channel get failed!");
                }
                break;
            }
            break;
        }
        return (primary_adapter_option, secondary_adapter_option);
    }
}

#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyList for BinReplicatorAdapter {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, key);

        let (primary_adapter_option, secondary_adapter_option) =
            self.get_read_replicas_access_new().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let mut read_keys = vec![];
        read_keys.push(wrapped_key.to_string());
        if !self.with_lock {
            self.lock_client
                .acquire_locks(self.lockkey_decorator(read_keys.clone()), vec![])
                .await?;
        }

        // Get all logs
        let list_ret = self
            .get_list_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
            )
            .await;
        let res = list_ret.unwrap();
        // println!("get list action: {:?}", res.0);
        if !self.with_lock {
            self.lock_client
                .release_locks(self.lockkey_decorator(read_keys), vec![])
                .await?;
        }
        return Ok(res);
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let mut write_keys = vec![];
        write_keys.push(kv.key.to_string());
        if !self.with_lock {
            self.lock_client
                .acquire_locks(vec![], self.lockkey_decorator(write_keys.clone()))
                .await?;
        }

        // Try to append the entry in primary and secondary
        let result = self
            .append_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kv,
            )
            .await;
        if !self.with_lock {
            self.lock_client
                .release_locks(vec![], self.lockkey_decorator(write_keys))
                .await?;
        }
        return result;
    }

    async fn list_set(&self, kl: &storage::KeyValueList) -> TribResult<bool> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kl.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let mut write_keys = vec![];
        write_keys.push(kl.key.to_string());
        if !self.with_lock {
            self.lock_client
                .acquire_locks(vec![], self.lockkey_decorator(write_keys.clone()))
                .await?;
        }

        // Try to append the entry in primary and secondary
        let result = self
            .set_list_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kl,
            )
            .await;

        if !self.with_lock {
            self.lock_client
                .release_locks(vec![], self.lockkey_decorator(write_keys))
                .await?;
        }
        return result;
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let mut write_keys = vec![];
        write_keys.push(kv.key.to_string());
        if !self.with_lock {
            self.lock_client
                .acquire_locks(vec![], self.lockkey_decorator(write_keys.clone()))
                .await?;
        }

        // Try to remove the entry in primary and secondary
        let result = self
            .remove_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kv,
            )
            .await;

        if !self.with_lock {
            self.lock_client
                .release_locks(vec![], self.lockkey_decorator(write_keys))
                .await?;
        }
        return result;
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let wrapped_prefx = format!("{}{}", LIST_LOG_PREFIX, p.prefix);

        // Get ther first alive and valid bin.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_read_replicas_access_new().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let mut read_keys = vec![];
        read_keys.push(format!("{}{}", LIST_KEYS_PREFIX, self.bin.to_string()));
        if !self.with_lock {
            self.lock_client
                .acquire_locks(read_keys.clone(), vec![])
                .await?;
        }

        let potential_keys = self
            .list_keys_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &storage::Pattern {
                    prefix: wrapped_prefx.to_string(),
                    suffix: p.suffix.to_string(),
                },
            )
            .await;

        if let Err(e) = potential_keys {
            if !self.with_lock {
                self.lock_client.release_locks(read_keys, vec![]).await?;
            }
            return Err(e);
        }
        if !self.with_lock {
            self.lock_client.release_locks(read_keys, vec![]).await?;
        }
        let true_keys = potential_keys.unwrap();

        return Ok(storage::List(true_keys));
    }
}

#[async_trait]
impl storage::Storage for BinReplicatorAdapter {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        // Sync clock
        let mut clk = 0;
        if primary_adapter_option.is_none() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
            clk = secondary_bin_prefix_adapter.clock(at_least).await?;
        } else if secondary_adapter_option.is_none() {
            let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
            clk = primary_bin_prefix_adapter.clock(at_least).await?;
        } else {
            let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();

            let primary_chan_res = update_channel_cache(
                self.channel_cache.clone(),
                primary_bin_prefix_adapter.addr.clone(),
            )
            .await;
            if primary_chan_res.is_err() {
                // println!("Should not happen!")
            }
            let primary_pinger = StorageClient::new(
                &primary_bin_prefix_adapter.addr,
                Some(primary_chan_res.unwrap()),
            );
            // Check whether primary is valid
            let resp = primary_pinger.get(VALIDATION_BIT_KEY).await;
            // let validation = resp.unwrap();
            if !resp.is_err() && resp.unwrap() == None {
                // If primary is not valid, sync primary using backup
                clk = secondary_bin_prefix_adapter.clock(at_least).await?;
                let _ = primary_bin_prefix_adapter.clock(clk).await;
            } else {
                let clk_res = primary_bin_prefix_adapter.clock(at_least).await;
                if !clk_res.is_err() {
                    clk = clk_res?;
                    let _ = secondary_bin_prefix_adapter.clock(clk).await;
                } else {
                    let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
                    clk = secondary_bin_prefix_adapter.clock(at_least).await?;
                }
            }
        }
        Ok(clk)
    }
}
