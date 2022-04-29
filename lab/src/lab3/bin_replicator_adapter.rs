use super::bin_prefix_adapter::BinPrefixAdapter;
use serde::{Deserialize, Serialize};
use std::cmp::{self, min, Ordering};
use std::error;
use std::fmt;
use tribbler::err::TribResult;
use tribbler::storage::{self, Storage};

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
}

pub const STR_LOG_PREFIX: &str = "STR::";
pub const LIST_LOG_PREFIX: &str = "LIST::";

pub struct BinReplicatorAdapter {
    pub hash_index: u32,
    pub backs: Vec<String>,
    pub bin: String,
}

impl BinReplicatorAdapter {
    pub fn new(hash_index: u32, backs: Vec<String>, bin: &str) -> Self {
        Self {
            hash_index,
            backs: backs.clone(),
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
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = backs.len() + self.hash_index as usize;
        for i in start..end {
            let backend_index = i % backs.len();
            let backend_addr = &backs[backend_index];
            let bin_prefix_adapter = BinPrefixAdapter::new(&backend_addr, &self.bin.to_string());
            let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, key);
            let list_retrieved_res = bin_prefix_adapter.list_get(&wrapped_key).await;
            if list_retrieved_res.is_err() {
                continue;
            }
            let list_retrieved = list_retrieved_res.unwrap().0;
            let mut sorted_list = vec![];
            for element in list_retrieved {
                let log_entry: SortableLogRecord = serde_json::from_str(&element).unwrap();
                sorted_list.push(log_entry);
            }
            sorted_list.sort_by(|a, b| {
                if a.clock_id < b.clock_id {
                    return Ordering::Less;
                } else if a.clock_id > b.clock_id {
                    return Ordering::Greater;
                }
                return Ordering::Equal;
            });

            let mut ret_val = vec![];
            for element in sorted_list {
                ret_val.push(element.wrapped_string);
            }
            return Ok(storage::List(ret_val));
        }
        return Err(Box::new(NotEnoughServers));
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = backs.len() + self.hash_index as usize;
        for i in start..end {
            let primary_backend_index = i % backs.len();
            let primary_backend_addr = &backs[primary_backend_index];
            let primary_bin_prefix_adapter =
                BinPrefixAdapter::new(&primary_backend_addr, &self.bin.to_string());

            let primary_clock_res = primary_bin_prefix_adapter.clock(0).await;
            if primary_clock_res.is_err() {
                continue;
            }

            let primary_clock_id = primary_clock_res.unwrap();
            let log_entry = SortableLogRecord {
                clock_id: primary_clock_id,
                wrapped_string: kv.value.to_string(),
            };
            let log_entry_str = serde_json::to_string(&log_entry)?;

            let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key.to_string());
            let primary_append_res = primary_bin_prefix_adapter
                .list_append(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await;
            if primary_append_res.is_err() {
                continue;
            }
            for j in i + 1..end {
                let secondary_backend_index = j % backs.len();
                let secondary_backend_addr = &backs[secondary_backend_index];
                let secondary_bin_prefix_adapter =
                    BinPrefixAdapter::new(&secondary_backend_addr, &self.bin.to_string());
                let _ = secondary_bin_prefix_adapter.clock(primary_clock_id).await?;
                let secondary_append_res = secondary_bin_prefix_adapter
                    .list_append(&storage::KeyValue {
                        key: wrapped_key.to_string(),
                        value: log_entry_str.to_string(),
                    })
                    .await;
                if secondary_append_res.is_err() {
                    continue;
                }
                return Ok(true);
            }
            return Err(Box::new(NotEnoughServers));
        }
        return Err(Box::new(NotEnoughServers));
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = backs.len() + self.hash_index as usize;
        for i in start..end {
            let primary_backend_index = i % backs.len();
            let primary_backend_addr = &backs[primary_backend_index];
            let primary_bin_prefix_adapter =
                BinPrefixAdapter::new(&primary_backend_addr, &self.bin.to_string());

            let primary_clock_res = primary_bin_prefix_adapter.clock(0).await;
            if primary_clock_res.is_err() {
                continue;
            }

            let primary_clock_id = primary_clock_res.unwrap();
            let log_entry = SortableLogRecord {
                clock_id: primary_clock_id,
                wrapped_string: kv.value.to_string(),
            };
            let log_entry_str = serde_json::to_string(&log_entry)?;

            let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key.to_string());
            let primary_append_res = primary_bin_prefix_adapter
                .list_remove(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await;
            if primary_append_res.is_err() {
                continue;
            }
            let ret_val = primary_append_res.unwrap();
            for j in i + 1..end {
                let secondary_backend_index = j % backs.len();
                let secondary_backend_addr = &backs[secondary_backend_index];
                let secondary_bin_prefix_adapter =
                    BinPrefixAdapter::new(&secondary_backend_addr, &self.bin.to_string());
                let _ = secondary_bin_prefix_adapter.clock(primary_clock_id).await?;
                let secondary_append_res = secondary_bin_prefix_adapter
                    .list_remove(&storage::KeyValue {
                        key: wrapped_key.to_string(),
                        value: log_entry_str.to_string(),
                    })
                    .await;
                if secondary_append_res.is_err() {
                    continue;
                }
                return Ok(ret_val);
            }
            return Err(Box::new(NotEnoughServers));
        }
        return Err(Box::new(NotEnoughServers));
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = backs.len() + self.hash_index as usize;
        for i in start..end {
            let backend_index = i % backs.len();
            let backend_addr = &backs[backend_index];
            let bin_prefix_adapter = BinPrefixAdapter::new(&backend_addr, &self.bin.to_string());
            let wrapped_prefx = format!("{}{}", LIST_LOG_PREFIX, p.prefix);
            let key_retrieved_res = bin_prefix_adapter
                .list_keys(&storage::Pattern {
                    prefix: wrapped_prefx,
                    suffix: p.suffix.to_string(),
                })
                .await;
            if key_retrieved_res.is_err() {
                continue;
            }
            return key_retrieved_res;
        }
        return Err(Box::new(NotEnoughServers));
    }
}

#[async_trait]
impl storage::Storage for BinReplicatorAdapter {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = backs.len() + self.hash_index as usize;
        for i in start..end {
            let backend_index = i % backs.len();
            let backend_addr = &backs[backend_index];
            let bin_prefix_adapter = BinPrefixAdapter::new(&backend_addr, &self.bin.to_string());
            let clock_res = bin_prefix_adapter.clock(at_least).await;
            if clock_res.is_err() {
                continue;
            }
            return Ok(clock_res.unwrap());
        }
        return Err(Box::new(NotEnoughServers));
    }
}
