use super::bin_prefix_adapter::BinPrefixAdapter;
use serde::{Deserialize, Serialize};
use std::cmp::{self, min, Ordering};
use std::collections::HashSet;
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
pub const VERSION_LOG_KEY_NAME: &str = "VERSIONLOG";

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

#[async_trait]
pub trait BinReplicatorHelper {
    async fn get_replicas_addresses(&self) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>);
}

#[async_trait]
impl BinReplicatorHelper for BinReplicatorAdapter {
    async fn get_replicas_addresses(&self) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>) {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = start + backs.len();
        let mut primary_adapter_option = None;
        let mut secondary_adapter_option = None;
        for i in start..end {
            let primary_backend_index = i % backs.len();
            let primary_backend_addr = &backs[primary_backend_index];
            let primary_bin_prefix_adapter =
                BinPrefixAdapter::new(&primary_backend_addr, &self.bin.to_string());
            let primary_resp = primary_bin_prefix_adapter.clock(0).await;
            if primary_resp.is_err() {
                continue;
            }
            primary_adapter_option = Some(primary_bin_prefix_adapter);
            for j in i + 1..end {
                let secondary_backend_index = j % backs.len();
                let secondary_backend_addr = &backs[secondary_backend_index];
                let secondary_bin_prefix_adapter =
                    BinPrefixAdapter::new(&secondary_backend_addr, &self.bin.to_string());
                let secondary_resp = secondary_bin_prefix_adapter.clock(0).await;
                if secondary_resp.is_err() {
                    continue;
                }
                secondary_adapter_option = Some(secondary_bin_prefix_adapter);
                break;
            }
        }
        return (primary_adapter_option, secondary_adapter_option);
    }
}

#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyList for BinReplicatorAdapter {
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, key);

        let (primary_adapter_option, secondary_adapter_option) =
            self.get_replicas_addresses().await;
        if primary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }
        let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
        let primary_version = primary_bin_prefix_adapter
            .list_get(VERSION_LOG_KEY_NAME)
            .await?
            .0
            .len();
        let primary_log_vec = primary_bin_prefix_adapter.list_get(&wrapped_key).await?.0;

        let mut secondary_version = 0;
        let mut secondary_log_vec = vec![];
        if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
            secondary_version = secondary_bin_prefix_adapter
                .list_get(VERSION_LOG_KEY_NAME)
                .await?
                .0
                .len();
            secondary_log_vec = secondary_bin_prefix_adapter.list_get(&wrapped_key).await?.0;
        }

        let mut logs_chosen;
        if primary_version >= secondary_version {
            logs_chosen = primary_log_vec;
        } else {
            logs_chosen = secondary_log_vec;
        }

        let mut clock_id_set = HashSet::new();
        let mut dedup_logs = vec![];
        for element in logs_chosen {
            let log_entry: SortableLogRecord = serde_json::from_str(&element).unwrap();
            if clock_id_set.contains(&log_entry.clock_id) {
                continue;
            }
            clock_id_set.insert(log_entry.clock_id);
            dedup_logs.push(log_entry);
        }
        dedup_logs.sort_unstable_by(|a, b| {
            if a.clock_id < b.clock_id {
                return Ordering::Less;
            } else if a.clock_id > b.clock_id {
                return Ordering::Greater;
            }
            return Ordering::Equal;
        });

        let mut ret_val = vec![];
        for element in dedup_logs {
            ret_val.push(element.wrapped_string);
        }
        return Ok(storage::List(ret_val));
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key.to_string());
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_replicas_addresses().await;
        if primary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }
        let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
        // get primary clock as unique id
        let primary_clock_id = primary_bin_prefix_adapter.clock(0).await?;
        let log_entry = SortableLogRecord {
            clock_id: primary_clock_id,
            wrapped_string: kv.value.to_string(),
        };
        let log_entry_str = serde_json::to_string(&log_entry)?;
        let _ = primary_bin_prefix_adapter
            .list_append(&storage::KeyValue {
                key: wrapped_key.to_string(),
                value: log_entry_str.to_string(),
            })
            .await?;
        let _ = primary_bin_prefix_adapter
            .list_append(&storage::KeyValue {
                key: VERSION_LOG_KEY_NAME.to_string(),
                value: "0".to_string(),
            })
            .await?;
        if secondary_adapter_option.is_none() {
            return Ok(true);
        }
        let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
        let _ = secondary_bin_prefix_adapter.clock(primary_clock_id).await?;
        let _ = secondary_bin_prefix_adapter
            .list_append(&storage::KeyValue {
                key: wrapped_key.to_string(),
                value: log_entry_str.to_string(),
            })
            .await?;
        let _ = secondary_bin_prefix_adapter
            .list_append(&storage::KeyValue {
                key: VERSION_LOG_KEY_NAME.to_string(),
                value: "0".to_string(),
            })
            .await?;
        return Ok(true);
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key);

        let (primary_adapter_option, secondary_adapter_option) =
            self.get_replicas_addresses().await;
        if primary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }
        let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
        let primary_log_vec = primary_bin_prefix_adapter.list_get(&wrapped_key).await?.0;
        let mut primary_value_to_remove = "".to_string();
        for element in primary_log_vec {
            let log_entry: SortableLogRecord = serde_json::from_str(&element).unwrap();
            if log_entry.wrapped_string == kv.value {
                primary_value_to_remove = element.to_string();
                break;
            }
        }
        let num_removal = primary_bin_prefix_adapter
            .list_remove(&storage::KeyValue {
                key: wrapped_key.to_string(),
                value: primary_value_to_remove.to_string(),
            })
            .await?;
        let _ = primary_bin_prefix_adapter
            .list_append(&storage::KeyValue {
                key: VERSION_LOG_KEY_NAME.to_string(),
                value: "0".to_string(),
            })
            .await?;
        if secondary_adapter_option.is_none() {
            return Ok(num_removal);
        }
        let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
        let secondary_log_vec = secondary_bin_prefix_adapter.list_get(&wrapped_key).await?.0;
        let mut secondary_value_to_remove = "".to_string();
        for element in secondary_log_vec {
            let log_entry: SortableLogRecord = serde_json::from_str(&element).unwrap();
            if log_entry.wrapped_string == kv.value {
                secondary_value_to_remove = element.to_string();
                break;
            }
        }
        let _ = secondary_bin_prefix_adapter
            .list_remove(&storage::KeyValue {
                key: wrapped_key.to_string(),
                value: secondary_value_to_remove.to_string(),
            })
            .await?;
        let _ = secondary_bin_prefix_adapter
            .list_append(&storage::KeyValue {
                key: VERSION_LOG_KEY_NAME.to_string(),
                value: "0".to_string(),
            })
            .await?;
        return Ok(num_removal);
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let wrapped_prefx = format!("{}{}", LIST_LOG_PREFIX, p.prefix);

        let (primary_adapter_option, secondary_adapter_option) =
            self.get_replicas_addresses().await;
        if primary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }
        let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
        let primary_version = primary_bin_prefix_adapter
            .list_get(VERSION_LOG_KEY_NAME)
            .await?
            .0
            .len();
        let primary_keys = primary_bin_prefix_adapter
            .list_keys(&storage::Pattern {
                prefix: wrapped_prefx.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?
            .0;

        let mut secondary_version = 0;
        let mut secondary_keys = vec![];
        if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
            secondary_version = secondary_bin_prefix_adapter
                .list_get(VERSION_LOG_KEY_NAME)
                .await?
                .0
                .len();
            secondary_keys = secondary_bin_prefix_adapter
                .list_keys(&storage::Pattern {
                    prefix: wrapped_prefx.to_string(),
                    suffix: p.suffix.to_string(),
                })
                .await?
                .0;
        }

        let mut keys_vec = vec![];
        if primary_version >= secondary_version {
            keys_vec = primary_keys;
        } else {
            keys_vec = secondary_keys;
        }
        let mut ret_val = vec![];
        for element in keys_vec {
            let element_str = element.as_str();
            let mut extracted_key = "";
            if element_str.starts_with(LIST_LOG_PREFIX) {
                extracted_key = &element_str[LIST_LOG_PREFIX.len()..];
            } else if element_str.starts_with(STR_LOG_PREFIX) {
                extracted_key = &element_str[STR_LOG_PREFIX.len()..];
            } else if element_str == VERSION_LOG_KEY_NAME {
                continue;
            }
            ret_val.push(extracted_key.to_string());
        }
        return Ok(storage::List(ret_val));
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
