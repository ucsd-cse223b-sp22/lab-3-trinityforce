use super::bin_prefix_adapter::BinPrefixAdapter;
use super::constants::{
    APPEND_ACTION, LIST_LOG_PREFIX, REMOVE_ACTION, STR_LOG_PREFIX, VALIDATION_BIT_KEY,
};
use super::new_client;
use serde::{Deserialize, Serialize};
use std::cmp::{self, min, Ordering};
use std::collections::HashSet;
use std::error;
use std::fmt;
use std::sync::atomic::Ordering;
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
    pub action: String, // const APPEND_ACTION stands for append, REMOVE_ACTION stands for remove.
}

pub struct BinReplicatorAdapter {
    pub hash_index: u32,
    pub backs: Vec<String>,
    pub bin: String,
    back_status: Vec<bool>,
}

impl BinReplicatorAdapter {
    pub fn new(hash_index: u32, backs: Vec<String>, bin: &str, back_status: Vec<bool>) -> Self {
        Self {
            hash_index,
            backs: backs.clone(),
            bin: bin.to_string(),
            back_status: back_status.clone(),
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
    async fn get_read_replicas_access(&self) -> Option<BinPrefixAdapter>; // starting from returning the first living valid machine
    async fn get_write_replicas_access(
        &self,
    ) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>); // return two first trues from back_status
}

#[async_trait]
impl BinReplicatorHelper for BinReplicatorAdapter {
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
            let pinger = new_client(backend_addr).await.unwrap();
            let resp = pinger.get(VALIDATION_BIT_KEY).await;
            if resp.is_err() {
                continue;
            }
            let validation = resp.unwrap();
            if validation == None {
                continue;
            }
            return Some(BinPrefixAdapter::new(&backend_addr, &self.bin.to_string()));
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
            let primary_pinger = new_client(primary_backend_addr).await.unwrap();
            let primary_resp = primary_pinger.get("DUMMY").await;
            if primary_resp.is_err() {
                primary_adapter_option = None;
            } else {
                primary_adapter_option = Some(BinPrefixAdapter::new(
                    &primary_backend_addr,
                    &self.bin.to_string(),
                ));
            }

            // start scanning the secondary replica
            for j in i + 1..end {
                let secondary_backend_index = j % backs.len();
                if self.back_status[secondary_backend_index] == false {
                    continue;
                }
                let secondary_backend_addr = &backs[secondary_backend_index];
                let secondary_pinger = new_client(secondary_backend_addr).await.unwrap();
                let secondary_resp = secondary_pinger.get("DUMMY").await;
                if secondary_resp.is_err() {
                    secondary_adapter_option = None;
                } else {
                    secondary_adapter_option = Some(BinPrefixAdapter::new(
                        &secondary_backend_addr,
                        &self.bin.to_string(),
                    ));
                }
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

        // Get ther first alive and valid bin.
        let adapter_option = self.get_read_replicas_access().await;
        if adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }
        let bin_prefix_adapter = adapter_option.unwrap();

        // Get all logs. Sort and dedup.
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

        // Replay the whole log.
        let mut replay_set = HashSet::new();
        for element in logs_struct {
            if element.action == APPEND_ACTION {
                replay_set.insert(element.clock_id);
            } else if element.action == REMOVE_ACTION {
                replay_set.remove(&element.clock_id);
            } else {
                println!("The operation is not supported!!!"); // Sanity check for action.
            }
        }

        // Construct result string vector.
        let mut logs_result = vec![];
        for element in logs_struct {
            if replay_set.contains(&element.clock_id) {
                logs_result.push(element.wrapped_string);
            }
        }

        return Ok(storage::List(logs_result));
    }

    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        // Try to append to entry in the primary
        let mut primary_clock_id = 0;
        let mut log_entry_str = String::new();

        if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
            // get primary clock as unique id
            primary_clock_id = primary_bin_prefix_adapter.clock(0).await?;
            let log_entry = SortableLogRecord {
                clock_id: primary_clock_id,
                wrapped_string: kv.value.to_string(),
                action: APPEND_ACTION.to_string(),
            };
            log_entry_str = serde_json::to_string(&log_entry)?;
            let _ = primary_bin_prefix_adapter
                .list_append(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await?;
        }

        // Try to append to entry in the secondary
        if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
            let secondary_clock_id = secondary_bin_prefix_adapter.clock(primary_clock_id).await?;
            if primary_adapter_option.is_none() {
                let log_entry = SortableLogRecord {
                    clock_id: secondary_clock_id,
                    wrapped_string: kv.value.to_string(),
                    action: APPEND_ACTION.to_string(),
                };
                log_entry_str = serde_json::to_string(&log_entry)?;
            }
            let _ = secondary_bin_prefix_adapter
                .list_append(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await?;
        }

        return Ok(true);
    }

    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        // Try to remove the entry in primary
        let mut num_removal = 0;

        let mut primary_clock_id = 0;
        let mut log_entry_str = String::new();

        if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
            // get primary clock as unique id
            primary_clock_id = primary_bin_prefix_adapter.clock(0).await?;
            let log_entry = SortableLogRecord {
                clock_id: primary_clock_id,
                wrapped_string: kv.value.to_string(),
                action: REMOVE_ACTION.to_string(),
            };
            log_entry_str = serde_json::to_string(&log_entry)?;
            let _ = primary_bin_prefix_adapter
                .list_append(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await?;
        }

        // Try to remove the entry in secondary
        if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
            let secondary_clock_id = secondary_bin_prefix_adapter.clock(primary_clock_id).await?;
            if primary_adapter_option.is_none() {
                let log_entry = SortableLogRecord {
                    clock_id: secondary_clock_id,
                    wrapped_string: kv.value.to_string(),
                    action: APPEND_ACTION.to_string(),
                };
                log_entry_str = serde_json::to_string(&log_entry)?;
            }
            let _ = secondary_bin_prefix_adapter
                .list_append(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await?;
        }

        return Ok(num_removal);
    }

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let wrapped_prefx = format!("{}{}", LIST_LOG_PREFIX, p.prefix);

        let (primary_adapter_option, secondary_adapter_option) = self.get_replicas_access().await;
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
