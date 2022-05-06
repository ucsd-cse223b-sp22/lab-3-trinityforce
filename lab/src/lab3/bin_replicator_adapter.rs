use super::bin_client::update_channel_cache;
use super::bin_prefix_adapter::BinPrefixAdapter;
use super::client::StorageClient;
use super::constants::{
    APPEND_ACTION, LIST_LOG_PREFIX, REMOVE_ACTION, STR_LOG_PREFIX, VALIDATION_BIT_KEY,
};
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
}

impl BinReplicatorAdapter {
    pub fn new(
        hash_index: u32,
        backs: Vec<String>,
        bin: &str,
        back_status: Vec<bool>,
        channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    ) -> Self {
        Self {
            hash_index,
            backs: backs.clone(),
            bin: bin.to_string(),
            back_status: back_status.clone(),
            channel_cache,
        }
    }
}

use async_trait::async_trait;
#[async_trait] // VERY IMPORTANT !!!=
impl storage::KeyString for BinReplicatorAdapter {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let wrapped_key = format!("{}{}", STR_LOG_PREFIX, key);

        // Get ther first alive and valid bin.
        /*let adapter_option = self.get_read_replicas_access().await;
        if adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }
        let bin_prefix_adapter = adapter_option.unwrap();*/
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_read_replicas_access_new().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        // Get all logs
        let logs_string = self
            .get_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
            )
            .await?;
        //let logs_string = bin_prefix_adapter.list_get(&wrapped_key).await?.0;
        let mut logs_struct = vec![];
        for element in logs_string {
            let log_entry: SortableLogRecord = serde_json::from_str(&element).unwrap();
            logs_struct.push(log_entry);
        }

        // Get the log with largest clock

        let mut max_clock: u64 = 0;
        let mut result_str = "";
        for element in logs_struct.iter() {
            if element.clock_id >= max_clock {
                max_clock = element.clock_id;
                result_str = &element.wrapped_string;
            }
        }
        if result_str == "" {
            return Ok(None);
        }
        return Ok(Some(result_str.to_string()));
    }

    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let wrapped_key = format!("{}{}", STR_LOG_PREFIX, kv.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        // Try to append the entry in primary and secondary
        let _ = self
            .append_log_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kv,
                APPEND_ACTION,
            )
            .await?;

        return Ok(true);
    }

    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let wrapped_prefx = format!("{}{}", STR_LOG_PREFIX, p.prefix);

        // Get the first alive and valid bin.
        let adapter_option = self.get_read_replicas_access().await;
        if adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let bin_prefix_adapter = adapter_option.unwrap();

        let potential_keys = bin_prefix_adapter
            .list_keys(&storage::Pattern {
                prefix: wrapped_prefx.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?
            .0;

        let mut true_keys = vec![];
        for key in potential_keys {
            let logs_string = bin_prefix_adapter.list_get(&key).await?.0;
            let mut logs_struct = vec![];
            for element in logs_string {
                let log_entry: SortableLogRecord = serde_json::from_str(&element).unwrap();
                logs_struct.push(log_entry);
            }

            // Get the log with largest clock

            let mut max_clock: u64 = 0;
            let mut result_str = "";
            for element in logs_struct.iter() {
                if element.clock_id > max_clock {
                    max_clock = element.clock_id;
                    result_str = &element.wrapped_string;
                }
            }
            if result_str != "" {
                let extracted_key = &key[STR_LOG_PREFIX.len()..];
                true_keys.push(extracted_key.to_string())
            }
        }
        true_keys.sort_unstable();
        return Ok(storage::List(true_keys));
    }
}

#[async_trait]
pub trait BinReplicatorHelper {
    async fn get_read_replicas_access(&self) -> Option<BinPrefixAdapter>; // starting from returning the first living valid machine
    async fn get_write_replicas_access(
        &self,
    ) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>); // return two first trues from back_status
    async fn get_sorted_log_struct(
        &self,
        bin_prefix_adapter: &BinPrefixAdapter,
        wrapped_key: &str,
    ) -> TribResult<Vec<SortableLogRecord>>; // Get all logs. Sort and dedup.
    async fn append_log_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
        action: &str,
    ) -> TribResult<(u64, u64)>;
    async fn get_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
    ) -> TribResult<Vec<String>>;
    async fn keys_action(
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
    async fn get_read_replicas_access_new(
        &self,
    ) -> (Option<BinPrefixAdapter>, Option<BinPrefixAdapter>) {
        let backs = self.backs.clone();
        let start = self.hash_index as usize;
        let end = start + backs.len();
        let mut primary_adapter_option = None;
        let mut secondary_adapter_option = None;
        // println!("start {}, end {}", start, end);
        // println!("back_status: {:?};", self.back_status);
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
                println!("Channel get failed!");
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
                    println!("Channel get failed!");
                }
                break;
            }
            break;
        }
        return (primary_adapter_option, secondary_adapter_option);
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
                println!("Channel get failed!");
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
        // println!("start {}, end {}", start, end);
        // println!("back_status: {:?};", self.back_status);
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
                println!("Channel get failed!");
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
                    println!("Channel get failed!");
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

    async fn get_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
    ) -> TribResult<Vec<String>> {
        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap().0);
            }
            let vec_secondary = secondary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        } else if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let vec_primary = primary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_primary.is_err() {
                return Ok(vec_primary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        } else if secondary_adapter_option.is_some() {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let vec_secondary = secondary_bin_prefix_adapter.list_get(wrapped_key).await;
            if !vec_secondary.is_err() {
                return Ok(vec_secondary.unwrap().0);
            }
            return Ok(vec![] as Vec<String>);
        }
        return Ok(vec![] as Vec<String>);
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

    async fn append_log_action(
        &self,
        primary_adapter_option: &Option<BinPrefixAdapter>,
        secondary_adapter_option: &Option<BinPrefixAdapter>,
        wrapped_key: &str,
        kv: &storage::KeyValue,
        action: &str,
    ) -> TribResult<(u64, u64)> {
        let mut is_primary_fail = false;
        let mut is_secondary_fail = false;
        let mut primary_clock_id = 0;
        let mut secondary_clock_id = 0;

        if primary_adapter_option.is_some() && secondary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            // get primary clock as unique id
            let primary_clock_id_res = primary_bin_prefix_adapter.clock(0).await;
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            // get secondary clock as unique id if primary is none
            let secondary_clock_id_res = secondary_bin_prefix_adapter.clock(primary_clock_id).await;
            if primary_clock_id_res.is_err() {
                is_primary_fail = true;
                secondary_clock_id = secondary_clock_id_res.unwrap();
            } else if secondary_clock_id_res.is_err() {
                is_secondary_fail = true;
                primary_clock_id = primary_clock_id_res.unwrap();
            } else {
                secondary_clock_id = secondary_clock_id_res.unwrap();
                primary_clock_id = primary_clock_id_res.unwrap();
            }
        } else if primary_adapter_option.is_some() {
            is_secondary_fail = true;
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            primary_clock_id = primary_bin_prefix_adapter.clock(0).await?;
        } else {
            is_primary_fail = true;
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            secondary_clock_id = secondary_bin_prefix_adapter.clock(primary_clock_id).await?;
        }
        let max_clock = cmp::max(primary_clock_id, secondary_clock_id);
        let log_entry = SortableLogRecord {
            clock_id: max_clock,
            wrapped_string: kv.value.to_string(),
            action: action.to_string(),
        };
        let log_entry_str = serde_json::to_string(&log_entry)?;
        if !is_secondary_fail {
            let secondary_bin_prefix_adapter = secondary_adapter_option.as_ref().unwrap();
            let _ = secondary_bin_prefix_adapter
                .list_append(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await;
            let _ = secondary_bin_prefix_adapter.clock(max_clock).await;
        }
        if !is_primary_fail {
            let primary_bin_prefix_adapter = primary_adapter_option.as_ref().unwrap();
            let _ = primary_bin_prefix_adapter
                .list_append(&storage::KeyValue {
                    key: wrapped_key.to_string(),
                    value: log_entry_str.to_string(),
                })
                .await;
            let _ = primary_bin_prefix_adapter.clock(max_clock).await;
        }
        Ok((primary_clock_id, secondary_clock_id))
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
        let logs_struct = self
            .get_sorted_log_struct(&bin_prefix_adapter, &wrapped_key)
            .await?;

        // Replay the whole log.
        let mut logs_result = vec![];
        let mut replay_set = HashSet::new();
        for element in logs_struct.iter().rev() {
            if replay_set.contains(&element.wrapped_string) {
                continue;
            }
            if element.action == APPEND_ACTION {
                logs_result.push(element.wrapped_string.clone());
            } else if element.action == REMOVE_ACTION {
                replay_set.insert(&element.wrapped_string);
            } else {
                println!("The operation is not supported!!!"); // Sanity check for action.
            }
        }
        logs_result.reverse();

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

        // Try to append the entry in primary and secondary
        let _ = self
            .append_log_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kv,
                APPEND_ACTION,
            )
            .await?;

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

        // Try to remove the entry in primary and secondary
        let (primary_clock_id, secondary_clock_id) = self
            .append_log_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kv,
                REMOVE_ACTION,
            )
            .await?;

        let chosen_clock_id = cmp::max(primary_clock_id, secondary_clock_id);
        let adapter_option = self.get_read_replicas_access().await;
        if adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let bin_prefix_adapter = adapter_option.unwrap();
        let logs_struct = self
            .get_sorted_log_struct(&bin_prefix_adapter, &wrapped_key)
            .await?;
        // println!("chosen_clock_id: {}", chosen_clock_id);
        // println!("{:?}", logs_struct);

        let mut removal_num = 0;
        for element in logs_struct.iter().rev() {
            if element.clock_id >= chosen_clock_id {
                continue;
            }
            if element.wrapped_string != kv.value {
                continue;
            }
            if element.action == APPEND_ACTION {
                removal_num += 1;
            } else if element.action == REMOVE_ACTION {
                break;
            } else {
                println!("The operation is not supported!!!"); // Sanity check for action.
            }
        }

        return Ok(removal_num);
    }

    /*async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let wrapped_key = format!("{}{}", LIST_LOG_PREFIX, kv.key);

        // Get first two "valid" bins.
        let (primary_adapter_option, secondary_adapter_option) =
            self.get_write_replicas_access().await;
        if primary_adapter_option.is_none() && secondary_adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        // Try to remove the entry in primary and secondary
        let (primary_clock_id, secondary_clock_id) = self
            .append_log_action(
                &primary_adapter_option,
                &secondary_adapter_option,
                &wrapped_key,
                kv,
                REMOVE_ACTION,
            )
            .await?;

        // Try to count the removal number. Assume removal number of primary is always greater or equal to that in secondary.
        let mut logs_struct = vec![];
        let mut chosen_clock_id = 0;

        if primary_adapter_option.is_some() {
            let primary_bin_prefix_adapter = primary_adapter_option.unwrap();
            // Get all logs. Sort and dedup.
            logs_struct = self
                .get_sorted_log_struct(&primary_bin_prefix_adapter, &wrapped_key)
                .await?;
            chosen_clock_id = primary_clock_id;
        } else {
            let secondary_bin_prefix_adapter = secondary_adapter_option.unwrap();
            // Get all logs. Sort and dedup.
            logs_struct = self
                .get_sorted_log_struct(&secondary_bin_prefix_adapter, &wrapped_key)
                .await?;
            chosen_clock_id = secondary_clock_id;
        }

        let mut removal_num = 0;
        for element in logs_struct.iter().rev() {
            if element.clock_id >= chosen_clock_id {
                continue;
            }
            if element.wrapped_string != kv.value {
                continue;
            }
            if element.action == APPEND_ACTION {
                removal_num += 1;
            } else if element.action == REMOVE_ACTION {
                break;
            } else {
                println!("The operation is not supported!!!"); // Sanity check for action.
            }
        }

        return Ok(removal_num);
    }*/

    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let wrapped_prefx = format!("{}{}", LIST_LOG_PREFIX, p.prefix);

        // Get ther first alive and valid bin.
        let adapter_option = self.get_read_replicas_access().await;
        if adapter_option.is_none() {
            return Err(Box::new(NotEnoughServers));
        }

        let bin_prefix_adapter = adapter_option.unwrap();

        let potential_keys = bin_prefix_adapter
            .list_keys(&storage::Pattern {
                prefix: wrapped_prefx.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?
            .0;

        let mut true_keys = vec![];
        for key in potential_keys {
            let logs_struct = self
                .get_sorted_log_struct(&bin_prefix_adapter, &key)
                .await?;

            // Replay the whole log.
            let mut replay_set = HashSet::new();
            for element in logs_struct.iter().rev() {
                if replay_set.contains(&element.wrapped_string) {
                    continue;
                }
                if element.action == APPEND_ACTION {
                    let key_str = key.as_str();
                    let extracted_key = &key_str[LIST_LOG_PREFIX.len()..];
                    true_keys.push(extracted_key.to_string());
                    break;
                } else if element.action == REMOVE_ACTION {
                    replay_set.insert(&element.wrapped_string);
                } else {
                    println!("The operation is not supported!!!"); // Sanity check for action.
                }
            }
        }
        true_keys.sort_unstable();

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
                println!("Should not happen!")
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
