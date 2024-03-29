use crate::lab3::bin_client::update_channel_cache;
use crate::lab3::client::StorageClient;

use super::constants::{LIST_LOG_KEYWORD, STR_LOG_KEYWORD, VALIDATION_BIT_KEY};
use super::keeper_server::KeeperMigrator;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use tribbler::err::TribResult;
use tribbler::storage::{KeyList, KeyString, KeyValue, Pattern};

use async_trait::async_trait;
#[async_trait]
pub trait KeeperMigrationHelper {
    async fn migrate_to_joined_node(
        &self,
        joined_node_index: usize,
        back_status: Vec<bool>,
    ) -> TribResult<()>;
    async fn migrate_to_left_node(
        &self,
        leave_node_index: usize,
        back_status: Vec<bool>,
    ) -> TribResult<()>;
    // interval_start: inclusive; interval_end: inclusive
    async fn migrate_data(
        &self,
        from: usize,
        to: usize,
        interval_start: usize,
        interval_end: usize,
    ) -> TribResult<()>;
    // interval_start: inclusive; interval_end: inclusive
    fn falls_into_interval(
        &self,
        target_string: String,
        interval_start: usize,
        interval_end: usize,
    ) -> bool;
    async fn extract_raw_keys_from_addr(&self, addr: &str) -> TribResult<Vec<String>>;
    fn extract_bin_name_from_raw_key(&self, raw_key: &str) -> String;
}

#[async_trait]
impl KeeperMigrationHelper for KeeperMigrator {
    async fn extract_raw_keys_from_addr(&self, addr: &str) -> TribResult<Vec<String>> {
        let chan_res = update_channel_cache(self.channel_cache.clone(), addr.to_string()).await?;
        let raw_client = StorageClient::new(addr, Some(chan_res));
        //let raw_client = new_client(addr).await?;
        let keys_list = raw_client
            .list_keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await?
            .0;
        // println!("get all keys in {}: {:?}", addr, keys_list);
        let mut filtered = vec![];
        for element in keys_list {
            let splits = &element.split("::").collect::<Vec<&str>>();
            if splits.len() <= 2 {
                continue;
            }
            let identifier = splits[1];
            if identifier == LIST_LOG_KEYWORD || identifier == STR_LOG_KEYWORD {
                filtered.push(element.clone());
            }
        }
        return Ok(filtered);
    }

    fn extract_bin_name_from_raw_key(&self, raw_key: &str) -> String {
        let splits = raw_key.split("::").collect::<Vec<&str>>();
        if splits.len() < 2 {
            return "".to_string();
        }
        return splits[0].to_string();
    }

    // backend servers index
    // interval_start: inclusive; interval_end: inclusive
    fn falls_into_interval(
        &self,
        target_string: String,
        interval_start: usize,
        interval_end: usize,
    ) -> bool {
        let mut hasher = DefaultHasher::new();
        target_string.hash(&mut hasher);
        let hash_res = hasher.finish();
        let num_backs = self.backs.clone().len() as u64;
        let delta = (hash_res % num_backs) as usize;
        if interval_start == interval_end {
            return delta == interval_end;
        }
        if interval_start < interval_end {
            return interval_start <= delta && delta <= interval_end;
        } else {
            return interval_start <= delta || delta <= interval_end;
        }
    }

    async fn migrate_data(
        &self,
        from: usize,
        to: usize,
        interval_start: usize,
        interval_end: usize,
    ) -> TribResult<()> {
        // println!(
        //     "migration: from {}, to {}, interval start {}, interval end {}",
        //     from, to, interval_start, interval_end
        // );
        let addr_from = &self.backs[from];
        let addr_to = &self.backs[to];
        let chan_from =
            update_channel_cache(self.channel_cache.clone(), addr_from.to_string()).await?;
        let chan_to = update_channel_cache(self.channel_cache.clone(), addr_to.to_string()).await?;
        let client_from = StorageClient::new(addr_from, Some(chan_from));
        let client_to = StorageClient::new(addr_to, Some(chan_to));
        let raw_key_list = self.extract_raw_keys_from_addr(addr_from).await?;
        for element in raw_key_list.iter() {
            let bin_name = self.extract_bin_name_from_raw_key(element);
            // println!("bin name: {}, element: {}", &bin_name, &element);
            if bin_name == "" {
                continue;
            }
            let mut hasher = DefaultHasher::new();
            bin_name.hash(&mut hasher);
            if !self.falls_into_interval(bin_name, interval_start, interval_end) {
                continue;
            }
            let values_from = client_from.list_get(&element).await?.0;
            let values_to = client_to.list_get(&element).await?.0;
            let mut hash_str = HashSet::new();
            for s in values_to {
                hash_str.insert(s);
            }
            for s in values_from {
                if hash_str.contains(&s) {
                    continue;
                }
                client_to
                    .list_append(&KeyValue {
                        key: element.to_string(),
                        value: s.to_string(),
                    })
                    .await?;
                // println!(
                //     "{} start appending to {}---key: {}, value: {}",
                //     addr_from.to_string(),
                //     addr_to.to_string(),
                //     element.to_string(),
                //     s.to_string()
                // );
            }
        }
        client_to
            .set(&KeyValue {
                key: VALIDATION_BIT_KEY.to_string(),
                value: "true".to_string(),
            })
            .await?;
        Ok(())
    }

    async fn migrate_to_joined_node(
        &self,
        joined_node_index: usize,
        back_status: Vec<bool>,
    ) -> TribResult<()> {
        let backs_len = self.backs.len();
        let mut index = joined_node_index + backs_len - 1;
        let mut interval_start: usize = 0;
        let interval_end: usize = joined_node_index as usize;
        while index >= joined_node_index {
            if back_status[index % backs_len] {
                if index == joined_node_index {
                    // if the predecessor is itself, which means it is the only node.
                    return Ok(());
                }
                index = index - 1;
                // find the second predecessor
                while index >= joined_node_index {
                    if back_status[index % backs_len] {
                        interval_start = index % backs_len as usize;
                        break;
                    }
                    index = index - 1;
                }
                break;
            }
            index = index - 1;
        }
        // exclude start itself
        interval_start = (interval_start + 1) % backs_len;
        index = joined_node_index + 1;
        // find the successor, and fetch all the data from successor
        while index <= joined_node_index + backs_len {
            if back_status[index % backs_len] {
                self.migrate_data(
                    index % backs_len,
                    joined_node_index,
                    interval_start,
                    interval_end,
                )
                .await?;
            }
            index = index + 1;
        }
        Ok(())
    }

    async fn migrate_to_left_node(
        &self,
        left_node_index: usize,
        back_status: Vec<bool>,
    ) -> TribResult<()> {
        let backs_len = self.backs.len();
        let mut interval_start: usize = 0;
        let mut interval_end: usize = left_node_index as usize;
        let mut successor: usize = 0;
        let mut second_successor: usize = 0;
        let mut index = left_node_index + 1;
        let mut first_predecessor: usize = 0;
        let mut second_predecessor: usize = 0;
        // find the first and second successor
        while index <= left_node_index + backs_len {
            // if cannot find succesor, which means it is the last node in system, crash
            if index % backs_len == left_node_index {
                return Ok(());
            }
            if back_status[index % backs_len] {
                successor = index % backs_len;
                index = index + 1;
                while index <= left_node_index + backs_len {
                    // if cannot find second successor, which means there is only one successor left
                    if index % backs_len == left_node_index {
                        return Ok(());
                    }
                    if back_status[index % backs_len] {
                        second_successor = index % backs_len;
                        break;
                    }
                }
                break;
            }
            index = index + 1;
        }
        // try to find the first and second predecessor of successor
        index = successor + backs_len - 1;
        while index >= successor {
            if index == successor {
                // if the predecessor is itself, which means it is the only node.
                return Ok(());
            }
            if back_status[index % backs_len] {
                first_predecessor = index % backs_len;
                index = index - 1;
                // find the second predecessor
                while index >= successor {
                    if back_status[index % backs_len] {
                        second_predecessor = index % backs_len;
                        break;
                    }
                    index = index - 1;
                }
                break;
            }
            index = index - 1;
        }
        // exclude start itself
        interval_start = (second_predecessor + 1) % backs_len;
        interval_end = first_predecessor;
        let migrate_first =
            self.migrate_data(first_predecessor, successor, interval_start, interval_end);
        interval_start = (first_predecessor + 1) % backs_len;
        interval_end = left_node_index;
        let migrate_second =
            self.migrate_data(successor, second_successor, interval_start, interval_end);
        migrate_first.await?;
        migrate_second.await?;
        Ok(())
    }
}
