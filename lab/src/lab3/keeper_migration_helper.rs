use super::constants::{LIST_LOG_PREFIX, STR_LOG_PREFIX, VALIDATION_BIT_KEY};
use super::keeper_server::KeeperServer;
use super::new_client;
use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::{self, SyncSender};
use std::sync::Mutex;
use tribbler::err::TribResult;
use tribbler::storage::{KeyValue, Pattern, Storage};

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
    // interval_start: non-inclusive; interval_end: inclusive
    async fn migrate_data(
        &self,
        from: usize,
        to: usize,
        interval_start: usize,
        interval_end: usize,
    ) -> TribResult<()>;
    fn falls_into_interval(
        &self,
        target_string: String,
        interval_start: usize,
        interval_end: usize,
    ) -> bool;
}

#[async_trait]
impl KeeperMigrationHelper for KeeperServer {
    // backend servers index
    // interval_start: non-inclusive; interval_end: inclusive
    fn falls_into_interval(
        &self,
        target_string: String,
        interval_start: usize,
        interval_end: usize,
    ) -> bool {
        if interval_start == interval_end {
            return false;
        }
        let mut hasher = DefaultHasher::new();
        target_string.hash(&mut hasher);
        let hash_res = hasher.finish();
        let num_backs = self.backs.clone().len() as u64;
        let delta = (hash_res % num_backs) as usize;
        if interval_start < interval_end {
            return interval_start < delta && delta <= interval_end;
        } else {
            return interval_start < delta || delta <= interval_end;
        }
    }

    async fn migrate_data(
        &self,
        from: usize,
        to: usize,
        interval_start: usize,
        interval_end: usize,
    ) -> TribResult<()> {
        let addr_from = &self.backs[from];
        let addr_to = &self.backs[to];
        let client_from = new_client(addr_from).await?;
        let client_to = new_client(addr_to).await?;
        let keys_list_from = client_from
            .list_keys(&Pattern {
                prefix: LIST_LOG_PREFIX.to_string(),
                suffix: "".to_string(),
            })
            .await?
            .0;
        let keys_str_from = client_from
            .list_keys(&Pattern {
                prefix: STR_LOG_PREFIX.to_string(),
                suffix: "".to_string(),
            })
            .await?
            .0;
        let mut keys_from = vec![];
        for element in keys_list_from {
            keys_from.push(element);
        }
        for element in keys_str_from {
            keys_from.push(element);
        }
        for element in keys_from.iter() {
            let splits = element.split("::").collect::<Vec<&str>>();
            let mut bin_name = "";
            if splits.len() >= 2 {
                bin_name = splits[1];
            }
            let mut hasher = DefaultHasher::new();
            bin_name.hash(&mut hasher);
            let hash_res = hasher.finish();
            if interval_start < interval_end {
                if hash_res <= interval_start as u64 || hash_res > interval_end as u64 {
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
                            value: s,
                        })
                        .await?;
                }
            }
        }
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
        let mut interval_end: usize = joined_node_index as usize;
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
        self.migrate_data(first_predecessor, successor, interval_start, interval_end)
            .await?;
        interval_start = (first_predecessor + 1) % backs_len;
        interval_end = left_node_index;
        self.migrate_data(successor, second_successor, interval_start, interval_end)
            .await?;
        Ok(())
    }
}
