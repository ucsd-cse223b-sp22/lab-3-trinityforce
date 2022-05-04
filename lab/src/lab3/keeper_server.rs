use super::super::keeper;
use super::super::keeper::keeper_service_client::KeeperServiceClient;
use super::bin_client::update_channel_cache;
use super::client::StorageClient;
use super::keeper_migration_helper::KeeperMigrationHelper;
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tribbler::err::TribResult;
use tribbler::storage::{KeyString, Storage};

pub struct KeeperMigrator {
    pub backs: Vec<String>,
    pub keepers: Vec<String>,
    pub this: usize,
    pub my_addr: String,
    backs_status_mut: RwLock<Vec<bool>>,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
}

impl KeeperMigrator {
    pub fn new(
        this: usize,
        keepers: Vec<String>,
        backs: &Vec<String>,
        backs_status: Vec<bool>,
    ) -> Self {
        Self {
            this,
            keepers: keepers.clone(),
            my_addr: keepers[this].clone(),
            backs: backs.clone(),
            backs_status_mut: RwLock::new(backs_status.clone()),
            channel_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub struct KeeperClockBroadcastor {
    pub backs: Vec<String>,
    pub keepers: Vec<String>,
    pub this: usize,
    channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
}

impl KeeperClockBroadcastor {
    pub fn new(this: usize, keepers: Vec<String>, backs: &Vec<String>) -> Self {
        Self {
            this,
            keepers: keepers.clone(),
            backs: backs.clone(),
            channel_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

use async_trait::async_trait;
#[async_trait]
pub trait KeeperMigratorTrait {
    async fn check_migration(&self) -> TribResult<()>;
}

#[async_trait]
impl KeeperMigratorTrait for KeeperMigrator {
    async fn check_migration(&self) -> TribResult<()> {
        // my own jurisdiction starts from self.this
        // gonna cover the gap if my next keepers are down
        // scan for next available keeper
        let mut smallest_keeper_alive = self.this;
        for i in 0..self.keepers.len() {
            if i == self.this {
                continue;
            }
            let peer_addr = format!("http://{}", &self.keepers[i]);
            let chan_res = update_channel_cache(self.channel_cache.clone(), peer_addr).await;
            if chan_res.is_err() {
                continue;
            }
            let mut client = KeeperServiceClient::new(chan_res.unwrap().clone());
            let resp_res = client.ping(keeper::Heartbeat { value: true }).await;
            if resp_res.is_err() {
                continue;
            }
            smallest_keeper_alive = cmp::min(smallest_keeper_alive, i);
        }
        // scan 300
        let mut node_join_migration_index = None;
        let mut node_leave_migration_index = None;
        let mut back_status = self.backs_status_mut.write().await;
        for i in 0..self.backs.len() {
            let chan_res =
                update_channel_cache(self.channel_cache.clone(), self.backs[i].clone()).await;
            if chan_res.is_err() {
                if (*back_status)[i] == true {
                    // node leave from jurisdiction
                    node_leave_migration_index = Some(i);
                }
                (*back_status)[i] = false;
                continue;
            }
            let mut client = StorageClient::new(&self.backs[i], Some(chan_res.unwrap().clone()));
            let clock_res = client.get("DUMMY").await;
            if clock_res.is_err() {
                // server is now down
                if (*back_status)[i] == true {
                    // node leave from jurisdiction
                    node_leave_migration_index = Some(i);
                }
                (*back_status)[i] = false;
            } else {
                // server is now up
                if (*back_status)[i] == false {
                    // node join in jurisdictioin
                    node_join_migration_index = Some(i);
                }
                (*back_status)[i] = true;
            }
        }
        if smallest_keeper_alive != self.this {
            // only smallest keeper alive is in charge of migration
            return Ok(());
        }
        let back_status_copy = back_status.clone();
        drop(back_status);
        if node_join_migration_index.is_some() {
            self.migrate_to_joined_node(node_join_migration_index.unwrap(), back_status_copy)
                .await?;
        } else if node_leave_migration_index.is_some() {
            self.migrate_to_left_node(node_leave_migration_index.unwrap(), back_status_copy)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait KeeperClockBroadcastorTrait {
    async fn broadcast_logical_clock(&self) -> TribResult<()>;
    async fn get_clock_send(&self, i: usize) -> TribResult<u64>;
    async fn update_clock_send(&self, i: usize, max_clock: u64) -> TribResult<()>;
}

#[async_trait]
impl KeeperClockBroadcastorTrait for KeeperClockBroadcastor {
    async fn get_clock_send(&self, i: usize) -> TribResult<u64> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.backs[i].clone()).await?;
        let client = StorageClient::new(&self.backs[i], Some(chan));
        let target_clock = client.clock(0).await?;
        // println!("getting clock: {}", target_clock);
        return Ok(target_clock);
    }

    async fn update_clock_send(&self, i: usize, max_clock: u64) -> TribResult<()> {
        let chan = update_channel_cache(self.channel_cache.clone(), self.backs[i].clone()).await?;
        let client = StorageClient::new(&self.backs[i], Some(chan));
        client.clock(max_clock).await?;
        // println!("setting clock: {}", max_clock);
        return Ok(());
    }

    async fn broadcast_logical_clock(&self) -> TribResult<()> {
        let mut largest_keeper_alive = self.this;
        for i in 0..self.keepers.len() {
            if i == self.this {
                continue;
            }
            let peer_addr = format!("http://{}", &self.keepers[i]);
            let chan_res = update_channel_cache(self.channel_cache.clone(), peer_addr).await;
            if chan_res.is_err() {
                continue;
            }
            let mut client = KeeperServiceClient::new(chan_res.unwrap().clone());
            let resp_res = client.ping(keeper::Heartbeat { value: true }).await;
            if resp_res.is_err() {
                continue;
            }
            largest_keeper_alive = cmp::max(largest_keeper_alive, i);
        }
        if largest_keeper_alive != self.this {
            // only smallest keeper alive is in charge of migration
            return Ok(());
        }

        let len = self.backs.len();

        let mut max_clock: u64 = 0;

        for i in 0..len {
            let recv_clock_res = self.get_clock_send(i).await;
            if recv_clock_res.is_err() {
                continue;
            }
            let recv_clock = recv_clock_res.unwrap();
            // println!("getting {} and current max: {}", recv_clock, max_clock);
            max_clock = cmp::max(max_clock, recv_clock);
        }

        for i in 0..len {
            let _ = self.update_clock_send(i, max_clock).await;
        }
        Ok(())
    }
}
