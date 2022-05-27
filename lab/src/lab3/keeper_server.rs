use super::super::keeper;
use super::super::keeper::keeper_service_client::KeeperServiceClient;
use super::bin_client::init_lock_servers_addresses;
use super::bin_client::update_channel_cache;
use super::bin_client::BinStorageClient;
use super::client::StorageClient;
use super::constants::{
    BACK_STATUS_STORE_KEY, KEEPER_STORE_NAME, MIGRATION_LOG_KEY, SCAN_INTERVAL_CONSTANT,
};
use super::keeper_helper;
use super::lock_client::LockClient;
use serde::Deserialize;
use serde::Serialize;
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tribbler::err::TribResult;
use tribbler::storage::KeyValue;
use tribbler::storage::{KeyString, Storage};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MigrationLog {
    pub back_id: usize,
    pub leave: bool,
}
pub struct KeeperMigrator {
    pub backs: Vec<String>,
    pub keepers: Vec<String>,
    pub this: usize,
    pub my_addr: String,
    pub activated: bool,
    backs_status_mut: RwLock<Vec<bool>>,
    pub channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    pub lock_client: Arc<LockClient>,
}

impl KeeperMigrator {
    pub fn new(
        this: usize,
        keepers: Vec<String>,
        backs: &Vec<String>,
        backs_status: Vec<bool>,
    ) -> Self {
        let lock_addrs = init_lock_servers_addresses();
        Self {
            this,
            keepers: keepers.clone(),
            my_addr: keepers[this].clone(),
            backs: backs.clone(),
            activated: false,
            backs_status_mut: RwLock::new(backs_status.clone()),
            channel_cache: Arc::new(RwLock::new(HashMap::new())),
            lock_client: Arc::new(LockClient::new(lock_addrs, true)),
        }
    }

    pub fn new_with_channel(
        this: usize,
        keepers: Vec<String>,
        backs: &Vec<String>,
        backs_status: Vec<bool>,
        channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    ) -> Self {
        let lock_addrs = init_lock_servers_addresses();
        Self {
            this,
            keepers: keepers.clone(),
            my_addr: keepers[this].clone(),
            backs: backs.clone(),
            activated: false,
            backs_status_mut: RwLock::new(backs_status.clone()),
            channel_cache,
            lock_client: Arc::new(LockClient::new(lock_addrs, true)),
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

    pub fn new_with_channel(
        this: usize,
        keepers: Vec<String>,
        backs: &Vec<String>,
        channel_cache: Arc<RwLock<HashMap<String, Channel>>>,
    ) -> Self {
        Self {
            this,
            keepers: keepers.clone(),
            backs: backs.clone(),
            channel_cache,
        }
    }
}

use async_trait::async_trait;
#[async_trait]
pub trait KeeperMigratorTrait {
    async fn check_migration(&mut self) -> TribResult<()>;
}

#[async_trait]
impl KeeperMigratorTrait for KeeperMigrator {
    async fn check_migration<'life0>(self: &'life0 mut KeeperMigrator) -> TribResult<()> {
        // my own jurisdiction starts from self.this
        // gonna cover the gap if my next keepers are down
        // scan for next available keeper
        let mut smallest_keeper_alive = self.this;
        //println!("{}, begin broadcasting", self.this);
        for i in 0..self.keepers.len() {
            if i == self.this {
                continue;
            }
            let chan_res =
                update_channel_cache(self.channel_cache.clone(), self.keepers[i].clone()).await;
            if chan_res.is_err() {
                // println!("ping channel failed: from {}, to {}", self.this, i);
                continue;
            }
            let mut client = KeeperServiceClient::new(chan_res.unwrap().clone());
            let resp_res = client.ping(keeper::Heartbeat { value: true }).await;
            if resp_res.is_err() {
                // println!("ping failed: from {}, to {}", self.this, i);
                continue;
            }
            // println!("ping success: from {}, to {}", self.this, i);
            smallest_keeper_alive = cmp::min(smallest_keeper_alive, i);
        }
        if smallest_keeper_alive != self.this {
            // only smallest keeper alive is in charge of migration
            self.activated = false;
            return Ok(());
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
            let client = StorageClient::new(&self.backs[i], Some(chan_res.unwrap().clone()));
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
        /*if smallest_keeper_alive != self.this {
            // only smallest keeper alive is in charge of migration
            return Ok(());
        }*/
        let back_status_copy = back_status.clone();
        let mut bin_store =
            BinStorageClient::new_with_channel(&self.backs, self.channel_cache.clone());
        bin_store.update_lock_client(self.lock_client.clone());
        let bin_client = bin_store.bin_with_backs(KEEPER_STORE_NAME, &back_status_copy)?;

        // if the keeper is in its first round, fetch back status and migration log
        if !self.activated {
            // println!("Keeper {} is promoted", self.this);
            node_join_migration_index = None;
            node_leave_migration_index = None;
            println!("BACK_STATUS_STORE_KEY START 0");
            let back_status_str = bin_client.get(BACK_STATUS_STORE_KEY).await?;
            println!("BACK_STATUS_STORE_KEY END 1");
            println!("MIGRATION_LOG_KEY START 2");
            let migration_log_str = bin_client.get(MIGRATION_LOG_KEY).await?;
            println!("MIGRATION_LOG_KEY START 3");
            if back_status_str.is_none() {
                println!("BACK_STATUS_STORE_KEY START 4");
                bin_client
                    .set(&KeyValue {
                        key: BACK_STATUS_STORE_KEY.to_string(),
                        value: serde_json::to_string(&back_status_copy)?,
                    })
                    .await?;
                println!("BACK_STATUS_STORE_KEY START 5");
                self.activated = true;
                // println!("Get null back status");
            }
            if migration_log_str.is_none() {
                if !back_status_str.is_none() {
                    let back_status_old: Vec<bool> =
                        serde_json::from_str(&back_status_str.unwrap())?;
                    for i in 0..back_status.len() {
                        if back_status[i] != back_status_old[i] {
                            if back_status[i] {
                                node_join_migration_index = Some(i);
                            } else {
                                node_leave_migration_index = Some(i);
                            }
                        }
                    }
                }
            } else {
                let migration_log: MigrationLog =
                    serde_json::from_str(&migration_log_str.unwrap())?;
                if migration_log.leave {
                    node_leave_migration_index = Some(migration_log.back_id);
                } else {
                    node_join_migration_index = Some(migration_log.back_id);
                }
            }
        }
        drop(back_status);

        if node_join_migration_index.is_some() {
            self.activated = true;
            let node_join_index = node_join_migration_index.unwrap();
            let log_str = serde_json::to_string(&MigrationLog {
                back_id: node_join_index,
                leave: false,
            })?;
            tokio::time::sleep(Duration::from_secs(SCAN_INTERVAL_CONSTANT)).await;
            // append migration log
            println!("MIGRATION_LOG_KEY START 10");
            bin_client
                .set(&KeyValue {
                    key: MIGRATION_LOG_KEY.to_string(),
                    value: log_str.clone(),
                })
                .await?;
            println!("MIGRATION_LOG_KEY START 11");
            // update back status
            println!("BACK_STATUS_STORE_KEY START 7");
            bin_client
                .set(&KeyValue {
                    key: BACK_STATUS_STORE_KEY.to_string(),
                    value: serde_json::to_string(&back_status_copy)?,
                })
                .await?;
            println!("BACK_STATUS_STORE_KEY START 8");
            println!("Start migrate_to_joined_node");
            keeper_helper::migrate_to_joined_node(
                self.backs.clone(),
                self.channel_cache.clone(),
                node_join_index,
                back_status_copy,
                self.lock_client.clone(),
            )
            .await?;
            // println!("End migrate_to_joined_node");
            println!("MIGRATION_LOG_KEY START 20");
            bin_client
                .set(&KeyValue {
                    key: MIGRATION_LOG_KEY.to_string(),
                    value: "".to_string(),
                })
                .await?;
            println!("MIGRATION_LOG_KEY START 21");
            return Ok(());
        } else if node_leave_migration_index.is_some() {
            self.activated = true;
            let node_leave_index = node_leave_migration_index.unwrap();
            let log_str = serde_json::to_string(&MigrationLog {
                back_id: node_leave_index,
                leave: true,
            })?;
            tokio::time::sleep(Duration::from_secs(SCAN_INTERVAL_CONSTANT)).await;
            // append migration log
            println!("MIGRATION_LOG_KEY START 48");
            bin_client
                .set(&KeyValue {
                    key: MIGRATION_LOG_KEY.to_string(),
                    value: log_str.clone(),
                })
                .await?;
            // update back status
            println!("BACK_STATUS_STORE_KEY START 33");
            bin_client
                .set(&KeyValue {
                    key: BACK_STATUS_STORE_KEY.to_string(),
                    value: serde_json::to_string(&back_status_copy)?,
                })
                .await?;
            println!("BACK_STATUS_STORE_KEY START 34");
            // println!("Start migrate_to_left_node");
            keeper_helper::migrate_to_left_node(
                self.backs.clone(),
                self.channel_cache.clone(),
                node_leave_migration_index.unwrap(),
                back_status_copy,
                self.lock_client.clone(),
            )
            .await?;
            // println!("End migrate_to_left_node");
            println!("MIGRATION_LOG_KEY START 49");
            let res = bin_client
                .set(&KeyValue {
                    key: MIGRATION_LOG_KEY.to_string(),
                    value: "".to_string(),
                })
                .await;
            println!("MIGRATION_LOG_KEY START 50");
            if res.is_err() {
                // println!("Send back status error");
                // println!("{:?}", res);
            }
            return Ok(());
        }
        // only update back status if the keeper is in its first round or view change happens
        if !self.activated {
            self.activated = true;
            println!("BACK_STATUS_STORE_KEY START 37");
            bin_client
                .set(&KeyValue {
                    key: BACK_STATUS_STORE_KEY.to_string(),
                    value: serde_json::to_string(&back_status_copy)?,
                })
                .await?;
            println!("BACK_STATUS_STORE_KEY START 38");
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
            let chan_res =
                update_channel_cache(self.channel_cache.clone(), self.keepers[i].clone()).await;
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
