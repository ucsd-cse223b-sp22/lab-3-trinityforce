use super::super::keeper;
use super::super::keeper::keeper_service_client::KeeperServiceClient;
use super::client::StorageClient;
use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::{self, SyncSender};
use std::sync::Mutex;
use tribbler::err::TribResult;
use tribbler::storage::Storage;

pub struct KeeperServer {
    pub backs: Vec<String>,
    pub keepers: Vec<String>,
    pub this: usize,
    pub my_addr: String,
    backs_status_mut: Mutex<Vec<bool>>,
}

impl KeeperServer {
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
            backs_status_mut: Mutex::new(backs_status.clone()),
        }
    }
}

use async_trait::async_trait;
#[async_trait]
pub trait KeeperHelper {
    async fn check_migration(&self) -> TribResult<()>;
    async fn broadcast_logical_clock(&self) -> TribResult<()>;
    async fn get_clock_send(&self, i: usize) -> TribResult<u64>;
    async fn update_clock_send(&self, i: usize, max_clock: u64) -> TribResult<()>;
    // jurisdiction start inclusive, jurisdiction end non-inclusive:
    fn falls_into_jurisdiction(
        &self,
        backend_index: usize,
        juris_start: usize,
        juris_end: usize,
    ) -> bool;
}

#[async_trait]
impl KeeperHelper for KeeperServer {
    fn falls_into_jurisdiction(
        &self,
        backend_index: usize,
        juris_start: usize,
        juris_end: usize,
    ) -> bool {
        // juris_start is inclusive, juris_end is exclusive, unless they overlaps
        if juris_start == juris_end {
            return true;
        } else if juris_start < juris_end {
            return juris_start <= backend_index && backend_index < juris_end;
        } else {
            return juris_start <= backend_index || backend_index < juris_end;
        }
    }

    async fn check_migration(&self) -> TribResult<()> {
        // my own jurisdiction starts from self.this
        // gonna cover the gap if my next keepers are down
        // scan for next available keeper
        let NUM_KEEPERS_MAX = 10;
        let NUM_BACKEND_MAX = self.backs.len();
        let JURISDICTION_GAP = 30;

        // jurisdiction start: inclusive
        // jurisdiction end: non-inclusive
        let juris_start = self.this * JURISDICTION_GAP;
        let mut juris_end = juris_start;
        for i in self.this + 1..=self.this + NUM_KEEPERS_MAX {
            // should include node himself as the interval end; use inclusive
            let peer_index = i % NUM_KEEPERS_MAX;
            juris_end = (juris_end + JURISDICTION_GAP) % NUM_BACKEND_MAX;
            if peer_index == self.this {
                break;
            }
            let peer_addr = format!("http://{}", &self.keepers[peer_index]);
            let mut client = KeeperServiceClient::connect(peer_addr).await?;
            let resp_res = client.ping(keeper::Heartbeat { value: true }).await;
            if resp_res.is_err() {
                continue;
            }
            break;
        }
        // scan 300
        let mut node_join_index = None;
        let mut node_leave_index = None;
        for i in 0..NUM_BACKEND_MAX {
            let client = StorageClient::new(self.backs[i].as_str());
            let clock_res = client.clock(0).await;
            let mut back_status = self.backs_status_mut.lock().unwrap();
            if clock_res.is_err() {
                // server is now down
                if (*back_status)[i] == true
                    && self.falls_into_jurisdiction(i, juris_start, juris_end)
                {
                    // node leave from jurisdiction
                    node_leave_index = Some(i);
                }
                (*back_status)[i] = false;
            } else {
                // server is now up
                if (*back_status)[i] == false
                    && self.falls_into_jurisdiction(i, juris_start, juris_end)
                {
                    // node join in jurisdictioin
                    node_join_index = Some(i);
                }
                (*back_status)[i] = true;
            }
        }

        // do migration
        Ok(())
    }

    async fn get_clock_send(&self, i: usize) -> TribResult<u64> {
        let client = StorageClient::new(self.backs[i].as_str());
        let target_clock = client.clock(0).await?;
        // println!("getting clock: {}", target_clock);
        return Ok(target_clock);
    }

    async fn update_clock_send(&self, i: usize, max_clock: u64) -> TribResult<()> {
        let client = StorageClient::new(self.backs[i].as_str());
        client.clock(max_clock).await?;
        // println!("setting clock: {}", max_clock);
        return Ok(());
    }

    async fn broadcast_logical_clock(&self) -> TribResult<()> {
        let len = self.backs.len();

        let mut get_promises = vec![];
        let mut max_clock: u64 = 0;

        for i in 0..len {
            get_promises.push(self.get_clock_send(i));
        }

        for promise in get_promises {
            let recv_clock = promise.await?;
            // println!("getting {} and current max: {}", recv_clock, max_clock);
            max_clock = cmp::max(max_clock, recv_clock);
        }

        let mut update_promises = vec![];
        for i in 0..len {
            update_promises.push(self.update_clock_send(i, max_clock));
        }
        for promise in update_promises {
            promise.await?;
        }
        Ok(())
    }
}
