use super::super::keeper;
use super::super::keeper::keeper_service_client::KeeperServiceClient;
use super::client::StorageClient;
use std::cmp;
use std::sync::mpsc::{self, SyncSender};
use tribbler::err::TribResult;
use tribbler::storage::Storage;

pub struct KeeperServer {
    backs: Vec<String>,
    keepers: Vec<String>,
    this: usize,
    my_addr: String,
    backs_status: Vec<bool>,
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
            backs_status: backs_status.clone(),
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
    // interval_start: non-inclusive; interval_end: inclusive
    async fn migrate_data(
        &self,
        from: usize,
        to: usize,
        interval_start: usize,
        interval_end: usize,
    );
    async fn falls_into_interval(
        &self,
        target_string: String,
        interval_start: usize,
        interval_end: usize,
    ) -> bool;
}

#[async_trait]
impl KeeperHelper for KeeperServer {
    // backend servers index
    // interval_start: non-inclusive; interval_end: inclusive
    async fn falls_into_interval(
        &self,
        target_string: String,
        interval_start: usize,
        interval_end: usize,
    ) -> bool {
        todo!();
    }

    async fn check_migration(&self) -> TribResult<()> {
        // my own jurisdiction starts from self.this
        // gonna cover the gap if my next keepers are down
        // scan for next available keeper
        let NUM_KEEPERS_MAX = self.keepers.len();
        let start = self.this;
        let end = self.this + NUM_KEEPERS_MAX;

        let juris_start = start * 30;
        let juris_end = (start + 1) * 30;
        for i in start + 1..end {
            let peer_index = i % NUM_KEEPERS_MAX;
            let peer_addr = format!("http://{}", &self.keepers[peer_index]);
            let mut client = KeeperServiceClient::connect(peer_addr).await?;
            let resp_res = client.ping(keeper::Heartbeat { value: true }).await;
            if resp_res.is_err() {
                continue;
            }
        }
        // scan 300

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

    async fn migrate_data(
        &self,
        from: usize,
        to: usize,
        interval_start: usize,
        interval_end: usize,
    ) {
        todo!()
    }
}
