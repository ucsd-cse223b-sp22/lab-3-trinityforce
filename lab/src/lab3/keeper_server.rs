use super::client::StorageClient;
use std::cmp;
use std::sync::mpsc::{self, SyncSender};
use tribbler::err::TribResult;
use tribbler::storage::Storage;

pub struct KeeperServer {
    backs: Vec<String>,
    addr: String,
}
impl KeeperServer {
    pub fn new(addr: String, backs: &Vec<String>) -> Self {
        Self {
            backs: backs.clone(),
            addr: addr,
        }
    }
}

use async_trait::async_trait;
#[async_trait]
pub trait KeeperHelper {
    async fn broadcast_logical_clock(&self) -> TribResult<()>;
    async fn get_clock_send(&self, i: usize) -> TribResult<u64>;
    async fn update_clock_send(&self, i: usize, max_clock: u64) -> TribResult<()>;
}

#[async_trait]
impl KeeperHelper for KeeperServer {
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

        // for i in 0..len {
        //     get_promises.push(tokio::task::spawn(
        //         async move { self.get_clock_send(i).await },
        //     ));
        // }

        // for promise in get_promises {
        //     let recv_clock = promise.await??;
        //     max_clock = cmp::max(max_clock, recv_clock);
        // }

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
