use super::lab3;
use rand::Rng;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration,
};
use tokio::sync::mpsc::Sender as MpscSender;
use tribbler::config::KeeperConfig;
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab3::serve_back(cfg))
}

fn spawn_keep(kfg: KeeperConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab3::serve_keeper(kfg))
}

async fn setup(
    backs: Vec<String>,
    initial_back_indices: Vec<usize>,
    keepers: Vec<String>,
    initial_keeper_indices: Vec<usize>,
) -> (Vec<Option<MpscSender<()>>>, Vec<Option<MpscSender<()>>>) {
    let mut shutdown_back_send_chans = vec![];
    let back_hashset: HashSet<usize> = initial_back_indices.into_iter().collect();
    for i in 0..backs.len() {
        if !back_hashset.contains(&i) {
            shutdown_back_send_chans.push(None);
            continue;
        }
        let backend_addr = &backs[i];
        let shut_tx = setup_single_back(backend_addr.to_string()).await;
        shutdown_back_send_chans.push(Some(shut_tx.clone()));
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let keeper_hashset: HashSet<usize> = initial_keeper_indices.into_iter().collect();
    let mut shutdown_keeper_send_chans = vec![];
    for i in 0..keepers.len() {
        if !keeper_hashset.contains(&i) {
            shutdown_keeper_send_chans.push(None);
            continue;
        }
        let shut_tx = setup_single_keeper(i, keepers.clone(), backs.clone()).await;
        shutdown_keeper_send_chans.push(Some(shut_tx.clone()));
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    return (shutdown_back_send_chans, shutdown_keeper_send_chans);
}

async fn setup_single_keeper(
    i: usize,
    keeper_addrs: Vec<String>,
    backs: Vec<String>,
) -> MpscSender<()> {
    let (shut_tx_keeper, shut_rx_keeper) = tokio::sync::mpsc::channel(1);
    let kfg = KeeperConfig {
        backs: backs.clone(),
        addrs: keeper_addrs.clone(),
        this: i,
        id: rand::thread_rng().gen_range(0..300),
        ready: None,
        shutdown: Some(shut_rx_keeper),
    };
    let _ = spawn_keep(kfg);
    tokio::time::sleep(Duration::from_millis(100)).await;
    return shut_tx_keeper.clone();
}

async fn setup_single_back(backend_addr: String) -> MpscSender<()> {
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg = BackConfig {
        addr: backend_addr.to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx),
    };
    let _ = spawn_back(cfg);
    tokio::time::sleep(Duration::from_millis(100)).await;
    return shut_tx.clone();
}

fn generate_addresses(back_len: usize, keeper_len: usize) -> (Vec<String>, Vec<String>) {
    let mut back_addrs = vec![];
    let mut prefix = "127.0.0.1:57";
    for i in 0..back_len {
        let u32_i: u32 = i as u32;
        let full_str = format!("{}{:03}", prefix, u32_i);
        back_addrs.push(full_str.to_string());
    }
    let mut keeper_addrs = vec![];
    prefix = "127.0.0.1:43";
    for i in 0..keeper_len {
        let u32_i: u32 = i as u32;
        let full_str = format!("{}{:03}", prefix, u32_i);
        keeper_addrs.push(full_str.to_string());
    }
    return (back_addrs, keeper_addrs);
}

pub struct BigFuckingTester {
    pub keeper_addresses: Vec<String>,
    pub keeper_shutdown_send_chan: Vec<Option<MpscSender<()>>>,
    pub back_addresses: Vec<String>,
    pub back_shutdown_send_chan: Vec<Option<MpscSender<()>>>,
}

impl BigFuckingTester {
    pub async fn new(
        back_len: usize,
        initial_back_live_indices: Vec<usize>,
        keeper_len: usize,
        initial_keeper_live_indices: Vec<usize>,
    ) -> Self {
        let (back_addresses, keeper_addresses) = generate_addresses(back_len, keeper_len);
        let (back_shut_vec, keeper_shut_vec) = setup(
            back_addresses.clone(),
            initial_back_live_indices.clone(),
            keeper_addresses.clone(),
            initial_keeper_live_indices.clone(),
        )
        .await;
        Self {
            back_addresses: back_addresses,
            back_shutdown_send_chan: back_shut_vec,
            keeper_addresses: keeper_addresses,
            keeper_shutdown_send_chan: keeper_shut_vec,
        }
    }
}

use async_trait::async_trait;
#[async_trait]
pub trait BigFuckingTesterTrait {
    async fn keeper_node_leave(&mut self, leave_index: usize);
    async fn keeper_join(&mut self, join_index: usize);
    async fn back_node_leave(&mut self, leave_index: usize);
    async fn back_join(&mut self, leave_index: usize);
    async fn cleanup(&mut self);
}

#[async_trait]
impl BigFuckingTesterTrait for BigFuckingTester {
    async fn keeper_node_leave(&mut self, leave_index: usize) {
        self.keeper_shutdown_send_chan[leave_index]
            .as_ref()
            .unwrap()
            .send(())
            .await;
        self.keeper_shutdown_send_chan[leave_index] = None;
    }

    async fn keeper_join(&mut self, join_index: usize) {
        let shut_tx = setup_single_keeper(
            join_index,
            self.keeper_addresses.clone(),
            self.back_addresses.clone(),
        )
        .await;
        self.keeper_shutdown_send_chan[join_index] = Some(shut_tx);
    }

    async fn back_node_leave(&mut self, leave_index: usize) {
        self.back_shutdown_send_chan[leave_index]
            .as_ref()
            .unwrap()
            .send(())
            .await;
        self.back_shutdown_send_chan[leave_index] = None;
    }

    async fn back_join(&mut self, leave_index: usize) {
        let shut_tx = setup_single_back(self.back_addresses[leave_index].to_string()).await;
        self.back_shutdown_send_chan[leave_index] = Some(shut_tx);
    }

    async fn cleanup(&mut self) {
        for i in 0..self.keeper_shutdown_send_chan.len() {
            if self.keeper_shutdown_send_chan[i].is_none() {
                continue;
            }
            self.keeper_node_leave(i);
        }
        for i in 0..self.back_shutdown_send_chan.len() {
            if self.back_shutdown_send_chan[i].is_none() {
                continue;
            }
            self.back_node_leave(i);
        }
    }
}