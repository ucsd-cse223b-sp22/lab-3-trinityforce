use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration, collections::{HashMap, HashSet}, cmp,
};
use rand::Rng;
use lab::{self, lab3};
use tokio::{sync::mpsc::Sender as MpscSender};
use tribbler::{config::KeeperConfig};
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    println!("setting up back");
    tokio::spawn(lab3::serve_back(cfg))
}

fn spawn_keep(kfg: KeeperConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab3::serve_keeper(kfg))
}

fn generate_random_username(len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();

    let password: String = (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    return format!("{}{}", "a", password);
}

fn generate_addresses(len: u64, is_back: bool) -> Vec<String> {
    let mut addrs = vec![];
    let mut prefix = "127.0.0.1:57";
    if is_back {
        prefix = "127.0.0.1:43";
    }
    for i in 0..len {
        let u32_i : u32 = i as u32;
        let backend_addr = format!("{}{:03}", prefix, u32_i);
        addrs.push(backend_addr.to_string());
    }
    return addrs;
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

async fn setup_single_keeper(i: usize, keeper_addr: String, backs: Vec<String>) -> MpscSender<()> {
    let (shut_tx_keeper, shut_rx_keeper) = tokio::sync::mpsc::channel(1);
    let kfg = KeeperConfig {
        backs: backs.clone(),
        addrs: vec![keeper_addr.to_string()],
        this: i,
        id: rand::thread_rng().gen_range(0..300),
        ready: None,
        shutdown: Some(shut_rx_keeper),
    };
    let _ = spawn_keep(kfg);
    tokio::time::sleep(Duration::from_millis(100)).await;
    return shut_tx_keeper.clone();
}


async fn setup(backs: Vec<String>, keepers: Vec<String>) -> TribResult<(Vec<MpscSender<()>>, Vec<MpscSender<()>>)> {
    let mut shutdown_back_send_chans = vec![];
    for i in 0..backs.len() {
        let backend_addr = &backs[i];
        let shut_tx = setup_single_back(backend_addr.to_string()).await;
        shutdown_back_send_chans.push(shut_tx.clone());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut shutdown_keeper_send_chans = vec![];
    for i in 0..keepers.len() {
        let keeper_addr = &keepers[i];
        let shut_tx = setup_single_keeper(i, keeper_addr.to_string(), backs.clone()).await;
        shutdown_keeper_send_chans.push(shut_tx.clone());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    return Ok((shutdown_back_send_chans, shutdown_keeper_send_chans));
}

async fn shutdown_multiple_channels(shut_chans: Vec<MpscSender<()>>, exclude_indices: Vec<usize>) {
    for i in 0..shut_chans.len() {
        if exclude_indices.contains(&i) {
            continue;
        }
        let _ = shut_chans[i].send(()).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_list_append_one_node_dead() -> TribResult<()> {
    // Der Ring des Nibelungen:
    // Once a little elf try to snatch the precious backend server data from the holy Rhine River.
    // And thy should be aware of the imminent dangers and call upon our mighty keeper to protect the holy consistency of our data
    // Time to return the data back to Rhine River pal pal.
    let keeper_addresses = generate_addresses(1, false);
    let backend_addresses = generate_addresses(5, true);
    let (back_shut_vec, keeper_shut_vec) = setup(backend_addresses.clone(), keeper_addresses.clone()).await?;
    println!("The story begins, backends and keepers rise up!");
    let bin_client = lab3::new_bin_client(backend_addresses.clone()).await?;
    let adapter = bin_client.bin("alice").await?;
    let _ = adapter.list_append(&KeyValue { key: "key1".to_string(), value: "val1".to_string() }).await?;
    println!("And he elf wrecked one part of Rhine riverbacks and snatched the treasures");
    let _ = back_shut_vec[1].send(()).await;
    let get_res = adapter.list_get("key1").await?.0;
    println!("List retrieved: {:?}", get_res);
    if get_res.len() != 1 || get_res[0] != "val1" {
        assert!(false, "test_single_list_append_one_node_dead: list get not correct: {:?}", get_res);
    }
    println!("And thy shall seek for the holy keeper to move the data within next 20 seconds!...");
    tokio::time::sleep(Duration::from_secs(20)).await;
    let _ = shutdown_multiple_channels(keeper_shut_vec, vec![]).await;
    let _ = shutdown_multiple_channels(back_shut_vec, vec![1]).await;
    Ok(())
}

// cargo test --package lab --test lab3_test -- test_single_list_append_one_node_dead --exact --nocapture