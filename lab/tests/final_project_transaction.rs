use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration, collections::{HashMap, HashSet}, cmp, vec,
};
use rand::Rng;
use lab::{self, lab3::{self, new_txn_client, new_lock_client, new_bin_client, new_bin_client_for_txn}, big_fucking_tester::BigFuckingTesterTrait};
use tokio::{sync::mpsc::Sender as MpscSender, sync::RwLock};
use tribbler::{config::KeeperConfig};
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};
use lab::big_fucking_tester::BigFuckingTester;
use lab::big_fucking_tester::generate_random_username;
use futures::future::{join_all, ok, err};
use lab3::TxnClient;

async fn random_read_txn(mut txn_client: TxnClient) -> TribResult<()> {
    let NUM_OP_PER_TXN = 5;
    let NUM_BIN = 5;
    let mut read_keys_map = HashMap::new();
    let mut write_keys_map = HashMap::new();
    for j in 0..NUM_BIN {
        let rand_bin = generate_random_username(30);
        let mut bin_set = vec![];
        for i in 0..NUM_OP_PER_TXN {
            bin_set.push(generate_random_username(10));
        }
        read_keys_map.insert(rand_bin, bin_set);
    }
    let trans_key = txn_client.transaction_start(read_keys_map.clone(), write_keys_map.clone()).await?;
    txn_client.transaction_end(trans_key, read_keys_map.clone(), write_keys_map.clone());
    Ok(())
}

async fn random_write_txn(mut txn_client: TxnClient) -> TribResult<()> {
    let NUM_OP_PER_TXN = 5;
    let NUM_BIN = 5;
    let mut read_keys_map = HashMap::new();
    let mut write_keys_map = HashMap::new();
    for j in 0..NUM_BIN {
        let rand_bin = generate_random_username(30);
        let mut bin_set = vec![];
        for i in 0..NUM_OP_PER_TXN {
            bin_set.push(generate_random_username(10));
        }
        write_keys_map.insert(rand_bin, bin_set);
    }
    let trans_key = txn_client.transaction_start(read_keys_map.clone(), write_keys_map.clone()).await?;
    txn_client.transaction_end(trans_key, read_keys_map.clone(), write_keys_map.clone());
    Ok(())
}

async fn random_rw(mut txn_client: TxnClient) -> TribResult<()> {
    let mut rng = rand::thread_rng();
    let NUM_OP_PER_TXN = 10;
    let NUM_BIN = 5;
    let mut read_keys_map = HashMap::new();
    let mut write_keys_map = HashMap::new();
    for j in 0..NUM_BIN {
        let rand_bin = generate_random_username(30);
        let mut read_bin_set = vec![];
        let mut write_bin_set = vec![];
        for i in 0..NUM_OP_PER_TXN {
            let action_type: usize = rng.gen_range(0..2);
            if action_type == 0 {
                read_bin_set.push(generate_random_username(10));
            } else {
                write_bin_set.push(generate_random_username(10));
            }
        }
        read_keys_map.insert(rand_bin.to_string(), read_bin_set);
        write_keys_map.insert(rand_bin.to_string(), write_bin_set);
    }
    let trans_key = txn_client.transaction_start(read_keys_map.clone(), write_keys_map.clone()).await?;
    txn_client.transaction_end(trans_key, read_keys_map.clone(), write_keys_map.clone());
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]


async fn test_txn_read_throughput() -> TribResult<()> {
    lab3::new_lockserver_ping_test().await?;
    let mut bft = BigFuckingTester::new(15, 5, vec![0, 1, 2, 3, 4], 1, vec![0]).await;
    let channel_cache = Arc::new(RwLock::new(HashMap::new()));
    let bin_storage = Arc::new(new_bin_client_for_txn(bft.back_addresses));
    let lock_client = Arc::new(new_lock_client());
    let mut futures = vec![];
    let NUM_TXN_CLIENT = 5;
    for i in 0..NUM_TXN_CLIENT {
        let mut txn_client = new_txn_client(channel_cache.clone(), bin_storage.clone(), lock_client.clone());
        futures.push(random_read_txn(txn_client));
    }
    join_all(futures).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_txn_write_throughput() -> TribResult<()> {
    lab3::new_lockserver_ping_test().await?;
    let mut bft = BigFuckingTester::new(15, 5, vec![0, 1, 2, 3, 4], 1, vec![0]).await;
    let channel_cache = Arc::new(RwLock::new(HashMap::new()));
    let bin_storage = Arc::new(new_bin_client_for_txn(bft.back_addresses));
    let lock_client = Arc::new(new_lock_client());
    let mut futures = vec![];
    let NUM_TXN_CLIENT = 100;
    for i in 0..NUM_TXN_CLIENT {
        let mut txn_client = new_txn_client(channel_cache.clone(), bin_storage.clone(), lock_client.clone());
        futures.push(random_write_txn(txn_client));
    }
    join_all(futures).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_txn_read_write_throughput() -> TribResult<()> {
    lab3::new_lockserver_ping_test().await?;
    let mut bft = BigFuckingTester::new(15, 5, vec![0, 1, 2, 3, 4], 1, vec![0]).await;
    let channel_cache = Arc::new(RwLock::new(HashMap::new()));
    let bin_storage = Arc::new(new_bin_client_for_txn(bft.back_addresses));
    let lock_client = Arc::new(new_lock_client());
    let mut futures = vec![];
    let NUM_TXN_CLIENT = 100;
    for i in 0..NUM_TXN_CLIENT {
        let mut txn_client = new_txn_client(channel_cache.clone(), bin_storage.clone(), lock_client.clone());
        futures.push(random_rw(txn_client));
    }
    join_all(futures).await;
    Ok(())
}