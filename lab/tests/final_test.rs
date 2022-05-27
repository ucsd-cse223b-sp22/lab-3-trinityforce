use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration, collections::{HashMap, HashSet}, cmp,
};
use rand::Rng;
use lab::{self, lab3, big_fucking_tester::BigFuckingTesterTrait};
use tokio::{sync::mpsc::Sender as MpscSender};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_simple_list() -> TribResult<()> {
    lab3::new_lockserver_ping_test().await?;
    let mut bft = BigFuckingTester::new(5, 
        5, 
        vec![0, 1, 2, 3, 4], 
        1, 
        vec![0]).await;
    let bin_client = lab3::new_bin_client(bft.back_addresses.clone()).await?;
    let target_bin = bin_client.bin("alice").await?;
    let _ = target_bin.list_append(&KeyValue { key: "key1".to_string(), value: "val1".to_string() }).await?;
    let get_res = target_bin.list_get("key1").await?.0;
    if get_res.len() != 1 || get_res[0] != "val1" {
        assert!(false, "test_simple_list: list get not correct: {:?}", get_res);
    }
    bft.cleanup().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_keeper_two_backs_dead() -> TribResult<()> {
    lab3::new_lockserver_ping_test().await?;
    let mut bft = BigFuckingTester::new(6, 5, vec![0, 1, 2, 3, 4], 1, vec![0]).await;
    let mut key_val_map = HashMap::new();
    let mut key_bin_map = HashMap::new();

    let STRING_LEN = 30;
    let NUM_KEYS = 12;
    let bin_client = lab3::new_bin_client(bft.back_addresses.clone()).await?;

    for i in 0..NUM_KEYS {
        let bin_name = generate_random_username(STRING_LEN);
        let key = generate_random_username(STRING_LEN);
        let value = generate_random_username(STRING_LEN);
        key_val_map.insert(key.to_string(), value.to_string());
        key_bin_map.insert(key.to_string(), bin_name.to_string());
        let client = bin_client.bin(bin_name.as_str()).await?;
        client.set(&KeyValue {
            key: key.to_string(),
            value: value.to_string(),
        }).await?;
    }
    bft.back_node_leave(0).await;
    tokio::time::sleep(Duration::from_secs(20)).await;
    bft.back_node_leave(1).await;
    tokio::time::sleep(Duration::from_secs(20)).await;
    for key in key_val_map.keys() {
        let bin_name = key_bin_map.get(key).unwrap();
        let expected_value = key_val_map.get(key).unwrap();
        let client = bin_client.bin(bin_name.as_str()).await?;
        let actual_option = client.get(key).await?;
        if actual_option.is_none() {
            assert!(false, "actual: None, expect: {}", expected_value.to_string());
        }
        let actual_value = actual_option.unwrap();
        if actual_value != expected_value.to_string() {
            assert!(false, "actual: {}, expect: {}", actual_value, expected_value.to_string())
        }
    }
    Ok(())
}