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
async fn test_single_list_append_one_node_dead() -> TribResult<()> {
    // Der Ring des Nibelungen:
    // Once a little elf try to snatch the precious backend server data from the holy Rhine River.
    // And thy should be aware of the imminent dangers and call upon our mighty keeper to protect the holy consistency of our data
    // Time to return the data back to Rhine River pal pal.
    let mut bft = BigFuckingTester::new(10, 5, vec![0, 1, 2, 3, 4], 1, vec![0]).await;
    println!("The story begins, backends and keepers rise up!");
    let bin_client = lab3::new_bin_client(bft.back_addresses.clone()).await?;
    let target_bin = bin_client.bin("alice").await?;
    let _ = target_bin.list_append(&KeyValue { key: "key1".to_string(), value: "val1".to_string() }).await?;
    println!("And the elf wrecked one part of Rhine riverbacks and snatched the treasures");
    bft.back_node_leave(1).await;
    let get_res = target_bin.list_get("key1").await?.0;
    println!("List retrieved: {:?}", get_res);
    if get_res.len() != 1 || get_res[0] != "val1" {
        assert!(false, "test_single_list_append_one_node_dead: list get not correct: {:?}", get_res);
    }
    println!("And thy shall seek for the holy keeper to move the data within next 20 seconds!...");
    tokio::time::sleep(Duration::from_secs(20)).await;
    bft.cleanup().await;
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_keeper_successor_node_join() -> TribResult<()> {
    // Adventure time:
    let mut bft = BigFuckingTester::new(2, 5, vec![0, 2, 4], 1, vec![0]).await;
    let bin_client = lab3::new_bin_client(bft.back_addresses.clone()).await?;
    let target_bin = bin_client.bin("alice").await?;
    let _ = target_bin.list_append(&KeyValue { key: "key1".to_string(), value: "val1".to_string() }).await?;
    println!("An adventurer has joined the party!");
    bft.back_join(1).await;
    let get_res = target_bin.list_get("key1").await?.0;
    println!("List retrieved: {:?}", get_res);
    if get_res.len() != 1 || get_res[0] != "val1" {
        assert!(false, "test_single_keeper_successor_node_join: list get not correct: {:?}", get_res);
    }
    tokio::time::sleep(Duration::from_secs(20)).await;
    bft.cleanup().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_simple_keeper_kill() -> TribResult<()> {
    let mut bft = BigFuckingTester::new(3, 5, vec![0, 2, 4], 3, vec![0, 1, 2]).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("kill node 1");
    bft.keeper_node_leave(1).await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    bft.cleanup().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_keeper_two_backs_dead() -> TribResult<()> {
    let mut bft = BigFuckingTester::new(4, 5, vec![0, 1, 2, 3, 4], 1, vec![0]).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_keeper_leave() -> TribResult<()> {
    let mut bft = BigFuckingTester::new(11, 5, vec![0, 1, 2, 3, 4], 3, vec![0, 1, 2]).await;
    
    let bin_client = lab3::new_bin_client(bft.back_addresses.clone()).await?;
    let target_bin = bin_client.bin("alice").await?;
    let _ = target_bin.list_append(&KeyValue { key: "key1".to_string(), value: "val1".to_string() }).await?;

    bft.keeper_node_leave(0).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    bft.keeper_node_leave(1).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    bft.back_node_leave(0).await;
    tokio::time::sleep(Duration::from_secs(10)).await;

    let get_res = target_bin.list_get("key1").await?.0;
    println!("List retrieved: {:?}", get_res);
    if get_res.len() != 1 || get_res[0] != "val1" {
        assert!(false, "test_single_list_append_one_node_dead: list get not correct: {:?}", get_res);
    }
    println!("And thy shall seek for the holy keeper to move the data within next 20 seconds!...");
    tokio::time::sleep(Duration::from_secs(20)).await;
    bft.cleanup().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_back_join() -> TribResult<()> {
    let mut bft = BigFuckingTester::new(12, 5, vec![0, 2, 4], 1, vec![0]).await;
    
    let bin_client = lab3::new_bin_client(bft.back_addresses.clone()).await?;
    let mut target_bin = bin_client.bin("alice").await?;
    let _ = target_bin.list_append(&KeyValue { key: "key1".to_string(), value: "val1".to_string() }).await?;

    bft.back_join(1).await;
    tokio::time::sleep(Duration::from_secs(15)).await;


    target_bin = bin_client.bin("alice").await?;
    let _ = target_bin.list_append(&KeyValue { key: "key1".to_string(), value: "val2".to_string() }).await?;

    bft.back_join(3).await;
    tokio::time::sleep(Duration::from_secs(15)).await;

    target_bin = bin_client.bin("alice").await?;
    let _ = target_bin.list_append(&KeyValue { key: "key1".to_string(), value: "val3".to_string() }).await?;

    bft.back_node_leave(4).await;
    tokio::time::sleep(Duration::from_secs(15)).await;

    let get_res = target_bin.list_get("key1").await?.0;
    println!("List retrieved: {:?}", get_res);
    if get_res.len() != 3 || get_res[0] != "val1" || get_res[1] != "val2" || get_res[2] != "val3" {
        assert!(false, "test_single_list_append_one_node_dead: list get not correct: {:?}", get_res);
    }
    bft.cleanup().await;

    Ok(())
}

// cargo test --package lab --test lab3_test -- test_simple_keeper_kill --exact --nocapture