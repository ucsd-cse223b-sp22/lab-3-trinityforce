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

