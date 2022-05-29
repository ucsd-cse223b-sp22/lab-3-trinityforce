use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration, collections::{HashMap, HashSet}, cmp,
};
use rand::Rng;
use lab::{self, lab2, lab1, lab3};
use tokio::{sync::mpsc::Sender as MpscSender, time};
use tribbler::{config::KeeperConfig, storage::BinStorage};
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

const BIN_NUM: usize = 20;
const KEY_NUM: usize = 20;
const VAL_NUM: usize = 20;

fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    println!("setting up back");
    tokio::spawn(lab3::serve_back(cfg))
}

fn spawn_keep(kfg: KeeperConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab3::serve_keeper(kfg))
}

fn spawn_back2(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    println!("setting up back");
    tokio::spawn(lab1::serve_back(cfg))
}

fn spawn_keep2(kfg: KeeperConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab2::serve_keeper(kfg))
}

async fn perform_random_action(lab2: &Box<dyn BinStorage>, lab3: &Box<dyn BinStorage>, clock_records: &mut Vec<u64>) -> TribResult<()> {
    let mut rng = rand::thread_rng();
    // Action: set = 0 ,keys = 1, list_append = 2 ,list_remove = 3 ,list_keys = 4, clock = 5;
    let action: usize = rng.gen_range(0..6);
    // Bin, key, val
    let bin_name: usize = rng.gen_range(0..BIN_NUM);
    let bin_name_num = bin_name.clone();
    let bin_name = format!("bin{}", bin_name);
    let key: usize = rng.gen_range(0..KEY_NUM);
    let key = format!("key{}", key);
    let val: usize = rng.gen_range(0..VAL_NUM);
    let val = format!("val{}", val);
    // Perform action
    let adapter2 = lab2.bin(&bin_name).await?;
    let adapter3 = lab3.bin(&bin_name).await?;

    match action {
        0 => {
            println!("Get bin: {}, key: {}", bin_name, key);
            let ori2 = adapter2.get(&key).await?;
            let ori3 = adapter3.get(&key).await?;
            assert_eq!(ori2, ori3, "\n
            Get Inconsistent.\n
            lab2: {:?}\n
            lab3: {:?}",
            ori2, ori3);

            println!("Set bin: {}, key: {}, value: {}", bin_name, key, val);
            let _ = adapter2.set(&KeyValue { key: key.clone(), value: val.clone() }).await?;
            let _ = adapter3.set(&KeyValue { key: key.clone(), value: val.clone() }).await?;

            println!("Get bin: {}, key: {}", bin_name, key);
            let res2 = adapter2.get(&key).await?;
            let res3 = adapter3.get(&key).await?;
            assert_eq!(res2, res3, "\n
            Set Inconsistent.\n
            ori2: {:?}\n 
            ori3: {:?}\n
            new2: {:?}\n
            new3: {:?}\n
            Set key: {}, value: {}",
            ori2, ori3, res2, res3, key, val);
        },
        1 => {
            println!("Keys bin: {}", bin_name);
            let mut res2 = adapter2.keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
            let mut res3 = adapter3.keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
            res2.sort_unstable();
            res3.sort_unstable();
            assert_eq!(res2, res3, "\n
            Keys Inconsistent.\n
            lab2: {:?}\n 
            lab3: {:?}",
            res2, res3);
        },
        2 => {
            println!("List-Get bin: {}, key: {}", bin_name, key);
            let ori2 = adapter2.list_get(&key).await?.0;
            let ori3 = adapter3.list_get(&key).await?.0;
            assert_eq!(ori2, ori3, "\n
            List-Get Inconsistent.\n
            lab2: {:?}\n
            lab3: {:?}",
            ori2, ori3);

            println!("List-Append bin: {}, key: {}, value: {}", bin_name, key, val);
            let _ = adapter2.list_append(&KeyValue { key: key.clone(), value: val.clone() }).await?;
            let _ = adapter3.list_append(&KeyValue { key: key.clone(), value: val.clone() }).await?;

            println!("List-Get bin: {}, key: {}", bin_name, key);
            let res2 = adapter2.list_get(&key).await?.0;
            let res3 = adapter3.list_get(&key).await?.0;
            assert_eq!(res2, res3, "\n
            List-Append Inconsistent.\n
            ori2: {:?}\n
            ori3: {:?}\n
            new2: {:?}\n
            new3: {:?}\n
            Append key: {}, value: {}",
            ori2, ori3, res2, res3, key, val);
        },
        3 => {
            println!("List-Get bin: {}, key: {}", bin_name, key);
            let ori2 = adapter2.list_get(&key).await?.0;
            let ori3 = adapter3.list_get(&key).await?.0;
            assert_eq!(ori2, ori3, "\n
            List-Get Inconsistent.\n
            lab2: {:?}\n
            lab3: {:?}",
            ori2, ori3);

            println!("List-Remove bin: {}, key: {}, value: {}", bin_name, key, val);
            let res2 = adapter2.list_remove(&KeyValue { key: key.clone(), value: val.clone() }).await?;
            let res3 = adapter3.list_remove(&KeyValue { key: key.clone(), value: val.clone() }).await?;

            println!("List-Get bin: {}, key: {}", bin_name, key);
            let new2 = adapter2.list_get(&key).await?.0;
            let new3 = adapter3.list_get(&key).await?.0;

            assert_eq!(res2, res3, "\n
            List-Remove Inconsistent.\n 
            ori2: {:?}\n
            ori3: {:?}\n
            new2: {:?}\n
            new3: {:?}\n
            remove2: {}, remove3: {}\n
            Remove key: {}, value: {}",
            ori2, ori3, new2, new3, res2, res3, key, val);
        },
        4 => {
            println!("List-Keys bin: {}", bin_name);
            let res2 = adapter2.list_keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
            let res3 = adapter3.list_keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
            assert_eq!(res2, res3, "\n
            List-Keys Inconsistent.\n
            lab2: {:?}\n
            lab3: {:?}",
            res2, res3);
        },
        5 => {
            println!("Clock bin: {}", bin_name);
            let res3 = adapter3.clock(1).await?;
            assert!(res3 > clock_records[bin_name_num], "\n
            Clock does not increase!\n
            Before: {}, After {}.\n",
            clock_records[bin_name_num], res3);
            clock_records[bin_name_num] = res3;
        }
        _ => {println!("Should not reach here!")},
    };
    Ok(())
}

async fn check_final_status(lab2: &Box<dyn BinStorage>, lab3: &Box<dyn BinStorage>) -> TribResult<()>  {
    for bin_name in 0..BIN_NUM {
        let bin_name = format!("bin{}", bin_name);
        let adapter2 = lab2.bin(&bin_name).await?;
        let adapter3 = lab3.bin(&bin_name).await?;

        let mut res2 = adapter2.keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
        let mut res3 = adapter3.keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
        res2.sort_unstable();
        res3.sort_unstable();
        assert_eq!(res2, res3, "\n
            Keys Inconsistent.\n
            lab2: {:?}\n 
            lab3: {:?}",
            res2, res3);

        for key in res2.iter() {
            let _res2 = adapter2.get(&key).await?;
            let _res3 = adapter3.get(&key).await?;
            assert_eq!(_res2, _res3, "\n
            Get Inconsistent.\n
            lab2: {:?}\n
            lab3: {:?}",
             _res2, _res3);
        }

        let res2 = adapter2.list_keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
        let res3 = adapter3.list_keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
        assert_eq!(res2, res3, "\n
        List-Keys Inconsistent.\n
        lab2: {:?}\n
        lab3: {:?}",
        res2, res3);

        for key in res2.iter() {
            let _res2 = adapter2.list_get(&key).await?.0;
            let _res3 = adapter3.list_get(&key).await?.0;
            assert_eq!(_res2, _res3, "\n
            List-Get Inconsistent.\n
            lab2: {:?}\n
            lab3: {:?}",
            _res2, _res3);
        }
    }
    
    Ok(())
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

fn generate_addresses2(len: u64, is_back: bool) -> Vec<String> {
    let mut addrs = vec![];
    let mut prefix = "127.0.0.1:58";
    if is_back {
        prefix = "127.0.0.1:44";
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

async fn setup_single_keeper(i: usize, keepers: Vec<String>, backs: Vec<String>) -> MpscSender<()> {
    let (shut_tx_keeper, shut_rx_keeper) = tokio::sync::mpsc::channel(1);
    let kfg = KeeperConfig {
        backs: backs.clone(),
        addrs: keepers.clone(),
        this: i,
        id: rand::thread_rng().gen_range(0..300),
        ready: None,
        shutdown: Some(shut_rx_keeper),
    };
    let _ = spawn_keep(kfg);
    tokio::time::sleep(Duration::from_millis(100)).await;
    return shut_tx_keeper.clone();
}

async fn setup_single_back2(backend_addr: String) -> MpscSender<()> {
    let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
    let cfg = BackConfig {
        addr: backend_addr.to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx),
    };
    let _ = spawn_back2(cfg);
    tokio::time::sleep(Duration::from_millis(100)).await;
    return shut_tx.clone();
}

async fn setup_single_keeper2(i: usize, keepers: Vec<String>, backs: Vec<String>) -> MpscSender<()> {
    let (shut_tx_keeper, shut_rx_keeper) = tokio::sync::mpsc::channel(1);
    let kfg = KeeperConfig {
        backs: backs.clone(),
        addrs: keepers.clone(),
        this: i,
        id: rand::thread_rng().gen_range(0..300),
        ready: None,
        shutdown: Some(shut_rx_keeper),
    };
    let _ = spawn_keep2(kfg);
    tokio::time::sleep(Duration::from_millis(100)).await;
    return shut_tx_keeper.clone();
}


async fn setup(backs: Vec<String>, keepers: Vec<String>, live_backs: usize, live_keepers: usize) -> TribResult<(Vec<Option<MpscSender<()>>>, Vec<Option<MpscSender<()>>>)> {
    assert!(backs.len() >= live_backs);
    assert!(keepers.len() >= live_keepers);
    let mut shutdown_back_send_chans = vec![];
    for i in 0..live_backs {
        let backend_addr = &backs[i];
        let shut_tx = setup_single_back(backend_addr.to_string()).await;
        shutdown_back_send_chans.push(Some(shut_tx.clone()));
    }
    for _ in live_backs..backs.len() {
        shutdown_back_send_chans.push(None);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut shutdown_keeper_send_chans = vec![];
    for i in 0..live_keepers {
        let shut_tx = setup_single_keeper(i, keepers.clone(), backs.clone()).await;
        shutdown_keeper_send_chans.push(Some(shut_tx.clone()));
    }
    for _ in live_keepers..keepers.len() {
        shutdown_keeper_send_chans.push(None);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    return Ok((shutdown_back_send_chans, shutdown_keeper_send_chans));
}

async fn setup2(backs: Vec<String>, keepers: Vec<String>) -> TribResult<(Vec<MpscSender<()>>, Vec<MpscSender<()>>)> {
    let mut shutdown_back_send_chans = vec![];
    for i in 0..backs.len() {
        let backend_addr = &backs[i];
        let shut_tx = setup_single_back2(backend_addr.to_string()).await;
        shutdown_back_send_chans.push(shut_tx.clone());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut shutdown_keeper_send_chans = vec![];
    for i in 0..keepers.len() {
        let shut_tx = setup_single_keeper2(i, keepers.clone(), backs.clone()).await;
        shutdown_keeper_send_chans.push(shut_tx.clone());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    return Ok((shutdown_back_send_chans, shutdown_keeper_send_chans));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bin_storage() -> TribResult<()> {
    // Der Ring des Nibelungen:
    // Once a little elf try to snatch the precious backend server data from the holy Rhine River.
    // And thy should be aware of the imminent dangers and call upon our mighty keeper to protect the holy consistency of our data
    // Time to return the data back to Rhine River pal pal.
    let keeper_addresses2 = generate_addresses2(1, false);
    let backend_addresses2 = generate_addresses2(3, true);
    let (back_shut_vec2, keeper_shut_vec2) = setup2(backend_addresses2.clone(), keeper_addresses2.clone()).await?;

    let keeper_addresses3 = generate_addresses(3, false);
    let backend_addresses3 = generate_addresses(8, true);
    let keeper_addresses3_keep = keeper_addresses3.clone();
    let backend_addresses3_back = backend_addresses3.clone();
    let backend_addresses3_keep = backend_addresses3.clone();
    let (back_shut_vec3, keeper_shut_vec3) = setup(backend_addresses3.clone(), keeper_addresses3.clone(), 8, 2).await?;
    let mut back_shut_vec3_back = back_shut_vec3.clone();
    let mut keeper_shut_vec3_keep = keeper_shut_vec3.clone();
    
    println!("Start testing!");
    let bin_client2 = lab2::new_bin_client(backend_addresses2.clone()).await?;
    let bin_client3 = lab3::new_bin_client(backend_addresses3.clone()).await?;
    let bin_client3_clock = lab3::new_bin_client(backend_addresses3.clone()).await?;

    let mut op_count = 0;
    let mut clock_records = vec![0; BIN_NUM];

    tokio::spawn(async move {
        // Process each socket concurrently.
        let mut back_cp = 0;
        let mut backend_interval = time::interval(time::Duration::from_secs(30));
        loop {
            backend_interval.tick().await;
            match back_cp {
                0 => {},
                1 => {
                    let back_to_die = 0;
                    let _ = back_shut_vec3_back[back_to_die].as_ref().unwrap().send(()).await;
                    back_shut_vec3_back[back_to_die] = None;
                    println!("BACKEND {} DIE!!!!!!!", back_to_die);
                },
                2 => {
                    let back_to_die = 3;
                    let _ = back_shut_vec3_back[back_to_die].as_ref().unwrap().send(()).await;
                    back_shut_vec3_back[back_to_die] = None;
                    println!("BACKEND {} DIE!!!!!!!", back_to_die);
                },
                3 => {
                    let back_to_die = 7;
                    let _ = back_shut_vec3_back[back_to_die].as_ref().unwrap().send(()).await;
                    back_shut_vec3_back[back_to_die] = None;
                    println!("BACKEND {} DIE!!!!!!!", back_to_die);
                    
                },
                4 => {
                    let back_to_die = 2;
                    let _ = back_shut_vec3_back[back_to_die].as_ref().unwrap().send(()).await;
                    back_shut_vec3_back[back_to_die] = None;
                    println!("BACKEND {} DIE!!!!!!!", back_to_die);
                },
                5 => {
                    let back_to_die = 1;
                    let _ = back_shut_vec3_back[back_to_die].as_ref().unwrap().send(()).await;
                    back_shut_vec3_back[back_to_die] = None;
                    println!("BACKEND {} DIE!!!!!!!", back_to_die);
                },
                6 => {
                    let back_to_revive = 2;
                    let shut_tx = setup_single_back(backend_addresses3_back[back_to_revive].clone()).await;
                    back_shut_vec3_back[back_to_revive] = Some(shut_tx);
                    println!("BACKEND {} REVIVE!!!!!!!", back_to_revive);
                },
                7 => {
                    let back_to_revive = 7;
                    let shut_tx = setup_single_back(backend_addresses3_back[back_to_revive].clone()).await;
                    back_shut_vec3_back[back_to_revive] = Some(shut_tx);
                    println!("BACKEND {} REVIVE!!!!!!!", back_to_revive);
                },
                8 => {
                    let back_to_revive = 0;
                    let shut_tx = setup_single_back(backend_addresses3_back[back_to_revive].clone()).await;
                    back_shut_vec3_back[back_to_revive] = Some(shut_tx);
                    println!("BACKEND {} REVIVE!!!!!!!", back_to_revive);
                    
                },
                9 => {
                    let back_to_revive = 1;
                    let shut_tx = setup_single_back(backend_addresses3_back[back_to_revive].clone()).await;
                    back_shut_vec3_back[back_to_revive] = Some(shut_tx);
                    println!("BACKEND {} REVIVE!!!!!!!", back_to_revive);
                },
                10 => {
                    let back_to_revive = 3;
                    let shut_tx = setup_single_back(backend_addresses3_back[back_to_revive].clone()).await;
                    back_shut_vec3_back[back_to_revive] = Some(shut_tx);
                    println!("BACKEND {} REVIVE!!!!!!!", back_to_revive);
                },
                _ => {
                    continue;
                }
            }
            back_cp += 1;
        }
    });

    tokio::spawn(async move {
    let mut keeper_cp = 0;
    let mut keeper_interval = time::interval(time::Duration::from_secs(60));
    loop {
        keeper_interval.tick().await;
        match keeper_cp {
            0 => {},
            1 => {
                let keeper_to_die = 0;
                let _ = keeper_shut_vec3_keep[keeper_to_die].as_ref().unwrap().send(()).await;
                keeper_shut_vec3_keep[keeper_to_die] = None;
                println!("KEEPER {} DIE!!!!!!!", keeper_to_die);
            },
            2 => {
                let keeper_to_revive = 2;
                let shut_tx = setup_single_keeper(keeper_to_revive, keeper_addresses3_keep.clone(), backend_addresses3_keep.clone()).await;
                keeper_shut_vec3_keep[keeper_to_revive] = Some(shut_tx);
                println!("keeper {} REVIVE!!!!!!!", keeper_to_revive);
            },
            3 => {
                let keeper_to_die = 1;
                let _ = keeper_shut_vec3_keep[keeper_to_die].as_ref().unwrap().send(()).await;
                keeper_shut_vec3_keep[keeper_to_die] = None;
                println!("KEEPER {} DIE!!!!!!!", keeper_to_die);
            },
            4 => {
                let keeper_to_revive = 0;
                let shut_tx = setup_single_keeper(keeper_to_revive, keeper_addresses3_keep.clone(), backend_addresses3_keep.clone()).await;
                keeper_shut_vec3_keep[keeper_to_revive] = Some(shut_tx);
                println!("keeper {} REVIVE!!!!!!!", keeper_to_revive);
                
            },
            _ => {
                continue;
            }
        }
        keeper_cp += 1;
    }});

    let mut clock_interval = time::interval(time::Duration::from_secs(3));
    let mut action_interval = time::interval(time::Duration::from_millis(10));
    let mut rng = rand::thread_rng();
    let mut cur_clock = 0;

    loop {
        tokio::select! {
            _ = clock_interval.tick() => {
                let bin_name: usize = rng.gen_range(0..BIN_NUM);
                let bin_name = format!("bin{}", bin_name);
                let adapter3 = bin_client3_clock.bin(&bin_name).await.unwrap();
                println!("3s Clock bin: {}", bin_name);
                let res3 = adapter3.clock(0).await.unwrap();
                assert!(res3 >= cur_clock, "\n
                Clock is smaller!\n
                Before: {}, After {}.\n",
                cur_clock, res3);
                cur_clock = res3;
            },
            _ = action_interval.tick() => {
                let action_res = perform_random_action(&bin_client2, &bin_client3, &mut clock_records).await;
                assert!(!action_res.is_err(), "ONE ACTION FAIL!!!!!!");
                op_count += 1;
                if op_count % 100 == 0 {
                    println!("{}", op_count);
                }
                if op_count >= 1500 {
                    break;
                }
            }
        }
    }

    println!("End all operations");
    check_final_status(&bin_client2, &bin_client3).await?;
    println!("End final status check");
    
    Ok(())
}


// cargo test --package lab --test lab3_test -- test_bin_storage --exact --nocapture