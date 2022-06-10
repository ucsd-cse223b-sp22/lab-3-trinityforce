use std::{
    time::{Duration, Instant},
};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use lab::{self, lab3};
use tokio::{sync::mpsc::Sender as MpscSender, time};
use tribbler::{config::KeeperConfig, storage::BinStorage};
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

const BIN_NUM: usize = 1;
const KEY_NUM: usize = 1;
const VAL_NUM: usize = 20;
const READ_WRITE_RATIO: usize = 7; //0..=10
const TOTAL_OP: usize = 3000;
const NUM_CLIENT: u64 = 10;

fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    println!("setting up back");
    tokio::spawn(lab3::serve_back(cfg))
}

fn spawn_keep(kfg: KeeperConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab3::serve_keeper(kfg))
}

async fn perform_random_action(lab3: &Box<dyn BinStorage>, rng: &mut StdRng) -> TribResult<()> {
    let action_type: usize = rng.gen_range(0..10);
    let action: usize;
    if action_type >= READ_WRITE_RATIO {
        // Action: get = 0, list_get = 1, keys = 2, list_keys = 3
        // action = rng.gen_range(0..4);
        action = 0;
    } else {
        // Action: set = 4, list_append = 5, list_remove = 6
        // action = rng.gen_range(4..7);
        action = 4;
    }

    // Bin, key, val
    let bin_name: usize = rng.gen_range(0..BIN_NUM);
    let bin_name = format!("bin{}", bin_name);
    let key: usize = rng.gen_range(0..KEY_NUM);
    let key = format!("key{}", key);
    let val: usize = rng.gen_range(0..VAL_NUM);
    let val = format!("val{}", val);

    // Perform action
    let adapter3 = lab3.bin(&bin_name).await?;

    match action {
        0 => {
            let _ = adapter3.get(&key).await?;
        },
        1 => {
            let _ = adapter3.list_get(&key).await?.0;
        },
        2 => {
            let _ = adapter3.keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
        },
        3 => {
            let _ = adapter3.list_keys(&Pattern{prefix: "".to_string(), suffix: "".to_string()}).await?.0;
        },
        4 => {
            let _ = adapter3.set(&KeyValue { key: key.clone(), value: val.clone() }).await?;
        },
        5 => {
            let _ = adapter3.list_append(&KeyValue { key: key.clone(), value: val.clone() }).await?;
        },
        6 => {
            let _ = adapter3.list_remove(&KeyValue { key: key.clone(), value: val.clone() }).await?;
        },
        _ => {
            println!("Should not reach here!")
        },
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bin_storage_failure() -> TribResult<()> {
    lab3::new_lockserver_ping_test().await?;
    let keeper_addresses3 = generate_addresses(3, false);
    let backend_addresses3 = generate_addresses(8, true);
    let keeper_addresses3_keep = keeper_addresses3.clone();
    let backend_addresses3_back = backend_addresses3.clone();
    let backend_addresses3_keep = backend_addresses3.clone();
    let (back_shut_vec3, keeper_shut_vec3) = setup(backend_addresses3.clone(), keeper_addresses3.clone(), 8, 2).await?;
    let mut back_shut_vec3_back = back_shut_vec3.clone();
    let mut keeper_shut_vec3_keep = keeper_shut_vec3.clone();
    
    println!("Start testing!");
    let bin_client3 = lab3::new_bin_client(backend_addresses3.clone()).await?;

    let mut op_count = 0;


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

    let mut rng = StdRng::seed_from_u64(2022);
    let start_time = Instant::now();
    loop {
        let _ = perform_random_action(&bin_client3, &mut rng).await;
        op_count += 1;
        if op_count % 250 == 0 {
            println!("{}", op_count);
        }
        if op_count >= TOTAL_OP {
            break;
        }
    }
    let end_time = Instant::now();
    println!("End all operations");
    println!("Total Time cost: {:?}", end_time.duration_since(start_time));
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bin_storage_no_failure() -> TribResult<()> {
    let keeper_addresses3 = generate_addresses(3, false);
    let backend_addresses3 = generate_addresses(8, true);
    let keeper_addresses3_keep = keeper_addresses3.clone();
    let backend_addresses3_back = backend_addresses3.clone();
    let backend_addresses3_keep = backend_addresses3.clone();
    let (back_shut_vec3, keeper_shut_vec3) = setup(backend_addresses3.clone(), keeper_addresses3.clone(), 8, 2).await?;
    let mut back_shut_vec3_back = back_shut_vec3.clone();
    let mut keeper_shut_vec3_keep = keeper_shut_vec3.clone();
    
    println!("Start testing!");
    
    let mut clients = vec![];

    let start_time = Instant::now();

    for _ in 0..NUM_CLIENT {
        let back_addr = backend_addresses3.clone();
        clients.push(tokio::spawn(async move {
            let bin_client3 = lab3::new_bin_client(back_addr.clone()).await.unwrap();
            let mut op_count = 0;
            let mut rng = StdRng::seed_from_u64(2022);
            loop {
                let _ = perform_random_action(&bin_client3, &mut rng).await;
                op_count += 1;
                if op_count % 250 == 0 {
                    println!("{}", op_count);
                }
                if op_count >= TOTAL_OP {
                    break;
                }
            }
        }));
    }
    futures::future::join_all(clients).await;
    let end_time = Instant::now();
    println!("End all operations");
    println!("Time cost: {:?}", end_time.duration_since(start_time));
    Ok(())
}

// cargo test --package lab --test final_project_test_and_evaluation -- test_bin_storage_failure --exact --nocapture