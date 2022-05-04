use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration, collections::{HashMap, HashSet}, cmp, vec,
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

async fn setup(backs: Vec<String>, keeper_addr: Vec<String>) -> TribResult<(MpscSender<()>, MpscSender<()>, MpscSender<()>, MpscSender<()>, MpscSender<()>, MpscSender<()>)> {
    let (shut_tx1, shut_rx1) = tokio::sync::mpsc::channel(1);
    let (shut_tx2, shut_rx2) = tokio::sync::mpsc::channel(1);
    let (shut_tx3, shut_rx3) = tokio::sync::mpsc::channel(1);
    let (shut_tx4, shut_rx4) = tokio::sync::mpsc::channel(1);
    let (shut_tx5, shut_rx5) = tokio::sync::mpsc::channel(1);
    let (shut_tx6, shut_rx6) = tokio::sync::mpsc::channel(1);
    let cfg1 = BackConfig {
        addr: backs[0].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx1),
    };
    let cfg2 = BackConfig {
        addr: backs[1].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx2),
    };
    let cfg3 = BackConfig {
        addr: backs[2].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx3),
    };
    let cfg4 = BackConfig {
        addr: backs[3].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx4),
    };
    let kfg1 = KeeperConfig {
        backs: backs.clone(),
        addrs: keeper_addr.clone(),
        this: 0,
        id: 1,
        ready: None,
        shutdown: Some(shut_rx5),
    };
    let kfg2 = KeeperConfig {
        backs: backs.clone(),
        addrs: keeper_addr.clone(),
        this: 1,
        id: 2,
        ready: None,
        shutdown: Some(shut_rx6),
    };
    spawn_back(cfg1);
    spawn_back(cfg2);
    spawn_back(cfg3);
    spawn_back(cfg4);
    spawn_keep(kfg1);
    spawn_keep(kfg2);
    tokio::time::sleep(Duration::from_millis(777)).await;
    Ok((shut_tx1.clone(), shut_tx2.clone(), shut_tx3.clone(), shut_tx4.clone(), shut_tx5.clone(), shut_tx6.clone()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_follow() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33950".to_string(),
        "127.0.0.1:33951".to_string(),
        "127.0.0.1:33952".to_string(),
        "127.0.0.1:33953".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33954".to_string(),
        "127.0.0.1:33955".to_string(),
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone().clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;
    let crazy_cs_following_list = frontend.following(crazy_fan_johnny_su).await?;
    println!("test_simple_follow: {:?}", crazy_cs_following_list);
    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_duplicate_follow() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33001".to_string(),
        "127.0.0.1:33002".to_string(),
        "127.0.0.1:33003".to_string(),
        "127.0.0.1:33004".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33005".to_string(),
        "127.0.0.1:33006".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;
    let res = frontend.follow(crazy_fan_johnny_su, speechless_professor).await;
    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    if res.is_ok() {
        assert!(false, "what the fuck dude you are not supposed to follow professor twice you creepy motherfucker!");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_duplicate_unfollow() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33011".to_string(),
        "127.0.0.1:33012".to_string(),
        "127.0.0.1:33013".to_string(),
        "127.0.0.1:33014".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33015".to_string(),
        "127.0.0.1:33016".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;

    let mut res = frontend.unfollow(crazy_fan_johnny_su, speechless_professor).await;
    if res.is_ok() {
        assert!(false, "what the fuck dude you can't unfollow if you aren't following!");
    }

    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;
    frontend.unfollow(crazy_fan_johnny_su, speechless_professor).await?;
    res = frontend.unfollow(crazy_fan_johnny_su, speechless_professor).await;
    if res.is_ok() {
        assert!(false, "what the fuck dude you can't do duplicate unfollow ;( I'm so disappointed at you!");
    }

    let following_list = frontend.following(crazy_fan_johnny_su).await?;
    println!("test_duplicate_unfollow final following list: {:?}", following_list);
    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33021".to_string(),
        "127.0.0.1:33022".to_string(),
        "127.0.0.1:33023".to_string(),
        "127.0.0.1:33024".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33025".to_string(),
        "127.0.0.1:33026".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend0 = lab3::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend0.sign_up(crazy_fan_johnny_su).await?;
    frontend0.sign_up(speechless_professor).await?;

    let mut promises = vec![];
    let NUM_CONCURRENCY = 10;
    let mut error_count = 0;

    // for i in 0..NUM_CONCURRENCY {
    //     promises.push(frontend.follow(crazy_fan_johnny_su, speechless_professor));
    // }

    // for promise in promises {
    //     let res = promise.await;
    //     if res.is_err() {
    //         error_count += 1;
    //     }
    // }

    for i in 0..NUM_CONCURRENCY {
        let bc = lab3::new_bin_client(backs.clone()).await?;
        let frontend = lab3::new_front(bc).await?;
        promises.push(tokio::task::spawn(async move { frontend.follow(crazy_fan_johnny_su, speechless_professor).await }));
    }

    for promise in promises {
        let res = promise.await?;
        if res.is_err() {
            error_count += 1;
        }
    }

    let bc = lab3::new_bin_client(backs.clone()).await?;
    let client_future_who = bc.bin(crazy_fan_johnny_su);
    let client_who = client_future_who.await?;
    let follow_log_key = format!("{}::{}", crazy_fan_johnny_su, "FOLLOWLOG");
    let follow_log = client_who.list_get(follow_log_key.as_str()).await?.0;
    println!("following log: {:?}", follow_log);

    if error_count != NUM_CONCURRENCY - 1 {
        assert!(false, "{}??? YOU KNOW WHAT IT IS!!! IT'S THE RACE!!! YOU DIDN'T SYNCHRONIZE AND NOW WE HAVE TO FACE THE KARMA!!!", error_count);
    }
    let following_list = frontend0.following(crazy_fan_johnny_su).await?;
    println!("test_concurrent_follow final following list: {:?}", following_list);

    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_bins_diff_keys_massive_set_get() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33031".to_string(),
        "127.0.0.1:33032".to_string(),
        "127.0.0.1:33033".to_string(),
        "127.0.0.1:33034".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33035".to_string(),
        "127.0.0.1:33036".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bin_client = lab3::new_bin_client(backs.clone()).await?;
    let STRING_LEN = 30;
    let NUM_KEYS = 777;

    let mut key_val_map = HashMap::new();
    let mut key_bin_map = HashMap::new();
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

    for key in key_val_map.keys() {
        let bin_name = key_bin_map.get(key).unwrap();
        let expected_value = key_val_map.get(key).unwrap();
        let client = bin_client.bin(bin_name.as_str()).await?;
        let actual_value = client.get(key).await?.unwrap();
        if actual_value != expected_value.to_string() {
            assert!(false, "OH BOY, OH BOY, OH BOY!!! dude you just failed the mass get set test 
            ;( sry but this is really nothing but a very basic feature yet you failed it")
        }
    }
    
    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_bins_same_key_massive_set_get() -> TribResult<()> {
    // funny story: 根据pigeonhole principle (雀巢定律，鸽笼定律，抽屉原理，whatever you called it blablabla)
    // 我们只需要4个bin!!!就可以测出你有没有分隔开来virtual bins!!!
    let backs = vec![
        "127.0.0.1:33041".to_string(),
        "127.0.0.1:33042".to_string(),
        "127.0.0.1:33043".to_string(),
        "127.0.0.1:33044".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33045".to_string(),
        "127.0.0.1:33046".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bin_client = lab3::new_bin_client(backs.clone()).await?;
    let STRING_LEN = 30;

    let KEY = "jerkoff";
    let mut bin_val_map = HashMap::new();
    for i in 0..1000 {
        let bin_name = generate_random_username(STRING_LEN);
        let value = generate_random_username(STRING_LEN);
        bin_val_map.insert(bin_name.to_string(), value.to_string());
        let client = bin_client.bin(bin_name.as_str()).await?;
        client.set(&KeyValue {
            key: KEY.to_string(),
            value: value.to_string(),
        }).await?;
    }

    for bin_name in bin_val_map.keys() {
        let expected_value = bin_val_map.get(bin_name).unwrap();
        let client = bin_client.bin(bin_name.as_str()).await?;
        let actual_value = client.get(KEY).await?.unwrap();
        if actual_value != expected_value.to_string() {
            assert!(false, "Where's our friendly little indirection layer that virtually separates the bins? Huh? WHERE IS IT? WHERE IS IT???!!!!");
        }
    }
    
    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_signup_users() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33051".to_string(),
        "127.0.0.1:33052".to_string(),
        "127.0.0.1:33053".to_string(),
        "127.0.0.1:33054".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33055".to_string(),
        "127.0.0.1:33056".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    let mut hashset = HashSet::new();
    let first_guy = generate_random_username(7);
    frontend.sign_up(first_guy.as_str()).await?;
    hashset.insert(first_guy.to_string());
    for i in 0..30 {
        let user_name = generate_random_username(7);
        hashset.insert(user_name.to_string());
        frontend.sign_up(user_name.as_str()).await?;
    }
    let thirtish_guy = generate_random_username(7);
    frontend.sign_up(thirtish_guy.as_str()).await?;
    hashset.insert(thirtish_guy.to_string());
    
    let register_list = frontend.list_users().await?;
    if register_list.len() < 20 {
        assert!(false, "register list has wrong len");
    }
    println!("register list: {:?}", register_list);
    for user in register_list {
        if !hashset.contains(&user) {
            assert!(false, "duuude you should contain user {}", user.to_string());
        }
    }

    let res_first = frontend.sign_up(first_guy.as_str()).await;
    if res_first.is_ok() {
        assert!(false, "duuude first guy should be an error");
    }

    let res_thir = frontend.sign_up(thirtish_guy.as_str()).await;
    if res_thir.is_ok() {
        assert!(false, "duuude thirtish guy should be an error");
    }

    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_signup_users_less_than_20() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33061".to_string(),
        "127.0.0.1:33062".to_string(),
        "127.0.0.1:33063".to_string(),
        "127.0.0.1:33064".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33065".to_string(),
        "127.0.0.1:33066".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    let mut hashset = HashSet::new();
    let NUMBER = 15;
    for i in 0..NUMBER {
        let user_name = generate_random_username(7);
        hashset.insert(user_name.to_string());
        frontend.sign_up(user_name.as_str()).await?;
    }
    
    let register_list = frontend.list_users().await?;
    if register_list.len() != NUMBER {
        assert!(false, "register list has wrong len");
    }
    println!("register list: {:?}", register_list);
    for user in register_list.clone() {
        if !hashset.contains(&user) {
            assert!(false, "duuude you should contain user {}", user.to_string());
        }
    }

    for user in register_list.clone() {
        let res = frontend.sign_up(&user).await;
        if res.is_ok() {
            assert!(false, "not supposed to sign up");
        }
    }

    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_tribs_1_and_3() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33071".to_string(),
        "127.0.0.1:33072".to_string(),
        "127.0.0.1:33073".to_string(),
        "127.0.0.1:33074".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33075".to_string(),
        "127.0.0.1:33076".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;

    frontend.post(speechless_professor, "al_p0", 0).await?;
    frontend.post(speechless_professor, "al_p1", 1).await?;
    frontend.post(speechless_professor, "al_p2", 2).await?;
    frontend.post(speechless_professor, "al_p3", 100).await?;
    
    let home_tribs = frontend.home(crazy_fan_johnny_su).await?;
    let mut max_clock = 0;
    for trib in home_tribs {
        max_clock = cmp::max(max_clock, trib.to_owned().clock);
    }
    frontend.post(crazy_fan_johnny_su, "su_p0", max_clock).await?;
    frontend.post(crazy_fan_johnny_su, "su_p1", max_clock).await?;
    let tribs = frontend.tribs(crazy_fan_johnny_su).await?;
    let last_clock = tribs[tribs.len()-1].to_owned().clock;
    let second_last_clock =  tribs[tribs.len()-2].to_owned().clock;

    if last_clock != 101 || second_last_clock != 100 {
        assert!(false, "Just put a bullet in my fxxking brain plz");
    }

    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_simple_tribs_2() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33081".to_string(),
        "127.0.0.1:33082".to_string(),
        "127.0.0.1:33083".to_string(),
        "127.0.0.1:33084".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33085".to_string(),
        "127.0.0.1:33086".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;

    frontend.post(speechless_professor, "al", 100).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    frontend.post(crazy_fan_johnny_su, "js", 0).await?;
    
    let home_tribs = frontend.home(crazy_fan_johnny_su).await?;
    if home_tribs.len() != 2 {
        assert!(false, "my soul is burning in hell");
    }
    let first_poster = &home_tribs[0].to_owned().user;
    let first_trib = &home_tribs[0].to_owned().message;
    let first_clock = &home_tribs[0].to_owned().clock;
    let second_poster =  &home_tribs[1].to_owned().user;
    let second_trib = &home_tribs[1].to_owned().message;
    let second_clock = &home_tribs[1].to_owned().clock;
    println!("{}, {}, {}, {}, {}, {}", first_poster, first_trib, first_clock, second_poster, second_trib, second_clock);

    if first_poster != speechless_professor 
    || second_poster != crazy_fan_johnny_su 
    || second_clock <= &100 
    || first_trib != "al"
    || second_trib != "js" {
        assert!(false, "highway to hell!!!!");
    }

    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_home_correctness() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33091".to_string(),
        "127.0.0.1:33092".to_string(),
        "127.0.0.1:33093".to_string(),
        "127.0.0.1:33094".to_string(),
    ];
    let keeper_addr = vec![
        "127.0.0.1:33095".to_string(),
        "127.0.0.1:33096".to_string()
    ];
    let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), keeper_addr.clone()).await?;
    let bc = lab3::new_bin_client(backs.clone()).await?;
    let frontend = lab3::new_front(bc).await?;

    frontend.sign_up("u1").await?;
    frontend.sign_up("u2").await?;
    frontend.follow("u1", "u2").await?;
    frontend.follow("u2", "u1").await?;
    
    frontend.post("u1", "trib1", 0).await?;
    frontend.post("u1", "trib2", 10).await?;
    frontend.post("u2", "trib3", 20).await?;

    println!("u2 tribs");
    let tribs = frontend.tribs("u2").await?;
    for trib in tribs {
        let user = &trib.to_owned().user;
        let msg = &trib.to_owned().message;
        println!("user: {}, msg: {}", user, msg);
    }

    println!("u1 home");
    let tribs = frontend.home("u1").await?;
    for trib in tribs {
        let user = &trib.to_owned().user;
        let msg = &trib.to_owned().message;
        println!("user: {}, msg: {}", user, msg);
    }

    println!("u2 home");
    let tribs = frontend.home("u2").await?;
    for trib in tribs {
        let user = &trib.to_owned().user;
        let msg = &trib.to_owned().message;
        println!("user: {}, msg: {}", user, msg);
    }

    println!("u1 unfollow u2 home");
    frontend.unfollow("u1", "u2").await?;
    let tribs = frontend.home("u1").await?;
    for trib in tribs {
        let user = &trib.to_owned().user;
        let msg = &trib.to_owned().message;
        println!("user: {}, msg: {}", user, msg);
    }

    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_massive_broadcast_test() -> TribResult<()> {
    let mut backs = vec![];
    let mut shutdown_send_chans = vec![];
    let prefix = "127.0.0.1:48";
    for i in 0..300 {
        let u32_i : u32 = i;
        let backend_addr = format!("{}{:03}", prefix, u32_i);
        backs.push(backend_addr.to_string());
        let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
        let cfg = BackConfig {
            addr: backend_addr.to_string(),
            storage: Box::new(MemStorage::default()),
            ready: None,
            shutdown: Some(shut_rx),
        };
        let _ = spawn_back(cfg);
        println!("set up: {}", backend_addr.to_string());
        shutdown_send_chans.push(shut_tx.clone());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (shut_tx_keeper, shut_rx_keeper) = tokio::sync::mpsc::channel(1);
    let keeper_addr = "127.0.0.1:49999";
    let kfg = KeeperConfig {
        backs: backs.clone(),
        addrs: vec![keeper_addr.to_string()],
        this: 0,
        id: 0,
        ready: None,
        shutdown: Some(shut_rx_keeper),
    };
    spawn_keep(kfg);

    tokio::time::sleep(Duration::from_secs(10)).await;
    for shutdown_chan in shutdown_send_chans {
        let _ = shutdown_chan.send(()).await;
    }
    let _ = shut_tx_keeper.send(()).await;
    Ok(())
}

// cargo test -p lab --test lab2_test -- --nocapture