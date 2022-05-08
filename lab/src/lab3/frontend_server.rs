use async_trait::async_trait;

use serde::{Deserialize, Serialize};
use std::cmp::{min, Ordering};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tribbler::err::{TribResult, TribblerError};
use tribbler::storage::BinStorage;
use tribbler::storage::{self, KeyValue};
use tribbler::trib::{
    is_valid_username, Server, Trib, MAX_FOLLOWING, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER,
};

pub const REGISTERD_USERS_TABLE_NAME: &str = "REGISTERED_USERS";
pub const SIGNUP_KEY_SUFFIX: &str = "SIGNUP";
pub const TRIB_KEY_SUFFIX: &str = "TRIBS";
pub const FOLLOWLOG_KEY_SUFFIX: &str = "FOLLOWLOG";

pub struct FrontendServer {
    bin_client: Box<dyn BinStorage>,
}

impl FrontendServer {
    pub fn new(bin_client: Box<dyn BinStorage>) -> Self {
        Self { bin_client }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FollowingLogRecord {
    /// who starts the following operation
    pub who: String,
    /// who will be following
    pub whom: String,
    /// whether it's follow or unfollow
    pub is_follow_flag: bool,
    /// logical clock when appending the opeartion
    pub clock_id: u64,
}

#[async_trait]
pub trait FrontendHelper {
    async fn user_exist(&self, who: &str) -> TribResult<bool>;
    async fn get_following_list(&self, who: &str) -> TribResult<Vec<String>>;
    // is follow: Follow or Unfollow
    async fn start_following_transaction(
        &self,
        who: &str,
        whom: &str,
        is_follow_flag: bool,
    ) -> TribResult<()>;
    fn list_tribs(&self, tribs: Vec<Arc<Trib>>) -> Vec<Arc<Trib>>;
}

#[async_trait]
impl FrontendHelper for FrontendServer {
    async fn user_exist(&self, who: &str) -> TribResult<bool> {
        let client_future = self.bin_client.bin(who);
        let client = client_future.await?;
        let signup_key = format!("{}::{}", who, SIGNUP_KEY_SUFFIX);
        let res = client.get(signup_key.as_str()).await?;
        if res.is_none() {
            return Ok(false);
        }
        return Ok(true);
    }

    fn list_tribs(&self, mut tribs: Vec<Arc<Trib>>) -> Vec<Arc<Trib>> {
        tribs.sort_by(|a, b| {
            if a.clock < b.clock {
                return Ordering::Less;
            } else if a.clock > b.clock {
                return Ordering::Greater;
            }

            if a.time < b.time {
                return Ordering::Less;
            } else if a.time > b.time {
                return Ordering::Greater;
            }

            if a.user < b.user {
                return Ordering::Less;
            } else if a.user > b.user {
                return Ordering::Greater;
            }

            if a.message < b.message {
                return Ordering::Less;
            } else if a.message > b.message {
                return Ordering::Greater;
            }

            return Ordering::Equal;
        });
        let ntrib = tribs.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };
        (&tribs[start..]).to_vec()
    }

    async fn get_following_list(&self, who: &str) -> TribResult<Vec<String>> {
        let client_future_who = self.bin_client.bin(who);
        let client_who = client_future_who.await?;
        let follow_log_key = format!("{}::{}", who, FOLLOWLOG_KEY_SUFFIX);
        let follow_log = client_who.list_get(follow_log_key.as_str()).await?.0;
        // println!("following log: {:?}", follow_log);

        let mut following_hashset: HashSet<String> = HashSet::new();
        for following_record_string in follow_log {
            let record: FollowingLogRecord =
                serde_json::from_str(&following_record_string).unwrap();
            if record.is_follow_flag {
                following_hashset.insert(record.whom.to_string());
            } else {
                following_hashset.remove(&record.whom.to_string());
            }
        }

        let mut following_list = following_hashset.into_iter().collect::<Vec<String>>();
        following_list.sort();
        // println!("following list: {:?}", following_list);
        return Ok(following_list);
    }

    async fn start_following_transaction(
        &self,
        who: &str,
        whom: &str,
        is_follow_flag: bool,
    ) -> TribResult<()> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }

        let who_exist = self.user_exist(who).await?;
        if !who_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        let whom_exist = self.user_exist(whom).await?;
        if !whom_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        // Append Log Entry
        let client_future_who = self.bin_client.bin(who);
        let client_who = client_future_who.await?;
        let follow_log_key = format!("{}::{}", who, FOLLOWLOG_KEY_SUFFIX);
        let my_clock_id = client_who.clock(0).await?;

        let record_to_append = FollowingLogRecord {
            who: who.to_string(),
            whom: whom.to_string(),
            is_follow_flag: is_follow_flag,
            clock_id: my_clock_id,
        };
        let record_string_to_append = serde_json::to_string(&record_to_append)?;

        let append_flag = client_who
            .list_append(&KeyValue {
                key: follow_log_key.to_string(),
                value: record_string_to_append.clone(),
            })
            .await?;
        if !append_flag {
            return Err(Box::new(TribblerError::Unknown(
                "Following log record append error".to_string(),
            )));
        }

        // Race Check
        let follow_log = client_who.list_get(follow_log_key.as_str()).await?.0;
        let mut following_hashset: HashSet<String> = HashSet::new();
        for record_string in follow_log {
            let record: FollowingLogRecord = serde_json::from_str(&record_string).unwrap();
            if record.whom == whom && record.clock_id == my_clock_id {
                // Found my record!!!
                // Now starts to check race
                if is_follow_flag & following_hashset.contains(whom) {
                    // I try to follow somebody
                    // But somebody beats me
                    // // clean up my invalid log
                    client_who
                        .list_remove(&KeyValue {
                            key: follow_log_key.to_string(),
                            value: record_string_to_append.clone(),
                        })
                        .await?;
                    return Err(Box::new(TribblerError::AlreadyFollowing(
                        who.to_string(),
                        whom.to_string(),
                    )));
                } else if !is_follow_flag && !following_hashset.contains(whom) {
                    // I try to unfollow somebody
                    // But somebody beats me
                    // // clean up my invalid log
                    client_who
                        .list_remove(&KeyValue {
                            key: follow_log_key.to_string(),
                            value: record_string_to_append.clone(),
                        })
                        .await?;
                    return Err(Box::new(TribblerError::NotFollowing(
                        who.to_string(),
                        whom.to_string(),
                    )));
                }
            }
            if record.is_follow_flag {
                following_hashset.insert(record.whom.to_string());
            } else {
                following_hashset.remove(&record.whom.to_string());
            }
        }
        Ok(())
    }
}

#[async_trait] // VERY IMPORTANT !!!=
impl Server for FrontendServer {
    async fn sign_up(&self, user: &str) -> tribbler::err::TribResult<()> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        let user_exist = self.user_exist(user).await?;
        if user_exist {
            return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
        }
        // update kv hashset for user
        let mut client_future = self.bin_client.bin(user);
        let mut client = client_future.await?;
        let signup_key = format!("{}::{}", user, SIGNUP_KEY_SUFFIX);
        client
            .set(&storage::KeyValue {
                key: signup_key.to_string(),
                value: "ok".to_string(),
            })
            .await?;
        // update list if needed
        client_future = self.bin_client.bin(REGISTERD_USERS_TABLE_NAME);
        client = client_future.await?;
        let k = client.list_get(REGISTERD_USERS_TABLE_NAME).await?.0;
        if k.len() >= MIN_LIST_USER {
            return Ok(());
        }
        let _ = client
            .list_append(&storage::KeyValue {
                key: REGISTERD_USERS_TABLE_NAME.to_string(),
                value: user.to_string(),
            })
            .await?;
        Ok(())
    }

    async fn list_users(&self) -> TribResult<Vec<String>> {
        let client_future = self.bin_client.bin(REGISTERD_USERS_TABLE_NAME);
        let client = client_future.await?;
        let mut k = client.list_get(REGISTERD_USERS_TABLE_NAME).await?.0;
        k.sort();
        let sorted = k[..min(MIN_LIST_USER, k.len())].to_vec();
        let res: Vec<String> = sorted
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        Ok(res)
    }

    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }
        if clock == u64::MAX {
            return Err(Box::new(TribblerError::MaxedSeq));
        }

        let user_exist = self.user_exist(who).await?;
        if !user_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        let client_future = self.bin_client.bin(who);
        let client = client_future.await?;
        let max_clock = client.clock(clock + 1).await?;
        // println!("server clock: {}; client clock: {}", max_clock, clock);

        // serialize trib
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let trib = Trib {
            user: who.to_string(),
            message: post.to_string(),
            time,
            clock: max_clock,
        };
        let trib_val = serde_json::to_string(&trib)?;

        // add it to my tribs
        let trib_key = format!("{}::{}", who, TRIB_KEY_SUFFIX);
        client
            .list_append(&storage::KeyValue {
                key: trib_key,
                value: trib_val.to_string(),
            })
            .await?;

        Ok(())
    }

    async fn tribs(&self, user: &str) -> TribResult<Vec<std::sync::Arc<tribbler::trib::Trib>>> {
        let user_exist = self.user_exist(user).await?;
        if !user_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        let client = self.bin_client.bin(user).await?;
        let trib_key = format!("{}::{}", user, TRIB_KEY_SUFFIX);
        let trib_strs = client.list_get(trib_key.as_str()).await?.0;
        let mut trib_vecs: Vec<Arc<Trib>> = vec![];

        for trib_str in trib_strs {
            let trib: Trib = serde_json::from_str(&trib_str).unwrap();
            trib_vecs.push(Arc::new(trib))
        }
        let retval = self.list_tribs(trib_vecs);
        Ok(retval)
    }

    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        // Section 1: error check
        let following_list = self.get_following_list(who).await?;
        if following_list.len() >= MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }

        let _ = self.start_following_transaction(who, whom, true).await?;
        Ok(())
    }

    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        let _ = self.start_following_transaction(who, whom, false).await?;
        Ok(())
    }

    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        // Section 1: error check
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }

        let who_exist = self.user_exist(who).await?;
        if !who_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        let whom_exist = self.user_exist(whom).await?;
        if !whom_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let following_list = self.get_following_list(who).await?;
        if !following_list.contains(&whom.to_string()) {
            return Ok(false);
        }
        Ok(true)
    }

    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        let who_exist = self.user_exist(who).await?;
        if !who_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let following_list = self.get_following_list(who).await?;
        Ok(following_list)
    }

    async fn home(&self, user: &str) -> TribResult<Vec<std::sync::Arc<tribbler::trib::Trib>>> {
        let user_exist = self.user_exist(user).await?;
        if !user_exist {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        let client_future = self.bin_client.bin(user);
        let client = client_future.await?;
        let own_trib_key = format!("{}::{}", user, TRIB_KEY_SUFFIX);
        let trib_strs = client.list_get(own_trib_key.as_str()).await?.0;
        let mut trib_vecs: Vec<Arc<Trib>> = vec![];

        for trib_str in trib_strs {
            let trib: Trib = serde_json::from_str(&trib_str).unwrap();
            trib_vecs.push(Arc::new(trib));
        }

        let following_list = self.get_following_list(user).await?;
        for following in following_list {
            let client_future_following_trib = self.bin_client.bin(following.as_str());
            let client_following_trib = client_future_following_trib.await?;
            let following_trib_key = format!("{}::{}", following, TRIB_KEY_SUFFIX);
            let following_trib_strs = client_following_trib
                .list_get(following_trib_key.as_str())
                .await?
                .0;
            for trib_str in following_trib_strs {
                let trib: Trib = serde_json::from_str(&trib_str).unwrap();
                trib_vecs.push(Arc::new(trib));
            }
        }

        let retval = self.list_tribs(trib_vecs);
        Ok(retval)
    }
}
