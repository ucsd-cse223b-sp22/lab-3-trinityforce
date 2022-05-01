use crate::lab3::keeper_server::KeeperHelper;

use super::keeper_rpc_receiver::KeeperRPCReceiver;
use super::keeper_server::KeeperServer;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time;
use tribbler::storage::BinStorage;
use tribbler::trib;
use tribbler::{config::BackConfig, err::TribResult};

use super::bin_client::BinStorageClient;
use super::frontend_server::FrontendServer;
use tribbler::config::KeeperConfig;

use super::backend_server::BackendServer;
use super::client::StorageClient;

use super::super::keeper::keeper_service_server::KeeperServiceServer;
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::storage::Storage;

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    let bin_client = BinStorageClient::new(backs);
    return Ok(Box::new(bin_client));
}

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.

#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let backs = kc.backs.clone();
    let mut backs_status = vec![];
    for ind in 0..backs.len() {
        let client = StorageClient::new(backs[ind].as_str());
        let res = client.clock(0).await;
        if res.is_err() {
            backs_status.push(false);
        } else {
            backs_status.push(true);
        }
    }
    let keeper = KeeperServer::new(kc.this, kc.addrs.clone(), &kc.backs.clone(), backs_status);
    let mut broadcast_logical_interval = time::interval(time::Duration::from_secs(1));
    let mut migrate_interval = time::interval(time::Duration::from_secs(5));

    let (mut rpc_server_ready_send, mut rpc_server_ready_recv): (Sender<()>, Receiver<()>) =
        mpsc::channel(1);
    let (mut rpc_server_shutdown_send, mut rpc_server_shutdown_recv): (Sender<()>, Receiver<()>) =
        mpsc::channel(1);
    tokio::spawn(async move {
        let keeper_rpc_server = KeeperRPCReceiver::new();
        let config_addr = &kc.addrs.clone()[kc.this];
        let config_addr_str = config_addr.as_str();
        let config_addr_string = config_addr_str.replace("localhost", "127.0.0.1");
        let replaced_config_addr_str = config_addr_string.as_str();
        let server_addr: SocketAddr;
        let parsed_addr = match replaced_config_addr_str.parse::<SocketAddr>() {
            Ok(value) => {
                server_addr = value;
            }
            Err(e) => return Err(Box::new(e)),
        };
        let server_status = tonic::transport::Server::builder()
            .add_service(KeeperServiceServer::new(keeper_rpc_server))
            .serve_with_shutdown(server_addr, async {
                rpc_server_ready_send.send(()).await;
                rpc_server_shutdown_recv.recv().await;
            })
            .await;
        Ok(())
    });

    rpc_server_ready_recv.recv().await;
    match kc.ready {
        Some(ready_chan) => {
            let _ = ready_chan.clone().send(true);
        }
        None => {}
    }
    match kc.shutdown {
        Some(mut shut_chan) => loop {
            tokio::select! {
                _ = migrate_interval.tick() => {
                    let _ = keeper.check_migration().await;
                }
                _ = broadcast_logical_interval.tick() => {
                    let _ = keeper.broadcast_logical_clock().await;
                }
                _ = shut_chan.recv() => {
                    rpc_server_shutdown_send.send(()).await;
                    break;
                }
            }
        },
        None => loop {
            tokio::select! {
                _ = migrate_interval.tick() => {
                    let _ = keeper.check_migration().await;
                }
                _ = broadcast_logical_interval.tick() => {
                    let _ = keeper.broadcast_logical_clock().await;
                }
            }
        },
    }
    Ok(())
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn trib::Server + Send + Sync>> {
    let frontend_server = FrontendServer::new(bin_storage);
    return Ok(Box::new(frontend_server));
}

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    // let ready_chan = config.ready.unwrap();
    let config_addr = config.addr.to_string();
    let config_addr_str = config_addr.as_str();
    let config_addr_string = config_addr_str.replace("localhost", "127.0.0.1");
    let replaced_config_addr_str = config_addr_string.as_str();
    let server_addr: SocketAddr;
    let parsed_addr = match replaced_config_addr_str.parse::<SocketAddr>() {
        Ok(value) => {
            server_addr = value;
        }
        Err(e) => return Err(Box::new(e)),
    };

    let server = BackendServer::new(server_addr.to_string(), config.storage);

    match config.shutdown {
        Some(mut shut_chan) => {
            let server_status = tonic::transport::Server::builder()
                .add_service(TribStorageServer::new(server))
                .serve_with_shutdown(server_addr, async {
                    if !config.ready.is_none() {
                        let ready_chan = config.ready.unwrap();
                        let sig_sent = ready_chan.clone().send(true);
                    }
                    shut_chan.recv().await;
                })
                .await;
            if server_status.is_err() {}
        }
        None => {
            if !config.ready.is_none() {
                let ready_chan = config.ready.unwrap();
                let sig_sent = ready_chan.clone().send(true);
            }
            let server_status = tonic::transport::Server::builder()
                .add_service(TribStorageServer::new(server))
                .serve(server_addr)
                .await;
            if server_status.is_err() {}
        }
    }
    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    // let mut client = TribStorageClient::connect(String::from(addr)).await?;
    let storage_client = StorageClient::new(addr);
    Ok(Box::new(storage_client))
}
