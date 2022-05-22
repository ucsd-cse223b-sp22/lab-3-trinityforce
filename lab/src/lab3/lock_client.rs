use tonic::transport::Channel;
use tribbler::rpc::trib_storage_client::TribStorageClient;
#[derive(Debug, Default)]
pub struct LockClient {
    pub addrs: Vec<String>,
}

// use tonic::transport::Endpoint;
use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::storage;

impl LockClient {
    pub fn new(addrs: Vec<String>) -> Self {
        Self {
            addrs: addrs.clone(),
        }
    }

    async fn connect(
        &self,
        addr: String,
    ) -> Result<TribStorageClient<Channel>, tonic::transport::Error> {
        let formatted_addr = format!("http://{}", addr);
        let client = TribStorageClient::connect(formatted_addr).await?;
        return Ok(client);
    }

    async fn get_lock(&self, read_keys: Vec<String>, write_keys: Vec<String>) -> TribResult<()> {
        Ok(())
    }
}
