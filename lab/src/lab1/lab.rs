use tribbler::{config::BackConfig, err::TribResult, storage::Storage};

use crate::lab3;

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    lab3::serve_back(config).await
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    lab3::new_client(addr).await
}
