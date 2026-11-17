use std::time::Duration;

use ntex::client::Client;

use super::{DestinationPolicy, ProviderExecutionError, provider_uri};
use crate::network::http;

pub(crate) async fn provider_client_for(
    timeout: Duration,
    destination: &DestinationPolicy,
    endpoint: &str,
) -> Result<Client, ProviderExecutionError> {
    match destination {
        DestinationPolicy::Standard => {
            let uri = provider_uri(endpoint).ok_or_else(|| ProviderExecutionError::Build {
                message: "invalid configured Provider endpoint".to_owned(),
            })?;
            Ok(http::standard_client(timeout, &uri).await?)
        }
        DestinationPolicy::ApprovedHttps { host, port, proxy } => {
            Ok(http::approved_client(timeout, host, *port, proxy.address()).await?)
        }
    }
}
