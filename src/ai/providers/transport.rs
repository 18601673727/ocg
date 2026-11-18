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
            let permitted = provider_uri(endpoint).is_some_and(|uri| {
                uri.scheme_str() == Some("https")
                    && uri
                        .host()
                        .is_some_and(|target| http::bare_host(target) == http::bare_host(host))
                    && uri.port_u16().unwrap_or(443) == *port
            });
            if !permitted {
                return Err(ProviderExecutionError::Build {
                    message: "Provider endpoint does not match the approved HTTPS destination"
                        .into(),
                });
            }
            Ok(http::approved_client(timeout, host, *port, proxy.address()).await?)
        }
    }
}
