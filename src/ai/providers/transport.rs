use ntex::client::Client;
use ntex::url::Url;

use super::{DestinationPolicy, ProviderExecutionError};
use crate::network::http;

pub(crate) async fn provider_client_for(
    destination: &DestinationPolicy,
    endpoint: &Url,
) -> Result<Client, ProviderExecutionError> {
    match destination {
        DestinationPolicy::Standard => Ok(http::standard_client(endpoint).await?),
        DestinationPolicy::ApprovedHttps { request_url, proxy } => {
            let host = endpoint.host().filter(|host| !host.is_empty());
            let Some(host) = host.filter(|_| {
                endpoint.scheme_str() == Some("https") && endpoint.as_str() == request_url
            }) else {
                return Err(ProviderExecutionError::Build {
                    message: "Provider request URL does not match the approved HTTPS endpoint"
                        .into(),
                });
            };
            let port = endpoint.port_u16().unwrap_or(443);
            Ok(http::approved_client(host, port, proxy.address()).await?)
        }
    }
}
