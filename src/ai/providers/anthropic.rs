use ntex::http::header::HeaderValue;

use super::ProviderExecutionError;

pub(crate) const API_VERSION: &str = "2023-06-01";

fn build(message: &str) -> ProviderExecutionError {
    ProviderExecutionError::Build {
        message: message.to_owned(),
    }
}

pub(crate) fn api_key(secret: &str) -> Result<HeaderValue, ProviderExecutionError> {
    let mut value = HeaderValue::try_from(secret)
        .map_err(|_| build("the resolved credential is not a valid HTTP header value"))?;
    value.set_sensitive(true);
    Ok(value)
}
