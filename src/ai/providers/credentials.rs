use std::{future::Future, pin::Pin};

use super::{CredentialResolver, EnvironmentCredentialResolver, ProviderExecutionError};
use crate::db::{DatabaseHandle, DevelopmentHandoff, StoredCredential};

impl CredentialResolver for DatabaseHandle {
    fn resolve<'a>(
        &'a self,
        credential_ref: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ProviderExecutionError>> + Send + 'a>>
    {
        Box::pin(async move {
            let Some(reference) = credential_ref else {
                return Ok(None);
            };
            let reference = reference.trim();
            if reference.is_empty() {
                return Ok(None);
            }
            let stored = self.stored_credential(reference).await.map_err(|error| {
                ProviderExecutionError::Build {
                    message: format!("credential store could not be read: {error}"),
                }
            })?;
            match stored {
                StoredCredential::Decrypted(secret) => Ok(Some(secret.into_inner())),
                StoredCredential::Undecryptable => Err(ProviderExecutionError::Build {
                    message: "stored credential cannot be decrypted; enter the key again"
                        .to_owned(),
                }),
                StoredCredential::Absent => {
                    EnvironmentCredentialResolver.resolve(Some(reference)).await
                }
            }
        })
    }
}

impl CredentialResolver for DevelopmentHandoff {
    fn resolve<'a>(
        &'a self,
        reference: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ProviderExecutionError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.credential(reference)
                .map(|secret| secret.map(|secret| secret.as_str().to_owned()))
                .map_err(|_| ProviderExecutionError::Build {
                    message: "handoff credential reference mismatch".to_owned(),
                })
        })
    }
}
