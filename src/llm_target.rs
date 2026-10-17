//! The LLM Executor, Provider and Profile an operator names by the
//! environment, registered once and reused, and `ocg register-llm`, which
//! registers them, prints their identities and exits. Nothing is sent to the
//! Provider and no Job is run. The credential is named only by reference:
//! it must be set where OCG runs, and is never stored, printed or logged.
//! This module belongs to the executable, not to the library.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseError, DatabaseHandle};
use ocg::host::{self, HostError};
use ocg::persistence::{
    AgentConfig, CapabilitySet, ExecutionOperation, ExecutionPoolConfig, ExecutionPoolId,
    ExecutorId, ExecutorType, LlmConfig, LlmParameters, LlmTransport, ModelCapability,
    NewExecutionPool, NewExecutor, NewLlmProvider, NewProfile, NewProfileConfig, ProfileConfig,
    ProfileId,
};

/// The command that registers the Control Plane's LLM target.
pub const COMMAND: &str = "register-llm";
/// The Provider's OpenAI-compatible endpoint, an `https://` URL.
pub const ENDPOINT_VARIABLE: &str = "OCG_PROVIDER_ENDPOINT";
pub const MODEL_VARIABLE: &str = "OCG_PROVIDER_MODEL";
/// The name of the environment variable that holds the Provider's key.
pub const CREDENTIAL_REF_VARIABLE: &str = "OCG_PROVIDER_CREDENTIAL_REF";
/// The name of the Executor and Provider `register-llm` registers.
const CONTROL_PLANE: &str = "control-plane";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0} must be set")]
    NotConfigured(&'static str),
    #[error("{ENDPOINT_VARIABLE} must be an https:// URL")]
    NotHttps,
    #[error("the variable named by {CREDENTIAL_REF_VARIABLE} is not set")]
    CredentialMissing,
    #[error("usage: ocg {COMMAND}")]
    Usage,
    #[error("database: {0}")]
    Database(#[from] DatabaseError),
    #[error("the host stopped before the target was registered")]
    NotRun,
    #[error(transparent)]
    Host(#[from] HostError),
}

/// An LLM target: one Llm Executor for its transport, one Provider and one
/// LLM Profile of the model, each found by what it is, or registered.
pub struct Target {
    /// The name of the Executor and the Provider when they are registered.
    pub name: String,
    pub endpoint: String,
    pub model: String,
    pub credential_ref: String,
    pub capabilities: BTreeSet<ModelCapability>,
}

impl Target {
    /// The endpoint, model and credential reference in the environment; the
    /// credential itself is only checked to exist.
    pub fn from_environment(
        name: &str,
        capabilities: BTreeSet<ModelCapability>,
    ) -> Result<Self, Error> {
        let variable = |name: &'static str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .ok_or(Error::NotConfigured(name))
        };
        let endpoint = variable(ENDPOINT_VARIABLE)?;
        if !endpoint.starts_with("https://") {
            return Err(Error::NotHttps);
        }
        let model = variable(MODEL_VARIABLE)?;
        let credential_ref = variable(CREDENTIAL_REF_VARIABLE)?;
        if std::env::var_os(&credential_ref).is_none_or(|credential| credential.is_empty()) {
            return Err(Error::CredentialMissing);
        }
        Ok(Self {
            name: name.to_owned(),
            endpoint,
            model,
            credential_ref,
            capabilities,
        })
    }
}

/// The Executor and LLM Profile of `target`, reusing those an earlier run
/// registered under the same name, for the same endpoint, credential
/// reference and model.
pub async fn targets(
    database: &DatabaseHandle,
    target: &Target,
) -> Result<(ExecutorId, ProfileId, ExecutionPoolId), DatabaseError> {
    let transport = LlmTransport::OpenAiChatCompletions;
    let pool = provision_pool(database, &target.name, transport, None).await?;
    let existing = database
        .executors(None, 1_000)
        .await?
        .into_iter()
        .find(|executor| {
            executor.name == target.name
                && executor.executor_type == ExecutorType::Llm
                && executor.llm_transports.contains(&transport)
        });
    let executor = match existing {
        Some(executor) => executor.id,
        None => {
            database
                .create_executor(NewExecutor {
                    name: target.name.clone(),
                    executor_type: ExecutorType::Llm,
                    concurrency_limit: NonZeroU32::MIN,
                    llm_transports: BTreeSet::from([transport]),
                })
                .await?
        }
    };
    let existing = database
        .llm_providers(None, 1_000)
        .await?
        .into_iter()
        .find(|provider| {
            provider.transport == transport
                && provider.endpoint.as_deref() == Some(target.endpoint.as_str())
                && provider.credential_ref.as_deref() == Some(target.credential_ref.as_str())
        });
    let provider = match existing {
        Some(provider) => provider.id,
        None => {
            database
                .create_llm_provider(NewLlmProvider {
                    name: target.name.clone(),
                    transport,
                    endpoint: Some(target.endpoint.clone()),
                    credential_ref: Some(target.credential_ref.clone()),
                })
                .await?
        }
    };
    let llm = LlmConfig {
        model: target.model.clone(),
        parameters: LlmParameters::default(),
        declared_capabilities: target.capabilities.clone(),
    };
    for profile in database.provider_profiles(provider).await? {
        if let Some(record) = database.profile(profile).await?
            && record.config == ProfileConfig::Llm(llm.clone())
        {
            return Ok((executor, profile, pool));
        }
    }
    let profile = database
        .create_profile(NewProfile {
            name: target.model.clone(),
            config: NewProfileConfig::Llm {
                provider,
                config: llm,
            },
        })
        .await?;
    Ok((executor, profile, pool))
}

/// The Agent Profile that targets `llm_profile` with no instructions of its
/// own, found by what it is, or registered, so a submission can name an
/// Agent Profile without the operator configuring one first.
pub async fn agent_profile(
    database: &DatabaseHandle,
    llm_profile: ProfileId,
) -> Result<ProfileId, DatabaseError> {
    let existing = database
        .profiles(None, 1_000)
        .await?
        .into_iter()
        .find(|record| {
            record.name == CONTROL_PLANE
                && record.config == ProfileConfig::Agent(AgentConfig::default())
                && record.llm_targets == [llm_profile]
        });
    if let Some(record) = existing {
        return Ok(record.id);
    }
    database
        .create_profile(NewProfile {
            name: CONTROL_PLANE.to_owned(),
            config: NewProfileConfig::Agent {
                config: AgentConfig::default(),
                llm_targets: vec![llm_profile],
            },
        })
        .await
}

/// `ocg register-llm`: registers, or finds, the Control Plane's chat target
/// named by the environment, and prints the identities a submission names,
/// as one JSON object on standard output.
pub fn register(data_directory: &DataDirectory, target: Target) -> Result<(), Error> {
    let registered = std::rc::Rc::new(std::cell::RefCell::new(None));
    let slot = registered.clone();
    host::run_hosted(data_directory, false, move |context| async move {
        let database = context.database();
        let outcome = match targets(database, &target).await {
            Ok((executor, llm_profile, pool)) => agent_profile(database, llm_profile)
                .await
                .map(|agent_profile| (executor, llm_profile, agent_profile, pool)),
            Err(error) => Err(error),
        };
        *slot.borrow_mut() = Some(outcome);
    })?;
    let (executor, llm_profile, agent_profile, pool) =
        registered.borrow_mut().take().ok_or(Error::NotRun)??;
    println!(
        "{}",
        serde_json::json!({
            "executor": executor.to_string(),
            "pool": pool.to_string(),
            "llm_profile": llm_profile.to_string(),
            "agent_profile": agent_profile.to_string(),
        })
    );
    Ok(())
}

/// The target `register-llm` registers: a chat model, run by the
/// `control-plane` Executor.
pub fn control_plane_target() -> Result<Target, Error> {
    Target::from_environment(CONTROL_PLANE, BTreeSet::from([ModelCapability::Chat]))
}

pub async fn provision_pool(
    database: &DatabaseHandle,
    name: &str,
    protocol: LlmTransport,
    concurrency: Option<NonZeroU32>,
) -> Result<ExecutionPoolId, DatabaseError> {
    let mut before = None;
    loop {
        let page = database.execution_pools(before, 1_000).await?;
        for pool in &page {
            if pool.enabled
                && let Some(profile) = database.profile(pool.profile).await?
                && let ProfileConfig::ExecutionPool(config) = profile.config
                && config
                    .capabilities
                    .supports(ExecutionOperation::LlmRequest, Some(protocol))
                && concurrency.is_none_or(|limit| config.concurrency_limit == limit)
            {
                return Ok(pool.id);
            }
        }
        if page.len() < 1_000 {
            break;
        }
        before = page.last().map(|pool| pool.id);
    }
    let profile = database
        .create_profile(NewProfile {
            name: name.to_owned(),
            config: NewProfileConfig::ExecutionPool(pool_config(protocol, concurrency)),
        })
        .await?;
    database
        .create_execution_pool(NewExecutionPool {
            name: name.to_owned(),
            profile,
            enabled: true,
        })
        .await
}

pub fn pool_config(protocol: LlmTransport, concurrency: Option<NonZeroU32>) -> ExecutionPoolConfig {
    ExecutionPoolConfig {
        concurrency_limit: concurrency.unwrap_or(NonZeroU32::new(16).expect("nonzero default")),
        capabilities: CapabilitySet {
            operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
            llm_protocols: BTreeSet::from([protocol]),
        },
    }
}
