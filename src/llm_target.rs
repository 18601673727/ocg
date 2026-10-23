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
    AgentBehavior, AgentConfig, CapabilitySet, ConfigBody, ConfigKind, ConnectionConfig,
    ExecutionOperation, ExecutionPoolConfig, ExecutionPoolId, ExecutorId, LlmParameters,
    LlmTransport, ModelCapability, ModelConfig, NewConfig, NewExecutionPool, ProfileId,
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
    #[error("OCG_PROVIDER_PROTOCOL must be openai_chat_completions or openai_responses")]
    InvalidProtocol,
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

/// An LLM target: one Pool for its transport, one Connection and one Model
/// config of the model, each found by what it is, or registered.
pub struct Target {
    /// The name of the Pool and the Connection when they are registered.
    pub name: String,
    pub protocol: LlmTransport,
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
        let protocol = protocol_from_environment()?;
        let model = variable(MODEL_VARIABLE)?;
        let credential_ref = variable(CREDENTIAL_REF_VARIABLE)?;
        if std::env::var_os(&credential_ref).is_none_or(|credential| credential.is_empty()) {
            return Err(Error::CredentialMissing);
        }
        Ok(Self {
            name: name.to_owned(),
            protocol,
            endpoint,
            model,
            credential_ref,
            capabilities,
        })
    }
}

/// The Pool and Model config of `target`, reusing those an earlier run
/// registered for the same endpoint, credential reference and model. The Pool
/// is the Executor that runs the target's work.
pub async fn targets(
    database: &DatabaseHandle,
    target: &Target,
) -> Result<(ExecutorId, ProfileId, ExecutionPoolId), DatabaseError> {
    let transport = target.protocol;
    let pool = provision_pool(database, &target.name, transport, None).await?;
    let connection = ConnectionConfig {
        protocol: transport,
        endpoint: Some(target.endpoint.clone()),
        credential_ref: Some(target.credential_ref.clone()),
    };
    let configs = database.all_configs(None).await?;
    let connection_id = match configs.iter().find(
        |config| matches!(&config.body, ConfigBody::Connection(found) if *found == connection),
    ) {
        Some(found) => found.id,
        None => {
            database
                .create_config(NewConfig {
                    name: target.name.clone(),
                    body: ConfigBody::Connection(connection),
                })
                .await?
        }
    };
    let model = ModelConfig {
        connection_id,
        model: target.model.clone(),
        context_window: None,
        parameters: LlmParameters::default(),
        declared_capabilities: target.capabilities.clone(),
    };
    let profile = match configs
        .iter()
        .find(|config| matches!(&config.body, ConfigBody::Model(found) if *found == model))
    {
        Some(found) => found.id,
        None => {
            database
                .create_config(NewConfig {
                    name: target.model.clone(),
                    body: ConfigBody::Model(model),
                })
                .await?
        }
    };
    Ok((pool.into(), profile.into(), pool))
}

/// The Agent that has Chat behavior and targets `llm_profile` with no
/// instructions of its own, found by what it is, or registered, so a
/// submission can name an Agent without the operator configuring one first.
pub async fn agent_profile(
    database: &DatabaseHandle,
    llm_profile: ProfileId,
) -> Result<ProfileId, DatabaseError> {
    let agent = AgentConfig {
        dispatch_to: Vec::new(),
        behavior: AgentBehavior::Chat,
        models: vec![llm_profile.into()],
        default_model: Some(llm_profile.into()),
        ..AgentConfig::default()
    };
    let existing = database
        .all_configs(Some(ConfigKind::Agent))
        .await?
        .into_iter()
        .find(|config| {
            config.name == CONTROL_PLANE && config.body == ConfigBody::Agent(agent.clone())
        });
    if let Some(config) = existing {
        return Ok(config.id.into());
    }
    database
        .create_config(NewConfig {
            name: CONTROL_PLANE.to_owned(),
            body: ConfigBody::Agent(agent),
        })
        .await
        .map(Into::into)
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
                && pool
                    .config
                    .capabilities
                    .supports(ExecutionOperation::LlmRequest, Some(protocol))
                && concurrency.is_none_or(|limit| pool.config.concurrency_limit == limit)
            {
                return Ok(pool.id);
            }
        }
        if page.len() < 1_000 {
            break;
        }
        before = page.last().map(|pool| pool.id);
    }
    database
        .create_execution_pool(NewExecutionPool {
            name: name.to_owned(),
            config: pool_config(protocol, concurrency),
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

pub(crate) fn protocol_from_environment() -> Result<LlmTransport, Error> {
    match std::env::var("OCG_PROVIDER_PROTOCOL") {
        Ok(value) => LlmTransport::parse(value.trim()).ok_or(Error::InvalidProtocol),
        Err(std::env::VarError::NotPresent) => Ok(LlmTransport::OpenAiChatCompletions),
        Err(std::env::VarError::NotUnicode(_)) => Err(Error::InvalidProtocol),
    }
}
