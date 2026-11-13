use std::collections::BTreeSet;

use crate::db::entities::{
    AgentCapability, AgentConfig, Config, ConfigBody, ConfigKind, ConnectionConfig,
    ExecutionOperation, ExecutionPoolId, ExecutorId, LlmParameters, LlmTransport, ModelCapability,
    ModelConfig, ProfileId,
};
use crate::db::{DatabaseError, DatabaseHandle};
use crate::system::data_directory::DataDirectory;
use crate::system::runtime::{self, HostError};

pub const COMMAND: &str = "register-llm";
pub const ENDPOINT_VARIABLE: &str = "OCG_PROVIDER_ENDPOINT";
pub const MODEL_VARIABLE: &str = "OCG_PROVIDER_MODEL";
pub const CREDENTIAL_REF_VARIABLE: &str = "OCG_PROVIDER_CREDENTIAL_REF";
const CONTROL_PLANE: &str = "control-plane";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0} must be set")]
    NotConfigured(&'static str),
    #[error("{ENDPOINT_VARIABLE} must be an https:// URL")]
    NotHttps,
    #[error(
        "OCG_PROVIDER_PROTOCOL must be openai_chat_completions, openai_responses or anthropic_messages"
    )]
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

pub struct Target {
    pub name: String,
    pub protocol: LlmTransport,
    pub endpoint: String,
    pub model: String,
    pub credential_ref: String,
    pub capabilities: BTreeSet<ModelCapability>,
}

impl Target {
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

pub async fn targets(
    database: &DatabaseHandle,
    target: &Target,
) -> Result<(ExecutorId, ProfileId, ExecutionPoolId), DatabaseError> {
    let transport = target.protocol;
    let (executor, pool) = native_executor(database, transport).await?;
    let connection = ConnectionConfig {
        protocol: transport,
        endpoint: Some(target.endpoint.clone()),
        credential_ref: Some(target.credential_ref.clone()),
        headers: Vec::new(),
    };
    let connection_id = match database
        .find_config(
            ConfigKind::Connection,
            |config| matches!(&config.body, ConfigBody::Connection(found) if *found == connection),
        )
        .await?
    {
        Some(found) => found.id,
        None => {
            database
                .create_config(Config {
                    name: target.name.clone(),
                    body: ConfigBody::Connection(connection),
                })
                .await?
        }
    };
    let model = ModelConfig {
        reasoning_efforts: None,
        connection_id,
        model: target.model.clone(),
        context_window: None,
        parameters: LlmParameters::default(),
        declared_capabilities: target.capabilities.clone(),
    };
    let profile = match database
        .find_config(
            ConfigKind::Model,
            |config| matches!(&config.body, ConfigBody::Model(found) if *found == model),
        )
        .await?
    {
        Some(found) => found.id,
        None => {
            database
                .create_config(Config {
                    name: target.model.clone(),
                    body: ConfigBody::Model(model),
                })
                .await?
        }
    };
    Ok((executor, profile.into(), pool))
}

async fn native_executor(
    database: &DatabaseHandle,
    protocol: LlmTransport,
) -> Result<(ExecutorId, ExecutionPoolId), DatabaseError> {
    use crate::db::redb::{NativeResource, NativeResourceKind};
    let mut before = None;
    loop {
        let page = database
            .native_resources(before, 100, NativeResourceKind::Executor)
            .await?;
        for resource in page.records {
            let NativeResource::Executor(executor) = resource else {
                continue;
            };
            if !executor.enabled
                || !executor
                    .capabilities
                    .contains(&(ExecutionOperation::LlmRequest, Some(protocol)))
            {
                continue;
            }
            let mut pool_before = None;
            loop {
                let pools = database
                    .native_resources(pool_before, 100, NativeResourceKind::Pool)
                    .await?;
                for pool in pools.records {
                    if let NativeResource::Pool(pool) = pool
                        && pool.id == executor.pool
                        && pool.enabled
                    {
                        return Ok((executor.id, pool.id));
                    }
                }
                match pools.next {
                    Some(next) => pool_before = Some(next),
                    None => break,
                }
            }
        }
        match page.next {
            Some(next) => before = Some(next),
            None => break,
        }
    }
    Err(DatabaseError::Rejected(
        crate::db::entities::Violation::InvalidValue(
            "executor",
            "no enabled native Executor supports this operation and protocol",
        ),
    ))
}

pub async fn agent_profile(
    database: &DatabaseHandle,
    llm_profile: ProfileId,
) -> Result<ProfileId, DatabaseError> {
    let agent = AgentConfig {
        capabilities: BTreeSet::from([AgentCapability::Chat]),
        models: vec![llm_profile.into()],
        ..AgentConfig::default()
    };
    let existing = database
        .find_config(ConfigKind::Agent, |config| {
            config.name == CONTROL_PLANE
                && matches!(&config.body, ConfigBody::Agent(found) if *found == agent)
        })
        .await?;
    if let Some(config) = existing {
        return Ok(config.id.into());
    }
    database
        .create_config(Config {
            name: CONTROL_PLANE.to_owned(),
            body: ConfigBody::Agent(agent),
        })
        .await
        .map(Into::into)
}

pub fn register(data_directory: &DataDirectory, target: Target) -> Result<(), Error> {
    let registered = std::rc::Rc::new(std::cell::RefCell::new(None));
    let slot = registered.clone();
    runtime::run_hosted(data_directory, false, move |context| async move {
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

pub fn control_plane_target() -> Result<Target, Error> {
    Target::from_environment(CONTROL_PLANE, BTreeSet::from([ModelCapability::Chat]))
}

pub(crate) fn protocol_from_environment() -> Result<LlmTransport, Error> {
    match std::env::var("OCG_PROVIDER_PROTOCOL") {
        Ok(value) => LlmTransport::parse(value.trim()).ok_or(Error::InvalidProtocol),
        Err(std::env::VarError::NotPresent) => Ok(LlmTransport::OpenAiChatCompletions),
        Err(std::env::VarError::NotUnicode(_)) => Err(Error::InvalidProtocol),
    }
}
