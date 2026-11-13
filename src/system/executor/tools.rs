use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use jsonschema::Validator;
use serde_json::Value;

use crate::db::entities::{JobId, ToolCallRef, ToolEffect, is_handler_version};
use crate::service::ai::providers::openai_compatible::admission;
use crate::service::ai::providers::openai_compatible::{
    CompletedToolCall, FunctionDefinitionWire, InvalidToolCall, MAX_TOOL_DEFINITION_BYTES,
    ToolCallRefusal, ToolWire, is_function_name,
};

pub(crate) type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, ToolFailure>> + 'a>>;

#[derive(Clone, Default)]
pub(crate) struct ToolStart {
    at: Arc<OnceLock<Instant>>,
    ready: crate::service::ai::providers::CancellationToken,
    starting: Arc<Mutex<Option<super::blocking::StartingCall>>>,
}

impl ToolStart {
    pub(crate) fn with_starting(starting: Option<super::blocking::StartingCall>) -> Self {
        Self {
            starting: Arc::new(Mutex::new(starting)),
            ..Self::default()
        }
    }

    pub(crate) fn take_starting(&self) -> Option<super::blocking::StartingCall> {
        self.starting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(crate) fn mark_started(&self) {
        if self.at.set(Instant::now()).is_ok() {
            self.ready.cancel();
        }
    }

    pub(crate) fn has_started(&self) -> bool {
        self.at.get().is_some()
    }

    pub(crate) async fn wait(&self) {
        self.ready.cancelled().await;
    }

    pub(crate) fn remaining(&self, limit: Duration) -> Duration {
        self.at
            .get()
            .map_or(limit, |at| limit.saturating_sub(at.elapsed()))
    }
}

pub(crate) trait ToolHandler: Send + Sync {
    fn version(&self) -> &str;

    fn effect(&self) -> ToolEffect;

    fn spawns(&self) -> bool {
        false
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_>;

    fn call_for(&self, _call: ToolCallRef, arguments: Value) -> ToolFuture<'_> {
        self.call(arguments)
    }

    fn call_with_start(
        &self,
        call: ToolCallRef,
        arguments: Value,
        start: ToolStart,
    ) -> ToolFuture<'_> {
        start.mark_started();
        self.call_for(call, arguments)
    }

    fn stop(&self, _job: JobId) -> Pin<Box<dyn Future<Output = Result<(), ToolFailure>> + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureCertainty {
    NotApplied,
    Uncertain,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ToolFailure {
    pub(crate) message: String,
    pub(crate) certainty: FailureCertainty,
}

impl ToolFailure {
    pub(crate) fn not_applied(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            certainty: FailureCertainty::NotApplied,
        }
    }

    pub(crate) fn uncertain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            certainty: FailureCertainty::Uncertain,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ToolRegistryError {
    #[error("`{0}` is not a valid function name")]
    InvalidName(String),
    #[error("tool `{0}` declares an invalid handler version")]
    InvalidVersion(String),
    #[error("a tool named `{0}` is already registered")]
    Duplicate(String),
    #[error("tool `{0}` has a parameter schema that cannot be enforced: {1}")]
    UnsupportedSchema(String, String),
    #[error("no tool named `{0}` is registered")]
    Unregistered(String),
    #[error("tool `{0}` is not authorized")]
    Unauthorized(String),
    #[error("tool `{0}` is selected more than once")]
    SelectedTwice(String),
    #[error("tool `{0}` was registered again after it was offered")]
    Changed(String),
    #[error("tool `{0}` cannot be granted: {1}")]
    Disabled(String, String),
}

struct Registration {
    generation: u64,
    description: Option<String>,
    parameters: Value,
    schema: String,
    version: String,
    effect: ToolEffect,
    validator: Arc<Validator>,
    handler: Arc<dyn ToolHandler>,
    authorized: bool,
}

#[derive(Default)]
struct Registry {
    tools: BTreeMap<String, Registration>,
    generations: u64,
}

#[derive(Clone, Default)]
pub(crate) struct ToolRegistry {
    inner: Arc<Mutex<Registry>>,
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registry = self.lock();
        formatter
            .debug_struct("ToolRegistry")
            .field("tools", &registry.tools.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ToolRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn register(
        &self,
        name: &str,
        description: Option<&str>,
        parameters: Value,
        handler: Arc<dyn ToolHandler>,
    ) -> Result<(), ToolRegistryError> {
        if !is_function_name(name) {
            return Err(ToolRegistryError::InvalidName(name.to_owned()));
        }
        let version = handler.version().to_owned();
        if !is_handler_version(&version) {
            return Err(ToolRegistryError::InvalidVersion(name.to_owned()));
        }
        let effect = handler.effect();
        let schema = parameters.to_string();
        if schema.len() > MAX_TOOL_DEFINITION_BYTES {
            return Err(ToolRegistryError::UnsupportedSchema(
                name.to_owned(),
                format!("is larger than {MAX_TOOL_DEFINITION_BYTES} bytes"),
            ));
        }
        let validator = admission::compile_parameters(&parameters)
            .map_err(|problem| ToolRegistryError::UnsupportedSchema(name.to_owned(), problem))?;
        let mut registry = self.lock();
        if registry.tools.contains_key(name) {
            return Err(ToolRegistryError::Duplicate(name.to_owned()));
        }
        registry.generations += 1;
        let generation = registry.generations;
        registry.tools.insert(
            name.to_owned(),
            Registration {
                generation,
                description: description.map(str::to_owned),
                parameters,
                schema,
                version,
                effect,
                validator: Arc::new(validator),
                handler,
                authorized: false,
            },
        );
        Ok(())
    }

    pub(crate) fn authorize(&self, name: &str) -> Result<(), ToolRegistryError> {
        let control = crate::system::execution_controls::ExecutionControl::ToolsAndChildJobs;
        if !control.is_enabled() {
            return Err(ToolRegistryError::Disabled(
                name.to_owned(),
                control.refusal(),
            ));
        }
        self.set_authorized(name, true)
    }

    fn set_authorized(&self, name: &str, authorized: bool) -> Result<(), ToolRegistryError> {
        let mut registry = self.lock();
        let tool = registry
            .tools
            .get_mut(name)
            .ok_or_else(|| ToolRegistryError::Unregistered(name.to_owned()))?;
        tool.authorized = authorized;
        Ok(())
    }

    pub(crate) fn offer(&self, names: &[&str]) -> Result<ToolOffer, ToolRegistryError> {
        let registry = self.lock();
        let mut tools = Vec::with_capacity(names.len());
        for name in names {
            if tools.iter().any(|(chosen, _)| chosen == name) {
                return Err(ToolRegistryError::SelectedTwice((*name).to_owned()));
            }
            let tool = registry
                .tools
                .get(*name)
                .ok_or_else(|| ToolRegistryError::Unregistered((*name).to_owned()))?;
            if !tool.authorized {
                return Err(ToolRegistryError::Unauthorized((*name).to_owned()));
            }
            tools.push(((*name).to_owned(), tool.generation));
        }
        Ok(ToolOffer {
            registry: self.clone(),
            tools,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ToolOffer {
    registry: ToolRegistry,
    tools: Vec<(String, u64)>,
}

pub(crate) struct Dispatch {
    pub(crate) handler: Arc<dyn ToolHandler>,
    pub(crate) arguments: Value,
    pub(crate) version: String,
    pub(crate) effect: ToolEffect,
    pub(crate) spawns: bool,
    pub(crate) schema: String,
}

impl ToolOffer {
    pub(crate) fn wires(&self) -> Result<Vec<ToolWire>, ToolRegistryError> {
        let registry = self.registry.lock();
        self.tools
            .iter()
            .map(|(name, generation)| {
                let tool = current(&registry, name, *generation)?;
                Ok(ToolWire {
                    tool_type: "function".to_owned(),
                    function: FunctionDefinitionWire {
                        name: name.clone(),
                        description: tool.description.clone(),
                        parameters: Some(tool.parameters.clone()),
                        strict: None,
                    },
                })
            })
            .collect()
    }

    pub(crate) fn resolve(&self, call: &CompletedToolCall) -> Result<Dispatch, InvalidToolCall> {
        let index = call.index;
        let refused =
            |message: String| InvalidToolCall::at(ToolCallRefusal::Unauthorized, index, message);
        let Some((name, generation)) = self.tools.iter().find(|(name, _)| *name == call.name)
        else {
            return Err(refused(format!(
                "tool call {index} names `{}`, which OCG did not offer",
                call.name
            )));
        };
        let (validator, handler, version, effect, spawns, schema) = {
            let registry = self.registry.lock();
            let tool = current(&registry, name, *generation)
                .map_err(|error| refused(format!("tool call {index} cannot run: {error}")))?;
            (
                tool.validator.clone(),
                tool.handler.clone(),
                tool.version.clone(),
                tool.effect,
                tool.handler.spawns(),
                tool.schema.clone(),
            )
        };
        let arguments = admission::admit_arguments(index, name, &call.arguments, &validator)?;
        Ok(Dispatch {
            handler,
            arguments,
            version,
            effect,
            spawns,
            schema,
        })
    }
}

fn current<'a>(
    registry: &'a Registry,
    name: &str,
    generation: u64,
) -> Result<&'a Registration, ToolRegistryError> {
    let tool = registry
        .tools
        .get(name)
        .ok_or_else(|| ToolRegistryError::Unregistered(name.to_owned()))?;
    if tool.generation != generation {
        return Err(ToolRegistryError::Changed(name.to_owned()));
    }
    if !tool.authorized {
        return Err(ToolRegistryError::Unauthorized(name.to_owned()));
    }
    Ok(tool)
}
