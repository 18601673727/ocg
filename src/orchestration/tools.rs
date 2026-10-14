//! The tools OCG may offer a Provider, and the dispatch of the calls a
//! Provider requests.
//!
//! Tools come only from OCG. A tool is registered with its exact name, its
//! parameter schema and its handler, and it may be offered and run only
//! while OCG has explicitly authorized it: registering authorizes nothing.
//! A Provider can only request a call. Whether the call runs is decided
//! here, immediately before it would run, against the registry as it is
//! then; nothing the Provider sent can register, redefine or authorize a
//! tool. This authorization is the registry's explicit grant alone; no
//! user, role or persistent policy is consulted, because none exists yet.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use jsonschema::Validator;
use serde_json::Value;

use crate::openai_compatible::admission;
use crate::openai_compatible::{
    CompletedToolCall, FunctionDefinitionWire, InvalidToolCall, ToolCallRefusal, ToolWire,
    is_function_name,
};

/// A tool call's result, or its handler's failure.
pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, ToolFailure>> + 'a>>;

/// Runs the calls of one registered tool.
pub trait ToolHandler: Send + Sync {
    /// Runs one call with arguments its registered schema accepted, exactly
    /// as the Provider sent them. A handler OCG registers must be free of
    /// side effects: no call of it is durably recorded, so one whose outcome
    /// is unknown, or that ran in an execution that never settled, cannot be
    /// reconciled. It should finish promptly and yield while it waits, so
    /// that its time limit and cancellation can stop it.
    fn call(&self, arguments: Value) -> ToolFuture<'_>;
}

/// A failure a handler reports.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ToolFailure {
    pub message: String,
}

impl ToolFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ToolRegistryError {
    #[error("`{0}` is not a valid function name")]
    InvalidName(String),
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
}

struct Registration {
    /// Which registration of the name this is. A name registered again, after
    /// it was removed, is another tool.
    generation: u64,
    description: Option<String>,
    parameters: Value,
    validator: Arc<Validator>,
    handler: Arc<dyn ToolHandler>,
    authorized: bool,
}

#[derive(Default)]
struct Registry {
    tools: BTreeMap<String, Registration>,
    generations: u64,
}

/// OCG's tool registry. Clones share it. It starts empty and grants nothing.
#[derive(Clone, Default)]
pub struct ToolRegistry {
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

    /// Registers a tool under its exact `name`, unauthorized. The parameter
    /// schema is required, and must be one that call admission enforces as
    /// written; a parameterless tool declares an empty-object schema.
    ///
    /// # Errors
    ///
    /// Refuses an invalid name, a name already registered, and a schema
    /// that cannot be enforced.
    pub fn register(
        &self,
        name: &str,
        description: Option<&str>,
        parameters: Value,
        handler: Arc<dyn ToolHandler>,
    ) -> Result<(), ToolRegistryError> {
        if !is_function_name(name) {
            return Err(ToolRegistryError::InvalidName(name.to_owned()));
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
                validator: Arc::new(validator),
                handler,
                authorized: false,
            },
        );
        Ok(())
    }

    /// Removes a tool. A call to it that was offered is refused from now on.
    pub fn unregister(&self, name: &str) -> Result<(), ToolRegistryError> {
        self.lock()
            .tools
            .remove(name)
            .map(drop)
            .ok_or_else(|| ToolRegistryError::Unregistered(name.to_owned()))
    }

    /// Grants the execution of a registered tool.
    pub fn authorize(&self, name: &str) -> Result<(), ToolRegistryError> {
        self.set_authorized(name, true)
    }

    /// Withdraws the grant. A call to the tool not yet run is refused.
    pub fn revoke(&self, name: &str) -> Result<(), ToolRegistryError> {
        self.set_authorized(name, false)
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

    /// Selects tools to offer, each by its exact name. Every one must be
    /// registered and authorized now; the selection keeps which registration
    /// it chose.
    ///
    /// # Errors
    ///
    /// Refuses a name selected twice, unregistered or unauthorized.
    pub fn offer(&self, names: &[&str]) -> Result<ToolOffer, ToolRegistryError> {
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

/// The tools OCG selected for one execution, each pinned to the
/// registration it chose.
#[derive(Clone, Debug)]
pub struct ToolOffer {
    registry: ToolRegistry,
    tools: Vec<(String, u64)>,
}

/// A call resolved for dispatch: the handler to run and its admitted
/// arguments.
pub(crate) struct Dispatch {
    pub(crate) handler: Arc<dyn ToolHandler>,
    pub(crate) arguments: Value,
}

impl ToolOffer {
    /// The definitions to send, read from the registry now. Every selected
    /// tool must still be the registration selected, and still authorized.
    ///
    /// # Errors
    ///
    /// Refuses when any selected tool no longer is.
    pub fn wires(&self) -> Result<Vec<ToolWire>, ToolRegistryError> {
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

    /// Resolves one call against the registry as it is now: the call must
    /// name a tool of this offer, under its exact name, that is still the
    /// registration offered and still authorized, and its arguments must
    /// satisfy that registration's own schema, whatever the request declared.
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
        let (validator, handler) = {
            let registry = self.registry.lock();
            let tool = current(&registry, name, *generation)
                .map_err(|error| refused(format!("tool call {index} cannot run: {error}")))?;
            (tool.validator.clone(), tool.handler.clone())
        };
        let arguments = admission::admit_arguments(index, name, &call.arguments, &validator)?;
        Ok(Dispatch { handler, arguments })
    }
}

/// The registration `generation` of `name`, if it is still registered and
/// authorized.
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
