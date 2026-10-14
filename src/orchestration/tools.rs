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
//!
//! Registration is a trusted-code boundary. Everything here is visible only
//! inside the crate, so no consumer of the library can register, authorize
//! or offer a handler, and a plan built outside the crate offers no tools.
//! Visibility restricts who may register a handler; it cannot prove what a
//! handler OCG registers does, so its declared version and effect, and the
//! fitness of its arguments and results to be recorded, stay the duty of the
//! OCG code that implements and registers it. Every call is recorded
//! durably before its handler runs, and its outcome after, see
//! [`crate::persistence::ToolEvidenceRecord`]. OCG registers no handler by
//! default; its handlers, `workspace_read_file` and `workspace_write_file`,
//! are registered only for an execution an operator enabled them for, see
//! [`crate::orchestration::workspace_read`] and
//! [`crate::orchestration::workspace_write`].
//!
//! A call is authorized for dispatch in [`ToolOffer::resolve`], under the
//! registry's lock, which [`ToolRegistry::revoke`], `unregister` and
//! `register` take too, so each authorization is ordered before or after
//! each of them. The runtime resolves a call before its durable admission
//! and again immediately before its handler runs; an authorization ordered
//! after a revocation is refused, and a handler authorized before it runs to
//! its end, as the lock is released before the handler is awaited. A registration is a new generation and starts
//! unauthorized, so it never inherits an earlier registration's grant.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use jsonschema::Validator;
use serde_json::Value;

use crate::openai_compatible::admission;
use crate::openai_compatible::{
    CompletedToolCall, FunctionDefinitionWire, InvalidToolCall, MAX_TOOL_DEFINITION_BYTES,
    ToolCallRefusal, ToolWire, is_function_name,
};
use crate::persistence::{ToolEffect, is_handler_version};

/// A tool call's result, or its handler's failure.
pub(crate) type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, ToolFailure>> + 'a>>;

/// Runs the calls of one registered tool. Only OCG's own code implements and
/// registers it, and only when its arguments and results may be recorded:
/// every call is recorded with its arguments, and every result with it, so
/// neither may carry a credential or other secret.
pub(crate) trait ToolHandler: Send + Sync {
    /// Identifies what the handler does, to be recorded with each of its
    /// calls, see [`is_handler_version`]. A change of behavior is a new
    /// version. It is read once, when the handler is registered.
    fn version(&self) -> &str;

    /// What a call may change outside OCG. It is read once, when the
    /// handler is registered.
    fn effect(&self) -> ToolEffect;

    /// Runs one call with arguments its registered schema accepted, exactly
    /// as the Provider sent them. It should finish promptly and yield while
    /// it waits, so that its time limit and cancellation can stop it; a
    /// handler stopped either way leaves its outcome uncertain.
    fn call(&self, arguments: Value) -> ToolFuture<'_>;
}

/// What a failing handler knows of its effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureCertainty {
    /// The handler has evidence that the call's effect did not happen.
    NotApplied,
    /// The effect may have happened, wholly or in part.
    Uncertain,
}

/// A failure a handler reports. Its message is recorded and may reach the
/// Job's result, so it must not repeat the call's arguments.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ToolFailure {
    pub(crate) message: String,
    pub(crate) certainty: FailureCertainty,
}

impl ToolFailure {
    /// A failure the handler has evidence left no effect. Only a handler
    /// that can tell may report it.
    pub(crate) fn not_applied(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            certainty: FailureCertainty::NotApplied,
        }
    }

    /// A failure that may have left an effect: any error, interruption or
    /// partial operation the handler cannot rule out an effect for.
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
}

struct Registration {
    /// Which registration of the name this is. A name registered again, after
    /// it was removed, is another tool.
    generation: u64,
    description: Option<String>,
    parameters: Value,
    /// `parameters` serialized as it is offered and recorded.
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

/// OCG's tool registry. Clones share it. It starts empty and grants nothing.
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

    /// Registers a tool under its exact `name`, unauthorized, with the
    /// version and effect its handler declares. The parameter schema is
    /// required, and must be one that call admission enforces as written; a
    /// parameterless tool declares an empty-object schema.
    ///
    /// # Errors
    ///
    /// Refuses an invalid name, an invalid handler version, a name already
    /// registered, and a schema that cannot be enforced or is larger than a
    /// tool definition may be.
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

    /// Removes a tool. A call to it that was offered is refused from now on.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "OCG withdraws no tool outside its tests yet")
    )]
    pub(crate) fn unregister(&self, name: &str) -> Result<(), ToolRegistryError> {
        self.lock()
            .tools
            .remove(name)
            .map(drop)
            .ok_or_else(|| ToolRegistryError::Unregistered(name.to_owned()))
    }

    /// Grants the execution of a registered tool.
    pub(crate) fn authorize(&self, name: &str) -> Result<(), ToolRegistryError> {
        self.set_authorized(name, true)
    }

    /// Withdraws the grant. A call to the tool not yet run is refused.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "OCG withdraws no tool outside its tests yet")
    )]
    pub(crate) fn revoke(&self, name: &str) -> Result<(), ToolRegistryError> {
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

/// The tools OCG selected for one execution, each pinned to the
/// registration it chose.
#[derive(Clone, Debug)]
pub(crate) struct ToolOffer {
    registry: ToolRegistry,
    tools: Vec<(String, u64)>,
}

/// A call resolved for dispatch: the handler to run, its admitted
/// arguments, and the registration's identity to record with the call.
pub(crate) struct Dispatch {
    pub(crate) handler: Arc<dyn ToolHandler>,
    pub(crate) arguments: Value,
    pub(crate) version: String,
    pub(crate) effect: ToolEffect,
    /// The parameter schema the arguments were checked against, serialized.
    pub(crate) schema: String,
}

impl ToolOffer {
    /// The definitions to send, read from the registry now. Every selected
    /// tool must still be the registration selected, and still authorized.
    ///
    /// # Errors
    ///
    /// Refuses when any selected tool no longer is.
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
        let (validator, handler, version, effect, schema) = {
            let registry = self.registry.lock();
            let tool = current(&registry, name, *generation)
                .map_err(|error| refused(format!("tool call {index} cannot run: {error}")))?;
            (
                tool.validator.clone(),
                tool.handler.clone(),
                tool.version.clone(),
                tool.effect,
                tool.schema.clone(),
            )
        };
        let arguments = admission::admit_arguments(index, name, &call.arguments, &validator)?;
        Ok(Dispatch {
            handler,
            arguments,
            version,
            effect,
            schema,
        })
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
