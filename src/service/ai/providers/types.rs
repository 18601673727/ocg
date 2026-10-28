use super::*;

/// The temporary bootstrap credential boundary. The name is persisted, while
/// this resolver returns the secret only to the request builder.
pub trait CredentialResolver: Send + Sync {
    fn resolve(
        &self,
        credential_ref: Option<&str>,
    ) -> Result<Option<String>, ProviderExecutionError>;
}

/// Receives text deltas while the provider stream is still active. The
/// runtime uses this to publish bounded intermediate outputs through the
/// authoritative database before final settlement.
///
/// It also receives the Token facts of the request as they become known, see
/// [`ProviderObservation`]. Each is awaited, so the producer never runs ahead
/// of what the sink has taken.
pub trait ProviderOutputSink {
    fn append<'a>(
        &'a mut self,
        kind: IntermediateOutputKind,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>>;

    fn sending<'a>(
        &'a mut self,
        _body: &'a Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Takes one Token fact. It cannot fail the request: a sink that cannot
    /// keep a fact owns that gap, and the provider outcome stays what the
    /// provider and transport made it.
    fn observe<'a>(
        &'a mut self,
        _observation: ProviderObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async {})
    }
}

/// A Token fact of one request, with the time the producer learned it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderObservation {
    pub observed_at: Timestamp,
    pub fact: ProviderFact,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderFact {
    /// The request was positively known not to have been sent: it failed
    /// while being built, or before any byte could leave. No Provider saw it.
    NotDelivered,
    /// The request was issued or may have been delivered: a response head
    /// arrived, or the send failed in a way that cannot rule delivery out.
    /// It is announced only once delivery is no longer excluded, so a
    /// request that can still be classified NotDelivered never has it.
    Delivered,
    /// One usage object the provider reported. Its counters are normalized
    /// only where valid, and the object is kept raw.
    Usage {
        usage: NormalizedUsage,
        finality: UsageFinality,
    },
}

/// A local failure to persist a streamed delta. It is a fact about this
/// runtime, not about the provider, so it never becomes a provider failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSinkError {
    pub message: String,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EnvironmentCredentialResolver;

impl CredentialResolver for EnvironmentCredentialResolver {
    fn resolve(
        &self,
        credential_ref: Option<&str>,
    ) -> Result<Option<String>, ProviderExecutionError> {
        let Some(name) = credential_ref else {
            return Ok(None);
        };
        std::env::var(name).map(Some).map_err(|error| match error {
            std::env::VarError::NotPresent => ProviderExecutionError::Build {
                message: format!("credential reference `{name}` is not present"),
            },
            std::env::VarError::NotUnicode(_) => ProviderExecutionError::Build {
                message: format!("credential reference `{name}` is not valid UTF-8"),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProviderExecutionConfig {
    pub transport: crate::db::entities::LlmTransport,
    pub session_id: Option<crate::db::entities::JobId>,
    pub timeout: Duration,
    pub first_delta_timeout: Option<Duration>,
    pub max_output_bytes: usize,
    pub cancellation: CancellationToken,
}

impl ProviderExecutionConfig {
    pub(crate) fn for_profile(&self, frozen: &FrozenExecutionConfig) -> Self {
        let mut config = self.clone();
        config.transport = frozen.llm.provider.transport;
        if frozen
            .llm
            .config
            .declared_capabilities
            .contains(&crate::db::entities::ModelCapability::Reasoning)
        {
            config.first_delta_timeout = Some(Duration::from_secs(600));
        }
        config
    }
}

impl Default for ProviderExecutionConfig {
    fn default() -> Self {
        Self {
            transport: crate::db::entities::LlmTransport::OpenAiChatCompletions,
            session_id: None,
            timeout: DEFAULT_TIMEOUT,
            first_delta_timeout: None,
            max_output_bytes: MAX_OUTPUT_BYTES,
            cancellation: CancellationToken::default(),
        }
    }
}

/// One finished Provider exchange. It may be assistant text, tool calls, or
/// both. Tool calls are complete requests the Provider made; `refused` names
/// those the request did not admit. None has been run, and a result carrying
/// calls is not a finished answer. Admission is not permission to execute.
/// Paths without a tool loop must require [`Self::admitted`].
#[derive(Debug, Clone)]
pub struct ProviderExecutionResult {
    pub response_items: Vec<Value>,
    pub text: String,
    /// The streamed `reasoning_content`, kept apart from the text and never
    /// published as output.
    pub reasoning: String,
    /// Ordered by their wire index, with ids, names and arguments as sent.
    pub tool_calls: Vec<CompletedToolCall>,
    /// Answerable refusals ordered by index, each also present in `tool_calls`.
    pub refused: Vec<InvalidToolCall>,
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
    pub status_code: u16,
    pub request_id: Option<String>,
    pub ttfb: Duration,
    pub finished_in: Duration,
}

/// The facts of a provider response observed before execution ended
/// abnormally. The status is always present: these facts exist only once
/// the response head arrived. A finish reason is the one an accepted event
/// reported; it is a raw observation, and whether it witnesses a normal
/// termination is decided only where it is classified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseFacts {
    pub status_code: u16,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderExecutionError {
    #[error("provider request could not be built: {message}")]
    Build { message: String },
    #[error("provider request failed before delivery: {message}")]
    NotDelivered { message: String },
    /// The transport failed, or the Provider fell silent, after the request
    /// may have been delivered.
    #[error("provider response failed after possible delivery: {message}")]
    Unknown {
        message: String,
        /// What the response had shown before the failure; `None` when no
        /// response head arrived.
        facts: Option<ResponseFacts>,
        /// Whether the response had already finished before the failure: its
        /// finish reason was accepted and no tool call of it is left
        /// unchecked. Its facts then stand apart from the later failure.
        finished: bool,
    },
    #[error("provider response failed after possible delivery: {failure}")]
    StreamFailure {
        failure: Box<ProviderFailure>,
        facts: ResponseFacts,
        finished: bool,
    },
    #[error("provider rejected the request: {failure}")]
    Rejected { failure: ProviderFailure },
    /// The response broke the protocol, or what it sent was refused, such
    /// as a malformed or unadmitted tool call. The refusal is about the
    /// response itself, so its facts never stand apart from it.
    #[error("provider stream was incomplete: {message}")]
    Incomplete {
        message: String,
        facts: ResponseFacts,
    },
    /// Execution was cancelled on this side before any response head
    /// arrived, after the request may have been sent. Delivery cannot be
    /// ruled out, and nothing about the Provider was observed: it is neither
    /// a Provider nor a transport fault.
    #[error("provider execution was interrupted locally: {message}")]
    Interrupted { message: String },
    /// Execution stopped on this side after the provider had answered: the
    /// caller cancelled, the response reached the local output limit, or the
    /// runtime could not persist streamed output. None is a provider or
    /// transport fault, so only the response facts already observed are kept.
    #[error("provider execution stopped locally: {message}")]
    Stopped {
        message: String,
        facts: ResponseFacts,
    },
    #[error("{stream}; additionally, streamed output could not be persisted: {message}")]
    OutputPersistence {
        message: String,
        stream: Box<ProviderExecutionError>,
    },
    /// The Provider response completed, but its last buffered output could not
    /// be persisted. The completed result travels with the failure so its
    /// text, reasoning and tool calls are not lost; the local persistence gap
    /// still leaves the outcome unknown, and no call of it is run.
    #[error("provider response completed but its output could not be persisted: {message}")]
    CompletionUnpersisted {
        message: String,
        result: Box<ProviderExecutionResult>,
    },
}

impl ProviderExecutionResult {
    /// Requires every call to be admitted when no tool loop can answer refusals.
    pub fn admitted(self) -> Result<Self, ProviderExecutionError> {
        if let Some(refusal) = self.refused.iter().min_by_key(|refusal| refusal.index) {
            return Err(incomplete_tool_calls(
                refusal.clone(),
                ResponseFacts {
                    status_code: self.status_code,
                    finish_reason: self.finish_reason,
                    raw_finish_reason: self.raw_finish_reason,
                },
            ));
        }
        Ok(self)
    }

    /// Whether the Provider asked for tools to be run before it can answer.
    #[must_use]
    pub fn requests_tools(&self) -> bool {
        !self.tool_calls.is_empty()
    }
}

impl ProviderExecutionError {
    pub fn failure(&self) -> Option<&ProviderFailure> {
        match self {
            Self::Rejected { failure } => Some(failure),
            Self::OutputPersistence { stream, .. } => stream.failure(),
            _ => None,
        }
    }
}

pub(crate) fn incomplete_tool_calls(
    error: InvalidToolCall,
    facts: ResponseFacts,
) -> ProviderExecutionError {
    ProviderExecutionError::Incomplete {
        message: error.to_string(),
        facts,
    }
}
