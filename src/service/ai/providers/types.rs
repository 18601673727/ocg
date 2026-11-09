use super::*;

/// The temporary bootstrap credential boundary. The name is persisted, while
/// this resolver returns the secret only to the request builder.
pub trait CredentialResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        credential_ref: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ProviderExecutionError>> + Send + 'a>>;
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
    fn resolve<'a>(
        &'a self,
        credential_ref: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ProviderExecutionError>> + Send + 'a>>
    {
        Box::pin(async move {
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
        })
    }
}

/// Where a Provider request may be connected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DestinationPolicy {
    /// The ordinary connector: the destination is whatever the frozen
    /// endpoint names. Every path that reaches it has already refused an
    /// endpoint that is not a literal loopback, see
    /// [`crate::system::http::direct_diagnostic_loopback`].
    #[default]
    Standard,
    /// One operator-approved HTTPS destination, for one manually invoked
    /// external exchange under `authority`. The connector refuses any other
    /// host or port, any address that is not public, any scheme but https,
    /// and any certificate that does not verify for exactly this host.
    ApprovedHttps {
        host: String,
        port: u16,
        route: ApprovedRoute,
        authority: ExternalAuthority,
    },
}

/// Which manual authority approved an [`DestinationPolicy::ApprovedHttps`]
/// destination. Each has its own execution control and enables nothing the
/// other governs: enabling one never lets the other's destination connect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalAuthority {
    /// `ocg external-trial`, under `manual_external_trial`. Direct or through
    /// a trusted loopback proxy.
    ManualExternalTrial,
    /// `ocg run-external-job`, under `manual_external_execution`. Through a
    /// trusted loopback proxy only.
    ManualExternalExecution,
    /// The native scheduler, under `scheduled_external_transport`, through a
    /// trusted loopback proxy only. It is formed from one durable scheduled
    /// external policy and admits exactly the Job binding that policy
    /// authorized; a manual approval of either kind neither forms nor enables
    /// it.
    ScheduledExternalTransport,
}

impl ExternalAuthority {
    /// The control that must be enabled for this authority to connect.
    pub(crate) const fn control(self) -> crate::system::execution_controls::ExecutionControl {
        use crate::system::execution_controls::ExecutionControl;
        match self {
            ExternalAuthority::ManualExternalTrial => ExecutionControl::ManualExternalTrial,
            ExternalAuthority::ManualExternalExecution => ExecutionControl::ManualExternalExecution,
            ExternalAuthority::ScheduledExternalTransport => {
                ExecutionControl::ScheduledExternalTransport
            }
        }
    }

    /// Whether this authority may use `route`.
    pub(crate) const fn admits_route(self, route: &ApprovedRoute) -> bool {
        match (self, route) {
            (
                ExternalAuthority::ManualExternalTrial,
                ApprovedRoute::Direct | ApprovedRoute::TrustedProxy(_),
            )
            | (ExternalAuthority::ManualExternalExecution, ApprovedRoute::TrustedProxy(_)) => true,
            (ExternalAuthority::ScheduledExternalTransport, ApprovedRoute::TrustedProxy(_)) => true,
            (
                ExternalAuthority::ManualExternalExecution
                | ExternalAuthority::ScheduledExternalTransport,
                ApprovedRoute::Direct,
            ) => false,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        self.control().name()
    }
}

/// How the connection to an approved destination is made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovedRoute {
    /// OCG resolves the name itself, requires every address to be public,
    /// and connects to exactly those addresses.
    Direct,
    /// An HTTP CONNECT tunnel through one explicitly named loopback proxy.
    /// The proxy resolves the name and chooses the route: OCG does not look
    /// the name up and cannot prove the address the proxy reaches. There is
    /// no fallback to a direct connection.
    TrustedProxy(LoopbackProxy),
}

/// The address of a proxy on this machine. It can only be built from a
/// loopback address, so no remote proxy can be named by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopbackProxy(std::net::SocketAddr);

impl LoopbackProxy {
    /// `None` unless `address` is a loopback address with a nonzero port.
    pub fn new(address: std::net::SocketAddr) -> Option<Self> {
        (address.ip().is_loopback() && address.port() != 0).then_some(Self(address))
    }

    pub fn address(self) -> std::net::SocketAddr {
        self.0
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
    pub destination: DestinationPolicy,
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
            destination: DestinationPolicy::Standard,
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
            Self::Build { .. }
            | Self::NotDelivered { .. }
            | Self::Unknown { .. }
            | Self::StreamFailure { .. }
            | Self::Incomplete { .. }
            | Self::Interrupted { .. }
            | Self::Stopped { .. }
            | Self::CompletionUnpersisted { .. } => None,
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
