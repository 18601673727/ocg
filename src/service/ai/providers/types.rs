use super::*;

pub trait CredentialResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        credential_ref: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, ProviderExecutionError>> + Send + 'a>>;
}

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

    fn observe<'a>(
        &'a mut self,
        _observation: ProviderObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async {})
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderObservation {
    pub observed_at: Timestamp,
    pub fact: ProviderFact,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderFact {
    NotDelivered,
    Delivered,
    Usage {
        usage: NormalizedUsage,
        finality: UsageFinality,
    },
}

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DestinationPolicy {
    #[default]
    Standard,
    ApprovedHttps {
        host: String,
        port: u16,
        route: ApprovedRoute,
        authority: ExternalAuthority,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalAuthority {
    ManualExternalTrial,
    ManualExternalExecution,
    ScheduledExternalTransport,
}

impl ExternalAuthority {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovedRoute {
    Direct,
    TrustedProxy(LoopbackProxy),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopbackProxy(std::net::SocketAddr);

impl LoopbackProxy {
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

#[derive(Debug, Clone)]
pub struct ProviderExecutionResult {
    pub response_items: Vec<Value>,
    pub text: String,
    pub reasoning: String,
    pub tool_calls: Vec<CompletedToolCall>,
    pub refused: Vec<InvalidToolCall>,
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
    pub status_code: u16,
    pub request_id: Option<String>,
    pub ttfb: Duration,
    pub finished_in: Duration,
}

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
    #[error("provider response failed after possible delivery: {message}")]
    Unknown {
        message: String,
        facts: Option<ResponseFacts>,
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
    #[error("provider stream was incomplete: {message}")]
    Incomplete {
        message: String,
        facts: ResponseFacts,
    },
    #[error("provider execution was interrupted locally: {message}")]
    Interrupted { message: String },
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
    #[error("provider response completed but its output could not be persisted: {message}")]
    CompletionUnpersisted {
        message: String,
        result: Box<ProviderExecutionResult>,
    },
}

impl ProviderExecutionResult {
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
