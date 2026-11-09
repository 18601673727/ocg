//! The execution capabilities this build may exercise, one control each.
//!
//! Each control is a compile-time constant on purpose: no environment
//! variable, request or stored row can enable one, so a capability changes
//! only through reviewed source. Unifying the database format activates none
//! of them. Components ask [`ExecutionControl::state`] at the point where the
//! capability would take effect and refuse when it is [`ControlState::Disabled`].

/// A capability whose activation is decided separately from every other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionControl {
    /// One explicitly invoked, bounded, tool-free Job against a literal
    /// loopback endpoint.
    ManualLoopbackExecution,
    /// One explicitly invoked, bounded, tool-free Job against one HTTPS
    /// endpoint and one credential reference the operator names for that
    /// invocation. It is not [`Self::ExternalProviderTransport`]: it admits
    /// no Job, endpoint or credential that the invocation did not name.
    ManualExternalTrial,
    /// One explicitly invoked, bounded, tool-free, ordinary native Generative
    /// Job against one HTTPS endpoint, credential reference and Model that the
    /// invocation names and the Job's frozen configuration already carries,
    /// through one named trusted loopback HTTP proxy. It is a separate
    /// authority from [`Self::ManualExternalTrial`] (neither enables the
    /// other) and is not [`Self::ExternalProviderTransport`],
    /// [`Self::ProviderDiscovery`], [`Self::AutonomousScheduling`] or
    /// [`Self::RecoveryAndRedispatch`]: it admits no Job, endpoint,
    /// credential or route the invocation did not name, and grants no tools,
    /// Workspace or child Jobs.
    ManualExternalExecution,
    /// A scheduler that picks and starts Jobs without a per-Job invocation.
    AutonomousScheduling,
    /// One scheduled external exchange: a Job the scheduler selected and
    /// reserved carries a durable scheduled external policy binding, and
    /// dispatch sends exactly the endpoint, credential reference, Model, trusted
    /// proxy and limits that policy names, revalidated in the start and send
    /// transactions. It is not [`Self::AutonomousScheduling`],
    /// [`Self::ExternalProviderTransport`] or [`Self::ManualExternalExecution`]:
    /// none of theirs stands in for it, and it authorizes nothing a manual
    /// invocation names.
    ScheduledExternalTransport,
    /// Provider requests to any endpoint that is not a literal loopback.
    ExternalProviderTransport,
    /// Interactive model-list requests (the Provider model import and the
    /// Decision model check) to any endpoint that is not a literal loopback.
    /// They carry the stored or supplied credential to the endpoint the
    /// caller names, so they are a Provider egress of their own. They are not
    /// execution: neither [`Self::ManualExternalTrial`] nor
    /// [`Self::ExternalProviderTransport`] enables them, and enabling them
    /// admits no execution.
    ProviderDiscovery,
    /// Adopting, retrying or redispatching work after a restart.
    RecoveryAndRedispatch,
    /// Tools, child Jobs and agent-originated calls.
    ToolsAndChildJobs,
    /// Creating or changing files in a Workspace.
    WorkspaceMutation,
    /// The pre-native Job claim, grant and real-provider execution path.
    LegacyProviderExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlState {
    Enabled,
    Disabled,
}

impl ControlState {
    pub(crate) const fn is_enabled(self) -> bool {
        match self {
            ControlState::Enabled => true,
            ControlState::Disabled => false,
        }
    }
}

impl ExecutionControl {
    pub(crate) const ALL: [ExecutionControl; 11] = [
        ExecutionControl::ManualLoopbackExecution,
        ExecutionControl::ManualExternalTrial,
        ExecutionControl::ManualExternalExecution,
        ExecutionControl::AutonomousScheduling,
        ExecutionControl::ScheduledExternalTransport,
        ExecutionControl::ExternalProviderTransport,
        ExecutionControl::ProviderDiscovery,
        ExecutionControl::RecoveryAndRedispatch,
        ExecutionControl::ToolsAndChildJobs,
        ExecutionControl::WorkspaceMutation,
        ExecutionControl::LegacyProviderExecution,
    ];

    pub(crate) const fn state(self) -> ControlState {
        match self {
            ExecutionControl::ManualLoopbackExecution => ControlState::Enabled,
            ExecutionControl::ManualExternalTrial
            | ExecutionControl::ManualExternalExecution
            | ExecutionControl::AutonomousScheduling
            | ExecutionControl::ScheduledExternalTransport
            | ExecutionControl::ExternalProviderTransport
            | ExecutionControl::ProviderDiscovery
            | ExecutionControl::RecoveryAndRedispatch
            | ExecutionControl::ToolsAndChildJobs
            | ExecutionControl::WorkspaceMutation
            | ExecutionControl::LegacyProviderExecution => ControlState::Disabled,
        }
    }

    pub(crate) const fn is_enabled(self) -> bool {
        self.state().is_enabled()
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            ExecutionControl::ManualLoopbackExecution => "manual_loopback_execution",
            ExecutionControl::ManualExternalTrial => "manual_external_trial",
            ExecutionControl::ManualExternalExecution => "manual_external_execution",
            ExecutionControl::AutonomousScheduling => "autonomous_scheduling",
            ExecutionControl::ScheduledExternalTransport => "scheduled_external_transport",
            ExecutionControl::ExternalProviderTransport => "external_provider_transport",
            ExecutionControl::ProviderDiscovery => "provider_discovery",
            ExecutionControl::RecoveryAndRedispatch => "recovery_and_redispatch",
            ExecutionControl::ToolsAndChildJobs => "tools_and_child_jobs",
            ExecutionControl::WorkspaceMutation => "workspace_mutation",
            ExecutionControl::LegacyProviderExecution => "legacy_provider_execution",
        }
    }

    /// The refusal text for a disabled control.
    pub(crate) fn refusal(self) -> String {
        format!("{} is disabled in this build", self.name())
    }
}
