#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionControl {
    ManualLoopbackExecution,
    ManualExternalTrial,
    ManualExternalExecution,
    AutonomousScheduling,
    ScheduledExternalTransport,
    ExternalProviderTransport,
    ProviderDiscovery,
    RecoveryAndRedispatch,
    ToolsAndChildJobs,
    WorkspaceMutation,
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
            ExecutionControl::AutonomousScheduling => ControlState::Enabled,
            ExecutionControl::ToolsAndChildJobs => ControlState::Enabled,
            ExecutionControl::WorkspaceMutation => ControlState::Enabled,
            ExecutionControl::ManualExternalTrial
            | ExecutionControl::ManualExternalExecution
            | ExecutionControl::ScheduledExternalTransport
            | ExecutionControl::ExternalProviderTransport
            | ExecutionControl::ProviderDiscovery
            | ExecutionControl::RecoveryAndRedispatch
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

    pub(crate) fn refusal(self) -> String {
        format!("{} is disabled in this build", self.name())
    }
}
