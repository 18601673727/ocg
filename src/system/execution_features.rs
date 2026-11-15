#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionFeature {
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
pub(crate) enum FeatureState {
    Enabled,
    Disabled,
}

impl FeatureState {
    pub(crate) const fn is_enabled(self) -> bool {
        match self {
            FeatureState::Enabled => true,
            FeatureState::Disabled => false,
        }
    }
}

impl ExecutionFeature {
    pub(crate) const ALL: [ExecutionFeature; 11] = [
        ExecutionFeature::ManualLoopbackExecution,
        ExecutionFeature::ManualExternalTrial,
        ExecutionFeature::ManualExternalExecution,
        ExecutionFeature::AutonomousScheduling,
        ExecutionFeature::ScheduledExternalTransport,
        ExecutionFeature::ExternalProviderTransport,
        ExecutionFeature::ProviderDiscovery,
        ExecutionFeature::RecoveryAndRedispatch,
        ExecutionFeature::ToolsAndChildJobs,
        ExecutionFeature::WorkspaceMutation,
        ExecutionFeature::LegacyProviderExecution,
    ];

    pub(crate) const fn state(self) -> FeatureState {
        match self {
            ExecutionFeature::ManualLoopbackExecution => FeatureState::Enabled,
            ExecutionFeature::AutonomousScheduling => FeatureState::Enabled,
            ExecutionFeature::ScheduledExternalTransport => FeatureState::Enabled,
            ExecutionFeature::ToolsAndChildJobs => FeatureState::Enabled,
            ExecutionFeature::WorkspaceMutation => FeatureState::Enabled,
            ExecutionFeature::ManualExternalTrial
            | ExecutionFeature::ManualExternalExecution
            | ExecutionFeature::ExternalProviderTransport
            | ExecutionFeature::ProviderDiscovery
            | ExecutionFeature::RecoveryAndRedispatch
            | ExecutionFeature::LegacyProviderExecution => FeatureState::Disabled,
        }
    }

    pub(crate) const fn is_enabled(self) -> bool {
        self.state().is_enabled()
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            ExecutionFeature::ManualLoopbackExecution => "manual_loopback_execution",
            ExecutionFeature::ManualExternalTrial => "manual_external_trial",
            ExecutionFeature::ManualExternalExecution => "manual_external_execution",
            ExecutionFeature::AutonomousScheduling => "autonomous_scheduling",
            ExecutionFeature::ScheduledExternalTransport => "scheduled_external_transport",
            ExecutionFeature::ExternalProviderTransport => "external_provider_transport",
            ExecutionFeature::ProviderDiscovery => "provider_discovery",
            ExecutionFeature::RecoveryAndRedispatch => "recovery_and_redispatch",
            ExecutionFeature::ToolsAndChildJobs => "tools_and_child_jobs",
            ExecutionFeature::WorkspaceMutation => "workspace_mutation",
            ExecutionFeature::LegacyProviderExecution => "legacy_provider_execution",
        }
    }

    pub(crate) fn refusal(self) -> String {
        format!("{} is disabled in this build", self.name())
    }
}
