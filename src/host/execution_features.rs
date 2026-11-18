pub(crate) const SCHEDULED_EXTERNAL: &str = "scheduled_external_transport";

pub(crate) const AUTONOMOUS_SCHEDULING: &str = "autonomous_scheduling";

// Compiled capabilities. The host reports scheduler activation separately.
pub(crate) const COMPILED_EXECUTION_FEATURES: [(&str, bool); 11] = [
    ("manual_loopback_execution", true),
    ("manual_external_trial", false),
    ("manual_external_execution", false),
    (AUTONOMOUS_SCHEDULING, true),
    (SCHEDULED_EXTERNAL, true),
    ("external_provider_transport", true),
    ("provider_discovery", true),
    ("recovery_and_redispatch", false),
    ("tools_and_child_jobs", true),
    ("workspace_mutation", true),
    ("legacy_provider_execution", false),
];
