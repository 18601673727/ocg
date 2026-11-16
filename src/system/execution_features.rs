pub(crate) const SCHEDULED_EXTERNAL: (&str, bool) = ("scheduled_external_transport", true);

pub(crate) const EXECUTION_FEATURES: [(&str, bool); 11] = [
    ("manual_loopback_execution", true),
    ("manual_external_trial", false),
    ("manual_external_execution", false),
    ("autonomous_scheduling", true),
    SCHEDULED_EXTERNAL,
    ("external_provider_transport", false),
    ("provider_discovery", false),
    ("recovery_and_redispatch", false),
    ("tools_and_child_jobs", true),
    ("workspace_mutation", true),
    ("legacy_provider_execution", false),
];
