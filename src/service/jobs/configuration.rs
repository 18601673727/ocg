use crate::db::entities::*;
use crate::db::{BatchError, Failure, StorageError};

pub(crate) struct ConfigurationResolver<F> {
    read: F,
}

impl<F> ConfigurationResolver<F>
where
    F: FnMut(ConfigId) -> Result<Option<ConfigRecord>, BatchError>,
{
    pub(crate) fn new(read: F) -> Self {
        Self { read }
    }

    fn require_config(&mut self, id: ConfigId) -> Result<ConfigRecord, BatchError> {
        (self.read)(id)?.ok_or_else(|| {
            BatchError(Failure::Rejected(Violation::NotFound(RecordRef::Profile(
                id.into(),
            ))))
        })
    }

    fn require_config_kind(
        &mut self,
        id: ConfigId,
        kind: ConfigKind,
    ) -> Result<ConfigRecord, BatchError> {
        let row = self.require_config(id)?;
        if row.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(id.into())));
        }
        if row.kind != kind {
            return self.reject(Violation::WrongConfigKind {
                config: id,
                expected: kind,
                found: row.kind,
            });
        }
        Ok(row)
    }

    fn reject<T>(&self, violation: Violation) -> Result<T, BatchError> {
        Err(BatchError(Failure::Rejected(violation)))
    }

    fn corrupted<T>(&self, detail: String) -> Result<T, BatchError> {
        Err(BatchError(Failure::Storage(StorageError::corrupted(
            detail,
        ))))
    }

    pub(crate) fn project_decision(
        &mut self,
        policy: &ProjectDecisionConfig,
        active: &FrozenExecutionConfig,
    ) -> Result<Option<FrozenDecisionConfig>, BatchError> {
        if !policy.enabled {
            return Ok(None);
        }
        let frozen = if let Some(profile) = policy.decision_profile {
            self.freeze_decision_profile(profile)?
        } else {
            let mut active = active.clone();
            active.agent = None;
            active.agent_ref = None;
            FrozenDecisionConfig::Generative(active, None)
        };
        if let Err(reason) = frozen.validate() {
            return self.reject(Violation::InvalidValue("decision", reason));
        }
        Ok(Some(frozen))
    }

    pub(crate) fn freeze_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenConfiguration, BatchError> {
        let selected = self.require_config(configuration.llm_profile.into())?;
        if selected.kind == ConfigKind::DecisionModel {
            if configuration.reasoning_effort.is_some() {
                return self.reject(Violation::InvalidValue(
                    "reasoning_effort",
                    "native decision models do not accept generative effort",
                ));
            }
            return self
                .freeze_decision_configuration(configuration, selected)
                .map(FrozenConfiguration::Decision);
        }
        if selected.archived_at.is_some() || !selected.model_enabled {
            return self.reject(Violation::NotFound(RecordRef::Profile(
                configuration.llm_profile,
            )));
        }
        if selected.kind != ConfigKind::Model {
            return self.reject(Violation::WrongConfigKind {
                config: configuration.llm_profile.into(),
                expected: ConfigKind::Model,
                found: selected.kind,
            });
        }
        let model = selected;
        let ConfigBody::Model(mut model_body) = model.body else {
            return self.corrupted(format!("{} has a mismatched body", model.id));
        };
        if let Some(effort) = configuration.reasoning_effort {
            if model_body
                .reasoning_efforts
                .as_ref()
                .is_some_and(|values| !values.iter().any(|value| value == effort.as_str()))
            {
                return self.reject(Violation::InvalidValue(
                    "reasoning_effort",
                    "not supported by the selected model",
                ));
            }
            model_body.parameters.reasoning_effort = Some(effort.as_str().to_owned());
        }
        let connection =
            self.require_config_kind(model_body.connection_id, ConfigKind::Connection)?;
        let ConfigBody::Connection(connection_body) = connection.body else {
            return self.corrupted(format!("{} has a mismatched body", connection.id));
        };
        let decision_profile = if let Some(id) = configuration.agent_profile {
            let row = self.require_config(id.into())?;
            if row.archived_at.is_some() {
                return self.reject(Violation::NotFound(RecordRef::Profile(id)));
            }
            if let ConfigBody::DecisionAgent(profile) = row.body {
                if !profile.models.contains(&model.id) {
                    return self.reject(Violation::NotAConfiguredTarget {
                        agent_profile: id,
                        llm_profile: configuration.llm_profile,
                    });
                }
                Some((
                    profile,
                    ConfigRef {
                        id: row.id,
                        edit_seq: row.edit_seq,
                    },
                ))
            } else {
                None
            }
        } else {
            None
        };
        let (agent, agent_ref) = match configuration
            .agent_profile
            .filter(|_| decision_profile.is_none())
        {
            None => (None, None),
            Some(agent_profile) => {
                let agent = self.require_config_kind(agent_profile.into(), ConfigKind::Agent)?;
                let ConfigBody::Agent(agent_body) = agent.body else {
                    return self.corrupted(format!("{} has a mismatched body", agent.id));
                };
                if !agent_body.models.contains(&model.id)
                    || !agent_body.supports_model(&ConfigBody::Model(model_body.clone()))
                {
                    return self.reject(Violation::NotAConfiguredTarget {
                        agent_profile,
                        llm_profile: configuration.llm_profile,
                    });
                }
                (
                    Some(agent_body),
                    Some(ConfigRef {
                        id: agent.id,
                        edit_seq: agent.edit_seq,
                    }),
                )
            }
        };
        let execution = FrozenExecutionConfig {
            agent,
            agent_ref,
            llm: FrozenLlmConfig {
                config: LlmConfig {
                    model: model_body.model,
                    context_window: model_body.context_window,
                    parameters: model_body.parameters,
                    declared_capabilities: model_body.declared_capabilities,
                },
                provider: ProviderSnapshot {
                    id: connection.id.into(),
                    edit_seq: connection.edit_seq,
                    name: connection.name,
                    transport: connection_body.protocol,
                    endpoint: connection_body.endpoint,
                    credential_ref: connection_body.credential_ref,
                },
                model: ConfigRef {
                    id: model.id,
                    edit_seq: model.edit_seq,
                },
            },
        };
        Ok(if decision_profile.is_some() {
            FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(
                execution,
                decision_profile,
            ))
        } else {
            FrozenConfiguration::Generative(execution)
        })
    }

    fn freeze_decision_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
        selected: ConfigRecord,
    ) -> Result<FrozenDecisionConfig, BatchError> {
        if selected.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(
                configuration.llm_profile,
            )));
        }
        if selected.kind != ConfigKind::DecisionModel {
            return self.reject(Violation::WrongConfigKind {
                config: configuration.llm_profile.into(),
                expected: ConfigKind::DecisionModel,
                found: selected.kind,
            });
        }
        let model = selected;
        let ConfigBody::DecisionModel(model_body) = model.body else {
            return self.corrupted("mismatched decision model".into());
        };
        let (agent_body, agent_ref) = if let Some(agent_profile) = configuration.agent_profile {
            let agent = self.require_config(agent_profile.into())?;
            if agent.archived_at.is_some() {
                return self.reject(Violation::NotFound(RecordRef::Profile(agent_profile)));
            }
            let body = match agent.body {
                ConfigBody::DecisionAgent(body) => {
                    crate::db::entities::FrozenDecisionAgent::Profile(body)
                }
                ConfigBody::Agent(body)
                    if body.supports_model(&ConfigBody::DecisionModel(model_body.clone())) =>
                {
                    crate::db::entities::FrozenDecisionAgent::Agent(Box::new(body))
                }
                ConfigBody::Agent(_)
                | ConfigBody::Model(_)
                | ConfigBody::DecisionModel(_)
                | ConfigBody::Connection(_)
                | ConfigBody::Pool(_) => {
                    return self.reject(Violation::InvalidValue(
                        "capabilities",
                        "the Agent must support decision work with this model",
                    ));
                }
            };
            if !body.models().contains(&model.id) {
                return self.reject(Violation::NotAConfiguredTarget {
                    agent_profile,
                    llm_profile: configuration.llm_profile,
                });
            }
            (
                Some(body),
                Some(ConfigRef {
                    id: agent.id,
                    edit_seq: agent.edit_seq,
                }),
            )
        } else {
            (None, None)
        };
        let connection =
            self.require_config_kind(model_body.connection_id, ConfigKind::Connection)?;
        let ConfigBody::Connection(connection_body) = connection.body else {
            return self.corrupted("mismatched connection".into());
        };
        Ok(FrozenDecisionConfig::Native(NativeDecisionConfig {
            agent: agent_body,
            agent_ref,
            model: model_body,
            model_ref: ConfigRef {
                id: model.id,
                edit_seq: model.edit_seq,
            },
            connection_ref: ConfigRef {
                id: connection.id,
                edit_seq: connection.edit_seq,
            },
            endpoint: connection_body.endpoint,
            credential_ref: connection_body.credential_ref,
        }))
    }

    pub(crate) fn freeze_decision_profile(
        &mut self,
        profile: ConfigId,
    ) -> Result<FrozenDecisionConfig, BatchError> {
        let row = self.require_config(profile)?;
        if row.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(profile.into())));
        }
        let (agent_profile, model) = match row.body {
            ConfigBody::DecisionAgent(agent) => (
                Some(profile.into()),
                agent
                    .default_model
                    .or_else(|| agent.models.first().copied()),
            ),
            ConfigBody::Agent(agent)
                if agent
                    .capabilities
                    .contains(&crate::db::entities::AgentCapability::Decision) =>
            {
                (Some(profile.into()), agent.default_model())
            }
            ConfigBody::Agent(_) => (None, None),
            ConfigBody::Model(_) | ConfigBody::DecisionModel(_) => (None, Some(profile)),
            ConfigBody::Connection(_) | ConfigBody::Pool(_) => (None, None),
        };
        let Some(model) = model else {
            return self.reject(Violation::InvalidValue(
                "decision",
                "select a configured model or a decision configuration with an active model",
            ));
        };
        let frozen = match self.freeze_configuration(ExecutionConfiguration {
            reasoning_effort: None,
            agent_profile,
            llm_profile: model.into(),
        })? {
            FrozenConfiguration::Decision(frozen) => frozen,
            FrozenConfiguration::Generative(frozen) => {
                FrozenDecisionConfig::Generative(frozen, None)
            }
        };
        if let Err(reason) = frozen.validate() {
            return self.reject(Violation::InvalidValue("decision", reason));
        }
        if !frozen.capabilities().contains(&DecisionCapability::Choice) {
            return self.reject(Violation::InvalidValue(
                "decision",
                "decision model must support Choice",
            ));
        }
        Ok(frozen)
    }
}
