use crate::db::entities::{
    ConfigBody, ConfigId, ConfigKind, ConnectionConfig, DecisionModelConfig,
    ExecutionConfiguration, ExecutionOperation, FrozenConfiguration, JobId, LlmTransport,
    ModelConfig, RecordRef, Violation, valid_credential_ref,
};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::codec::{Enc, TAG_GLOBAL_COMPACTION, TAG_GLOBAL_HEADERS};
use crate::db::redb::reads;
use crate::db::redb::reads::catalog::live_credential_refs;
use crate::db::redb::rows::{ConfigRow, decode_project};
use crate::db::redb::schema::{CONFIGS, CREDENTIALS, JOB_RUNTIME, META, PROJECTS};
use crate::db::redb::state::JobRuntime;
use ::redb::ReadableTable;
use std::collections::BTreeSet;

impl Batch {
    pub(crate) fn set_global_compaction_settings(
        &mut self,
        mut settings: crate::db::entities::GlobalCompactionSettings,
    ) -> Result<(), BatchError> {
        let current =
            self.step(|tx| reads::catalog::global_compaction_settings(&tx.open_table(META)?))?;
        if settings.revision != current.revision {
            return self.reject(Violation::InvalidValue(
                "revision",
                "global compaction settings changed; reload before saving",
            ));
        }
        self.check_compaction_policy(&settings.policy)?;
        let Some(revision) = settings.revision.checked_add(1) else {
            return self.corrupted("compaction revision exhausted".into());
        };
        settings.revision = revision;
        self.step(|tx| {
            let mut e = Enc::new(TAG_GLOBAL_COMPACTION);
            e.u64(settings.revision);
            e.bool(settings.prediction_enabled);
            e.compaction_policy(&settings.policy);
            tx.open_table(META)?
                .insert("compaction_settings", e.0.as_slice())?;
            Ok(())
        })
    }

    fn provision_decision_executor(&mut self, model: ConfigId) -> Result<(), BatchError> {
        use crate::db::entities::{ConfigBody, DecisionCapability, ExecutionOperation};
        let row = self.require_config(model)?;
        if !matches!(&row.body, ConfigBody::DecisionModel(body) if body.declared_capabilities.contains(&DecisionCapability::Choice))
        {
            return Ok(());
        }
        self.require_native_capability(ExecutionOperation::Decision, None)
    }

    pub(crate) fn agent_profile_ids(
        &mut self,
        after: u128,
        limit: usize,
    ) -> Result<(Vec<ConfigId>, Option<ConfigId>), BatchError> {
        self.step(|tx| {
            let mut ids = Vec::new();
            let mut next = None;
            for (scanned, entry) in tx
                .open_table(CONFIGS)?
                .range(after.saturating_add(1)..)?
                .enumerate()
            {
                let (key, bytes) = entry?;
                let id = key.value();
                let record = ConfigRow::decode_at(id, bytes.value())?;
                if record.kind == ConfigKind::Agent && record.archived_at.is_none() {
                    ids.push(record.id);
                }
                if ids.len() == limit || scanned == 511 {
                    next = Some(ConfigId::new(id));
                    break;
                }
            }
            Ok((ids, next))
        })
    }

    pub(crate) fn config(&mut self, id: ConfigId) -> Result<Option<ConfigRow>, BatchError> {
        self.step(|tx| {
            tx.open_table(CONFIGS)?
                .get(id.get())?
                .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()))
                .transpose()
        })
    }

    pub(crate) fn config_record(
        &mut self,
        id: ConfigId,
    ) -> Result<Option<crate::db::entities::ConfigRecord>, BatchError> {
        self.config(id)
            .map(|row| row.map(reads::catalog::config_record))
    }

    pub(crate) fn require_config(&mut self, id: ConfigId) -> Result<ConfigRow, BatchError> {
        match self.config(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Profile(id.into()))),
        }
    }

    pub(crate) fn require_config_kind(
        &mut self,
        id: ConfigId,
        kind: ConfigKind,
    ) -> Result<ConfigRow, BatchError> {
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

    fn resolve_and_write_credential(
        &mut self,
        change: crate::db::mutations::ProfileCredentialRef,
        prior: Option<String>,
        credential: Option<&crate::db::mutations::CredentialPayload>,
    ) -> Result<Option<String>, BatchError> {
        use crate::db::mutations::ProfileCredentialRef;
        let base = match change {
            ProfileCredentialRef::Preserve => prior,
            ProfileCredentialRef::Clear => None,
            ProfileCredentialRef::Explicit(reference) => Some(reference),
            ProfileCredentialRef::PreserveOrGenerate(base) => match prior {
                Some(reference) => Some(reference),
                None => Some(self.allocate_credential_ref(&base)?),
            },
        };
        let resolved = match (credential, base) {
            (Some(_), Some(reference)) => Some(self.credential_generation(&reference)?),
            (Some(_), None) => {
                return self.reject(Violation::InvalidValue(
                    "credential_ref",
                    "credential secret requires a reference",
                ));
            }
            (None, reference) => reference,
        };
        if let (Some(secret), Some(reference)) = (credential, resolved.as_deref()) {
            self.write_credential(reference, secret)?;
        }
        Ok(resolved)
    }

    fn find_or_create_connection(
        &mut self,
        name: String,
        body: ConnectionConfig,
        require_unarchived: bool,
    ) -> Result<ConfigId, BatchError> {
        let existing = self.step(|tx| {
            let mut found = None;
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if (!require_unarchived || row.archived_at.is_none())
                    && matches!(&row.body, ConfigBody::Connection(c) if c == &body)
                {
                    found = Some(row.id);
                    if require_unarchived {
                        break;
                    }
                }
            }
            Ok(found)
        })?;
        match existing {
            Some(id) => Ok(id),
            None => self.create_config(name, ConfigBody::Connection(body)),
        }
    }

    pub(crate) fn save_model_profile(
        &mut self,
        save: crate::db::mutations::ModelProfileSave,
    ) -> Result<(ConfigId, Option<String>), BatchError> {
        use crate::db::mutations::ModelProfileDefinition;

        let profile_kind = match &save.definition {
            ModelProfileDefinition::Llm { .. } => ConfigKind::Model,
            ModelProfileDefinition::Decision { .. } => ConfigKind::DecisionModel,
        };
        let previous = save
            .existing_id
            .map(|id| self.require_config_kind(id.into(), profile_kind))
            .transpose()?;
        let previous_connection = if let Some(profile) = &previous {
            let connection_id = match &profile.body {
                ConfigBody::Model(model) => model.connection_id,
                ConfigBody::DecisionModel(model) => model.connection_id,
                ConfigBody::Connection(_) | ConfigBody::Agent(_) | ConfigBody::DecisionAgent(_) => {
                    return self.reject(Violation::WrongConfigKind {
                        config: profile.id,
                        expected: profile_kind,
                        found: profile.kind,
                    });
                }
            };
            let Some(connection) = self.config(connection_id)? else {
                return self.corrupted("Model Profile Connection is missing".into());
            };
            let ConfigBody::Connection(connection_body) = &connection.body else {
                return self.corrupted("Model Profile references a non-Connection config".into());
            };
            if let Some(guard) = &save.guard
                && (guard.profile_edit_seq != profile.edit_seq
                    || guard.connection_id != connection_id
                    || guard.connection_edit_seq != connection.edit_seq)
            {
                return self.reject(Violation::ProfileChanged);
            }
            Some((connection_id, connection_body.clone()))
        } else {
            if save.guard.is_some() {
                return self.reject(Violation::ProfileChanged);
            }
            None
        };

        let protocol = match &save.definition {
            ModelProfileDefinition::Llm { protocol, .. } => protocol
                .or_else(|| {
                    previous_connection
                        .as_ref()
                        .map(|(_, connection)| connection.protocol)
                })
                .unwrap_or(LlmTransport::OpenAiChatCompletions),
            ModelProfileDefinition::Decision { .. } => LlmTransport::OpenAiChatCompletions,
        };
        let prior_credential_ref = previous_connection
            .as_ref()
            .and_then(|(_, connection)| connection.credential_ref.clone());
        let credential_ref = self.resolve_and_write_credential(
            save.credential_ref,
            prior_credential_ref,
            save.credential.as_ref(),
        )?;

        let previous_headers = previous_connection
            .as_ref()
            .map(|(_, connection)| connection.headers.clone())
            .unwrap_or_default();
        let headers = self.apply_header_change(&previous_headers, save.headers)?;

        if matches!(&save.definition, ModelProfileDefinition::Llm { .. }) {
            self.require_llm_executor_if_executable(protocol)?;
        }

        let connection_body = ConnectionConfig {
            protocol,
            endpoint: save.endpoint,
            credential_ref: credential_ref.clone(),
            headers,
        };
        let connection_id =
            self.find_or_create_connection(save.connection_name, connection_body, false)?;
        if profile_kind == ConfigKind::Model {
            self.set_config_enabled(connection_id, ConfigKind::Connection, true)?;
        }

        let body = match save.definition {
            ModelProfileDefinition::Llm {
                model,
                context_window,
                parameters,
                declared_capabilities,
                ..
            } => {
                let reasoning_efforts = previous.as_ref().and_then(|profile| {
                    let ConfigBody::Model(previous) = &profile.body else {
                        return None;
                    };
                    (previous.connection_id == connection_id && previous.model == model)
                        .then(|| previous.reasoning_efforts.clone())
                        .flatten()
                });
                ConfigBody::Model(ModelConfig {
                    reasoning_efforts,
                    connection_id,
                    model,
                    context_window,
                    parameters,
                    declared_capabilities,
                })
            }
            ModelProfileDefinition::Decision {
                protocol,
                model,
                limits,
                declared_capabilities,
            } => ConfigBody::DecisionModel(DecisionModelConfig {
                connection_id,
                model,
                protocol,
                limits,
                declared_capabilities,
            }),
        };
        let profile_id = match save.existing_id {
            Some(id) => {
                self.update_config(id.into(), save.name, body)?;
                id.into()
            }
            None => self.create_config(save.name, body)?,
        };
        self.purge_unused_credentials()?;
        Ok((profile_id, credential_ref))
    }

    fn apply_header_change(
        &mut self,
        previous: &[crate::db::entities::OutboundHeader],
        change: crate::db::mutations::HeaderChange,
    ) -> Result<Vec<crate::db::entities::OutboundHeader>, BatchError> {
        use crate::db::entities::{HeaderScope, OutboundHeader, OutboundHeaderValue};
        use crate::db::mutations::{HeaderChange, HeaderEditValue};
        let edits = match change {
            HeaderChange::Preserve => return Ok(previous.to_vec()),
            HeaderChange::Replace(edits) => edits,
        };
        let mut fields = Vec::with_capacity(edits.len());
        for edit in edits {
            let existing = previous.iter().find_map(|field| match &field.value {
                OutboundHeaderValue::Secret(reference) if field.name == edit.name => {
                    Some(reference.clone())
                }
                OutboundHeaderValue::Secret(_) | OutboundHeaderValue::Plain(_) => None,
            });
            let value = match edit.value {
                HeaderEditValue::Plain(value) => OutboundHeaderValue::Plain(value),
                HeaderEditValue::Secret(None) => match existing {
                    Some(reference) => OutboundHeaderValue::Secret(reference),
                    None => {
                        return self.reject(Violation::InvalidValue(
                            "headers",
                            "no stored secret exists for this header; supply its value",
                        ));
                    }
                },
                HeaderEditValue::Secret(Some(payload)) => {
                    let sanitized: String = edit
                        .name
                        .chars()
                        .map(|character| {
                            if character.is_ascii_alphanumeric() || character == '-' {
                                character
                            } else {
                                '-'
                            }
                        })
                        .take(100)
                        .collect();
                    let reference = self.allocate_credential_ref(&format!("header-{sanitized}"))?;
                    self.write_credential(&reference, &payload)?;
                    OutboundHeaderValue::Secret(reference)
                }
            };
            fields.push(OutboundHeader {
                name: edit.name,
                value,
            });
        }
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        if let Err(error) = crate::db::entities::validate_fields(&fields, HeaderScope::Connection) {
            return self.reject(Violation::InvalidValue("headers", error.reason()));
        }
        Ok(fields)
    }

    fn credential_generation(&mut self, reference: &str) -> Result<String, BatchError> {
        if self.allocate_credential_ref(reference)? == reference {
            return Ok(reference.to_owned());
        }
        let stem = reference
            .rsplit_once('_')
            .filter(|(stem, number)| {
                !stem.is_empty()
                    && number
                        .parse::<u64>()
                        .is_ok_and(|value| value >= 2 && value.to_string() == *number)
            })
            .map_or(reference, |(stem, _)| stem);
        let mut end = stem.len().min(100);
        while !stem.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        self.allocate_credential_ref(&stem[..end])
    }

    fn allocate_credential_ref(&mut self, base: &str) -> Result<String, BatchError> {
        let (base_taken, suffixes) = self.step(|tx| {
            let mut base_taken = false;
            let mut suffixes = std::collections::BTreeSet::new();
            let mut record = |reference: &str| {
                if reference == base {
                    base_taken = true;
                } else if let Some(suffix) = reference
                    .strip_prefix(base)
                    .and_then(|value| value.strip_prefix('_'))
                    && let Ok(number) = suffix.parse::<u64>()
                    && number.to_string() == suffix
                {
                    suffixes.insert(number);
                }
            };
            let table = tx.open_table(CONFIGS)?;
            for entry in table.iter()? {
                let (key, bytes) = entry?;
                let row = ConfigRow::decode_at(key.value(), bytes.value())?;
                if let ConfigBody::Connection(connection) = &row.body {
                    connection.credential_refs().for_each(&mut record);
                }
            }
            for entry in tx.open_table(CREDENTIALS)?.iter()? {
                let (key, _) = entry?;
                record(key.value());
            }
            Ok((base_taken, suffixes))
        })?;
        if !base_taken {
            return Ok(base.to_owned());
        }
        let mut suffix = 2u64;
        while suffixes.contains(&suffix) {
            suffix = suffix.checked_add(1).ok_or({
                BatchError(Failure::Rejected(Violation::InvalidValue(
                    "credential_ref",
                    "could not allocate a unique reference",
                )))
            })?;
        }
        Ok(format!("{base}_{suffix}"))
    }

    fn write_credential(
        &mut self,
        reference: &str,
        credential: &crate::db::mutations::CredentialPayload,
    ) -> Result<(), BatchError> {
        if !valid_credential_ref(reference) {
            return self.reject(Violation::InvalidValue(
                "credential_ref",
                "must be 1..=128 non-control bytes",
            ));
        }
        self.step(|tx| {
            let encrypted = credential.key.encrypt(reference, &credential.secret)?;
            tx.open_table(CREDENTIALS)?
                .insert(reference, encrypted.as_slice())?;
            Ok(())
        })
    }

    fn purge_unused_credentials(&mut self) -> Result<(), BatchError> {
        self.step(|tx| {
            let used = live_credential_refs(tx)?;
            let mut credentials = tx.open_table(CREDENTIALS)?;
            let unused: Vec<String> = credentials
                .iter()?
                .map(|entry| entry.map(|(key, _)| key.value().to_owned()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|reference| !used.contains(reference))
                .collect();
            for reference in unused {
                credentials.remove(reference.as_str())?;
            }
            Ok(())
        })
    }

    fn require_llm_executor_if_executable(
        &mut self,
        protocol: LlmTransport,
    ) -> Result<(), BatchError> {
        if protocol.execution_refusal().is_none() {
            self.require_native_capability(ExecutionOperation::LlmRequest, Some(protocol))?;
        }
        Ok(())
    }

    fn check_config_body(&mut self, name: &str, body: &ConfigBody) -> Result<(), BatchError> {
        if name.is_empty() || name.len() > 255 {
            return self.reject(Violation::InvalidValue("name", "must be 1 to 255 bytes"));
        }
        match body {
            ConfigBody::Connection(_) => {}
            ConfigBody::Model(model) => {
                self.require_config_kind(model.connection_id, ConfigKind::Connection)?;
            }
            ConfigBody::Agent(agent) => {
                if let Some(policy) = &agent.compaction_policy {
                    self.check_compaction_policy(policy)?;
                }
                if agent.capabilities.is_empty() {
                    return self.reject(Violation::InvalidValue(
                        "capabilities",
                        "select Agent capabilities",
                    ));
                }
                for model in &agent.models {
                    let record = self.require_config(*model)?;
                    if record.archived_at.is_some() || !agent.supports_model(&record.body) {
                        return self.reject(Violation::InvalidValue(
                            "models",
                            "each assigned model must support every selected Agent capability",
                        ));
                    }
                }
            }
            ConfigBody::DecisionModel(dm) => {
                self.require_config_kind(dm.connection_id, ConfigKind::Connection)?;
            }
            ConfigBody::DecisionAgent(da) => {
                for model in &da.models {
                    let row = self.require_config(*model)?;
                    if row.archived_at.is_some()
                        || !matches!(row.kind, ConfigKind::Model | ConfigKind::DecisionModel)
                    {
                        return self.reject(Violation::InvalidValue(
                            "models",
                            "decision targets must be configured models",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn check_compaction_policy(
        &mut self,
        policy: &crate::db::entities::CompactionPolicy,
    ) -> Result<(), BatchError> {
        if let Err(reason) = policy.validate() {
            return self.reject(Violation::InvalidValue("compaction_policy", reason));
        }
        if let Some(model) = policy.summarization_model {
            self.require_config_kind(model, ConfigKind::Model)?;
        }
        if let Some(predictive) = &policy.predictive {
            let agent =
                self.require_config_kind(predictive.decision_agent, ConfigKind::DecisionAgent)?;
            let ConfigBody::DecisionAgent(body) = agent.body else {
                return self.corrupted("mismatched decision agent".into());
            };
            if body.models.is_empty() {
                return self.reject(Violation::InvalidValue(
                    "decision_agent",
                    "predictive compaction requires a configured decision model",
                ));
            }
            let selected = body.default_model.or_else(|| body.models.first().copied());
            for model in body.models {
                let record = self.require_config_kind(model, ConfigKind::DecisionModel)?;
                if selected == Some(model)
                    && !matches!(record.body, ConfigBody::DecisionModel(model) if model.declared_capabilities.contains(&crate::db::entities::DecisionCapability::Choice))
                {
                    return self.reject(Violation::InvalidValue(
                        "decision_agent",
                        "context prediction requires Choice capability",
                    ));
                }
            }
        }
        Ok(())
    }

    fn put_config(&mut self, row: &ConfigRow) -> Result<(), BatchError> {
        self.insert(CONFIGS, row.id.get(), row.encode().as_slice())
    }

    pub(crate) fn set_config_enabled(
        &mut self,
        id: ConfigId,
        kind: ConfigKind,
        enabled: bool,
    ) -> Result<(), BatchError> {
        let mut row = self.require_config_kind(id, kind)?;
        if row.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(id.into())));
        }
        let value = match kind {
            ConfigKind::Connection => &mut row.discovery_visible,
            ConfigKind::Model => &mut row.model_enabled,
            ConfigKind::Agent | ConfigKind::DecisionModel | ConfigKind::DecisionAgent => {
                return self.reject(Violation::InvalidValue(
                    "kind",
                    "only connections and models have availability",
                ));
            }
        };
        if *value == enabled {
            return Ok(());
        }
        *value = enabled;
        row.updated_at = self.now;
        row.edit_seq += 1;
        self.catalog_changed = true;
        self.put_config(&row)
    }

    pub(crate) fn create_config(
        &mut self,
        name: String,
        body: ConfigBody,
    ) -> Result<ConfigId, BatchError> {
        self.check_config_body(&name, &body)?;
        let id = ConfigId::new(self.next_id()?);
        let row = ConfigRow {
            id,
            kind: body.kind(),
            name,
            edit_seq: 1,
            created_at: self.now,
            updated_at: self.now,
            archived_at: None,
            model_enabled: true,
            discovery_visible: true,
            body,
        };
        self.put_config(&row)?;
        if row.kind == ConfigKind::DecisionModel {
            self.provision_decision_executor(id)?;
        }
        Ok(id)
    }

    pub(crate) fn update_config(
        &mut self,
        id: ConfigId,
        name: String,
        body: ConfigBody,
    ) -> Result<(), BatchError> {
        self.update_config_from(id, name, body, None)
    }

    pub(crate) fn update_config_from(
        &mut self,
        id: ConfigId,
        name: String,
        body: ConfigBody,
        expected_edit_seq: Option<u32>,
    ) -> Result<(), BatchError> {
        let existing = self.require_config(id)?;
        if existing.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(id.into())));
        }
        if expected_edit_seq.is_some_and(|expected| expected != existing.edit_seq) {
            return self.reject(Violation::ProfileChanged);
        }
        if existing.kind != body.kind() {
            return self.reject(Violation::WrongConfigKind {
                config: id,
                expected: existing.kind,
                found: body.kind(),
            });
        }
        self.check_config_body(&name, &body)?;
        let row = ConfigRow {
            name,
            edit_seq: existing.edit_seq + 1,
            updated_at: self.now,
            body,
            ..existing
        };
        self.put_config(&row)?;
        if row.kind == ConfigKind::DecisionModel {
            self.provision_decision_executor(id)?;
        }
        Ok(())
    }

    pub(crate) fn import_models(
        &mut self,
        save: crate::db::mutations::ImportModelsSave,
    ) -> Result<(Vec<ConfigId>, usize), BatchError> {
        use crate::db::entities::{LlmParameters, ModelConfig};
        let crate::db::mutations::ImportModelsSave {
            headers,
            mut connection,
            credential_ref,
            secret: _,
            credential,
            models,
            capabilities,
        } = save;
        if models.is_empty() || models.len() > 200 || capabilities.is_empty() {
            return self.reject(Violation::InvalidValue(
                "models",
                "select 1 to 200 models with capabilities",
            ));
        }
        let prior_ref = match &connection.body {
            ConfigBody::Connection(wanted) => wanted.credential_ref.clone(),
            _ => None,
        };
        let resolved_ref =
            self.resolve_and_write_credential(credential_ref, prior_ref, credential.as_ref())?;
        let ConfigBody::Connection(wanted) = &mut connection.body else {
            return self.reject(Violation::InvalidValue(
                "connection",
                "must be a Connection",
            ));
        };
        let own_headers = wanted.headers.clone();
        wanted.headers = self.apply_header_change(&own_headers, headers)?;
        wanted.credential_ref = resolved_ref;
        let wanted = wanted.clone();
        let wanted_protocol = wanted.protocol;
        let connection_id = self.find_or_create_connection(connection.name, wanted, true)?;
        self.set_config_enabled(connection_id, ConfigKind::Connection, true)?;
        self.require_llm_executor_if_executable(wanted_protocol)?;
        let mut known = self.step(|tx| {
            let mut known = std::collections::BTreeMap::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none()
                    && let ConfigBody::Model(model) = row.body
                    && model.connection_id == connection_id
                    && models.iter().any(|entry| entry.id == model.model)
                {
                    known.entry(model.model).or_insert(row.id);
                }
            }
            Ok(known)
        })?;
        let mut ids = Vec::with_capacity(models.len());
        let mut created = 0;
        for entry in models {
            let crate::db::entities::DiscoveredModel {
                id: model,
                context_window,
                reasoning_efforts,
            } = entry;
            if model.trim().is_empty()
                || model.len() > 256
                || model.chars().any(char::is_control)
                || context_window == Some(0)
            {
                return self.reject(Violation::InvalidValue(
                    "model",
                    "invalid model or context window",
                ));
            }
            let id = match known.get(&model) {
                Some(id) => {
                    let id = *id;
                    let row = self.require_config(id)?;
                    if let ConfigBody::Model(mut body) = row.body
                        && body.context_window.is_none()
                        && context_window.is_some()
                    {
                        body.context_window = context_window;
                        self.update_config(id, row.name, ConfigBody::Model(body))?;
                    }
                    id
                }
                None => {
                    let name: String = model
                        .chars()
                        .scan(0usize, |bytes, ch| {
                            *bytes += ch.len_utf8();
                            (*bytes <= 128).then_some(ch)
                        })
                        .collect();
                    let id = self.create_config(
                        name,
                        ConfigBody::Model(ModelConfig {
                            connection_id,
                            reasoning_efforts,
                            model: model.clone(),
                            context_window,
                            parameters: LlmParameters::default(),
                            declared_capabilities: capabilities.clone(),
                        }),
                    )?;
                    known.insert(model, id);
                    created += 1;
                    id
                }
            };
            ids.push(id);
        }
        self.purge_unused_credentials()?;
        Ok((ids, created))
    }

    pub(crate) fn refresh_model_metadata(
        &mut self,
        connection: crate::db::entities::ConnectionConfig,
        models: Vec<crate::db::entities::DiscoveredModel>,
    ) -> Result<(), BatchError> {
        let models = models
            .into_iter()
            .map(|model| (model.id.clone(), model))
            .collect::<std::collections::BTreeMap<_, _>>();
        let rows = self.step(|tx| {
            let mut rows = Vec::new();
            let mut connections = std::collections::BTreeSet::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none() && matches!(&row.body, ConfigBody::Connection(body) if body.protocol == connection.protocol && body.credential_ref == connection.credential_ref && {
                    let default = if body.protocol == LlmTransport::AnthropicMessages { "https://api.anthropic.com/v1" } else { "https://api.openai.com/v1" };
                    body.endpoint.as_deref().unwrap_or(default).trim_end_matches('/') == connection.endpoint.as_deref().unwrap_or(default).trim_end_matches('/')
                }) {
                    connections.insert(row.id);
                }
            }
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none() && matches!(&row.body, ConfigBody::Model(model) if connections.contains(&model.connection_id)) {
                    rows.push(row);
                }
            }
            Ok(rows)
        })?;
        for row in rows {
            let ConfigBody::Model(mut body) = row.body else {
                continue;
            };
            let efforts = models
                .get(&body.model)
                .and_then(|model| model.reasoning_efforts.clone());
            let context_window = body.context_window.or_else(|| {
                models
                    .get(&body.model)
                    .and_then(|model| model.context_window)
                    .filter(|value| *value > 0)
            });
            if body.reasoning_efforts != efforts || body.context_window != context_window {
                body.reasoning_efforts = efforts;
                body.context_window = context_window;
                self.update_config(row.id, row.name, ConfigBody::Model(body))?;
            }
        }
        Ok(())
    }

    pub(crate) fn archive_model(&mut self, id: ConfigId) -> Result<(), BatchError> {
        let row = self.require_config(id)?;
        if !matches!(
            row.kind,
            ConfigKind::Model | ConfigKind::DecisionModel | ConfigKind::DecisionAgent
        ) {
            return self.reject(Violation::WrongConfigKind {
                config: id,
                expected: ConfigKind::Model,
                found: row.kind,
            });
        }
        if row.archived_at.is_some() {
            return Ok(());
        }
        self.archive_models(vec![row])
    }

    fn archive_models(&mut self, rows: Vec<ConfigRow>) -> Result<(), BatchError> {
        if rows.is_empty() {
            return self.purge_unused_credentials();
        }
        let ids: BTreeSet<_> = rows.iter().map(|row| row.id).collect();
        let agents = self.step(|tx| {
            let mut agents = Vec::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none()
                    && matches!(
                        &row.body,
                        ConfigBody::Agent(_) | ConfigBody::DecisionAgent(_)
                    )
                {
                    agents.push(row);
                }
            }
            Ok(agents)
        })?;
        let mut emptied: BTreeSet<_> = rows
            .iter()
            .filter(|row| row.kind == ConfigKind::DecisionAgent)
            .map(|row| row.id)
            .collect();
        for row in &agents {
            if let ConfigBody::DecisionAgent(agent) = &row.body
                && agent.models.iter().any(|model| ids.contains(model))
                && agent.models.iter().all(|model| ids.contains(model))
            {
                emptied.insert(row.id);
            }
        }
        let clear_policy = |policy: &mut crate::db::entities::CompactionPolicy| {
            if policy
                .summarization_model
                .is_some_and(|model| ids.contains(&model))
            {
                policy.summarization_model = None;
            }
            if policy
                .predictive
                .as_ref()
                .is_some_and(|prediction| emptied.contains(&prediction.decision_agent))
            {
                policy.predictive = None;
            } else if let Some(prediction) = &mut policy.predictive
                && prediction
                    .acceptance
                    .as_ref()
                    .is_some_and(|pin| ids.contains(&pin.model.id))
            {
                prediction.mode = crate::db::entities::PredictionMode::Shadow;
                prediction.acceptance = None;
            }
        };
        for mut agent_row in agents {
            let before = agent_row.body.clone();
            match &mut agent_row.body {
                ConfigBody::Agent(agent) => {
                    agent.models.retain(|model| !ids.contains(model));
                    if let Some(policy) = &mut agent.compaction_policy {
                        clear_policy(policy);
                    }
                }
                ConfigBody::DecisionAgent(agent) => {
                    agent.models.retain(|model| !ids.contains(model));
                    if agent
                        .default_model
                        .is_some_and(|model| ids.contains(&model))
                    {
                        agent.default_model = None;
                    }
                }
                ConfigBody::Connection(_) | ConfigBody::Model(_) | ConfigBody::DecisionModel(_) => {
                }
            }
            if agent_row.body != before {
                self.update_config(agent_row.id, agent_row.name, agent_row.body)?;
            }
        }
        let conversations = self.step(|tx| {
            let mut changes = Vec::new();
            for entry in tx.open_table(JOB_RUNTIME)?.iter()? {
                let (key, bytes) = entry?;
                if let Some(mut policy) = JobRuntime::decode_at(key.value(), bytes.value())?
                    .lifecycle
                    .compaction_policy
                {
                    let before = policy.clone();
                    clear_policy(&mut policy);
                    if policy != before {
                        changes.push((JobId::new(key.value()), policy));
                    }
                }
            }
            Ok(changes)
        })?;
        for (conversation, policy) in conversations {
            crate::db::mutations::set_conversation(
                self,
                conversation,
                crate::db::entities::JobEdit::CompactionPolicy(Some(policy)),
            )?;
        }
        let projects = self.step(|tx| {
            let mut changes = Vec::new();
            for entry in tx.open_table(PROJECTS)?.iter()? {
                let (key, bytes) = entry?;
                let project = decode_project(key.value(), bytes.value())?;
                if project
                    .decision
                    .as_ref()
                    .and_then(|policy| policy.decision_profile)
                    .is_some_and(|profile| ids.contains(&profile) || emptied.contains(&profile))
                {
                    changes.push(project);
                }
            }
            Ok(changes)
        })?;
        for project in projects {
            let mut policy = project.decision_policy();
            policy.decision_profile = None;
            crate::db::mutations::configure_project_decision(
                self,
                project.id,
                project.updated_at,
                Some(policy),
            )?;
        }
        let mut settings =
            self.step(|tx| reads::catalog::global_compaction_settings(&tx.open_table(META)?))?;
        let before = settings.policy.clone();
        clear_policy(&mut settings.policy);
        if settings.policy != before {
            self.set_global_compaction_settings(settings)?;
        }
        for mut row in rows {
            row.archived_at = Some(self.now);
            row.updated_at = self.now;
            row.edit_seq += 1;
            self.put_config(&row)?;
        }
        self.purge_unused_credentials()?;
        Ok(())
    }

    pub(crate) fn remove_provider(&mut self, id: ConfigId) -> Result<(), BatchError> {
        self.set_config_enabled(id, ConfigKind::Connection, false)?;
        let models = self.step(|tx| {
            let mut models = Vec::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none()
                    && matches!(&row.body, ConfigBody::Model(model) if model.connection_id == id)
                {
                    models.push(row);
                }
            }
            Ok(models)
        })?;
        self.archive_models(models)
    }
}

impl Batch {
    pub(crate) fn global_headers(
        &mut self,
    ) -> Result<crate::db::entities::GlobalHeaders, BatchError> {
        self.step(|tx| reads::catalog::global_outbound_headers(&tx.open_table(META)?))
    }

    pub(crate) fn save_global_headers(
        &mut self,
        expected_revision: u32,
        fields: Vec<crate::db::entities::OutboundHeader>,
    ) -> Result<crate::db::entities::GlobalHeaders, BatchError> {
        use crate::db::entities::{HeaderScope, validate_fields};
        let current = self.global_headers()?;
        if current.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "revision",
                "global headers changed; reload before saving",
            ));
        }
        if let Err(error) = validate_fields(&fields, HeaderScope::Global) {
            return self.reject(Violation::InvalidValue("headers", error.reason()));
        }
        let Some(revision) = current.revision.checked_add(1) else {
            return self.corrupted("global header revision exhausted".into());
        };
        let saved = crate::db::entities::GlobalHeaders { revision, fields };
        self.step(|tx| {
            let mut e = Enc::new(TAG_GLOBAL_HEADERS);
            e.u32(saved.revision);
            e.outbound_headers(&saved.fields);
            tx.open_table(META)?
                .insert("outbound_headers", e.0.as_slice())?;
            Ok(())
        })?;
        Ok(saved)
    }

    pub(crate) fn freeze_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenConfiguration, BatchError> {
        if configuration.agent_profile.is_none() {
            return self.reject(Violation::InvalidValue(
                "agent_profile_required",
                "name an Agent that lists this Model",
            ));
        }
        let global = self.global_headers()?;
        crate::service::jobs::ConfigurationResolver::new(|id| {
            self.config(id)
                .map(|row| row.map(reads::catalog::config_record))
        })
        .with_global_headers(global)
        .freeze_configuration(configuration)
    }

    pub(crate) fn freeze_system_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenConfiguration, BatchError> {
        let global = self.global_headers()?;
        crate::service::jobs::ConfigurationResolver::new(|id| {
            self.config(id)
                .map(|row| row.map(reads::catalog::config_record))
        })
        .with_global_headers(global)
        .freeze_configuration(configuration)
    }
}
