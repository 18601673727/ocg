use super::super::contracts::{OutboundHeaderView, compaction_policy_view, envelope};
use super::super::wire::{INCONSISTENT_RECORD, Refusal, Reply};
use super::headers::header_views;
use super::validation::profile_id;
use super::{
    DecisionAgentProfileView, DecisionLimitsView, DecisionProfileView, LlmProfileView,
    ManagedAgentView, ProfileExecutorView, ProfilesView, ProviderView,
};
use crate::db::DatabaseHandle;
use crate::db::entities::{ConfigBody, ConfigId, ConnectionConfig, ExecutionOperation, ProfileId};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_PROFILE_PAGE: usize = 50;
const PROFILE_SCAN_FACTOR: usize = 4;
const MAX_PROFILE_PAGE_BYTES: usize = 512 * 1024;

pub(crate) async fn list_profiles(
    database: &DatabaseHandle,
    before: Option<ConfigId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database
        .profile_page(before, limit, limit.saturating_mul(PROFILE_SCAN_FACTOR))
        .await?;
    Ok(Reply::ok(build_profiles_page(page, None)?))
}

pub(super) async fn build_profiles_snapshot(
    database: &DatabaseHandle,
    saved_profile: Option<ProfileId>,
) -> Result<Value, Refusal> {
    let page = database
        .profile_page(
            None,
            DEFAULT_PROFILE_PAGE,
            DEFAULT_PROFILE_PAGE.saturating_mul(PROFILE_SCAN_FACTOR),
        )
        .await?;
    build_profiles_page(page, saved_profile)
}

fn has_profile_credential(credentials: &BTreeSet<String>, reference: &str) -> bool {
    let reference = reference.trim();
    !reference.is_empty()
        && (credentials.contains(reference)
            || std::env::var_os(reference).is_some_and(|value| !value.is_empty()))
}

fn connection_for_model<'a>(
    connections: &'a BTreeMap<ConfigId, (String, ConnectionConfig, bool)>,
    credentials: &BTreeSet<String>,
    connection_id: ConfigId,
) -> Result<(&'a str, &'a ConnectionConfig, bool, bool), Refusal> {
    let Some((provider_name, connection, discovery_visible)) = connections.get(&connection_id)
    else {
        return Err(INCONSISTENT_RECORD);
    };
    let has_credential = connection
        .credential_ref
        .as_deref()
        .is_some_and(|reference| has_profile_credential(credentials, reference));
    Ok((
        provider_name,
        connection,
        *discovery_visible,
        has_credential,
    ))
}

fn push_page_item<T: serde::Serialize>(
    list: &mut Vec<T>,
    item: T,
    id: ConfigId,
    page_bytes: &mut usize,
    last_emitted: &mut Option<ConfigId>,
    next_before: &mut Option<ConfigId>,
) -> Result<bool, Refusal> {
    if !fits_profile_page(&item, page_bytes)? {
        *next_before = *last_emitted;
        return Ok(false);
    }
    list.push(item);
    *last_emitted = Some(id);
    Ok(true)
}

fn build_profiles_page(
    page: crate::db::entities::ProfilePageRecords,
    saved_profile: Option<ProfileId>,
) -> Result<Value, Refusal> {
    let crate::db::entities::ProfilePageRecords {
        records,
        connections,
        credentials,
        next_before,
    } = page;
    let mut llm_profiles = Vec::new();
    let mut agent_profiles = Vec::new();
    let mut decision_profiles = Vec::new();
    let mut decision_agent_profiles = Vec::new();
    let mut executor_protocols = Vec::new();

    struct ProviderGroup {
        name: String,
        protocol: &'static str,
        endpoint: Option<String>,
        credential_ref: Option<String>,
        has_credential: bool,
        headers: Vec<OutboundHeaderView>,
        profile_ids: Vec<String>,
        models: BTreeSet<String>,
    }
    let mut provider_groups: BTreeMap<ConfigId, ProviderGroup> = BTreeMap::new();
    let mut last_emitted = None;
    let mut page_bytes = 0;
    let mut next_before = next_before;

    for record in records {
        let record = match record {
            crate::db::entities::ProfilePageRecord::Config(record) => record,
            crate::db::entities::ProfilePageRecord::Executor(executor) => {
                let offered: Vec<_> = executor
                    .capabilities
                    .iter()
                    .filter_map(|(operation, protocol)| match (operation, protocol) {
                        (ExecutionOperation::LlmRequest, Some(protocol))
                            if protocol.execution_refusal().is_none() =>
                        {
                            Some(ProfileExecutorView {
                                id: executor.id.to_string(),
                                protocol: protocol.as_str().to_owned(),
                            })
                        }
                        _ => None,
                    })
                    .collect();
                if !offered.is_empty() {
                    if !fits_profile_page(&offered, &mut page_bytes)? {
                        next_before = last_emitted;
                        break;
                    }
                    executor_protocols.extend(offered);
                    last_emitted = Some(ConfigId::new(executor.id.get()));
                }
                continue;
            }
        };
        let id = ProfileId::from(record.id);
        match record.body {
            ConfigBody::Model(model) => {
                let (provider_name, connection, discovery_visible, has_credential) =
                    connection_for_model(&connections, &credentials, model.connection_id)?;
                let protocol = connection.protocol.as_str();
                let id_str = id.to_string();
                let profile = LlmProfileView {
                    edit_seq: record.edit_seq,
                    enabled: record.model_enabled,
                    reasoning_efforts: model.reasoning_efforts,
                    id: id_str.clone(),
                    profile_type: Some("llm".to_owned()),
                    name: record.name,
                    model: model.model.clone(),
                    context_window: model.context_window,
                    protocol: protocol.to_owned(),
                    endpoint: connection.endpoint.clone(),
                    credential_ref: connection.credential_ref.clone(),
                    has_credential,
                    headers: header_views(&connection.headers, &credentials),
                    declared_capabilities: model
                        .declared_capabilities
                        .iter()
                        .map(|cap| cap.as_str().to_owned())
                        .collect(),
                    parameters: model.parameters,
                    editable: true,
                    blocking_jobs: Vec::new(),
                    more_blocking_jobs: false,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut llm_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
                if discovery_visible {
                    let group = provider_groups
                        .entry(model.connection_id)
                        .or_insert_with(|| ProviderGroup {
                            name: provider_name.to_owned(),
                            protocol,
                            endpoint: connection.endpoint.clone(),
                            credential_ref: connection.credential_ref.clone(),
                            has_credential,
                            headers: header_views(&connection.headers, &credentials),
                            profile_ids: Vec::new(),
                            models: BTreeSet::new(),
                        });
                    group.profile_ids.push(id_str);
                    group.models.insert(model.model);
                }
            }
            ConfigBody::Agent(agent) => {
                let profile = ManagedAgentView {
                    id: id.to_string(),
                    edit_seq: record.edit_seq,
                    profile_type: Some("agent".to_owned()),
                    name: record.name,
                    instructions: agent.instructions,
                    capabilities: agent.capabilities,
                    llm_targets: agent
                        .models
                        .iter()
                        .map(|id| ProfileId::from(*id).to_string())
                        .collect(),
                    compaction_policy: agent.compaction_policy.map(compaction_policy_view),
                    editable: true,
                    blocking_jobs: Vec::new(),
                    more_blocking_jobs: false,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut agent_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
            }
            ConfigBody::Connection(_) => {}
            ConfigBody::DecisionModel(model) => {
                let (_, connection, _, has_credential) =
                    connection_for_model(&connections, &credentials, model.connection_id)?;
                let profile = DecisionProfileView {
                    id: id.to_string(),
                    edit_seq: record.edit_seq,
                    name: record.name,
                    model: model.model.clone(),
                    protocol: model.protocol.as_str().to_owned(),
                    endpoint: connection.endpoint.clone(),
                    credential_ref: connection.credential_ref.clone(),
                    has_credential,
                    headers: header_views(&connection.headers, &credentials),
                    declared_capabilities: model
                        .declared_capabilities
                        .iter()
                        .map(|capability| capability.as_str().to_owned())
                        .collect(),
                    limits: DecisionLimitsView {
                        max_input_tokens_per_request: model.limits.max_input_tokens_per_request,
                        max_output_tokens_per_request: model.limits.max_output_tokens_per_request,
                        max_input_tokens_per_question: model.limits.max_input_tokens_per_question,
                        max_output_tokens_per_question: model.limits.max_output_tokens_per_question,
                    },
                    editable: true,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut decision_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
            }
            ConfigBody::DecisionAgent(agent) => {
                let profile = DecisionAgentProfileView {
                    id: id.to_string(),
                    edit_seq: record.edit_seq,
                    name: record.name,
                    instructions: agent.instructions,
                    models: agent
                        .models
                        .iter()
                        .map(|id| ProfileId::from(*id).to_string())
                        .collect(),
                    default_model: agent
                        .default_model
                        .map(|id| ProfileId::from(id).to_string()),
                    editable: true,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut decision_agent_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
            }
        }
    }
    llm_profiles.sort_by_key(|profile| {
        std::cmp::Reverse(profile_id(&profile.id).ok().map_or(0, ProfileId::get))
    });
    agent_profiles.sort_by_key(|profile| {
        std::cmp::Reverse(profile_id(&profile.id).ok().map_or(0, ProfileId::get))
    });

    let providers = provider_groups
        .into_iter()
        .map(|(key, group)| ProviderView {
            id: ProfileId::from(key).to_string(),
            name: group.name,
            protocol: group.protocol.to_owned(),
            endpoint: group.endpoint,
            credential_ref: group.credential_ref,
            has_credential: group.has_credential,
            headers: group.headers,
            profile_ids: group.profile_ids,
            models: group.models.into_iter().collect(),
        })
        .collect();

    let result = envelope(serde_json::json!(ProfilesView {
        llm_profiles,
        agent_profiles,
        decision_profiles: (!decision_profiles.is_empty()).then_some(decision_profiles),
        decision_agent_profiles: (!decision_agent_profiles.is_empty())
            .then_some(decision_agent_profiles),
        executor_protocols,
        providers,
        has_more: next_before.is_some(),
        next_before: next_before.map(|id| ProfileId::from(id).to_string()),
        profile_id: saved_profile.map(|id| id.to_string()),
    }));
    let result = {
        let mut result = result;
        if let Some(object) = result.as_object_mut() {
            object.insert("resource_model".into(), "independent".into());
        }
        result
    };
    Ok(result)
}

fn fits_profile_page(entry: &impl serde::Serialize, bytes: &mut usize) -> Result<bool, Refusal> {
    let size = serde_json::to_vec(entry).map_err(|_| INCONSISTENT_RECORD)?;
    if *bytes > 0 && *bytes + size.len() > MAX_PROFILE_PAGE_BYTES {
        return Ok(false);
    }
    *bytes += size.len();
    Ok(true)
}
