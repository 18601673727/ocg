use super::super::wire::{
    DATABASE_BUSY, DATABASE_UNAVAILABLE, INTERNAL, INVALID_REQUEST, Refusal, SHUTTING_DOWN,
};
use super::headers::INVALID_HEADERS;
use crate::db::DatabaseError;
use crate::db::entities::{
    ConfigId, ConfigRecord, CredentialSecret, ProfileId, RecordRef, Violation,
};
use crate::db::mutations::ProfileCredentialRef;
use ntex::http::StatusCode;
use std::collections::BTreeSet;

pub(super) const MAX_PROFILE_NAME_BYTES: usize = 128;
pub(super) const MAX_MODEL_BYTES: usize = 256;
pub(super) const MAX_ENDPOINT_BYTES: usize = 512;
const MAX_CREDENTIAL_REF_BYTES: usize = 128;
pub(super) const MAX_PROVIDER_MESSAGE_BYTES: usize = 1024;
const MAX_INSTRUCTIONS_BYTES: usize = 65_536;

pub(super) const INVALID_PROFILE_BODY: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_body",
    "the request body is not a valid Profile configuration payload",
);
pub(super) const PROFILE_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::NOT_FOUND,
    "profile_not_found",
    "the named Profile does not exist",
);
pub(super) const INVALID_PROFILE_NAME: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_profile_name",
    "Profile name must be 1..=128 non-control characters",
);
pub(super) const INVALID_MODEL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_model",
    "model identifier must be 1..=256 non-control characters",
);
pub(super) const UNSUPPORTED_PROTOCOL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "unsupported_protocol",
    "protocol must be openai_chat_completions, openai_responses or anthropic_messages",
);
pub(super) const INVALID_ENDPOINT: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_endpoint",
    "endpoint must use https:// (or http:// on localhost, 127.0.0.1 or ::1), at most 512 characters without whitespace",
);
pub(super) const INVALID_CREDENTIAL_REF: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_credential_ref",
    "credential_ref must be 1..=128 ASCII letters, digits, '_', '-' or '.'",
);
pub(super) const INVALID_CREDENTIAL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_credential",
    "credential secret must be 1..=4096 bytes and a valid HTTP Bearer token",
);
pub(super) const INVALID_CAPABILITIES: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_capabilities",
    "declared_capabilities must include at least one of chat, tools, vision, reasoning",
);
pub(super) const INVALID_PARAMETERS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_parameters",
    "LLM parameters must be finite numbers within valid bounds (temperature 0..=2, top_p 0..=1, one token limit >= 1, reasoning_effort supported by the wire, penalties -2..=2)",
);
const INVALID_INSTRUCTIONS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_instructions",
    "Agent instructions must be at most 65536 bytes without control characters",
);
pub(super) const MISSING_LLM_TARGETS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "missing_llm_targets",
    "an Agent Profile must select at least one target LLM Profile",
);
pub(super) const INVALID_LLM_TARGET: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_llm_target",
    "every llm_target must be an existing LLM Profile without duplicates",
);
pub(super) const INVALID_AGENT_CAPABILITIES: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_agent_capabilities",
    "select chat, coding or decision capabilities",
);
pub(super) const UNSUPPORTED_DECISION_PROTOCOL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "unsupported_decision_protocol",
    "decision protocol must be typesafe_systemone",
);
pub(super) const INVALID_DECISION_CAPABILITIES: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_capabilities",
    "declared_capabilities must include at least one of choice, ordinal_score, binary_probability",
);
pub(super) const INVALID_DECISION_LIMITS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_limits",
    "decision limits must be between 1 and 1000000, and a per-question limit cannot exceed its per-request limit",
);
pub(super) const INVALID_DECISION_MODELS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_models",
    "every decision target must be an existing model from Models without duplicates",
);
pub(super) const MISSING_DECISION_MODELS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "missing_decision_models",
    "a decision configuration must select at least one model from Models",
);

pub(super) const WRONG_PROFILE_KIND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "wrong_profile_kind",
    "a Profile's kind cannot be changed after creation",
);
const PROFILE_CHANGED: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "profile_changed",
    "the Profile changed while it was being validated; reload and retry",
);
pub(super) const PROFILE_REVISION_REQUIRED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "profile_revision_required",
    "an update must name the revision it was made from (expected_edit_seq)",
);
const PROFILE_REVISION_CHANGED: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "profile_revision_changed",
    "the Profile was changed elsewhere; reload it and apply the edit again",
);
const STORAGE_REFUSED: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "storage_refused",
    "storage refused to commit the Profile change",
);

pub(crate) fn profile_id(text: &str) -> Result<ProfileId, Refusal> {
    crate::db::entities::parse_entity_id(text)
        .map(ProfileId::new)
        .ok_or(INVALID_REQUEST)
}

pub(super) fn valid_single_line(text: &str, max_bytes: usize) -> bool {
    let trimmed = text.trim();
    !trimmed.is_empty() && trimmed.len() <= max_bytes && !trimmed.chars().any(char::is_control)
}

fn valid_multiline(text: &str, max_bytes: usize) -> bool {
    text.len() <= max_bytes
        && !text
            .chars()
            .any(|ch| ch.is_control() && !matches!(ch, '\t' | '\n' | '\r'))
}

pub(super) fn normalize_endpoint(endpoint: Option<&str>) -> Result<Option<String>, Refusal> {
    match endpoint.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(url) => {
            if url.len() > MAX_ENDPOINT_BYTES
                || crate::ai::providers::model_endpoint(url).is_err()
                || url.chars().any(|ch| ch.is_whitespace() || ch.is_control())
            {
                return Err(INVALID_ENDPOINT);
            }
            Ok(Some(url.to_owned()))
        }
    }
}

pub(super) fn normalize_instructions(
    instructions: Option<&str>,
) -> Result<Option<String>, Refusal> {
    match instructions
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        None => Ok(None),
        Some(text) if valid_multiline(text, MAX_INSTRUCTIONS_BYTES) => Ok(Some(text.to_owned())),
        Some(_) => Err(INVALID_INSTRUCTIONS),
    }
}

pub(super) fn valid_credential_ref(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= MAX_CREDENTIAL_REF_BYTES
        && reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

pub(super) fn default_credential_ref(name: &str) -> String {
    let mut slug = String::from("OCG_PROFILE_");
    for ch in name.chars() {
        if slug.len() >= 48 {
            break;
        }
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_uppercase());
        } else if !slug.ends_with('_') {
            slug.push('_');
        }
    }
    while slug.ends_with('_') {
        slug.pop();
    }
    if slug == "OCG_PROFILE" {
        slug.push_str("_DEFAULT");
    }
    slug.push_str("_API_KEY");
    slug
}

pub(crate) fn map_mutation_error(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::InvalidValue("executor", reason)) => {
            Refusal::new(StatusCode::CONFLICT, "executor_unavailable", reason)
        }
        DatabaseError::Rejected(
            reason @ Violation::InvalidValue(
                "reasoning_effort" | "presence_penalty" | "frequency_penalty" | "temperature",
                _,
            ),
        ) => INVALID_PARAMETERS.with_reason(reason.to_string()),
        DatabaseError::Rejected(Violation::NotFound(RecordRef::Profile(_))) => PROFILE_NOT_FOUND,
        DatabaseError::Rejected(Violation::WrongConfigKind { .. }) => WRONG_PROFILE_KIND,
        DatabaseError::Rejected(reason @ Violation::ConnectionHeaders { .. }) => {
            INVALID_HEADERS.with_reason(reason.to_string())
        }
        DatabaseError::Rejected(Violation::ProfileChanged) => PROFILE_CHANGED,
        DatabaseError::Rejected(Violation::ProfileArchiveRefused(reason)) => Refusal::new(
            StatusCode::CONFLICT,
            "model_archive_refused",
            "A configuration that depends on these profiles prevents archiving them.",
        )
        .with_reason(reason),
        DatabaseError::Rejected(Violation::DuplicateReference(_)) => INVALID_LLM_TARGET,
        DatabaseError::QueueFull | DatabaseError::ReadQueueFull => DATABASE_BUSY,
        DatabaseError::ShuttingDown | DatabaseError::ReadsClosed => SHUTTING_DOWN,
        DatabaseError::DiskProtected(_)
        | DatabaseError::RefusedByDiskProtection { .. }
        | DatabaseError::InsufficientSpace { .. }
        | DatabaseError::RolledBack(_)
        | DatabaseError::NeverExecuted { .. } => STORAGE_REFUSED,
        DatabaseError::ReadsStorageFailed(_)
        | DatabaseError::ReadInterrupted
        | DatabaseError::ReadPoolStopped
        | DatabaseError::Read(_) => DATABASE_UNAVAILABLE,
        _ => INTERNAL,
    }
}

pub(super) fn config_revision(
    current: u32,
    expected_edit_seq: Option<u32>,
) -> Result<Option<u32>, Refusal> {
    match expected_edit_seq {
        None => Err(PROFILE_REVISION_REQUIRED),
        Some(expected) if expected != current => Err(PROFILE_REVISION_CHANGED),
        Some(expected) => Ok(Some(expected)),
    }
}

pub(super) fn revision_guard(
    previous: Option<crate::db::mutations::ProfileSaveGuard>,
    expected_edit_seq: Option<u32>,
) -> Result<Option<crate::db::mutations::ProfileSaveGuard>, Refusal> {
    let Some(guard) = previous else {
        return Ok(None);
    };
    match expected_edit_seq {
        None => Err(PROFILE_REVISION_REQUIRED),
        Some(expected) if expected != guard.profile_edit_seq => Err(PROFILE_REVISION_CHANGED),
        Some(_) => Ok(Some(guard)),
    }
}

pub(super) fn resolve_credential_input(
    profile_name: &str,
    credential_ref: Option<&str>,
    credential: Option<&str>,
    clear_credential: bool,
) -> Result<(ProfileCredentialRef, Option<CredentialSecret>), Refusal> {
    let explicit_ref = credential_ref
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if let Some(reference) = explicit_ref.as_deref()
        && !valid_credential_ref(reference)
    {
        return Err(INVALID_CREDENTIAL_REF);
    }
    let secret = credential.map(str::trim).filter(|value| !value.is_empty());
    if let Some(secret_value) = secret
        && (secret_value.len() > 4096
            || ntex::http::header::HeaderValue::try_from(format!("Bearer {secret_value}")).is_err())
    {
        return Err(INVALID_CREDENTIAL);
    }
    if clear_credential {
        return Ok((ProfileCredentialRef::Clear, None));
    }
    match secret {
        Some(secret_value) => {
            let credential_ref = match explicit_ref {
                Some(reference) => ProfileCredentialRef::Explicit(reference),
                None => {
                    ProfileCredentialRef::PreserveOrGenerate(default_credential_ref(profile_name))
                }
            };
            Ok((
                credential_ref,
                Some(
                    CredentialSecret::new(secret_value.to_owned())
                        .map_err(|_| INVALID_CREDENTIAL)?,
                ),
            ))
        }
        None => Ok((
            explicit_ref.map_or(
                ProfileCredentialRef::Preserve,
                ProfileCredentialRef::Explicit,
            ),
            None,
        )),
    }
}

pub(super) fn parse_capability_set<T: Ord>(
    items: &[String],
    parse: fn(&str) -> Option<T>,
    err: Refusal,
) -> Result<BTreeSet<T>, Refusal> {
    let mut set = BTreeSet::new();
    for item in items {
        set.insert(parse(item.trim()).ok_or_else(|| err.clone())?);
    }
    if set.is_empty() {
        return Err(err);
    }
    Ok(set)
}

pub(super) fn valid_secret_header(secret: &str) -> bool {
    !secret.is_empty()
        && secret.len() <= 4096
        && ntex::http::header::HeaderValue::try_from(format!("Bearer {secret}")).is_ok()
}

pub(super) async fn parse_profile_refs(
    database: &crate::db::DatabaseHandle,
    raw_ids: &[String],
    invalid: Refusal,
    accept: impl Fn(&ConfigRecord) -> bool,
) -> Result<Vec<ConfigId>, Refusal> {
    let mut parsed = Vec::with_capacity(raw_ids.len());
    for raw in raw_ids {
        let id = crate::db::entities::parse_entity_id(raw.trim())
            .map(ConfigId::new)
            .ok_or_else(|| invalid.clone())?;
        if parsed.contains(&id) {
            return Err(invalid);
        }
        match database.config(id).await? {
            Some(record) if accept(&record) => parsed.push(id),
            _ => return Err(invalid),
        }
    }
    Ok(parsed)
}
