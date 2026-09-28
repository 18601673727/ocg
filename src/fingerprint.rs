//! Versioned RFC 8785 fingerprint profiles for executable semantic inputs.
//!
//! Keep these input DTOs as the only field source for the fingerprint domains.
//! Presentation, lifecycle, identity, revision, and approval state do not enter
//! these hashes.

use crate::core_contract::{ChildPolicy, EntityRef, ExecutionPolicy, FailureClass, ProjectScope};
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ts_rs::TS;

const COMMAND_DOMAIN: &str = "OCG-FP/COMMAND/v1";
const CHANGESET_DOMAIN: &str = "OCG-FP/CHANGESET/v1";
const SPAWN_DOMAIN: &str = "OCG-FP/SPAWN/v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CommandFingerprintInputV1 {
    pub schema_version: u32,
    pub project_scope: ProjectScope,
    pub action: String,
    pub target: Option<EntityRef>,
    pub arguments: serde_json::Value,
}

impl CommandFingerprintInputV1 {
    pub fn new(
        project_scope: ProjectScope,
        action: impl Into<String>,
        target: Option<EntityRef>,
        arguments: serde_json::Value,
    ) -> Self {
        Self {
            schema_version: 1,
            project_scope,
            action: action.into(),
            target,
            arguments,
        }
    }

    pub fn fingerprint(&self) -> Result<String> {
        reject_binary_floats(&self.arguments)?;
        fingerprint(COMMAND_DOMAIN, self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ChangeSetFingerprintInputV1 {
    pub schema_version: u32,
    pub project_scope: ProjectScope,
    pub targets: Vec<serde_json::Value>,
    pub preconditions: Vec<serde_json::Value>,
    pub operations: Vec<serde_json::Value>,
}

impl ChangeSetFingerprintInputV1 {
    pub fn new(
        project_scope: ProjectScope,
        targets: Vec<serde_json::Value>,
        preconditions: Vec<serde_json::Value>,
        operations: Vec<serde_json::Value>,
    ) -> Self {
        Self {
            schema_version: 1,
            project_scope,
            targets,
            preconditions,
            operations,
        }
    }

    pub fn fingerprint(&self) -> Result<String> {
        for value in self
            .targets
            .iter()
            .chain(&self.preconditions)
            .chain(&self.operations)
        {
            reject_binary_floats(value)?;
        }
        fingerprint(CHANGESET_DOMAIN, self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SpawnChildSpecV1 {
    pub spec: serde_json::Value,
    pub execution_policy: ExecutionPolicy,
    pub dependency_refs: Vec<EntityRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SpawnFingerprintInputV1 {
    pub schema_version: u32,
    pub project_scope: ProjectScope,
    pub child_spec: SpawnChildSpecV1,
    pub child_policy: ChildPolicy,
}

impl SpawnFingerprintInputV1 {
    pub fn new(
        project_scope: ProjectScope,
        spec: serde_json::Value,
        mut execution_policy: ExecutionPolicy,
        dependency_refs: impl IntoIterator<Item = EntityRef>,
        child_policy: ChildPolicy,
    ) -> Self {
        let mut dependency_refs: Vec<_> = dependency_refs.into_iter().collect();
        dependency_refs.sort_by(|left, right| {
            entity_kind_name(left)
                .cmp(entity_kind_name(right))
                .then_with(|| left.id.cmp(&right.id))
        });
        dependency_refs.dedup();
        execution_policy
            .retry
            .retryable_failure_classes
            .sort_by_key(|class| failure_class_name(*class));
        execution_policy.retry.retryable_failure_classes.dedup();
        Self {
            schema_version: 1,
            project_scope,
            child_spec: SpawnChildSpecV1 {
                spec,
                execution_policy,
                dependency_refs,
            },
            child_policy,
        }
    }

    pub fn fingerprint(&self) -> Result<String> {
        reject_binary_floats(&self.child_spec.spec)?;
        fingerprint(SPAWN_DOMAIN, self)
    }
}

fn entity_kind_name(reference: &EntityRef) -> &'static str {
    use crate::core_contract::EntityKind::*;
    match reference.kind {
        WorkNode => "work_node",
        Run => "run",
        Call => "call",
        Command => "command",
        Approval => "approval",
        Artifact => "artifact",
        ChangeSet => "change_set",
        Conversation => "conversation",
        Message => "message",
        Fact => "fact",
        BudgetScope => "budget_scope",
        CapabilityRevocation => "capability_revocation",
    }
}

fn failure_class_name(class: FailureClass) -> &'static str {
    match class {
        FailureClass::Validation => "validation",
        FailureClass::Authentication => "authentication",
        FailureClass::Authorization => "authorization",
        FailureClass::Conflict => "conflict",
        FailureClass::Concurrency => "concurrency",
        FailureClass::NotFound => "not_found",
        FailureClass::Sandbox => "sandbox",
        FailureClass::Capability => "capability",
        FailureClass::Provider => "provider",
        FailureClass::Budget => "budget",
        FailureClass::ResourceLimit => "resource_limit",
        FailureClass::RateLimit => "rate_limit",
        FailureClass::Timeout => "timeout",
        FailureClass::Cancelled => "cancelled",
        FailureClass::Preempted => "preempted",
        FailureClass::Internal => "internal",
        FailureClass::Unknown => "unknown",
    }
}

fn reject_binary_floats(value: &serde_json::Value) -> Result<()> {
    match value {
        serde_json::Value::Number(number) if number.is_f64() => Err(OcgError::config(
            "fingerprint semantic values must encode decimals as canonical strings, not binary floats",
        )),
        serde_json::Value::Array(values) => {
            for value in values {
                reject_binary_floats(value)?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                reject_binary_floats(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn fingerprint<T: Serialize>(domain: &str, input: &T) -> Result<String> {
    let canonical = serde_json_canonicalizer::to_vec(input)
        .map_err(|error| OcgError::config(format!("canonical fingerprint input: {error}")))?;
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(canonical);
    Ok(hex(&digest.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_contract::{
        ChildCancellationPolicy, ChildFailurePolicy, ChildJoinPolicy, ExecutionMode,
        ExecutionRetryPolicy, FailureClass,
    };
    use serde_json::json;

    fn scope() -> ProjectScope {
        ProjectScope::new("project-a").unwrap()
    }

    fn ref_for(id: &str) -> EntityRef {
        EntityRef {
            kind: crate::core_contract::EntityKind::WorkNode,
            id: crate::core_contract::EntityId::new(id).unwrap(),
        }
    }

    fn execution_policy() -> ExecutionPolicy {
        ExecutionPolicy {
            schema_version: 1,
            mode: ExecutionMode::SingleShot,
            retry: ExecutionRetryPolicy {
                max_attempts_per_phase: 2,
                retryable_failure_classes: vec![FailureClass::Provider],
            },
        }
    }

    fn child_policy() -> ChildPolicy {
        ChildPolicy {
            join: ChildJoinPolicy::Required,
            cancellation: ChildCancellationPolicy::Cascade,
            failure: ChildFailurePolicy::BlockParent,
            grace_period_ms: None,
        }
    }

    #[test]
    fn command_fingerprint_matches_profile_vector() {
        let input =
            CommandFingerprintInputV1::new(scope(), "update", None, json!({"z": 1, "a": "é"}));
        assert_eq!(
            input.fingerprint().unwrap(),
            "7e1831114fcad22f89f3c5729df04d223fb7f044b0deaa106b1e08e0e66ef603"
        );
    }

    #[test]
    fn command_defaults_materialize_null_and_semantic_changes_change_hash() {
        let absent = CommandFingerprintInputV1::new(scope(), "post", None, json!({}));
        let normalized = serde_json::to_value(&absent).unwrap();
        assert_eq!(normalized["target"], serde_json::Value::Null);
        let explicit_null_target: Option<EntityRef> =
            serde_json::from_value(serde_json::Value::Null).unwrap();
        let explicit_null = CommandFingerprintInputV1 {
            target: explicit_null_target,
            ..absent.clone()
        };
        assert_eq!(
            absent.fingerprint().unwrap(),
            explicit_null.fingerprint().unwrap()
        );
        let changed = CommandFingerprintInputV1::new(scope(), "post", None, json!({"body":"x"}));
        assert_ne!(
            absent.fingerprint().unwrap(),
            changed.fingerprint().unwrap()
        );
    }

    #[test]
    fn spawn_dependency_order_and_duplicates_do_not_change_hash() {
        let a = ref_for("018f1f44-2a8b-7abc-8def-0123456789ab");
        let b = ref_for("018f1f44-2a8b-7abc-8def-0123456789ac");
        let one = SpawnFingerprintInputV1::new(
            scope(),
            json!({"title":"child"}),
            execution_policy(),
            [a.clone(), b.clone()],
            child_policy(),
        );
        let two = SpawnFingerprintInputV1::new(
            scope(),
            json!({"title":"child"}),
            execution_policy(),
            [b, a.clone(), a],
            child_policy(),
        );
        assert_eq!(one.fingerprint().unwrap(), two.fingerprint().unwrap());
    }

    #[test]
    fn changeset_operation_order_is_semantic() {
        let first = ChangeSetFingerprintInputV1::new(
            scope(),
            vec![],
            vec![],
            vec![json!({"op":"a"}), json!({"op":"b"})],
        );
        let reversed = ChangeSetFingerprintInputV1::new(
            scope(),
            vec![],
            vec![],
            vec![json!({"op":"b"}), json!({"op":"a"})],
        );
        assert_ne!(
            first.fingerprint().unwrap(),
            reversed.fingerprint().unwrap()
        );
        assert_eq!(
            first.fingerprint().unwrap(),
            "644b4d9f9960cb9fc977809ca46bf44651a082648b90cc212254497dc250b9fa"
        );
    }

    #[test]
    fn spawn_fingerprint_matches_profile_vector() {
        let input = SpawnFingerprintInputV1::new(
            scope(),
            json!({"title":"café","cost":"0.10"}),
            execution_policy(),
            [],
            child_policy(),
        );
        assert_eq!(
            input.fingerprint().unwrap(),
            "d4e9d4cbad948f57b66f17274c910eeba9a5183e09aee563ca181dc7b4db6c4f"
        );
    }

    #[test]
    fn semantic_float_values_are_rejected() {
        let input = CommandFingerprintInputV1::new(scope(), "set", None, json!({"ratio": 1.25}));
        assert!(input.fingerprint().is_err());
        let decimal_string =
            CommandFingerprintInputV1::new(scope(), "set", None, json!({"ratio": "1.25"}));
        assert!(decimal_string.fingerprint().is_ok());
    }
}
