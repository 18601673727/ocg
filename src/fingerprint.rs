//! Versioned RFC 8785 fingerprint profiles for executable semantic inputs.
//!
//! Keep these input DTOs as the only field source for the fingerprint domains.
//! Presentation, lifecycle, identity, revision, and approval state do not enter
//! these hashes.

use crate::core_contract::{EntityRef, ProjectScope};
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ts_rs::TS;

const COMMAND_DOMAIN: &str = "OCG-FP/COMMAND/v1";
const CHANGESET_DOMAIN: &str = "OCG-FP/CHANGESET/v1";

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
