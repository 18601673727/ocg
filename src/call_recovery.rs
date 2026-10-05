//! Call Recovery: typed dispatch failures and deterministic recovery.
//!
//! A provider emits a tool call, the Runtime either dispatches it or cannot.
//! This module owns the "cannot" branch, so a Call that the Runtime can already
//! characterize never costs an LLM reasoning turn to explain.
//!
//! The layers, in order:
//!
//! ```text
//! Call
//!  ↓
//! schema validation (preflight, against the real registry schema)
//!  ↓
//! valid ─────────────────────────→ execute
//! invalid
//!  ↓
//! typed failure (CallFailure)
//!  ↓
//! deterministic recovery?      ← registry-declared bindings only
//!  ├─ yes → repair → execute
//!  └─ no
//!       ↓
//!       constrained repair?     ← regenerate the bad fields only
//!       ├─ yes → repair → execute
//!       └─ no → Agent reasoning
//! ```
//!
//! Two rules are load-bearing and are enforced structurally rather than by
//! convention:
//!
//! 1. **No value amplification.** A failure observation names fields and
//!    defects. It never re-emits the arguments that produced it, however large
//!    they were. The arguments stay in the Call record and journal, reachable by
//!    id. [`OBSERVED_VALUE_CAP`] bounds the only place a value may appear at
//!    all, and values above it are dropped rather than truncated.
//!
//! 2. **No inferred resources.** Deterministic recovery may only fill a field
//!    the registry declares as that tool's [`TargetBinding`] target, and only
//!    from a binding the Runtime itself established on an earlier Call of the
//!    same Attempt. Nothing is ever recovered from assistant free text.

use crate::error::Result;
use crate::native_tools::{NameMatch, NativeToolDefinition, NativeToolRegistry};
use crate::orchestration::domain::Call;
use serde_json::{Map, Value};

/// Largest offending value a failure observation may quote.
///
/// A value above this is omitted entirely rather than truncated: a prefix of a
/// source file is not evidence and is pure context cost.
pub const OBSERVED_VALUE_CAP: usize = 48;

/// Largest failure observation, in bytes, regardless of how many fields failed.
pub const FAILURE_OBSERVATION_CAP: usize = 512;

/// Largest number of field names one observation will enumerate.
const MAX_LISTED_FIELDS: usize = 8;

/// What is wrong with one argument field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgumentDefect {
    /// A required field is absent, or present as `null`.
    Missing,
    /// The field is not declared by the tool schema.
    Unexpected,
    /// The field is present but has the wrong type.
    WrongType { expected: String },
    /// The field is present, correctly typed, and violates a schema constraint.
    Constraint { constraint: String },
}

impl ArgumentDefect {
    fn detail(&self) -> String {
        match self {
            Self::Missing => "absent".to_string(),
            Self::Unexpected => "not declared by this tool".to_string(),
            Self::WrongType { expected } => format!("expected {expected}"),
            Self::Constraint { constraint } => constraint.clone(),
        }
    }
}

/// One field-level defect, in a form that is safe to put in front of a model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldIssue {
    /// The top-level argument field, without any instance path prefix.
    pub field: String,
    /// The instance path, for fields nested inside arrays or objects.
    pub instance_path: String,
    pub defect: ArgumentDefect,
    /// The offending value, only when it is small enough to be evidence.
    ///
    /// `None` means the value was omitted on purpose. A missing `oldString` must
    /// not drag a 40 KB replacement body into the failure report.
    pub observed: Option<String>,
}

impl FieldIssue {
    fn describe(&self) -> String {
        match &self.observed {
            Some(observed) => {
                format!(
                    "{} ({}, saw {})",
                    self.field,
                    self.defect.detail(),
                    observed
                )
            }
            None => format!("{} ({})", self.field, self.defect.detail()),
        }
    }
}

/// A schema preflight failure, split by what the model would have to do about
/// it. This is the analogue of `InvalidArguments { missing, invalid,
/// unexpected }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidArguments {
    pub tool: String,
    /// Required fields the model must supply.
    pub missing: Vec<FieldIssue>,
    /// Present fields whose value the model must replace.
    pub invalid: Vec<FieldIssue>,
    /// Fields the tool does not declare. These are dropped, not repaired.
    pub unexpected: Vec<FieldIssue>,
}

impl InvalidArguments {
    /// Fields only the model can supply: missing ones, plus invalid ones it
    /// must replace. Unexpected fields are excluded because the Runtime knows
    /// to drop them.
    pub fn repairable(&self) -> Vec<&FieldIssue> {
        self.missing.iter().chain(self.invalid.iter()).collect()
    }

    /// The repairable field names, deduplicated and ordered as reported.
    pub fn repairable_fields(&self) -> Vec<String> {
        let mut seen: Vec<String> = Vec::new();
        for issue in self.repairable() {
            if !seen.iter().any(|name| name == &issue.field) {
                seen.push(issue.field.clone());
            }
        }
        seen
    }
}

/// Why a Call could not be dispatched.
///
/// Every variant is decidable from Runtime state alone. None of them requires
/// reading the conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallFailure {
    /// The requested tool does not resolve to exactly one registry entry.
    UnknownTool {
        requested: String,
        /// Candidate canonical names, offered when resolution was ambiguous.
        candidates: Vec<String>,
    },
    /// The arguments were not a JSON object, so no schema applies to them.
    MalformedArguments { tool: String, reason: String },
    /// The arguments are an object but violate the tool schema.
    InvalidArguments(InvalidArguments),
}

impl CallFailure {
    /// Stable machine-readable classification.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::UnknownTool { .. } => "unknown_tool",
            Self::MalformedArguments { .. } => "malformed_arguments",
            Self::InvalidArguments(_) => "invalid_arguments",
        }
    }

    /// The tool this failure is about, as the provider named it.
    pub fn tool(&self) -> &str {
        match self {
            Self::UnknownTool { requested, .. } => requested,
            Self::MalformedArguments { tool, .. } => tool,
            Self::InvalidArguments(invalid) => &invalid.tool,
        }
    }

    /// Whether the Runtime can plausibly resolve this without a new reasoning
    /// turn, i.e. whether it is a candidate for a constrained repair rather
    /// than a full re-reason.
    pub fn is_field_scoped(&self) -> bool {
        match self {
            Self::UnknownTool { .. } => false,
            Self::MalformedArguments { .. } => false,
            Self::InvalidArguments(_) => true,
        }
    }

    /// The compact observation handed back to the model.
    ///
    /// This is the whole point of the module: it is bounded, it names only what
    /// must change, and it deliberately omits every value the model already
    /// produced.
    pub fn compact_observation(&self, reference: &str) -> String {
        let mut lines = vec![format!("Call failed: {}", self.kind())];
        lines.push(format!("tool: {}", self.tool()));
        match self {
            Self::UnknownTool { candidates, .. } => {
                if !candidates.is_empty() {
                    lines.push(format!("candidates: {}", joined(candidates)));
                } else {
                    let available: Vec<String> = NativeToolRegistry::names()
                        .into_iter()
                        .map(str::to_string)
                        .collect();
                    lines.push(format!("available: {}", joined(&available)));
                }
            }
            Self::MalformedArguments { reason, .. } => lines.push(format!("reason: {reason}")),
            Self::InvalidArguments(invalid) => {
                for (label, issues) in [
                    ("missing", &invalid.missing),
                    ("invalid", &invalid.invalid),
                    ("unexpected", &invalid.unexpected),
                ] {
                    if issues.is_empty() {
                        continue;
                    }
                    let names: Vec<String> = issues
                        .iter()
                        .take(MAX_LISTED_FIELDS)
                        .map(FieldIssue::describe)
                        .collect();
                    let overflow = issues.len().saturating_sub(MAX_LISTED_FIELDS);
                    lines.push(format!(
                        "{label}: {}{}",
                        joined(&names),
                        if overflow > 0 {
                            format!(" (+{overflow} more)")
                        } else {
                            String::new()
                        }
                    ));
                }
            }
        }
        lines.push(format!(
            "arguments retained under {reference}; not repeated here."
        ));
        bounded(&lines.join("\n"))
    }
}

/// One deterministic repair the Runtime applied, with its provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Repair {
    /// A requested name resolved to a registry entry.
    ToolResolved {
        requested: String,
        canonical: String,
        confidence: NameMatch,
    },
    /// An undeclared argument field was renamed to the canonical one.
    ArgumentRenamed {
        tool: String,
        from: String,
        to: String,
    },
    /// A missing target field was filled from a binding the Runtime held.
    TargetRebound {
        tool: String,
        field: String,
        value: String,
        /// The earlier Call whose admitted arguments established the binding.
        from_call: String,
    },
}

/// The minimum request that repairs only the fields the model got wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstrainedRepair {
    pub tool: String,
    /// Only these fields are asked for again.
    pub fields: Vec<FieldIssue>,
    /// The Call being repaired. Arguments stay addressable here rather than
    /// being re-sent.
    pub reference: String,
}

impl ConstrainedRepair {
    /// The repair request, as a tool message.
    ///
    /// # What this can and cannot guarantee
    ///
    /// The request names only the bad fields, and explicitly tells the model
    /// that the tool name and the other arguments are already bound, so the
    /// repair is scoped as narrowly as the wire protocol allows.
    ///
    /// It does not *enforce* that scope. The OpenAI-compatible function-calling
    /// protocol has no partial-argument primitive: a tool call is one atomic
    /// argument object, and there is no channel that accepts an argument patch
    /// for an already-emitted `tool_call_id`. A model that re-emits the whole
    /// call therefore still pays for the whole call, even though only one field
    /// was wrong.
    ///
    /// [`ConstrainedRepair`] is therefore the data model and the decision, not a
    /// saving that is already banked. The saving becomes real when a transport
    /// carries a repair channel: a request scoped to `fields`, addressed by
    /// [`ConstrainedRepair::reference`], merged into the retained arguments
    /// without the model resending them. [`RecoveryAction::ConstrainedRepair`]
    /// is the seam that transport plugs into; nothing above it changes.
    pub fn instruction(&self) -> String {
        let names: Vec<String> = self
            .fields
            .iter()
            .take(MAX_LISTED_FIELDS)
            .map(|issue| issue.describe())
            .collect();
        let overflow = self.fields.len().saturating_sub(MAX_LISTED_FIELDS);
        let mut text = format!(
            "Call rejected: InvalidArguments\ntool: {}\nrepair: {}{}\n\
             Send only the fields listed above. Do not repeat the tool name or any \
             other argument: they are already bound to {}.\n",
            self.tool,
            joined(&names),
            if overflow > 0 {
                format!(" (+{overflow} more)")
            } else {
                String::new()
            },
            self.reference,
        );
        text.push_str(&format!(
            "Tool schema: {}",
            schema_digest(&NativeToolRegistry::get(&self.tool))
        ));
        bounded(&text)
    }
}

/// What recovery decided to do with a Call.
#[derive(Debug, Clone)]
pub enum RecoveryAction {
    /// Dispatch this, with any deterministic repairs already applied.
    Dispatch {
        definition: NativeToolDefinition,
        arguments: Value,
        repairs: Vec<Repair>,
    },
    /// Ask the model for only these fields, then dispatch the result.
    ConstrainedRepair(ConstrainedRepair),
    /// Recovery cannot proceed without new reasoning.
    NeedsReasoning(CallFailure),
}

impl RecoveryAction {
    /// Whether this action costs no new reasoning turn at all.
    ///
    /// Only a `Dispatch` with at least one repair qualifies: the Runtime settled
    /// the whole problem from state it already held.
    ///
    /// `ConstrainedRepair` is deliberately **not** free. The model still has to
    /// produce the missing fields, and the current provider protocol makes it
    /// resend the whole Call to do so. The decision narrows what is asked for;
    /// it does not by itself remove the turn. See
    /// [`ConstrainedRepair::instruction`] for what would.
    pub fn is_free(&self) -> bool {
        matches!(self, Self::Dispatch { repairs, .. } if !repairs.is_empty())
    }

    /// Whether this action avoids asking the model to re-derive the whole Call.
    pub fn is_field_scoped(&self) -> bool {
        matches!(self, Self::ConstrainedRepair(_))
    }
}

/// A resource a tool is bound to, established by an admitted Call.
///
/// This is the only source deterministic argument recovery may draw on. It is
/// structured Runtime state: the earlier Call was admitted, validated and
/// journaled, so its arguments are a fact rather than a guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetBinding {
    pub tool: &'static str,
    pub field: &'static str,
    pub value: String,
    /// The Call that established this binding, for provenance.
    pub call_id: String,
}

/// Every target the Runtime has bound during one Attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetBindings {
    bindings: Vec<TargetBinding>,
}

impl TargetBindings {
    /// Harvest bindings from Calls the Runtime already admitted.
    ///
    /// Only Calls that reached `completed` count, and only for the field the
    /// registry declares as that tool's target. A Call that failed may have
    /// named a path that did not work, and a field that is not a target says
    /// nothing about which resource was meant.
    pub fn from_calls(calls: &[Call]) -> Self {
        let mut bindings = Vec::new();
        for call in calls {
            if call.state != "completed" {
                continue;
            }
            let Ok(payload) = serde_json::from_str::<Value>(&call.request) else {
                continue;
            };
            if payload.get("kind").and_then(Value::as_str) != Some("native_tool") {
                continue;
            }
            let Some(name) = payload.get("name").and_then(Value::as_str) else {
                continue;
            };
            let Some(definition) = NativeToolRegistry::get(name) else {
                continue;
            };
            let Some(field) = definition.target_field else {
                continue;
            };
            let Some(value) = payload
                .get("arguments")
                .and_then(|arguments| arguments.get(field))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if value.is_empty() {
                continue;
            }
            let already = bindings.iter().any(|binding: &TargetBinding| {
                binding.tool == definition.name && binding.field == field && binding.value == value
            });
            if !already {
                bindings.push(TargetBinding {
                    tool: definition.name,
                    field,
                    value: value.to_string(),
                    call_id: call.id.clone(),
                });
            }
        }
        Self { bindings }
    }

    /// The single resource this tool is bound to, if exactly one is bound.
    ///
    /// Two bindings are never resolved to one. Ambiguity is reported as
    /// ambiguity, because guessing here is precisely the blind `path`
    /// inference this module exists to prevent.
    pub fn unique_target(&self, tool: &str, field: &str) -> Option<&TargetBinding> {
        let matches: Vec<&TargetBinding> = self
            .bindings
            .iter()
            .filter(|binding| binding.tool == tool && binding.field == field)
            .collect();
        if matches.len() == 1 {
            Some(matches[0])
        } else {
            None
        }
    }
}

/// Resolve a requested tool name against the registry.
///
/// Returns `None` unless the registry names exactly one entry for the request.
/// Ambiguity is never broken here; it is reported so the model can choose.
pub fn resolve_tool(requested: &str) -> Option<(NativeToolDefinition, NameMatch)> {
    let (definition, match_kind) = NativeToolRegistry::get_by_alias(requested)?;
    // An alias shared by two tools would make this silently lossy. The registry
    // is the owner of aliases, so this is the point where that ownership is
    // checked rather than assumed.
    let contenders = NativeToolRegistry::definitions()
        .into_iter()
        .filter(|tool| tool.aliases.contains(&requested))
        .count();
    if contenders > 1 {
        return None;
    }
    Some((definition, match_kind))
}

/// Fold case and separators, so `FileSystem.Edit` and `filesystem_edit` are one
/// name rather than two. Symmetric and total: it never invents a name.
fn normalize(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_lowercase())
        .collect()
}

/// Canonical tool names that plausibly match a request, for the ambiguous case.
pub fn tool_candidates(requested: &str) -> Vec<String> {
    let normalized = normalize(requested);
    NativeToolRegistry::definitions()
        .into_iter()
        .filter(|tool| {
            normalize(tool.name).contains(&normalized)
                || tool
                    .aliases
                    .iter()
                    .any(|alias| normalize(alias) == normalized)
        })
        .map(|tool| tool.name.to_string())
        .collect()
}

/// Validate arguments against a tool's real schema before dispatch.
///
/// Returns `None` when the arguments are dispatchable. The failure it returns
/// is classified, and no offending value larger than [`OBSERVED_VALUE_CAP`]
/// survives classification.
pub fn preflight(
    definition: &NativeToolDefinition,
    arguments: &Value,
) -> Result<Option<CallFailure>> {
    if arguments.as_object().is_none() {
        return Ok(Some(CallFailure::MalformedArguments {
            tool: definition.name.to_string(),
            reason: "arguments must be a JSON object".to_string(),
        }));
    };
    let validator = jsonschema::options()
        .build(&definition.parameters)
        .map_err(|error| {
            crate::error::OcgError::config(format!(
                "compile native tool schema for '{}': {error}",
                definition.name
            ))
        })?;
    // Every error is collected, not just the first: a Call missing `file` and
    // carrying a wrongly-typed `offset` should cost one observation, not two
    // round trips that each reveal one more field.
    let errors: Vec<jsonschema::ValidationError<'_>> = validator.iter_errors(arguments).collect();
    if errors.is_empty() {
        return Ok(None);
    }
    Ok(Some(CallFailure::InvalidArguments(classify(
        definition.name,
        &errors,
    ))))
}

/// Group schema errors into the three things a caller could do about them.
fn classify(tool: &str, errors: &[jsonschema::ValidationError<'_>]) -> InvalidArguments {
    let mut missing = Vec::new();
    let mut invalid = Vec::new();
    let mut unexpected = Vec::new();
    let mut described: Vec<String> = Vec::new();

    for error in errors {
        let instance_path = error.instance_path().to_string();
        let defect = defect_of(error);
        // A `required` or `additionalProperties` failure is attributed to the
        // enclosing object, not to the offending field, so its instance path is
        // the object's own. Taking the field from that path would report every
        // such failure as `<arguments>`, which names nothing the model can act
        // on. The field name lives in the error kind instead.
        let field = match named_field(error) {
            Some(name) => name,
            None => top_level_field(&instance_path),
        };
        let reported_path = if named_field(error).is_some() {
            format!("{instance_path}/{field}")
        } else {
            instance_path
        };
        // One field is reported once per defect kind. A schema failure typically
        // produces several errors per field, and repeating them helps nobody.
        let key = format!("{}|{:?}", field, defect);
        if described.contains(&key) {
            continue;
        }
        described.push(key);
        let issue = FieldIssue {
            field: field.clone(),
            instance_path: reported_path,
            observed: match defect {
                // A required field has no instance of its own; the value is
                // absent, which is exactly the defect. Reporting the enclosing
                // object here would quote the whole argument blob.
                ArgumentDefect::Missing => None,
                _ => observed_value(error.instance().as_ref()),
            },
            defect,
        };
        match issue.defect {
            ArgumentDefect::Unexpected => unexpected.push(issue),
            // A required field the model did not send belongs in `missing`.
            // Routing it to `invalid` would ask the model to replace a value it
            // never sent.
            ArgumentDefect::Missing => missing.push(issue),
            _ => invalid.push(issue),
        }
    }

    // A required field sent as `null` is absent as far as any caller is
    // concerned, and repairing it means supplying it. Reporting it as a type
    // error would push the model toward retyping a value it never sent.
    for issue in &mut invalid {
        if matches!(issue.defect, ArgumentDefect::WrongType { .. })
            && issue.observed.as_deref() == Some("null")
        {
            missing.push(FieldIssue {
                field: issue.field.clone(),
                instance_path: issue.instance_path.clone(),
                defect: ArgumentDefect::Missing,
                observed: None,
            });
        }
    }
    invalid.retain(|issue| {
        issue.observed.as_deref() != Some("null")
            || !matches!(issue.defect, ArgumentDefect::WrongType { .. })
    });

    InvalidArguments {
        tool: tool.to_string(),
        missing,
        invalid,
        unexpected,
    }
}

fn defect_of(error: &jsonschema::ValidationError<'_>) -> ArgumentDefect {
    use jsonschema::error::ValidationErrorKind as Kind;
    match error.kind() {
        Kind::Required { .. } => ArgumentDefect::Missing,
        // The offending names are named on the instance path and value, not here, so
        // a long unexpected-property list cannot bloat the observation.
        Kind::AdditionalProperties { .. } | Kind::UnevaluatedProperties { .. } => {
            ArgumentDefect::Unexpected
        }
        Kind::Type { kind } => ArgumentDefect::WrongType {
            expected: match kind {
                jsonschema::error::TypeKind::Single(name) => name.to_string(),
                jsonschema::error::TypeKind::Multiple(names) => {
                    let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
                    names.join(" or ")
                }
            },
        },
        Kind::Enum { .. } => ArgumentDefect::Constraint {
            constraint: "not an allowed value".to_string(),
        },
        Kind::Minimum { .. }
        | Kind::Maximum { .. }
        | Kind::ExclusiveMinimum { .. }
        | Kind::ExclusiveMaximum { .. }
        | Kind::MultipleOf { .. } => ArgumentDefect::Constraint {
            constraint: "out of numeric range".to_string(),
        },
        Kind::MinLength { .. } | Kind::MaxLength { .. } => ArgumentDefect::Constraint {
            constraint: "out of length range".to_string(),
        },
        Kind::MinItems { .. } | Kind::MaxItems { .. } | Kind::UniqueItems => {
            ArgumentDefect::Constraint {
                constraint: "invalid array shape".to_string(),
            }
        }
        Kind::MinProperties { .. } | Kind::MaxProperties { .. } => ArgumentDefect::Constraint {
            constraint: "invalid object shape".to_string(),
        },
        Kind::Pattern { .. } | Kind::Format { .. } => ArgumentDefect::Constraint {
            constraint: "does not match the required format".to_string(),
        },
        _ => ArgumentDefect::Constraint {
            constraint: "violates the tool schema".to_string(),
        },
    }
}

/// The field a container-level error is about, when the error names it.
///
/// `required` and `additionalProperties` are attributed to the enclosing object,
/// so the field name is only available from the error kind.
fn named_field(error: &jsonschema::ValidationError<'_>) -> Option<String> {
    use jsonschema::error::ValidationErrorKind as Kind;
    match error.kind() {
        Kind::Required { property } => property.as_str().map(str::to_string),
        Kind::AdditionalProperties { unexpected } | Kind::UnevaluatedProperties { unexpected } => {
            unexpected.first().cloned()
        }
        _ => None,
    }
}

/// The first instance-path segment, which is the argument field a caller sets.
fn top_level_field(instance_path: &str) -> String {
    instance_path
        .split('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or("<arguments>")
        .to_string()
}

/// Quote an offending value only when it is small enough to be evidence.
fn observed_value(instance: &Value) -> Option<String> {
    // An unexpected-property error carries the entire arguments object as its
    // instance. Naming its fields describes the problem without re-sending the
    // blob, which is the whole point of this function.
    if let Value::Object(fields) = instance {
        let names: Vec<&str> = fields.keys().map(String::as_str).collect();
        return Some(format!("object{{{}}}", names.join(",")));
    }
    let rendered = match instance {
        Value::String(text) => text.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    };
    if rendered.len() > OBSERVED_VALUE_CAP {
        // Dropped, not truncated: a prefix of a source file is not evidence.
        return None;
    }
    Some(rendered)
}

/// Apply deterministic recovery, then decide what still needs the model.
///
/// The order matters: recovery runs first and its result is re-preflighted, so
/// a repair that does not actually fix the Call cannot reach dispatch.
pub fn recover(
    requested: &str,
    arguments: &Value,
    bindings: &TargetBindings,
    reference: &str,
) -> RecoveryAction {
    let Some((definition, confidence)) = resolve_tool(requested) else {
        return RecoveryAction::NeedsReasoning(CallFailure::UnknownTool {
            requested: requested.to_string(),
            candidates: tool_candidates(requested),
        });
    };
    // A name that did not arrive under its canonical or wire spelling is a
    // deterministic repair even when its arguments are already valid, and it is
    // recorded so the dispatch is auditable.
    let mut repairs = Vec::new();
    if requested != definition.name {
        repairs.push(Repair::ToolResolved {
            requested: requested.to_string(),
            canonical: definition.name.to_string(),
            confidence,
        });
    }

    let Some(failure) = (match preflight(&definition, arguments) {
        Ok(failure) => failure,
        Err(_) => {
            return RecoveryAction::NeedsReasoning(CallFailure::MalformedArguments {
                tool: definition.name.to_string(),
                reason: "tool schema could not be compiled".to_string(),
            })
        }
    }) else {
        return RecoveryAction::Dispatch {
            definition,
            arguments: arguments.clone(),
            repairs,
        };
    };

    let CallFailure::InvalidArguments(invalid) = &failure else {
        return RecoveryAction::NeedsReasoning(failure);
    };

    let mut repaired = arguments.as_object().cloned().unwrap_or_else(Map::new);

    // Deterministic recovery: normalize an argument field the schema does not
    // declare to the one it does, when the registry declares the alias.
    // `additionalProperties: false` would otherwise turn a naming convention
    // into a whole reasoning turn.
    for issue in &invalid.unexpected {
        let renamed = definition
            .argument_aliases
            .iter()
            .find(|(alias, _)| *alias == issue.field)
            .map(|(_, canonical)| (*canonical).to_string());
        match renamed {
            // The canonical field must not already be set: two values for one
            // field is a conflict, not a rename.
            Some(canonical) if !repaired.contains_key(&canonical) => {
                if let Some(value) = repaired.remove(&issue.field) {
                    repairs.push(Repair::ArgumentRenamed {
                        tool: definition.name.to_string(),
                        from: issue.field.clone(),
                        to: canonical.clone(),
                    });
                    repaired.insert(canonical, value);
                }
            }
            // Nothing declares this field, so it is dropped rather than guessed
            // at. The value may still be the one the tool needs under a
            // different name, and inventing that mapping is the blind `path`
            // inference this module exists to prevent.
            _ => {
                repaired.remove(&issue.field);
            }
        }
    }

    // Deterministic recovery: fill the registry-declared target field from a
    // binding this Runtime already holds. Every other missing field is left
    // missing on purpose, so it reaches the constrained repair instead of being
    // invented here — which is why only `target_field` is a candidate, and why
    // two bound targets are never resolved to one.
    let recoverable = definition
        .target_field
        .filter(|field| invalid.missing.iter().any(|issue| issue.field == **field));
    if let Some(field) = recoverable {
        if let Some(binding) = bindings.unique_target(definition.name, field) {
            repaired.insert(field.to_string(), Value::String(binding.value.clone()));
            repairs.push(Repair::TargetRebound {
                tool: definition.name.to_string(),
                field: field.to_string(),
                value: binding.value.clone(),
                from_call: binding.call_id.clone(),
            });
        }
    }
    // A rename or rebound changes the field set, so what remains outstanding is
    // recomputed by re-validating the repaired arguments. Patching the original
    // list instead would let a repaired field survive as outstanding, and the
    // model would be asked for a value the Runtime already has.
    let repaired_arguments = Value::Object(repaired);
    match preflight(&definition, &repaired_arguments) {
        Ok(None) => RecoveryAction::Dispatch {
            definition,
            arguments: repaired_arguments,
            repairs,
        },
        Ok(Some(CallFailure::InvalidArguments(remaining))) => {
            let fields = remaining.repairable().into_iter().cloned().collect();
            RecoveryAction::ConstrainedRepair(ConstrainedRepair {
                tool: definition.name.to_string(),
                fields,
                reference: reference.to_string(),
            })
        }
        // The repaired arguments stopped being dispatchable in a way preflight
        // did not name, or the schema no longer compiles. Neither is a field the
        // model can supply, so it goes back for reasoning.
        Ok(Some(other)) => RecoveryAction::NeedsReasoning(other),
        Err(_) => RecoveryAction::NeedsReasoning(CallFailure::MalformedArguments {
            tool: definition.name.to_string(),
            reason: "tool schema could not be compiled".to_string(),
        }),
    }
}

fn joined(items: &[String]) -> String {
    items.join(", ")
}

fn bounded(text: &str) -> String {
    if text.len() <= FAILURE_OBSERVATION_CAP {
        return text.to_string();
    }
    let mut end = FAILURE_OBSERVATION_CAP;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[truncated]", &text[..end])
}

/// A one-line schema reminder for the repair request.
///
/// It lists required fields and types only. Field bodies are the context cost
/// this module exists to avoid, so no description or example is carried.
fn schema_digest(definition: &Option<NativeToolDefinition>) -> String {
    let Some(definition) = definition else {
        return "unavailable".to_string();
    };
    let schema = &definition.parameters;
    let properties = schema.get("properties").and_then(Value::as_object);
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let Some(properties) = properties else {
        return "unavailable".to_string();
    };
    properties
        .iter()
        .map(|(name, property)| {
            let kind = property
                .get("type")
                .map(|kind| match kind {
                    Value::String(name) => name.clone(),
                    Value::Array(names) => names
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|name| *name != "null")
                        .collect::<Vec<_>>()
                        .join(" or "),
                    _ => "any".to_string(),
                })
                .unwrap_or_else(|| "any".to_string());
            let marker = if required.contains(&name.as_str()) {
                "required"
            } else {
                "optional"
            };
            format!("{name}: {kind} ({marker})")
        })
        .collect::<Vec<_>>()
        .join("; ")
}
