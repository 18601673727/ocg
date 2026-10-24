use super::*;

/// `result` is deliberately absent: a final result output is only published
/// by [`JobFinalization`], in the same transaction that finalizes the Job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntermediateOutputKind {
    Progress,
    Evaluation,
    Reasoning,
    Decision,
    Summarization,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputKind {
    Progress,
    Evaluation,
    Reasoning,
    Result,
    Decision,
    Summarization,
}

impl From<IntermediateOutputKind> for OutputKind {
    fn from(kind: IntermediateOutputKind) -> Self {
        match kind {
            IntermediateOutputKind::Progress => Self::Progress,
            IntermediateOutputKind::Evaluation => Self::Evaluation,
            IntermediateOutputKind::Reasoning => Self::Reasoning,
            IntermediateOutputKind::Decision => Self::Decision,
            IntermediateOutputKind::Summarization => Self::Summarization,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyCondition {
    Success,
    Settled,
}

/// How a dependency currently stands. A prerequisite is settled once it has
/// a confirmed outcome, or an unknown outcome whose execution is confirmed
/// stopped; until then the dependency waits. A completed prerequisite
/// satisfies either condition. Any other settled prerequisite satisfies a
/// settled dependency, while a success dependency on it can never be met and
/// is not a wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyState {
    Waiting,
    Satisfied,
    Impossible,
}

/// What a Job is asked to do: its objective, the Attachments it carries and
/// its limits on descendants. It becomes part of the Job's spec.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobInputDraft {
    pub objective: Option<String>,
    /// The Attachments the input carries, in order. Each must exist and must
    /// not be cleaned up; see [`AttachmentRecord`].
    pub attachments: Vec<AttachmentId>,
}

/// Every relationship here is fixed once the Job is submitted. The Project
/// is independent of `parent`: a child never inherits its parent's Project.
#[derive(Clone, Debug, PartialEq)]
pub struct NewJob {
    pub name: Option<String>,
    pub eligible_at: Option<Timestamp>,
    /// Top-level work names the Genesis Job (0), see
    /// [`crate::database::DatabaseHandle::genesis`].
    pub parent: JobId,
    pub project: Option<ProjectId>,
    /// The CLI grant stored with a Process plan. Stored turns use the frozen Agent.
    pub process_grant: Option<String>,
    pub input: JobInputDraft,
    /// Present for an LLM execution child; absent for orchestrating or
    /// system work.
    pub configuration: Option<ExecutionConfiguration>,
    /// The conversation tip this directly constructed turn follows.
    pub previous: Option<JobId>,
}

/// The configuration an LLM execution child Job is submitted with. Both
/// references are fixed at submission. Work that runs an LLM directly names
/// no Agent; when one is named, `llm_profile` must be one of its Models. The
/// values the Job executes with are frozen into its spec at submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionConfiguration {
    pub agent_profile: Option<ProfileId>,
    pub llm_profile: ProfileId,
}

/// `value` carries a machine-readable payload, such as the conclusion of a
/// capability check. It is published as given and never changes the Job's
/// outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct NewJobOutput {
    pub kind: IntermediateOutputKind,
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub value: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FinalResult {
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub value: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JobFinalization {
    Completed(FinalResult),
    Failed(FinalResult),
    /// A confirmed cancellation still publishes a final result, which may be
    /// no more than a short explanation of the cancellation.
    Cancelled(FinalResult),
    /// An unknown outcome is recorded without publishing a final result output.
    Unknown,
}

/// A Job's terminal outcome without its published result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobOutcome {
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

impl JobOutcome {
    /// An unknown outcome publishes no result, so `result` is dropped for it.
    pub fn finalization(self, result: FinalResult) -> JobFinalization {
        match self {
            Self::Completed => JobFinalization::Completed(result),
            Self::Failed => JobFinalization::Failed(result),
            Self::Cancelled => JobFinalization::Cancelled(result),
            Self::Unknown => JobFinalization::Unknown,
        }
    }
}

/// An Executor is a Pool config. It offers `LlmRequest` for each of the LLM
/// transports its adapter implements and nothing else, so one without
/// transports is never granted LLM work.
#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutor {
    pub name: String,
    pub concurrency_limit: NonZeroU32,
    /// The LLM transports this Executor's adapter implements, fixed at
    /// registration.
    pub llm_transports: BTreeSet<LlmTransport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedOutput {
    pub output: JobOutputId,
    pub seq: i64,
}

/// The normalized decision contract, independent of a provider's wire format.
pub const DECISION_SCHEMA_VERSION: u32 = 1;
pub const MAX_DECISION_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_DECISION_OPTIONS: usize = 256;

/// Counts a shadow prediction may see. Conversation text and workspace
/// content are absent on purpose. The recommendation is recorded for
/// comparison; it does not authorize summarization.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictionSnapshot {
    pub schema_version: u32,
    pub capacity: Option<u32>,
    pub reserve: u32,
    pub occupancy_tokens: u32,
    pub recent_growth_tokens: u32,
    pub pressure: &'static str,
    pub settled_turns: u32,
    pub active: bool,
}

impl PredictionSnapshot {
    /// A choice among NoNeed, Defer, Prepare and InsufficientEvidence.
    /// The prompt names the counts and asks for no arithmetic.
    pub fn request(&self) -> DecisionRequest {
        DecisionRequest {
            schema_version: DECISION_SCHEMA_VERSION,
            state: serde_json::json!({
                "schema_version": self.schema_version,
                "capacity": self.capacity,
                "reserve": self.reserve,
                "occupancy_tokens": self.occupancy_tokens,
                "recent_growth_tokens": self.recent_growth_tokens,
                "pressure": self.pressure,
                "settled_turns": self.settled_turns,
                "active": self.active,
            }),
            question: DecisionQuestion::Choice {
                prompt: "Choose one recommendation from the counts. Do not calculate further totals. NoNeed means no preparation. Defer means wait. Prepare means a later policy may summarize. InsufficientEvidence means the counts are not enough.".to_owned(),
                options: ["no_need", "defer", "prepare", "insufficient_evidence"]
                    .into_iter()
                    .map(|id| ChoiceOption {
                        id: id.to_owned(),
                        description: id.replace('_', " "),
                    })
                    .collect(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChoiceOption {
    pub id: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScoreLevel {
    pub value: i32,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionQuestion {
    Choice {
        prompt: String,
        options: Vec<ChoiceOption>,
    },
    BinaryProbability {
        prompt: String,
    },
    OrdinalScore {
        prompt: String,
        levels: Vec<ScoreLevel>,
    },
}

impl DecisionQuestion {
    pub fn capability(&self) -> DecisionCapability {
        match self {
            Self::Choice { .. } => DecisionCapability::Choice,
            Self::BinaryProbability { .. } => DecisionCapability::BinaryProbability,
            Self::OrdinalScore { .. } => DecisionCapability::OrdinalScore,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionRequest {
    pub schema_version: u32,
    /// Explicit caller-supplied evidence, never an implicit workspace grant.
    pub state: serde_json::Value,
    pub question: DecisionQuestion,
}

impl DecisionRequest {
    pub fn validate(
        &self,
        capabilities: &BTreeSet<DecisionCapability>,
    ) -> Result<(), &'static str> {
        if self.schema_version != DECISION_SCHEMA_VERSION {
            return Err("unsupported decision schema version");
        }
        if !capabilities.contains(&self.question.capability()) {
            return Err("decision primitive is not supported by the model");
        }
        if serde_json::to_vec(self)
            .map_err(|_| "invalid decision request")?
            .len()
            > MAX_DECISION_REQUEST_BYTES
        {
            return Err("decision request exceeds 1 MiB");
        }
        let prompt = match &self.question {
            DecisionQuestion::Choice { prompt, options } => {
                if !(2..=MAX_DECISION_OPTIONS).contains(&options.len()) {
                    return Err("choice requires 2 to 256 options");
                }
                let mut ids = BTreeSet::new();
                for option in options {
                    if option.id.trim().is_empty()
                        || option.id.len() > 128
                        || !ids.insert(&option.id)
                    {
                        return Err(
                            "choice option IDs must be nonempty, unique and at most 128 bytes",
                        );
                    }
                    if option.description.trim().is_empty() {
                        return Err("choice options require descriptions");
                    }
                }
                prompt
            }
            DecisionQuestion::OrdinalScore { prompt, levels } => {
                if !(2..=MAX_DECISION_OPTIONS).contains(&levels.len())
                    || levels.windows(2).any(|pair| pair[0].value >= pair[1].value)
                    || levels
                        .iter()
                        .any(|level| level.description.trim().is_empty())
                {
                    return Err("score requires 2 to 256 increasing, described levels");
                }
                prompt
            }
            DecisionQuestion::BinaryProbability { prompt } => prompt,
        };
        if prompt.trim().is_empty() {
            return Err("decision prompt must not be empty");
        }
        Ok(())
    }
}

/// Immutable work payload carried by the existing Turn execution descriptor.
/// Admission stays closed until an executor implements the typed decision path.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionWork {
    pub frozen: FrozenDecisionConfig,
    pub request: DecisionRequest,
}

impl DecisionWork {
    pub fn validate(&self) -> Result<(), &'static str> {
        let frozen = &self.frozen;
        if frozen.agent_ref.edit_seq == 0
            || frozen.model_ref.edit_seq == 0
            || frozen.connection_ref.edit_seq == 0
            || frozen.model.connection_id != frozen.connection_ref.id
            || !frozen.agent.models.contains(&frozen.model_ref.id)
        {
            return Err("decision work has inconsistent frozen identities");
        }
        self.request.validate(&frozen.model.declared_capabilities)
    }
}

/// Provider-specific uncertainty evidence. `semantics` identifies its formula;
/// a consumer must recognize that meaning before applying any threshold.
/// It is not a calibrated probability of correctness by default.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionConfidence {
    pub value: f64,
    pub semantics: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnswer {
    Choice {
        selected: String,
        distribution: Option<std::collections::BTreeMap<String, f64>>,
        confidence: Option<DecisionConfidence>,
    },
    BinaryProbability {
        true_probability: f64,
    },
    OrdinalScore {
        expectation: f64,
        /// Probabilities in the request's level order, when supplied.
        distribution: Option<Vec<f64>>,
        confidence: Option<DecisionConfidence>,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionOutcome {
    Answer { answer: DecisionAnswer },
    Uncertain { reason: String },
    Unusable { reason: String },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionResult {
    pub schema_version: u32,
    pub outcome: DecisionOutcome,
    pub requested_model: String,
    pub reported_model: Option<String>,
    /// Only present when a provider or a pinned configuration establishes it.
    pub model_version: Option<String>,
    pub model: ConfigRef,
    pub agent: ConfigRef,
    pub connection: ConfigRef,
    /// Existing provider-request identity; usage and native evidence belong there.
    pub request: RequestRef,
}

impl DecisionResult {
    pub fn validate(
        &self,
        input: &DecisionRequest,
        frozen: &FrozenDecisionConfig,
        request: RequestRef,
    ) -> Result<(), &'static str> {
        input.validate(&frozen.model.declared_capabilities)?;
        if self.schema_version != DECISION_SCHEMA_VERSION
            || self.request != request
            || self.model != frozen.model_ref
            || self.agent != frozen.agent_ref
            || self.connection != frozen.connection_ref
            || self.requested_model != frozen.model.model
        {
            return Err("decision result provenance does not match the frozen request");
        }
        if [&self.reported_model, &self.model_version]
            .into_iter()
            .any(|identity| {
                identity
                    .as_ref()
                    .is_some_and(|value| value.is_empty() || value.len() > 512)
            })
        {
            return Err("reported model identities must be nonempty and at most 512 bytes");
        }
        let answer = match &self.outcome {
            DecisionOutcome::Answer { answer } => answer,
            DecisionOutcome::Uncertain { reason } | DecisionOutcome::Unusable { reason } => {
                return if reason.trim().is_empty() || reason.len() > 4096 {
                    Err("unusable predictions require a bounded reason")
                } else {
                    Ok(())
                };
            }
        };
        let confidence = match (&input.question, answer) {
            (
                DecisionQuestion::Choice { options, .. },
                DecisionAnswer::Choice {
                    selected,
                    distribution,
                    confidence,
                },
            ) => {
                if !options.iter().any(|option| option.id == *selected) {
                    return Err("unknown choice selection");
                }
                if let Some(distribution) = distribution {
                    if distribution.len() != options.len()
                        || options
                            .iter()
                            .any(|option| !distribution.contains_key(&option.id))
                    {
                        return Err(
                            "choice distribution does not cover exactly the requested options",
                        );
                    }
                    validate_distribution(distribution.values().copied())?;
                }
                confidence
            }
            (
                DecisionQuestion::BinaryProbability { .. },
                DecisionAnswer::BinaryProbability { true_probability },
            ) => {
                if !probability(*true_probability) {
                    return Err("invalid binary probability");
                }
                return Ok(());
            }
            (
                DecisionQuestion::OrdinalScore { levels, .. },
                DecisionAnswer::OrdinalScore {
                    expectation,
                    distribution,
                    confidence,
                },
            ) => {
                let low = f64::from(levels[0].value);
                let high = f64::from(levels[levels.len() - 1].value);
                if !expectation.is_finite() || !(low..=high).contains(expectation) {
                    return Err("invalid score expectation");
                }
                if let Some(distribution) = distribution {
                    if distribution.len() != levels.len() {
                        return Err("score distribution does not cover requested levels");
                    }
                    validate_distribution(distribution.iter().copied())?;
                    let derived: f64 = levels
                        .iter()
                        .zip(distribution)
                        .map(|(level, p)| f64::from(level.value) * p)
                        .sum();
                    if (derived - expectation).abs() > 1e-6 * (1.0 + low.abs().max(high.abs())) {
                        return Err("score expectation disagrees with distribution");
                    }
                }
                confidence
            }
            _ => return Err("decision result primitive differs from request"),
        };
        if confidence.as_ref().is_some_and(|confidence| {
            !confidence.value.is_finite()
                || confidence.semantics.trim().is_empty()
                || confidence.semantics.len() > 256
        }) {
            return Err("confidence requires a finite value and bounded semantic identifier");
        }
        Ok(())
    }
}

fn probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

fn validate_distribution(values: impl Iterator<Item = f64>) -> Result<(), &'static str> {
    let mut sum = 0.0;
    for value in values {
        if !probability(value) {
            return Err("invalid distribution probability");
        }
        sum += value;
    }
    if (sum - 1.0).abs() > 1e-6 {
        return Err("distribution must sum to one within 1e-6");
    }
    Ok(())
}

#[cfg(test)]
mod decision_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn frozen() -> FrozenDecisionConfig {
        let id: ConfigId = "00000001-0000-7000-8000-000000000001"
            .parse()
            .expect("UUID");
        FrozenDecisionConfig {
            agent: DecisionAgentConfig::default(),
            agent_ref: ConfigRef { id, edit_seq: 1 },
            model: DecisionModelConfig {
                connection_id: id,
                model: "requested-alias".into(),
                protocol: DecisionProtocol::TypeSafeSystemOne,
                limits: DecisionLimits::default(),
                declared_capabilities: BTreeSet::from([
                    DecisionCapability::Choice,
                    DecisionCapability::BinaryProbability,
                    DecisionCapability::OrdinalScore,
                ]),
            },
            model_ref: ConfigRef { id, edit_seq: 2 },
            connection_ref: ConfigRef { id, edit_seq: 3 },
            endpoint: None,
            credential_ref: None,
        }
    }
    fn request(question: DecisionQuestion) -> DecisionRequest {
        DecisionRequest {
            schema_version: DECISION_SCHEMA_VERSION,
            state: serde_json::json!({"utilization_tokens": 50000}),
            question,
        }
    }
    fn result(outcome: DecisionOutcome) -> (DecisionResult, FrozenDecisionConfig, RequestRef) {
        let frozen = frozen();
        let request = RequestRef {
            job: "00000002-0000-7000-8000-000000000002"
                .parse()
                .expect("UUID"),
            request: RequestNumber::FIRST,
        };
        (
            DecisionResult {
                schema_version: DECISION_SCHEMA_VERSION,
                outcome,
                requested_model: frozen.model.model.clone(),
                reported_model: None,
                model_version: None,
                model: frozen.model_ref,
                agent: frozen.agent_ref,
                connection: frozen.connection_ref,
                request,
            },
            frozen,
            request,
        )
    }
    fn choice() -> DecisionRequest {
        request(DecisionQuestion::Choice {
            prompt: "prepare?".into(),
            options: vec![
                ChoiceOption {
                    id: "skip".into(),
                    description: "no pressure".into(),
                },
                ChoiceOption {
                    id: "prepare".into(),
                    description: "pressure expected".into(),
                },
            ],
        })
    }
    #[test]
    fn decision_requests_require_typed_answer_spaces_and_capabilities() {
        let mut input = choice();
        assert!(
            input
                .validate(&frozen().model.declared_capabilities)
                .is_ok()
        );
        assert!(input.validate(&BTreeSet::new()).is_err());
        if let DecisionQuestion::Choice { options, .. } = &mut input.question {
            options[1].id = "skip".into();
        }
        assert!(
            input
                .validate(&frozen().model.declared_capabilities)
                .is_err()
        );
        let input = request(DecisionQuestion::OrdinalScore {
            prompt: "pressure".into(),
            levels: vec![
                ScoreLevel {
                    value: 2,
                    description: "high".into(),
                },
                ScoreLevel {
                    value: 1,
                    description: "low".into(),
                },
            ],
        });
        assert!(
            input
                .validate(&frozen().model.declared_capabilities)
                .is_err()
        );
        assert!(
            serde_json::from_value::<DecisionRequest>(
                serde_json::json!({"schema_version":1,"state":{},"question":"text"})
            )
            .is_err()
        );
    }
    #[test]
    fn decision_results_validate_distributions_uncertainty_and_provenance() {
        let (mut answer, frozen, reference) = result(DecisionOutcome::Answer {
            answer: DecisionAnswer::Choice {
                selected: "prepare".into(),
                distribution: Some(BTreeMap::from([
                    ("skip".into(), 0.2),
                    ("prepare".into(), 0.8),
                ])),
                confidence: Some(DecisionConfidence {
                    value: 0.6,
                    semantics: "provider.choice.margin.v1".into(),
                }),
            },
        });
        assert!(answer.validate(&choice(), &frozen, reference).is_ok());
        let encoded = serde_json::to_value(&answer).expect("serialize");
        assert_eq!(encoded["request"]["job"], reference.job.to_string());
        assert_eq!(
            serde_json::from_value::<DecisionResult>(encoded).expect("decode"),
            answer
        );
        answer.model.edit_seq += 1;
        assert!(answer.validate(&choice(), &frozen, reference).is_err());
        answer.model = frozen.model_ref;
        for distribution in [
            BTreeMap::from([("skip".into(), 0.2)]),
            BTreeMap::from([("skip".into(), 0.5), ("prepare".into(), f64::NAN)]),
            BTreeMap::from([("skip".into(), 0.2), ("prepare".into(), 0.2)]),
        ] {
            answer.outcome = DecisionOutcome::Answer {
                answer: DecisionAnswer::Choice {
                    selected: "prepare".into(),
                    distribution: Some(distribution),
                    confidence: None,
                },
            };
            assert!(answer.validate(&choice(), &frozen, reference).is_err());
        }
        answer.outcome = DecisionOutcome::Uncertain {
            reason: "insufficient growth evidence".into(),
        };
        assert!(answer.validate(&choice(), &frozen, reference).is_ok());
        answer.outcome = DecisionOutcome::Unusable {
            reason: String::new(),
        };
        assert!(answer.validate(&choice(), &frozen, reference).is_err());
    }
    #[test]
    fn decision_probability_and_score_keep_distinct_semantics() {
        let binary = request(DecisionQuestion::BinaryProbability {
            prompt: "pressure soon?".into(),
        });
        let (mut output, frozen, reference) = result(DecisionOutcome::Answer {
            answer: DecisionAnswer::BinaryProbability {
                true_probability: 0.5,
            },
        });
        assert!(output.validate(&binary, &frozen, reference).is_ok());
        for value in [-0.1, 1.1, f64::INFINITY, f64::NAN] {
            output.outcome = DecisionOutcome::Answer {
                answer: DecisionAnswer::BinaryProbability {
                    true_probability: value,
                },
            };
            assert!(output.validate(&binary, &frozen, reference).is_err());
        }
        let score = request(DecisionQuestion::OrdinalScore {
            prompt: "pressure?".into(),
            levels: vec![
                ScoreLevel {
                    value: 0,
                    description: "low".into(),
                },
                ScoreLevel {
                    value: 10,
                    description: "high".into(),
                },
            ],
        });
        output.outcome = DecisionOutcome::Answer {
            answer: DecisionAnswer::OrdinalScore {
                expectation: 7.5,
                distribution: Some(vec![0.25, 0.75]),
                confidence: None,
            },
        };
        assert!(output.validate(&score, &frozen, reference).is_ok());
        assert!(output.validate(&binary, &frozen, reference).is_err());
        output.outcome = DecisionOutcome::Answer {
            answer: DecisionAnswer::OrdinalScore {
                expectation: 2.0,
                distribution: Some(vec![0.25, 0.75]),
                confidence: None,
            },
        };
        assert!(output.validate(&score, &frozen, reference).is_err());
        assert!(
            serde_json::from_value::<RequestRef>(
                serde_json::json!({"job":reference.job,"request":0})
            )
            .is_err()
        );
    }
}
