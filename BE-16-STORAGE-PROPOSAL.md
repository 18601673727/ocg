# BE-16 Storage Design Proposal

**Date:** 2026-10-23  
**Requires owner approval before implementation**

## Summary

This proposes extending the existing config/conversation persistence model to support Decision Profiles and context compaction, while maintaining clean separation between Generative and Decision types at the domain level.

## Current State

- **FORMAT_VERSION**: 26 (in schema.rs, schema_tests.rs, README)
- **Config Storage**: Single `CONFIGS` table with `ConfigBody` enum (Connection, Model, Agent, Pool)
- **Agent Types**: `AgentBehavior::Chat | General` — both are generative
- **Model Config**: `ModelConfig` with connection_id, model string, context_window, LlmParameters, declared capabilities
- **Conversation**: Stores agent/project/limits, uses `Entry::Decision` for approval evidence
- **JobKind**: Preserves existing `Conversation` and `Turn` variants

## Proposed Changes

### 1. Profile Type Distinction

Split Agent and Model configs into Generative vs Decision variants:

**New ConfigBody variants:**
```rust
pub enum ConfigBody {
    Connection(ConnectionConfig),
    Model(ModelConfig),           // Rename to GenerativeModel(GenerativeModelConfig)
    Agent(AgentConfig),            // Rename to GenerativeAgent(GenerativeAgentConfig)
    Pool(PoolConfig),
    DecisionModel(DecisionModelConfig),
    DecisionAgent(DecisionAgentConfig),
}
```

**Alternative (preserve ConfigBody shape):**
Keep `Model`/`Agent` variant names, but split the config body structs themselves:

```rust
pub enum AgentConfigBody {
    Generative(GenerativeAgentConfig),
    Decision(DecisionAgentConfig),
}

pub enum ModelConfigBody {
    Generative(GenerativeModelConfig),
    Decision(DecisionModelConfig),
}

pub enum ConfigBody {
    Connection(ConnectionConfig),
    Model(ModelConfigBody),
    Agent(AgentConfigBody),
    Pool(PoolConfig),
}
```

**Recommendation:** Use the second approach (nested enum) to preserve ConfigBody's existing 4-variant structure and minimize call-site disruption. The type distinction happens one level deeper.

### 2. Decision Model Configuration

```rust
pub struct DecisionModelConfig {
    pub connection_id: ConfigId,
    pub model: String,
    pub protocol: DecisionProtocol,
    /// Per-request and per-question limits, not a single context window
    pub limits: DecisionLimits,
    pub declared_capabilities: BTreeSet<DecisionCapability>,
}

pub enum DecisionProtocol {
    TypeSafeSystemOne { version: String },
    // Gateway variants added when approved
}

pub struct DecisionLimits {
    pub max_request_tokens: Option<u32>,
    pub max_question_tokens: Option<u32>,
    pub max_questions_per_request: Option<u32>,
}

pub enum DecisionCapability {
    Choice,
    BinaryProbability,  // Noul
    OrdinalScore,
    // Experimental Box, long-context opt-in not enabled implicitly
}
```

**No LlmParameters:** Decision models don't accept temperature, top_p, max_tokens, reasoning_effort, etc.

### 3. Decision Agent Configuration

```rust
pub struct DecisionAgentConfig {
    pub instructions: Option<String>,
    pub models: Vec<ConfigId>,            // References to DecisionModel configs
    pub default_model: Option<ConfigId>,
    pub workspace_access: WorkspaceAccess,
}

pub enum WorkspaceAccess {
    InputOnly,
    WorkspaceRead,  // If approved for initial release; explicit bounded read
}
```

**No tool grant, no dispatch, no approval authority:** Decision agents cannot execute tools, dispatch work, approve anything, or write code.

**Behavior field removed:** Decision vs Generative is the type-level distinction; no AgentBehavior enum within DecisionAgent.

### 4. Compaction Policy Configuration

Stored on **ConversationRecord** (conversation-level override) and as **host default** (source TBD - likely separate host config, not part of this task's encoding change):

```rust
pub struct CompactionPolicy {
    pub mode: CompactionMode,
    pub summarizer: Option<ConfigId>,  // Reference to GenerativeModel for summaries
    pub decision_profile: Option<ConfigId>,  // Reference to DecisionAgent for prediction
    pub predictive_enabled: bool,
}

pub enum CompactionMode {
    Disabled,
    Conservative,
    Balanced,
    Aggressive,
    Custom(CustomPolicy),
}

pub struct CustomPolicy {
    pub min_source_tokens: u32,
    pub min_savings_tokens: u32,
    pub recent_tail_turns: u8,
    pub cooldown_minutes: u16,
}
```

**Resolution order:** conversation → Profile → host → shipped default. Explicit Disabled overrides inheritance.

### 5. Context Preparation and Adoption Metadata

**Internal turn metadata** (not exposed as user Entry):

```rust
pub struct ContextPreparation {
    pub boundary: SourceBoundary,
    pub status: PreparationStatus,
    pub artifact_id: Option<JobId>,  // Reference to summarization Job
    pub requested_at: Timestamp,
    pub policy_snapshot: CompactionPolicy,
}

pub enum PreparationStatus {
    Planning,
    Requested,
    Running(JobId),
    Completed(JobId),
    Failed,
    Incompatible,
}

pub struct SourceBoundary {
    pub last_turn: JobId,
    pub last_entry_seq: u32,
    pub fingerprint: ContentDigest,  // SHA-256 of covered records
}

pub struct ContextUse {
    pub artifact_id: JobId,
    pub adopted_at: Timestamp,
    pub covered_boundary: SourceBoundary,
}
```

**Storage location:** New optional fields on turn's `PlanSource::Stored` variant or as separate internal metadata table. **Decision needed:** Extend PlanSource (changes existing row encoding) or add derived metadata table (keeps turn rows unchanged, adds new table).

**Recommendation:** Extend `PlanSource::Stored` with optional preparation/adoption fields to keep context metadata with the turn it belongs to, avoiding cross-table joins. This changes turn encoding.

### 6. Decision Request and Result Types

```rust
pub struct DecisionRequest {
    pub request_type: DecisionRequestType,
    pub state: String,
    pub model_version: Option<String>,  // Pin for evaluated policies
}

pub enum DecisionRequestType {
    Choice {
        question: String,
        options: Vec<ChoiceOption>,
    },
    BinaryProbability {
        question: String,
    },
    OrdinalScore {
        question: String,
        levels: Vec<ScoreLevel>,
    },
}

pub struct DecisionResult {
    pub answer: DecisionAnswer,
    pub uncertainty: Option<f64>,  // Preserve provider-specific confidence
    pub model_reported: String,
    pub usage: Option<NormalizedUsage>,
}

pub enum DecisionAnswer {
    Choice { selected: usize, distribution: Option<Vec<f64>> },
    BinaryProbability { probability: f64 },
    OrdinalScore { expectation: f64, distribution: Option<Vec<f64>> },
}
```

**Stored as:** Specialized `OutputKind` variant or job-type-specific result encoding. **Decision needed:** Extend OutputKind vs separate decision output table.

**Recommendation:** Extend `OutputKind` enum with `Decision(DecisionResult)` variant. Output storage already handles typed results; reuse existing infrastructure.

### 7. Summarization Artifact

```rust
pub struct SummarizationArtifact {
    pub summary: String,
    pub coverage: SourceBoundary,
    pub recipe_version: String,
    pub producer_config: FrozenExecutionConfig,
    pub source_references: Vec<(JobId, u32)>,  // (turn_id, entry_seq) pairs
    pub estimated_tokens: Option<u32>,
    pub omissions: Vec<OmissionReason>,
}

pub enum OmissionReason {
    TooLarge,
    UnavailableAttachment,
    Uncertain,
}
```

**Stored as:** `OutputKind::Summarization(SummarizationArtifact)` on the producing Job's output. Consumers store a reference (JobId) only.

## Format Impact Assessment

### Tables Changed
- **CONFIGS**: Config body encoding changes for Agent/Model split (new enum codes)
- **JOBS**: Turn's PlanSource encoding extends with preparation metadata (if stored there)
- **JOB_OUTPUTS**: New OutputKind variants for Decision and Summarization results
- **CONVERSATIONS** (derived or authoritative): Compaction policy field

### New Enum Codes
- `ConfigBody`: 2 new variants (DecisionModel, DecisionAgent) OR nested enum discriminants
- `OutputKind`: 2 new variants (Decision, Summarization)
- `WorkspaceAccess`: 2 codes (InputOnly, WorkspaceRead)
- `DecisionProtocol`: 1+ codes
- `DecisionCapability`: 3+ codes
- `CompactionMode`: 5 codes
- `PreparationStatus`: 6 codes

### Derived Tables/Indexes
- Ready/prepared/latest-use lookups: derived from Job outputs and turn references
- Decision-capable model/agent discovery: derived from config bodies

### Migration Strategy
**None.** Old format-26 directories are refused. New format-27 directory starts fresh. No data conversion, no compatibility shim.

## Key Design Decisions Required

### 1. ConfigBody Structure
**Options:**
- A) Four new top-level variants: `GenerativeModel`, `GenerativeAgent`, `DecisionModel`, `DecisionAgent`
- B) Nested enums: `Model(ModelConfigBody)`, `Agent(AgentConfigBody)` where inner enums discriminate

**Tradeoff:** (A) is clearer type-wise but breaks every ConfigBody match. (B) preserves existing 4-variant structure, localizes changes to model/agent handling code.

**Recommendation:** (B) - nested enums

### 2. Context Metadata Storage
**Options:**
- A) Extend `PlanSource::Stored` with optional preparation/adoption fields
- B) New authoritative metadata table keyed by JobId
- C) Purely derived from Job outputs and references

**Tradeoff:** (A) keeps metadata with its turn, simple to query, changes turn encoding. (B) avoids turn encoding change, adds table. (C) minimizes storage but requires reconstruction on every query.

**Recommendation:** (A) - extend PlanSource. The metadata belongs to the turn; joining another table is overhead.

### 3. Decision Output Storage
**Options:**
- A) Extend `OutputKind` with `Decision` variant
- B) New `DECISION_OUTPUTS` table

**Tradeoff:** (A) reuses existing output infrastructure, simple. (B) isolates decision-specific schema, but duplicates output lifecycle.

**Recommendation:** (A) - extend OutputKind

### 4. Conversation Policy Storage
**Options:**
- A) Optional field on ConversationRecord
- B) Separate policy table
- C) Stored only when non-default (sparse)

**Tradeoff:** (A) is simplest, always readable. (B) avoids conversation encoding change. (C) minimizes writes but complicates inheritance resolution.

**Recommendation:** (A) - optional field on ConversationRecord

### 5. Host Default Policy Location
**Out of scope for BE-16 encoding:** Host defaults likely live in a separate host config or environment; not part of per-conversation storage. BE-21 owns policy resolution logic.

## Invalid Combinations to Prevent

1. **Decision Model with LlmParameters:** Type-level prevention via separate struct
2. **Decision Agent with tool grant/dispatch/approval:** Struct doesn't have these fields
3. **Generative work with Decision Model:** Checked at placement/claim time
4. **Decision work with Generative Model:** Checked at submission validation
5. **Summary artifact adopted with wrong source fingerprint:** Runtime validation in BE-19

## Acceptance Criteria Mapping

- **Old generative behavior remains valid:** Existing GenerativeAgent/GenerativeModel configs work unchanged in format 27
- **Decision-only models cannot accept generative parameters:** Type-level prevention
- **Frozen identities remain interpretable:** ConfigRef still works; frozen configs snapshot at claim time
- **Context-use records are auditable:** Stored with turn, includes boundary fingerprint and timestamps
- **Derived indexes reproducible:** Ready/prepared status computed from outputs and references
- **Wrong-kind refusal:** Profile CRUD validates Agent/Model type match

## Open Questions for Owner

1. **Nested vs flat ConfigBody?** (Recommendation: nested)
2. **PlanSource extension vs metadata table?** (Recommendation: extend PlanSource)
3. **Conversation policy encoding location?** (Recommendation: ConversationRecord field)
4. **Host default policy storage?** (Out of scope; BE-21 decides)
5. **Initial WorkspaceRead support or defer?** (Spec says coordinate; if deferred, refuse explicitly)
6. **Version bump to 27 or other number?** (Standard increment: 26→27)

## Implementation Order

1. Domain types (enums, structs) in `src/persistence/records.rs` and `conversations.rs`
2. Codec changes in `src/storage/codec.rs` (encode/decode methods)
3. Schema changes in `src/storage/schema.rs` (if new tables needed)
4. Validation in `src/control_plane/profiles.rs` (CRUD enforcement)
5. Exhaustive match updates in transactions, derived indexes, API projections
6. FORMAT_VERSION bump to 27 in `schema.rs`, `schema_tests.rs`, README

## Next Steps

**Awaiting owner approval on:**
- Overall approach (nested ConfigBody, extend PlanSource, extend OutputKind)
- Format version bump to 27
- Any adjustments to struct shapes or enum codes
- Decision on WorkspaceRead initial support

Once approved, implementation proceeds with full validation suite before marking BE-16 complete.
