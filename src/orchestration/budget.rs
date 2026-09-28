//! Mandatory Mission monetary budget and quota admission.
//!
//! This module is the **economic safety boundary** of the control plane. It
//! answers exactly one question before any provider-costly side effect:
//!
//! ```text
//! given this Mission's durable budget,
//!       this proposed bounded spend,
//!       the current quota facts,
//! may OCG intentionally start this provider-costly work?
//! ```
//!
//! Two properties make it different from the optional Policy layer:
//!
//! 1. **It is not bypassable.** A configured hard Mission budget is a cutoff,
//!    not an alert. The admission is evaluated independently of
//!    `policy.enabled`, and a generic approval can never authorize exceeding a
//!    hard cap. The only way past a cap is to explicitly change the hard budget
//!    itself (`ocg budget set`).
//! 2. **Unknown never becomes optimistic.** When a cost or quota fact is
//!    required for economic safety and is unknown or stale, the action is
//!    deferred rather than assumed free, unlimited or available. No currency is
//!    ever converted.
//!
//! Money is a fixed-point integer (micro-units of a single currency), never a
//! binary float. Spend is durably reserved before the side effect and settled
//! once afterwards, so a crash, restart, rollover, retry or recovery pass
//! cannot double-count a bounded spend. An uncertain dispatch keeps its
//! reservation rather than optimistically releasing it.
//!
//! The module owns only the budget model and its pure decision function. It
//! performs no runtime side effect and no network I/O; the registry read used
//! for quota facts is a descriptive, fail-soft lookup.

use crate::error::{OcgError, Result};
use crate::resources::{ResourceId, ResourceIdentity, ResourceProvenance};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

/// Schema version for a durable Mission budget. It is additive to the Mission
/// schema, so old records deserialize with a default (unconfigured) budget.
pub const BUDGET_SCHEMA_VERSION: u32 = 1;
/// Upper bound on retained reservations per Mission.
pub const MAX_RESERVATIONS: usize = 32;
/// Upper bound on retained co-firing economic blocks in one assessment.
pub const MAX_SPEND_BLOCKS: usize = 8;
/// Upper bound on a bounded human-readable economic reason.
pub const MAX_REASON_BYTES: usize = 240;
/// Upper bound on a currency code (ASCII).
pub const MAX_CURRENCY_BYTES: usize = 8;
/// The number of micro-units in one currency unit (10^-6 precision).
pub const MICROS_PER_UNIT: i64 = 1_000_000;

/// The action is admissible and, when a hard limit applies, a reservation was
/// recorded.
pub const REASON_ALLOWED: &str = "mission_spend_allowed";
/// No hard budget is configured, so there is no economic cutoff.
pub const REASON_UNCONFIGURED: &str = "mission_budget_unconfigured";
/// An explicit per-Mission hard limit was set.
pub const REASON_EXPLICIT_LIMIT: &str = "mission_budget_explicit_limit";
/// The bounded spend would push committed spend past the hard cap.
pub const REASON_HARD_LIMIT: &str = "mission_hard_budget_exceeded";
/// Settled spend already exceeds the hard cap.
pub const REASON_BREACHED: &str = "mission_budget_breached";
/// Currencies differ; OCG never performs FX conversion.
pub const REASON_CURRENCY: &str = "mission_budget_currency_mismatch";
/// The cost of a required provider-costly action is unknown.
pub const REASON_COST_UNKNOWN: &str = "mission_cost_unknown";
/// The quota fact reports no remaining capacity.
pub const REASON_QUOTA_EXHAUSTED: &str = "mission_quota_exhausted";
/// No authoritative quota fact is known while a quota is required.
pub const REASON_QUOTA_UNKNOWN: &str = "mission_quota_unknown";
/// The latest quota fact is stale and is not treated as a current fact.
pub const REASON_QUOTA_STALE: &str = "mission_quota_stale";

/// A fixed-point monetary amount: an integer number of micro-units of one
/// currency. Binary floats are never used for money.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Money {
    pub micros: i64,
    pub currency: String,
}

impl Money {
    pub fn new(micros: i64, currency: impl Into<String>) -> Self {
        Self {
            micros,
            currency: currency.into(),
        }
    }

    pub fn zero(currency: impl Into<String>) -> Self {
        Self {
            micros: 0,
            currency: currency.into(),
        }
    }

    pub fn is_zero(&self) -> bool {
        self.micros == 0
    }
}

/// Normalize and validate a currency code. OCG never guesses or converts.
pub fn normalize_currency(raw: &str) -> Result<String> {
    let value = raw.trim().to_ascii_uppercase();
    if value.is_empty()
        || value.len() > MAX_CURRENCY_BYTES
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(OcgError::config(format!(
            "currency '{raw}' must be 1-{MAX_CURRENCY_BYTES} ASCII letters/digits"
        )));
    }
    Ok(value)
}

/// Where a Mission's effective hard limit comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetOrigin {
    /// No hard budget was ever configured for this Mission.
    #[default]
    LegacyUnconfigured,
    /// The limit was materialized from the effective `budget` configuration.
    SystemDefault,
    /// The limit was explicitly set by an operator (`ocg budget set`).
    ExplicitUserLimit,
}

impl BudgetOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LegacyUnconfigured => "legacy_unconfigured",
            Self::SystemDefault => "system_default",
            Self::ExplicitUserLimit => "explicit_user_limit",
        }
    }
}

/// The economic status of a Mission budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetStatus {
    /// No hard limit is configured; there is no cutoff.
    #[default]
    Unconfigured,
    /// A hard limit is configured and committed spend is below it.
    Active,
    /// Committed spend has reached the hard limit; further paid work is denied.
    Exhausted,
    /// Settled spend exceeded the hard limit. The overage is recorded, never
    /// clamped, and all further paid work is denied.
    Breached,
}

impl BudgetStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unconfigured => "unconfigured",
            Self::Active => "active",
            Self::Exhausted => "exhausted",
            Self::Breached => "breached",
        }
    }
}

/// The action class being authorized. Only classes with a provider-costly side
/// effect ever require a reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpendAction {
    /// Resume a bounded continuation on the target execution. This is the one
    /// OCG-initiated operation that is `DefinitelyProviderCostly` today: the V2
    /// adapter injects a synthetic provider message with `resume: true`.
    ResumeContinuation,
    /// One migrated provider network attempt, including root and worker turns.
    ProviderDispatch,
}

impl SpendAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResumeContinuation => "resume_continuation",
            Self::ProviderDispatch => "provider_dispatch",
        }
    }
}

/// The basis for a proposed spend's amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CostBasis {
    /// A bounded pre-authorization estimate.
    Estimated(Money),
    /// No cost fact exists. It is never treated as free.
    Unknown,
}

/// The typed economic decision. `Deny` is a hard cutoff that is not retryable
/// by waiting and cannot be overridden by a generic approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpendDecision {
    Allow,
    Defer,
    Deny,
}

impl SpendDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Defer => "defer",
            Self::Deny => "deny",
        }
    }

    fn precedence(self) -> u8 {
        match self {
            Self::Allow => 0,
            Self::Defer => 1,
            Self::Deny => 2,
        }
    }
}

/// One co-firing economic block. All of them are retained (bounded) so a hard
/// cap and an exhausted quota are never silently reduced to one another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendBlock {
    pub reason_code: String,
    pub decision: SpendDecision,
    pub reason: String,
}

impl SpendBlock {
    fn deny(reason_code: &str, reason: impl Into<String>) -> Self {
        Self {
            reason_code: reason_code.to_string(),
            decision: SpendDecision::Deny,
            reason: bounded(&reason.into()),
        }
    }

    fn defer(reason_code: &str, reason: impl Into<String>) -> Self {
        Self {
            reason_code: reason_code.to_string(),
            decision: SpendDecision::Defer,
            reason: bounded(&reason.into()),
        }
    }

    /// A bounded `reason_code=decision` summary.
    pub fn summary(&self) -> String {
        format!("{}={}", self.reason_code, self.decision.as_str())
    }
}

/// The complete, inspectable result of one economic admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendAssessment {
    pub decision: SpendDecision,
    /// The primary (highest-precedence) reason code.
    pub reason_code: String,
    /// The primary bounded reason.
    pub reason: String,
    /// Every co-firing block, in deterministic order.
    pub blocks: Vec<SpendBlock>,
    pub origin: BudgetOrigin,
    pub status: BudgetStatus,
    pub currency: String,
    pub hard_limit: Option<Money>,
    pub committed: Money,
    /// The amount to reserve when the decision is `Allow` and a hard limit
    /// applies. `None` means no reservation is needed.
    pub amount: Option<Money>,
    /// The deterministic reservation id, set once the caller records the
    /// reservation durably.
    pub reservation_id: Option<String>,
}

impl SpendAssessment {
    pub fn is_allowed(&self) -> bool {
        self.decision == SpendDecision::Allow
    }

    /// A fail-closed assessment for a hard budget that *is* configured but could
    /// not be applied to this Mission (for example an accounting-currency
    /// conflict). It denies rather than falling back to an unconfigured,
    /// uncapped admission, so the absence of an enforceable limit never means
    /// "allow".
    pub fn configured_but_unenforceable(
        budget: &MissionBudget,
        reason_code: &str,
        reason: impl Into<String>,
    ) -> Self {
        let block = SpendBlock::deny(reason_code, reason);
        Self {
            decision: SpendDecision::Deny,
            reason_code: block.reason_code.clone(),
            reason: block.reason.clone(),
            blocks: vec![block],
            origin: budget.origin,
            status: budget.status,
            currency: budget.currency.clone(),
            hard_limit: budget.hard_limit.clone(),
            committed: budget.committed(),
            amount: None,
            reservation_id: None,
        }
    }
}

/// A durable reservation: bounded spend authorized before a provider-costly
/// side effect. Its id is deterministic over the exact operation, so replaying
/// the same operation never reserves twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Reservation {
    pub reservation_id: String,
    pub action: String,
    pub operation_id: String,
    pub amount: Money,
    pub state: ReservationState,
    /// True when the dispatch outcome is uncertain (for example a transport
    /// failure). An uncertain reservation is retained, never optimistically
    /// released.
    pub unresolved: bool,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub settled_amount: Option<Money>,
}

impl Default for Reservation {
    fn default() -> Self {
        Self {
            reservation_id: String::new(),
            action: String::new(),
            operation_id: String::new(),
            amount: Money::default(),
            state: ReservationState::Reserved,
            unresolved: false,
            created_at: 0,
            settled_at: None,
            settled_amount: None,
        }
    }
}

/// The lifecycle of one reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationState {
    /// Authorized and not yet settled. It counts against the hard cap.
    #[default]
    Reserved,
    /// Settled exactly once against real or estimated actual usage.
    Settled,
    /// Proven not to have reached the provider; it no longer counts.
    Released,
}

impl ReservationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Settled => "settled",
            Self::Released => "released",
        }
    }
}

/// The durable Mission budget. It is the durable accounting substrate that
/// survives restart, rollover, retries and recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MissionBudget {
    pub schema_version: u32,
    /// The Mission's accounting currency. Empty until a currency is known.
    pub currency: String,
    /// The durable hard limit, materialized once from configuration or set
    /// explicitly by an operator. Never silently changed by a later config
    /// edit: past a cap, the user must explicitly change the budget.
    pub hard_limit: Option<Money>,
    pub origin: BudgetOrigin,
    /// Settled spend. Actual overage is recorded here, never clamped.
    pub settled: Money,
    /// Sum of outstanding reservations (derived; kept for inspection).
    pub reserved: Money,
    /// Sum of outstanding reservations whose dispatch outcome is uncertain.
    pub unresolved: Money,
    pub status: BudgetStatus,
    pub reservations: Vec<Reservation>,
    /// The reason code of the last economic assessment.
    pub reason: Option<String>,
    pub updated_at: i64,
}

impl Default for MissionBudget {
    fn default() -> Self {
        Self {
            schema_version: BUDGET_SCHEMA_VERSION,
            currency: String::new(),
            hard_limit: None,
            origin: BudgetOrigin::LegacyUnconfigured,
            settled: Money::default(),
            reserved: Money::default(),
            unresolved: Money::default(),
            status: BudgetStatus::Unconfigured,
            reservations: Vec::new(),
            reason: None,
            updated_at: 0,
        }
    }
}

impl MissionBudget {
    /// Total committed spend: settled plus outstanding reservations.
    pub fn committed(&self) -> Money {
        Money::new(
            self.settled.micros.saturating_add(self.reserved.micros),
            self.currency.clone(),
        )
    }

    /// Remaining headroom under the hard limit, floored at zero.
    pub fn available(&self) -> Option<Money> {
        let limit = self.hard_limit.as_ref()?;
        Some(Money::new(
            limit.micros.saturating_sub(self.committed().micros).max(0),
            self.currency.clone(),
        ))
    }

    /// Recompute the derived rollups and status from the reservation ledger.
    /// The authoritative accumulators (`settled`) are never derived.
    pub fn recompute(&mut self) {
        let mut reserved = 0i64;
        let mut unresolved = 0i64;
        for reservation in &self.reservations {
            if reservation.state == ReservationState::Reserved {
                reserved = reserved.saturating_add(reservation.amount.micros);
                if reservation.unresolved {
                    unresolved = unresolved.saturating_add(reservation.amount.micros);
                }
            }
        }
        self.reserved = Money::new(reserved, self.currency.clone());
        self.unresolved = Money::new(unresolved, self.currency.clone());
        self.status = self.status_for();
    }

    fn status_for(&self) -> BudgetStatus {
        let Some(limit) = self.hard_limit.as_ref() else {
            return BudgetStatus::Unconfigured;
        };
        if self.settled.micros > limit.micros {
            return BudgetStatus::Breached;
        }
        if self.committed().micros >= limit.micros {
            return BudgetStatus::Exhausted;
        }
        BudgetStatus::Active
    }

    /// Materialize a configured default cap into durable state exactly once.
    /// A later config edit never silently changes an existing hard budget.
    pub fn materialize_config(&mut self, config: &BudgetConfig) -> bool {
        if self.hard_limit.is_some() {
            return false;
        }
        let (Some(micros), Some(currency)) = (config.hard_limit_micros, config.currency.as_ref())
        else {
            return false;
        };
        if !self.currency.is_empty() && self.currency != *currency {
            // Settled/spent money already exists in another currency. Do not
            // convert; leave the budget unconfigured so admission fails closed.
            return false;
        }
        self.hard_limit = Some(Money::new(micros, currency.clone()));
        self.currency = currency.clone();
        self.origin = BudgetOrigin::SystemDefault;
        self.recompute();
        true
    }

    /// Explicitly set or replace the hard budget. This is the only supported
    /// way past a hard cap. It refuses to reinterpret already-accounted money
    /// in a different currency.
    pub fn set_hard_limit(&mut self, amount: Money, now: i64) -> Result<bool> {
        if amount.micros <= 0 {
            return Err(OcgError::config(
                "a hard Mission budget must be a positive amount",
            ));
        }
        let currency = normalize_currency(&amount.currency)?;
        if !self.currency.is_empty() && self.currency != currency {
            return Err(OcgError::config(format!(
                "Mission budget is accounted in {}; refusing to reinterpret it as {currency} (no FX conversion)",
                self.currency
            )));
        }
        for reservation in &self.reservations {
            if reservation.state == ReservationState::Reserved
                && !reservation.amount.currency.is_empty()
                && reservation.amount.currency != currency
            {
                return Err(OcgError::config(
                    "an outstanding reservation uses another currency; refusing to reinterpret it",
                ));
            }
        }
        self.hard_limit = Some(Money::new(amount.micros, currency.clone()));
        self.currency = currency;
        self.origin = BudgetOrigin::ExplicitUserLimit;
        self.recompute();
        self.updated_at = now;
        Ok(true)
    }

    /// Find an existing reservation for exactly this operation.
    pub fn reservation_for(
        &self,
        action: SpendAction,
        mission_id: &str,
        generation: u32,
        operation_id: &str,
    ) -> Option<&Reservation> {
        let id = reservation_id(action, mission_id, generation, operation_id);
        self.reservations
            .iter()
            .find(|reservation| reservation.reservation_id == id)
    }

    /// Record a reservation for a bounded spend. Idempotent: an existing live or
    /// settled reservation for the same operation is reused, never doubled.
    pub fn reserve(
        &mut self,
        action: SpendAction,
        mission_id: &str,
        generation: u32,
        operation_id: &str,
        amount: Money,
        now: i64,
    ) -> bool {
        let id = reservation_id(action, mission_id, generation, operation_id);
        if let Some(index) = self
            .reservations
            .iter()
            .position(|reservation| reservation.reservation_id == id)
        {
            match self.reservations[index].state {
                ReservationState::Reserved | ReservationState::Settled => return false,
                ReservationState::Released => {}
            }
            self.reservations.remove(index);
        }
        self.reservations.push(Reservation {
            reservation_id: id,
            action: action.as_str().to_string(),
            operation_id: operation_id.to_string(),
            amount,
            state: ReservationState::Reserved,
            unresolved: false,
            created_at: now,
            settled_at: None,
            settled_amount: None,
        });
        self.prune();
        self.recompute();
        self.updated_at = now;
        true
    }

    /// Mark an outstanding reservation as an uncertain dispatch. It is retained
    /// and keeps counting against the hard cap.
    pub fn mark_unresolved(&mut self, reservation_id: &str, now: i64) -> bool {
        let Some(index) = self.reservations.iter().position(|reservation| {
            reservation.reservation_id == reservation_id
                && reservation.state == ReservationState::Reserved
        }) else {
            return false;
        };
        if self.reservations[index].unresolved {
            return false;
        }
        self.reservations[index].unresolved = true;
        self.recompute();
        self.updated_at = now;
        true
    }

    /// Settle an outstanding reservation exactly once. An actual amount larger
    /// than the reservation is recorded in full (breach), never clamped. A
    /// smaller actual releases the difference.
    pub fn settle(
        &mut self,
        reservation_id: &str,
        actual: Option<Money>,
        now: i64,
    ) -> Result<bool> {
        let Some(index) = self.reservations.iter().position(|reservation| {
            reservation.reservation_id == reservation_id
                && reservation.state == ReservationState::Reserved
        }) else {
            // Already settled or released: a duplicate settlement is a no-op.
            return Ok(false);
        };
        let reserved = self.reservations[index].amount.clone();
        let settled_amount = match actual {
            Some(actual) => {
                let currency = normalize_currency(&actual.currency)?;
                if !self.currency.is_empty() && currency != self.currency {
                    return Err(OcgError::config(format!(
                        "settlement currency {currency} differs from the Mission budget currency {} (no FX conversion)",
                        self.currency
                    )));
                }
                Money::new(actual.micros, currency)
            }
            None => reserved,
        };
        self.settled.micros = self.settled.micros.saturating_add(settled_amount.micros);
        if self.currency.is_empty() {
            self.currency = settled_amount.currency.clone();
        }
        self.settled.currency = self.currency.clone();
        self.reservations[index].state = ReservationState::Settled;
        self.reservations[index].unresolved = false;
        self.reservations[index].settled_at = Some(now);
        self.reservations[index].settled_amount = Some(settled_amount);
        self.recompute();
        self.updated_at = now;
        Ok(true)
    }

    /// Release an outstanding reservation that is proven not to have reached
    /// the provider. It stops counting against the hard cap.
    pub fn release(&mut self, reservation_id: &str, now: i64) -> bool {
        let Some(index) = self.reservations.iter().position(|reservation| {
            reservation.reservation_id == reservation_id
                && reservation.state == ReservationState::Reserved
        }) else {
            return false;
        };
        self.reservations[index].state = ReservationState::Released;
        self.reservations[index].unresolved = false;
        self.recompute();
        self.updated_at = now;
        true
    }

    /// Bound the ledger without ever discarding a live reservation. Only
    /// terminal entries are eligible for pruning; a live `Reserved` entry is
    /// always retained, because dropping one would silently release its amount
    /// from `committed` and could let a later spend exceed the hard cap.
    fn prune(&mut self) {
        if self.reservations.len() <= MAX_RESERVATIONS {
            return;
        }
        let mut excess = self.reservations.len() - MAX_RESERVATIONS;
        let mut index = 0;
        while index < self.reservations.len() && excess > 0 {
            if self.reservations[index].state != ReservationState::Reserved {
                self.reservations.remove(index);
                excess -= 1;
            } else {
                index += 1;
            }
        }
        // Any remaining excess is all live reservations: correctness (never
        // silently release an outstanding bounded spend) outranks the bound.
    }

    /// A bounded, serializable projection for receipts and CLI output.
    pub fn receipt(&self) -> MissionBudgetReceipt {
        MissionBudgetReceipt {
            status: self.status.as_str().to_string(),
            origin: self.origin.as_str().to_string(),
            currency: self.currency.clone(),
            hard_limit_micros: self.hard_limit.as_ref().map(|limit| limit.micros),
            settled_micros: self.settled.micros,
            reserved_micros: self.reserved.micros,
            unresolved_micros: self.unresolved.micros,
            reason: self.reason.clone(),
        }
    }
}

/// A bounded projection of a Mission budget for durable receipts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MissionBudgetReceipt {
    pub status: String,
    pub origin: String,
    pub currency: String,
    pub hard_limit_micros: Option<i64>,
    pub settled_micros: i64,
    pub reserved_micros: i64,
    pub unresolved_micros: i64,
    pub reason: Option<String>,
}

/// A proposed bounded spend.
#[derive(Debug, Clone)]
pub struct SpendRequest<'a> {
    pub action: SpendAction,
    pub operation_id: &'a str,
    pub estimate: CostBasis,
    pub quota: QuotaFacts,
    /// True when a live or already-settled reservation exists for exactly this
    /// operation. The bounded amount is already part of `committed`, so
    /// re-admission is idempotent: it neither double-counts against the hard
    /// limit nor re-checks a quota that already authorized the reservation.
    pub already_reserved: bool,
}

/// The fact status of a quota observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaState {
    /// No authoritative quota fact is known.
    Unknown,
    /// A fact exists but is older than the accepted freshness window.
    Stale,
    /// A fresh authoritative fact reports remaining capacity.
    Available { remaining: u64 },
    /// A fresh authoritative fact reports zero remaining capacity.
    Exhausted,
}

/// The quota facts used by the quota gate. They come only from the descriptive
/// Resource Registry; `ResourceHealth::Available` is never treated as quota.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaFacts {
    pub state: QuotaState,
    pub reset_at: Option<i64>,
    pub provenance: String,
}

impl QuotaFacts {
    pub fn unknown() -> Self {
        Self {
            state: QuotaState::Unknown,
            reset_at: None,
            provenance: "unknown".to_string(),
        }
    }
}

/// Read the quota fact for a resource from the registry. Fail-soft: a missing
/// or unreadable registry is simply Unknown, never an optimistic value.
pub fn quota_facts(root: &Path, identity: &ResourceIdentity, now: i64) -> QuotaFacts {
    let loaded = crate::resources::load(root);
    let Some(record) = loaded.registry.resource(&ResourceId::derive(identity)) else {
        return QuotaFacts::unknown();
    };
    let fact = &record.quota;
    if fact.provenance == ResourceProvenance::Unknown {
        return QuotaFacts::unknown();
    }
    let Some(value) = fact.value.as_ref() else {
        return QuotaFacts::unknown();
    };
    let observed_at = fact.observed_at.filter(|at| *at != 0);
    let age_reference = observed_at.unwrap_or(record.updated_at);
    let provenance = fact.provenance.as_str().to_string();
    if observed_at.is_some() || record.updated_at != 0 {
        let age = now.saturating_sub(age_reference);
        if age > crate::resources::DEFAULT_STALE_AFTER_SECONDS {
            return QuotaFacts {
                state: QuotaState::Stale,
                reset_at: value.reset_at,
                provenance,
            };
        }
    }
    let state = match value.remaining {
        Some(0) => QuotaState::Exhausted,
        Some(remaining) => QuotaState::Available { remaining },
        None => QuotaState::Unknown,
    };
    QuotaFacts {
        state,
        reset_at: value.reset_at,
        provenance,
    }
}

/// Deterministic reservation identity. Replaying the same operation always maps
/// to the same reservation, so a retry cannot reserve twice.
pub fn reservation_id(
    action: SpendAction,
    mission_id: &str,
    generation: u32,
    operation_id: &str,
) -> String {
    let key = format!(
        "ocg-reservation-v1|{mission_id}|{generation}|{}|{operation_id}",
        action.as_str()
    );
    let digest = crate::runtime::hash::sha256_hex(key.as_bytes());
    format!("rsv-{}", digest.get(..16).unwrap_or(&digest))
}

/// Evaluate the mandatory economic admission for one proposed bounded spend.
///
/// This is pure. It evaluates every applicable block, keeps all of them
/// (bounded), and aggregates by a strict precedence so a hard cap and an
/// exhausted quota are both visible rather than one hiding the other.
pub fn admit(
    budget: &MissionBudget,
    require_quota: bool,
    request: &SpendRequest<'_>,
) -> SpendAssessment {
    let mut blocks: Vec<SpendBlock> = Vec::new();
    // A replay of an operation that already holds a reservation (or already
    // settled) is already accounted in `committed`. Re-adding its amount would
    // double-count and spuriously deny the retry, and its quota was already
    // checked when the reservation was first granted.
    let already = request.already_reserved;

    match budget.hard_limit.as_ref() {
        None => {}
        Some(limit) => {
            if budget.currency.is_empty() {
                blocks.push(SpendBlock::deny(
                    REASON_CURRENCY,
                    "the Mission has a hard budget but no accounting currency",
                ));
            } else if limit.currency != budget.currency {
                blocks.push(SpendBlock::deny(
                    REASON_CURRENCY,
                    format!(
                        "the hard limit currency {} differs from the Mission budget currency {}; no FX conversion is performed",
                        limit.currency, budget.currency
                    ),
                ));
            } else {
                if budget.settled.micros > limit.micros {
                    blocks.push(SpendBlock::deny(
                        REASON_BREACHED,
                        format!(
                            "settled spend {} exceeds the hard Mission budget {}; the overage is recorded and further paid work is denied",
                            budget.settled.micros, limit.micros
                        ),
                    ));
                }
                match &request.estimate {
                    CostBasis::Unknown if !already => blocks.push(SpendBlock::defer(
                        REASON_COST_UNKNOWN,
                        "the cost of this provider-costly action is unknown; refusing to authorize spend against a hard budget",
                    )),
                    CostBasis::Unknown => {}
                    CostBasis::Estimated(amount) => {
                        if amount.currency != limit.currency {
                            blocks.push(SpendBlock::deny(
                                REASON_CURRENCY,
                                format!(
                                    "the estimated cost currency {} differs from the hard limit currency {}; no FX conversion is performed",
                                    amount.currency, limit.currency
                                ),
                            ));
                        } else {
                            let next = if already {
                                budget.committed().micros
                            } else {
                                budget.committed().micros.saturating_add(amount.micros)
                            };
                            if next > limit.micros {
                                blocks.push(SpendBlock::deny(
                                    REASON_HARD_LIMIT,
                                    format!(
                                        "reserving {} would raise committed spend to {} above the hard Mission budget {}",
                                        amount.micros, next, limit.micros
                                    ),
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    if require_quota && !already {
        match &request.quota.state {
            QuotaState::Available { remaining } if *remaining > 0 => {}
            QuotaState::Exhausted => blocks.push(SpendBlock::defer(
                REASON_QUOTA_EXHAUSTED,
                format!(
                    "the associated resource quota is exhausted{}",
                    reset_suffix(request.quota.reset_at)
                ),
            )),
            QuotaState::Stale => blocks.push(SpendBlock::defer(
                REASON_QUOTA_STALE,
                format!(
                    "the latest quota fact is stale and is not a current fact{}",
                    reset_suffix(request.quota.reset_at)
                ),
            )),
            QuotaState::Unknown | QuotaState::Available { .. } => blocks.push(SpendBlock::defer(
                REASON_QUOTA_UNKNOWN,
                "a quota check is required but no authoritative quota fact is known",
            )),
        }
    }

    blocks.truncate(MAX_SPEND_BLOCKS);

    let primary = blocks
        .iter()
        .max_by_key(|block| block.decision.precedence())
        .cloned();
    let decision = primary
        .as_ref()
        .map(|block| block.decision)
        .unwrap_or(SpendDecision::Allow);
    let reason_code = match &primary {
        Some(block) => block.reason_code.clone(),
        None => {
            if budget.hard_limit.is_some() {
                REASON_ALLOWED.to_string()
            } else {
                REASON_UNCONFIGURED.to_string()
            }
        }
    };
    let reason = match &primary {
        Some(block) => block.reason.clone(),
        None if budget.hard_limit.is_some() => {
            "the bounded spend is within the hard Mission budget".to_string()
        }
        None => "no hard Mission budget is configured".to_string(),
    };

    // `already` means no new reservation is recorded, so there is no new amount
    // to reserve even when the replayed action is allowed.
    let amount = if decision == SpendDecision::Allow && budget.hard_limit.is_some() && !already {
        match &request.estimate {
            CostBasis::Estimated(amount) if !amount.is_zero() => Some(amount.clone()),
            _ => None,
        }
    } else {
        None
    };

    SpendAssessment {
        decision,
        reason_code,
        reason,
        blocks,
        origin: budget.origin,
        status: budget.status,
        currency: budget.currency.clone(),
        hard_limit: budget.hard_limit.clone(),
        committed: budget.committed(),
        amount,
        reservation_id: None,
    }
}

fn reset_suffix(reset_at: Option<i64>) -> String {
    match reset_at {
        Some(at) => format!(" (expected reset at {at})"),
        None => String::new(),
    }
}

/// Declarative `budget` configuration.
///
/// There is deliberately no `enabled` flag that could disable enforcement of a
/// configured `hardLimitMicros`: a configured hard limit is always enforced.
/// The absence of a limit is the absence of a cap, not a bypass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct BudgetConfig {
    pub currency: Option<String>,
    /// A default hard Mission budget in micro-units, materialized once into each
    /// Mission that does not already have an explicit budget.
    pub hard_limit_micros: Option<i64>,
    /// The bounded pre-authorization estimate for a provider-costly operation.
    /// Without it, a hard-budgeted provider-costly action is deferred rather
    /// than assumed free.
    pub estimated_operation_cost_micros: Option<i64>,
    /// Whether a fresh, authoritative quota fact is required before a
    /// provider-costly action. Unknown quota then defers rather than assuming
    /// unlimited capacity.
    pub require_quota: bool,
}

impl BudgetConfig {
    /// Parse the top-level `budget` section, falling back to the defaults.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(value) = data.get("budget") else {
            return Ok(Self::default());
        };
        if value.is_null() {
            return Ok(Self::default());
        }
        let object = value.as_object().ok_or_else(|| {
            OcgError::config("budget must be a JSON object with currency and hardLimitMicros")
        })?;
        let mut config = Self::default();
        if let Some(currency) = object.get("currency") {
            let raw = currency
                .as_str()
                .ok_or_else(|| OcgError::config("budget.currency must be a string"))?;
            config.currency = Some(normalize_currency(raw)?);
        }
        for (key, slot, label) in [
            (
                "hardLimitMicros",
                &mut config.hard_limit_micros,
                "budget.hardLimitMicros",
            ),
            (
                "estimatedOperationCostMicros",
                &mut config.estimated_operation_cost_micros,
                "budget.estimatedOperationCostMicros",
            ),
        ] {
            if let Some(value) = object.get(key) {
                if value.is_null() {
                    continue;
                }
                let amount = value.as_i64().ok_or_else(|| {
                    OcgError::config(format!("{label} must be a positive integer"))
                })?;
                *slot = Some(amount);
            }
        }
        if let Some(value) = object.get("requireQuota") {
            config.require_quota = value
                .as_bool()
                .ok_or_else(|| OcgError::config("budget.requireQuota must be a boolean"))?;
        }
        config.validate_values()?;
        Ok(config)
    }

    /// Collect every budget problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    fn validate_values(&self) -> Result<()> {
        if let Some(currency) = self.currency.as_deref() {
            normalize_currency(currency)?;
        }
        if let Some(limit) = self.hard_limit_micros {
            if limit <= 0 {
                return Err(OcgError::config(
                    "budget.hardLimitMicros must be a positive integer",
                ));
            }
            if self.currency.is_none() {
                return Err(OcgError::config(
                    "budget.hardLimitMicros requires budget.currency (OCG never guesses a currency)",
                ));
            }
        }
        if let Some(estimate) = self.estimated_operation_cost_micros {
            if estimate <= 0 {
                return Err(OcgError::config(
                    "budget.estimatedOperationCostMicros must be a positive integer",
                ));
            }
            if self.currency.is_none() {
                return Err(OcgError::config(
                    "budget.estimatedOperationCostMicros requires budget.currency",
                ));
            }
        }
        Ok(())
    }

    /// The cost basis for a provider-costly action. Unknown unless a bounded
    /// estimate and a currency are configured. It is never defaulted.
    pub fn estimated_cost(&self) -> CostBasis {
        match (self.estimated_operation_cost_micros, self.currency.as_ref()) {
            (Some(micros), Some(currency)) => {
                CostBasis::Estimated(Money::new(micros, currency.clone()))
            }
            _ => CostBasis::Unknown,
        }
    }

    /// A stable fingerprint of the budget configuration.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::runtime::hash::sha256_hex(value.to_string().as_bytes())
    }
}

fn bounded(text: &str) -> String {
    let redacted = crate::telemetry::task::redact(text);
    if redacted.len() <= MAX_REASON_BYTES {
        return redacted;
    }
    let mut end = MAX_REASON_BYTES;
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &redacted[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn configured_limit(micros: i64, currency: &str) -> BudgetConfig {
        BudgetConfig {
            currency: Some(currency.to_string()),
            hard_limit_micros: Some(micros),
            estimated_operation_cost_micros: Some(micros / 10),
            require_quota: false,
        }
    }

    fn request<'a>(estimate: &'a CostBasis, quota: QuotaFacts) -> SpendRequest<'a> {
        SpendRequest {
            action: SpendAction::ResumeContinuation,
            operation_id: "op-1",
            estimate: estimate.clone(),
            quota,
            already_reserved: false,
        }
    }

    #[test]
    fn replaying_a_live_reservation_does_not_double_count() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(100_000, "USD"));
        let estimate = CostBasis::Estimated(Money::new(60_000, "USD"));
        assert!(admit(&budget, false, &request(&estimate, QuotaFacts::unknown())).is_allowed());
        assert!(budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            Money::new(60_000, "USD"),
            1
        ));
        // Replaying the exact operation while its reservation is live must stay
        // allowed: its amount is already in `committed`.
        let mut replay = request(&estimate, QuotaFacts::unknown());
        replay.already_reserved = true;
        let assessment = admit(&budget, true, &replay);
        assert!(assessment.is_allowed(), "{:?}", assessment.blocks);
        assert!(
            assessment.amount.is_none(),
            "no new reservation for a replay"
        );
        // And a distinct operation still cannot fit.
        let mut second = request(&estimate, QuotaFacts::unknown());
        second.operation_id = "op-2";
        assert_eq!(
            admit(&budget, false, &second).reason_code,
            REASON_HARD_LIMIT
        );
    }

    #[test]
    fn unconfigured_budget_allows_and_never_reserves() {
        let budget = MissionBudget::default();
        let config = BudgetConfig::default();
        let assessment = admit(
            &budget,
            config.require_quota,
            &request(&CostBasis::Unknown, QuotaFacts::unknown()),
        );
        assert_eq!(assessment.decision, SpendDecision::Allow);
        assert_eq!(assessment.reason_code, REASON_UNCONFIGURED);
        assert!(assessment.amount.is_none());
        assert_eq!(assessment.status, BudgetStatus::Unconfigured);
    }

    #[test]
    fn configured_hard_limit_materializes_once_and_is_never_silently_changed() {
        let mut budget = MissionBudget::default();
        let config = configured_limit(1_000_000, "USD");
        assert!(budget.materialize_config(&config));
        assert!(!budget.materialize_config(&config));
        assert_eq!(budget.hard_limit, Some(Money::new(1_000_000, "USD")));
        assert_eq!(budget.origin, BudgetOrigin::SystemDefault);
        assert_eq!(budget.status, BudgetStatus::Active);

        // A later config edit never silently changes the durable limit.
        let higher = configured_limit(9_000_000, "USD");
        assert!(!budget.materialize_config(&higher));
        assert_eq!(budget.hard_limit, Some(Money::new(1_000_000, "USD")));
    }

    #[test]
    fn a_spend_over_the_hard_limit_is_denied_not_clamped() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(100_000, "USD"));
        let estimate = CostBasis::Estimated(Money::new(60_000, "USD"));
        let first = admit(&budget, false, &request(&estimate, QuotaFacts::unknown()));
        assert!(first.is_allowed());
        assert_eq!(first.amount, Some(Money::new(60_000, "USD")));

        // Reserve it.
        assert!(budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            first.amount.clone().unwrap(),
            11
        ));
        assert_eq!(budget.status, BudgetStatus::Active);
        assert_eq!(budget.reserved.micros, 60_000);

        // A second, distinct operation cannot fit.
        let second_estimate = CostBasis::Estimated(Money::new(60_000, "USD"));
        let mut second = request(&second_estimate, QuotaFacts::unknown());
        second.operation_id = "op-2";
        let second = admit(&budget, false, &second);
        assert_eq!(second.decision, SpendDecision::Deny);
        assert_eq!(second.reason_code, REASON_HARD_LIMIT);
    }

    #[test]
    fn unknown_cost_defers_when_a_hard_limit_applies() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(100_000, "USD"));
        let assessment = admit(
            &budget,
            false,
            &request(&CostBasis::Unknown, QuotaFacts::unknown()),
        );
        assert_eq!(assessment.decision, SpendDecision::Defer);
        assert_eq!(assessment.reason_code, REASON_COST_UNKNOWN);
        assert!(assessment.amount.is_none());
    }

    #[test]
    fn currency_mismatch_is_denied_and_never_converted() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(100_000, "USD"));
        let estimate = CostBasis::Estimated(Money::new(10_000, "EUR"));
        let assessment = admit(&budget, false, &request(&estimate, QuotaFacts::unknown()));
        assert_eq!(assessment.decision, SpendDecision::Deny);
        assert_eq!(assessment.reason_code, REASON_CURRENCY);
    }

    #[test]
    fn settle_records_an_overage_as_a_breach_without_clamping() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(100_000, "USD"));
        assert!(budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            Money::new(10_000, "USD"),
            5
        ));
        assert!(budget
            .settle(
                &reservation_id(SpendAction::ResumeContinuation, "task-1", 1, "op-1"),
                Some(Money::new(150_000, "USD")),
                6
            )
            .unwrap());
        assert_eq!(budget.settled.micros, 150_000);
        assert_eq!(budget.status, BudgetStatus::Breached);
        // Further admission is denied because settled already exceeds the cap.
        let estimate = CostBasis::Estimated(Money::new(1, "USD"));
        let assessment = admit(&budget, false, &request(&estimate, QuotaFacts::unknown()));
        assert_eq!(assessment.decision, SpendDecision::Deny);
    }

    #[test]
    fn reserve_and_settle_are_idempotent() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(1_000_000, "USD"));
        assert!(budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            Money::new(10_000, "USD"),
            5
        ));
        // A duplicate reserve for the same operation is a no-op.
        assert!(!budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            Money::new(10_000, "USD"),
            5
        ));
        assert_eq!(budget.reserved.micros, 10_000);
        let id = reservation_id(SpendAction::ResumeContinuation, "task-1", 1, "op-1");
        assert!(budget.settle(&id, None, 6).unwrap());
        assert!(!budget.settle(&id, None, 7).unwrap());
        assert_eq!(budget.settled.micros, 10_000);
        // A settled reservation is reused, never doubled.
        assert!(!budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            Money::new(10_000, "USD"),
            8
        ));
        assert_eq!(budget.reserved.micros, 0);
        assert_eq!(budget.settled.micros, 10_000);
    }

    #[test]
    fn uncertain_dispatch_keeps_the_reservation_and_release_drops_it() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(1_000_000, "USD"));
        let id = reservation_id(SpendAction::ResumeContinuation, "task-1", 1, "op-1");
        assert!(budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            Money::new(10_000, "USD"),
            5
        ));
        assert!(budget.mark_unresolved(&id, 6));
        assert_eq!(budget.reserved.micros, 10_000);
        assert_eq!(budget.unresolved.micros, 10_000);
        assert!(budget.release(&id, 7));
        assert_eq!(budget.reserved.micros, 0);
        assert_eq!(budget.unresolved.micros, 0);
    }

    #[test]
    fn quota_gate_defers_on_exhausted_unknown_and_stale() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&BudgetConfig {
            require_quota: false,
            ..configured_limit(1_000_000, "USD")
        });
        let estimate = CostBasis::Estimated(Money::new(1_000, "USD"));

        let exhausted = QuotaFacts {
            state: QuotaState::Exhausted,
            reset_at: Some(99),
            provenance: "provider_reported".to_string(),
        };
        let assessment = admit(&budget, true, &request(&estimate, exhausted));
        assert_eq!(assessment.decision, SpendDecision::Defer);
        assert_eq!(assessment.reason_code, REASON_QUOTA_EXHAUSTED);
        assert!(assessment.reason.contains("99"));

        let unknown = admit(&budget, true, &request(&estimate, QuotaFacts::unknown()));
        assert_eq!(unknown.reason_code, REASON_QUOTA_UNKNOWN);

        let stale = QuotaFacts {
            state: QuotaState::Stale,
            reset_at: None,
            provenance: "runtime_observed".to_string(),
        };
        assert_eq!(
            admit(&budget, true, &request(&estimate, stale)).reason_code,
            REASON_QUOTA_STALE
        );

        let available = QuotaFacts {
            state: QuotaState::Available { remaining: 5 },
            reset_at: None,
            provenance: "provider_reported".to_string(),
        };
        assert!(admit(&budget, true, &request(&estimate, available)).is_allowed());
    }

    #[test]
    fn co_firing_blocks_are_all_retained() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(100_000, "USD"));
        let estimate = CostBasis::Estimated(Money::new(150_000, "USD"));
        let quota = QuotaFacts {
            state: QuotaState::Exhausted,
            reset_at: Some(3),
            provenance: "provider_reported".to_string(),
        };
        let assessment = admit(&budget, true, &request(&estimate, quota));
        assert_eq!(assessment.decision, SpendDecision::Deny);
        assert_eq!(assessment.reason_code, REASON_HARD_LIMIT);
        let codes: Vec<&str> = assessment
            .blocks
            .iter()
            .map(|block| block.reason_code.as_str())
            .collect();
        assert!(codes.contains(&REASON_QUOTA_EXHAUSTED), "{codes:?}");
        assert!(codes.contains(&REASON_HARD_LIMIT), "{codes:?}");
    }

    #[test]
    fn pruning_never_drops_a_live_reservation() {
        let mut budget = MissionBudget::default();
        budget.materialize_config(&configured_limit(1_000_000_000, "USD"));
        let total = MAX_RESERVATIONS + 8;
        for index in 0..total {
            assert!(budget.reserve(
                SpendAction::ResumeContinuation,
                "task-1",
                1,
                &format!("op-{index}"),
                Money::new(1_000, "USD"),
                10,
            ));
        }
        // Every live reservation is retained: dropping one would silently
        // release its amount from `committed` and could let spend exceed the cap.
        assert_eq!(budget.reservations.len(), total);
        assert_eq!(
            budget
                .reservations
                .iter()
                .filter(|reservation| reservation.state == ReservationState::Reserved)
                .count(),
            total
        );
        assert_eq!(budget.reserved.micros, (total as i64) * 1_000);
    }

    #[test]
    fn config_validation_rejects_unknown_currency_and_missing_currency() {
        assert!(BudgetConfig::from_config(&json!({})).unwrap() == BudgetConfig::default());
        assert!(BudgetConfig::from_config(&json!({
            "budget": {"hardLimitMicros": 100, "currency": "usd"}
        }))
        .is_ok());
        assert!(BudgetConfig::from_config(&json!({
            "budget": {"hardLimitMicros": 100}
        }))
        .is_err());
        assert!(BudgetConfig::from_config(&json!({
            "budget": {"currency": "us$", "hardLimitMicros": 100}
        }))
        .is_err());
        assert!(BudgetConfig::from_config(&json!({
            "budget": {"hardLimitMicros": 0, "currency": "USD"}
        }))
        .is_err());
        assert!(BudgetConfig::from_config(&json!({
            "budget": {"requireQuota": "yes"}
        }))
        .is_err());
        assert!(
            BudgetConfig::validate(&json!({"budget": {"hardLimitMicros": -5, "currency": "USD"}}))
                .len()
                == 1
        );
    }

    #[test]
    fn explicit_limit_overrides_and_survives_config_changes() {
        let mut budget = MissionBudget::default();
        assert!(budget
            .set_hard_limit(Money::new(500_000, "usd"), 1)
            .unwrap());
        assert_eq!(budget.hard_limit, Some(Money::new(500_000, "USD")));
        assert_eq!(budget.origin, BudgetOrigin::ExplicitUserLimit);
        // A system default can no longer override it.
        assert!(!budget.materialize_config(&configured_limit(9_000_000, "USD")));
        assert_eq!(budget.hard_limit, Some(Money::new(500_000, "USD")));
        // And it refuses a contradictory currency after accounting began.
        let id = reservation_id(SpendAction::ResumeContinuation, "task-1", 1, "op-1");
        assert!(budget.reserve(
            SpendAction::ResumeContinuation,
            "task-1",
            1,
            "op-1",
            Money::new(1_000, "USD"),
            2
        ));
        assert!(budget.settle(&id, None, 3).unwrap());
        assert!(budget.set_hard_limit(Money::new(1, "EUR"), 4).is_err());
    }

    #[test]
    fn configured_estimate_is_only_produced_with_a_currency() {
        let config = BudgetConfig {
            currency: Some("USD".to_string()),
            hard_limit_micros: Some(10),
            estimated_operation_cost_micros: Some(5),
            require_quota: false,
        };
        assert_eq!(
            config.estimated_cost(),
            CostBasis::Estimated(Money::new(5, "USD"))
        );
        let config = BudgetConfig::default();
        assert_eq!(config.estimated_cost(), CostBasis::Unknown);
        assert_ne!(
            configured_limit(1, "USD").fingerprint(),
            configured_limit(2, "USD").fingerprint()
        );
    }
}
