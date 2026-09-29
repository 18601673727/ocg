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
//! # Reservation, actual, released, unresolved
//!
//! Four states of money are kept strictly apart, because collapsing any two of
//! them would hide either a real spend or a real refund:
//!
//! - **reserved** — a bounded amount held before the provider runs. It counts
//!   against the hard cap for as long as it is held.
//! - **actual** — canonical Money valued from a *provider-reported* usage
//!   record. It only exists when OCG holds a canonical price for the route and
//!   every component that price bills was reported. There is no other source of
//!   truth for what a provider actually charged.
//! - **released** — money that provably never left OCG, plus the difference
//!   between a reservation and a smaller actual. It returns to the Project's
//!   headroom.
//! - **unresolved** — money that may or may not have been spent, with no
//!   reliable actual. The reservation is *retained* and still counts, because
//!   releasing it would hand back money nobody can prove was never charged.
//!
//! Reported usage, a locally estimated cost and a reserved maximum are three
//! different things and never stand in for one another. Unknown usage is not
//! zero, and it is not the reservation either.
//!
//! # The Project is the budget scope
//!
//! The ledger is keyed by Project. There is no Mission budget: a Mission holds
//! no money and can neither authorize nor account for a provider Call.
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
/// Upper bound on retained settlement records per Mission. This bounds a
/// recent, inspectable view only: the authoritative accumulators are never
/// derived from this list, and the durable `domain_settlements` table keeps
/// every settlement ever recorded, so the 65th settlement and every later one
/// settle, participate in idempotence and conflict detection, and survive a
/// process reopen exactly like the first 64.
pub const MAX_SETTLEMENTS: usize = 64;
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

/// A provider-reported usage record was priced into canonical Money.
pub const REASON_ACTUAL_REPORTED: &str = "project_actual_usage_priced";
/// The provider reported usage but OCG holds no canonical price for it, so no
/// actual is fabricated.
pub const REASON_USAGE_UNPRICED: &str = "project_provider_usage_unpriced";
/// The provider returned no usage at all. Unknown usage is never zero.
pub const REASON_USAGE_ABSENT: &str = "project_provider_usage_absent";
/// A component the price bills was not reported, or its non-overlapping
/// quantity could not be normalized from what was reported, so the actual is not
/// reliable.
pub const REASON_USAGE_INCOMPLETE: &str = "project_provider_usage_incomplete";
/// The price currency differs from the accounting currency. OCG never converts.
pub const REASON_USAGE_CURRENCY: &str = "project_usage_price_currency_mismatch";
/// The request provably never left OCG, so the whole reservation is returned.
pub const REASON_NOT_DISPATCHED: &str = "project_reservation_released_not_dispatched";
/// The money may or may not have been spent. The reservation is retained.
pub const REASON_EFFECT_UNKNOWN: &str = "project_reservation_unresolved_effect_unknown";
/// Usage arrived under an Attempt that no longer holds authority, so it is
/// retained as evidence and the reservation stays unresolved.
pub const REASON_STALE_AUTHORITY: &str = "project_usage_retained_stale_authority";
/// A losing Attempt asked to release a reservation. Releasing hands money back,
/// so it is refused and the reservation is retained as unresolved instead.
pub const REASON_RELEASE_UNAUTHORIZED: &str = "project_reservation_release_requires_authority";
/// The recorded actual exceeded the reserved maximum. It is recorded in full.
pub const REASON_OVERAGE: &str = "project_actual_exceeded_reservation";
/// The dispatch never held a reservation, so there is nothing to settle.
pub const REASON_NO_RESERVATION: &str = "project_dispatch_has_no_reservation";
/// The same settlement identity arrived carrying a different canonical
/// payload. It is retained as evidence and never booked: idempotence identity
/// is not payload equality, and a conflicting fact must not silently replace
/// or silently disappear as an ordinary duplicate.
pub const REASON_SETTLEMENT_CONFLICT: &str = "project_settlement_conflict";

/// The exact-match wildcard accepted for a price's provider or model.
pub const PRICE_WILDCARD: &str = "*";
/// The number of tokens one configured rate is quoted per.
pub const TOKENS_PER_PRICE_UNIT: i64 = 1_000_000;
/// Upper bound on one per-million-token rate. It keeps the money conversion
/// inside `i128` and rejects an obviously misconfigured price.
pub const MAX_RATE_MICROS_PER_MILLION: i64 = 1_000_000_000_000;

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

// ---------------------------------------------------------------------------
// Provider usage and its canonical Money valuation
//
// Reported usage, locally estimated usage and reserved maximums are three
// different things. Only a provider-reported usage record that OCG can price in
// the ledger's own currency becomes an `actual`; everything else leaves the
// reservation outstanding and unresolved rather than inventing a number.
//
// Valuation runs over one direction only:
//
// ```text
// UsageRecord    what the provider stated, in the provider's idiom
//     |          (retained as evidence; overlap between its counters is unknown)
//     v
// BillableUsage  disjoint billable quantities, produced by the provider adapter
//     |
//     v
// Money          TokenPrice::price_usage, exact fixed-point integer arithmetic
// ```
//
// `actual` is that valuation and nothing more: the canonical cost of accepted
// provider-reported usage under the pricing basis frozen for the dispatch. It
// is not a provider invoice, a card charge, or a discount-, tax- or
// FX-adjusted total, and no part of this layer reconciles against one.
// ---------------------------------------------------------------------------

/// Where a usage record came from. Usage is only ever provider-reported here:
/// OCG has no other source of truth for what a provider actually billed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageSource {
    #[default]
    ProviderReported,
}

/// The token counters a provider reported for one request, in the provider's
/// own idiom.
///
/// Every counter stays `None` when the provider omitted it. An omitted counter
/// is never fabricated as zero, because zero is a fact the provider did not
/// state.
///
/// These counters are *not* a pricing basis. Whether `input_tokens` already
/// contains `cache_read_tokens`, and whether `output_tokens` already contains
/// `reasoning_tokens`, is a property of the reporting provider rather than of
/// this record: one model publishes `prompt_tokens` as
/// `uncached_input + cached_input`, another publishes it as `uncached_input`
/// alone, and both look identical here. Feeding these counters straight into a
/// price would therefore bill a cached token at the input rate *and* at the
/// cache rate for one provider while under-billing the other. A record like
/// this one is retained as evidence and as the input to normalization; what
/// gets priced is [`BillableUsage`], which the provider adapter produces.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UsageRecord {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    /// A provider-reported subset of `output_tokens`. It is recorded for audit
    /// and never added on top of the output total, which would bill the same
    /// tokens twice.
    pub reasoning_tokens: Option<u64>,
    pub source: UsageSource,
}

impl UsageRecord {
    /// Whether the provider stated any counter at all.
    pub fn is_reported(&self) -> bool {
        self.input_tokens.is_some()
            || self.output_tokens.is_some()
            || self.cache_read_tokens.is_some()
            || self.cache_write_tokens.is_some()
            || self.reasoning_tokens.is_some()
    }

    /// The input plus output totals, when both were reported. The provider's own
    /// `total` is never trusted over the two counters it derives from.
    pub fn total_tokens(&self) -> Option<u64> {
        match (self.input_tokens, self.output_tokens) {
            (Some(input), Some(output)) => Some(input.saturating_add(output)),
            _ => None,
        }
    }
}

/// The quantities one price may bill without counting any token twice.
///
/// This is the only usage shape the pricing layer accepts. Each component is a
/// disjoint bucket of tokens: the input component holds exactly the tokens that
/// are billed at the input rate, so a token priced as a cache read is by
/// construction absent from it. The provider adapter owns the normalization that
/// turns a [`UsageRecord`] into these quantities, because only the adapter knows
/// whether a given provider's counters are inclusive or exclusive; the pricing
/// layer never has to.
///
/// A component stays `None` when the adapter cannot establish it — the provider
/// omitted it, or reported an inclusive total without the counters that would
/// have to be carved out of it. `None` is unknown, never zero: a price that
/// bills an unknown component yields [`PriceOutcome::Unpriced`] rather than a
/// guess.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BillableUsage {
    /// Tokens billed at the input rate, with every separately priced cache
    /// component already removed.
    pub input_tokens: Option<u64>,
    /// Tokens billed at the output rate. Providers report this total inclusive
    /// of reasoning, and a single output rate prices text and reasoning alike,
    /// so the inclusive total is already the right quantity.
    pub output_tokens: Option<u64>,
    /// Tokens billed at the cache-read rate. Disjoint from
    /// [`Self::input_tokens`].
    pub cache_read_tokens: Option<u64>,
    /// Tokens billed at the cache-write rate. Disjoint from
    /// [`Self::input_tokens`].
    pub cache_write_tokens: Option<u64>,
}

impl BillableUsage {
    /// Whether the adapter established any billable quantity at all.
    pub fn is_reported(&self) -> bool {
        self.input_tokens.is_some()
            || self.output_tokens.is_some()
            || self.cache_read_tokens.is_some()
            || self.cache_write_tokens.is_some()
    }
}

/// A canonical per-token price for one provider route.
///
/// Rates are integer micro-units of `currency` per [`TOKENS_PER_PRICE_UNIT`]
/// tokens. A rate of `0` declares that component unbilled, so an unreported
/// counter for it is deterministic; a non-zero rate declares that the provider
/// must have reported it for the actual amount to be reliable.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TokenPrice {
    pub provider: String,
    pub model: String,
    pub currency: String,
    pub input_micros_per_million: i64,
    pub output_micros_per_million: i64,
    pub cache_read_micros_per_million: i64,
    pub cache_write_micros_per_million: i64,
}

/// The reason a usage record could not become canonical Money.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceRefusal {
    pub reason_code: &'static str,
    pub reason: String,
}

/// The result of valuing one usage record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PriceOutcome {
    /// The usage was fully priced into canonical Money.
    Priced(Money),
    /// The usage exists but no reliable actual can be derived from it.
    Unpriced(PriceRefusal),
}

impl PriceOutcome {
    pub fn is_priced(&self) -> bool {
        matches!(self, Self::Priced(_))
    }

    /// The canonical actual, or `None` when the usage could not be valued.
    pub fn actual(&self) -> Option<Money> {
        match self {
            Self::Priced(money) => Some(money.clone()),
            Self::Unpriced(_) => None,
        }
    }
}

impl TokenPrice {
    /// Whether this price covers the route at a given specificity: `2` is an
    /// exact provider *and* model match, `1` an exact provider with a wildcard
    /// model, `0` a fully wildcarded fallback.
    fn covers(&self, provider: &str, model: &str, level: u8) -> bool {
        let provider_ok = match level {
            0 => self.provider == PRICE_WILDCARD,
            _ => self.provider == provider,
        };
        let model_ok = match level {
            0 | 1 => self.model == PRICE_WILDCARD,
            _ => self.model == model,
        };
        provider_ok && model_ok
    }

    /// The rates this price actually bills.
    fn rates(&self) -> [i64; 4] {
        [
            self.input_micros_per_million,
            self.output_micros_per_million,
            self.cache_read_micros_per_million,
            self.cache_write_micros_per_million,
        ]
    }

    /// Value one set of billable quantities in canonical Money.
    ///
    /// The arithmetic is exact fixed-point integer math rounded half away from
    /// zero, so the same usage against the same price always yields the same
    /// Money and no binary float ever touches a monetary value. Nothing here
    /// converts currency: a price in another currency is refused.
    ///
    /// The input is [`BillableUsage`] rather than a [`UsageRecord`] on purpose:
    /// the components are disjoint by construction, so no token can be billed at
    /// two rates, and which provider folds which counter into which is the
    /// adapter's concern, not this function's. A component this price bills that
    /// normalization could not establish yields [`PriceOutcome::Unpriced`]; an
    /// unbilled component (rate `0`) contributes nothing whether or not it was
    /// established, which keeps the actual deterministic.
    pub fn price_usage(&self, usage: &BillableUsage, accounting_currency: &str) -> PriceOutcome {
        if !accounting_currency.is_empty() && accounting_currency != self.currency {
            return PriceOutcome::Unpriced(PriceRefusal {
                reason_code: REASON_USAGE_CURRENCY,
                reason: format!(
                    "the configured price is in {} but the Project is accounted in {accounting_currency}; no FX conversion is performed",
                    self.currency
                ),
            });
        }
        let rates = self.rates();
        if rates.iter().any(|rate| *rate != 0) && !usage.is_reported() {
            return PriceOutcome::Unpriced(PriceRefusal {
                reason_code: REASON_USAGE_ABSENT,
                reason: format!(
                    "the provider for {}/{} reported no token usage; unknown usage is not zero, so the reservation stays unresolved",
                    self.provider, self.model
                ),
            });
        }
        let components: [(Option<u64>, i64, &str); 4] = [
            (usage.input_tokens, rates[0], "input"),
            (usage.output_tokens, rates[1], "output"),
            (usage.cache_read_tokens, rates[2], "cache-read"),
            (usage.cache_write_tokens, rates[3], "cache-write"),
        ];
        let mut total: i64 = 0;
        for (tokens, rate, label) in components {
            if rate == 0 {
                // An unbilled component contributes nothing whether or not the
                // provider reported it, so the actual stays deterministic.
                continue;
            }
            let Some(tokens) = tokens else {
                return PriceOutcome::Unpriced(PriceRefusal {
                    reason_code: REASON_USAGE_INCOMPLETE,
                    reason: format!(
                        "the billable {label} tokens for {}/{} could not be established from what the provider reported, and this price bills them; the actual would be a guess",
                        self.provider, self.model
                    ),
                });
            };
            match scaled_micros(tokens, rate) {
                Some(micros) => total = total.saturating_add(micros),
                None => {
                    return PriceOutcome::Unpriced(PriceRefusal {
                        reason_code: REASON_USAGE_INCOMPLETE,
                        reason: format!(
                            "{label} usage for {}/{} is too large to value in one currency unit's micro-units",
                            self.provider, self.model
                        ),
                    })
                }
            }
        }
        PriceOutcome::Priced(Money::new(total, self.currency.clone()))
    }

    /// The most specific configured price for one route. Resolution is
    /// deterministic: an exact route wins over a wildcard model, which wins over
    /// the fully wildcarded fallback.
    pub fn resolve<'a>(
        prices: &'a [TokenPrice],
        provider: &str,
        model: &str,
    ) -> Option<&'a TokenPrice> {
        (0..=2).rev().find_map(|level| {
            prices
                .iter()
                .find(|price| price.covers(provider, model, level))
        })
    }

    fn validate_values(&self) -> Result<()> {
        if self.provider.is_empty() || self.provider.len() > MAX_CURRENCY_BYTES * 4 {
            return Err(OcgError::config(
                "budget.pricing[].provider must be a short non-empty identifier",
            ));
        }
        if self.model.is_empty() || self.model.len() > MAX_CURRENCY_BYTES * 8 {
            return Err(OcgError::config(
                "budget.pricing[].model must be a short non-empty identifier",
            ));
        }
        normalize_currency(&self.currency)?;
        for rate in self.rates() {
            if !(0..=MAX_RATE_MICROS_PER_MILLION).contains(&rate) {
                return Err(OcgError::config(format!(
                    "budget.pricing[] rates must be between 0 and {MAX_RATE_MICROS_PER_MILLION} micro-units per {TOKENS_PER_PRICE_UNIT} tokens"
                )));
            }
        }
        Ok(())
    }

    fn from_config(value: &Value) -> Result<Vec<TokenPrice>> {
        if value.is_null() {
            return Ok(Vec::new());
        }
        let entries = value.as_array().ok_or_else(|| {
            OcgError::config("budget.pricing must be an array of per-token prices")
        })?;
        let mut prices = Vec::with_capacity(entries.len());
        for entry in entries {
            let object = entry
                .as_object()
                .ok_or_else(|| OcgError::config("each budget.pricing entry must be an object"))?;
            let mut price = TokenPrice::default();
            for (key, slot) in [
                ("provider", &mut price.provider),
                ("model", &mut price.model),
                ("currency", &mut price.currency),
            ] {
                if let Some(value) = object.get(key) {
                    *slot = value
                        .as_str()
                        .ok_or_else(|| {
                            OcgError::config(format!("budget.pricing[].{key} must be a string"))
                        })?
                        .trim()
                        .to_string();
                }
            }
            for (key, slot) in [
                ("inputMicrosPerMillion", &mut price.input_micros_per_million),
                (
                    "outputMicrosPerMillion",
                    &mut price.output_micros_per_million,
                ),
                (
                    "cacheReadMicrosPerMillion",
                    &mut price.cache_read_micros_per_million,
                ),
                (
                    "cacheWriteMicrosPerMillion",
                    &mut price.cache_write_micros_per_million,
                ),
            ] {
                if let Some(value) = object.get(key) {
                    if value.is_null() {
                        continue;
                    }
                    *slot = value.as_i64().ok_or_else(|| {
                        OcgError::config(format!("budget.pricing[].{key} must be an integer"))
                    })?;
                }
            }
            price.currency = normalize_currency(&price.currency)?;
            prices.push(price);
        }
        Ok(prices)
    }
}

/// The pricing facts frozen for one dispatch, durably recorded before the
/// provider is ever asked to do anything.
///
/// A settlement must value the usage the provider eventually reports against
/// the price that applied to the route OCG actually dispatched — not against
/// whatever the pricing configuration happens to say a minute later, and not
/// against the route the operator asked for when routing, an alias or a
/// gateway changed it. Freezing the basis here is what makes the canonical
/// Money for a Call a function of the Call rather than of the wall clock: a
/// later pricing revision can never reprice a dispatch that already happened.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PricingBasis {
    /// The provider of the route that was actually dispatched, which is the
    /// route the price was resolved against. It is deliberately not the
    /// provider that was requested: the requested route is already durable in
    /// the dispatch intent's request, and the two are kept apart so a routing
    /// change is visible rather than silently absorbed.
    pub effective_provider: String,
    /// The model of the route that was actually dispatched.
    pub effective_model: String,
    /// The currency the resolved price quotes. Empty when no price resolved.
    pub currency: String,
    /// The price resolved for the effective route. `None` records that OCG held
    /// no price for this dispatch when it was frozen, which is a fact about the
    /// dispatch: a price configured later does not value this Call.
    pub price: Option<TokenPrice>,
    /// A stable fingerprint of the pricing table the price was resolved from:
    /// the revision this settlement is pinned to. It identifies the basis; it is
    /// never used to re-resolve it.
    pub pricing_revision: String,
}

impl PricingBasis {
    /// Resolve the pricing basis for one effective dispatched route from the
    /// configuration that is current *now*, so it can be frozen before the
    /// provider runs. The absence of a price is recorded, never guessed.
    pub fn resolve(config: &BudgetConfig, provider: &str, model: &str) -> Self {
        let price = config.price_for(provider, model);
        Self {
            effective_provider: provider.to_string(),
            effective_model: model.to_string(),
            currency: price
                .map(|price| price.currency.clone())
                .unwrap_or_default(),
            price: price.cloned(),
            pricing_revision: config.pricing_fingerprint(),
        }
    }
}

/// Convert tokens to micro-units at a per-million rate, rounding half away from
/// zero so the same inputs always produce the same integer. `i128` keeps the
/// intermediate exact for every rate the configuration accepts.
fn scaled_micros(tokens: u64, micros_per_million: i64) -> Option<i64> {
    let half = i128::from(TOKENS_PER_PRICE_UNIT) / 2;
    let numerator = i128::from(tokens) * i128::from(micros_per_million) + half;
    i64::try_from(numerator / i128::from(TOKENS_PER_PRICE_UNIT)).ok()
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

/// The accounting disposition one reservation reached.
///
/// The three are deliberately distinct and never collapse into one another:
///
/// - `Settled` — a provider-reported usage record was priced into canonical
///   Money. The actual is stated, and the difference against the reservation is
///   either released or recorded as an overage.
/// - `Released` — the request provably never reached the provider, so the whole
///   reservation returns to the Project's headroom.
/// - `Unresolved` — the money may or may not have been spent and no reliable
///   actual exists. The reservation is *retained* and keeps counting against
///   the hard cap, because releasing it optimistically would hand back money
///   nobody can prove was never spent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementDisposition {
    Settled,
    Released,
    /// The conservative default: retain the money rather than assume it was
    /// never spent.
    #[default]
    Unresolved,
}

impl SettlementDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Settled => "settled",
            Self::Released => "released",
            Self::Unresolved => "unresolved",
        }
    }

    /// Whether the disposition asserts a Money amount. Only the authority may
    /// assert one.
    pub fn asserts_actual(self) -> bool {
        matches!(self, Self::Settled | Self::Released)
    }
}

/// How a settled actual compares to the reservation it discharges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementVariance {
    /// `actual == reserved`.
    Exact,
    /// `actual < reserved`; the difference is released.
    Under,
    /// `actual > reserved`; the excess is recorded, never clamped.
    Over,
}

/// The money effect of one settlement, derived from the reservation ledger.
///
/// `actual` is the canonical amount that was really spent. `released` is the
/// part of the reservation that returned to the Project, and `overage` is the
/// part of the actual that exceeded the reservation. Both are zero unless they
/// apply, so a reader never has to infer a direction from a comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SettlementEffect {
    pub reserved: Money,
    pub actual: Money,
    pub released: Money,
    pub overage: Money,
    pub variance: Option<SettlementVariance>,
    /// True when the recorded actual pushed settled spend past the hard limit.
    /// The overage is still recorded in full: a breach is made visible, never
    /// clamped back under the cap.
    pub exceeded_hard_limit: bool,
}

impl SettlementEffect {
    /// The effect a discharge *would* have, before it is applied.
    ///
    /// This mirrors exactly what [`MissionBudget::settle_actual`],
    /// [`MissionBudget::release`] and [`MissionBudget::mark_unresolved`] book,
    /// field for field, but as a pure function of the disposition, the actual
    /// and the reserved amount. It exists so the payload digest of a settlement
    /// can be computed before the discharge is applied — and so a re-delivery of
    /// an already-discharged reservation reproduces the effect the original
    /// settlement had, which is what makes "same identity, same payload" a
    /// duplicate even after the reservation has gone terminal.
    ///
    /// `exceeded_hard_limit` is always false here: it depends on the ledger
    /// state the discharge runs against, not on the payload, and is deliberately
    /// excluded from the digest for that reason.
    pub fn for_discharge(
        disposition: SettlementDisposition,
        actual: Option<&Money>,
        reserved: &Money,
    ) -> Self {
        let zero = Money::zero(reserved.currency.clone());
        let (released, overage, variance) = match (disposition, actual) {
            (SettlementDisposition::Settled, Some(actual)) => {
                match actual.micros.cmp(&reserved.micros) {
                    std::cmp::Ordering::Less => (
                        Money::new(reserved.micros - actual.micros, reserved.currency.clone()),
                        zero.clone(),
                        Some(SettlementVariance::Under),
                    ),
                    std::cmp::Ordering::Equal => {
                        (zero.clone(), zero.clone(), Some(SettlementVariance::Exact))
                    }
                    std::cmp::Ordering::Greater => (
                        zero.clone(),
                        Money::new(actual.micros - reserved.micros, reserved.currency.clone()),
                        Some(SettlementVariance::Over),
                    ),
                }
            }
            (SettlementDisposition::Released, _) => (reserved.clone(), zero.clone(), None),
            // A settled disposition always carries an actual by construction;
            // the shape it would have without one is the same as no discharge.
            (SettlementDisposition::Unresolved, _) | (SettlementDisposition::Settled, None) => {
                (zero.clone(), zero.clone(), None)
            }
        };
        Self {
            reserved: reserved.clone(),
            actual: actual.cloned().unwrap_or(zero),
            released,
            overage,
            variance,
            exceeded_hard_limit: false,
        }
    }
}

/// One durable accounting fact: how a single reservation was discharged.
///
/// The record is evidence about money, not the money itself. The authoritative
/// accumulators live on the Project budget; this is the per-operation audit
/// trail that says which usage produced which amount, under which price, and
/// with which authority.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settlement {
    /// Deterministic over the reservation, the authority and the disposition, so
    /// replaying the same fact can never book it twice.
    pub settlement_id: String,
    pub reservation_id: String,
    pub project_id: String,
    pub call_id: String,
    pub dispatch_intent_id: Option<String>,
    pub attempt_id: String,
    pub generation: u64,
    pub disposition: SettlementDisposition,
    /// The exact price that valued the usage, when one applied.
    pub pricing: Option<TokenPrice>,
    /// The provider-reported usage, when one was observed. It is retained even
    /// when it could not be priced, so a later operator can see the spend
    /// exists even though no actual was booked.
    pub usage: Option<UsageRecord>,
    /// The non-overlapping quantities `usage` was normalized into by the
    /// provider adapter, and which `pricing` was applied to. Recording it makes
    /// the canonical Money auditable without a reader having to re-derive which
    /// counters the provider's totals were inclusive of.
    pub billable: Option<BillableUsage>,
    /// The content digest of this fact, computed by
    /// [`settlement_payload_digest`]. It is derived, so it is not part of the
    /// digest itself.
    pub payload_digest: String,
    pub effect: SettlementEffect,
    pub reason_code: String,
    /// False when the usage was observed but the reporting Attempt no longer
    /// held authority, so nothing was booked against the budget.
    pub usage_authoritative: bool,
    pub created_at: i64,
}

/// Deterministic settlement identity.
///
/// The disposition *and* the reason are part of the key on purpose. An uncertain
/// dispatch first records `Unresolved` because its effect is unknown, and a
/// later provider result from a fenced Attempt records `Unresolved` again
/// because it carries usage that may not be booked — two genuinely different
/// facts about the same money, so they get two ids. Two identical facts, by
/// contrast, are the same fact delivered twice and the second one is dropped.
///
/// The identity is deliberately *not* the content. The same id can arrive
/// carrying a different payload — a re-delivered provider result with different
/// usage, a retried completion with a different actual — and that is a conflict,
/// not a duplicate. [`settlement_payload_digest`] is what separates the two.
pub fn settlement_id(
    reservation_id: &str,
    attempt_id: &str,
    generation: u64,
    disposition: SettlementDisposition,
    reason_code: &str,
) -> String {
    let key = format!(
        "ocg-settlement-v1|{reservation_id}|{attempt_id}|{generation}|{}|{reason_code}",
        disposition.as_str()
    );
    let digest = crate::runtime::hash::sha256_hex(key.as_bytes());
    format!("stl-{}", digest.get(..16).unwrap_or(&digest))
}

/// The deterministic content of one settlement, as distinct from its identity.
///
/// The id makes a byte-identical retry a no-op; this digest is what makes the
/// same id carrying different content a conflict rather than a silent
/// duplicate. It covers every field that states a fact about the money — the
/// reservation, the authority, the disposition, the price, the usage, the
/// billable quantities it was priced as, the effect and the reason — and
/// excludes three things that are not facts about the settlement:
///
/// - `created_at`, which says when the fact was recorded rather than what it
///   is; a retry delivered a second later is the same fact, not a new one.
/// - `payload_digest`, which is derived from the rest.
/// - `effect.exceeded_hard_limit`, which is a consequence of the ledger state
///   the discharge ran against rather than a property of the payload. The rest
///   of the effect is a pure function of the disposition, the actual and the
///   reserved amount, so two payloads that agree on those agree on it.
///
/// The encoding is canonical JSON, so incidental key ordering can never make
/// two equal payloads compare unequal.
pub fn settlement_payload_digest(settlement: &Settlement) -> String {
    let mut value = serde_json::to_value(settlement).unwrap_or(Value::Null);
    if let Some(object) = value.as_object_mut() {
        object.remove("created_at");
        object.remove("payload_digest");
        if let Some(effect) = object
            .get_mut("effect")
            .and_then(|effect| effect.as_object_mut())
        {
            effect.remove("exceeded_hard_limit");
        }
    }
    let canonical =
        serde_json_canonicalizer::to_vec(&value).unwrap_or_else(|_| value.to_string().into_bytes());
    crate::runtime::hash::sha256_hex(canonical.as_slice())
}

/// The durable id of the record that retains one *rejected* settlement payload.
///
/// A conflicting payload is never booked as the settlement it claims to be, so
/// it cannot take the business id. It gets a derived one instead, which is
/// itself idempotent: the same conflicting payload delivered again is a
/// duplicate of the record that already retained it.
pub fn conflict_settlement_id(settlement_id: &str, payload_digest: &str) -> String {
    let key = format!("ocg-settlement-conflict-v1|{settlement_id}|{payload_digest}");
    let digest = crate::runtime::hash::sha256_hex(key.as_bytes());
    format!("stlc-{}", digest.get(..16).unwrap_or(&digest))
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
    /// Total money returned to the Project's headroom, by releases and by
    /// settlements below their reservation. This is an authoritative
    /// accumulator: it is increased by exactly the effect of each settlement as
    /// it is recorded and is never re-derived from the bounded
    /// [`Self::settlements`] list, so pruning that list cannot shrink it.
    pub released: Money,
    /// Total money spent above the reserved maximum. An authoritative
    /// accumulator for the same reason as [`Self::released`], and an inspection
    /// figure only: the hard-cap decision reads `settled` and `reserved`, never
    /// this.
    pub overage: Money,
    pub status: BudgetStatus,
    pub reservations: Vec<Reservation>,
    /// Bounded, per-operation accounting records. The accumulators above are
    /// authoritative and are never recomputed from this list.
    pub settlements: Vec<Settlement>,
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
            released: Money::default(),
            overage: Money::default(),
            status: BudgetStatus::Unconfigured,
            reservations: Vec::new(),
            settlements: Vec::new(),
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

    /// Recompute the derived rollups and status from the reservation ledger. The
    /// authoritative accumulators (`settled`, `released`, `overage`) are never
    /// derived: they are increased by the discharge that produced each
    /// settlement, so they cannot be shrunk by pruning the bounded settlement
    /// list.
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

    /// Find the live (`Reserved`) entry for one reservation id.
    fn live_reservation_index(&self, reservation_id: &str) -> Option<usize> {
        self.reservations.iter().position(|reservation| {
            reservation.reservation_id == reservation_id
                && reservation.state == ReservationState::Reserved
        })
    }

    /// Mark an outstanding reservation as an uncertain dispatch. It is retained
    /// and keeps counting against the hard cap.
    ///
    /// This is the only disposition a losing authority may apply: it asserts no
    /// amount, and it can only make the budget more conservative, never less.
    pub fn mark_unresolved(&mut self, reservation_id: &str, now: i64) -> Option<SettlementEffect> {
        let index = self.live_reservation_index(reservation_id)?;
        let reserved = self.reservations[index].amount.clone();
        if self.reservations[index].unresolved {
            return None;
        }
        self.reservations[index].unresolved = true;
        self.updated_at = now;
        Some(SettlementEffect {
            reserved: reserved.clone(),
            actual: Money::zero(reserved.currency.clone()),
            released: Money::zero(reserved.currency.clone()),
            overage: Money::zero(reserved.currency.clone()),
            variance: None,
            exceeded_hard_limit: false,
        })
    }

    /// Settle an outstanding reservation against a reliably known actual.
    ///
    /// A smaller actual releases the difference back to the Project's headroom.
    /// A larger actual is recorded in full and never clamped: the excess becomes
    /// a recorded overage, and once settled spend passes the hard limit the
    /// Project reads as `Breached` and every further paid dispatch is denied.
    /// The only way past a cap remains an explicit change to the cap itself.
    ///
    /// Returns `None` when the reservation is not live, which is what makes a
    /// duplicate settlement a no-op rather than a second charge.
    pub fn settle_actual(
        &mut self,
        reservation_id: &str,
        actual: Money,
        now: i64,
    ) -> Result<Option<SettlementEffect>> {
        let Some(index) = self.live_reservation_index(reservation_id) else {
            return Ok(None);
        };
        let reserved = self.reservations[index].amount.clone();
        let currency = normalize_currency(&actual.currency)?;
        if !self.currency.is_empty() && currency != self.currency {
            return Err(OcgError::config(format!(
                "settlement currency {currency} differs from the Project budget currency {} (no FX conversion)",
                self.currency
            )));
        }
        if actual.micros < 0 {
            return Err(OcgError::config(
                "a settled actual cannot be negative money",
            ));
        }
        let (released, overage, variance) = match actual.micros.cmp(&reserved.micros) {
            std::cmp::Ordering::Less => (
                Money::new(reserved.micros - actual.micros, currency.clone()),
                Money::zero(currency.clone()),
                SettlementVariance::Under,
            ),
            std::cmp::Ordering::Equal => (
                Money::zero(currency.clone()),
                Money::zero(currency.clone()),
                SettlementVariance::Exact,
            ),
            std::cmp::Ordering::Greater => (
                Money::zero(currency.clone()),
                Money::new(actual.micros - reserved.micros, currency.clone()),
                SettlementVariance::Over,
            ),
        };
        self.settled.micros = self.settled.micros.saturating_add(actual.micros);
        if self.currency.is_empty() {
            self.currency = currency.clone();
        }
        self.settled.currency = self.currency.clone();
        self.reservations[index].state = ReservationState::Settled;
        self.reservations[index].unresolved = false;
        self.reservations[index].settled_at = Some(now);
        self.reservations[index].settled_amount = Some(actual.clone());
        self.updated_at = now;
        let exceeded_hard_limit = self
            .hard_limit
            .as_ref()
            .is_some_and(|limit| self.settled.micros > limit.micros);
        Ok(Some(SettlementEffect {
            reserved,
            actual,
            released,
            overage,
            variance: Some(variance),
            exceeded_hard_limit,
        }))
    }

    /// Release an outstanding reservation that is proven not to have reached the
    /// provider. The whole amount returns to the Project's headroom.
    ///
    /// Returns `None` when the reservation is not live, so a duplicate release
    /// can never hand back the same money twice.
    pub fn release(&mut self, reservation_id: &str, now: i64) -> Option<SettlementEffect> {
        let index = self.live_reservation_index(reservation_id)?;
        let reserved = self.reservations[index].amount.clone();
        self.reservations[index].state = ReservationState::Released;
        self.reservations[index].unresolved = false;
        self.updated_at = now;
        let released = reserved.clone();
        Some(SettlementEffect {
            actual: Money::zero(reserved.currency.clone()),
            overage: Money::zero(reserved.currency.clone()),
            reserved,
            released,
            variance: None,
            exceeded_hard_limit: false,
        })
    }

    /// Append one accounting fact to the bounded settlement ledger.
    ///
    /// The authoritative accumulators were already moved by the discharge that
    /// produced it; this is the per-operation record of what moved and why.
    /// `released` and `overage` are increased by this fact's effect here, at
    /// record time, so the bounded list below can never change what the Project
    /// returned or overspent.
    pub fn record_settlement(&mut self, settlement: Settlement) {
        let recorded_at = settlement.created_at;
        if self.currency.is_empty() {
            self.currency = settlement.effect.released.currency.clone();
        }
        self.released.micros = self
            .released
            .micros
            .saturating_add(settlement.effect.released.micros);
        self.overage.micros = self
            .overage
            .micros
            .saturating_add(settlement.effect.overage.micros);
        self.settlements.push(settlement);
        self.prune_settlements();
        self.recompute();
        self.updated_at = recorded_at;
    }

    /// Replace the last economic reason. An additive accounting fact never
    /// clears it, so an operator can always see why the most recent decision was
    /// what it was.
    pub fn set_reason(&mut self, reason_code: &str, now: i64) {
        self.reason = Some(reason_code.to_string());
        self.updated_at = now;
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

    /// Bound the settlement ledger the same way. Every entry here is terminal by
    /// construction, so the oldest go first. Dropping one loses inspection
    /// detail only: `settled`, `released` and `overage` are authoritative
    /// accumulators that are never re-derived from this list, and the durable
    /// `domain_settlements` table keeps the full per-operation history.
    fn prune_settlements(&mut self) {
        if self.settlements.len() <= MAX_SETTLEMENTS {
            return;
        }
        let excess = self.settlements.len() - MAX_SETTLEMENTS;
        self.settlements.drain(0..excess);
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
            released_micros: self.released.micros,
            overage_micros: self.overage.micros,
            reservation_count: self.reservations.len(),
            settlement_count: self.settlements.len(),
            unresolved_settlement_count: self
                .settlements
                .iter()
                .filter(|settlement| settlement.disposition == SettlementDisposition::Unresolved)
                .count(),
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
    /// Canonical Money actually settled, including any recorded overage.
    pub settled_micros: i64,
    /// Money still held by outstanding reservations.
    pub reserved_micros: i64,
    /// The part of `reserved_micros` whose actual spend is not yet known.
    pub unresolved_micros: i64,
    /// Money returned to the Project's headroom by releases and under-settles.
    pub released_micros: i64,
    /// Money spent above the reserved maximum.
    pub overage_micros: i64,
    pub reservation_count: usize,
    pub settlement_count: usize,
    pub unresolved_settlement_count: usize,
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
    /// Canonical per-token prices used to turn a provider-reported usage record
    /// into Money. The list is empty by default and is never filled in from a
    /// heuristic: with no price, a completed dispatch settles as `Unresolved`
    /// rather than inventing an actual.
    pub pricing: Vec<TokenPrice>,
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
        if let Some(value) = object.get("pricing") {
            config.pricing = TokenPrice::from_config(value)?;
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
        for price in &self.pricing {
            price.validate_values()?;
        }
        Ok(())
    }

    /// The canonical price for one provider route, resolved most-specific
    /// first. `None` means OCG holds no authoritative price, and OCG then never
    /// states an actual for that route.
    pub fn price_for(&self, provider: &str, model: &str) -> Option<&TokenPrice> {
        TokenPrice::resolve(&self.pricing, provider, model)
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

    /// A stable fingerprint of the pricing table alone: the revision a frozen
    /// [`PricingBasis`] records. Two configurations with the same fingerprint
    /// resolve the same price for every route, so a settlement pinned to one
    /// can be recognized as still matching the current table.
    pub fn pricing_fingerprint(&self) -> String {
        let value = serde_json::to_value(&self.pricing).unwrap_or(Value::Null);
        let canonical = serde_json_canonicalizer::to_vec(&value)
            .unwrap_or_else(|_| value.to_string().into_bytes());
        crate::runtime::hash::sha256_hex(canonical.as_slice())
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
