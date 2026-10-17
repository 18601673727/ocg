//! Rate and Capacity Governor for provider execution resources.
//!
//! The Governor distinguishes:
//! - **Rate**: time-window-based request limits (requests/second, requests/minute)
//! - **Provider Execution Capacity**: concurrent provider request slots
//! - **Project Workload Capacity**: broader scheduler admission slots (already handled
//!   in admission.rs via `dispatch_job_for_admission`)
//!
//! Health Probes respect provider rate and execution capacity but bypass Project
//! workload capacity, because a diagnostic probe should not displace normal work.
//!
//! The governor answers: "May this candidate consume this resource now?" and
//! enforces that answer under concurrency through atomic permit acquisition.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use thiserror::Error;

/// Errors raised while accessing governor state.
#[derive(Debug, Error)]
pub enum GovernorError {
    #[error("governor lock poisoned")]
    LockPoisoned,
}

pub type Result<T> = std::result::Result<T, GovernorError>;

/// Governor decision for a specific execution target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GovernorDecision {
    /// Execution may proceed immediately. A permit has been acquired.
    AllowedNow,
    /// Rate limit prevents execution. The target may retry after the specified duration.
    RateLimited { retry_after: Duration },
    /// Provider execution capacity is exhausted. The target may retry when capacity is released.
    CapacityUnavailable,
}

/// Scope at which a limit applies.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GovernorScope {
    /// Provider-level limit (all requests to this provider).
    Provider(String),
    /// Provider + Model limit (requests to a specific provider/model pair).
    ProviderModel { provider: String, model: String },
}

impl GovernorScope {
    pub fn provider(provider: impl Into<String>) -> Self {
        Self::Provider(provider.into())
    }

    pub fn provider_model(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self::ProviderModel {
            provider: provider.into(),
            model: model.into(),
        }
    }
}

/// Configuration for rate limiting at a specific scope.
#[derive(Debug, Clone, Copy)]
pub struct RateConfig {
    /// Maximum number of requests allowed in the time window.
    pub max_requests: u32,
    /// Time window duration.
    pub window: Duration,
}

/// Configuration for execution capacity at a specific scope.
#[derive(Debug, Clone, Copy)]
pub struct CapacityConfig {
    /// Maximum concurrent executions.
    pub max_concurrent: usize,
}

/// A permit that represents reserved execution capacity.
/// Capacity is released when the permit is dropped.
///
/// Every permit counts as one in-flight execution of its scope from its
/// acquisition to its drop, whether or not a capacity limit was configured
/// when it was acquired, so it releases exactly the one unit it added.
pub struct CapacityPermit {
    scope: GovernorScope,
    governor: Arc<Mutex<GovernorState>>,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        // A poisoned lock still holds this permit's unit; it is released.
        let mut state = self.governor.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(in_flight) = state.in_flight.get_mut(&self.scope) {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                state.in_flight.remove(&self.scope);
            }
        }
    }
}

/// Rate limiter state for one scope using a sliding window.
#[derive(Debug, Clone)]
struct RateLimiterState {
    config: RateConfig,
    /// Timestamps of recent requests within the window.
    requests: Vec<Instant>,
    /// Optional cooldown from upstream throttling (HTTP 429 Retry-After).
    cooldown_until: Option<Instant>,
}

impl RateLimiterState {
    fn new(config: RateConfig) -> Self {
        Self {
            config,
            requests: Vec::new(),
            cooldown_until: None,
        }
    }

    /// Check if a request can proceed now, without recording it. Only expired
    /// window entries and an expired cooldown are dropped.
    fn check(&mut self, now: Instant) -> std::result::Result<(), Duration> {
        // First check upstream cooldown
        if let Some(until) = self.cooldown_until {
            if now < until {
                return Err(until.duration_since(now));
            }
            self.cooldown_until = None;
        }

        // Remove expired requests from the sliding window
        let window_start = now.checked_sub(self.config.window).unwrap_or(now);
        self.requests.retain(|&timestamp| timestamp > window_start);

        // Check if we're within the limit
        if self.requests.len() >= self.config.max_requests as usize {
            // Calculate when the oldest request will expire
            if let Some(&oldest) = self.requests.first() {
                let retry_after = oldest
                    .checked_add(self.config.window)
                    .and_then(|expiry| expiry.checked_duration_since(now))
                    .unwrap_or(Duration::from_secs(1));
                return Err(retry_after);
            }
            return Err(Duration::from_secs(1));
        }
        Ok(())
    }

    /// Record a request that [`Self::check`] allowed at `now`.
    fn record(&mut self, now: Instant) {
        self.requests.push(now);
    }

    /// Record upstream throttling feedback (HTTP 429 Retry-After).
    fn set_cooldown(&mut self, retry_after: Duration) {
        let until = Instant::now() + retry_after;
        self.cooldown_until = Some(until);
    }
}

/// Internal governor state protected by a mutex.
#[derive(Debug)]
struct GovernorState {
    rate_limiters: HashMap<GovernorScope, RateLimiterState>,
    rate_configs: HashMap<GovernorScope, RateConfig>,
    /// The capacity limit in effect for each configured scope, read at every
    /// acquisition.
    capacity_configs: HashMap<GovernorScope, CapacityConfig>,
    /// The live permits of each scope, configured or not, so that a limit
    /// configured later counts the executions already running. A scope with
    /// none has no entry.
    in_flight: HashMap<GovernorScope, usize>,
}

impl GovernorState {
    fn new() -> Self {
        Self {
            rate_limiters: HashMap::new(),
            rate_configs: HashMap::new(),
            capacity_configs: HashMap::new(),
            in_flight: HashMap::new(),
        }
    }

    fn in_flight(&self, scope: &GovernorScope) -> usize {
        self.in_flight.get(scope).copied().unwrap_or(0)
    }

    /// Whether a capacity limit, if one is configured, leaves room for one
    /// more execution of `scope` now.
    fn capacity_available(&self, scope: &GovernorScope) -> bool {
        self.capacity_configs
            .get(scope)
            .is_none_or(|config| self.in_flight(scope) < config.max_concurrent)
    }

    fn ensure_rate_limiter(&mut self, scope: &GovernorScope) {
        if !self.rate_limiters.contains_key(scope)
            && let Some(&config) = self.rate_configs.get(scope)
        {
            self.rate_limiters
                .insert(scope.clone(), RateLimiterState::new(config));
        }
    }

    /// Ensure a limiter exists so upstream throttling feedback is never lost.
    ///
    /// A provider-asserted cooldown (HTTP 429 Retry-After) is a fact about the
    /// upstream, not local rate policy. When no local rate limit is configured
    /// for the scope, track the cooldown under a permissive local policy that
    /// never denies on its own; only the recorded cooldown can deny.
    fn ensure_rate_limiter_for_feedback(&mut self, scope: &GovernorScope) {
        if !self.rate_limiters.contains_key(scope) {
            let config = self.rate_configs.get(scope).copied().unwrap_or(RateConfig {
                max_requests: u32::MAX,
                window: Duration::from_secs(60),
            });
            self.rate_limiters
                .insert(scope.clone(), RateLimiterState::new(config));
        }
    }
}

/// The Governor that manages rate and capacity for provider execution.
#[derive(Clone, Debug)]
pub struct Governor {
    state: Arc<Mutex<GovernorState>>,
}

impl Governor {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(GovernorState::new())),
        }
    }

    /// Configure rate limiting for a scope.
    pub fn configure_rate(&self, scope: GovernorScope, config: RateConfig) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| GovernorError::LockPoisoned)?;
        state.rate_configs.insert(scope.clone(), config);
        // A limiter created earlier for throttling feedback tracks provider
        // cooldowns under a permissive default. A later local configuration
        // must take effect on that same limiter rather than linger unseen.
        if let Some(limiter) = state.rate_limiters.get_mut(&scope) {
            let cooldown_until = limiter.cooldown_until;
            *limiter = RateLimiterState {
                config,
                requests: std::mem::take(&mut limiter.requests),
                cooldown_until,
            };
        }
        Ok(())
    }

    /// Configure capacity limiting for a scope. The limit applies to every
    /// later acquisition and counts the permits already held, including any
    /// acquired before the scope had a limit; none of them is revoked. A
    /// limit at or below the permits held admits nothing until enough of
    /// them are released.
    pub fn configure_capacity(&self, scope: GovernorScope, config: CapacityConfig) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| GovernorError::LockPoisoned)?;
        state.capacity_configs.insert(scope, config);
        Ok(())
    }

    /// Check and acquire execution permission for a scope.
    /// Returns a permit if successful; the permit must be held for the duration of execution.
    ///
    /// Both resources are checked before either is taken, under one lock: a
    /// rate refusal takes no capacity, a capacity refusal takes no rate
    /// budget, and an admission records one request and one in-flight
    /// execution together.
    pub fn acquire(
        &self,
        scope: GovernorScope,
    ) -> Result<std::result::Result<CapacityPermit, GovernorDecision>> {
        let mut state = self.state.lock().map_err(|_| GovernorError::LockPoisoned)?;
        let now = Instant::now();

        state.ensure_rate_limiter(&scope);
        if let Some(rate_limiter) = state.rate_limiters.get_mut(&scope)
            && let Err(retry_after) = rate_limiter.check(now)
        {
            return Ok(Err(GovernorDecision::RateLimited { retry_after }));
        }
        if !state.capacity_available(&scope) {
            return Ok(Err(GovernorDecision::CapacityUnavailable));
        }

        if let Some(rate_limiter) = state.rate_limiters.get_mut(&scope) {
            rate_limiter.record(now);
        }
        *state.in_flight.entry(scope.clone()).or_insert(0) += 1;
        Ok(Ok(CapacityPermit {
            scope,
            governor: Arc::clone(&self.state),
        }))
    }

    /// Record upstream throttling feedback for a scope.
    ///
    /// This always records the cooldown, even when no local rate limit is
    /// configured: dropping provider-asserted throttling would leave Placement
    /// evaluation and final acquisition blind to a real upstream refusal.
    pub fn record_throttling(&self, scope: GovernorScope, retry_after: Duration) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| GovernorError::LockPoisoned)?;
        state.ensure_rate_limiter_for_feedback(&scope);
        if let Some(rate_limiter) = state.rate_limiters.get_mut(&scope) {
            rate_limiter.set_cooldown(retry_after);
        }
        Ok(())
    }

    /// Get current capacity usage for a scope (for diagnostics): its
    /// in-flight permits and its limit, or `None` when it has no limit.
    pub fn capacity_usage(&self, scope: &GovernorScope) -> Result<Option<(usize, usize)>> {
        let state = self.state.lock().map_err(|_| GovernorError::LockPoisoned)?;
        Ok(state
            .capacity_configs
            .get(scope)
            .map(|config| (state.in_flight(scope), config.max_concurrent)))
    }

    /// Evaluate rate eligibility without consuming a rate token.
    ///
    /// This is a non-consuming read-only check for placement evaluation.
    /// Returns (is_available, retry_after_millis).
    pub fn evaluate_rate(&self, scope: &GovernorScope) -> Result<(bool, Option<u64>)> {
        let mut state = self.state.lock().map_err(|_| GovernorError::LockPoisoned)?;

        state.ensure_rate_limiter(scope);

        if let Some(rate_limiter) = state.rate_limiters.get(scope) {
            let now = Instant::now();

            // Check upstream cooldown first
            if let Some(until) = rate_limiter.cooldown_until
                && now < until
            {
                let retry_after = until.duration_since(now);
                return Ok((false, Some(retry_after.as_millis() as u64)));
            }

            // Check sliding window without modifying it
            let window_start = now.checked_sub(rate_limiter.config.window).unwrap_or(now);
            let active_requests = rate_limiter
                .requests
                .iter()
                .filter(|&&timestamp| timestamp > window_start)
                .count();

            if active_requests >= rate_limiter.config.max_requests as usize {
                // Calculate when oldest will expire
                let retry_after = rate_limiter
                    .requests
                    .iter()
                    .filter(|&&timestamp| timestamp > window_start)
                    .min()
                    .and_then(|&oldest| {
                        oldest
                            .checked_add(rate_limiter.config.window)
                            .and_then(|expiry| expiry.checked_duration_since(now))
                    })
                    .unwrap_or(Duration::from_secs(1));
                return Ok((false, Some(retry_after.as_millis() as u64)));
            }

            Ok((true, None))
        } else {
            // No rate limiter configured = available
            Ok((true, None))
        }
    }

    /// Evaluate capacity eligibility without acquiring a permit.
    ///
    /// This is a non-consuming read-only check for placement evaluation.
    /// Returns true if capacity is currently available.
    pub fn evaluate_capacity(&self, scope: &GovernorScope) -> Result<bool> {
        let state = self.state.lock().map_err(|_| GovernorError::LockPoisoned)?;
        // No capacity config = available
        Ok(state.capacity_available(scope))
    }
}

impl Default for Governor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    fn scope() -> GovernorScope {
        GovernorScope::provider("p")
    }

    fn capacity(max_concurrent: usize) -> CapacityConfig {
        CapacityConfig { max_concurrent }
    }

    /// A rate limit whose window no test outlives.
    fn rate(max_requests: u32) -> RateConfig {
        RateConfig {
            max_requests,
            window: Duration::from_secs(3_600),
        }
    }

    fn permit(governor: &Governor, scope: &GovernorScope) -> CapacityPermit {
        match governor.acquire(scope.clone()).expect("lock") {
            Ok(permit) => permit,
            Err(decision) => panic!("expected a permit, got {decision:?}"),
        }
    }

    fn refusal(governor: &Governor, scope: &GovernorScope) -> GovernorDecision {
        match governor.acquire(scope.clone()).expect("lock") {
            Ok(_) => panic!("expected a refusal"),
            Err(decision) => decision,
        }
    }

    fn usage(governor: &Governor, scope: &GovernorScope) -> Option<(usize, usize)> {
        governor.capacity_usage(scope).expect("lock")
    }

    #[test]
    fn a_capacity_limit_admits_up_to_its_size() {
        let governor = Governor::new();
        governor
            .configure_capacity(scope(), capacity(2))
            .expect("lock");
        let held = [permit(&governor, &scope()), permit(&governor, &scope())];
        assert_eq!(
            refusal(&governor, &scope()),
            GovernorDecision::CapacityUnavailable
        );
        assert!(!governor.evaluate_capacity(&scope()).expect("lock"));
        assert_eq!(usage(&governor, &scope()), Some((2, 2)));
        drop(held);
        assert_eq!(usage(&governor, &scope()), Some((0, 2)));

        governor
            .configure_capacity(scope(), capacity(0))
            .expect("lock");
        assert_eq!(
            refusal(&governor, &scope()),
            GovernorDecision::CapacityUnavailable
        );
        assert_eq!(usage(&governor, &scope()), Some((0, 0)));
    }

    #[test]
    fn a_live_limit_change_applies_at_once_and_keeps_held_permits() {
        let governor = Governor::new();
        governor
            .configure_capacity(scope(), capacity(2))
            .expect("lock");
        let first = permit(&governor, &scope());
        let second = permit(&governor, &scope());

        governor
            .configure_capacity(scope(), capacity(1))
            .expect("lock");
        assert_eq!(
            usage(&governor, &scope()),
            Some((2, 1)),
            "no permit is revoked"
        );
        assert_eq!(
            refusal(&governor, &scope()),
            GovernorDecision::CapacityUnavailable
        );
        drop(first);
        assert_eq!(usage(&governor, &scope()), Some((1, 1)));
        assert_eq!(
            refusal(&governor, &scope()),
            GovernorDecision::CapacityUnavailable
        );
        drop(second);
        let third = permit(&governor, &scope());

        governor
            .configure_capacity(scope(), capacity(3))
            .expect("lock");
        let more = [permit(&governor, &scope()), permit(&governor, &scope())];
        assert_eq!(usage(&governor, &scope()), Some((3, 3)));
        assert_eq!(
            refusal(&governor, &scope()),
            GovernorDecision::CapacityUnavailable
        );
        drop(third);
        drop(more);
        assert_eq!(usage(&governor, &scope()), Some((0, 3)));
    }

    #[test]
    fn a_limit_configured_later_counts_the_permits_already_held() {
        let governor = Governor::new();
        let early = permit(&governor, &scope());
        let earlier = permit(&governor, &scope());
        assert_eq!(
            usage(&governor, &scope()),
            None,
            "an unconfigured scope reports none"
        );
        assert!(governor.evaluate_capacity(&scope()).expect("lock"));

        governor
            .configure_capacity(scope(), capacity(2))
            .expect("lock");
        assert_eq!(usage(&governor, &scope()), Some((2, 2)));
        assert_eq!(
            refusal(&governor, &scope()),
            GovernorDecision::CapacityUnavailable
        );
        // Released in another order than taken.
        drop(earlier);
        let late = permit(&governor, &scope());
        assert_eq!(usage(&governor, &scope()), Some((2, 2)));
        drop(early);
        assert_eq!(
            usage(&governor, &scope()),
            Some((1, 2)),
            "the late permit stays counted"
        );
        drop(late);
        assert_eq!(
            usage(&governor, &scope()),
            Some((0, 2)),
            "no release went below zero"
        );
        let [_a, _b] = [permit(&governor, &scope()), permit(&governor, &scope())];
        assert_eq!(
            refusal(&governor, &scope()),
            GovernorDecision::CapacityUnavailable
        );
    }

    #[test]
    fn a_capacity_refusal_consumes_no_rate_budget() {
        let governor = Governor::new();
        governor.configure_rate(scope(), rate(1)).expect("lock");
        governor
            .configure_capacity(scope(), capacity(0))
            .expect("lock");
        for _ in 0..3 {
            assert_eq!(
                refusal(&governor, &scope()),
                GovernorDecision::CapacityUnavailable
            );
        }
        assert_eq!(
            governor.evaluate_rate(&scope()).expect("lock"),
            (true, None)
        );

        governor
            .configure_capacity(scope(), capacity(1))
            .expect("lock");
        let held = permit(&governor, &scope());
        drop(held);
        assert!(
            matches!(
                refusal(&governor, &scope()),
                GovernorDecision::RateLimited { .. }
            ),
            "the admission took the one request"
        );
    }

    #[test]
    fn a_rate_refusal_takes_no_capacity() {
        let governor = Governor::new();
        governor
            .configure_capacity(scope(), capacity(1))
            .expect("lock");
        governor.configure_rate(scope(), rate(0)).expect("lock");
        assert!(matches!(
            refusal(&governor, &scope()),
            GovernorDecision::RateLimited { .. }
        ));
        assert_eq!(usage(&governor, &scope()), Some((0, 1)));

        governor.configure_rate(scope(), rate(1)).expect("lock");
        governor
            .record_throttling(scope(), Duration::from_secs(3_600))
            .expect("lock");
        assert!(matches!(
            refusal(&governor, &scope()),
            GovernorDecision::RateLimited { .. }
        ));
        assert_eq!(
            usage(&governor, &scope()),
            Some((0, 1)),
            "a cooldown takes none either"
        );

        let other = GovernorScope::provider("q");
        governor
            .configure_rate(other.clone(), rate(1))
            .expect("lock");
        governor
            .configure_capacity(other.clone(), capacity(2))
            .expect("lock");
        let held = permit(&governor, &other);
        assert!(matches!(
            refusal(&governor, &other),
            GovernorDecision::RateLimited { .. }
        ));
        assert_eq!(usage(&governor, &other), Some((1, 2)));
        drop(held);
    }

    #[test]
    fn provider_and_provider_model_scopes_are_accounted_apart() {
        let governor = Governor::new();
        let provider = GovernorScope::provider("p");
        let model = GovernorScope::provider_model("p", "m");
        let other_model = GovernorScope::provider_model("p", "n");
        for scope in [&provider, &model] {
            governor
                .configure_capacity(scope.clone(), capacity(1))
                .expect("lock");
        }
        let held = [
            permit(&governor, &provider),
            permit(&governor, &model),
            permit(&governor, &other_model),
        ];
        assert_eq!(
            refusal(&governor, &provider),
            GovernorDecision::CapacityUnavailable
        );
        assert_eq!(
            refusal(&governor, &model),
            GovernorDecision::CapacityUnavailable
        );
        governor
            .configure_capacity(other_model.clone(), capacity(1))
            .expect("lock");
        assert_eq!(usage(&governor, &other_model), Some((1, 1)));
        let [provider_permit, model_permit, other_permit] = held;
        drop(model_permit);
        assert_eq!(usage(&governor, &provider), Some((1, 1)));
        assert_eq!(usage(&governor, &model), Some((0, 1)));
        assert_eq!(usage(&governor, &other_model), Some((1, 1)));
        drop(provider_permit);
        drop(other_permit);
    }

    #[test]
    fn concurrent_callers_never_exceed_the_limit_and_release_exactly() {
        const THREADS: usize = 16;
        const LIMIT: usize = 4;
        let governor = Governor::new();
        governor
            .configure_capacity(scope(), capacity(LIMIT))
            .expect("lock");
        let start = Arc::new(Barrier::new(THREADS));
        let acquired = Arc::new(Barrier::new(THREADS));
        let release = Arc::new(Barrier::new(THREADS + 1));
        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                // Every thread uses its own clone of the Governor.
                let governor = governor.clone();
                let (start, acquired, release) = (start.clone(), acquired.clone(), release.clone());
                thread::spawn(move || {
                    start.wait();
                    let held = governor.acquire(scope()).expect("lock").ok();
                    acquired.wait();
                    let won = held.is_some();
                    release.wait();
                    drop(held);
                    // Churn: acquisitions racing releases.
                    for _ in 0..200 {
                        if let Ok(permit) = governor.acquire(scope()).expect("lock") {
                            let (in_flight, limit) = governor
                                .capacity_usage(&scope())
                                .expect("lock")
                                .expect("configured");
                            assert!(in_flight <= limit, "{in_flight} in flight under {limit}");
                            drop(permit);
                        }
                    }
                    won
                })
            })
            .collect();
        // The winners hold their permits until every thread is released.
        let deadline = Instant::now() + crate::test_support::WAIT;
        while usage(&governor, &scope()) != Some((LIMIT, LIMIT)) {
            assert!(Instant::now() < deadline, "the limit was never reached");
            thread::yield_now();
        }
        release.wait();
        let winners = workers
            .into_iter()
            .map(|worker| worker.join().expect("no worker panicked"))
            .filter(|won| *won)
            .count();
        assert_eq!(winners, LIMIT);
        assert_eq!(usage(&governor, &scope()), Some((0, LIMIT)));
    }
}
