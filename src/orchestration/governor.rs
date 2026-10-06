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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{OcgError, Result};

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}

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
pub struct CapacityPermit {
    scope: GovernorScope,
    governor: Arc<Mutex<GovernorState>>,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.governor.lock() {
            if let Some(capacity) = state.capacity_state.get_mut(&self.scope) {
                if capacity.current > 0 {
                    capacity.current -= 1;
                }
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

    /// Check if a request can proceed now and record it if so.
    fn check_and_acquire(&mut self, now: Instant) -> std::result::Result<(), Duration> {
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

        // Acquire the request slot
        self.requests.push(now);
        Ok(())
    }

    /// Record upstream throttling feedback (HTTP 429 Retry-After).
    fn set_cooldown(&mut self, retry_after: Duration) {
        let until = Instant::now() + retry_after;
        self.cooldown_until = Some(until);
    }
}

/// Capacity state for one scope.
#[derive(Debug, Clone)]
struct CapacityState {
    config: CapacityConfig,
    /// Current number of in-flight executions.
    current: usize,
}

impl CapacityState {
    fn new(config: CapacityConfig) -> Self {
        Self { config, current: 0 }
    }

    /// Try to acquire a capacity slot.
    fn try_acquire(&mut self) -> bool {
        if self.current < self.config.max_concurrent {
            self.current += 1;
            true
        } else {
            false
        }
    }
}

/// Internal governor state protected by a mutex.
#[derive(Debug)]
struct GovernorState {
    rate_limiters: HashMap<GovernorScope, RateLimiterState>,
    capacity_state: HashMap<GovernorScope, CapacityState>,
    rate_configs: HashMap<GovernorScope, RateConfig>,
    capacity_configs: HashMap<GovernorScope, CapacityConfig>,
}

impl GovernorState {
    fn new() -> Self {
        Self {
            rate_limiters: HashMap::new(),
            capacity_state: HashMap::new(),
            rate_configs: HashMap::new(),
            capacity_configs: HashMap::new(),
        }
    }

    fn ensure_rate_limiter(&mut self, scope: &GovernorScope) {
        if !self.rate_limiters.contains_key(scope) {
            if let Some(&config) = self.rate_configs.get(scope) {
                self.rate_limiters
                    .insert(scope.clone(), RateLimiterState::new(config));
            }
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

    fn ensure_capacity_state(&mut self, scope: &GovernorScope) {
        if !self.capacity_state.contains_key(scope) {
            if let Some(&config) = self.capacity_configs.get(scope) {
                self.capacity_state
                    .insert(scope.clone(), CapacityState::new(config));
            }
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("governor lock poisoned"))?;
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

    /// Configure capacity limiting for a scope.
    pub fn configure_capacity(&self, scope: GovernorScope, config: CapacityConfig) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("governor lock poisoned"))?;
        state.capacity_configs.insert(scope, config);
        Ok(())
    }

    /// Check and acquire execution permission for a scope.
    /// Returns a permit if successful; the permit must be held for the duration of execution.
    pub fn acquire(
        &self,
        scope: GovernorScope,
    ) -> Result<std::result::Result<CapacityPermit, GovernorDecision>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("governor lock poisoned"))?;

        // Check rate limit first
        state.ensure_rate_limiter(&scope);
        if let Some(rate_limiter) = state.rate_limiters.get_mut(&scope) {
            if let Err(retry_after) = rate_limiter.check_and_acquire(Instant::now()) {
                return Ok(Err(GovernorDecision::RateLimited { retry_after }));
            }
        }

        // Then check capacity
        state.ensure_capacity_state(&scope);
        if let Some(capacity) = state.capacity_state.get_mut(&scope) {
            if !capacity.try_acquire() {
                return Ok(Err(GovernorDecision::CapacityUnavailable));
            }
        }

        // Success: return a permit that will release capacity on drop
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("governor lock poisoned"))?;
        state.ensure_rate_limiter_for_feedback(&scope);
        if let Some(rate_limiter) = state.rate_limiters.get_mut(&scope) {
            rate_limiter.set_cooldown(retry_after);
        }
        Ok(())
    }

    /// Get current capacity usage for a scope (for diagnostics).
    pub fn capacity_usage(&self, scope: &GovernorScope) -> Result<Option<(usize, usize)>> {
        let state = self
            .state
            .lock()
            .map_err(|_| invalid("governor lock poisoned"))?;
        Ok(state
            .capacity_state
            .get(scope)
            .map(|capacity| (capacity.current, capacity.config.max_concurrent)))
    }

    /// Evaluate rate eligibility without consuming a rate token.
    ///
    /// This is a non-consuming read-only check for placement evaluation.
    /// Returns (is_available, retry_after_millis).
    pub fn evaluate_rate(&self, scope: &GovernorScope) -> Result<(bool, Option<u64>)> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("governor lock poisoned"))?;

        state.ensure_rate_limiter(scope);

        if let Some(rate_limiter) = state.rate_limiters.get(scope) {
            let now = Instant::now();

            // Check upstream cooldown first
            if let Some(until) = rate_limiter.cooldown_until {
                if now < until {
                    let retry_after = until.duration_since(now);
                    return Ok((false, Some(retry_after.as_millis() as u64)));
                }
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("governor lock poisoned"))?;

        state.ensure_capacity_state(scope);

        if let Some(capacity) = state.capacity_state.get(scope) {
            Ok(capacity.current < capacity.config.max_concurrent)
        } else {
            // No capacity config = available
            Ok(true)
        }
    }
}

impl Default for Governor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_basic() {
        let gov = Governor::new();
        let scope = GovernorScope::provider("test-provider");

        gov.configure_rate(
            scope.clone(),
            RateConfig {
                max_requests: 2,
                window: Duration::from_secs(1),
            },
        )
        .unwrap();
        gov.configure_capacity(scope.clone(), CapacityConfig { max_concurrent: 10 })
            .unwrap();

        // First two requests should succeed
        let permit1 = gov.acquire(scope.clone()).unwrap();
        assert!(matches!(permit1, Ok(_)));
        let permit2 = gov.acquire(scope.clone()).unwrap();
        assert!(matches!(permit2, Ok(_)));

        // Third request should be rate limited
        let result = gov.acquire(scope.clone()).unwrap();
        assert!(matches!(result, Err(GovernorDecision::RateLimited { .. })));
    }

    #[test]
    fn test_capacity_limit() {
        let gov = Governor::new();
        let scope = GovernorScope::provider("test-provider");

        gov.configure_capacity(scope.clone(), CapacityConfig { max_concurrent: 2 })
            .unwrap();

        // First two requests should succeed
        let permit1 = gov.acquire(scope.clone()).unwrap().unwrap();
        let _permit2 = gov.acquire(scope.clone()).unwrap().unwrap();

        // Third request should fail due to capacity
        let result = gov.acquire(scope.clone()).unwrap();
        assert!(matches!(result, Err(GovernorDecision::CapacityUnavailable)));

        // Release one permit
        drop(permit1);

        // Now should succeed
        let permit3 = gov.acquire(scope.clone()).unwrap();
        assert!(matches!(permit3, Ok(_)));
    }

    #[test]
    fn test_upstream_cooldown() {
        let gov = Governor::new();
        let scope = GovernorScope::provider("test-provider");

        gov.configure_rate(
            scope.clone(),
            RateConfig {
                max_requests: 100,
                window: Duration::from_secs(60),
            },
        )
        .unwrap();

        // Record upstream throttling
        gov.record_throttling(scope.clone(), Duration::from_millis(100))
            .unwrap();

        // Should be throttled immediately
        let result = gov.acquire(scope.clone()).unwrap();
        assert!(matches!(result, Err(GovernorDecision::RateLimited { .. })));

        // Wait for cooldown
        std::thread::sleep(Duration::from_millis(150));

        // Should succeed now
        let result = gov.acquire(scope.clone()).unwrap();
        assert!(matches!(result, Ok(_)));
    }
}
