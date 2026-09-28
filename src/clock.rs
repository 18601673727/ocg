//! Time source.
//!
//! Everything that needs "now" goes through [`Clock`] so cache expiry and
//! install timestamps can be tested with a fixed clock instead of the wall
//! clock.

use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds since the Unix epoch.
pub trait Clock: Send + Sync {
    fn now_unix(&self) -> i64;
}

/// The real wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0)
    }
}

/// A clock frozen at a fixed instant. Used by tests.
#[derive(Debug, Clone, Copy)]
pub struct FixedClock {
    now: i64,
}

impl FixedClock {
    pub fn new(now: i64) -> Self {
        Self { now }
    }

    pub fn advance(&mut self, seconds: i64) {
        self.now += seconds;
    }

    pub fn set(&mut self, now: i64) {
        self.now = now;
    }
}

impl Clock for FixedClock {
    fn now_unix(&self) -> i64 {
        self.now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_clock_advances_without_the_wall_clock() {
        let mut clock = FixedClock::new(1_000);
        assert_eq!(clock.now_unix(), 1_000);
        clock.advance(50);
        assert_eq!(clock.now_unix(), 1_050);
        clock.set(7);
        assert_eq!(clock.now_unix(), 7);
    }
}
