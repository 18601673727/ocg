use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use crate::db::entities::{AllocationToken, JobId};
use crate::support::cancellation::{CancellationReason, CancellationToken};

#[derive(Clone, Default)]
pub(crate) struct NativeStops(Arc<Mutex<BTreeMap<JobId, (AllocationToken, CancellationToken)>>>);

impl std::fmt::Debug for NativeStops {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativeStops")
    }
}

impl NativeStops {
    pub(crate) fn register(
        &self,
        token: AllocationToken,
        cancellation: CancellationToken,
    ) -> Registration {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(token.job, (token, cancellation));
        Registration {
            stops: self.clone(),
            token,
        }
    }

    pub(crate) fn cancel(&self, token: AllocationToken) -> bool {
        match self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&token.job)
        {
            Some((registered, cancellation)) if *registered == token => {
                cancellation.cancel_for(CancellationReason::UserStop);
                true
            }
            Some(_) | None => false,
        }
    }
}

pub(crate) struct Registration {
    stops: NativeStops,
    token: AllocationToken,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut stops = self.stops.0.lock().unwrap_or_else(PoisonError::into_inner);
        if stops
            .get(&self.token.job)
            .is_some_and(|(registered, _)| *registered == self.token)
        {
            stops.remove(&self.token.job);
        }
    }
}
