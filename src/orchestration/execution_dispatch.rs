//! Bounded handoff between canonical admission and execution workers.

use crate::error::{OcgError, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}

#[derive(Clone)]
pub struct ExecutionEnvelope {
    pub call_id: String,
    pub job_id: String,
    pub attempt_id: String,
    pub executor_id: Option<String>,
    pub generation: u64,
    pub payload: String,
    pub dispatch_id: Option<String>,
    pub events: flume::Sender<ExecutionEvent>,
    /// Frozen provider execution configuration for this Call.
    /// Only populated for provider Calls; None for native tool Calls.
    pub provider_config: Option<ProviderExecutionConfig>,
    /// Runtime shutdown and chat cancellation are separate concerns. This
    /// token belongs to this dispatched Call and is never persisted.
    pub cancelled: CallCancellation,
}

/// Cancellation for one dispatched Call.
///
/// The flag is a control signal; durable Attempt/Call fencing remains the
/// authority to act. The channel is what lets a future that is *parked* be
/// woken: a provider socket read waiting for bytes that may never arrive cannot
/// observe a flag, so without a wakeup, cancelling a turn would have to wait for
/// the upstream to send another chunk. Cancelling sets the flag and posts on the
/// channel; cancellation closes the sender, waking current waiters and leaving
/// the receiver disconnected so later waiters return immediately.
#[derive(Clone, Debug)]
pub struct CallCancellation {
    inner: Arc<CancellationInner>,
}

#[derive(Debug)]
struct CancellationInner {
    flag: AtomicBool,
    /// Bounded to one post. A second cancel is already covered by the flag.
    signal: Mutex<Option<flume::Sender<()>>>,
    wake: flume::Receiver<()>,
}

impl Default for CallCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl CallCancellation {
    pub fn new() -> Self {
        let (signal, wake) = flume::bounded::<()>(1);
        Self {
            inner: Arc::new(CancellationInner {
                flag: AtomicBool::new(false),
                signal: Mutex::new(Some(signal)),
                wake,
            }),
        }
    }

    /// Whether this Call has been cancelled. Cheap enough for the existing
    /// per-chunk and per-round checks.
    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::SeqCst)
    }

    /// Cancel the Call and wake anything waiting on it. Idempotent across clones.
    pub fn cancel(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        // Closing the channel wakes every waiter without consuming a wakeup.
        let signal = self
            .inner
            .signal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(signal);
    }

    /// Resolve once this Call is cancelled.
    ///
    /// This is the transport-level interrupt: racing it against a provider read
    /// ends that read without waiting for another byte from the upstream.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        match self.inner.wake.recv_async().await {
            Ok(()) | Err(flume::RecvError::Disconnected) => {}
        }
    }

    pub fn wait_timeout(&self, timeout: std::time::Duration) {
        if !self.is_cancelled() {
            match self.inner.wake.recv_timeout(timeout) {
                Ok(())
                | Err(flume::RecvTimeoutError::Disconnected | flume::RecvTimeoutError::Timeout) => {
                }
            }
        }
    }
}

/// Frozen provider execution configuration associated with a specific Call.
/// This is durably stored and never includes the raw credential/token. The
/// `upstream_model_id` is the provider-facing model id sent on the wire; it is
/// frozen so recovery and re-execution always use the same identity.
#[derive(Debug, Clone)]
pub struct ProviderExecutionConfig {
    /// The Profile model key the Call was launched with.
    pub provider_key: String,
    pub model: String,
    /// The provider-facing model id sent to the endpoint.
    pub upstream_model_id: String,
    pub endpoint: String,
    /// The Vault credential reference. `None` means no Authorization header.
    pub credential_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    Started,
    Provider(crate::openai_compatible::stream::ChatStreamEvent),
    Failed(String),
    Finished,
}

/// Admit one executable Call and place its exact canonical identity on the
/// bounded dispatcher. The database admission happens before queue handoff;
/// a queue item is never authority by itself.
pub fn admit_call(
    domain: &mut crate::orchestration::domain::DomainRepository,
    authority: &crate::orchestration::domain::AttemptAuthority,
    executor_id: &str,
    side_effect: bool,
    request: &str,
    dispatcher: &BoundedDispatcher,
) -> Result<crate::orchestration::domain::Call> {
    let (call, _events) = admit_call_with_events(
        domain,
        authority,
        executor_id,
        side_effect,
        request,
        dispatcher,
    )?;
    Ok(call)
}

pub fn admit_call_with_events(
    domain: &mut crate::orchestration::domain::DomainRepository,
    authority: &crate::orchestration::domain::AttemptAuthority,
    executor_id: &str,
    side_effect: bool,
    request: &str,
    dispatcher: &BoundedDispatcher,
) -> Result<(
    crate::orchestration::domain::Call,
    flume::Receiver<ExecutionEvent>,
)> {
    admit_call_with_cancellation_and_events(
        domain,
        authority,
        executor_id,
        side_effect,
        request,
        dispatcher,
        CallCancellation::new(),
    )
}

pub(crate) fn admit_call_with_cancellation(
    domain: &mut crate::orchestration::domain::DomainRepository,
    authority: &crate::orchestration::domain::AttemptAuthority,
    executor_id: &str,
    side_effect: bool,
    request: &str,
    dispatcher: &BoundedDispatcher,
    cancelled: CallCancellation,
) -> Result<crate::orchestration::domain::Call> {
    let (call, _events) = admit_call_with_cancellation_and_events(
        domain,
        authority,
        executor_id,
        side_effect,
        request,
        dispatcher,
        cancelled,
    )?;
    Ok(call)
}

fn admit_call_with_cancellation_and_events(
    domain: &mut crate::orchestration::domain::DomainRepository,
    authority: &crate::orchestration::domain::AttemptAuthority,
    executor_id: &str,
    side_effect: bool,
    request: &str,
    dispatcher: &BoundedDispatcher,
    cancelled: CallCancellation,
) -> Result<(
    crate::orchestration::domain::Call,
    flume::Receiver<ExecutionEvent>,
)> {
    let input: serde_json::Value = serde_json::from_str(request)
        .map_err(|error| invalid(&format!("invalid Call input JSON: {error}")))?;
    crate::orchestration::call_schema::validate_input(&input)?;
    let call = domain.create_call_with_effect(
        &authority.attempt_id,
        Some(executor_id),
        authority.generation,
        if side_effect {
            crate::orchestration::domain::EffectIntentKind::StrictFenced
        } else {
            crate::orchestration::domain::EffectIntentKind::Idempotent
        },
        request,
    )?;
    domain.mark_dispatch_queued(&call.id)?;
    let (events, receiver) = flume::unbounded();
    if let Err(error) = dispatcher.send(ExecutionEnvelope {
        call_id: call.id.clone(),
        job_id: authority.job_id.clone(),
        attempt_id: authority.attempt_id.clone(),
        executor_id: Some(executor_id.to_string()),
        generation: authority.generation,
        payload: request.to_string(),
        dispatch_id: None,
        events,
        provider_config: None,
        cancelled,
    }) {
        let _ = domain.finish_dispatch_intent(
            &call.id,
            "failed",
            crate::orchestration::domain::EffectIntentState::NotStarted,
            Some("bounded_dispatch_disconnected"),
        );
        return Err(error);
    }
    Ok((call, receiver))
}

/// Publish an already economically admitted Call to the bounded execution
/// lane. The Call and intent already exist in SQLite; this operation only
/// advances the durable intent before applying backpressure.
pub fn queue_call(
    domain: &mut crate::orchestration::domain::DomainRepository,
    call: &crate::orchestration::domain::Call,
    authority: &crate::orchestration::domain::AttemptAuthority,
    request: &str,
    dispatch_id: Option<String>,
    dispatcher: &BoundedDispatcher,
) -> Result<flume::Receiver<ExecutionEvent>> {
    let (events, receiver) = flume::unbounded();
    domain.mark_budget_admitted(&call.id)?;
    domain.mark_dispatch_queued(&call.id)?;
    if let Err(error) = dispatcher.send(ExecutionEnvelope {
        call_id: call.id.clone(),
        job_id: authority.job_id.clone(),
        attempt_id: authority.attempt_id.clone(),
        executor_id: call.executor_id.clone(),
        generation: authority.generation,
        payload: request.to_string(),
        dispatch_id,
        events,
        provider_config: None,
        cancelled: CallCancellation::new(),
    }) {
        let _ = domain.finish_dispatch_intent(
            &call.id,
            "failed",
            crate::orchestration::domain::EffectIntentState::NotStarted,
            Some("bounded_dispatch_disconnected"),
        );
        return Err(error);
    }
    Ok(receiver)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationOutcome {
    ConfirmedStopped,
    StopUnknown,
    Orphaned,
}

pub trait CancellationTarget: Send + Sync {
    fn cancel_executor(&self, attempt_id: &str) -> Result<CancellationOutcome>;
    fn cancel_calls(&self, attempt_id: &str) -> Result<CancellationOutcome>;
    fn cancel_subprocess(&self, attempt_id: &str) -> Result<CancellationOutcome>;
}

pub fn cancel_attempt(
    domain: &mut crate::orchestration::domain::DomainRepository,
    target: &dyn CancellationTarget,
    attempt_id: &str,
) -> Result<CancellationOutcome> {
    domain.request_cancel(attempt_id)?;
    let outcomes = [
        target.cancel_executor(attempt_id)?,
        target.cancel_calls(attempt_id)?,
        target.cancel_subprocess(attempt_id)?,
    ];
    let outcome = if outcomes
        .iter()
        .all(|value| *value == CancellationOutcome::ConfirmedStopped)
    {
        domain.confirm_cancel(attempt_id, true)?;
        CancellationOutcome::ConfirmedStopped
    } else if outcomes.contains(&CancellationOutcome::Orphaned) {
        domain.mark_orphaned(attempt_id)?;
        CancellationOutcome::Orphaned
    } else {
        domain.confirm_cancel(attempt_id, false)?;
        CancellationOutcome::StopUnknown
    };
    Ok(outcome)
}

/// A bounded handoff. `send` blocks when full, so execution admission cannot
/// outrun the execution worker and no cache silently becomes a second queue.
pub struct BoundedDispatcher {
    capacity: usize,
    sender: Arc<Mutex<Option<flume::Sender<ExecutionEnvelope>>>>,
    receiver: flume::Receiver<ExecutionEnvelope>,
    cancellations: Arc<Mutex<HashMap<String, Vec<Weak<CancellationInner>>>>>,
    ownership: Arc<Mutex<HashMap<String, AttemptOwnership>>>,
}

/// What this runtime holds of one Attempt's execution: envelopes waiting in
/// the queue, and execution actors working on dequeued ones.
#[derive(Debug, Default)]
struct AttemptOwnership {
    queued: usize,
    active: usize,
}

/// One execution actor's hold on an Attempt, from the moment a worker takes
/// its envelope off the queue until the actor finishes, fails or unwinds.
/// Ownership is exactly the set of live leases and queued envelopes: nothing
/// else, a cancellation token or an observer of the execution included, can
/// keep an Attempt owned.
pub struct ExecutionLease {
    ownership: Arc<Mutex<HashMap<String, AttemptOwnership>>>,
    attempt_id: String,
}

impl Drop for ExecutionLease {
    fn drop(&mut self) {
        let mut ownership = self
            .ownership
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(held) = ownership.get_mut(&self.attempt_id) {
            held.active = held.active.saturating_sub(1);
            if held.queued == 0 && held.active == 0 {
                ownership.remove(&self.attempt_id);
            }
        }
    }
}

impl std::fmt::Debug for BoundedDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedDispatcher")
            .field("capacity", &self.capacity)
            .finish()
    }
}

impl Clone for BoundedDispatcher {
    fn clone(&self) -> Self {
        Self {
            capacity: self.capacity,
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            cancellations: self.cancellations.clone(),
            ownership: self.ownership.clone(),
        }
    }
}

impl BoundedDispatcher {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(invalid("dispatcher capacity must be greater than zero"));
        }
        let (sender, receiver) = flume::bounded(capacity);
        Ok(Self {
            capacity,
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver,
            cancellations: Arc::new(Mutex::new(HashMap::new())),
            ownership: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Blocking producer operation used by the blocking worker.
    pub fn send(&self, value: ExecutionEnvelope) -> Result<()> {
        let sender = self
            .sender
            .lock()
            .map_err(|_| invalid("dispatcher lock poisoned"))?
            .clone()
            .ok_or_else(|| invalid("dispatcher is closed"))?;
        {
            let mut cancellations = self
                .cancellations
                .lock()
                .map_err(|_| invalid("dispatcher cancellation lock poisoned"))?;
            cancellations.retain(|_, tokens| {
                tokens.retain(|token| token.strong_count() > 0);
                !tokens.is_empty()
            });
            cancellations
                .entry(value.attempt_id.clone())
                .or_default()
                .push(Arc::downgrade(&value.cancelled.inner));
        }
        let attempt_id = value.attempt_id.clone();
        self.ownership
            .lock()
            .map_err(|_| invalid("dispatcher ownership lock poisoned"))?
            .entry(attempt_id.clone())
            .or_default()
            .queued += 1;
        sender.send(value).map_err(|_| {
            self.dequeued(&attempt_id);
            invalid("dispatcher is closed")
        })
    }

    /// An envelope left the queue.
    fn dequeued(&self, attempt_id: &str) {
        let mut ownership = self
            .ownership
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(held) = ownership.get_mut(attempt_id) {
            held.queued = held.queued.saturating_sub(1);
            if held.queued == 0 && held.active == 0 {
                ownership.remove(attempt_id);
            }
        }
    }

    /// Move a dequeued envelope's Attempt to an active execution actor.
    fn lease(&self, envelope: &ExecutionEnvelope) -> ExecutionLease {
        let mut ownership = self
            .ownership
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let held = ownership.entry(envelope.attempt_id.clone()).or_default();
        held.queued = held.queued.saturating_sub(1);
        held.active += 1;
        ExecutionLease {
            ownership: self.ownership.clone(),
            attempt_id: envelope.attempt_id.clone(),
        }
    }

    pub fn cancel_attempt(&self, attempt_id: &str) -> Result<bool> {
        let mut cancellations = self
            .cancellations
            .lock()
            .map_err(|_| invalid("dispatcher cancellation lock poisoned"))?;
        let tokens = cancellations.entry(attempt_id.to_string()).or_default();
        let mut signalled = false;
        for inner in tokens.iter().filter_map(|token| token.upgrade()) {
            CallCancellation { inner }.cancel();
            signalled = true;
        }
        Ok(signalled)
    }

    /// Whether this runtime still owns the Attempt's execution: an envelope
    /// of it is queued, or an execution actor holds its lease.
    ///
    /// This is not a heartbeat and not execution progress: an actor may be
    /// blocked on an HTTP read whose deadline is the transport timeout. It is
    /// also independent of every cancellation token and observer, which may
    /// outlive the actor. Absence means this process no longer executes the
    /// Attempt, which is the only fact a reconciler may treat as
    /// disappearance.
    pub fn owns_attempt(&self, attempt_id: &str) -> Result<bool> {
        Ok(self
            .ownership
            .lock()
            .map_err(|_| invalid("dispatcher ownership lock poisoned"))?
            .get(attempt_id)
            .is_some_and(|held| held.queued > 0 || held.active > 0))
    }

    /// Blocking consumer operation for a synchronous execution lane: take
    /// the next envelope and become its execution actor until the
    /// returned lease is dropped.
    pub fn recv_leased(&self) -> Result<Option<(ExecutionEnvelope, ExecutionLease)>> {
        match self.receiver.recv() {
            Ok(value) => {
                let lease = self.lease(&value);
                Ok(Some((value, lease)))
            }
            Err(_) => Ok(None),
        }
    }

    /// Async consumer operation for a dispatcher owned by an async execution
    /// runtime. The channel remains bounded; only the receive operation changes
    /// how the worker waits for the next envelope.
    pub async fn recv_async(&self) -> Result<Option<(ExecutionEnvelope, ExecutionLease)>> {
        match self.receiver.recv_async().await {
            Ok(value) => {
                let lease = self.lease(&value);
                Ok(Some((value, lease)))
            }
            Err(_) => Ok(None),
        }
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<(ExecutionEnvelope, ExecutionLease)>> {
        match self.receiver.recv_timeout(timeout) {
            Ok(value) => {
                let lease = self.lease(&value);
                Ok(Some((value, lease)))
            }
            Err(flume::RecvTimeoutError::Timeout | flume::RecvTimeoutError::Disconnected) => {
                Ok(None)
            }
        }
    }
    pub fn close(&mut self) -> Result<()> {
        let mut sender = self
            .sender
            .lock()
            .map_err(|_| invalid("dispatcher lock poisoned"))?;
        sender.take();
        Ok(())
    }

    /// Shutdown the dispatcher by dropping the sender, which unblocks all
    /// consumers waiting on recv().
    pub fn shutdown(&self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.receiver.len())
    }

    pub(crate) fn is_closed(&self) -> Result<bool> {
        Ok(self
            .sender
            .lock()
            .map_err(|_| invalid("dispatcher lock poisoned"))?
            .is_none())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.receiver.is_empty())
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
