//! Bounded handoff between canonical admission and execution workers.

use crate::error::{OcgError, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}

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
/// The flag is the authority every existing poller reads, so nothing about who
/// may still act changes. The channel is what lets a future that is *parked* be
/// woken: a provider socket read waiting for bytes that may never arrive cannot
/// observe a flag, so without a wakeup, cancelling a turn would have to wait for
/// the upstream to send another chunk. Cancelling sets the flag and posts on the
/// channel; a waiter takes the receiving half once, so cancellation stays
/// terminal for the Call it belongs to.
#[derive(Clone, Debug)]
pub struct CallCancellation {
    inner: Arc<CancellationInner>,
}

#[derive(Debug)]
struct CancellationInner {
    flag: AtomicBool,
    /// Bounded to one post. A second cancel is already covered by the flag.
    signal: flume::Sender<()>,
    wake: Mutex<Option<flume::Receiver<()>>>,
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
                signal,
                wake: Mutex::new(Some(wake)),
            }),
        }
    }

    /// Whether this Call has been cancelled. Cheap enough for the existing
    /// per-chunk and per-round checks.
    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::SeqCst)
    }

    /// Cancel the Call and wake anything waiting on it. Idempotent: the flag is
    /// the authority and the bounded post is best effort.
    pub fn cancel(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        let _ = self.inner.signal.try_send(());
    }

    /// Resolve once this Call is cancelled.
    ///
    /// This is the transport-level interrupt: racing it against a provider read
    /// ends that read without waiting for another byte from the upstream.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let wake = self.inner.wake.lock().ok().and_then(|mut wake| wake.take());
        if let Some(wake) = wake {
            let _ = wake.recv_async().await;
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
        sender
            .send(value)
            .map_err(|_| invalid("dispatcher is closed"))
    }

    /// Blocking consumer operation intended to run inside a dedicated execution thread.
    pub fn recv(&self) -> Result<Option<ExecutionEnvelope>> {
        match self.receiver.recv() {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(None),
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

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.receiver.is_empty())
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
