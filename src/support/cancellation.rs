use std::sync::{
    Arc, Mutex, PoisonError, Weak,
    atomic::{AtomicU8, Ordering},
};
use std::task::{Poll, Waker};
use std::time::Instant;

#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<Cancellation>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum CancellationReason {
    Unspecified = 1,
    UserStop,
    HostShutdown,
    Deadline,
    EvidenceUnavailable,
}

#[derive(Debug, Default)]
struct Cancellation {
    reason: AtomicU8,
    deadline: Mutex<Option<Instant>>,
    waiters: Mutex<Vec<Waker>>,
    children: Mutex<Vec<Weak<Cancellation>>>,
}

impl CancellationToken {
    pub fn cancel(&self) {
        self.cancel_for(CancellationReason::Unspecified);
    }

    pub(crate) fn cancel_for(&self, reason: CancellationReason) {
        let deadline = self
            .0
            .deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let reason = if deadline.is_some_and(|limit| limit <= Instant::now()) {
            CancellationReason::Deadline
        } else {
            reason
        };
        if self
            .0
            .reason
            .compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        drop(deadline);
        let waiters = std::mem::take(
            &mut *self
                .0
                .waiters
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for waiter in waiters {
            waiter.wake();
        }
        let children = std::mem::take(
            &mut *self
                .0
                .children
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for child in children.iter().filter_map(Weak::upgrade) {
            Self(child).cancel_for(reason);
        }
    }

    pub fn child(&self) -> Self {
        let child = Self::default();
        let mut children = self
            .0
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(reason) = self.reason() {
            drop(children);
            child.cancel_for(reason);
            return child;
        }
        children.retain(|registered| registered.strong_count() > 0);
        children.push(Arc::downgrade(&child.0));
        child
    }

    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }

    pub(crate) fn tighten_deadline(&self, limit: Instant) {
        let mut deadline = self
            .0
            .deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *deadline = Some(deadline.map_or(limit, |current| current.min(limit)));
    }

    pub(crate) fn reason(&self) -> Option<CancellationReason> {
        use CancellationReason::{
            Deadline, EvidenceUnavailable, HostShutdown, Unspecified, UserStop,
        };
        let mut reason = self.0.reason.load(Ordering::Acquire);
        if reason == 0 {
            let deadline = self
                .0
                .deadline
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            reason = self.0.reason.load(Ordering::Acquire);
            if reason == 0 {
                return deadline
                    .is_some_and(|limit| limit <= Instant::now())
                    .then_some(Deadline);
            }
        }
        match reason {
            value if value == UserStop as u8 => Some(UserStop),
            value if value == HostShutdown as u8 => Some(HostShutdown),
            value if value == Deadline as u8 => Some(Deadline),
            value if value == EvidenceUnavailable as u8 => Some(EvidenceUnavailable),
            _ => Some(Unspecified),
        }
    }

    pub async fn cancelled(&self) {
        std::future::poll_fn(|cx| {
            if self.is_cancelled() {
                return Poll::Ready(());
            }
            {
                let mut waiters = self
                    .0
                    .waiters
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if !waiters.iter().any(|waiter| waiter.will_wake(cx.waker())) {
                    waiters.push(cx.waker().clone());
                }
            }
            if self.is_cancelled() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
