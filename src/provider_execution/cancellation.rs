use super::*;

/// A cancellation shared by an execution and whoever may stop it. Cancelling
/// wakes every task waiting in [`CancellationToken::cancelled`], so a wait on
/// a stalled Provider ends at once instead of at its timeout. Clones share
/// one cancellation; [`CancellationToken::child`] makes a distinct one that
/// its parent's cancellation reaches.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(pub(super) Arc<Cancellation>);

#[derive(Debug, Default)]
pub(super) struct Cancellation {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
    /// The children made since the last cancellation, held weakly so a
    /// long-lived parent keeps none that was dropped. Registration and
    /// cancellation both take this lock, so a child is either registered
    /// before the parent's cancellation takes the list, and cancelled with
    /// it, or sees the parent already cancelled.
    pub(super) children: Mutex<Vec<Weak<Cancellation>>>,
}

impl CancellationToken {
    /// Cancels this token and every child made from it, and theirs.
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
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
            Self(child).cancel();
        }
    }

    /// A new cancellation of its own, cancelled when this one is, already
    /// if this one is now. Cancelling the child cancels neither this token
    /// nor any other child of it.
    pub fn child(&self) -> Self {
        let child = Self::default();
        let mut children = self
            .0
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.is_cancelled() {
            drop(children);
            child.cancel();
            return child;
        }
        children.retain(|registered| registered.strong_count() > 0);
        children.push(Arc::downgrade(&child.0));
        child
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    /// Resolves once the token is cancelled. Needs no particular async runtime.
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
            // Checked again after registering, so a cancellation between the
            // first check and the registration is not missed.
            if self.is_cancelled() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
