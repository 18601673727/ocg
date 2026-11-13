use super::*;

#[derive(Clone, Debug, Default)]
pub struct CancellationToken(pub(super) Arc<Cancellation>);

#[derive(Debug, Default)]
pub(super) struct Cancellation {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
    pub(super) children: Mutex<Vec<Weak<Cancellation>>>,
}

impl CancellationToken {
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
