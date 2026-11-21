use std::time::{Duration, Instant};

use ntex::util::{Either, select};

use super::allocation::Allocator;
use crate::ai::providers::CancellationToken;
use crate::db::protection::ProtectionWatch;
use crate::db::{DatabaseHandle, SchedulerWakeSubscription};

const BATCH_FLOOR: Duration = Duration::from_millis(5);
const RECONCILE: Duration = Duration::from_secs(30);

pub(crate) struct Scheduler {
    wake: SchedulerWakeSubscription,
    protection: ProtectionWatch,
    shutdown: CancellationToken,
}

impl Scheduler {
    pub(crate) fn new(
        database: &DatabaseHandle,
        wake: SchedulerWakeSubscription,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            wake,
            protection: database.watch_disk_protection(),
            shutdown,
        }
    }

    pub(super) async fn run(self, scheduler: &Allocator) {
        let mut previous_batch = None;
        let mut next_due = None;
        let mut recheck_at = Instant::now();
        loop {
            if self.shutdown.is_cancelled() {
                return;
            }
            let mut database_changed = false;
            let mut recovered = false;
            while self.protection.status().lock.is_some() {
                match select(self.shutdown.cancelled(), self.protection.changed()).await {
                    Either::Left(()) => return,
                    Either::Right(Some(_)) => recovered = true,
                    Either::Right(None) => {
                        self.shutdown.cancelled().await;
                        return;
                    }
                }
            }
            if let Some(previous) = previous_batch {
                if !recovered {
                    let timer = batch_delay(previous, next_due)
                        .min(recheck_at.saturating_duration_since(Instant::now()));
                    let waking = select(
                        self.wake.recv_async(),
                        select(self.protection.changed(), ntex::time::sleep(timer)),
                    );
                    match select(self.shutdown.cancelled(), waking).await {
                        Either::Left(()) => return,
                        Either::Right(Either::Left(Err(_))) => return,
                        Either::Right(Either::Left(Ok(()))) => database_changed = true,
                        Either::Right(Either::Right(_)) => {}
                    }
                }
                let remaining = BATCH_FLOOR.saturating_sub(previous.elapsed());
                if let Either::Left(()) =
                    select(self.shutdown.cancelled(), ntex::time::sleep(remaining)).await
                {
                    return;
                }
            }
            if self.protection.status().lock.is_some() {
                continue;
            }
            while self.wake.try_recv().is_ok() {
                database_changed = true;
            }
            let recheck = Instant::now() >= recheck_at;
            if recheck {
                recheck_at = Instant::now() + RECONCILE;
            }
            let result = scheduler.dispatch(database_changed, recheck).await;
            previous_batch = Some(Instant::now());
            match result {
                Ok(due) => next_due = due,
                Err(error) => {
                    log::error!("scheduler dispatch failed: {error}");
                    next_due = None;
                }
            }
        }
    }
}

fn batch_delay(previous: Instant, next_due: Option<Duration>) -> Duration {
    let due = next_due
        .map(|due| due.saturating_sub(previous.elapsed()))
        .unwrap_or(RECONCILE)
        .min(RECONCILE);
    due.max(BATCH_FLOOR.saturating_sub(previous.elapsed()))
}
