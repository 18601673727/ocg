use std::future::Future;
use std::time::{Duration, Instant};

use ntex::util::{Either, select};

use crate::database::{DatabaseError, DatabaseHandle, SchedulerWakeSubscription};
use crate::disk_protection::ProtectionWatch;
use crate::provider_execution::CancellationToken;

const BATCH_FLOOR: Duration = Duration::from_millis(5);
const RECONCILE: Duration = Duration::from_secs(30);

pub(crate) struct DispatchBatch<G> {
    pub(crate) grants: Vec<G>,
    pub(crate) next_due: Option<Duration>,
}

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

    pub(crate) async fn run<G, Claim, ClaimFuture, Spawn, SpawnFuture>(
        self,
        mut claim: Claim,
        mut spawn: Spawn,
    ) where
        Claim: FnMut() -> ClaimFuture,
        ClaimFuture: Future<Output = Result<DispatchBatch<G>, DatabaseError>>,
        Spawn: FnMut(G) -> SpawnFuture,
        SpawnFuture: Future<Output = ()>,
    {
        let mut previous_batch = None;
        let mut next_due = None;
        loop {
            if self.shutdown.is_cancelled() {
                return;
            }
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
                    let timer = batch_delay(previous, next_due);
                    let waking = select(
                        self.wake.recv_async(),
                        select(self.protection.changed(), ntex::time::sleep(timer)),
                    );
                    match select(self.shutdown.cancelled(), waking).await {
                        Either::Left(()) => return,
                        Either::Right(Either::Left(Err(_))) => return,
                        Either::Right(_) => {}
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
            while self.wake.try_recv().is_ok() {}
            let batch = claim().await;
            previous_batch = Some(Instant::now());
            match batch {
                Ok(batch) => {
                    next_due = batch.next_due;
                    for grant in batch.grants {
                        spawn(grant).await;
                    }
                }
                Err(error) => {
                    log::error!("dispatcher claim batch failed: {error}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::test_support::data_directory;
    use std::cell::RefCell;

    #[test]
    fn committed_grants_spawn_in_batch_order() {
        let (_root, directory) = data_directory("dispatcher", "grant-order");
        let mut worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let wake = worker.take_scheduler_wake().expect("one dispatcher");
        ntex::rt::System::new("dispatcher-test", ntex::rt::DefaultRuntime).block_on(async move {
            let shutdown = CancellationToken::default();
            let spawned = RefCell::new(Vec::new());
            let mut batches = 0;
            Scheduler::new(&database, wake, shutdown.clone())
                .run(
                    || {
                        batches += 1;
                        std::future::ready(Ok(DispatchBatch {
                            grants: vec![1, 2, 3],
                            next_due: None,
                        }))
                    },
                    |grant| {
                        spawned.borrow_mut().push(grant);
                        if grant == 3 {
                            shutdown.cancel();
                        }
                        std::future::ready(())
                    },
                )
                .await;
            assert_eq!(*spawned.borrow(), vec![1, 2, 3]);
            assert_eq!(batches, 1);
        });
        worker.shutdown().expect("shutdown database");
    }

    #[test]
    fn a_refused_batch_spawns_nothing() {
        let (_root, directory) = data_directory("dispatcher", "refused-batch");
        let mut worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let wake = worker.take_scheduler_wake().expect("one dispatcher");
        ntex::rt::System::new("dispatcher-test", ntex::rt::DefaultRuntime).block_on(async move {
            let shutdown = CancellationToken::default();
            let spawned = RefCell::new(Vec::<u32>::new());
            let mut batches = 0;
            Scheduler::new(&database, wake, shutdown.clone())
                .run(
                    || {
                        batches += 1;
                        shutdown.cancel();
                        std::future::ready(Err::<DispatchBatch<u32>, _>(DatabaseError::QueueFull))
                    },
                    |grant| {
                        spawned.borrow_mut().push(grant);
                        std::future::ready(())
                    },
                )
                .await;
            assert!(spawned.borrow().is_empty());
            assert_eq!(batches, 1);
        });
        worker.shutdown().expect("shutdown database");
    }
}
