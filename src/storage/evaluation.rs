use super::*;
use std::cell::Cell;
use std::ops::Bound;
use std::time::Instant;

struct Evaluation<'a> {
    transaction: &'a ReadTransaction,
    certified: bool,
    work: Cell<u32>,
    settlements: RefCell<HashMap<u64, Settlement>>,
}

use std::cell::RefCell;

impl Evaluation<'_> {
    fn charge(&self) {
        self.work.set(self.work.get().saturating_add(1));
    }

    fn job(&self, id: JobId) -> Result<Result<JobRow, Why>, StorageError> {
        self.charge();
        let jobs = self.transaction.open_table(JOBS)?;
        let Some(value) = jobs.get(id.get())? else {
            return Ok(Err(Why::MissingJob));
        };
        Ok(match JobRow::decode(value.value()) {
            Ok(row) if row.id == id => Ok(row),
            _ => Err(Why::Corrupt),
        })
    }

    fn settlement(&self, row: &JobRow) -> Result<Result<Settlement, Why>, StorageError> {
        if let Some(settlement) = Settlement::of_row(row, self.certified) {
            return Ok(Ok(settlement));
        }
        if let Some(settlement) = self.settlements.borrow().get(&row.id.get()) {
            return Ok(Ok(*settlement));
        }
        let result = self.walk(row.id)?;
        if let Ok(settlement) = result
            && settlement != Settlement::Unbounded
        {
            self.settlements
                .borrow_mut()
                .try_reserve(1)
                .map_err(execution_claim::allocation_error)?;
            self.settlements
                .borrow_mut()
                .insert(row.id.get(), settlement);
        }
        Ok(result)
    }

    fn walk(&self, root: JobId) -> Result<Result<Settlement, Why>, StorageError> {
        let children = self.transaction.open_table(CHILDREN)?;
        let mut visited = HashSet::new();
        visited
            .try_reserve(MAX_WAITING_JOBS + 1)
            .map_err(execution_claim::allocation_error)?;
        visited.insert(root.get());
        let mut frontier = Vec::new();
        frontier
            .try_reserve_exact(MAX_WAITING_JOBS)
            .map_err(execution_claim::allocation_error)?;
        frontier.push(root.get());
        let mut next = Vec::new();
        next.try_reserve_exact(MAX_WAITING_JOBS)
            .map_err(execution_claim::allocation_error)?;
        let mut examined = 0;
        for _ in 0..MAX_WAITING_LAYERS {
            if frontier.is_empty() {
                return Ok(Ok(Settlement::Settled));
            }
            next.clear();
            for parent in frontier.drain(..) {
                self.charge();
                for entry in children.range((parent, 0)..=(parent, u64::MAX))? {
                    self.charge();
                    let child = entry?.0.value().1;
                    examined += 1;
                    if examined > MAX_WAITING_JOBS {
                        return Ok(Ok(Settlement::Unbounded));
                    }
                    if !visited.insert(child) {
                        continue;
                    }
                    if visited.len() > MAX_WAITING_JOBS {
                        return Ok(Ok(Settlement::Unbounded));
                    }
                    let row = match self.job(JobId::new(child))? {
                        Ok(row) => row,
                        Err(Why::MissingJob) => {
                            return Ok(Err(Why::MissingPrerequisite(JobId::new(child))));
                        }
                        Err(why) => return Ok(Err(why)),
                    };
                    match Settlement::of_row(&row, self.certified) {
                        Some(Settlement::Settled) => {}
                        Some(_) => return Ok(Ok(Settlement::Unsettled)),
                        None => next.push(child),
                    }
                }
            }
            std::mem::swap(&mut frontier, &mut next);
        }
        Ok(Ok(if frontier.is_empty() {
            Settlement::Settled
        } else {
            Settlement::Unbounded
        }))
    }

    fn compatible(
        &self,
        job: JobId,
    ) -> Result<Result<BTreeSet<ExecutionPoolId>, Why>, StorageError> {
        self.charge();
        let input = self
            .transaction
            .open_table(HAS_INPUT)?
            .get(job.get())?
            .map(|value| value.value());
        let Some(input) = input else {
            return Ok(Err(Why::Corrupt));
        };
        self.charge();
        let inputs = self.transaction.open_table(JOB_INPUTS)?;
        let Some(value) = inputs.get(input)? else {
            return Ok(Err(Why::Corrupt));
        };
        let input = match JobInputRow::decode(value.value()) {
            Ok(input) => input,
            Err(_) => return Ok(Err(Why::Corrupt)),
        };
        let protocol = input
            .configuration
            .as_ref()
            .map(|config| config.llm.provider.transport);
        self.charge();
        let table = self.transaction.open_table(COMPATIBLE_POOLS)?;
        let pools = match table.get(pools::compatibility_key(protocol))? {
            Some(value) => pools::decode_compatible(value.value())?,
            None => BTreeSet::new(),
        };
        Ok(Ok(pools))
    }
}

pub(crate) fn evaluate_pending(
    database: &Database,
    request: EvalRequest,
) -> Result<EvalPage, StorageError> {
    let began = Instant::now();
    if request.candidates.is_empty()
        || request.candidates.len() > crate::persistence::EVALUATION_CANDIDATES
    {
        return Err(StorageError {
            kind: StorageErrorKind::Other,
            os_error: None,
            detail: "evaluate_pending requires 1..=64 candidates".into(),
        });
    }
    let transaction = read(database)?;
    let meta = transaction.open_table(META)?;
    let genesis = genesis_id(&meta)?;
    let certified = meta
        .get(waiting::SUBTREE_CLOSURE_KEY)?
        .is_some_and(|value| value.value() == b"certified");
    let evaluation = Evaluation {
        transaction: &transaction,
        certified,
        work: Cell::new(2),
        settlements: RefCell::default(),
    };
    let now = Timestamp::now();
    let mut verdicts = Vec::new();
    verdicts
        .try_reserve_exact(request.candidates.len())
        .map_err(execution_claim::allocation_error)?;
    let mut continuation = None;
    let mut first_prerequisite = true;
    for job in request.candidates {
        if !verdicts.is_empty() && evaluation.work.get() >= request.budget.get() {
            break;
        }
        let row = match evaluation.job(job)? {
            Ok(row) => row,
            Err(why) => {
                verdicts.push((job, Readiness::Indeterminate(why)));
                continue;
            }
        };
        let blocked = if job == genesis {
            Some(Block::Genesis)
        } else if !row.awaits_start() {
            Some(Block::NotPending)
        } else if row.paused_at.is_some() {
            Some(Block::Paused)
        } else {
            row.eligible_at
                .filter(|at| *at > now)
                .map(|eligible_at| Block::NotYetEligible { eligible_at })
        };
        if let Some(block) = blocked {
            verdicts.push((job, Readiness::Blocked(block)));
            continue;
        }
        let pools = match evaluation.compatible(job)? {
            Ok(pools) => pools,
            Err(why) => {
                verdicts.push((job, Readiness::Indeterminate(why)));
                continue;
            }
        };
        if pools.is_empty() {
            verdicts.push((job, Readiness::Blocked(Block::NoCompatiblePool)));
            continue;
        }
        let previous = request
            .continuation
            .filter(|cursor| verdicts.is_empty() && cursor.job == job)
            .map(|cursor| cursor.after_prerequisite);
        let mut satisfied = previous;
        let start = previous.map_or(Bound::Included((job.get(), 0)), |previous| {
            Bound::Excluded((job.get(), previous.get()))
        });
        evaluation.charge();
        let dependencies = transaction.open_table(DEPENDS_ON)?;
        let mut verdict = Readiness::Ready {
            compatible_pools: pools,
        };
        for entry in dependencies.range((start, Bound::Included((job.get(), u64::MAX))))? {
            // Seeking/yielding is charged even if this entry is deferred. The
            // cursor records only prerequisites actually proved Satisfied.
            evaluation.charge();
            let (key, condition) = entry?;
            let prerequisite = JobId::new(key.value().1);
            if !first_prerequisite && evaluation.work.get() >= request.budget.get() {
                verdict = Readiness::BudgetExhausted;
                continuation = Some(EvalContinuation {
                    job,
                    after_prerequisite: satisfied.unwrap_or(JobId::new(0)),
                });
                break;
            }
            first_prerequisite = false;
            let Some(condition) = DependencyCondition::from_code(condition.value()) else {
                verdict = Readiness::Indeterminate(Why::Corrupt);
                break;
            };
            let prerequisite_row = match evaluation.job(prerequisite)? {
                Ok(row) => row,
                Err(Why::MissingJob) => {
                    verdict = Readiness::Indeterminate(Why::MissingPrerequisite(prerequisite));
                    break;
                }
                Err(why) => {
                    verdict = Readiness::Indeterminate(why);
                    break;
                }
            };
            let settlement = if condition == DependencyCondition::Settled {
                match evaluation.settlement(&prerequisite_row)? {
                    Ok(settlement) => settlement,
                    Err(why) => {
                        verdict = Readiness::Indeterminate(why);
                        break;
                    }
                }
            } else {
                Settlement::Unsettled
            };
            if settlement == Settlement::Unbounded {
                verdict = Readiness::Indeterminate(Why::Unbounded);
                break;
            }
            match dependency_state_with(
                condition,
                &prerequisite_row,
                settlement == Settlement::Settled,
            ) {
                DependencyState::Satisfied => satisfied = Some(prerequisite),
                DependencyState::Waiting => {
                    verdict = Readiness::Blocked(Block::DependencyWaiting);
                    break;
                }
                DependencyState::Impossible => {
                    verdict = Readiness::Blocked(Block::DependencyImpossible);
                    break;
                }
            }
        }
        let exhausted = verdict == Readiness::BudgetExhausted;
        verdicts.push((job, verdict));
        if exhausted {
            break;
        }
    }
    Ok(EvalPage {
        verdicts,
        continuation,
        work_used: evaluation.work.get(),
        service_time: began.elapsed(),
    })
}
