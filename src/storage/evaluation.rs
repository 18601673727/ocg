use super::*;
use crate::persistence::{Block, ConfigBody, EvalPage, EvalRequest, Readiness, Why};

/// Evaluates the candidates of a sweep against the readiness tables: a
/// candidate in `ready` is ready for the enabled Pools that offer what it
/// needs, one in `delayed` is blocked until its time, and one in neither is
/// blocked on a dependency. Each candidate costs one unit of the budget.
pub(crate) fn evaluate_pending(
    database: &Database,
    request: EvalRequest,
) -> Result<EvalPage, StorageError> {
    let began = std::time::Instant::now();
    let rx = read(database)?;
    let clock = open::promotion_clock();
    let pools: Vec<_> = {
        let mut pools = Vec::new();
        for entry in rx.open_table(CONFIGS)?.iter()? {
            let row = ConfigRow::decode(entry?.1.value())?;
            if let ConfigBody::Pool(body) = row.body
                && body.enabled
                && row.archived_at.is_none()
            {
                pools.push((row.id, body));
            }
        }
        pools
    };
    let mut verdicts = Vec::new();
    let mut work = 0u32;
    for job in request.candidates {
        if work >= request.budget.get() {
            verdicts.push((job, Readiness::BudgetExhausted));
            break;
        }
        work += 1;
        let Some(row) = rx
            .open_table(JOBS)?
            .get(job.get())?
            .map(|bytes| JobRow::decode(bytes.value()))
            .transpose()?
        else {
            verdicts.push((job, Readiness::Indeterminate(Why::MissingJob)));
            continue;
        };
        let state = rx
            .open_table(JOB_STATES)?
            .get(job.get())?
            .map(|bytes| JobStateRow::decode(bytes.value()))
            .transpose()?
            .ok_or_else(|| StorageError::corrupted(format!("{job} has no job_state")))?;
        if state.claim.is_some() || state.outcome.is_some() {
            verdicts.push((job, Readiness::Blocked(Block::NotPending)));
            continue;
        }
        let requirement = transactions::job_requirement(&row);
        let ready = rx
            .open_table(READY)?
            .get((requirement, job.get()))?
            .is_some();
        let due = match row.eligible_at.map(Timestamp::as_unix_micros) {
            Some(eligible_at) if !ready => {
                let delayed = rx
                    .open_table(DELAYED)?
                    .get((eligible_at, job.get()))?
                    .is_some();
                if delayed && eligible_at > clock {
                    verdicts.push((
                        job,
                        Readiness::Blocked(Block::NotYetEligible {
                            eligible_at: Timestamp::from_unix_micros(eligible_at),
                        }),
                    ));
                    continue;
                }
                delayed
            }
            _ => false,
        };
        if !ready && !due {
            verdicts.push((job, Readiness::Blocked(Block::DependencyWaiting)));
            continue;
        }
        let compatible: BTreeSet<ExecutionPoolId> = pools
            .iter()
            .filter(|(_, body)| {
                body.capabilities.iter().any(|(operation, protocol)| {
                    transactions::requirement(Some(*operation), *protocol) == requirement
                })
            })
            .map(|(id, _)| (*id).into())
            .collect();
        verdicts.push((
            job,
            if compatible.is_empty() {
                Readiness::Blocked(Block::NoCompatiblePool)
            } else {
                Readiness::Ready {
                    compatible_pools: compatible,
                }
            },
        ));
    }
    Ok(EvalPage {
        verdicts,
        continuation: None,
        work_used: work,
        service_time: began.elapsed(),
    })
}
