use std::cell::RefCell;

use redb::Table;

use super::*;

// ---- waiting graph -------------------------------------------------------------

/// The state of a dependency on `prerequisite`. `subtree_settled` is the
/// prerequisite's subtree settlement; only a `Settled` condition reads it.
///
/// A `Success` dependency is impossible as soon as the prerequisite stopped
/// without completing, even while its subtree still runs, because an
/// outcome is never converted to Completed. That is a statement about the
/// outcome, not about waiting: the edge to the prerequisite stays in the
/// waiting graph until the subtree has settled, see
/// [`WaitingTables::waits_for`].
pub(crate) fn dependency_state_with(
    condition: DependencyCondition,
    prerequisite: &JobRow,
    subtree_settled: bool,
) -> DependencyState {
    if prerequisite.completed_at.is_some() {
        return DependencyState::Satisfied;
    }
    match condition {
        DependencyCondition::Settled if subtree_settled => DependencyState::Satisfied,
        DependencyCondition::Settled => DependencyState::Waiting,
        DependencyCondition::Success if prerequisite.locally_stopped() => {
            DependencyState::Impossible
        }
        DependencyCondition::Success => DependencyState::Waiting,
    }
}

/// [`dependency_state_with`] for a prerequisite whose children are not
/// consulted, as the tests that pin the row-only reading build it.
#[cfg(test)]
pub(crate) fn dependency_state(
    condition: DependencyCondition,
    prerequisite: &JobRow,
) -> DependencyState {
    dependency_state_with(condition, prerequisite, prerequisite.locally_stopped())
}

pub(super) enum WaitingVerdict {
    Acyclic,
    Cycle(Vec<JobId>),
    Limit,
}

/// Whether a Job's subtree has settled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Settlement {
    Settled,
    Unsettled,
    /// The traversal reached its bound before it could tell. Callers refuse
    /// rather than read this as either answer.
    Unbounded,
}

impl Settlement {
    /// What the row alone decides. A Job whose own execution may still run
    /// has not settled. A confirmed outcome settles the subtree without a
    /// traversal only where the file is `certified`: finalization requires
    /// every child subtree to have settled and the Job accepts no children
    /// afterwards, but an earlier build required only that each child had
    /// locally stopped, see [`certify_subtree_closure`]. Anything else
    /// depends on the descendants.
    pub(super) fn of_row(job: &JobRow, certified: bool) -> Option<Self> {
        if !job.locally_stopped() {
            Some(Self::Unsettled)
        } else if job.confirmed_terminal() && certified {
            Some(Self::Settled)
        } else {
            None
        }
    }
}

/// The meta entry that records the most recent [`certify_subtree_closure`]
/// as passed. A missing entry means the confirmed outcomes of the file are
/// not trusted to have settled subtrees.
pub(super) const SUBTREE_CLOSURE_KEY: &str = "subtree_closure";
const CERTIFIED: &[u8] = b"certified";

/// Whether the file's confirmed outcomes were certified when it was opened.
pub(super) fn closure_certified(tx: &WriteTransaction) -> Result<bool, StorageError> {
    Ok(tx
        .open_table(META)?
        .get(SUBTREE_CLOSURE_KEY)?
        .is_some_and(|v| v.value() == CERTIFIED))
}

/// Records the result of [`certify_subtree_closure`] in the open transaction.
pub(super) fn record_closure(tx: &WriteTransaction, certified: bool) -> Result<(), StorageError> {
    let mut meta = tx.open_table(META)?;
    if certified {
        meta.insert(SUBTREE_CLOSURE_KEY, CERTIFIED)?;
    } else {
        meta.remove(SUBTREE_CLOSURE_KEY)?;
    }
    Ok(())
}

/// Whether no Job that may still run lies below a confirmed outcome, which
/// is what lets [`Settlement::of_row`] settle a confirmed outcome from its
/// row. A file written by this build keeps that true, but a file written by
/// an earlier build can hold a confirmed outcome over a locally stopped
/// unknown outcome over a Job that still runs, since that build finalized a
/// Job once each child had merely stopped locally. A matching format version
/// does not say which build wrote the file, so every opening reads the whole
/// Job table once, in time linear in the Jobs.
///
/// Only a locally stopped unknown outcome lets a descendant escape its
/// confirmed ancestor's finalization, so each Job that may still run follows
/// its ancestors through such outcomes, and the first confirmed ancestor
/// fails the check. An ancestor that was followed once is not followed
/// again. A Job whose parent record is missing is corruption.
pub(super) fn certify_subtree_closure(tx: &WriteTransaction) -> Result<bool, StorageError> {
    let jobs = tx.open_table(JOBS)?;
    let child_of = tx.open_table(CHILD_OF)?;
    let mut followed = HashSet::new();
    for entry in jobs.iter()? {
        let (id, row) = entry?;
        if JobRow::decode(row.value())?.locally_stopped() {
            continue;
        }
        let mut at = id.value();
        let mut path = HashSet::new();
        loop {
            let parent = child_of
                .get(at)?
                .ok_or_else(|| StorageError::corrupted(format!("{at} has no parent record")))?
                .value();
            // The Genesis Job is its own parent.
            if parent == at || followed.contains(&parent) {
                break;
            }
            let Some(parent_row) = jobs.get(parent)? else {
                return Err(StorageError::corrupted(format!(
                    "{at} names the missing parent {parent}"
                )));
            };
            let parent_row = JobRow::decode(parent_row.value())?;
            if parent_row.confirmed_terminal() {
                return Ok(false);
            }
            // A Job that may still run is checked for its own ancestors.
            if !parent_row.locally_stopped() {
                break;
            }
            if !path.insert(parent) {
                return Err(StorageError::corrupted(format!(
                    "{parent} is its own ancestor"
                )));
            }
            at = parent;
        }
        followed.extend(path);
    }
    Ok(true)
}

pub(super) struct WaitingTables<J, D, C> {
    jobs: J,
    depends_on: D,
    children: C,
    /// Whether confirmed outcomes settle their subtrees, see
    /// [`certify_subtree_closure`].
    certified: bool,
    /// The Job an admission is about to create. It has no record yet and is
    /// the only Job allowed to be missing.
    candidate: Option<u64>,
    settlements: RefCell<HashMap<u64, Settlement>>,
}

impl<'tx>
    WaitingTables<
        Table<'tx, u64, &'static [u8]>,
        Table<'tx, (u64, u64), u8>,
        Table<'tx, (u64, u64), ()>,
    >
{
    pub(super) fn open(
        tx: &'tx WriteTransaction,
        certified: bool,
        candidate: Option<JobId>,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            jobs: tx.open_table(JOBS)?,
            depends_on: tx.open_table(DEPENDS_ON)?,
            children: tx.open_table(CHILDREN)?,
            certified,
            candidate: candidate.map(JobId::get),
            settlements: RefCell::default(),
        })
    }
}

impl<J, D, C> WaitingTables<J, D, C>
where
    J: ReadableTable<u64, &'static [u8]>,
    D: ReadableTable<(u64, u64), u8>,
    C: ReadableTable<(u64, u64), ()>,
{
    pub(super) fn job(&self, id: u64) -> Result<Option<JobRow>, StorageError> {
        self.jobs
            .get(id)?
            .map(|v| JobRow::decode(v.value()))
            .transpose()
    }

    /// Whether the Job and every descendant have stopped. A Job that may
    /// still run is decided by its row, and so is a confirmed outcome in a
    /// certified file. Otherwise each descendant is followed until one is
    /// found that may still run; in a certified file a confirmed descendant
    /// ends its branch. The Genesis self-parent marker is not in the
    /// children index, and the Job itself starts out visited, so nothing is
    /// followed twice. A missing child record is corruption.
    pub(super) fn subtree_settlement(&self, job: &JobRow) -> Result<Settlement, StorageError> {
        if let Some(settlement) = Settlement::of_row(job, self.certified) {
            return Ok(settlement);
        }
        if let Some(settlement) = self.settlements.borrow().get(&job.id.get()) {
            return Ok(*settlement);
        }
        let settlement = self.walk_descendants(job.id.get())?;
        if settlement != Settlement::Unbounded {
            self.settlements
                .borrow_mut()
                .insert(job.id.get(), settlement);
        }
        Ok(settlement)
    }

    fn walk_descendants(&self, root: u64) -> Result<Settlement, StorageError> {
        let mut visited = HashSet::from([root]);
        let mut frontier = vec![root];
        let mut layers = 0;
        while !frontier.is_empty() {
            if layers == MAX_WAITING_LAYERS {
                return Ok(Settlement::Unbounded);
            }
            layers += 1;
            let mut next = Vec::new();
            for parent in frontier {
                for entry in self.children.range((parent, 0)..=(parent, u64::MAX))? {
                    let child = entry?.0.value().1;
                    if !visited.insert(child) {
                        continue;
                    }
                    if visited.len() > MAX_WAITING_JOBS {
                        return Ok(Settlement::Unbounded);
                    }
                    let Some(row) = self.job(child)? else {
                        return Err(StorageError::corrupted(format!(
                            "{parent} lists the missing child {child}"
                        )));
                    };
                    match Settlement::of_row(&row, self.certified) {
                        Some(Settlement::Settled) => {}
                        Some(_) => return Ok(Settlement::Unsettled),
                        None => next.push(child),
                    }
                }
            }
            frontier = next;
        }
        Ok(Settlement::Settled)
    }

    /// Adds `id`, which `by` names, to `targets` when its subtree has not
    /// settled. Returns `false` when that could not be decided within the
    /// bounds. A persisted relationship never names a missing Job, not even
    /// the candidate of an admission, which no relationship names yet.
    fn push_unsettled(
        &self,
        targets: &mut Vec<u64>,
        id: u64,
        by: u64,
    ) -> Result<bool, StorageError> {
        let Some(row) = self.job(id)? else {
            return Err(StorageError::corrupted(format!(
                "{by} names the missing Job {id}"
            )));
        };
        match self.subtree_settlement(&row)? {
            Settlement::Settled => {}
            Settlement::Unsettled => targets.push(id),
            Settlement::Unbounded => return Ok(false),
        }
        Ok(true)
    }

    /// Active waiting edges out of `id`: prerequisites and children whose
    /// subtree has not settled. A dependency waits on the whole subtree of
    /// its prerequisite, whatever its condition, because a `Success`
    /// dependency that can no longer be satisfied still orders the two Jobs
    /// until the prerequisite's descendants have stopped.
    ///
    /// A confirmed outcome of a certified file waits for nothing. A Job that
    /// stopped never runs again, so its dependencies are obsolete, but it
    /// still waits for every child whose subtree is active. Only an unknown
    /// outcome, or a confirmed outcome of an uncertified file, can have one,
    /// and that edge is what keeps a descendant from being made to wait on
    /// it. The Genesis root marker is not in the children index, so the
    /// Genesis Job never waits for itself.
    ///
    /// A Job that a relationship names must be stored, and so must `id`,
    /// unless it is the admission's candidate. `None` when a subtree
    /// traversal reached its bound.
    pub(super) fn waits_for(&self, id: u64) -> Result<Option<Vec<u64>>, StorageError> {
        let Some(job) = self.job(id)? else {
            // The candidate has no record and no persisted relationships; its
            // proposed edges are added by the caller. Any other Job reached
            // here was named by a relationship or a required reference.
            return if self.candidate == Some(id) {
                Ok(Some(Vec::new()))
            } else {
                Err(StorageError::corrupted(format!(
                    "the waiting graph reaches the missing Job {id}"
                )))
            };
        };
        if job.confirmed_terminal() && self.certified {
            return Ok(Some(Vec::new()));
        }
        let mut targets = Vec::new();
        if !job.locally_stopped() {
            for entry in self.depends_on.range((id, 0)..=(id, u64::MAX))? {
                let (key, condition) = entry?;
                DependencyCondition::from_code(condition.value())
                    .ok_or_else(|| StorageError::corrupted("invalid dependency condition"))?;
                if !self.push_unsettled(&mut targets, key.value().1, id)? {
                    return Ok(None);
                }
            }
        }
        for entry in self.children.range((id, 0)..=(id, u64::MAX))? {
            if !self.push_unsettled(&mut targets, entry?.0.value().1, id)? {
                return Ok(None);
            }
        }
        Ok(Some(targets))
    }
}

/// The verdict on the waiting graph with the `proposed` edges added. Every
/// Job the graph reaches must be stored, except `candidate`, the Job the
/// admission is about to create.
pub(super) fn waiting_verdict(
    tx: &WriteTransaction,
    certified: bool,
    candidate: JobId,
    proposed: &[(JobId, JobId)],
) -> Result<WaitingVerdict, StorageError> {
    let tables = WaitingTables::open(tx, certified, Some(candidate))?;
    let mut graph: DiGraph<u64, ()> = DiGraph::new();
    let mut nodes: HashMap<u64, NodeIndex> = HashMap::new();
    let mut node = |graph: &mut DiGraph<u64, ()>, id: u64| -> Option<NodeIndex> {
        if let Some(index) = nodes.get(&id) {
            return Some(*index);
        }
        if nodes.len() >= MAX_WAITING_JOBS {
            return None;
        }
        let index = graph.add_node(id);
        nodes.insert(id, index);
        Some(index)
    };
    let mut extra: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut edges = HashSet::new();
    let mut expanded = HashSet::new();
    let mut frontier = Vec::new();
    for (from, to) in proposed {
        extra.entry(from.get()).or_default().push(to.get());
        let (Some(f), Some(t)) = (node(&mut graph, from.get()), node(&mut graph, to.get())) else {
            return Ok(WaitingVerdict::Limit);
        };
        if edges.insert((f, t)) {
            graph.add_edge(f, t, ());
        }
        if !frontier.contains(&t) {
            frontier.push(t);
        }
    }
    let mut layers = 0;
    while !frontier.is_empty() {
        if layers == MAX_WAITING_LAYERS {
            return Ok(WaitingVerdict::Limit);
        }
        layers += 1;
        let mut next = Vec::new();
        for index in frontier {
            if !expanded.insert(index) {
                continue;
            }
            let id = graph[index];
            let Some(mut targets) = tables.waits_for(id)? else {
                return Ok(WaitingVerdict::Limit);
            };
            if let Some(more) = extra.get(&id) {
                targets.extend(more);
            }
            for target in targets {
                let Some(t) = node(&mut graph, target) else {
                    return Ok(WaitingVerdict::Limit);
                };
                if edges.insert((index, t)) {
                    graph.add_edge(index, t, ());
                }
                if !expanded.contains(&t) {
                    next.push(t);
                }
            }
        }
        frontier = next;
    }
    if !petgraph::algo::is_cyclic_directed(&graph) {
        return Ok(WaitingVerdict::Acyclic);
    }
    let cycle = petgraph::algo::tarjan_scc(&graph)
        .into_iter()
        .find(|component| component.len() > 1 || graph.contains_edge(component[0], component[0]))
        .map(|component| {
            component
                .into_iter()
                .map(|index| JobId::new(graph[index]))
                .collect()
        })
        .unwrap_or_default();
    Ok(WaitingVerdict::Cycle(cycle))
}
