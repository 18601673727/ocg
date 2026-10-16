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
    /// What the row alone decides. A confirmed outcome needed every child
    /// subtree settled when it was written and the Job accepts no children
    /// afterwards, so it settles the subtree without a traversal. A Job
    /// whose own execution may still run has not settled. Only a locally
    /// stopped Job with an unknown outcome depends on its descendants.
    pub(super) fn of_row(job: &JobRow) -> Option<Self> {
        if job.confirmed_terminal() {
            Some(Self::Settled)
        } else if !job.locally_stopped() {
            Some(Self::Unsettled)
        } else {
            None
        }
    }
}

pub(super) struct WaitingTables<J, D, C> {
    jobs: J,
    depends_on: D,
    children: C,
    settlements: RefCell<HashMap<u64, Settlement>>,
}

impl<'tx>
    WaitingTables<
        Table<'tx, u64, &'static [u8]>,
        Table<'tx, (u64, u64), u8>,
        Table<'tx, (u64, u64), ()>,
    >
{
    pub(super) fn open(tx: &'tx WriteTransaction) -> Result<Self, StorageError> {
        Ok(Self {
            jobs: tx.open_table(JOBS)?,
            depends_on: tx.open_table(DEPENDS_ON)?,
            children: tx.open_table(CHILDREN)?,
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

    /// Whether the Job and every descendant have stopped. A confirmed
    /// outcome and a Job that may still run are decided by their rows. A
    /// locally stopped unknown outcome claims nothing about its children, so
    /// each descendant is followed until one is found that may still run;
    /// a confirmed descendant ends its branch. The Genesis self-parent
    /// marker is not in the children index, and the Job itself starts out
    /// visited, so nothing is followed twice. A missing child record is
    /// corruption.
    pub(super) fn subtree_settlement(&self, job: &JobRow) -> Result<Settlement, StorageError> {
        if let Some(settlement) = Settlement::of_row(job) {
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
                    match Settlement::of_row(&row) {
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

    /// Adds `id` to `targets` when its subtree has not settled. Returns
    /// `false` when that could not be decided within the bounds.
    fn push_unsettled(&self, targets: &mut Vec<u64>, id: u64) -> Result<bool, StorageError> {
        let Some(row) = self.job(id)? else {
            return Ok(true);
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
    /// A confirmed outcome waits for nothing. A Job that stopped with an
    /// unknown outcome never runs again, so its dependencies are obsolete,
    /// but it still waits for every child whose subtree is active: that
    /// edge is what keeps a descendant from being made to wait on it. The
    /// Genesis root marker is not in the children index, so the Genesis Job
    /// never waits for itself.
    ///
    /// `None` when a subtree traversal reached its bound.
    pub(super) fn waits_for(&self, id: u64) -> Result<Option<Vec<u64>>, StorageError> {
        let Some(job) = self.job(id)? else {
            return Ok(Some(Vec::new()));
        };
        if job.confirmed_terminal() {
            return Ok(Some(Vec::new()));
        }
        let mut targets = Vec::new();
        if !job.locally_stopped() {
            for entry in self.depends_on.range((id, 0)..=(id, u64::MAX))? {
                let (key, condition) = entry?;
                DependencyCondition::from_code(condition.value())
                    .ok_or_else(|| StorageError::corrupted("invalid dependency condition"))?;
                if !self.push_unsettled(&mut targets, key.value().1)? {
                    return Ok(None);
                }
            }
        }
        for entry in self.children.range((id, 0)..=(id, u64::MAX))? {
            if !self.push_unsettled(&mut targets, entry?.0.value().1)? {
                return Ok(None);
            }
        }
        Ok(Some(targets))
    }
}

pub(super) fn waiting_verdict(
    tx: &WriteTransaction,
    proposed: &[(JobId, JobId)],
) -> Result<WaitingVerdict, StorageError> {
    let tables = WaitingTables::open(tx)?;
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
