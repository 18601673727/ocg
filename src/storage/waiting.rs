use super::*;

// ---- waiting graph -------------------------------------------------------------

pub(crate) fn dependency_state(
    condition: DependencyCondition,
    prerequisite: &JobRow,
) -> DependencyState {
    if prerequisite.completed_at.is_some() {
        DependencyState::Satisfied
    } else if prerequisite.settled() {
        match condition {
            DependencyCondition::Settled => DependencyState::Satisfied,
            DependencyCondition::Success => DependencyState::Impossible,
        }
    } else {
        DependencyState::Waiting
    }
}

pub(super) enum WaitingVerdict {
    Acyclic,
    Cycle(Vec<JobId>),
    Limit,
}

pub(super) struct WaitingTables<J, D, C> {
    jobs: J,
    depends_on: D,
    children: C,
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

    /// Active waiting edges out of `id`: dependencies still waiting on an
    /// unsettled prerequisite and unsettled children. A settled Job waits
    /// for nothing. The Genesis root marker is not in the children index, so
    /// the Genesis Job never waits for itself.
    pub(super) fn waits_for(&self, id: u64) -> Result<Vec<u64>, StorageError> {
        let Some(job) = self.job(id)? else {
            return Ok(Vec::new());
        };
        if job.settled() {
            return Ok(Vec::new());
        }
        let mut targets = Vec::new();
        for entry in self.depends_on.range((id, 0)..=(id, u64::MAX))? {
            let (key, condition) = entry?;
            let prerequisite = key.value().1;
            let condition = DependencyCondition::from_code(condition.value())
                .ok_or_else(|| StorageError::corrupted("invalid dependency condition"))?;
            if let Some(row) = self.job(prerequisite)?
                && dependency_state(condition, &row) == DependencyState::Waiting
            {
                targets.push(prerequisite);
            }
        }
        for entry in self.children.range((id, 0)..=(id, u64::MAX))? {
            let child = entry?.0.value().1;
            if let Some(row) = self.job(child)?
                && !row.settled()
            {
                targets.push(child);
            }
        }
        Ok(targets)
    }
}

pub(super) fn waiting_verdict(
    tx: &WriteTransaction,
    proposed: &[(JobId, JobId)],
) -> Result<WaitingVerdict, StorageError> {
    let tables = WaitingTables {
        jobs: tx.open_table(JOBS)?,
        depends_on: tx.open_table(DEPENDS_ON)?,
        children: tx.open_table(CHILDREN)?,
    };
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
            let mut targets = tables.waits_for(id)?;
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
