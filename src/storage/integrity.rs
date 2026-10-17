use super::*;

/// Test fault probe: replaces IN_PROJECT with a table of another key type,
/// so every later read that traverses a Job's Projects fails instead of
/// succeeding unnoticed.
#[cfg(test)]
pub(crate) fn break_project_links(database: &Database) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.delete_table(IN_PROJECT)?;
    tx.open_table(TableDefinition::<&str, u64>::new("in_project"))?;
    tx.commit()?;
    Ok(())
}

/// Test fault: removes a Job's HAS_INPUT edge, leaving its incoming index.
#[cfg(test)]
pub(crate) fn remove_input_link(database: &Database, job: JobId) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.open_table(HAS_INPUT)?.remove(job.get())?;
    tx.commit()?;
    Ok(())
}

/// Test fault: removes a Job's row while its observations still link to it,
/// a dangling observation reference.
#[cfg(test)]
pub(crate) fn remove_job_row(database: &Database, job: JobId) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.open_table(JOBS)?.remove(job.get())?;
    tx.commit()?;
    Ok(())
}

/// Checks every relationship table against its incoming index and its
/// endpoint tables, and the Genesis root against the parent tree. Returns
/// one line per inconsistency.
#[cfg(test)]
pub(crate) fn relationship_inconsistencies(
    database: &Database,
) -> Result<Vec<String>, StorageError> {
    use std::collections::BTreeMap;

    pub(super) fn ids(rx: &ReadTransaction, table: Records) -> Result<BTreeSet<u64>, StorageError> {
        let mut ids = BTreeSet::new();
        for entry in rx.open_table(table)?.iter()? {
            ids.insert(entry?.0.value());
        }
        Ok(ids)
    }
    pub(super) fn link_pairs(
        rx: &ReadTransaction,
        table: Link,
    ) -> Result<BTreeSet<(u64, u64)>, StorageError> {
        let mut pairs = BTreeSet::new();
        for entry in rx.open_table(table)?.iter()? {
            let (from, to) = entry?;
            pairs.insert((from.value(), to.value()));
        }
        Ok(pairs)
    }
    pub(super) fn set_pairs(
        rx: &ReadTransaction,
        table: PairSet,
    ) -> Result<BTreeSet<(u64, u64)>, StorageError> {
        let mut pairs = BTreeSet::new();
        for entry in rx.open_table(table)?.iter()? {
            pairs.insert(entry?.0.value());
        }
        Ok(pairs)
    }
    pub(super) fn reversed(pairs: &BTreeSet<(u64, u64)>) -> BTreeSet<(u64, u64)> {
        pairs.iter().map(|(from, to)| (*to, *from)).collect()
    }

    let rx = read(database)?;
    let genesis = genesis_id(&rx.open_table(META)?)?.get();
    let jobs = ids(&rx, JOBS)?;
    let inputs = ids(&rx, JOB_INPUTS)?;
    let executors = ids(&rx, EXECUTORS)?;
    let projects = ids(&rx, PROJECTS)?;
    let agents = ids(&rx, AGENTS)?;
    let providers = ids(&rx, LLM_PROVIDERS)?;
    let metrics = ids(&rx, METRICS)?;
    let availability_stats = ids(&rx, AVAILABILITY_STATS)?;
    let attachments = ids(&rx, ATTACHMENTS)?;
    let mut profile_kinds = BTreeMap::new();
    for entry in rx.open_table(PROFILES)?.iter()? {
        let (id, bytes) = entry?;
        profile_kinds.insert(id.value(), ProfileRow::decode(bytes.value())?.config.kind());
    }
    let agent_profiles: BTreeSet<u64> = profile_kinds
        .iter()
        .filter(|(_, kind)| **kind == ProfileKind::Agent)
        .map(|(id, _)| *id)
        .collect();
    let llm_profiles: BTreeSet<u64> = profile_kinds
        .iter()
        .filter(|(_, kind)| **kind == ProfileKind::Llm)
        .map(|(id, _)| *id)
        .collect();

    let mut problems = Vec::new();
    let mut has_attachment = BTreeSet::new();
    let mut positions: BTreeMap<u64, u32> = BTreeMap::new();
    for entry in rx.open_table(HAS_ATTACHMENT)?.iter()? {
        let (key, attachment) = entry?;
        let (input, position) = key.value();
        let next = positions.entry(input).or_default();
        if position != *next {
            problems.push(format!(
                "the Attachments of input {input} are not numbered from 0 without gaps"
            ));
        }
        *next = position.saturating_add(1);
        if !has_attachment.insert((input, attachment.value())) {
            problems.push(format!(
                "input {input} names attachment {} more than once",
                attachment.value()
            ));
        }
    }
    let mut check_endpoints =
        |name: &str, pairs: &BTreeSet<(u64, u64)>, from: &BTreeSet<u64>, to: &BTreeSet<u64>| {
            for (source, target) in pairs {
                if !from.contains(source) || !to.contains(target) {
                    problems.push(format!(
                        "{name} ({source}, {target}) names a missing or mistyped endpoint"
                    ));
                }
            }
        };

    let has_input = link_pairs(&rx, HAS_INPUT)?;
    let child_of = link_pairs(&rx, CHILD_OF)?;
    let retry_of = link_pairs(&rx, RETRY_OF)?;
    let in_project = set_pairs(&rx, IN_PROJECT)?;
    let uses_profile = set_pairs(&rx, USES_PROFILE)?;
    let uses_provider = link_pairs(&rx, USES_PROVIDER)?;
    let measures = link_pairs(&rx, MEASURES)?;
    let observes = link_pairs(&rx, OBSERVES)?;
    check_endpoints("HAS_INPUT", &has_input, &jobs, &inputs);
    check_endpoints("CHILD_OF", &child_of, &jobs, &jobs);
    check_endpoints(
        "ASSIGNED_TO",
        &link_pairs(&rx, ASSIGNED_TO)?,
        &jobs,
        &executors,
    );
    check_endpoints("PREVIOUS", &link_pairs(&rx, PREVIOUS)?, &jobs, &jobs);
    check_endpoints("REPLACES", &link_pairs(&rx, REPLACES)?, &jobs, &jobs);
    check_endpoints("RETRY_OF", &retry_of, &jobs, &jobs);
    check_endpoints("IN_PROJECT", &in_project, &jobs, &projects);
    check_endpoints("USES_PROFILE", &uses_profile, &agents, &agent_profiles);
    check_endpoints("USES_PROVIDER", &uses_provider, &llm_profiles, &providers);
    check_endpoints("MEASURES", &measures, &metrics, &jobs);
    check_endpoints("OBSERVES", &observes, &availability_stats, &jobs);
    let targets_llm = set_pairs(&rx, TARGETS_LLM)?;
    let applies_profile = link_pairs(&rx, APPLIES_PROFILE)?;
    let runs_llm_profile = link_pairs(&rx, RUNS_LLM_PROFILE)?;
    check_endpoints("TARGETS_LLM", &targets_llm, &agent_profiles, &llm_profiles);
    check_endpoints("APPLIES_PROFILE", &applies_profile, &jobs, &agent_profiles);
    check_endpoints("RUNS_LLM_PROFILE", &runs_llm_profile, &jobs, &llm_profiles);
    check_endpoints("HAS_ATTACHMENT", &has_attachment, &inputs, &attachments);

    let mut executor_rows = BTreeMap::new();
    let mut expected_transports = BTreeSet::new();
    for entry in rx.open_table(EXECUTORS)?.iter()? {
        let (id, bytes) = entry?;
        let row = ExecutorRow::decode(bytes.value())?;
        if !row.llm_transports.is_empty() && row.executor_type != ExecutorType::Llm {
            problems.push(format!(
                "executor {} is not an Llm Executor but names LLM transports",
                id.value()
            ));
        }
        expected_transports.extend(
            row.llm_transports
                .iter()
                .map(|transport| (transport.code(), id.value())),
        );
        executor_rows.insert(id.value(), row);
    }
    let mut indexed_transports = BTreeSet::new();
    for entry in rx.open_table(TRANSPORT_EXECUTORS)?.iter()? {
        indexed_transports.insert(entry?.0.value());
    }
    if indexed_transports != expected_transports {
        problems.push("the LLM transport index and the Executor records disagree".into());
    }

    let ordinary_children: BTreeSet<(u64, u64)> = child_of
        .iter()
        .copied()
        .filter(|(child, parent)| child != parent)
        .collect();
    for (name, forward, incoming) in [
        ("HAS_INPUT", &has_input, set_pairs(&rx, INPUT_JOBS)?),
        ("CHILD_OF", &ordinary_children, set_pairs(&rx, CHILDREN)?),
        ("RETRY_OF", &retry_of, set_pairs(&rx, RETRIES)?),
        ("IN_PROJECT", &in_project, set_pairs(&rx, PROJECT_JOBS)?),
        (
            "USES_PROFILE",
            &uses_profile,
            set_pairs(&rx, PROFILE_AGENTS)?,
        ),
        (
            "USES_PROVIDER",
            &uses_provider,
            set_pairs(&rx, PROVIDER_PROFILES)?,
        ),
        ("MEASURES", &measures, set_pairs(&rx, JOB_METRICS)?),
        ("OBSERVES", &observes, set_pairs(&rx, JOB_AVAILABILITY)?),
        (
            "APPLIES_PROFILE",
            &applies_profile,
            set_pairs(&rx, AGENT_PROFILE_JOBS)?,
        ),
        (
            "RUNS_LLM_PROFILE",
            &runs_llm_profile,
            set_pairs(&rx, LLM_PROFILE_JOBS)?,
        ),
        (
            "HAS_ATTACHMENT",
            &has_attachment,
            set_pairs(&rx, ATTACHMENT_INPUTS)?,
        ),
    ] {
        if reversed(forward) != incoming {
            problems.push(format!("{name} and its incoming index disagree"));
        }
    }

    let roots: Vec<u64> = child_of
        .iter()
        .filter(|(child, parent)| child == parent)
        .map(|(child, _)| *child)
        .collect();
    if roots != [genesis] {
        problems.push(format!(
            "expected only the Genesis Job {genesis} to be its own parent, found {roots:?}"
        ));
    }
    let parents: BTreeMap<u64, u64> = child_of.iter().copied().collect();
    let job_inputs: BTreeSet<u64> = has_input.iter().map(|(job, _)| *job).collect();
    for job in &jobs {
        if !job_inputs.contains(job) {
            problems.push(format!("job {job} has no input"));
        }
        let mut ancestor = *job;
        let mut steps = 0;
        while ancestor != genesis {
            match parents.get(&ancestor) {
                Some(parent) if steps <= jobs.len() => ancestor = *parent,
                _ => {
                    problems.push(format!("job {job} does not reach the Genesis Job"));
                    break;
                }
            }
            steps += 1;
        }
    }
    let agent_profile_of: BTreeMap<u64, u64> = applies_profile.iter().copied().collect();
    let llm_profile_of: BTreeMap<u64, u64> = runs_llm_profile.iter().copied().collect();
    let mut targets = HashMap::new();
    for (name, records, links, index, decode_at) in [
        (
            "metrics",
            METRICS,
            &measures,
            METRICS_BY_SCOPE,
            (|bytes| MetricsRow::decode(bytes).map(|row| row.observed_at))
                as fn(&[u8]) -> DecodeResult<Timestamp>,
        ),
        (
            "availability_stat",
            AVAILABILITY_STATS,
            &observes,
            AVAILABILITY_BY_SCOPE,
            |bytes| AvailabilityStatRow::decode(bytes).map(|row| row.observed_at),
        ),
    ] {
        let job_of: BTreeMap<u64, u64> = links.iter().copied().collect();
        let mut expected = BTreeSet::new();
        for entry in rx.open_table(records)?.iter()? {
            let (id, bytes) = entry?;
            let observed_at = decode_at(bytes.value())?.as_unix_micros();
            let Some(job) = job_of.get(&id.value()) else {
                problems.push(format!("{name} {} is linked to no Job", id.value()));
                continue;
            };
            let target = job_target(&rx, &mut targets, JobId::new(*job))?;
            if target.is_none() && name == "availability_stat" {
                problems.push(format!(
                    "{name} {} observes a Job that ran no LLM",
                    id.value()
                ));
            }
            // Stated here rather than taken from `Scope::of`: a record without
            // an LLM belongs to `All` only.
            let mut scopes = vec![Scope::All];
            if let Some(target) = &target {
                let (provider, model) = (
                    target.frozen.provider.id,
                    target.frozen.config.model.as_str(),
                );
                scopes.extend([
                    Scope::LlmProfile(target.llm_profile),
                    Scope::Provider(provider),
                    Scope::Model(model),
                    Scope::ProviderModel(provider, model),
                ]);
            }
            for scope in scopes {
                expected.insert((scope.key(), observed_at, id.value()));
            }
        }
        let mut indexed = BTreeSet::new();
        for entry in rx.open_table(index)?.iter()? {
            let key = entry?.0;
            let (scope, at, id) = key.value();
            indexed.insert((scope.to_vec(), at, id));
        }
        if indexed != expected {
            problems.push(format!(
                "the {name} scope index disagrees with its records and their Jobs' frozen targets"
            ));
        }
    }
    let assigned: BTreeMap<u64, u64> = link_pairs(&rx, ASSIGNED_TO)?.into_iter().collect();
    let input_of: BTreeMap<u64, u64> = has_input.iter().copied().collect();
    let job_inputs_table = rx.open_table(JOB_INPUTS)?;
    let jobs_table = rx.open_table(JOBS)?;
    for job in &jobs {
        let frozen = match input_of.get(job) {
            Some(input) => job_inputs_table
                .get(*input)?
                .map(|v| JobInputRow::decode(v.value()))
                .transpose()?
                .and_then(|row| row.configuration),
            None => None,
        };
        let row = jobs_table
            .get(*job)?
            .map(|v| JobRow::decode(v.value()))
            .transpose()?;
        if row
            .as_ref()
            .is_some_and(|row| row.stop_confirmed_at.is_some() && row.unknown_at.is_none())
        {
            problems.push(format!(
                "job {job} has a confirmed stop without an unknown outcome"
            ));
        }
        let holds = row.is_some_and(|row| row.holds_profiles());
        match (agent_profile_of.get(job), llm_profile_of.get(job), frozen) {
            (None, None, None) => {}
            (agent_profile, Some(llm_profile), Some(frozen))
                if agent_profile.is_some() == frozen.agent.is_some() =>
            {
                // A holding Job holds both Profiles, so its submission-time
                // target membership still holds; released work keeps only provenance.
                if let Some(agent_profile) = agent_profile
                    && holds
                    && !targets_llm.contains(&(*agent_profile, *llm_profile))
                {
                    problems.push(format!(
                        "holding job {job} runs an LLM Profile its Agent Profile no longer targets"
                    ));
                }
                if let Some(executor) = assigned.get(job)
                    && !executor_rows.get(executor).is_some_and(|row| {
                        row.executor_type == ExecutorType::Llm
                            && row.llm_transports.contains(&frozen.llm.provider.transport)
                    })
                {
                    problems.push(format!("job {job} is assigned an Executor that does not support its frozen transport"));
                }
            }
            _ => problems.push(format!(
                "job {job} has an incomplete configuration or snapshot"
            )),
        }
    }
    let mut indexed_tool_call_ids = BTreeSet::new();
    for entry in rx.open_table(TOOL_CALL_IDS)?.iter()? {
        let (key, value) = entry?;
        let (job, id) = key.value();
        let (request, index) = value.value();
        indexed_tool_call_ids.insert((job, id.to_owned(), request, index));
    }
    let mut expected_tool_call_ids = BTreeSet::new();
    for entry in rx.open_table(TOOL_EVIDENCE)?.iter()? {
        let (key, bytes) = entry?;
        let call = tool_call_ref(key.value())?;
        let record = decode_tool_evidence(call, bytes.value())?;
        let job = jobs_table
            .get(call.job.get())?
            .map(|v| JobRow::decode(v.value()))
            .transpose()?;
        match job {
            None => problems.push(format!("{call} belongs to a missing Job")),
            Some(job) if record.is_unresolved() && job.confirmed_terminal() => {
                problems.push(format!(
                    "{call} is unresolved but its Job has a confirmed outcome"
                ));
            }
            Some(_) => {}
        }
        expected_tool_call_ids.insert((
            call.job.get(),
            record.tool_call_id,
            call.request.get(),
            call.index,
        ));
    }
    if indexed_tool_call_ids != expected_tool_call_ids {
        problems.push("the tool call id index and the tool evidence disagree".into());
    }
    let mut submitted = BTreeSet::new();
    for entry in rx.open_table(SUBMISSIONS)?.iter()? {
        let (key, bytes) = entry?;
        let record = decode_submission(key.value(), bytes.value())?;
        if !jobs.contains(&record.job.get()) {
            problems.push(format!("{:?} names the missing {}", record.key, record.job));
        }
        if !submitted.insert(record.job) {
            problems.push(format!("{} was created by two submissions", record.job));
        }
    }
    for (profile, kind) in &profile_kinds {
        let has_provider = uses_provider.iter().any(|(source, _)| source == profile);
        if (*kind == ProfileKind::Llm) != has_provider {
            problems.push(format!(
                "profile {profile} and its USES_PROVIDER edge disagree"
            ));
        }
    }
    Ok(problems)
}
