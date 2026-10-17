use super::*;

/// The LLM target a stored Job froze, read from only the links and the input
/// that define it: RUNS_LLM_PROFILE, APPLIES_PROFILE for its consistency,
/// HAS_INPUT and the frozen input. `None` for a Job submitted without an
/// LLM configuration. Unlike [`submitted_configuration`], it assembles no
/// [`JobRecord`], so the Job's Projects, parent, assignment and lineage are
/// never read.
pub(super) fn frozen_llm_target(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<LlmTarget>, StorageError> {
    if rx
        .open_table(JOBS)?
        .get(job.get())?
        .map(|v| JobRow::decode(v.value()))
        .transpose()?
        .is_none()
    {
        return Err(StorageError::corrupted(format!(
            "an observation names missing {job}"
        )));
    }
    let Some(llm_profile) = read_link(rx, RUNS_LLM_PROFILE, job.get())? else {
        if read_link(rx, APPLIES_PROFILE, job.get())?.is_some() {
            return Err(StorageError::corrupted(format!(
                "{job} applies an Agent Profile without an LLM Profile"
            )));
        }
        return Ok(None);
    };
    let input = read_link(rx, HAS_INPUT, job.get())?
        .map(JobInputId::new)
        .ok_or_else(|| StorageError::corrupted(format!("{job} has no input")))?;
    let row = rx
        .open_table(JOB_INPUTS)?
        .get(input.get())?
        .map(|v| JobInputRow::decode(v.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{job} names missing {input}")))?;
    let frozen = row.configuration.ok_or_else(|| {
        StorageError::corrupted(format!(
            "the configured {job} has no frozen configuration in {input}"
        ))
    })?;
    Ok(Some(LlmTarget {
        llm_profile: ProfileId::new(llm_profile),
        frozen: frozen.llm,
    }))
}

/// The LLM a Job ran, as its input froze it, cached per Job for one read.
pub(super) fn job_target(
    rx: &ReadTransaction,
    cache: &mut HashMap<JobId, Option<LlmTarget>>,
    job: JobId,
) -> Result<Option<LlmTarget>, StorageError> {
    if let Some(target) = cache.get(&job) {
        return Ok(target.clone());
    }
    let target = frozen_llm_target(rx, job)?;
    cache.insert(job, target.clone());
    Ok(target)
}

/// A Metrics record whose Job and target the caller has read.
pub(super) fn metrics_from(
    rx: &ReadTransaction,
    id: u64,
    job: JobId,
    target: Option<LlmTarget>,
) -> Result<MetricsRecord, StorageError> {
    let bytes = rx
        .open_table(METRICS)?
        .get(id)?
        .ok_or_else(|| StorageError::corrupted(format!("an index names missing metrics {id}")))?;
    let row = MetricsRow::decode(bytes.value())?;
    Ok(MetricsRecord {
        id: row.id,
        job,
        request: row.request,
        target,
        observed_at: row.observed_at,
        measurements: row.measurements,
        token: row.token,
        recorded_at: row.recorded_at,
    })
}

/// An AvailabilityStat record whose Job and target the caller has read.
pub(super) fn availability_from(
    rx: &ReadTransaction,
    id: u64,
    job: JobId,
    target: Option<LlmTarget>,
) -> Result<AvailabilityStatRecord, StorageError> {
    let bytes = rx.open_table(AVAILABILITY_STATS)?.get(id)?.ok_or_else(|| {
        StorageError::corrupted(format!("an index names missing availability_stat {id}"))
    })?;
    let row = AvailabilityStatRow::decode(bytes.value())?;
    let target = target.ok_or_else(|| {
        StorageError::corrupted(format!(
            "availability_stat {id} observes {job}, which ran no LLM"
        ))
    })?;
    Ok(AvailabilityStatRecord {
        id: row.id,
        job,
        request: row.request,
        target,
        observed_at: row.observed_at,
        observation: row.observation,
        recorded_at: row.recorded_at,
    })
}

pub(super) type BuildObservation<T> =
    fn(&ReadTransaction, u64, JobId, Option<LlmTarget>) -> Result<T, StorageError>;

pub(super) fn observation_job(
    rx: &ReadTransaction,
    link: Link,
    id: u64,
) -> Result<JobId, StorageError> {
    read_link(rx, link, id)?.map(JobId::new).ok_or_else(|| {
        StorageError::corrupted(format!("observation record {id} is linked to no Job"))
    })
}

/// Records a Job links to through `incoming`, in recording order. The page of
/// linked IDs is read first, so a Job with no observations needs no frozen
/// target: only a page with records resolves the Job's target, and a dangling
/// link there is corrupted.
pub(super) fn job_observations<T>(
    rx: &ReadTransaction,
    incoming: PairSet,
    build: BuildObservation<T>,
    job: JobId,
    after: Option<u64>,
    limit: usize,
) -> Result<Vec<T>, StorageError> {
    let ids = read_pairs(rx, incoming, job.get(), after, limit.min(MAX_PAGE))?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let target = frozen_llm_target(rx, job)?;
    ids.into_iter()
        .map(|id| build(rx, id, job, target.clone()))
        .collect()
}

/// One page of an observation query. It range-reads the filter's scope from
/// `index` within the observation time range, strictly after `after`, in
/// (observed_at, record) order, and checks every entry against the whole
/// filter. It reads at most `scan_limit` entries, matching or not, and
/// returns at most `limit` records, each clamped to at least 1 and `limit`
/// to [`MAX_PAGE`]. The cursor it returns is the last entry read, so a scan
/// that found nothing still advances.
#[allow(clippy::too_many_arguments)]
pub(super) fn scan_observations<T>(
    rx: &ReadTransaction,
    index: ScopeIndex,
    link: Link,
    build: BuildObservation<T>,
    filter: &ObservationFilter,
    after: Option<ObservationCursor<u64>>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservationPage<T, u64>, StorageError> {
    let done = |records| {
        Ok(ObservationPage {
            records,
            next: None,
        })
    };
    let (limit, scan_limit) = (limit.clamp(1, MAX_PAGE), scan_limit.max(1));
    let until = filter.observed.until.as_unix_micros();
    let mut start = (filter.observed.from.as_unix_micros(), 0);
    if let Some(ObservationCursor {
        observed_at,
        record,
    }) = after
    {
        let at = observed_at.as_unix_micros();
        let next = match (record.checked_add(1), at.checked_add(1)) {
            (Some(record), _) => (at, record),
            (None, Some(later)) => (later, 0),
            (None, None) => return done(Vec::new()),
        };
        start = start.max(next);
    }
    if start.0 >= until {
        return done(Vec::new());
    }
    let scope = Scope::for_filter(filter).key();
    let table = rx.open_table(index)?;
    // Record IDs start at 1, so (time, 0) sorts before every entry at that time.
    let mut entries =
        table.range((scope.as_slice(), start.0, start.1)..(scope.as_slice(), until, 0))?;
    let mut cache = HashMap::new();
    let (mut records, mut scanned, mut last) = (Vec::new(), 0, None);
    while records.len() < limit && scanned < scan_limit {
        let Some(entry) = entries.next() else {
            return done(records);
        };
        let key = entry?.0;
        let (_, at, id) = key.value();
        scanned += 1;
        last = Some(ObservationCursor {
            observed_at: Timestamp::from_unix_micros(at),
            record: id,
        });
        let job = observation_job(rx, link, id)?;
        let target = job_target(rx, &mut cache, job)?;
        if filter_matches(filter, target.as_ref()) {
            records.push(build(rx, id, job, target)?);
        }
    }
    let more = entries.next().transpose()?.is_some();
    Ok(ObservationPage {
        records,
        next: if more { last } else { None },
    })
}

/// A Job's Metrics in recording order, strictly after `after` when given.
pub(crate) fn read_job_metrics(
    database: &Database,
    job: JobId,
    after: Option<MetricsId>,
    limit: usize,
) -> Result<Vec<MetricsRecord>, StorageError> {
    job_observations(
        &read(database)?,
        JOB_METRICS,
        metrics_from,
        job,
        after.map(MetricsId::get),
        limit,
    )
}

/// A Job's AvailabilityStats in recording order, strictly after `after` when given.
pub(crate) fn read_job_availability(
    database: &Database,
    job: JobId,
    after: Option<AvailabilityStatId>,
    limit: usize,
) -> Result<Vec<AvailabilityStatRecord>, StorageError> {
    job_observations(
        &read(database)?,
        JOB_AVAILABILITY,
        availability_from,
        job,
        after.map(AvailabilityStatId::get),
        limit,
    )
}

/// The tag of [`Scope::ProviderModel`] keys.
pub(super) const PROVIDER_MODEL_TAG: u8 = 4;

/// The (Provider, model) pair a [`Scope::ProviderModel`] key names.
pub(super) fn observed_model(scope: &[u8]) -> Result<ObservedModel, StorageError> {
    match scope {
        [PROVIDER_MODEL_TAG, rest @ ..] if rest.len() >= 8 => {
            let (provider, model) = rest.split_at(8);
            let provider =
                LlmProviderId::new(u64::from_be_bytes(provider.try_into().expect("8 bytes")));
            let model = String::from_utf8(model.to_vec()).map_err(|_| {
                StorageError::corrupted("a scope key names a model that is not UTF-8")
            })?;
            Ok(ObservedModel { provider, model })
        }
        _ => Err(StorageError::corrupted(
            "expected a Provider and model scope key",
        )),
    }
}

/// The first [`Scope::ProviderModel`] key in `index` strictly after the
/// `after` key, or from `lower` without one, and before `upper`. One seek:
/// the entries of a skipped key are never read.
pub(super) fn next_provider_model(
    table: &impl ReadableTable<(&'static [u8], i64, u64), ()>,
    after: Option<&[u8]>,
    lower: &[u8],
    upper: &[u8],
) -> Result<Option<Vec<u8>>, StorageError> {
    use std::ops::Bound;
    let start = match after {
        Some(after) if after >= upper => return Ok(None),
        Some(after) if after >= lower => Bound::Excluded((after, i64::MAX, u64::MAX)),
        _ => Bound::Included((lower, i64::MIN, 0)),
    };
    let mut entries =
        table.range::<(&[u8], i64, u64)>((start, Bound::Excluded((upper, i64::MIN, 0))))?;
    Ok(match entries.next() {
        Some(entry) => Some(entry?.0.value().0.to_vec()),
        None => None,
    })
}

/// Whether `index` has an entry of `scope` observed within [from, until).
pub(super) fn observed_within(
    table: &impl ReadableTable<(&'static [u8], i64, u64), ()>,
    scope: &[u8],
    from: i64,
    until: i64,
) -> Result<bool, StorageError> {
    // An iteration error is a read failure, not evidence that an entry exists.
    Ok(table
        .range((scope, from, 0)..(scope, until, 0))?
        .next()
        .transpose()?
        .is_some())
}

/// One page of distinct historical (Provider, model) pairs from the
/// Provider-and-model scope entries of both observation indexes, merged in
/// key order, which is Provider ID and then model bytes. Each examined pair
/// costs at most two seeks and two range checks, whatever its record count;
/// at most `scan_limit` pairs are examined and `limit` returned, each
/// clamped to at least 1 and `limit` to [`MAX_PAGE`]. The cursor is the
/// last pair examined, so a page whose pairs all fell outside the time range
/// still advances. One call reads one snapshot.
pub(crate) fn read_observed_models(
    database: &Database,
    filter: &ObservedModelFilter,
    after: Option<&ObservedModel>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservedModelPage, StorageError> {
    let (limit, scan_limit) = (limit.clamp(1, MAX_PAGE), scan_limit.max(1));
    let (from, until) = (
        filter.observed.from.as_unix_micros(),
        filter.observed.until.as_unix_micros(),
    );
    let done = |choices| {
        Ok(ObservedModelPage {
            choices,
            next: None,
        })
    };
    if from >= until {
        return done(Vec::new());
    }
    let (lower, upper) = match filter.provider {
        Some(provider) => {
            let lower = Scope::ProviderModel(provider, "").key();
            let upper = match provider.get().checked_add(1) {
                Some(following) => Scope::ProviderModel(LlmProviderId::new(following), "").key(),
                None => vec![PROVIDER_MODEL_TAG + 1],
            };
            (lower, upper)
        }
        None => (vec![PROVIDER_MODEL_TAG], vec![PROVIDER_MODEL_TAG + 1]),
    };
    let after = after.map(|model| Scope::ProviderModel(model.provider, &model.model).key());
    let rx = read(database)?;
    let tables = [
        rx.open_table(METRICS_BY_SCOPE)?,
        rx.open_table(AVAILABILITY_BY_SCOPE)?,
    ];
    let mut heads = [None, None];
    for (head, table) in heads.iter_mut().zip(&tables) {
        *head = next_provider_model(table, after.as_deref(), &lower, &upper)?;
    }
    let (mut choices, mut examined, mut last) = (Vec::new(), 0, None);
    while choices.len() < limit && examined < scan_limit {
        let Some(key) = heads.iter().flatten().min().cloned() else {
            return done(choices);
        };
        examined += 1;
        let mut present = [false, false];
        for ((head, table), present) in heads.iter_mut().zip(&tables).zip(&mut present) {
            if head.as_deref() == Some(key.as_slice()) {
                *present = observed_within(table, &key, from, until)?;
                *head = next_provider_model(table, Some(&key), &lower, &upper)?;
            }
        }
        let model = observed_model(&key)?;
        if present.contains(&true) {
            choices.push(ObservedModelChoice {
                model: model.clone(),
                has_metrics: present[0],
                has_availability_stats: present[1],
            });
        }
        last = Some(model);
    }
    let more = heads.iter().any(Option::is_some);
    Ok(ObservedModelPage {
        choices,
        next: if more { last } else { None },
    })
}

/// One page of the Metrics `filter` selects, see [`scan_observations`].
pub(crate) fn read_metrics(
    database: &Database,
    filter: &ObservationFilter,
    after: Option<ObservationCursor<MetricsId>>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservationPage<MetricsRecord, MetricsId>, StorageError> {
    let after = after.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: cursor.record.get(),
    });
    let page = scan_observations(
        &read(database)?,
        METRICS_BY_SCOPE,
        MEASURES,
        metrics_from,
        filter,
        after,
        limit,
        scan_limit,
    )?;
    let next = page.next.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: MetricsId::new(cursor.record),
    });
    Ok(ObservationPage {
        records: page.records,
        next,
    })
}

/// One page of the AvailabilityStats `filter` selects, see [`scan_observations`].
pub(crate) fn read_availability_stats(
    database: &Database,
    filter: &ObservationFilter,
    after: Option<ObservationCursor<AvailabilityStatId>>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservationPage<AvailabilityStatRecord, AvailabilityStatId>, StorageError> {
    let after = after.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: cursor.record.get(),
    });
    let page = scan_observations(
        &read(database)?,
        AVAILABILITY_BY_SCOPE,
        OBSERVES,
        availability_from,
        filter,
        after,
        limit,
        scan_limit,
    )?;
    let next = page.next.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: AvailabilityStatId::new(cursor.record),
    });
    Ok(ObservationPage {
        records: page.records,
        next,
    })
}
