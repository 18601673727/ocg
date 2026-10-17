use super::*;

// ---- open ----------------------------------------------------------------------

pub(crate) enum OpenError {
    Storage(StorageError),
    /// The file is a redb database that this application did not create.
    ForeignFile,
    /// The file uses a storage format this build does not support.
    UnsupportedFormat {
        found: u32,
    },
}

impl From<StorageError> for OpenError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

pub(crate) fn database_file(directory: &Path) -> PathBuf {
    directory.join(DATABASE_FILE_NAME)
}

/// An opened database file with this opening's operation epoch and the
/// instance's Genesis Job.
pub(crate) struct OpenedFile {
    pub(crate) database: Database,
    pub(crate) epoch: u64,
    pub(crate) genesis: JobId,
}

/// Opens or creates the database file, checks its storage format, creates
/// the Genesis Job with a new file or verifies the recorded one, and durably
/// allocates this opening's operation epoch, all in one transaction.
pub(crate) fn open(directory: &Path) -> Result<OpenedFile, OpenError> {
    let path = database_file(directory);
    prepare_private_file(&path)?;
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    // A writable opening lets redb rewrite its own bookkeeping in the file,
    // so an existing file is first checked through a read-only opening and a
    // file of another format is refused without one. A file that cannot be
    // opened read-only, such as one that needs repair, is checked again by
    // `initialize` before anything is written to it.
    if path.metadata().is_ok_and(|metadata| metadata.len() > 0)
        && let Ok(existing) = builder.open_read_only(&path)
    {
        check_existing_format(&existing)?;
    }
    let database = builder.create(&path).map_err(StorageError::from)?;
    let (epoch, genesis) = initialize(&database)?;
    Ok(OpenedFile {
        database,
        epoch,
        genesis,
    })
}

/// Creates the database file empty and private to its owner (0600 on Unix)
/// when it is missing, so redb initializes a file nobody else can read, and
/// makes an existing regular file private. A symbolic link is not followed.
pub(super) fn prepare_private_file(path: &Path) -> Result<(), StorageError> {
    let io_error = |error: std::io::Error| StorageError {
        kind: StorageErrorKind::Io(error.kind()),
        os_error: error.raw_os_error(),
        detail: format!("cannot make {} private: {error}", path.display()),
    };
    match crate::data_directory::private_file(&mut std::fs::OpenOptions::new())
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            crate::data_directory::restrict_file(path).map_err(io_error)
        }
        Err(error) => Err(io_error(error)),
    }
}

/// How a file's recorded format compares with this build's.
pub(super) enum FormatVerdict {
    /// A file with no tables, which this opening will initialize.
    New,
    Supported,
}

pub(super) fn format_verdict(
    format: Option<Vec<u8>>,
    version: Option<Vec<u8>>,
    existing_tables: usize,
) -> Result<FormatVerdict, OpenError> {
    match (format, version) {
        (None, None) if existing_tables == 0 => Ok(FormatVerdict::New),
        (Some(format), Some(version)) if format == FORMAT_NAME => {
            let found = <[u8; 4]>::try_from(version.as_slice())
                .map(u32::from_le_bytes)
                .unwrap_or(u32::MAX);
            match found {
                FORMAT_VERSION => Ok(FormatVerdict::Supported),
                _ => Err(OpenError::UnsupportedFormat { found }),
            }
        }
        _ => Err(OpenError::ForeignFile),
    }
}

pub(super) fn check_existing_format(database: &redb::ReadOnlyDatabase) -> Result<(), OpenError> {
    let rx = database.begin_read().map_err(StorageError::from)?;
    let existing_tables = rx.list_tables().map_err(StorageError::from)?.count();
    let (format, version) = match rx.open_table(META) {
        Ok(meta) => (
            meta.get("format")
                .map_err(StorageError::from)?
                .map(|v| v.value().to_vec()),
            meta.get("format_version")
                .map_err(StorageError::from)?
                .map(|v| v.value().to_vec()),
        ),
        Err(redb::TableError::TableDoesNotExist(_)) => (None, None),
        Err(error) => return Err(StorageError::from(error).into()),
    };
    format_verdict(format, version, existing_tables).map(|_| ())
}

/// Refuses a foreign or other-format file before writing anything to it.
pub(super) fn initialize(database: &Database) -> Result<(u64, JobId), OpenError> {
    let mut tx = database.begin_write().map_err(StorageError::from)?;
    tx.set_durability(Durability::Immediate)
        .map_err(StorageError::from)?;
    let existing_tables = tx.list_tables().map_err(StorageError::from)?.count();
    let opened = {
        let mut meta = tx.open_table(META).map_err(StorageError::from)?;
        let format = meta
            .get("format")
            .map_err(StorageError::from)?
            .map(|v| v.value().to_vec());
        let version = meta
            .get("format_version")
            .map_err(StorageError::from)?
            .map(|v| v.value().to_vec());
        let verdict = match format_verdict(format, version, existing_tables) {
            Ok(verdict) => verdict,
            Err(refusal) => {
                drop(meta);
                tx.abort().map_err(StorageError::from)?;
                return Err(refusal);
            }
        };
        let created = matches!(verdict, FormatVerdict::New);
        if created {
            meta.insert("format", FORMAT_NAME)
                .map_err(StorageError::from)?;
            meta.insert("format_version", FORMAT_VERSION.to_le_bytes().as_slice())
                .map_err(StorageError::from)?;
        }
        drop(meta);
        create_tables(&tx)?;
        let genesis = if created {
            create_genesis(&tx)?
        } else {
            verify_genesis(&tx)?
        };
        // A new file holds no Job below a confirmed outcome. An existing one
        // is read again at every opening, because the version it carries does
        // not say whether an earlier build wrote it or changed it since.
        let certified = created || certify_subtree_closure(&tx)?;
        record_closure(&tx, certified)?;
        // Rebuilt even in a new file, which has no table yet, and whatever the
        // table held: only this build maintains it.
        rebuild_pending_jobs(&tx, genesis)?;
        let mut sequences = tx.open_table(SEQUENCES).map_err(StorageError::from)?;
        let epoch = sequences
            .get(OPEN_EPOCH)
            .map_err(StorageError::from)?
            .map_or(0, |v| v.value())
            + 1;
        sequences
            .insert(OPEN_EPOCH, epoch)
            .map_err(StorageError::from)?;
        (epoch, genesis)
    };
    tx.commit().map_err(StorageError::from)?;
    Ok(opened)
}

/// Replaces [`PENDING_JOBS`] with the Jobs that [`JobRow::awaits_start`]
/// says wait, read from the Job table in the open transaction. It reads
/// every Job once, in time linear in the Jobs, and writes only the entries
/// of the Jobs that wait. A failure leaves the transaction to be abandoned
/// with the table as it was.
pub(super) fn rebuild_pending_jobs(
    tx: &WriteTransaction,
    genesis: JobId,
) -> Result<(), StorageError> {
    tx.delete_table(PENDING_JOBS)?;
    let mut pending = tx.open_table(PENDING_JOBS)?;
    let jobs = tx.open_table(JOBS)?;
    for entry in jobs.iter()? {
        let (id, bytes) = entry?;
        let row = JobRow::decode(bytes.value())?;
        if row.id.get() != id.value() {
            return Err(StorageError::corrupted(format!(
                "the Job record {} holds {}",
                id.value(),
                row.id
            )));
        }
        if row.id != genesis && row.awaits_start() {
            pending.insert(row.pending_key(), ())?;
        }
    }
    Ok(())
}

pub(super) fn create_tables(tx: &WriteTransaction) -> Result<(), StorageError> {
    for table in [
        JOBS,
        JOB_INPUTS,
        JOB_OUTPUTS,
        EXECUTORS,
        EXECUTION_POOLS,
        PROJECTS,
        PROFILES,
        AGENTS,
        LLM_PROVIDERS,
        METRICS,
        AVAILABILITY_STATS,
        ATTACHMENTS,
    ] {
        tx.open_table(table)?;
    }
    for table in [
        HAS_INPUT,
        LATEST_OUTPUT,
        CHILD_OF,
        ASSIGNED_TO,
        PREVIOUS,
        REPLACES,
        RETRY_OF,
        USES_PROVIDER,
        APPLIES_PROFILE,
        RUNS_LLM_PROFILE,
        MEASURES,
        OBSERVES,
        LOCAL_EXECUTIONS,
    ] {
        tx.open_table(table)?;
    }
    for table in [
        INPUT_JOBS,
        CHILDREN,
        RETRIES,
        IN_PROJECT,
        PROJECT_JOBS,
        USES_PROFILE,
        PROFILE_AGENTS,
        PROVIDER_PROFILES,
        TARGETS_LLM,
        AGENT_PROFILE_JOBS,
        LLM_PROFILE_JOBS,
        POOL_PROFILE_JOBS,
        JOB_METRICS,
        JOB_AVAILABILITY,
        ATTACHMENT_INPUTS,
    ] {
        tx.open_table(table)?;
    }
    for table in [METRICS_BY_SCOPE, AVAILABILITY_BY_SCOPE] {
        tx.open_table(table)?;
    }
    tx.open_table(AVAILABILITY_SAMPLES)?;
    tx.open_table(AVAILABILITY_MINUTES)?;
    tx.open_table(AGGREGATION_PROGRESS)?;
    tx.open_table(TOKEN_REQUESTS)?;
    tx.open_table(TOKEN_MINUTES)?;
    tx.open_table(TOOL_EVIDENCE)?;
    tx.open_table(TOOL_CALL_IDS)?;
    tx.open_table(SUBMISSIONS)?;
    tx.open_table(TRANSPORT_EXECUTORS)?;
    tx.open_table(HAS_OUTPUT)?;
    tx.open_table(HAS_ATTACHMENT)?;
    tx.open_table(DEPENDS_ON)?;
    tx.open_table(OPERATIONS)?;
    tx.open_table(SEQUENCES)?;
    Ok(())
}

pub(super) fn allocate(tx: &WriteTransaction, sequence: Sequence) -> Result<u64, StorageError> {
    let mut table = tx.open_table(SEQUENCES)?;
    let next = table.get(sequence.key())?.map_or(0, |v| v.value()) + 1;
    table.insert(sequence.key(), next)?;
    Ok(next)
}

/// Creates the Genesis Job with its frozen input and its self-parent root
/// marker, and records its identity. Runs only for a new file.
pub(super) fn create_genesis(tx: &WriteTransaction) -> Result<JobId, StorageError> {
    let now = Timestamp::now();
    let input = JobInputId::new(allocate(tx, Sequence::JobInput)?);
    let genesis = JobId::new(allocate(tx, Sequence::Job)?);
    let input_row = JobInputRow {
        id: input,
        submission_id: None,
        objective: None,
        max_depth: None,
        max_children_per_job: None,
        max_total_descendants: None,
        submitted_at: Some(now),
        created_at: now,
        updated_at: now,
        configuration: None,
    };
    let job_row = JobRow {
        id: genesis,
        name: Some("Genesis".into()),
        execution_mode: ExecutionMode::Continuous,
        eligible_at: None,
        started_at: None,
        paused_at: None,
        sterilized_at: None,
        completed_at: None,
        failed_at: None,
        cancelled_at: None,
        unknown_at: None,
        stop_confirmed_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        scheduled_at: None,
        created_at: now,
        updated_at: now,
    };
    tx.open_table(JOB_INPUTS)?
        .insert(input.get(), input_row.encode().as_slice())?;
    tx.open_table(JOBS)?
        .insert(genesis.get(), job_row.encode().as_slice())?;
    tx.open_table(HAS_INPUT)?
        .insert(genesis.get(), input.get())?;
    tx.open_table(INPUT_JOBS)?
        .insert((input.get(), genesis.get()), ())?;
    tx.open_table(CHILD_OF)?
        .insert(genesis.get(), genesis.get())?;
    tx.open_table(META)?
        .insert(GENESIS_KEY, genesis.get().to_le_bytes().as_slice())?;
    Ok(genesis)
}

pub(super) fn genesis_id(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<JobId, StorageError> {
    let bytes = meta
        .get(GENESIS_KEY)?
        .ok_or_else(|| StorageError::corrupted("the database records no Genesis Job"))?;
    <[u8; 8]>::try_from(bytes.value())
        .map(|id| JobId::new(u64::from_le_bytes(id)))
        .map_err(|_| StorageError::corrupted("the recorded Genesis Job identity is malformed"))
}

/// The recorded Genesis Job must exist and carry its root marker.
pub(super) fn verify_genesis(tx: &WriteTransaction) -> Result<JobId, StorageError> {
    let genesis = genesis_id(&tx.open_table(META)?)?;
    if tx.open_table(JOBS)?.get(genesis.get())?.is_none() {
        return Err(StorageError::corrupted(format!(
            "the Genesis Job {genesis} is missing"
        )));
    }
    if tx
        .open_table(CHILD_OF)?
        .get(genesis.get())?
        .map(|v| v.value())
        != Some(genesis.get())
    {
        return Err(StorageError::corrupted(format!(
            "the Genesis Job {genesis} lost its root marker"
        )));
    }
    Ok(genesis)
}
