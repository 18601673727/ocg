//! Opt-in WorkNode/Run control substrate. Legacy Mission/replay JSON remains the
//! authority for legacy controller operations until an explicit migration.
//! This repository alone owns the SQLite tables below; no legacy state is
//! mirrored into them. Caches, logs and artifacts are not control truth.

use crate::error::{OcgError, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use ts_rs::TS;
use typed_index_collections::TiVec;

macro_rules! local_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub usize);
        impl From<usize> for $name {
            fn from(value: usize) -> Self {
                Self(value)
            }
        }
        impl From<$name> for usize {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}
local_id!(WorkNodeId);
local_id!(RunId);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionId(String);

impl MissionId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if !crate::orchestration::checkpoint::is_safe_id(&value) {
            return Err(OcgError::config("invalid MissionId"));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkState {
    Ready,
    Running,
    Completed,
    Failed,
    Cancelled,
}
impl WorkState {
    fn text(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    fn parse(s: &str) -> Result<Self> {
        match s {
            "ready" => Ok(Self::Ready),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(invalid("invalid WorkNode state")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Active,
    Completed,
    Failed,
    Cancelled,
    Superseded,
    Fenced,
}
impl RunState {
    fn text(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Superseded => "superseded",
            Self::Fenced => "fenced",
        }
    }
    fn parse(s: &str) -> Result<Self> {
        match s {
            "active" => Ok(Self::Active),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "superseded" => Ok(Self::Superseded),
            "fenced" => Ok(Self::Fenced),
            _ => Err(invalid("invalid Run state")),
        }
    }
}

/// Frozen executor selection. No lifecycle API exposes a mutable contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunContract {
    pub executor: String,
    pub model: String,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkNode {
    pub parent_node_id: Option<WorkNodeId>,
    pub spawned_by_run_id: Option<RunId>,
    pub state: WorkState,
    pub active_run_id: Option<RunId>,
    pub generation: u64,
    pub payload: String,
    pub priority: i64,
    pub not_before: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub node_id: WorkNodeId,
    pub generation: u64,
    contract: RunContract,
    /// External runtime/session identity; never the canonical RunId.
    pub runtime_execution_id: Option<String>,
    /// The concrete host session reported after dispatch, when the runtime
    /// exposes one. It is evidence about the execution, never the Run id.
    pub host_session_id: Option<String>,
    pub state: RunState,
    pub created_at: i64,
    pub finished_at: Option<i64>,
    pub result: Option<String>,
}
impl Run {
    pub fn contract(&self) -> &RunContract {
        &self.contract
    }
}

/// The durable dispatch witness: the only identity that correlates one
/// external execution result back to exactly one Run.
///
/// It is bound at dispatch time, persisted *before* the external execution
/// starts, and validated on every completion. Prompt text, agent role, runtime
/// session identity and ready-queue position are never part of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct DispatchWitness {
    pub mission_id: String,
    pub work_node_id: usize,
    pub run_id: usize,
    pub run_generation: u64,
    pub runtime_execution_id: String,
    /// Identifies this individual invocation. Several dispatches may share a
    /// runtime binding, so it is never derived from one.
    pub dispatch_id: String,
}

impl DispatchWitness {
    /// The compact transport spelling used by the bridge, the generated plugin
    /// envelope and the control surface.
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Parse and structurally validate a witness. Structural validity is not
    /// authority: [`SubstrateRepository::validate_witness`] decides whether the
    /// named Run may still mutate its WorkNode.
    pub fn from_json(value: &Value) -> Result<Self> {
        let text = |key: &str| -> Result<String> {
            value
                .get(key)
                .and_then(Value::as_str)
                .filter(|raw| !raw.is_empty())
                .map(str::to_string)
                .ok_or_else(|| invalid("dispatch witness is missing a field"))
        };
        let index = |key: &str| -> Result<usize> {
            value
                .get(key)
                .and_then(Value::as_u64)
                .and_then(|raw| usize::try_from(raw).ok())
                .ok_or_else(|| invalid("dispatch witness is missing a field"))
        };
        let generation = value
            .get("run_generation")
            .and_then(Value::as_u64)
            .filter(|raw| *raw > 0)
            .ok_or_else(|| invalid("dispatch witness is missing a field"))?;
        let witness = Self {
            mission_id: text("mission_id")?,
            work_node_id: index("work_node_id")?,
            run_id: index("run_id")?,
            run_generation: generation,
            runtime_execution_id: text("runtime_execution_id")?,
            dispatch_id: text("dispatch_id")?,
        };
        MissionId::new(&witness.mission_id)?;
        if !crate::orchestration::checkpoint::is_safe_id(&witness.dispatch_id) {
            return Err(invalid("invalid dispatch_id"));
        }
        Ok(witness)
    }
}

/// What a validated witness is allowed to do to authoritative state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WitnessDisposition {
    /// The witness names the active Run of its WorkNode at its own
    /// generation. It may complete the node exactly once.
    Authoritative,
    /// The Run was replaced/fenced/superseded. The result is retained as
    /// evidence and can never change WorkNode or Mission state.
    LateEvidence,
    /// The exact witness already applied its result. A duplicate delivery is
    /// acknowledged and ignored.
    AlreadyApplied,
    /// The named Run is already terminal and this witness is a different,
    /// earlier attempt. No mutation.
    Superseded,
}

impl WitnessDisposition {
    pub fn text(self) -> &'static str {
        match self {
            Self::Authoritative => "authoritative",
            Self::LateEvidence => "late_evidence",
            Self::AlreadyApplied => "already_applied",
            Self::Superseded => "superseded",
        }
    }
    /// Whether this disposition may mutate the WorkNode.
    pub fn is_authoritative(self) -> bool {
        matches!(self, Self::Authoritative)
    }
}

/// The result of applying one witnessed delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchOutcome {
    pub witness: DispatchWitness,
    pub disposition: String,
    /// The WorkNode state after the delivery, or the unchanged prior state for
    /// a non-authoritative delivery.
    pub node_state: String,
    pub run_state: String,
    /// Whether the delivery was retained as durable evidence without authority.
    pub evidence_only: bool,
}

/// One durable verification evidence record. Model output is never evidence;
/// this record is produced by the trusted verification runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationRecord {
    pub verification_id: String,
    pub node_id: usize,
    pub run_id: usize,
    pub dispatch_id: String,
    /// `passed` or `failed`; derived from the trusted report, never asserted.
    pub outcome: String,
    /// Whether a trusted verification command actually ran. `false` means this
    /// is an explicit operator assertion, which can never complete a Run.
    pub trusted: bool,
    pub passed: bool,
    pub commands: Vec<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependency {
    pub node_id: WorkNodeId,
    pub depends_on_node_id: WorkNodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub seq: u64,
    pub kind: String,
    pub payload: String,
    pub by_run_id: Option<RunId>,
}

/// One row of [`SubstrateRepository::list_missions`]: mission id, creation
/// timestamp, lifecycle state and optional completion timestamp.
pub type MissionListRow = (String, i64, String, Option<i64>);

#[derive(Debug)]
pub struct MissionState {
    pub id: MissionId,
    pub work_nodes: TiVec<WorkNodeId, WorkNode>,
    pub runs: TiVec<RunId, Run>,
    pub dependencies: Vec<Dependency>,
    pub events: Vec<Event>,
}

impl MissionState {
    pub fn root(&self) -> WorkNodeId {
        WorkNodeId(0)
    }
    pub fn children(&self, parent: WorkNodeId) -> Vec<WorkNodeId> {
        self.work_nodes
            .iter_enumerated()
            .filter_map(|(id, node)| (node.parent_node_id == Some(parent)).then_some(id))
            .collect()
    }
    /// The durable ready set, not a separate queue. A dependency is satisfied
    /// only by a completed node, regardless of ownership topology.
    pub fn ready(&self, now: i64) -> Vec<WorkNodeId> {
        self.work_nodes
            .iter_enumerated()
            .filter_map(|(id, node)| {
                (node.state == WorkState::Ready
                    && node.active_run_id.is_none()
                    && node.not_before <= now
                    && self
                        .dependencies
                        .iter()
                        .filter(|d| d.node_id == id)
                        .all(|d| {
                            self.work_nodes[d.depends_on_node_id].state == WorkState::Completed
                        }))
                .then_some(id)
            })
            .collect()
    }
    pub fn authoritative(&self, node: WorkNodeId, run: RunId) -> bool {
        self.work_nodes.get(node).is_some_and(|n| {
            n.active_run_id == Some(run)
                && self.runs.get(run).is_some_and(|r| {
                    r.node_id == node && r.generation == n.generation && r.state == RunState::Active
                })
        })
    }
}

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}
fn sql(error: rusqlite::Error) -> OcgError {
    OcgError::config(format!("substrate SQLite: {error}"))
}
fn id(value: i64) -> Result<usize> {
    usize::try_from(value).map_err(|_| invalid("negative or oversized local ID"))
}
fn num(value: usize) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid("local ID exceeds SQLite range"))
}
fn optional_id(value: Option<i64>) -> Result<Option<usize>> {
    value.map(id).transpose()
}
fn json<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|e| invalid(&format!("substrate JSON: {e}")))
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS missions (
 mission_id TEXT PRIMARY KEY, created_at INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','completed','failed','cancelled')),
 completed_at INTEGER
) STRICT;
CREATE TABLE IF NOT EXISTS work_nodes (
 mission_id TEXT NOT NULL REFERENCES missions(mission_id), node_id INTEGER NOT NULL CHECK(node_id >= 0),
 parent_node_id INTEGER, spawned_by_run_id INTEGER, state TEXT NOT NULL,
 active_run_id INTEGER, generation INTEGER NOT NULL CHECK(generation >= 0), payload TEXT NOT NULL,
 priority INTEGER NOT NULL, not_before INTEGER NOT NULL,
 PRIMARY KEY(mission_id,node_id),
 FOREIGN KEY(mission_id,parent_node_id) REFERENCES work_nodes(mission_id,node_id),
 FOREIGN KEY(mission_id,spawned_by_run_id) REFERENCES runs(mission_id,run_id),
 FOREIGN KEY(mission_id,active_run_id) REFERENCES runs(mission_id,run_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE IF NOT EXISTS runs (
 mission_id TEXT NOT NULL, run_id INTEGER NOT NULL CHECK(run_id >= 0), node_id INTEGER NOT NULL,
 generation INTEGER NOT NULL CHECK(generation > 0), contract TEXT NOT NULL, runtime_execution_id TEXT,
 host_session_id TEXT,
 state TEXT NOT NULL, created_at INTEGER NOT NULL, finished_at INTEGER, result TEXT,
 PRIMARY KEY(mission_id,run_id),
 FOREIGN KEY(mission_id,node_id) REFERENCES work_nodes(mission_id,node_id)
) STRICT;
CREATE TABLE IF NOT EXISTS dependencies (
 mission_id TEXT NOT NULL, node_id INTEGER NOT NULL, depends_on_node_id INTEGER NOT NULL,
 CHECK(node_id != depends_on_node_id), PRIMARY KEY(mission_id,node_id,depends_on_node_id),
 FOREIGN KEY(mission_id,node_id) REFERENCES work_nodes(mission_id,node_id),
 FOREIGN KEY(mission_id,depends_on_node_id) REFERENCES work_nodes(mission_id,node_id)
) STRICT;
CREATE TABLE IF NOT EXISTS domain_events (
 mission_id TEXT NOT NULL REFERENCES missions(mission_id), seq INTEGER NOT NULL CHECK(seq > 0),
 kind TEXT NOT NULL, payload TEXT NOT NULL, by_run_id INTEGER,
 PRIMARY KEY(mission_id,seq),
 FOREIGN KEY(mission_id,by_run_id) REFERENCES runs(mission_id,run_id)
) STRICT;
-- One row per dispatch attempt. The witness is committed in the same
-- transaction as the Run it names, before any external execution starts.
CREATE TABLE IF NOT EXISTS dispatches (
 mission_id TEXT NOT NULL, dispatch_id TEXT NOT NULL,
 node_id INTEGER NOT NULL, run_id INTEGER NOT NULL, generation INTEGER NOT NULL CHECK(generation > 0),
 runtime_execution_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','delivered','late')),
 created_at INTEGER NOT NULL, delivered_at INTEGER,
 PRIMARY KEY(mission_id,dispatch_id),
 FOREIGN KEY(mission_id,node_id) REFERENCES work_nodes(mission_id,node_id),
 FOREIGN KEY(mission_id,run_id) REFERENCES runs(mission_id,run_id)
) STRICT;
-- Results delivered for a generation that no longer holds authority. They
-- are inspectable evidence and can never change WorkNode or Mission state.
CREATE TABLE IF NOT EXISTS late_results (
 mission_id TEXT NOT NULL, late_id INTEGER NOT NULL CHECK(late_id > 0),
 node_id INTEGER NOT NULL, run_id INTEGER NOT NULL, dispatch_id TEXT NOT NULL,
 result TEXT NOT NULL, created_at INTEGER NOT NULL,
 PRIMARY KEY(mission_id,late_id)
) STRICT;
-- Trusted verification evidence, separate from every model output.
CREATE TABLE IF NOT EXISTS verifications (
 mission_id TEXT NOT NULL, verification_id TEXT NOT NULL,
 node_id INTEGER NOT NULL, run_id INTEGER NOT NULL, dispatch_id TEXT NOT NULL,
 outcome TEXT NOT NULL CHECK(outcome IN ('passed','failed')),
 report TEXT NOT NULL, created_at INTEGER NOT NULL,
 PRIMARY KEY(mission_id,verification_id),
 FOREIGN KEY(mission_id,node_id) REFERENCES work_nodes(mission_id,node_id),
 FOREIGN KEY(mission_id,run_id) REFERENCES runs(mission_id,run_id)
) STRICT;
-- Pre-run Mission configuration. Mutable only until the first dispatch; a
-- dispatched Run keeps its frozen executor contract forever.
CREATE TABLE IF NOT EXISTS mission_config (
 mission_id TEXT PRIMARY KEY REFERENCES missions(mission_id),
 config TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision > 0), updated_at INTEGER NOT NULL
) STRICT;
-- Machine-enforced repository identity for the whole durable store.
CREATE TABLE IF NOT EXISTS substrate_identity (
 id INTEGER PRIMARY KEY CHECK(id = 1),
 root TEXT NOT NULL, boundary TEXT NOT NULL, created_at INTEGER NOT NULL
) STRICT;
"#;

/// One connection owned at the repository boundary. Reopen in another process
/// to reconstruct; SQLite WAL serializes writers and readers independently.
pub struct SubstrateRepository {
    conn: Connection,
    path: PathBuf,
}
impl SubstrateRepository {
    pub fn open(root: &Path) -> Result<Self> {
        crate::runtime::install::ensure_gitignore(root)?;
        let path = crate::orchestration::state::state_dir(root).join("substrate.sqlite3");
        let marker = path.with_extension("initialized");
        if marker.exists() && !path.is_file() {
            return Err(invalid(
                "canonical substrate database is missing after initialization",
            ));
        }
        std::fs::create_dir_all(path.parent().unwrap())
            .map_err(|e| OcgError::io("create substrate directory", e))?;
        let conn = Connection::open(&path).map_err(sql)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sql)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(sql)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(sql)?;
        conn.execute_batch(SCHEMA).map_err(sql)?;
        // Existing baseline databases predate witness columns. SQLite cannot
        // alter STRICT table definitions through CREATE IF NOT EXISTS, so the
        // migration is explicit and idempotent. No old control decision is
        // inferred from prompt/session data.
        migrate_column(&conn, "missions", "state", "TEXT NOT NULL DEFAULT 'active'")?;
        migrate_column(&conn, "missions", "completed_at", "INTEGER")?;
        migrate_column(&conn, "runs", "host_session_id", "TEXT")?;
        if !marker.exists() {
            std::fs::write(&marker, b"sqlite-worknode-v2-witness\n")
                .map_err(|e| OcgError::write(&marker, e))?;
        }
        // A substrate is bound to one canonical repository root. A database
        // copied into a sibling checkout therefore fails closed instead of
        // silently accepting another project's execution evidence.
        let boundary = crate::project::resolve(root);
        let root_text = crate::project::canonicalize(root)
            .to_string_lossy()
            .to_string();
        let boundary_text = boundary.root().to_string_lossy().to_string();
        let existing: Option<(String, String)> = conn
            .query_row(
                "SELECT root,boundary FROM substrate_identity WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        match existing {
            Some((stored_root, stored_boundary))
                if stored_root != root_text || stored_boundary != boundary_text =>
            {
                return Err(invalid(
                    "canonical substrate database belongs to another repository boundary",
                ));
            }
            None => {
                conn.execute(
                    "INSERT INTO substrate_identity VALUES (1,?1,?2,?3)",
                    params![root_text, boundary_text, now_unix()],
                )
                .map_err(sql)?;
            }
            Some(_) => {}
        }
        Ok(Self { conn, path })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn journal_mode(&self) -> Result<String> {
        self.conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(sql)
    }

    pub fn create_mission(
        &mut self,
        mission: &MissionId,
        payload: &str,
        now: i64,
    ) -> Result<MissionState> {
        let tx = self.conn.transaction().map_err(sql)?;
        tx.execute(
            "INSERT INTO missions (mission_id,created_at) VALUES (?1,?2)",
            params![mission.as_str(), now],
        )
        .map_err(sql)?;
        tx.execute(
            "INSERT INTO work_nodes (mission_id,node_id,parent_node_id,spawned_by_run_id,state,active_run_id,generation,payload,priority,not_before) VALUES (?1,0,NULL,NULL,'ready',NULL,0,?2,0,0)",
            params![mission.as_str(), payload],
        )
        .map_err(sql)?;
        event(&tx, mission, "mission_created", "{}", None)?;
        tx.commit().map_err(sql)?;
        self.load(mission)?
            .ok_or_else(|| invalid("created mission missing"))
    }

    /// Establish a live Mission, its authoritative root Run and the root
    /// dispatch witness in one commit. The witness is durable before the
    /// external execution is allowed to start.
    pub fn create_live_mission(
        &mut self,
        mission: &MissionId,
        payload: &str,
        contract: RunContract,
        runtime_execution_id: &str,
        now: i64,
    ) -> Result<DispatchWitness> {
        if runtime_execution_id.is_empty() {
            return Err(invalid("missing runtime binding"));
        }
        let tx = self.conn.transaction().map_err(sql)?;
        tx.execute(
            "INSERT INTO missions (mission_id,created_at) VALUES (?1,?2)",
            params![mission.as_str(), now],
        )
        .map_err(sql)?;
        tx.execute(
            "INSERT INTO work_nodes (mission_id,node_id,parent_node_id,spawned_by_run_id,state,active_run_id,generation,payload,priority,not_before) VALUES (?1,0,NULL,NULL,'running',0,1,?2,0,0)",
            params![mission.as_str(), payload],
        )
        .map_err(sql)?;
        insert_run(
            &tx,
            mission,
            0,
            WorkNodeId(0),
            1,
            &contract,
            Some(runtime_execution_id),
            now,
        )?;
        let witness =
            record_dispatch(&tx, mission, 0, WorkNodeId(0), 1, runtime_execution_id, now)?;
        event(&tx, mission, "mission_created", "{}", None)?;
        event(
            &tx,
            mission,
            "run_started",
            &json(&serde_json::json!({"node_id":0,"run_id":0,"dispatch_id":witness.dispatch_id}))?,
            Some(RunId(0)),
        )?;
        event(
            &tx,
            mission,
            "run_dispatched",
            &json(&witness.to_json())?,
            Some(RunId(0)),
        )?;
        tx.commit().map_err(sql)?;
        Ok(witness)
    }

    /// Dispatch the root Run of an already-created, undispatched Mission.
    ///
    /// This is the pre-run -> dispatched transition: the root WorkNode becomes
    /// running, its frozen contract and binding are committed, and the dispatch
    /// witness is published in the same transaction.
    pub fn activate_mission(
        &mut self,
        mission: &MissionId,
        contract: RunContract,
        runtime_execution_id: &str,
        now: i64,
    ) -> Result<DispatchWitness> {
        if runtime_execution_id.is_empty() {
            return Err(invalid("missing runtime binding"));
        }
        let tx = self.conn.transaction().map_err(sql)?;
        let run = start_run_transaction(
            &tx,
            mission,
            WorkNodeId(0),
            &contract,
            Some(runtime_execution_id),
            now,
        )?;
        let witness = witness_for(&tx, mission, run)?;
        event(
            &tx,
            mission,
            "run_dispatched",
            &json(&witness.to_json())?,
            Some(run),
        )?;
        tx.commit().map_err(sql)?;
        Ok(witness)
    }

    pub fn load(&mut self, mission: &MissionId) -> Result<Option<MissionState>> {
        let tx = self.conn.transaction().map_err(sql)?;
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM missions WHERE mission_id=?1)",
                [mission.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if !exists {
            return Ok(None);
        }
        let mut state = MissionState {
            id: mission.clone(),
            work_nodes: TiVec::new(),
            runs: TiVec::new(),
            dependencies: Vec::new(),
            events: Vec::new(),
        };
        let mut stmt = tx.prepare("SELECT node_id,parent_node_id,spawned_by_run_id,state,active_run_id,generation,payload,priority,not_before FROM work_nodes WHERE mission_id=?1 ORDER BY node_id").map_err(sql)?;
        let rows = stmt
            .query_map([mission.as_str()], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, i64>(8)?,
                ))
            })
            .map_err(sql)?;
        for row in rows {
            let (key, parent, spawned, status, active, generation, payload, priority, not_before) =
                row.map_err(sql)?;
            if id(key)? != state.work_nodes.len() {
                return Err(invalid("non-dense WorkNode IDs"));
            }
            state.work_nodes.push(WorkNode {
                parent_node_id: optional_id(parent)?.map(WorkNodeId),
                spawned_by_run_id: optional_id(spawned)?.map(RunId),
                state: WorkState::parse(&status)?,
                active_run_id: optional_id(active)?.map(RunId),
                generation: u64::try_from(generation).map_err(|_| invalid("invalid generation"))?,
                payload,
                priority,
                not_before,
            });
        }
        if state.work_nodes.is_empty() || state.work_nodes[WorkNodeId(0)].parent_node_id.is_some() {
            return Err(invalid("mission must have one root WorkNode"));
        }
        let mut stmt = tx.prepare("SELECT run_id,node_id,generation,contract,runtime_execution_id,host_session_id,state,created_at,finished_at,result FROM runs WHERE mission_id=?1 ORDER BY run_id").map_err(sql)?;
        let rows = stmt
            .query_map([mission.as_str()], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, Option<i64>>(8)?,
                    r.get::<_, Option<String>>(9)?,
                ))
            })
            .map_err(sql)?;
        for row in rows {
            let (
                key,
                node,
                generation,
                contract,
                runtime_execution_id,
                host_session_id,
                status,
                created_at,
                finished_at,
                result,
            ) = row.map_err(sql)?;
            if id(key)? != state.runs.len() {
                return Err(invalid("non-dense Run IDs"));
            }
            state.runs.push(Run {
                node_id: WorkNodeId(id(node)?),
                generation: u64::try_from(generation).map_err(|_| invalid("invalid generation"))?,
                contract: serde_json::from_str(&contract)
                    .map_err(|e| invalid(&format!("invalid RunContract: {e}")))?,
                runtime_execution_id,
                host_session_id,
                state: RunState::parse(&status)?,
                created_at,
                finished_at,
                result,
            });
        }
        for (key, node) in state.work_nodes.iter_enumerated() {
            if key == WorkNodeId(0) && node.spawned_by_run_id.is_some()
                || key != WorkNodeId(0) && node.parent_node_id.is_none()
            {
                return Err(invalid("invalid ownership root"));
            }
            if node
                .parent_node_id
                .is_some_and(|parent| state.work_nodes.get(parent).is_none())
            {
                return Err(invalid("invalid ownership parent"));
            }
            if node.parent_node_id.is_some_and(|p| p.0 >= key.0) {
                return Err(invalid("ownership must precede child"));
            }
            if let Some(spawned) = node.spawned_by_run_id {
                let parent = node
                    .parent_node_id
                    .ok_or_else(|| invalid("missing parent"))?;
                if !state
                    .runs
                    .get(spawned)
                    .is_some_and(|run| run.node_id == parent)
                {
                    return Err(invalid("invalid spawn provenance"));
                }
            }
            if let Some(run) = node.active_run_id {
                if !state.authoritative(key, run) {
                    return Err(invalid("invalid active Run witness"));
                }
            }
        }
        let witnessed: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM dispatches WHERE mission_id=?1",
                [mission.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if witnessed != state.runs.len() as i64 {
            return Err(invalid("every Run must own exactly one dispatch witness"));
        }
        for (key, run) in state.runs.iter_enumerated() {
            let node = state
                .work_nodes
                .get(run.node_id)
                .ok_or_else(|| invalid("invalid Run owner"))?;
            let binding: String = tx
                .query_row(
                    "SELECT runtime_execution_id FROM dispatches WHERE mission_id=?1 AND run_id=?2",
                    params![mission.as_str(), num(key.0)?],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            if Some(&binding) != run.runtime_execution_id.as_ref() {
                return Err(invalid("Run binding does not match its dispatch witness"));
            }
            if run.generation == 0
                || run.generation > node.generation
                || (run.state == RunState::Active && node.active_run_id != Some(key))
            {
                return Err(invalid("invalid Run generation or authority"));
            }
        }
        let mut stmt = tx.prepare("SELECT node_id,depends_on_node_id FROM dependencies WHERE mission_id=?1 ORDER BY node_id,depends_on_node_id").map_err(sql)?;
        let rows = stmt
            .query_map([mission.as_str()], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(sql)?;
        for row in rows {
            let (a, b) = row.map_err(sql)?;
            if state.work_nodes.get(WorkNodeId(id(a)?)).is_none()
                || state.work_nodes.get(WorkNodeId(id(b)?)).is_none()
            {
                return Err(invalid("invalid dependency endpoint"));
            }
            state.dependencies.push(Dependency {
                node_id: WorkNodeId(id(a)?),
                depends_on_node_id: WorkNodeId(id(b)?),
            });
        }
        let mut stmt = tx.prepare("SELECT seq,kind,payload,by_run_id FROM domain_events WHERE mission_id=?1 ORDER BY seq").map_err(sql)?;
        let rows = stmt
            .query_map([mission.as_str()], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                ))
            })
            .map_err(sql)?;
        for row in rows {
            let (seq, kind, payload, by_run) = row.map_err(sql)?;
            if id(seq)? != state.events.len() + 1 {
                return Err(invalid("non-contiguous event sequence"));
            }
            state.events.push(Event {
                seq: seq as u64,
                kind,
                payload,
                by_run_id: optional_id(by_run)?.map(RunId),
            });
        }
        Ok(Some(state))
    }

    /// Stale/fenced runs fail closed before any authoritative mutation.
    pub fn create_child(
        &mut self,
        mission: &MissionId,
        parent: WorkNodeId,
        by: RunId,
        payload: &str,
        now: i64,
    ) -> Result<WorkNodeId> {
        self.create_child_work(mission, parent, by, payload, &[], now)
    }

    /// One atomic CreateChildWork: ownership, provenance, and dependency DAG.
    pub fn create_child_work(
        &mut self,
        mission: &MissionId,
        parent: WorkNodeId,
        by: RunId,
        payload: &str,
        dependencies: &[WorkNodeId],
        now: i64,
    ) -> Result<WorkNodeId> {
        let tx = self.conn.transaction().map_err(sql)?;
        require_authority(&tx, mission, parent, by)?;
        let next = next_id(&tx, "work_nodes", "node_id", mission)?;
        let mut unique = std::collections::BTreeSet::new();
        for dep in dependencies {
            if dep.0 >= next || !unique.insert(dep.0) {
                return Err(invalid("invalid or repeated Dependency"));
            }
            node_witness(&tx, mission, *dep)?;
        }
        tx.execute(
            "INSERT INTO work_nodes (mission_id,node_id,parent_node_id,spawned_by_run_id,state,active_run_id,generation,payload,priority,not_before) VALUES (?1,?2,?3,?4,'ready',NULL,0,?5,0,?6)",
            params![
                mission.as_str(),
                num(next)?,
                num(parent.0)?,
                num(by.0)?,
                payload,
                now
            ],
        )
        .map_err(sql)?;
        for dep in unique {
            tx.execute(
                "INSERT INTO dependencies VALUES (?1,?2,?3)",
                params![mission.as_str(), num(next)?, num(dep)?],
            )
            .map_err(sql)?;
        }
        event(
            &tx,
            mission,
            "child_created",
            &json(
                &serde_json::json!({"node_id":next,"parent_node_id":parent.0,"dependencies":dependencies.iter().map(|id| id.0).collect::<Vec<_>>()}),
            )?,
            Some(by),
        )?;
        if dependencies_complete(&tx, mission, WorkNodeId(next))? {
            event(
                &tx,
                mission,
                "worknode_ready",
                &json(&serde_json::json!({"node_id":next}))?,
                Some(by),
            )?;
        }
        tx.commit().map_err(sql)?;
        Ok(WorkNodeId(next))
    }

    pub fn add_dependency(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        prerequisite: WorkNodeId,
    ) -> Result<()> {
        let tx = self.conn.transaction().map_err(sql)?;
        // Dependency edits belong to planning, before either node is dispatched.
        let state = load_states_for_dependency(&tx, mission, node, prerequisite)?;
        if node == prerequisite || state.iter().any(|s| *s != "ready") {
            return Err(invalid("dependency requires distinct ready nodes"));
        }
        let cycle: bool = tx.query_row("WITH RECURSIVE ancestors(id) AS (SELECT depends_on_node_id FROM dependencies WHERE mission_id=?1 AND node_id=?2 UNION SELECT d.depends_on_node_id FROM dependencies d JOIN ancestors a ON d.node_id=a.id WHERE d.mission_id=?1) SELECT EXISTS(SELECT 1 FROM ancestors WHERE id=?3)", params![mission.as_str(),num(prerequisite.0)?,num(node.0)?], |r| r.get(0)).map_err(sql)?;
        if cycle {
            return Err(invalid("dependency cycle"));
        }
        tx.execute(
            "INSERT INTO dependencies VALUES (?1,?2,?3)",
            params![mission.as_str(), num(node.0)?, num(prerequisite.0)?],
        )
        .map_err(sql)?;
        event(
            &tx,
            mission,
            "dependency_added",
            &json(&serde_json::json!({"node_id":node.0,"depends_on_node_id":prerequisite.0}))?,
            None,
        )?;
        tx.commit().map_err(sql)
    }

    /// Plan a Run for a ready WorkNode without an external binding. The
    /// committed witness still exists (with an OCG-issued planned binding), so
    /// the Run is never uncorrelatable; only the external host session is
    /// unknown.
    pub fn start_run(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        contract: RunContract,
        now: i64,
    ) -> Result<RunId> {
        self.start_run_with_binding(mission, node, contract, None, now)
            .map(|witness| RunId(witness.run_id))
    }

    fn start_run_with_binding(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        contract: RunContract,
        binding: Option<&str>,
        now: i64,
    ) -> Result<DispatchWitness> {
        let tx = self.conn.transaction().map_err(sql)?;
        let run = start_run_transaction(&tx, mission, node, &contract, binding, now)?;
        let witness = witness_for(&tx, mission, run)?;
        tx.commit().map_err(sql)?;
        Ok(witness)
    }

    /// Start a ready WorkNode with an explicit external runtime binding and
    /// return the durable dispatch witness for exactly this invocation.
    pub fn dispatch_run(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        by: RunId,
        contract: RunContract,
        binding: &str,
        now: i64,
    ) -> Result<DispatchWitness> {
        if binding.is_empty() {
            return Err(invalid("missing runtime binding"));
        }
        let tx = self.conn.transaction().map_err(sql)?;
        let parent: Option<i64> = tx
            .query_row(
                "SELECT parent_node_id FROM work_nodes WHERE mission_id=?1 AND node_id=?2",
                params![mission.as_str(), num(node.0)?],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("unknown WorkNode"))?;
        let parent = parent.ok_or_else(|| invalid("root Run already established"))?;
        require_authority(&tx, mission, WorkNodeId(id(parent)?), by)?;
        let new_run = start_run_transaction(&tx, mission, node, &contract, Some(binding), now)?;
        let witness = witness_for(&tx, mission, new_run)?;
        event(
            &tx,
            mission,
            "run_dispatched",
            &json(&witness.to_json())?,
            Some(new_run),
        )?;
        tx.commit().map_err(sql)?;
        Ok(witness)
    }

    /// Shared root/child replacement primitive; commit before updating any
    /// caller's TiVec projection. Reload after commit to reconcile memory.
    pub fn replace_run(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        old: RunId,
        contract: RunContract,
        now: i64,
    ) -> Result<RunId> {
        self.replace_run_with_binding(mission, node, old, contract, None, now)
            .map(|witness| RunId(witness.run_id))
    }

    fn replace_run_with_binding(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        old: RunId,
        contract: RunContract,
        binding: Option<&str>,
        now: i64,
    ) -> Result<DispatchWitness> {
        let tx = self.conn.transaction().map_err(sql)?;
        let (state, generation, active, _) = node_witness(&tx, mission, node)?;
        if active != Some(old.0) && !(state == "failed" && active.is_none()) {
            return Err(invalid("stale Run cannot replace current authority"));
        }
        if active.is_some() {
            require_authority(&tx, mission, node, old)?;
        } else {
            let failed: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM runs WHERE mission_id=?1 AND run_id=?2 AND node_id=?3 AND generation=?4 AND state='failed')",params![mission.as_str(),num(old.0)?,num(node.0)?,generation],|r|r.get(0)).map_err(sql)?;
            if !failed {
                return Err(invalid("stale Run cannot replace current authority"));
            }
        }
        let next = next_id(&tx, "runs", "run_id", mission)?;
        let new_generation = generation
            .checked_add(1)
            .ok_or_else(|| invalid("generation overflow"))?;
        tx.execute("UPDATE runs SET state='fenced',finished_at=?4 WHERE mission_id=?1 AND run_id=?2 AND state IN ('active','failed') AND generation=?3",params![mission.as_str(),num(old.0)?,generation,now]).map_err(sql)?;
        let owned;
        let binding = match binding {
            Some(binding) if !binding.is_empty() => binding,
            _ => {
                owned = planned_binding(mission, next, new_generation);
                owned.as_str()
            }
        };
        insert_run(
            &tx,
            mission,
            next,
            node,
            new_generation,
            &contract,
            Some(binding),
            now,
        )?;
        record_dispatch(&tx, mission, next, node, new_generation, binding, now)?;
        tx.execute("UPDATE work_nodes SET state='running',generation=?3,active_run_id=?4 WHERE mission_id=?1 AND node_id=?2",params![mission.as_str(),num(node.0)?,new_generation,num(next)?]).map_err(sql)?;
        event(
            &tx,
            mission,
            "run_fenced",
            &json(&serde_json::json!({"node_id":node.0,"run_id":old.0}))?,
            Some(old),
        )?;
        let witness = witness_for(&tx, mission, RunId(next))?;
        event(
            &tx,
            mission,
            "run_replaced",
            &json(
                &serde_json::json!({"node_id":node.0,"old_run_id":old.0,"new_run_id":next,"dispatch_id":witness.dispatch_id}),
            )?,
            Some(RunId(next)),
        )?;
        event(
            &tx,
            mission,
            "run_dispatched",
            &json(&witness.to_json())?,
            Some(RunId(next)),
        )?;
        tx.commit().map_err(sql)?;
        Ok(witness)
    }

    /// Shared root/worker replacement with a new external binding. The old
    /// generation is fenced in the same commit that publishes the new witness,
    /// so a late delivery from the previous generation can never regain
    /// authority.
    pub fn replace_bound_run(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        old: RunId,
        contract: RunContract,
        binding: &str,
        now: i64,
    ) -> Result<DispatchWitness> {
        if binding.is_empty() {
            return Err(invalid("missing runtime binding"));
        }
        self.replace_run_with_binding(mission, node, old, contract, Some(binding), now)
    }

    pub fn finish_run(
        &mut self,
        mission: &MissionId,
        node: WorkNodeId,
        run: RunId,
        outcome: RunState,
        result: Option<&str>,
        now: i64,
    ) -> Result<()> {
        if matches!(outcome, RunState::Active | RunState::Fenced) {
            return Err(invalid("invalid terminal outcome"));
        }
        let tx = self.conn.transaction().map_err(sql)?;
        require_authority(&tx, mission, node, run)?;
        tx.execute(
            "UPDATE runs SET state=?3,finished_at=?4,result=?5 WHERE mission_id=?1 AND run_id=?2",
            params![mission.as_str(), num(run.0)?, outcome.text(), now, result],
        )
        .map_err(sql)?;
        let work = match outcome {
            RunState::Completed => WorkState::Completed,
            RunState::Cancelled => WorkState::Cancelled,
            _ => WorkState::Failed,
        };
        tx.execute(
            "UPDATE work_nodes SET state=?3,active_run_id=NULL WHERE mission_id=?1 AND node_id=?2",
            params![mission.as_str(), num(node.0)?, work.text()],
        )
        .map_err(sql)?;
        event(
            &tx,
            mission,
            match outcome {
                RunState::Completed => "run_completed",
                RunState::Failed => "run_failed",
                _ => "run_finished",
            },
            &json(&serde_json::json!({"node_id":node.0,"run_id":run.0,"state":outcome.text()}))?,
            Some(run),
        )?;
        if work == WorkState::Completed {
            let mut stmt = tx.prepare("SELECT node_id FROM dependencies WHERE mission_id=?1 AND depends_on_node_id=?2 ORDER BY node_id").map_err(sql)?;
            let nodes = stmt
                .query_map(params![mission.as_str(), num(node.0)?], |r| {
                    r.get::<_, i64>(0)
                })
                .map_err(sql)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(sql)?;
            drop(stmt);
            for dependent in nodes {
                let dependent = WorkNodeId(id(dependent)?);
                if node_witness(&tx, mission, dependent)?.0 == "ready"
                    && dependencies_complete(&tx, mission, dependent)?
                {
                    event(
                        &tx,
                        mission,
                        "worknode_ready",
                        &json(&serde_json::json!({"node_id":dependent.0}))?,
                        Some(run),
                    )?;
                }
            }
        }
        tx.commit().map_err(sql)
    }

    /// Every late (non-authoritative) result retained for a Mission, oldest
    /// first. These are evidence only: they never changed WorkNode state.
    pub fn late_results(
        &mut self,
        mission: &MissionId,
    ) -> Result<Vec<(usize, usize, String, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT node_id,run_id,dispatch_id,result FROM late_results WHERE mission_id=?1 ORDER BY late_id")
            .map_err(sql)?;
        let rows = stmt
            .query_map([mission.as_str()], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .map_err(sql)?;
        let mut out = Vec::new();
        for row in rows {
            let (node, run, dispatch_id, result) = row.map_err(sql)?;
            out.push((id(node)?, id(run)?, dispatch_id, result));
        }
        Ok(out)
    }

    /// The durable witness for one dispatch id. Restart recovery uses it; no
    /// in-memory map or ready-queue scan is involved.
    pub fn witness(
        &mut self,
        mission: &MissionId,
        dispatch_id: &str,
    ) -> Result<Option<DispatchWitness>> {
        let row = self
            .conn
            .query_row(
                "SELECT node_id,run_id,generation,runtime_execution_id FROM dispatches WHERE mission_id=?1 AND dispatch_id=?2",
                params![mission.as_str(), dispatch_id],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sql)?;
        Ok(match row {
            None => None,
            Some((node, run, generation, binding)) => Some(DispatchWitness {
                mission_id: mission.as_str().to_string(),
                work_node_id: id(node)?,
                run_id: id(run)?,
                run_generation: u64::try_from(generation)
                    .map_err(|_| invalid("invalid generation"))?,
                runtime_execution_id: binding,
                dispatch_id: dispatch_id.to_string(),
            }),
        })
    }

    /// The witness of the current dispatch attempt for a Run, if any.
    pub fn witness_for_run(
        &mut self,
        mission: &MissionId,
        run: RunId,
    ) -> Result<Option<DispatchWitness>> {
        let dispatch_id: Option<String> = self
            .conn
            .query_row(
                "SELECT dispatch_id FROM dispatches WHERE mission_id=?1 AND run_id=?2",
                params![mission.as_str(), num(run.0)?],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        match dispatch_id {
            Some(dispatch_id) => self.witness(mission, &dispatch_id),
            None => Ok(None),
        }
    }

    /// Every dispatch that has not yet delivered a result. This is the whole
    /// recovery input after a restart: pending work is durable, not guessed.
    pub fn pending_dispatches(&mut self, mission: &MissionId) -> Result<Vec<DispatchWitness>> {
        let mut stmt = self
            .conn
            .prepare("SELECT dispatch_id,node_id,run_id,generation,runtime_execution_id FROM dispatches WHERE mission_id=?1 AND state='pending' ORDER BY created_at,dispatch_id")
            .map_err(sql)?;
        let rows = stmt
            .query_map([mission.as_str()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .map_err(sql)?;
        let mut out = Vec::new();
        for row in rows {
            let (dispatch_id, node, run, generation, binding) = row.map_err(sql)?;
            out.push(DispatchWitness {
                mission_id: mission.as_str().to_string(),
                work_node_id: id(node)?,
                run_id: id(run)?,
                run_generation: u64::try_from(generation)
                    .map_err(|_| invalid("invalid generation"))?,
                runtime_execution_id: binding,
                dispatch_id,
            });
        }
        Ok(out)
    }

    /// Whether this exact witness may still mutate its WorkNode.
    ///
    /// A structurally invalid, unknown or mismatched witness is a hard error
    /// (fail closed). A structurally valid witness naming a non-authoritative
    /// Run returns an explicit non-authoritative disposition instead, so the
    /// delivery can be retained as evidence.
    pub fn validate_witness(&mut self, witness: &DispatchWitness) -> Result<WitnessDisposition> {
        let mission = MissionId::new(&witness.mission_id)?;
        let tx = self.conn.transaction().map_err(sql)?;
        let dispatch: Option<(i64, i64, i64, String, String)> = tx
            .query_row(
                "SELECT node_id,run_id,generation,runtime_execution_id,state FROM dispatches WHERE mission_id=?1 AND dispatch_id=?2",
                params![mission.as_str(), witness.dispatch_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(sql)?;
        let (node, run, generation, binding, dispatch_state) =
            dispatch.ok_or_else(|| invalid("unknown dispatch witness"))?;
        if id(node)? != witness.work_node_id
            || id(run)? != witness.run_id
            || u64::try_from(generation).ok() != Some(witness.run_generation)
            || binding != witness.runtime_execution_id
        {
            return Err(invalid(
                "dispatch witness does not match the durable dispatch",
            ));
        }
        let run_state: Option<String> = tx
            .query_row(
                "SELECT state FROM runs WHERE mission_id=?1 AND run_id=?2",
                params![mission.as_str(), num(witness.run_id)?],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        let run_state = run_state.ok_or_else(|| invalid("unknown dispatch witness Run"))?;
        // A fenced or superseded generation can never regain authority, even
        // for a dispatch whose earlier delivery already failed the Run.
        if matches!(run_state.as_str(), "fenced" | "superseded") {
            return Ok(WitnessDisposition::LateEvidence);
        }
        if dispatch_state == "delivered" {
            return Ok(WitnessDisposition::AlreadyApplied);
        }
        if run_state != "active" {
            return Ok(WitnessDisposition::Superseded);
        }
        let (state, node_generation, active, _) =
            node_witness(&tx, &mission, WorkNodeId(witness.work_node_id))?;
        if state != "running"
            || active != Some(witness.run_id)
            || u64::try_from(node_generation).ok() != Some(witness.run_generation)
        {
            return Err(invalid("dispatch witness does not name the active Run"));
        }
        Ok(WitnessDisposition::Authoritative)
    }

    /// Record the concrete host session for a dispatched Run. The canonical
    /// binding never changes; this only makes the external execution visible
    /// to recovery and inspection.
    pub fn bind_host_session(
        &mut self,
        witness: &DispatchWitness,
        host_session_id: &str,
    ) -> Result<()> {
        if host_session_id.is_empty() {
            return Err(invalid("missing host session"));
        }
        let mission = MissionId::new(&witness.mission_id)?;
        let tx = self.conn.transaction().map_err(sql)?;
        let dispatch: Option<(i64, i64, i64, String)> = tx
            .query_row(
                "SELECT node_id,run_id,generation,runtime_execution_id FROM dispatches WHERE mission_id=?1 AND dispatch_id=?2",
                params![mission.as_str(), witness.dispatch_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .map_err(sql)?;
        let dispatch = dispatch.ok_or_else(|| invalid("unknown dispatch witness"))?;
        if id(dispatch.0)? != witness.work_node_id
            || id(dispatch.1)? != witness.run_id
            || u64::try_from(dispatch.2).ok() != Some(witness.run_generation)
            || dispatch.3 != witness.runtime_execution_id
        {
            return Err(invalid(
                "dispatch witness does not match the durable dispatch",
            ));
        }
        let updated = tx
            .execute(
                "UPDATE runs SET host_session_id=?3 WHERE mission_id=?1 AND run_id=?2 AND (host_session_id IS NULL OR host_session_id=?3)",
                params![mission.as_str(), num(witness.run_id)?, host_session_id],
            )
            .map_err(sql)?;
        if updated != 1 {
            return Err(invalid("Run is already bound to another host session"));
        }
        event(
            &tx,
            &mission,
            "run_bound",
            &json(
                &serde_json::json!({"node_id":witness.work_node_id,"run_id":witness.run_id,"host_session_id":host_session_id}),
            )?,
            Some(RunId(witness.run_id)),
        )?;
        tx.commit().map_err(sql)
    }

    /// The host session bound to a Run, when the runtime reported one.
    pub fn host_session(&mut self, mission: &MissionId, run: RunId) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT host_session_id FROM runs WHERE mission_id=?1 AND run_id=?2",
                params![mission.as_str(), num(run.0)?],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()
            .map(|value| value.flatten())
            .map_err(sql)
    }

    /// Store trusted verification evidence for exactly one dispatch attempt.
    /// The record is append-only and independent of the model output.
    pub fn record_verification(
        &mut self,
        witness: &DispatchWitness,
        passed: bool,
        report: &Value,
        commands: &[String],
        now: i64,
    ) -> Result<VerificationRecord> {
        let mission = MissionId::new(&witness.mission_id)?;
        let disposition = self.validate_witness(witness)?;
        if matches!(
            disposition,
            WitnessDisposition::LateEvidence | WitnessDisposition::Superseded
        ) {
            return Err(invalid("a fenced Run cannot record new evidence"));
        }
        let tx = self.conn.transaction().map_err(sql)?;
        let attempt: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM verifications WHERE mission_id=?1 AND dispatch_id=?2",
                params![mission.as_str(), witness.dispatch_id],
                |r| r.get(0),
            )
            .map_err(sql)?;
        let attempt = attempt + 1;
        // A report the caller labelled is not evidence the trusted runner
        // produced, so it can never satisfy a completion.
        let trusted = is_trusted_report(&json(&report).unwrap_or_else(|_| "{}".to_string()));
        let record = VerificationRecord {
            verification_id: format!("vf-{}-{attempt}", witness.dispatch_id),
            node_id: witness.work_node_id,
            run_id: witness.run_id,
            dispatch_id: witness.dispatch_id.clone(),
            outcome: if passed { "passed" } else { "failed" }.to_string(),
            trusted,
            passed,
            commands: commands.to_vec(),
            created_at: now,
        };
        // The trusted command list is part of the durable report so the
        // evidence can be re-read (and audited) without the caller.
        let mut stored = report.clone();
        if let Some(object) = stored.as_object_mut() {
            object.insert(
                "commands".to_string(),
                Value::Array(
                    commands
                        .iter()
                        .map(|command| Value::String(command.clone()))
                        .collect(),
                ),
            );
            // Only a report the runner actually produced may claim the
            // runner as its source. A caller-labelled report keeps its own
            // label so it can never be mistaken for a trusted command result.
            if object.get("evidence").and_then(Value::as_str).is_none() {
                object.insert(
                    "source".to_string(),
                    Value::String("ocg verification runner".to_string()),
                );
            }
        }
        tx.execute(
            "INSERT INTO verifications (mission_id,verification_id,node_id,run_id,dispatch_id,outcome,report,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                mission.as_str(),
                record.verification_id,
                num(witness.work_node_id)?,
                num(witness.run_id)?,
                witness.dispatch_id,
                record.outcome,
                json(&stored)?,
                now
            ],
        )
        .map_err(sql)?;
        event(
            &tx,
            &mission,
            "verification_recorded",
            &json(&serde_json::json!({
                "node_id":witness.work_node_id,
                "run_id":witness.run_id,
                "verification_id":record.verification_id,
                "outcome":record.outcome
            }))?,
            Some(RunId(witness.run_id)),
        )?;
        tx.commit().map_err(sql)?;
        Ok(record)
    }

    /// The latest verification evidence for one dispatch attempt.
    pub fn verification_for(
        &mut self,
        mission: &MissionId,
        witness: &DispatchWitness,
    ) -> Result<Option<VerificationRecord>> {
        let row: Option<(String, i64, i64, String, String, String, i64)> = self
            .conn
            .query_row(
                "SELECT verification_id,node_id,run_id,dispatch_id,outcome,report,created_at FROM verifications WHERE mission_id=?1 AND dispatch_id=?2 ORDER BY verification_id DESC LIMIT 1",
                params![mission.as_str(), witness.dispatch_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .optional()
            .map_err(sql)?;
        let Some((verification_id, node, run, dispatch_id, outcome, report, created_at)) = row
        else {
            return Ok(None);
        };
        let commands: Vec<String> = serde_json::from_str(&report)
            .ok()
            .and_then(|value: Value| {
                value.get("commands").and_then(Value::as_array).map(|list| {
                    list.iter()
                        .filter_map(|item| item.as_str().map(str::to_string))
                        .collect()
                })
            })
            .unwrap_or_default();
        Ok(Some(VerificationRecord {
            verification_id,
            node_id: id(node)?,
            run_id: id(run)?,
            dispatch_id,
            trusted: is_trusted_report(&report),
            passed: outcome == "passed",
            outcome,
            commands,
            created_at,
        }))
    }

    /// Every verification record for a Mission, oldest first.
    pub fn verifications(&mut self, mission: &MissionId) -> Result<Vec<VerificationRecord>> {
        let mut stmt = self
            .conn
            .prepare("SELECT dispatch_id,verification_id,node_id,run_id,outcome,report,created_at FROM verifications WHERE mission_id=?1 ORDER BY created_at,verification_id")
            .map_err(sql)?;
        let rows = stmt
            .query_map([mission.as_str()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, i64>(6)?,
                ))
            })
            .map_err(sql)?;
        let mut out = Vec::new();
        for row in rows {
            let (dispatch_id, verification_id, node, run, outcome, report, created_at) =
                row.map_err(sql)?;
            let commands: Vec<String> = serde_json::from_str(&report)
                .ok()
                .and_then(|value: Value| {
                    value.get("commands").and_then(Value::as_array).map(|list| {
                        list.iter()
                            .filter_map(|item| item.as_str().map(str::to_string))
                            .collect()
                    })
                })
                .unwrap_or_default();
            out.push(VerificationRecord {
                verification_id,
                node_id: id(node)?,
                run_id: id(run)?,
                dispatch_id,
                trusted: is_trusted_report(&report),
                passed: outcome == "passed",
                outcome,
                commands,
                created_at,
            });
        }
        Ok(out)
    }

    /// Apply one witnessed delivery. A successful completion additionally
    /// requires passing verification evidence for the same dispatch: model
    /// output alone never completes a WorkNode.
    pub fn complete_dispatch(
        &mut self,
        witness: &DispatchWitness,
        outcome: RunState,
        result: &str,
        now: i64,
    ) -> Result<DispatchOutcome> {
        if matches!(outcome, RunState::Active | RunState::Fenced) {
            return Err(invalid("invalid terminal Run outcome"));
        }
        let mission = MissionId::new(&witness.mission_id)?;
        let disposition = self.validate_witness(witness)?;
        let report = |disposition: WitnessDisposition, node_state: String, run_state: String| {
            DispatchOutcome {
                witness: witness.clone(),
                disposition: disposition.text().to_string(),
                node_state,
                run_state,
                // Only a late-evidence delivery retains a result without
                // authority; an acknowledged duplicate changes nothing.
                evidence_only: disposition == WitnessDisposition::LateEvidence,
            }
        };
        match disposition {
            WitnessDisposition::AlreadyApplied | WitnessDisposition::Superseded => {
                let (node_state, run_state) = self.node_and_run_state(&mission, witness)?;
                return Ok(report(disposition, node_state, run_state));
            }
            WitnessDisposition::LateEvidence => {
                self.retain_late_evidence(&mission, witness, result, now)?;
                let (node_state, run_state) = self.node_and_run_state(&mission, witness)?;
                return Ok(report(disposition, node_state, run_state));
            }
            WitnessDisposition::Authoritative => {}
        }
        if outcome == RunState::Completed {
            let evidence = self.verification_for(&mission, witness)?;
            match evidence {
                Some(evidence) if evidence.passed && evidence.trusted => {}
                Some(evidence) if !evidence.trusted => {
                    return Err(invalid(
                        "verification evidence is not trusted: run the configured stage to complete a Run",
                    ))
                }
                Some(_) => {
                    return Err(invalid(
                        "verification failed: replacement or failure required",
                    ))
                }
                None => {
                    return Err(invalid(
                        "verification evidence is required before a Run can complete",
                    ))
                }
            }
        }
        self.finish_run(
            &mission,
            WorkNodeId(witness.work_node_id),
            RunId(witness.run_id),
            outcome,
            Some(result),
            now,
        )?;
        self.mark_dispatch(&mission, &witness.dispatch_id, "delivered", now)?;
        let (node_state, run_state) = self.node_and_run_state(&mission, witness)?;
        Ok(report(
            WitnessDisposition::Authoritative,
            node_state,
            run_state,
        ))
    }

    fn node_and_run_state(
        &mut self,
        mission: &MissionId,
        witness: &DispatchWitness,
    ) -> Result<(String, String)> {
        let node_state: Option<String> = self
            .conn
            .query_row(
                "SELECT state FROM work_nodes WHERE mission_id=?1 AND node_id=?2",
                params![mission.as_str(), num(witness.work_node_id)?],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("unknown WorkNode"))?;
        let run_state: Option<String> = self
            .conn
            .query_row(
                "SELECT state FROM runs WHERE mission_id=?1 AND run_id=?2",
                params![mission.as_str(), num(witness.run_id)?],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("unknown Run"))?;
        Ok((
            node_state.unwrap_or_default(),
            run_state.unwrap_or_default(),
        ))
    }

    fn retain_late_evidence(
        &mut self,
        mission: &MissionId,
        witness: &DispatchWitness,
        result: &str,
        now: i64,
    ) -> Result<()> {
        let tx = self.conn.transaction().map_err(sql)?;
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM runs WHERE mission_id=?1 AND run_id=?2",
                params![mission.as_str(), num(witness.run_id)?],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        if state.as_deref() == Some("fenced") {
            // The generation's own terminal result is never rewritten by a
            // late delivery. The late text is retained separately so it stays
            // inspectable evidence without mutating history.
            let late_id: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(late_id),0)+1 FROM late_results WHERE mission_id=?1",
                    [mission.as_str()],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            tx.execute(
                "INSERT INTO late_results (mission_id,late_id,node_id,run_id,dispatch_id,result,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    mission.as_str(),
                    late_id,
                    num(witness.work_node_id)?,
                    num(witness.run_id)?,
                    witness.dispatch_id,
                    result,
                    now
                ],
            )
            .map_err(sql)?;
            event(
                &tx,
                mission,
                "late_result_retained",
                &json(&serde_json::json!({
                    "node_id":witness.work_node_id,
                    "run_id":witness.run_id,
                    "dispatch_id":witness.dispatch_id,
                    "late_id":late_id
                }))?,
                Some(RunId(witness.run_id)),
            )?;
            let updated = tx
                .execute(
                    "UPDATE runs SET result=?3 WHERE mission_id=?1 AND run_id=?2 AND result IS NULL",
                    params![mission.as_str(), num(witness.run_id)?, result],
                )
                .map_err(sql)?;
            if updated == 1 {
                event(
                    &tx,
                    mission,
                    "fenced_result",
                    &json(
                        &serde_json::json!({"run_id":witness.run_id,"dispatch_id":witness.dispatch_id}),
                    )?,
                    Some(RunId(witness.run_id)),
                )?;
            }
        }
        let updated = tx
            .execute(
                "UPDATE dispatches SET state='late',delivered_at=?3 WHERE mission_id=?1 AND dispatch_id=?2 AND state='pending'",
                params![mission.as_str(), witness.dispatch_id, now],
            )
            .map_err(sql)?;
        if updated == 1 {
            event(
                &tx,
                mission,
                "late_result_retained",
                &json(&witness.to_json())?,
                Some(RunId(witness.run_id)),
            )?;
        }
        tx.commit().map_err(sql)
    }

    fn mark_dispatch(
        &mut self,
        mission: &MissionId,
        dispatch_id: &str,
        state: &str,
        now: i64,
    ) -> Result<()> {
        let updated = self
            .conn
            .execute(
                "UPDATE dispatches SET state=?3,delivered_at=?4 WHERE mission_id=?1 AND dispatch_id=?2",
                params![mission.as_str(), dispatch_id, state, now],
            )
            .map_err(sql)?;
        if updated != 1 {
            return Err(invalid("unknown dispatch witness"));
        }
        Ok(())
    }

    /// The terminal Mission transition. It requires a completed root, no
    /// non-terminal WorkNode, and passing verification evidence for the root
    /// Run. It is idempotent: a repeated terminal call is a no-op.
    pub fn complete_mission(&mut self, mission: &MissionId, state: &str, now: i64) -> Result<()> {
        if !matches!(state, "completed" | "failed" | "cancelled") {
            return Err(invalid("invalid terminal Mission state"));
        }
        let tx = self.conn.transaction().map_err(sql)?;
        let current: Option<String> = tx
            .query_row(
                "SELECT state FROM missions WHERE mission_id=?1",
                [mission.as_str()],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("unknown canonical Mission"))?;
        if current.as_deref() != Some("active") {
            tx.commit().map_err(sql)?;
            return Ok(());
        }
        let root: String = tx
            .query_row(
                "SELECT state FROM work_nodes WHERE mission_id=?1 AND node_id=0",
                [mission.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if state == "completed" && root != "completed" {
            return Err(invalid("Mission cannot complete before its root WorkNode"));
        }
        let open: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM work_nodes WHERE mission_id=?1 AND state IN ('ready','running')",
                [mission.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if open != 0 {
            return Err(invalid("Mission still has non-terminal WorkNodes"));
        }
        if state == "completed" {
            // The terminal transition accepts only *trusted* evidence for the
            // root Run, exactly as `complete_dispatch` does for a WorkNode. An
            // operator assertion is never enough to close a Mission.
            let evidence: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM verifications v JOIN runs r ON r.mission_id=v.mission_id AND r.run_id=v.run_id JOIN work_nodes root ON root.mission_id=r.mission_id AND root.node_id=0 WHERE v.mission_id=?1 AND r.node_id=0 AND r.generation=root.generation AND r.state='completed' AND v.outcome='passed' AND COALESCE(json_extract(v.report, '$.evidence'), '') != 'operator-assertion')",
                    [mission.as_str()],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            if !evidence {
                return Err(invalid(
                    "Mission completion requires passing verification evidence",
                ));
            }
        }
        tx.execute(
            "UPDATE missions SET state=?2,completed_at=?3 WHERE mission_id=?1",
            params![mission.as_str(), state, now],
        )
        .map_err(sql)?;
        event(
            &tx,
            mission,
            "mission_completed",
            &json(&serde_json::json!({"state":state}))?,
            None,
        )?;
        tx.commit().map_err(sql)
    }

    /// The durable Mission lifecycle state, or `None` for an unknown Mission.
    pub fn mission_state_row(
        &mut self,
        mission: &MissionId,
    ) -> Result<Option<(String, i64, Option<i64>)>> {
        self.conn
            .query_row(
                "SELECT state,created_at,completed_at FROM missions WHERE mission_id=?1",
                [mission.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sql)
    }

    /// Every canonical Mission with its lifecycle state, newest first.
    pub fn list_missions(&mut self) -> Result<Vec<MissionListRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT mission_id,created_at,state,completed_at FROM missions ORDER BY created_at DESC,mission_id")
            .map_err(sql)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                ))
            })
            .map_err(sql)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(sql)?);
        }
        Ok(out)
    }

    /// The authoritative Run bound to exactly this execution identity, in a
    /// named Mission. This is how a Lead or Worker session is mapped back to
    /// its WorkNode and Run without consulting prompt text, agent name or
    /// ready-queue position.
    pub fn authoritative_run_by_binding(
        &mut self,
        mission: &MissionId,
        binding: &str,
    ) -> Result<Option<(WorkNodeId, RunId)>> {
        if binding.is_empty() {
            return Err(invalid("missing runtime binding"));
        }
        let row: Option<(i64, i64)> = self
            .conn
            .query_row(
                "SELECT node_id,run_id FROM runs WHERE mission_id=?1 AND (runtime_execution_id=?2 OR host_session_id=?2) AND state='active' ORDER BY run_id",
                params![mission.as_str(), binding],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        let Some((node, run)) = row else {
            return Ok(None);
        };
        let node = WorkNodeId(id(node)?);
        let run = RunId(id(run)?);
        Ok(self
            .load(mission)?
            .filter(|state| state.authoritative(node, run))
            .map(|_| (node, run)))
    }

    /// Missions whose current runtime binding or bound host session is exactly
    /// this execution identity. This is durable, and it is how a Lead session
    /// is mapped to its canonical Mission without inspecting prompt text.
    pub fn missions_by_binding(&mut self, runtime_execution_id: &str) -> Result<Vec<MissionId>> {
        if runtime_execution_id.is_empty() {
            return Err(invalid("missing runtime binding"));
        }
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT mission_id FROM runs WHERE runtime_execution_id=?1 OR host_session_id=?1 ORDER BY mission_id")
            .map_err(sql)?;
        let rows = stmt
            .query_map([runtime_execution_id], |r| r.get::<_, String>(0))
            .map_err(sql)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(MissionId::new(row.map_err(sql)?)?);
        }
        Ok(out)
    }

    /// Read the pre-run Mission configuration and its revision.
    pub fn mission_config(&mut self, mission: &MissionId) -> Result<Option<(Value, u64)>> {
        let row: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT config,revision FROM mission_config WHERE mission_id=?1",
                [mission.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        match row {
            None => Ok(None),
            Some((config, revision)) => Ok(Some((
                serde_json::from_str(&config)
                    .map_err(|e| invalid(&format!("invalid MissionConfig: {e}")))?,
                u64::try_from(revision).map_err(|_| invalid("invalid config revision"))?,
            ))),
        }
    }

    /// Replace the pre-run Mission configuration. Once any Run is dispatched
    /// the configuration is frozen: a dispatched Run keeps its frozen executor
    /// contract, and only a new Mission may change the pre-run contract.
    pub fn set_mission_config(
        &mut self,
        mission: &MissionId,
        config: &Value,
        now: i64,
    ) -> Result<u64> {
        if !config.is_object() {
            return Err(invalid("MissionConfig must be an object"));
        }
        let tx = self.conn.transaction().map_err(sql)?;
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM missions WHERE mission_id=?1)",
                [mission.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if !exists {
            return Err(invalid("unknown canonical Mission"));
        }
        let dispatched: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runs WHERE mission_id=?1)",
                [mission.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if dispatched {
            return Err(invalid(
                "Mission is already dispatched: pre-run configuration is frozen",
            ));
        }
        let revision: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(revision),0)+1 FROM mission_config WHERE mission_id=?1",
                [mission.as_str()],
                |r| r.get(0),
            )
            .map_err(sql)?;
        tx.execute(
            "INSERT INTO mission_config (mission_id,config,revision,updated_at) VALUES (?1,?2,?3,?4) ON CONFLICT(mission_id) DO UPDATE SET config=?2,revision=?3,updated_at=?4",
            params![mission.as_str(), json(config)?, revision, now],
        )
        .map_err(sql)?;
        event(
            &tx,
            mission,
            "mission_configured",
            &json(&serde_json::json!({"revision":revision}))?,
            None,
        )?;
        tx.commit().map_err(sql)?;
        u64::try_from(revision).map_err(|_| invalid("invalid config revision"))
    }

    /// Late evidence is not authority: it does not change WorkNode state.
    pub fn record_fenced_result(
        &mut self,
        mission: &MissionId,
        run: RunId,
        result: &str,
    ) -> Result<()> {
        let tx = self.conn.transaction().map_err(sql)?;
        let updated = tx.execute(
            "UPDATE runs SET result=?3 WHERE mission_id=?1 AND run_id=?2 AND state='fenced' AND result IS NULL",
            params![mission.as_str(), num(run.0)?, result],
        ).map_err(sql)?;
        if updated != 1 {
            return Err(invalid("Run is not fenced or already has a result"));
        }
        event(&tx, mission, "fenced_result", "{}", Some(run))?;
        tx.commit().map_err(sql)
    }
}

/// Whether a stored verification report came from the trusted runner.
///
/// A runner report carries no `evidence` label. Any explicit label — an
/// operator assertion, or a stage that did not run — marks a report that was
/// not produced by executing a trusted command, so it can never satisfy a
/// completion.
fn is_trusted_report(report: &str) -> bool {
    serde_json::from_str::<Value>(report)
        .ok()
        .and_then(|value| value.get("evidence").and_then(Value::as_str).map(|_| false))
        .unwrap_or(true)
}

fn migrate_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(sql)?;
    let found = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sql)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql)?
        .into_iter()
        .any(|name| name == column);
    drop(stmt);
    if !found {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )
        .map_err(sql)?;
    }
    Ok(())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn next_id(tx: &Transaction<'_>, table: &str, column: &str, mission: &MissionId) -> Result<usize> {
    // Only internal static identifiers are passed here, never user input.
    let n: i64 = tx
        .query_row(
            &format!("SELECT COALESCE(MAX({column})+1,0) FROM {table} WHERE mission_id=?1"),
            [mission.as_str()],
            |r| r.get(0),
        )
        .map_err(sql)?;
    id(n)
}
fn event(
    tx: &Transaction<'_>,
    mission: &MissionId,
    kind: &str,
    payload: &str,
    by: Option<RunId>,
) -> Result<()> {
    let seq: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(seq)+1,1) FROM domain_events WHERE mission_id=?1",
            [mission.as_str()],
            |r| r.get(0),
        )
        .map_err(sql)?;
    tx.execute(
        "INSERT INTO domain_events VALUES (?1,?2,?3,?4,?5)",
        params![
            mission.as_str(),
            seq,
            kind,
            payload,
            by.map(|v| num(v.0)).transpose()?
        ],
    )
    .map_err(sql)?;
    Ok(())
}
fn node_witness(
    tx: &Transaction<'_>,
    mission: &MissionId,
    node: WorkNodeId,
) -> Result<(String, i64, Option<usize>, i64)> {
    let row = tx.query_row("SELECT state,generation,active_run_id,not_before FROM work_nodes WHERE mission_id=?1 AND node_id=?2",params![mission.as_str(),num(node.0)?],|r| Ok((r.get(0)?,r.get(1)?,r.get::<_,Option<i64>>(2)?,r.get(3)?))).optional().map_err(sql)?.ok_or_else(|| invalid("unknown WorkNode"))?;
    Ok((row.0, row.1, optional_id(row.2)?, row.3))
}
fn require_authority(
    tx: &Transaction<'_>,
    mission: &MissionId,
    node: WorkNodeId,
    run: RunId,
) -> Result<()> {
    let (state, generation, active, _) = node_witness(tx, mission, node)?;
    let valid: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM runs WHERE mission_id=?1 AND run_id=?2 AND node_id=?3 AND generation=?4 AND state='active')",params![mission.as_str(),num(run.0)?,num(node.0)?,generation],|r| r.get(0)).map_err(sql)?;
    if state != "running" || active != Some(run.0) || !valid {
        return Err(invalid("Run is not authoritative"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_run(
    tx: &Transaction<'_>,
    mission: &MissionId,
    run: usize,
    node: WorkNodeId,
    generation: i64,
    contract: &RunContract,
    runtime_execution_id: Option<&str>,
    now: i64,
) -> Result<()> {
    if contract.executor.is_empty() || contract.model.is_empty() || contract.role.is_empty() {
        return Err(invalid("incomplete RunContract"));
    }
    let binding = match runtime_execution_id {
        Some(binding) if !binding.is_empty() => binding,
        _ => {
            return Err(invalid("missing runtime binding"));
        }
    };
    tx.execute(
        "INSERT INTO runs (mission_id,run_id,node_id,generation,contract,runtime_execution_id,state,created_at) VALUES (?1,?2,?3,?4,?5,?6,'active',?7)",
        params![
            mission.as_str(),
            num(run)?,
            num(node.0)?,
            generation,
            json(contract)?,
            binding,
            now
        ],
    )
    .map_err(sql)?;
    Ok(())
}

/// A deterministic, durable identity for one dispatch attempt. It is derived
/// from the Mission, the dense Run id and the Run generation, so it is stable
/// across restart and never collides across generations.
fn dispatch_identity(mission: &MissionId, run: usize, generation: i64) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(mission.as_str().as_bytes());
    hasher.update([0u8]);
    hasher.update(run.to_string().as_bytes());
    hasher.update([0u8]);
    hasher.update(generation.to_string().as_bytes());
    let digest = hasher.finalize();
    let short: String = digest[..6]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("d-{short}-r{run}-g{generation}")
}

/// The OCG-issued planned binding for a Run that has no external host session
/// yet. It is a real, restart-stable execution identity — never a prompt hash
/// and never a host session id.
fn planned_binding(mission: &MissionId, run: usize, generation: i64) -> String {
    format!(
        "ocg-planned-{}",
        dispatch_identity(mission, run, generation)
    )
}

fn record_dispatch(
    tx: &Transaction<'_>,
    mission: &MissionId,
    run: usize,
    node: WorkNodeId,
    generation: i64,
    runtime_execution_id: &str,
    now: i64,
) -> Result<DispatchWitness> {
    if runtime_execution_id.is_empty() {
        return Err(invalid("missing runtime binding"));
    }
    let witness = DispatchWitness {
        mission_id: mission.as_str().to_string(),
        work_node_id: node.0,
        run_id: run,
        run_generation: u64::try_from(generation).map_err(|_| invalid("invalid generation"))?,
        runtime_execution_id: runtime_execution_id.to_string(),
        dispatch_id: dispatch_identity(mission, run, generation),
    };
    tx.execute(
        "INSERT INTO dispatches (mission_id,dispatch_id,node_id,run_id,generation,runtime_execution_id,state,created_at) VALUES (?1,?2,?3,?4,?5,?6,'pending',?7)",
        params![
            mission.as_str(),
            witness.dispatch_id,
            num(node.0)?,
            num(run)?,
            generation,
            runtime_execution_id,
            now
        ],
    )
    .map_err(sql)?;
    Ok(witness)
}

fn witness_for(tx: &Transaction<'_>, mission: &MissionId, run: RunId) -> Result<DispatchWitness> {
    let row = tx
        .query_row(
            "SELECT dispatch_id,node_id,generation,runtime_execution_id FROM dispatches WHERE mission_id=?1 AND run_id=?2",
            params![mission.as_str(), num(run.0)?],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(sql)?
        .ok_or_else(|| invalid("Run has no dispatch witness"))?;
    Ok(DispatchWitness {
        mission_id: mission.as_str().to_string(),
        work_node_id: id(row.1)?,
        run_id: run.0,
        run_generation: u64::try_from(row.2).map_err(|_| invalid("invalid generation"))?,
        runtime_execution_id: row.3,
        dispatch_id: row.0,
    })
}

fn start_run_transaction(
    tx: &Transaction<'_>,
    mission: &MissionId,
    node: WorkNodeId,
    contract: &RunContract,
    binding: Option<&str>,
    now: i64,
) -> Result<RunId> {
    let state = node_witness(tx, mission, node)?;
    if state.0 != "ready"
        || state.2.is_some()
        || now < state.3
        || !dependencies_complete(tx, mission, node)?
    {
        return Err(invalid("WorkNode not ready"));
    }
    let next = next_id(tx, "runs", "run_id", mission)?;
    let generation = state
        .1
        .checked_add(1)
        .ok_or_else(|| invalid("generation overflow"))?;
    let owned;
    let binding = match binding {
        Some(binding) if !binding.is_empty() => binding,
        _ => {
            owned = planned_binding(mission, next, generation);
            owned.as_str()
        }
    };
    insert_run(
        tx,
        mission,
        next,
        node,
        generation,
        contract,
        Some(binding),
        now,
    )?;
    record_dispatch(tx, mission, next, node, generation, binding, now)?;
    tx.execute("UPDATE work_nodes SET state='running',generation=?3,active_run_id=?4 WHERE mission_id=?1 AND node_id=?2",params![mission.as_str(),num(node.0)?,generation,num(next)?]).map_err(sql)?;
    event(
        tx,
        mission,
        "run_started",
        &json(&serde_json::json!({"node_id":node.0,"run_id":next}))?,
        Some(RunId(next)),
    )?;
    Ok(RunId(next))
}
fn dependencies_complete(
    tx: &Transaction<'_>,
    mission: &MissionId,
    node: WorkNodeId,
) -> Result<bool> {
    let missing: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM dependencies d JOIN work_nodes n ON n.mission_id=d.mission_id AND n.node_id=d.depends_on_node_id WHERE d.mission_id=?1 AND d.node_id=?2 AND n.state!='completed')",params![mission.as_str(),num(node.0)?],|r| r.get(0)).map_err(sql)?;
    Ok(!missing)
}
fn load_states_for_dependency(
    tx: &Transaction<'_>,
    mission: &MissionId,
    node: WorkNodeId,
    prerequisite: WorkNodeId,
) -> Result<Vec<String>> {
    let mut states = Vec::new();
    for id in [node, prerequisite] {
        let (state, _, active, _) = node_witness(tx, mission, id)?;
        if active.is_some() {
            return Err(invalid("cannot edit dispatched dependencies"));
        }
        states.push(state);
    }
    Ok(states)
}
