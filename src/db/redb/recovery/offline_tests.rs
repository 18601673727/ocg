use std::fs::{self, File};
use std::num::NonZeroU32;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use ::redb::{Database, ReadableDatabase, ReadableTable};
use serde_json::{Value as Json, json};

use super::{apply_compaction_ref_rebuild, verify_compaction_ref_publication_delta};
use crate::db::entities::{
    AllocationExecutor, AllocationPool, CompactionPolicy, ConfigBody, ConfigId, ConnectionConfig,
    DecisionAgentConfig, DecisionCapability, DecisionLimits, DecisionModelConfig, DecisionProtocol,
    ExecutionOperation, ExecutionPoolId, ExecutorId, GlobalCompactionSettings, JobEdit, JobId,
    LlmTransport, ModelCapability, ModelConfig, PredictiveCompaction,
};
use crate::db::redb::batch::{Commit, run_batch_with};
use crate::db::redb::recovery;
use crate::db::redb::schema::{
    self, CONFIGS, CONVERSATION_CONFIG_REFS, CONVERSATIONS, JOB_RUNTIME, META,
};
use crate::host::data_directory::lock_database;

type RefKey = (u8, u128, u128);

struct Fixture {
    dir: PathBuf,
    owner: JobId,
    summary: ConfigId,
    expected: [RefKey; 3],
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("ocg-ref-rebuild-{}", uuid::Uuid::now_v7()));
        fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        let _lock = lock_database(&dir).unwrap();
        let opened = recovery::open(&dir).unwrap();
        let (owner, summary, predictor, decision) =
            run_batch_with(&opened.database, opened.epoch, Commit::Durable, |batch| {
                let pool = ExecutionPoolId::new(uuid::Uuid::now_v7().as_u128());
                batch.register_allocation_pool(AllocationPool {
                    id: pool,
                    name: "Synthetic pool".into(),
                    capacity: NonZeroU32::MIN,
                    enabled: true,
                    archived: false,
                    revision: 1,
                })?;
                batch.register_allocation_executor(AllocationExecutor {
                    id: ExecutorId::new(uuid::Uuid::now_v7().as_u128()),
                    pool,
                    name: "Synthetic executor".into(),
                    enabled: true,
                    archived: false,
                    revision: 1,
                    capabilities: [
                        (
                            ExecutionOperation::LlmRequest,
                            Some(LlmTransport::OpenAiChatCompletions),
                        ),
                        (ExecutionOperation::Decision, None),
                    ]
                    .into(),
                })?;
                let connection = batch.create_config(
                    "Synthetic connection".into(),
                    ConfigBody::Connection(ConnectionConfig {
                        protocol: LlmTransport::OpenAiChatCompletions,
                        endpoint: Some("https://example.invalid/v1".into()),
                        credential_ref: None,
                        headers: Vec::new(),
                    }),
                )?;
                let summary = batch.create_config(
                    "Synthetic summarizer".into(),
                    ConfigBody::Model(ModelConfig {
                        reasoning_efforts: None,
                        connection_id: connection,
                        model: "summary".into(),
                        context_window: Some(8192),
                        parameters: Default::default(),
                        declared_capabilities: [ModelCapability::Chat].into(),
                    }),
                )?;
                let decision = batch.create_config(
                    "Synthetic decision model".into(),
                    ConfigBody::DecisionModel(DecisionModelConfig {
                        connection_id: connection,
                        protocol: DecisionProtocol::TypeSafeSystemOne,
                        model: "predictor".into(),
                        limits: DecisionLimits::default(),
                        declared_capabilities: [
                            DecisionCapability::Choice,
                            DecisionCapability::BinaryProbability,
                        ]
                        .into(),
                    }),
                )?;
                let predictor = batch.create_config(
                    "Synthetic predictor".into(),
                    ConfigBody::DecisionAgent(DecisionAgentConfig {
                        instructions: None,
                        models: vec![decision],
                        default_model: Some(decision),
                    }),
                )?;
                let owner = batch.create_mutable_conversation(None)?;
                batch.create_mutable_conversation(None)?;
                let policy = CompactionPolicy {
                    summarization_model: Some(summary),
                    predictive: Some(PredictiveCompaction::automatic(
                        predictor,
                        batch.freeze_decision_profile(predictor)?.model_ref(),
                    )),
                    ..GlobalCompactionSettings::default().policy
                };
                crate::db::mutations::set_conversation(
                    batch,
                    owner,
                    JobEdit::CompactionPolicy(Some(policy)),
                )?;
                Ok((owner, summary, predictor, decision))
            })
            .result
            .unwrap();
        recovery::record_clean_shutdown(&opened.database).unwrap();
        Self {
            dir,
            owner,
            summary,
            expected: [
                (1, summary.get(), owner.get()),
                (2, predictor.get(), owner.get()),
                (3, decision.get(), owner.get()),
            ],
        }
    }

    fn path(&self) -> PathBuf {
        recovery::database_file(&self.dir)
    }

    fn edit(&self, edit: impl FnOnce(&::redb::WriteTransaction)) {
        let database = Database::open(self.path()).unwrap();
        let tx = database.begin_write().unwrap();
        edit(&tx);
        tx.commit().unwrap();
    }

    fn damage_index(&self, missing: bool, extra: bool) {
        self.edit(|tx| {
            let mut index = tx.open_table(CONVERSATION_CONFIG_REFS).unwrap();
            if missing {
                index.remove(self.expected[0]).unwrap().unwrap();
            }
            if extra {
                index.insert((255, 42, self.owner.get()), ()).unwrap();
            }
        });
    }

    fn bytes(&self) -> Vec<u8> {
        fs::read(self.path()).unwrap()
    }

    fn invoke(&self, arguments: &[&str], crash_at: Option<&str>) -> Output {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", REBUILD_CHILD, "--ignored", "--nocapture"])
            .env("OCG_TEST_REBUILD_DIRECTORY", &self.dir)
            .env("OCG_TEST_REBUILD_ARGUMENTS", arguments.join(" "))
            .env_remove("OCG_RECONCILE_CRASH_AT");
        if let Some(stage) = crash_at {
            command.env("OCG_RECONCILE_CRASH_AT", stage);
        }
        command.output().unwrap()
    }

    fn report(&self, arguments: &[&str], success: bool) -> Json {
        let output = self.invoke(arguments, None);
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_eq!(
            output.status.success(),
            success,
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::Deserializer::from_str(&stdout[stdout.find('{').unwrap()..])
            .into_iter::<Json>()
            .next()
            .unwrap()
            .unwrap()
    }

    fn assert_rebuilt(&self) {
        let database = ::redb::ReadOnlyDatabase::open(self.path()).unwrap();
        let tx = database.begin_read().unwrap();
        let actual: Vec<_> = tx
            .open_table(CONVERSATION_CONFIG_REFS)
            .unwrap()
            .iter()
            .unwrap()
            .map(|entry| entry.unwrap().0.value())
            .collect();
        assert_eq!(actual, self.expected);
        assert!(recovery::verify_reserved_residual(&database).is_ok());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

const REBUILD_CHILD: &str = "db::redb::recovery::offline::tests::rebuild_child";

#[test]
#[ignore = "subprocess entry point for offline publication tests"]
fn rebuild_child() {
    let Ok(directory) = std::env::var("OCG_TEST_REBUILD_DIRECTORY") else {
        return;
    };
    let flags = std::env::var("OCG_TEST_REBUILD_ARGUMENTS").unwrap();
    let mut arguments = vec![directory];
    arguments.extend(flags.split_whitespace().map(str::to_owned));
    let status = crate::cli::run_compaction_ref_rebuild(arguments);
    std::process::exit(i32::from(status.is_err()));
}

#[test]
fn rebuild_reconstructs_missing_and_extra_references_and_preserves_other_tables() {
    for (missing, extra) in [(true, false), (false, true), (true, true)] {
        let fixture = Fixture::new();
        fixture.assert_rebuilt();
        fixture.damage_index(missing, extra);
        let source = fixture.bytes();
        assert!(recovery::open(&fixture.dir).is_err());
        assert_eq!(fixture.bytes(), source);

        let plan = fixture.report(&["--dry-run"], true);
        assert_eq!(plan["plan"]["missing_refs"], u64::from(missing));
        assert_eq!(plan["plan"]["extra_refs"], u64::from(extra));
        assert_eq!(plan["plan"]["expected_refs"], 3);
        let digest = plan["plan_digest"].as_str().unwrap();
        let wrong = fixture.report(&["--apply", &"0".repeat(64)], false);
        assert_eq!(wrong["code"], "plan_digest_mismatch");
        assert_eq!(fixture.bytes(), source);

        let applied = fixture.report(&["--apply", digest], true);
        assert_eq!(applied["state"], "PUBLISHED_FINALIZED");
        let rollback = Path::new(applied["rollback_artifact"]["path"].as_str().unwrap());
        assert_eq!(fs::read(rollback).unwrap(), source);
        fixture.assert_rebuilt();
        let before = ::redb::ReadOnlyDatabase::open(rollback).unwrap();
        let after = ::redb::ReadOnlyDatabase::open(fixture.path()).unwrap();
        schema::verify_compaction_rebuild_preservation(
            &before.begin_read().unwrap(),
            &after.begin_read().unwrap(),
        )
        .unwrap();
        let ready = fixture.report(&["--dry-run"], true);
        assert_eq!(ready["plan"]["changes_required"], false);
        let published = fixture.bytes();
        let noop = fixture.report(&["--apply", ready["plan_digest"].as_str().unwrap()], false);
        assert_eq!(noop["code"], "index_already_current");
        assert_eq!(fixture.bytes(), published);
        assert!(crate::cli::check_offline_publication(&fixture.dir).is_ok());
    }
}

#[test]
fn rebuild_refuses_corrupt_source_rows_and_unrelated_damage() {
    for table in [JOB_RUNTIME, CONVERSATIONS, CONFIGS] {
        let fixture = Fixture::new();
        fixture.damage_index(true, true);
        fixture.edit(|tx| {
            let mut rows = tx.open_table(table).unwrap();
            let (key, header) = {
                let (key, bytes) = rows.first().unwrap().unwrap();
                (key.value(), [bytes.value()[0], bytes.value()[1]])
            };
            rows.insert(key, header.as_slice()).unwrap();
        });
        let source = fixture.bytes();
        let refused = fixture.report(&["--dry-run"], false);
        assert_eq!(refused["code"], "inconsistent_or_corrupt", "{refused}");
        assert_eq!(fixture.bytes(), source);
    }

    let fixture = Fixture::new();
    fixture.damage_index(true, true);
    fixture.edit(|tx| {
        tx.open_table(CONFIGS)
            .unwrap()
            .remove(fixture.summary.get())
            .unwrap()
            .unwrap();
    });
    let source = fixture.bytes();
    let refused = fixture.report(&["--dry-run"], false);
    assert_eq!(refused["code"], "inconsistent_or_corrupt");
    assert!(
        refused["detail"]
            .as_str()
            .unwrap()
            .contains("missing config")
    );
    assert_eq!(fixture.bytes(), source);
}

#[test]
fn rebuild_delta_refuses_unrelated_metadata_changes() {
    let fixture = Fixture::new();
    fixture.damage_index(true, true);
    let plan = fixture.report(&["--dry-run"], true);
    let digest = plan["plan_digest"].as_str().unwrap();
    let sha256 = plan["original"]["sha256"].as_str().unwrap();
    let work = fixture.dir.join("synthetic-work.redb");
    fs::copy(fixture.path(), &work).unwrap();
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&work)
        .unwrap();
    apply_compaction_ref_rebuild(file, &fixture.dir, sha256, digest).unwrap();
    let database = Database::open(&work).unwrap();
    let tx = database.begin_write().unwrap();
    tx.open_table(META)
        .unwrap()
        .insert("clean_shutdown", [0u8].as_slice())
        .unwrap();
    tx.commit().unwrap();
    drop(database);

    let err = verify_compaction_ref_publication_delta(
        &fixture.dir,
        &File::open(fixture.path()).unwrap(),
        &File::open(work).unwrap(),
        sha256,
        &plan["plan"],
        digest,
    )
    .unwrap_err();
    assert_eq!(err.code, "audit_failed");
    assert!(err.detail.contains("preserved table meta"));
}

#[test]
fn rebuild_requires_exclusive_ownership() {
    let fixture = Fixture::new();
    fixture.damage_index(true, false);
    let source = fixture.bytes();
    let lock = lock_database(&fixture.dir).unwrap();
    assert_eq!(
        fixture.report(&["--dry-run"], false)["code"],
        "host_running"
    );
    drop(lock);
    assert_eq!(fixture.bytes(), source);
}

#[cfg(debug_assertions)]
#[test]
fn rebuild_recovers_interrupted_publication_with_evidence() {
    for crash_at in ["before-rename", "after-rename"] {
        let fixture = Fixture::new();
        fixture.damage_index(true, true);
        let source = fixture.bytes();
        let plan = fixture.report(&["--dry-run"], true);
        let digest = plan["plan_digest"].as_str().unwrap();

        let interrupted = fixture.invoke(&["--apply", digest], Some(crash_at));
        assert_eq!(
            interrupted.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&interrupted.stdout)
        );
        assert!(crate::cli::check_offline_publication(&fixture.dir).is_err());
        if crash_at == "before-rename" {
            assert_eq!(fixture.bytes(), source);
            let pending = fixture.report(&["--finalize"], false);
            assert_eq!(pending["state"], "PREPARED_NOT_PUBLISHED");
            assert_eq!(
                fixture.report(&["--apply", digest], true)["outcome"],
                "rebuilt"
            );
        } else {
            fixture.assert_rebuilt();
            let published = fixture.bytes();
            assert_eq!(
                fixture.report(&["--apply", digest], false)["code"],
                "finalization_required"
            );
            let finalized = fixture.report(&["--finalize"], true);
            assert_eq!(finalized["outcome"], "finalized");
            assert_eq!(finalized["rebuild_transaction_reexecuted"], json!(false));
            assert_eq!(fixture.bytes(), published);
        }
        assert!(crate::cli::check_offline_publication(&fixture.dir).is_ok());
        assert_eq!(
            fixture.report(&["--finalize"], true)["outcome"],
            "already_finalized"
        );
        fixture.assert_rebuilt();
    }
}
