use super::*;

fn model_profile_save_bytes(save: &mutations::ModelProfileSave) -> u64 {
    let credential_ref_bytes = match &save.credential_ref {
        mutations::ProfileCredentialRef::Preserve | mutations::ProfileCredentialRef::Clear => 0,
        mutations::ProfileCredentialRef::Explicit(reference)
        | mutations::ProfileCredentialRef::PreserveOrGenerate(reference) => {
            byte_count(reference.len())
        }
    };
    let definition_bytes = match &save.definition {
        mutations::ModelProfileDefinition::Llm {
            model,
            declared_capabilities,
            ..
        } => byte_count(model.len())
            .saturating_add(byte_count(declared_capabilities.len()).saturating_mul(8))
            .saturating_add(512),
        mutations::ModelProfileDefinition::Decision {
            model,
            declared_capabilities,
            ..
        } => byte_count(model.len())
            .saturating_add(byte_count(declared_capabilities.len()).saturating_mul(8))
            .saturating_add(2048),
    };
    let secret_bytes = save
        .secret
        .as_ref()
        .map_or(0, |secret| byte_count(secret.as_str().len()));
    byte_count(save.name.len())
        .saturating_add(byte_count(save.connection_name.len()))
        .saturating_add(
            save.endpoint
                .as_ref()
                .map_or(0, |endpoint| byte_count(endpoint.len())),
        )
        .saturating_add(credential_ref_bytes)
        .saturating_add(secret_bytes)
        .saturating_add(header_change_bytes(&save.headers))
        .saturating_add(definition_bytes)
        .saturating_add(4096)
}

fn header_change_bytes(change: &mutations::HeaderChange) -> u64 {
    match change {
        mutations::HeaderChange::Preserve => 0,
        mutations::HeaderChange::Replace(edits) => edits.iter().fold(0, |bytes, edit| {
            let value = match &edit.value {
                mutations::HeaderEditValue::Plain(value) => value.len(),
                mutations::HeaderEditValue::Secret(payload) => payload
                    .as_ref()
                    .map_or(0, |payload| payload.secret.as_str().len()),
            };
            bytes
                .saturating_add(byte_count(edit.name.len()))
                .saturating_add(byte_count(value))
                .saturating_add(256)
        }),
    }
}
use crate::db::commands::{byte_count, command_bytes, compaction_bytes, config_bytes, text_bytes};

impl DatabaseHandle {
    pub async fn project(&self, id: ProjectId) -> Result<Option<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_project(database, id))
            .await
    }

    pub async fn projects(
        &self,
        before: Option<ProjectId>,
        limit: usize,
    ) -> Result<Vec<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_projects(database, before, limit))
            .await
    }

    pub(crate) async fn agent_model_configs(
        &self,
        ids: Vec<ConfigId>,
    ) -> Result<Vec<crate::db::entities::AgentModelConfig>, DatabaseError> {
        self.read(move |database| redb::read_agent_model_configs(database, &ids))
            .await
    }

    pub async fn config(&self, id: ConfigId) -> Result<Option<ConfigRecord>, DatabaseError> {
        self.read(move |database| redb::read_config(database, id))
            .await
    }

    pub(crate) async fn credential_ref_in_use(
        &self,
        reference: &str,
    ) -> Result<bool, DatabaseError> {
        let reference = reference.to_owned();
        self.read(move |database| redb::read_credential_ref_in_use(database, &reference))
            .await
    }

    pub async fn configs(
        &self,
        kind: Option<ConfigKind>,
        before: Option<ConfigId>,
        limit: usize,
    ) -> Result<crate::db::entities::CursorPage<ConfigRecord, ConfigId>, DatabaseError> {
        self.read(move |database| redb::read_configs(database, kind, before, limit))
            .await
    }

    pub async fn global_outbound_headers(
        &self,
    ) -> Result<crate::db::entities::GlobalHeaders, DatabaseError> {
        self.read(redb::read_global_outbound_headers).await
    }

    pub async fn global_compaction_settings(
        &self,
    ) -> Result<crate::db::entities::GlobalCompactionSettings, DatabaseError> {
        self.read(redb::read_global_compaction_settings).await
    }

    pub(crate) async fn find_config(
        &self,
        kind: ConfigKind,
        matches: impl Fn(&ConfigRecord) -> bool,
    ) -> Result<Option<ConfigRecord>, DatabaseError> {
        let mut before = None;
        loop {
            let page = self.configs(Some(kind), before, 1_000).await?;
            if let Some(record) = page.records.into_iter().find(&matches) {
                return Ok(Some(record));
            }
            before = page.next;
            if before.is_none() {
                return Ok(None);
            }
        }
    }

    pub(crate) async fn profile_page(
        &self,
        before: Option<ConfigId>,
        limit: usize,
        scan: usize,
    ) -> Result<crate::db::entities::ProfilePageRecords, DatabaseError> {
        let key = self.shared.credential_key();
        self.read(move |database| {
            redb::read_profile_page(database, key.as_deref(), before, limit, scan)
        })
        .await
    }

    pub async fn create_config(&self, config: Config) -> Result<ConfigId, DatabaseError> {
        self.write_sized(
            config,
            config_bytes,
            OperationKind::CreateProfile,
            mutations::create_config,
        )
        .await
    }

    pub(crate) async fn set_config_enabled(
        &self,
        id: ConfigId,
        kind: crate::db::entities::ConfigKind,
        enabled: bool,
    ) -> Result<(), DatabaseError> {
        self.write(
            32,
            OperationKind::UpdateProfile,
            (id, kind, enabled),
            |b, (id, k, en)| b.set_config_enabled(id, k, en),
        )
        .await
    }

    pub(crate) async fn remove_provider(&self, id: ConfigId) -> Result<(), DatabaseError> {
        self.write(16, OperationKind::UpdateProfile, id, |b, id| {
            b.remove_provider(id)
        })
        .await
    }

    pub(crate) async fn save_model_profile(
        &self,
        mut save: mutations::ModelProfileSave,
    ) -> Result<(ConfigId, Option<String>), DatabaseError> {
        if let Some(secret) = save.secret.clone() {
            let key = self.ensure_credential_key().await?;
            save.credential = Some(mutations::CredentialPayload { secret, key });
        }
        let kind = if save.existing_id.is_some() {
            OperationKind::UpdateProfile
        } else {
            OperationKind::CreateProfile
        };
        self.write_sized(save, model_profile_save_bytes, kind, |b, s| {
            mutations::save_model_profile(b, s)
        })
        .await
    }

    pub async fn refresh_model_metadata(
        &self,
        connection: crate::db::entities::ConnectionConfig,
        models: Vec<crate::db::entities::DiscoveredModel>,
    ) -> Result<(), DatabaseError> {
        self.write_sized(
            (connection, models),
            |(c, m)| {
                text_bytes(c.endpoint.as_deref())
                    .saturating_add(text_bytes(c.credential_ref.as_deref()))
                    .saturating_add(
                        m.iter()
                            .map(|model| byte_count(model.id.len()).saturating_add(512))
                            .sum::<u64>(),
                    )
            },
            OperationKind::UpdateProfile,
            |b, (c, m)| b.refresh_model_metadata(c, m),
        )
        .await
    }

    pub(crate) async fn import_models(
        &self,
        mut save: mutations::ImportModelsSave,
    ) -> Result<(Vec<ConfigId>, usize), DatabaseError> {
        if let Some(secret) = save.secret.clone() {
            let key = self.ensure_credential_key().await?;
            save.credential = Some(mutations::CredentialPayload { secret, key });
        }
        self.write_sized(
            save,
            |save| {
                let ref_bytes = match &save.credential_ref {
                    mutations::ProfileCredentialRef::Preserve => match &save.connection.body {
                        ConfigBody::Connection(body) => text_bytes(body.credential_ref.as_deref()),
                        _ => 0,
                    },
                    mutations::ProfileCredentialRef::Clear => 0,
                    mutations::ProfileCredentialRef::Explicit(r)
                    | mutations::ProfileCredentialRef::PreserveOrGenerate(r) => byte_count(r.len()),
                };
                let endpoint_bytes = match &save.connection.body {
                    ConfigBody::Connection(body) => text_bytes(body.endpoint.as_deref()),
                    _ => 0,
                };
                let secret_bytes = save
                    .secret
                    .as_ref()
                    .map_or(0, |s| byte_count(s.as_str().len()));
                let base = endpoint_bytes
                    .saturating_add(ref_bytes)
                    .saturating_add(header_change_bytes(&save.headers))
                    .saturating_add(secret_bytes)
                    .saturating_add(byte_count(save.connection.name.len()));
                save.models.iter().fold(base, |bytes, model| {
                    bytes
                        .saturating_add(byte_count(model.id.len()))
                        .saturating_add(528)
                })
            },
            OperationKind::CreateProfile,
            mutations::import_models,
        )
        .await
    }

    pub async fn set_global_outbound_headers(
        &self,
        expected_revision: u32,
        fields: Vec<crate::db::entities::OutboundHeader>,
    ) -> Result<crate::db::entities::GlobalHeaders, DatabaseError> {
        self.write_sized(
            (expected_revision, fields),
            |(_, fields)| {
                byte_count(fields.len())
                    .saturating_mul(512)
                    .saturating_add(4096)
            },
            OperationKind::UpdateProfile,
            |b, (rev, fields)| b.save_global_headers(rev, fields),
        )
        .await
    }

    pub(crate) async fn credential_payload(
        &self,
        secret: crate::db::entities::CredentialSecret,
    ) -> Result<mutations::CredentialPayload, DatabaseError> {
        let key = self.ensure_credential_key().await?;
        Ok(mutations::CredentialPayload { secret, key })
    }

    pub async fn set_global_compaction_settings(
        &self,
        settings: crate::db::entities::GlobalCompactionSettings,
    ) -> Result<(), DatabaseError> {
        self.write_sized(
            settings,
            |s| compaction_bytes(Some(&s.policy)).saturating_add(64),
            OperationKind::UpdateProfile,
            |b, s| b.set_global_compaction_settings(s),
        )
        .await
    }

    pub async fn archive_model(&self, id: ConfigId) -> Result<(), DatabaseError> {
        self.write(16, OperationKind::UpdateProfile, id, |b, id| {
            mutations::archive_model(b, id)
        })
        .await
    }

    pub async fn update_config(
        &self,
        id: ConfigId,
        config: Config,
        expected_edit_seq: u32,
    ) -> Result<(), DatabaseError> {
        self.write_sized(
            (id, config, expected_edit_seq),
            |(_, c, _)| config_bytes(c),
            OperationKind::UpdateProfile,
            |b, (id, c, exp)| mutations::update_config(b, id, c, exp),
        )
        .await
    }

    pub async fn create_project(&self, project: Project) -> Result<ProjectId, DatabaseError> {
        self.write_sized(
            project,
            |project| {
                byte_count(project.name.len())
                    .saturating_add(text_bytes(project.description.as_deref()))
                    .saturating_add(byte_count(project.root.len()))
                    .saturating_add(command_bytes(&project.commands))
            },
            OperationKind::CreateProject,
            mutations::create_project,
        )
        .await
    }

    pub(crate) async fn configure_project_decision(
        &self,
        id: ProjectId,
        expected: crate::db::entities::Timestamp,
        assignment: Option<entities::ProjectDecisionConfig>,
    ) -> Result<entities::ProjectRecord, DatabaseError> {
        self.write(
            32 * 1024,
            OperationKind::UpdateProject,
            (id, expected, assignment),
            |batch, (id, expected, assignment)| {
                mutations::configure_project_decision(batch, id, expected, assignment)
            },
        )
        .await
    }

    pub async fn update_project(
        &self,
        id: ProjectId,
        name: String,
        description: Option<String>,
        commands: Vec<ProjectCommand>,
    ) -> Result<(), DatabaseError> {
        self.write_sized(
            (id, name, description, commands),
            |(_, name, description, commands)| {
                byte_count(name.len())
                    .saturating_add(text_bytes(description.as_deref()))
                    .saturating_add(command_bytes(commands))
            },
            OperationKind::UpdateProject,
            |batch, (id, name, description, commands)| {
                mutations::update_project(batch, id, name, description, commands)
            },
        )
        .await
    }

    pub async fn create_user_attachment(
        &self,
        attachment: Attachment,
    ) -> Result<AttachmentId, AttachmentError> {
        mutations::check_new_attachment(&attachment).map_err(DatabaseError::Rejected)?;
        admitted(
            &self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )?;
        let permit = self.attachment_permit().await?;
        let (handoff_tx, handoff_rx) = flume::bounded(1);
        let (settle_tx, settle_rx) = flume::bounded(1);
        let submitted = Arc::new(AtomicBool::new(false));
        let submitted_for_worker = submitted.clone();
        let shared = self.shared.clone();
        let blocking = ntex::rt::spawn_blocking(move || {
            let pin = match shared.attachments.publish(&attachment.content) {
                Ok(pin) => pin,
                Err(error) => {
                    let _ = handoff_tx.send(Err(AttachmentError::NotStored(error)));
                    drop(permit);
                    return;
                }
            };
            let published = PublishedAttachment {
                sha256: pin.digest(),
                byte_length: attachment.content.len() as u64,
                mime: attachment.mime,
                display_name: attachment.display_name,
                origin: AttachmentOrigin::UserProvided,
            };
            if handoff_tx.send(Ok(published)).is_err() {
                pin.release();
                drop(permit);
                return;
            }
            match settle_rx.recv() {
                Ok(true) => {
                    pin.release();
                }
                Ok(false) => {}
                Err(_) => {
                    if !submitted_for_worker.load(Ordering::Acquire) {
                        pin.release();
                    }
                }
            }
            drop(permit);
        });
        let published = match handoff_rx.recv_async().await {
            Ok(Ok(published)) => published,
            Ok(Err(error)) => {
                let _ = blocking.await;
                return Err(error);
            }
            Err(_) => {
                let _ = blocking.await;
                return Err(DatabaseError::WorkerPanicked.into());
            }
        };
        let created = self
            .write_sized(
                Box::new(published),
                |attachment| {
                    byte_count(attachment.mime.len())
                        .saturating_add(text_bytes(attachment.display_name.as_deref()))
                },
                OperationKind::CreateAttachment,
                move |batch, attachment| {
                    submitted.store(true, Ordering::Release);
                    mutations::create_attachment(batch, *attachment)
                },
            )
            .await;
        let settled = match &created {
            Ok(_) => true,
            Err(error) => matches!(
                error.command_outcome(),
                Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted)
            ),
        };
        let _ = settle_tx.send(settled);
        let _ = blocking.await;
        Ok(created?)
    }

    pub async fn attachment(
        &self,
        id: AttachmentId,
    ) -> Result<Option<AttachmentRecord>, DatabaseError> {
        self.read(move |database| redb::read_attachment(database, id))
            .await
    }

    pub async fn attachments(
        &self,
        before: Option<AttachmentId>,
        limit: usize,
    ) -> Result<Vec<AttachmentRecord>, DatabaseError> {
        self.read(move |database| redb::read_attachments(database, before, limit))
            .await
    }

    pub async fn attachment_content(
        &self,
        id: AttachmentId,
    ) -> Result<AttachmentContent, AttachmentError> {
        let record = self
            .attachment(id)
            .await?
            .ok_or(AttachmentError::NotFound(id))?;
        if let Some(cleanup) = record.cleanup {
            return Err(AttachmentError::CleanedUp { id, cleanup });
        }
        let permit = self.attachment_permit().await?;
        let shared = self.shared.clone();
        let digest = record.sha256;
        let length = record.byte_length;
        let loaded = ntex::rt::spawn_blocking(move || {
            let result = shared.attachments.load(digest, length);
            drop(permit);
            result
        })
        .await
        .map_err(|_| DatabaseError::WorkerPanicked)?;
        let bytes = match loaded {
            Ok(bytes) => bytes,
            Err(StoreError::Missing) => {
                return Err(match self.attachment(id).await?.and_then(|r| r.cleanup) {
                    Some(cleanup) => AttachmentError::CleanedUp { id, cleanup },
                    None => AttachmentError::Content {
                        id,
                        error: StoreError::Missing,
                    },
                });
            }
            Err(error) => return Err(AttachmentError::Content { id, error }),
        };
        Ok(AttachmentContent { record, bytes })
    }

    async fn ensure_credential_key(&self) -> Result<Arc<redb::CredentialKey>, DatabaseError> {
        if let Some(key) = self.shared.credential_key() {
            return Ok(key);
        }
        {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admitted(&admission)?;
            if self.shared.update_paused.load(Ordering::SeqCst) {
                return Err(DatabaseError::ShuttingDown);
            }
        }
        let shared = self.shared.clone();
        ntex::rt::spawn_blocking(move || shared.ensure_credential_key())
            .await
            .map_err(|_| DatabaseError::WorkerPanicked)?
            .map_err(DatabaseError::RolledBack)
    }

    pub(crate) async fn resolve_credential(
        &self,
        reference: &str,
    ) -> Result<Option<String>, crate::service::ai::providers::ProviderExecutionError> {
        use crate::service::ai::providers::{
            CredentialResolver, EnvironmentCredentialResolver, ProviderExecutionError,
        };
        let reference = reference.trim();
        if reference.is_empty() {
            return Ok(None);
        }
        let name = reference.to_owned();
        let key = self.shared.credential_key();
        let stored = self
            .read(move |database| redb::read_credential(database, &name, key.as_deref()))
            .await
            .map_err(|error| ProviderExecutionError::Build {
                message: format!("credential store could not be read: {error}"),
            })?;
        match stored {
            redb::StoredCredential::Decrypted(secret) => Ok(Some(secret.into_inner())),
            redb::StoredCredential::Undecryptable => Err(ProviderExecutionError::Build {
                message: "stored credential cannot be decrypted; enter the key again".to_owned(),
            }),
            redb::StoredCredential::Absent => {
                EnvironmentCredentialResolver.resolve(Some(reference)).await
            }
        }
    }

    pub fn credential_resolver(
        &self,
    ) -> Arc<dyn crate::service::ai::providers::CredentialResolver> {
        Arc::new(LocalCredentialResolver {
            database: self.clone(),
        })
    }
}

struct LocalCredentialResolver {
    database: DatabaseHandle,
}

impl crate::service::ai::providers::CredentialResolver for LocalCredentialResolver {
    fn resolve<'a>(
        &'a self,
        credential_ref: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Option<String>,
                        crate::service::ai::providers::ProviderExecutionError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(name) = credential_ref else {
                return Ok(None);
            };
            self.database.resolve_credential(name).await
        })
    }
}
