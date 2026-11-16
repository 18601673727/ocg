use super::StorageError;
use crate::db::entities::{
    CursorPage, FrozenConfiguration, FrozenExecutionConfig, JobId, ScheduledExternalPolicyId,
    Violation,
};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::codec::{
    decode_scheduled_external_binding, decode_scheduled_external_policy,
    encode_scheduled_external_policy,
};
use crate::db::redb::reads::read;
use crate::db::redb::schema::{MAX_PAGE, SCHEDULED_EXTERNAL_BINDING, SCHEDULED_EXTERNAL_POLICY};
use ::redb::{Database, ReadableTable};

pub(super) enum ScheduledPolicyMatch {
    None,
    One {
        id: ScheduledExternalPolicyId,
        revision: u32,
    },
    Ambiguous,
}

impl Batch {
    fn scheduled_external_policy_by_id(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
    ) -> Result<Option<crate::db::entities::ScheduledExternalPolicyRecord>, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
            table
                .get(id.get())?
                .map(|row| decode_scheduled_external_policy(id.get(), row.value()))
                .transpose()
        })
    }

    fn scheduled_external_binding_by_job(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::ScheduledExternalBinding>, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(SCHEDULED_EXTERNAL_BINDING)?;
            table
                .get(job.get())?
                .map(|row| decode_scheduled_external_binding(job.get(), row.value()))
                .transpose()
        })
    }

    pub(crate) fn create_scheduled_external_policy(
        &mut self,
        draft: &crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        use crate::db::entities::{ScheduledExternalPolicyId, ScheduledExternalPolicyRecord};
        let id = ScheduledExternalPolicyId::new(self.next_id()?);
        let now = self.now;
        let record = ScheduledExternalPolicyRecord {
            id,
            endpoint: draft.endpoint().to_owned(),
            credential_ref: draft.credential_ref().to_owned(),
            model: draft.model().to_owned(),
            proxy: draft.proxy().to_owned(),
            max_request_bytes: u32::try_from(draft.limits().max_request_bytes)
                .map_err(|_| self.invalid("scheduled_external_policy"))?,
            max_output_tokens: draft.limits().max_output_tokens,
            timeout_secs: u32::try_from(draft.limits().timeout.as_secs())
                .map_err(|_| self.invalid("scheduled_external_policy"))?,
            enabled: false,
            revoked: false,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let bytes = encode_scheduled_external_policy(&record);
        self.step(|tx| {
            tx.open_table(SCHEDULED_EXTERNAL_POLICY)?
                .insert(id.get(), bytes.as_slice())?;
            Ok(())
        })?;
        Ok(record)
    }

    fn mutate_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        reject_revoked: Option<&'static str>,
        apply: impl FnOnce(
            &mut Self,
            crate::db::entities::ScheduledExternalPolicyRecord,
        ) -> Result<
            Option<crate::db::entities::ScheduledExternalPolicyRecord>,
            BatchError,
        >,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        let Some(record) = self.scheduled_external_policy_by_id(id)? else {
            return self.reject(Violation::NotFound(
                crate::db::entities::RecordRef::ScheduledExternalPolicy(id),
            ));
        };
        if record.revoked
            && let Some(msg) = reject_revoked
        {
            return self.reject(Violation::InvalidValue("scheduled_external_policy", msg));
        }
        if record.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the policy revision changed; reload before saving",
            ));
        }
        let Some(mut updated) = apply(self, record.clone())? else {
            return Ok(record);
        };
        updated.revision = record.revision.checked_add(1).ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "scheduled external policy revision exhausted",
            )))
        })?;
        updated.updated_at = self.now;
        let bytes = encode_scheduled_external_policy(&updated);
        self.step(|tx| {
            tx.open_table(SCHEDULED_EXTERNAL_POLICY)?
                .insert(id.get(), bytes.as_slice())?;
            Ok(())
        })?;
        Ok(updated)
    }

    pub(crate) fn update_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        draft: &crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        self.mutate_scheduled_external_policy(
            id,
            expected_revision,
            Some("a revoked policy is immutable; create a new one"),
            |batch, record| {
                Ok(Some(crate::db::entities::ScheduledExternalPolicyRecord {
                    endpoint: draft.endpoint().to_owned(),
                    credential_ref: draft.credential_ref().to_owned(),
                    model: draft.model().to_owned(),
                    proxy: draft.proxy().to_owned(),
                    max_request_bytes: u32::try_from(draft.limits().max_request_bytes)
                        .map_err(|_| batch.invalid("scheduled_external_policy"))?,
                    max_output_tokens: draft.limits().max_output_tokens,
                    timeout_secs: u32::try_from(draft.limits().timeout.as_secs())
                        .map_err(|_| batch.invalid("scheduled_external_policy"))?,
                    ..record
                }))
            },
        )
    }

    pub(crate) fn set_scheduled_external_policy_enabled(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        enabled: bool,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        self.mutate_scheduled_external_policy(
            id,
            expected_revision,
            Some("a revoked policy cannot be enabled"),
            |_, record| {
                Ok((record.enabled != enabled).then_some(
                    crate::db::entities::ScheduledExternalPolicyRecord { enabled, ..record },
                ))
            },
        )
    }

    pub(crate) fn revoke_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        self.mutate_scheduled_external_policy(id, expected_revision, None, |_, record| {
            Ok(
                (!record.revoked).then_some(crate::db::entities::ScheduledExternalPolicyRecord {
                    enabled: false,
                    revoked: true,
                    ..record
                }),
            )
        })
    }

    pub(super) fn scheduled_external_policy_for(
        &mut self,
        frozen: &FrozenExecutionConfig,
    ) -> Result<ScheduledPolicyMatch, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
            let mut matching = ScheduledPolicyMatch::None;
            for entry in table.iter()? {
                let (key, row) = entry?;
                let record = decode_scheduled_external_policy(key.value(), row.value())?;
                if record.is_live()
                    && crate::system::scheduled_external::record_admits(&record, frozen)
                {
                    matching = match matching {
                        ScheduledPolicyMatch::None => ScheduledPolicyMatch::One {
                            id: record.id,
                            revision: record.revision,
                        },
                        ScheduledPolicyMatch::One { .. } | ScheduledPolicyMatch::Ambiguous => {
                            ScheduledPolicyMatch::Ambiguous
                        }
                    };
                }
            }
            Ok(matching)
        })
    }

    pub(crate) fn scheduled_external_authorization(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::ScheduledExternalPolicyRecord>, BatchError> {
        let Some(binding) = self.scheduled_external_binding_by_job(job)? else {
            return Ok(None);
        };
        let Some(record) = self.scheduled_external_policy_by_id(binding.policy)? else {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the bound policy no longer exists",
            ));
        };
        if !record.is_live() || record.revision != binding.policy_revision {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the bound policy is revoked, disabled or changed since reservation",
            ));
        }
        let allocation = self.pool_allocation(job)?;
        if allocation.is_none_or(|allocation| {
            allocation.token.epoch != binding.allocation_epoch
                || allocation.token.generation != binding.allocation_generation
        }) {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the binding does not name this Allocation",
            ));
        }
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(crate::db::entities::RecordRef::Job(
                job,
            )));
        };
        let frozen = self.frozen_configuration(&row)?;
        let FrozenConfiguration::Generative(frozen) = &frozen else {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "a scheduled external Job must be Generative",
            ));
        };
        if !crate::system::scheduled_external::record_admits(&record, frozen) {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the Job's frozen configuration no longer matches the bound policy",
            ));
        }
        Ok(Some(record))
    }

    fn invalid(&self, field: &'static str) -> BatchError {
        BatchError(Failure::Rejected(Violation::InvalidValue(
            field,
            "value out of range",
        )))
    }
}

pub(crate) fn read_scheduled_external_policy(
    database: &Database,
    id: crate::db::entities::ScheduledExternalPolicyId,
) -> Result<Option<crate::db::entities::ScheduledExternalPolicyRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
    table
        .get(id.get())?
        .map(|row| decode_scheduled_external_policy(id.get(), row.value()))
        .transpose()
}

pub(crate) fn read_scheduled_external_policies(
    database: &Database,
    after: Option<crate::db::entities::ScheduledExternalPolicyId>,
    limit: usize,
) -> Result<
    CursorPage<
        crate::db::entities::ScheduledExternalPolicyRecord,
        crate::db::entities::ScheduledExternalPolicyId,
    >,
    StorageError,
> {
    let rx = read(database)?;
    let table = rx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut records = Vec::new();
    let mut more = false;
    for entry in table.range((
        std::ops::Bound::Excluded(after.map_or(0, |id| id.get())),
        std::ops::Bound::Unbounded,
    ))? {
        if records.len() == limit {
            more = true;
            break;
        }
        let (key, row) = entry?;
        records.push(decode_scheduled_external_policy(key.value(), row.value())?);
    }
    let next = records.last().map(|record| record.id).filter(|_| more);
    Ok(CursorPage { records, next })
}

pub(crate) fn read_scheduled_external_binding_with_policy(
    database: &Database,
    job: JobId,
) -> Result<
    Option<(
        crate::db::entities::ScheduledExternalBinding,
        crate::db::entities::ScheduledExternalPolicyRecord,
    )>,
    StorageError,
> {
    let rx = read(database)?;
    let Some(row) = rx.open_table(SCHEDULED_EXTERNAL_BINDING)?.get(job.get())? else {
        return Ok(None);
    };
    let binding = decode_scheduled_external_binding(job.get(), row.value())?;
    let Some(policy_row) = rx
        .open_table(SCHEDULED_EXTERNAL_POLICY)?
        .get(binding.policy.get())?
    else {
        return Err(StorageError::corrupted(
            "a scheduled external binding names a missing policy",
        ));
    };
    let policy = decode_scheduled_external_policy(binding.policy.get(), policy_row.value())?;
    Ok(Some((binding, policy)))
}
