use super::ConfigurationResolver;
use crate::db::entities::*;
use crate::db::{Batch, BatchError};

impl Batch {
    pub(crate) fn project_decision(
        &mut self,
        project: Option<ProjectId>,
        active: &FrozenExecutionConfig,
    ) -> Result<Option<(FrozenDecisionConfig, Option<DecisionAssignment>)>, BatchError> {
        let row = project
            .map(|id| self.require_project_record(id))
            .transpose()?;
        let policy = row
            .as_ref()
            .map(ProjectRecord::decision_policy)
            .unwrap_or_default();
        let Some(frozen) = ConfigurationResolver::new(|id| self.config_record(id))
            .project_decision(&policy, active)?
        else {
            return Ok(None);
        };
        if !self.decision_executor_available(&frozen)? {
            return Ok(None);
        }
        Ok(Some((
            frozen,
            row.map(|row| DecisionAssignment {
                project: row.id,
                revision: row.decision_revision,
            }),
        )))
    }

    pub(crate) fn freeze_decision_profile(
        &mut self,
        profile: ConfigId,
    ) -> Result<FrozenDecisionConfig, BatchError> {
        ConfigurationResolver::new(|id| self.config_record(id)).freeze_decision_profile(profile)
    }

    pub(crate) fn decision_assignment_current(
        &mut self,
        work: &DecisionWork,
    ) -> Result<bool, BatchError> {
        if let Some(assignment) = &work.assignment {
            let row = self.require_project_record(assignment.project)?;
            if !row.decision_policy().enabled || row.decision_revision != assignment.revision {
                return Ok(false);
            }
        }
        for reference in work.frozen.references() {
            if self
                .config(reference.id)?
                .is_none_or(|row| row.archived_at.is_some() || row.edit_seq != reference.edit_seq)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub(crate) fn bind_prediction(
    policy: &mut CompactionPolicy,
    decision: &FrozenDecisionConfig,
) -> PredictiveCompaction {
    let mut binding = policy.predictive.take().unwrap_or_else(|| {
        PredictiveCompaction::automatic(decision.profile_ref().id, decision.model_ref())
    });
    binding.decision_agent = decision.profile_ref().id;
    binding.mode = PredictionMode::Prepare;
    let acceptance = binding.acceptance.get_or_insert(PredictionAcceptance {
        model: decision.model_ref(),
        reported_model: String::new(),
        confidence_semantics: decision.confidence_semantics().into(),
        minimum_probability_millionths: 800_000,
        minimum_confidence_millionths: 800_000,
    });
    acceptance.model = decision.model_ref();
    acceptance.reported_model.clear();
    acceptance.confidence_semantics = decision.confidence_semantics().into();
    policy.predictive = Some(binding.clone());
    binding
}
