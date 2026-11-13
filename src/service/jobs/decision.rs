use super::ConfigurationResolver;
use crate::db::entities::*;
use crate::db::{Batch, BatchError};

impl Batch {
    pub(crate) fn freeze_decision_profile(
        &mut self,
        profile: ConfigId,
    ) -> Result<FrozenDecisionConfig, BatchError> {
        let global = self.global_headers()?;
        ConfigurationResolver::new(|id| self.config_record(id))
            .with_global_headers(global)
            .freeze_decision_profile(profile)
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
