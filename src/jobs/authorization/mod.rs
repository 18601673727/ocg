pub(crate) mod scheduled;

use crate::ai::development::{DevelopmentResolvedCandidate, DevelopmentToolOffer};
use crate::db::DatabaseError;
use crate::db::entities::Violation;

pub(crate) fn authorize_development_candidate(
    resolved: &DevelopmentResolvedCandidate,
    offer: Option<&DevelopmentToolOffer>,
) -> Result<(), DatabaseError> {
    let unavailable = || {
        DatabaseError::Rejected(Violation::InvalidValue(
            "handoff",
            "execution context or policy cannot be safely prepared",
        ))
    };
    match resolved {
        DevelopmentResolvedCandidate::Decision(context) => {
            if context.conversation.parent_id.is_some() || offer.is_some() {
                return Err(unavailable());
            }
        }
        DevelopmentResolvedCandidate::Generative(context) => {
            let snapshot = &context.snapshot;
            let frozen = &snapshot.configuration;
            let is_summary = snapshot.is_summary_work();
            let granted = !is_summary
                && frozen
                    .workspace_enabled(snapshot.project.is_some())
                    .map_err(|_| unavailable())?;
            let manages = frozen
                .agent
                .as_ref()
                .is_some_and(|agent| agent.manages_jobs());
            if is_summary {
                if frozen.provider_refusal().is_some() || offer.is_some() {
                    return Err(unavailable());
                }
            } else if frozen.native_execution_refusal().is_some()
                || (granted && offer.is_none_or(|offer| offer.grant.is_none()))
                || (!granted && offer.is_some() && !manages)
                || offer.is_some_and(|offer| offer.wires.is_empty())
            {
                return Err(unavailable());
            }
        }
    }
    Ok(())
}
