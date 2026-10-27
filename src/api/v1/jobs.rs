use super::*;

/// The Job's settlement: the one outcome its record holds, or none yet; a
/// record holding more than one is not described at all.
pub(super) fn settlement_view(record: &JobRecord) -> Result<serde_json::Value, Refusal> {
    let outcomes = [
        (serde_json::json!("completed"), record.completed_at),
        (serde_json::json!("failed"), record.failed_at),
        (serde_json::json!("cancelled"), record.cancelled_at),
        (serde_json::json!("unknown"), record.unknown_at),
    ]
    .into_iter()
    .filter_map(|(state, at)| at.map(|at| (state, at)))
    .collect::<Vec<_>>();
    let mut outcomes = outcomes.into_iter();
    let value = match outcomes.next() {
        None => serde_json::json!({
            "state": serde_json::json!("unsettled"),
            "settled_at_us": serde_json::Value::Null,
        }),
        Some((state, at)) => serde_json::json!({
            "state": state,
            "settled_at_us": Some(at.as_unix_micros()),
        }),
    };
    if outcomes.next().is_some() {
        return Err(INCONSISTENT_RECORD);
    }
    Ok(value)
}

pub(super) fn job_view(record: &JobRecord) -> Result<serde_json::Value, Refusal> {
    Ok({
        let mut object = serde_json::json!({
            "id": record.id.to_string(),
            "name": record.name.clone(),
            "title": record.objective.clone(),
            "is_chat": record.group.is_some(),
            "execution_mode": "once".to_owned(),
            "input": record.input.to_string(),
            "previous": record.previous.map(|id| id.to_string()),
            "kind": if record.kind.is_group() {
                serde_json::json!("conversation")
            } else {
                serde_json::json!("turn")
            },
            "conversation": record.group.map(|id| id.to_string()),
            "assigned_executor": record.assigned_executor.map(|id| id.to_string()),
            "created_at_us": record.created_at.as_unix_micros(),
            "updated_at_us": record.updated_at.as_unix_micros(),
            "started_at_us": record.started_at.map(|at| at.as_unix_micros()),
            "paused_at_us": serde_json::Value::Null,
            "sterilized_at_us": record.children_closed_at.map(|at| at.as_unix_micros()),
            "settlement": settlement_view(record)?,
            "stop_confirmed_at_us": record.stop_confirmed_at.map(|at| at.as_unix_micros()),
        });
        if let Some(value) = record.parent_id.map(|id| id.to_string()) {
            object["parent"] = serde_json::json!(value);
        }
        object
    })
}
