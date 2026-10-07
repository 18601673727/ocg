#[path = "smoke/client.rs"]
mod client;
#[path = "smoke/hardening.rs"]
mod hardening;
#[path = "smoke/harness.rs"]
mod harness;
#[path = "smoke/provider.rs"]
mod provider;

use harness::{array, string, Result, SmokeHarness};
use provider::Reply;
use serde_json::json;

#[test]
fn failed_chat_retry_reexecutes_the_same_job_and_turn() -> Result<()> {
    let mut smoke = SmokeHarness::start(Reply::Text(vec![]))?;
    smoke.configure_provider()?;
    let project = smoke.add_project("retry-project")?;
    let project_id = string(&project, "project_id")?;
    let session_id = smoke.create_untitled_session(&project_id)?;
    let original = smoke.send_chat(&project_id, &session_id, "Review this project")?;
    assert!(smoke.consume_chat(&original).is_err());
    smoke.assert_terminal_execution(&original, "failed")?;
    let messages_path =
        format!("/api/v1/canonical/chat/messages?project_id={project_id}&session_id={session_id}");
    let messages = smoke.json("GET", &messages_path, None)?;
    let failed = array(&messages, "messages")?
        .iter()
        .find(|item| item["role"] == "assistant")
        .ok_or("assistant missing")?;
    let message_id = string(failed, "message_id")?;
    smoke.provider_reply(Reply::Text(vec!["Review completed".into()]))?;
    let retry = smoke.retry_failed_turn(&original, &message_id)?;
    assert_eq!(original.job_id, retry.job_id);
    assert_eq!(smoke.consume_chat(&retry)?, "Review completed");
    let snapshot = smoke.assert_terminal_execution(&retry, "completed")?;
    let execution = &snapshot["job"];
    assert_eq!(execution["job"]["generation"], 2);
    let attempts = array(execution, "attempts")?;
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0]["id"], attempts[1]["id"]);
    assert!(attempts
        .iter()
        .any(|attempt| attempt["generation"] == 1 && attempt["state"] == "failed"));
    // The replacement republishes the frozen target: no new Placement.
    let target = |intent: &serde_json::Value| -> Result<serde_json::Value> {
        let request: serde_json::Value = serde_json::from_str(&string(intent, "request")?)?;
        Ok(json!([
            intent["provider_key"],
            intent["model"],
            intent["upstream_model_id"],
            intent["endpoint"],
            request["arguments"]["reasoning_effort"],
        ]))
    };
    let provider_intents = array(execution, "dispatch_intents")?
        .iter()
        .filter(|intent| !intent["provider_key"].is_null())
        .collect::<Vec<_>>();
    let first = provider_intents
        .iter()
        .find(|intent| intent["generation"] == 1)
        .ok_or("original provider intent missing")?;
    let replacement = provider_intents
        .iter()
        .find(|intent| intent["generation"] == 2)
        .ok_or("replacement provider intent missing")?;
    assert_eq!(target(first)?, target(replacement)?);
    let messages = smoke.json("GET", &messages_path, None)?;
    let messages = array(&messages, "messages")?;
    assert_eq!(messages.len(), 2, "retry must not add a Chat turn");
    let assistant = messages
        .iter()
        .find(|item| item["role"] == "assistant")
        .ok_or("assistant missing")?;
    assert_eq!(assistant["message_id"], message_id.as_str());
    assert_eq!(assistant["job_id"], original.job_id.as_str());
    assert_eq!(assistant["state"], "complete");
    assert_eq!(assistant["content"], "Review completed");
    assert_eq!(assistant["attempt_state"], "completed");
    smoke.finish()
}

#[test]
fn onboarding_same_session_two_chat_turns() -> Result<()> {
    let mut smoke = SmokeHarness::start(Reply::Text(vec![
        "SMOKE_".into(),
        "TURN_".into(),
        "OK".into(),
    ]))?;
    smoke.configure_provider()?;
    let browse = smoke.json(
        "POST",
        "/api/v1/setup/browse",
        Some(&json!({"path": smoke.path("")})),
    )?;
    assert!(!array(&browse, "entries")?.is_empty());
    let project = smoke.add_project("chat-project")?;
    let project_id = string(&project, "project_id")?;
    let projects = smoke.json("GET", "/api/v1/canonical/projects", None)?;
    assert!(array(&projects, "projects")?
        .iter()
        .any(|project| project["project_id"] == project_id));
    let session_id = smoke.create_untitled_session(&project_id)?;

    for turn in ["ONE", "TWO"] {
        let expected = format!("SMOKE_TURN_{turn}_OK");
        smoke.provider_reply(Reply::Text(vec![
            "SMOKE_".into(),
            format!("TURN_{turn}"),
            "_OK".into(),
        ]))?;
        let chat = smoke.send_chat(
            &project_id,
            &session_id,
            &format!("Reply with exactly: {expected}"),
        )?;
        assert_eq!(smoke.consume_chat(&chat)?, expected);
        smoke.assert_terminal_execution(&chat, "completed")?;
    }
    let messages = smoke.json(
        "GET",
        &format!("/api/v1/canonical/chat/messages?project_id={project_id}&session_id={session_id}"),
        None,
    )?;
    assert_eq!(
        array(&messages, "messages")?
            .iter()
            .filter(|message| message["role"] == "assistant")
            .count(),
        2
    );
    smoke.finish()
}

#[test]
fn usage_routes_and_frontend_contract_cover_entire_conversation() -> Result<()> {
    let mut smoke = SmokeHarness::start(Reply::Text(vec!["ready".into()]))?;
    smoke.configure_provider()?;
    let project = smoke.add_project("usage-project")?;
    let project_id = string(&project, "project_id")?;
    let session = smoke.create_untitled_session(&project_id)?;
    for (n, reply) in [
        Reply::Text(vec!["ready".into()]),
        Reply::NativePwd,
        Reply::Text(vec!["done".into()]),
    ]
    .into_iter()
    .enumerate()
    {
        smoke.provider_reply(reply)?;
        let chat = smoke.send_chat(&project_id, &session, &format!("turn-{n}"))?;
        smoke.consume_chat(&chat)?;
        smoke.assert_terminal_execution(&chat, "completed")?;
        let usage = smoke.json(
            "GET",
            &format!(
                "/api/v1/canonical/jobs/usage?project_id={project_id}&job_id={}",
                chat.job_id
            ),
            None,
        )?;
        assert_eq!(usage["totals"]["provider_calls"], 1);
    }
    for window in ["today", "7d", "30d", "all"] {
        let usage = smoke.json(
            "GET",
            &format!("/api/v1/canonical/usage?project_id={project_id}&window={window}"),
            None,
        )?;
        assert_eq!(usage["window"], window);
        assert_eq!(usage["totals"]["turns"], 3);
        assert_eq!(usage["totals"]["provider_requests"]["value"], 4);
        assert_eq!(
            usage["totals"]["cost"]["actual_micros"],
            serde_json::Value::Null
        );
    }
    smoke.verify_usage_contract(&project_id, &session)?;
    smoke.finish()
}

#[test]
fn failed_and_retried_chat_usage_preserves_both_provider_calls() -> Result<()> {
    let mut smoke = SmokeHarness::start(Reply::Text(vec![]))?;
    smoke.configure_provider()?;
    let project = smoke.add_project("usage-retry")?;
    let project_id = string(&project, "project_id")?;
    let session = smoke.create_untitled_session(&project_id)?;
    let failed = smoke.send_chat(&project_id, &session, "fail safely")?;
    assert!(smoke.consume_chat(&failed).is_err());
    smoke.assert_terminal_execution(&failed, "failed")?;
    smoke.provider_reply(Reply::Text(vec!["recovered".into()]))?;
    let retried = smoke.send_chat(&project_id, &session, "retry safely")?;
    smoke.consume_chat(&retried)?;
    smoke.assert_terminal_execution(&retried, "completed")?;
    let usage = smoke.json(
        "GET",
        &format!("/api/v1/canonical/chat/usage?project_id={project_id}&session_id={session}"),
        None,
    )?;
    assert_eq!(usage["totals"]["jobs"], 2);
    assert_eq!(usage["totals"]["provider_calls"], 2);
    assert_eq!(usage["totals"]["provider_requests"]["value"], 2);
    assert_eq!(
        usage["totals"]["cost"]["actual_micros"],
        serde_json::Value::Null
    );
    smoke.finish()
}

#[test]
fn chat_presentation_contract() -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let output = std::process::Command::new("node")
        .current_dir(root)
        .arg(root.join("tests/smoke/chat-presentation.cjs"))
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "chat presentation contract: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}
