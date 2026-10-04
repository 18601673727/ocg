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
fn failed_chat_retry_creates_a_new_job_and_preserves_history() -> Result<()> {
    let mut smoke = SmokeHarness::start(Reply::Text(vec![]))?;
    smoke.configure_provider()?;
    let project = smoke.add_project("retry-project")?;
    let project_id = string(&project, "project_id")?;
    let session_id = smoke.create_untitled_session(&project_id)?;
    let original = smoke.send_chat(&project_id, &session_id, "Review this project")?;
    assert!(smoke.consume_chat(&original).is_err());
    smoke.assert_terminal_execution(&original, "failed")?;
    let messages = smoke.json("GET", &format!(
        "/api/v1/canonical/chat/messages?project_id={project_id}&session_id={session_id}"
    ), None)?;
    let failed = array(&messages, "messages")?.iter().find(|item| item["role"] == "assistant").ok_or("assistant missing")?;
    let message_id = string(failed, "message_id")?;
    smoke.provider_reply(Reply::Text(vec!["Review completed".into()]))?;
    let retry = smoke.retry_failed_turn(&original, &message_id)?;
    assert_ne!(original.job_id, retry.job_id);
    assert_eq!(smoke.consume_chat(&retry)?, "Review completed");
    smoke.assert_terminal_execution(&retry, "completed")?;
    smoke.assert_terminal_execution(&original, "failed")?;
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
        &format!(
            "/api/v1/canonical/chat/messages?project_id={project_id}&session_id={session_id}"
        ),
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
    for (n, reply) in [Reply::Text(vec!["ready".into()]), Reply::NativePwd, Reply::Text(vec!["done".into()])].into_iter().enumerate() {
        smoke.provider_reply(reply)?;
        let chat = smoke.send_chat(&project_id, &session, &format!("turn-{n}"))?;
        smoke.consume_chat(&chat)?;
        smoke.assert_terminal_execution(&chat, "completed")?;
        let usage = smoke.json("GET", &format!("/api/v1/canonical/jobs/usage?project_id={project_id}&job_id={}", chat.job_id), None)?;
        assert_eq!(usage["totals"]["provider_calls"], 1);
    }
    for window in ["today", "7d", "30d", "all"] {
        let usage = smoke.json("GET", &format!("/api/v1/canonical/usage?project_id={project_id}&window={window}"), None)?;
        assert_eq!(usage["window"], window);
        assert_eq!(usage["totals"]["turns"], 3);
        assert_eq!(usage["totals"]["provider_requests"]["value"], 4);
        assert_eq!(usage["totals"]["cost"]["actual_micros"], serde_json::Value::Null);
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
    let usage = smoke.json("GET", &format!("/api/v1/canonical/chat/usage?project_id={project_id}&session_id={session}"), None)?;
    assert_eq!(usage["totals"]["jobs"], 2);
    assert_eq!(usage["totals"]["provider_calls"], 2);
    assert_eq!(usage["totals"]["provider_requests"]["value"], 2);
    assert_eq!(usage["totals"]["cost"]["actual_micros"], serde_json::Value::Null);
    smoke.finish()
}
