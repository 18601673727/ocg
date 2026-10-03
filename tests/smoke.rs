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
