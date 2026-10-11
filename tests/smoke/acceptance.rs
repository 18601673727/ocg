//! Job-only-core acceptance behaviors, exercised against a running OCG.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rusqlite::Connection;
use serde_json::{json, Value};

use super::harness::{array, string, Chat, Result, SmokeHarness};
use super::provider::Reply;

/// A configured OCG with one Project and one Chat session.
fn session(name: &str) -> Result<(SmokeHarness, String, String)> {
    let mut smoke = SmokeHarness::start(Reply::Text(vec![]))?;
    smoke.configure_provider()?;
    let project = smoke.add_project(name)?;
    let project_id = string(&project, "project_id")?;
    let session_id = smoke.create_untitled_session(&project_id)?;
    Ok((smoke, project_id, session_id))
}

/// A Chat turn whose provider request is in flight and never answers. No
/// stream is ever attached to it.
fn hanging_turn(name: &str) -> Result<(SmokeHarness, Chat)> {
    let (mut smoke, project_id, session_id) = session(name)?;
    smoke.provider_reply(Reply::Hang)?;
    let chat = smoke.send_chat(&project_id, &session_id, "Wait for the provider")?;
    smoke.wait_until("the provider request", |smoke| {
        Ok(smoke.provider_completions() == 1)
    })?;
    Ok((smoke, chat))
}

fn assistant(smoke: &mut SmokeHarness, chat: &Chat) -> Result<Value> {
    let messages = smoke.json(
        "GET",
        &format!(
            "/api/v1/canonical/chat/messages?project_id={}&session_id={}",
            chat.project_id, chat.session_id
        ),
        None,
    )?;
    array(&messages, "messages")?
        .iter()
        .rev()
        .find(|message| message["role"] == "assistant" && message["job_id"] == chat.job_id.as_str())
        .cloned()
        .ok_or_else(|| "assistant message missing".into())
}

fn wait_for_job_state(smoke: &mut SmokeHarness, chat: &Chat, state: &str) -> Result<Value> {
    smoke.wait_until(&format!("Job {state}"), |smoke| {
        Ok(smoke.execution(chat)?["job"]["state"] == state)
    })?;
    smoke.execution(chat)
}

/// The settled execution of a turn stopped mid-Call: no authority remains,
/// the Attempt holds `state`, and the interrupted Call and its intent record
/// that the provider effect may have happened.
fn assert_stopped(execution: &Value, state: &str) -> Result<()> {
    assert!(
        execution["job"]["authoritative_attempt_id"].is_null(),
        "{execution}"
    );
    let attempts = array(execution, "attempts")?;
    assert_eq!(attempts.len(), 1, "{execution}");
    assert_eq!(attempts[0]["state"], state, "{execution}");
    assert!(!attempts[0]["finished_at"].is_null(), "{execution}");
    let calls = array(execution, "calls")?;
    assert_eq!(calls.len(), 1, "{execution}");
    assert_eq!(calls[0]["state"], "unknown", "{execution}");
    for intent in array(execution, "dispatch_intents")? {
        assert_eq!(intent["state"], "fenced", "{execution}");
        assert_eq!(intent["effect_state"], "unknown", "{execution}");
    }
    Ok(())
}

/// The frozen provider target of each generation's provider dispatch.
fn targets(execution: &Value) -> Result<Vec<(Value, Value)>> {
    array(execution, "dispatch_intents")?
        .iter()
        .filter(|intent| !intent["provider_key"].is_null())
        .map(|intent| {
            Ok((
                intent["generation"].clone(),
                json!([
                    intent["provider_key"],
                    intent["model"],
                    intent["upstream_model_id"],
                    intent["endpoint"],
                    intent["credential_ref"],
                ]),
            ))
        })
        .collect()
}

/// An endpoint nothing listens on.
fn unreachable_endpoint() -> Result<String> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(format!("http://127.0.0.1:{port}/v1/chat/completions"))
}

#[test]
fn chat_cancellation_without_a_stream_settles_the_turn() -> Result<()> {
    let (mut smoke, chat) = hanging_turn("cancel-project")?;

    let response = smoke.json(
        "POST",
        "/api/v1/canonical/chat/cancel",
        Some(&json!({"project_id": chat.project_id, "session_id": chat.session_id})),
    )?;

    assert_eq!(response["cancelled"], true, "{response}");
    let execution = wait_for_job_state(&mut smoke, &chat, "cancelled")?;
    assert_stopped(&execution, "cancelled")?;
    assert_eq!(
        execution["job"]["termination_reason"]["code"],
        "job_cancelled"
    );
    assert!(execution["job"]["termination_reason"]["details"].is_null());
    let assistant = assistant(&mut smoke, &chat)?;
    assert_eq!(assistant["state"], "failed", "{assistant}");
    assert_eq!(assistant["attempt_state"], "cancelled", "{assistant}");
    assert_eq!(smoke.provider_completions(), 1);
    smoke.finish()
}

#[test]
fn chat_execution_deadline_is_enforced_without_a_stream() -> Result<()> {
    let (mut smoke, chat) = hanging_turn("deadline-project")?;
    let execution = smoke.execution(&chat)?;
    let attempt = string(&execution["job"], "authoritative_attempt_id")?;
    // The turn began longer ago than the Chat execution deadline allows.
    Connection::open(smoke.database_path())?.execute(
        "UPDATE domain_attempts SET created_at=created_at-1000 WHERE id=?1",
        [&attempt],
    )?;

    let execution = wait_for_job_state(&mut smoke, &chat, "cancelled")?;

    assert_stopped(&execution, "cancelled")?;
    assert_eq!(
        execution["job"]["termination_reason"]["details"]["cause"], "chat_execution_timeout",
        "{execution}"
    );
    let assistant = assistant(&mut smoke, &chat)?;
    assert_eq!(assistant["state"], "failed", "{assistant}");
    assert!(
        string(&assistant, "failure_reason")?.contains("deadline"),
        "{assistant}"
    );
    assert_eq!(smoke.provider_completions(), 1);
    smoke.finish()
}

#[test]
fn restart_settles_a_turn_without_its_chat_registry_and_retry_replays_frozen_input() -> Result<()> {
    let (mut smoke, chat) = hanging_turn("restart-project")?;

    smoke.crash_and_restart()?;

    // The restarted process has no record of the turn's Chat registration;
    // the durable state alone settles it.
    let execution = wait_for_job_state(&mut smoke, &chat, "unknown")?;
    assert_stopped(&execution, "unknown")?;
    let assistant_message = assistant(&mut smoke, &chat)?;
    assert_eq!(assistant_message["state"], "failed", "{assistant_message}");
    assert_eq!(
        assistant_message["failure_reason"], "restart_external_effect_unknown",
        "{assistant_message}"
    );
    assert_eq!(
        smoke.provider_completions(),
        1,
        "nothing ran again on restart"
    );

    // Configuration moves new work to an endpoint that cannot answer.
    let profile = smoke.json("GET", "/api/v1/profile", None)?;
    let mut edited = profile["profile"].clone();
    let original_endpoint = edited["providers"]["mock"]["endpoint"].clone();
    edited["providers"]["mock"]["endpoint"] = json!(unreachable_endpoint()?);
    smoke.json(
        "PUT",
        "/api/v1/profile",
        Some(&json!({"revision": profile["revision"], "profile": edited})),
    )?;
    smoke.provider_reply(Reply::Text(vec!["Recovered".into()]))?;

    let retry = smoke.json(
        "POST",
        &format!("/api/v1/canonical/jobs/{}/retry", chat.job_id),
        Some(&json!({"expected_generation": 1})),
    )?;
    assert_eq!(retry["accepted"], true, "{retry}");
    let execution = wait_for_job_state(&mut smoke, &chat, "completed")?;

    // The replacement ran from the target frozen at admission, not from the
    // configuration that is current now.
    let targets = targets(&execution)?;
    assert_eq!(targets.len(), 2, "{execution}");
    assert_eq!(targets[0].1, targets[1].1, "{execution}");
    assert_eq!(targets[1].1[3], original_endpoint, "{execution}");
    assert_eq!(smoke.provider_completions(), 2);
    let assistant_message = assistant(&mut smoke, &chat)?;
    assert_eq!(
        assistant_message["state"], "complete",
        "{assistant_message}"
    );
    assert_eq!(
        assistant_message["content"], "Recovered",
        "{assistant_message}"
    );

    // New work does use the current configuration.
    let next = smoke.send_chat(&chat.project_id, &chat.session_id, "After the change")?;
    let execution = wait_for_job_state(&mut smoke, &next, "failed")?;
    let targets = self::targets(&execution)?;
    assert_eq!(
        targets[0].1[3], edited["providers"]["mock"]["endpoint"],
        "{execution}"
    );
    assert_eq!(smoke.provider_completions(), 2);
    smoke.finish()
}

/// The provider dispatch of a Job's single generation.
fn provider_intent(execution: &Value) -> Result<Value> {
    let intents = array(execution, "dispatch_intents")?
        .iter()
        .filter(|intent| !intent["provider_key"].is_null())
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(intents.len(), 1, "{execution}");
    Ok(intents[0].clone())
}

#[test]
fn restart_runs_an_admitted_unstarted_dispatch_once_from_its_durable_request() -> Result<()> {
    let (mut smoke, project_id, first_session) = session("unstarted-project")?;
    let second_session = smoke.create_untitled_session(&project_id)?;
    smoke.provider_reply(Reply::Hang)?;
    let started = smoke.send_chat(&project_id, &first_session, "Wait for the provider")?;
    smoke.wait_until("the provider request", |smoke| {
        Ok(smoke.provider_completions() == 1)
    })?;
    // The Project's single provider slot is held by the started turn, so a
    // turn from another session is admitted and waits without starting.
    let queued = smoke.send_chat(&project_id, &second_session, "Wait for the slot")?;
    smoke.wait_until("the waiting turn's admitted dispatch", |smoke| {
        let execution = smoke.execution(&queued)?;
        Ok(provider_intent(&execution)?["budget_admitted"] == true)
    })?;
    let before = provider_intent(&smoke.execution(&queued)?)?;
    assert_eq!(before["effect_state"], "not_started", "{before}");
    assert_eq!(smoke.provider_completions(), 1);

    smoke.provider_reply(Reply::Text(vec!["Resumed".into()]))?;
    smoke.crash_and_restart()?;

    // The started effect is uncertain and is never replayed.
    let execution = wait_for_job_state(&mut smoke, &started, "unknown")?;
    assert_stopped(&execution, "unknown")?;
    // The unstarted one runs once, from the request and target it was
    // admitted with, and settles without the old Chat registry.
    let execution = wait_for_job_state(&mut smoke, &queued, "completed")?;
    assert!(
        execution["job"]["authoritative_attempt_id"].is_null(),
        "{execution}"
    );
    let attempts = array(&execution, "attempts")?;
    assert_eq!(attempts.len(), 1, "{execution}");
    assert_eq!(attempts[0]["state"], "completed", "{execution}");
    let after = provider_intent(&execution)?;
    for field in [
        "call_id",
        "request",
        "provider_key",
        "model",
        "upstream_model_id",
        "endpoint",
        "credential_ref",
    ] {
        assert_eq!(after[field], before[field], "{field}: {execution}");
    }
    assert_eq!(after["effect_state"], "settled", "{execution}");
    let calls = array(&execution, "calls")?;
    assert_eq!(calls.len(), 1, "{execution}");
    assert_eq!(calls[0]["state"], "completed", "{execution}");
    let message = assistant(&mut smoke, &queued)?;
    assert_eq!(message["state"], "complete", "{message}");
    assert_eq!(message["content"], "Resumed", "{message}");
    assert_eq!(smoke.provider_completions(), 2, "each effect ran once");
    smoke.finish()
}

#[test]
fn reconnect_after_the_terminal_replays_the_tail_without_executing_again() -> Result<()> {
    let (mut smoke, project_id, session_id) = session("reconnect-project")?;
    let released = Arc::new(AtomicBool::new(false));
    smoke.provider_reply(Reply::Gated {
        released: Arc::clone(&released),
        rest: vec!["second ".into(), "third".into()],
    })?;
    let chat = smoke.send_chat(&project_id, &session_id, "Stream in parts")?;

    // The client sees the first delta, then loses its connection.
    let mut first = smoke.open_chat_stream(&chat, None)?;
    let seen = loop {
        let (id, event) = first.next_sse_event()?.ok_or("stream closed early")?;
        if event["delta"] == "partial " {
            break id.ok_or("event without an id")?;
        }
    };
    first.abort()?;
    released.store(true, Ordering::SeqCst);
    let execution = wait_for_job_state(&mut smoke, &chat, "completed")?;
    assert_eq!(array(&execution, "attempts")?.len(), 1, "{execution}");
    assert_eq!(array(&execution, "calls")?.len(), 1, "{execution}");

    // Reconnecting after the turn ended resumes after the last event seen.
    let mut resumed = smoke.open_chat_stream(&chat, Some(seen))?;
    let mut text = String::new();
    loop {
        let (id, event) = resumed.next_sse_event()?.ok_or("replay closed early")?;
        assert!(
            id.is_some_and(|id| id > seen),
            "replayed {id:?} after {seen}"
        );
        if let Some(delta) = event["delta"].as_str() {
            text.push_str(delta);
        }
        if event["done"] == true {
            break;
        }
    }
    assert_eq!(text, "second third");

    // A consumed terminal is not replayed again, and nothing re-executes.
    smoke.wait_until("the consumed tail to be released", |smoke| {
        Ok(smoke.open_chat_stream(&chat, None)?.status == 404)
    })?;
    let execution = smoke.execution(&chat)?;
    assert_eq!(array(&execution, "attempts")?.len(), 1, "{execution}");
    assert_eq!(array(&execution, "calls")?.len(), 1, "{execution}");
    assert_eq!(smoke.provider_completions(), 1);
    let assistant = assistant(&mut smoke, &chat)?;
    assert_eq!(assistant["content"], "partial second third", "{assistant}");
    smoke.finish()
}

#[test]
fn the_profile_is_served_from_the_database_across_restart() -> Result<()> {
    let (mut smoke, project_id, session_id) = session("profile-project")?;
    let before = smoke.json("GET", "/api/v1/profile", None)?;

    smoke.crash_and_restart()?;

    let after = smoke.json("GET", "/api/v1/profile", None)?;
    assert_eq!(after, before);
    // An edit computed from an older revision is refused.
    let mut edited = after["profile"].clone();
    edited["providers"]["mock"]["label"] = json!("Renamed");
    let (status, refused) = smoke.request(
        "PUT",
        "/api/v1/profile",
        Some(&json!({"revision": "1", "profile": edited})),
    )?;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(smoke.json("GET", "/api/v1/profile", None)?, before);
    let changed = smoke.json(
        "PUT",
        "/api/v1/profile",
        Some(&json!({"revision": after["revision"], "profile": edited})),
    )?;
    assert_ne!(changed["revision"], after["revision"]);
    assert_eq!(changed["profile"]["providers"]["mock"]["label"], "Renamed");

    // The stored credential still authorizes the provider: the fixture
    // refuses any request without it.
    smoke.provider_reply(Reply::Text(vec!["Persisted".into()]))?;
    let chat = smoke.send_chat(&project_id, &session_id, "After restart")?;
    assert_eq!(smoke.consume_chat(&chat)?, "Persisted");
    smoke.assert_terminal_execution(&chat, "completed")?;
    smoke.finish()
}
