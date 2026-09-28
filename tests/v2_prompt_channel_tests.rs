#![cfg(unix)]
//! A server/plugin process inherits a channel *before* the URL exists. The
//! plugin then invokes the real strict bridge subprocess after late publication.

use ocg::orchestration::plugin::v2_plugin_source;
use ocg::orchestration::substrate::{RunId, SubstrateRepository, WitnessDisposition};
use ocg::proxy::ChildProxyEnv;
use ocg::runtime::compat::v2_rendezvous::{resolve, InvocationChannel, CHANNEL_ENV, ID_ENV};
use ocg::runtime::compat::v2_server::{OwnedV2Server, StartupBudget};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn late_registration_reaches_real_strict_plugin_bridge_and_canonical_root() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path();
    std::fs::write(
        project.join(".ocg.yaml"),
        concat!(
            "orchestration:\n  enabled: true\n  canonicalExecution: true\n",
            "profile:\n  origin: new\n  defaultModel: lead-model\n",
            "models:\n",
            "  providers:\n",
            "    opencode-go:\n      label: OpenCode Go\n      placeholder: false\n",
            "  models:\n",
            "    lead-model:\n      provider: opencode-go\n      id: space-bunny-free\n      placeholder: false\n"
        ),
    )
    .unwrap();
    std::fs::write(project.join("plugin.mjs"), v2_plugin_source()).unwrap();
    let channel = InvocationChannel::new(project).unwrap();
    assert_eq!(
        std::fs::metadata(channel.socket().parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert!(
        resolve(&channel.socket(), channel.identity()).is_err(),
        "not published before server startup"
    );

    // A local authenticated fake of the V2 session API. No model endpoint or
    // external inference is contacted; wrong credentials are rejected.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::clone(&stop);
    let api = std::thread::spawn(move || {
        while !done.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut buf = [0; 4096];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let authorized = request.lines().any(|line| {
                        line.eq_ignore_ascii_case(
                            "authorization: Basic b3BlbmNvZGU6dGVzdC1sb2NhbC1jcmVkZW50aWFs",
                        )
                    });
                    let body = if request.starts_with("POST /api/session/ses-owned/prompt ") {
                        // Keep the owned server and channel alive while the
                        // simulated inference response is in flight.
                        std::thread::sleep(Duration::from_millis(60));
                        r#"{"text":"321","model":"opencode-go/space-bunny-free"}"#
                    } else if request.starts_with("GET /api/session/ses-owned ") {
                        r#"{"data":{"id":"ses-owned","agent":"lead","model":{"providerID":"opencode-go","id":"space-bunny-free"}}}"#
                    } else {
                        r#"{"data":{}}"#
                    };
                    let status = if authorized {
                        "200 OK"
                    } else {
                        "401 Unauthorized"
                    };
                    let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = stream.write_all(response.as_bytes());
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(_) => break,
            }
        }
    });

    // The fake server reports its registration like OpenCode's handshake;
    // its inherited environment has only the channel, never URL/password.
    let server_script = project.join("fake-server.sh");
    std::fs::write(&server_script, format!("#!/bin/sh\nprintf 'server listening on http://127.0.0.1:{port}\\nserver password test-local-credential\\n'\nexec sleep 60\n")).unwrap();
    std::fs::set_permissions(&server_script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let env = vec![
        (CHANNEL_ENV.into(), channel.socket().into_os_string()),
        (ID_ENV.into(), channel.identity().into()),
    ];
    let proxy = ChildProxyEnv::default();
    let server = OwnedV2Server::start_with(
        &server_script,
        "{}",
        &env,
        &proxy,
        StartupBudget {
            handshake_deadline: Duration::from_secs(2),
            attempts: 30,
            interval: Duration::from_millis(20),
        },
    )
    .unwrap();
    assert!(
        resolve(&channel.socket(), channel.identity()).is_err(),
        "handshake is not publication"
    );
    channel.publish(&server).unwrap();
    assert!(resolve(&channel.socket(), "wrong-invocation").is_err());
    assert!(
        channel.publish(&server).is_err(),
        "registration cannot be replaced"
    );

    // Node stands in for the long-lived plugin/server process. It inherits
    // only the pre-spawn env and spawns the actual `ocg __bridge` through the
    // generated strictBridge implementation (not a mocked bridge).
    let script = r#"import plugin from './plugin.mjs';
let prompt;
await plugin.setup({
  session: {hook: async (name, fn) => { if (name === 'prompt') prompt = fn; return {dispose: async () => {}}; }},
  tool: {hook: async () => ({dispose: async () => {}})},
});
try {
  await prompt({sessionID:'ses-owned', prompt:{text:'offline lifecycle test'}});
  console.log('strict bridge admitted');
} catch(e) { console.error(e.message); process.exitCode = 1; }
"#;
    std::fs::write(project.join("check.mjs"), script).unwrap();
    let node = || {
        Command::new("node")
        .arg("check.mjs")
        .current_dir(project)
        .env(CHANNEL_ENV, channel.socket())
        .env(ID_ENV, channel.identity())
        .env("OCG_BRIDGE", env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_PROJECT", project)
        .env("OCG_PROJECT_CONFIG", project.join(".ocg.yaml"))
        .env("OCG_USER_CONFIG", project.join("no-user.yaml"))
        .env("OCG_ORCHESTRATION_ENABLED", "1")
        .env("OCG_LEAD_CONTRACT", r#"{"level":"lead-model","agent":"lead","provider_id":"opencode-go","model_id":"space-bunny-free"}"#)
        .env_remove("OCG_V2_SERVER_URL")
        .env_remove("OCG_V2_SERVER_PASSWORD")
        .output().unwrap()
    };
    let success = node();
    assert!(
        success.status.success(),
        "{}",
        String::from_utf8_lossy(&success.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&success.stdout).trim(),
        "strict bridge admitted"
    );

    let mut repo = SubstrateRepository::open(project).unwrap();
    let missions = repo.missions_by_binding("ses-owned").unwrap();
    assert_eq!(missions.len(), 1);
    let witness = repo
        .witness_for_run(&missions[0], RunId(0))
        .unwrap()
        .unwrap();
    assert_eq!(witness.runtime_execution_id, "ses-owned");
    assert_eq!(witness.work_node_id, 0);
    assert_eq!(
        repo.validate_witness(&witness).unwrap(),
        WitnessDisposition::Authoritative
    );
    drop(repo);

    // Complete a deterministic, offline prompt/response phase *after*
    // admission. The channel and owned child must not tear down during it.
    let response: serde_json::Value = reqwest::blocking::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/session/ses-owned/prompt"
        ))
        .basic_auth("opencode", Some("test-local-credential"))
        .json(&serde_json::json!({"text":"reply exactly: 321"}))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(response["text"], "321");
    assert!(
        channel.socket().exists(),
        "channel torn down before response completed"
    );
    assert!(resolve(&channel.socket(), channel.identity()).is_ok());
    #[cfg(target_os = "linux")]
    assert!(std::path::Path::new(&format!("/proc/{}", server.identity().pid.unwrap())).exists());

    let socket = channel.socket();
    let pid = server.identity().pid.unwrap();
    let identity = channel.identity().to_string();
    drop(server);
    drop(channel);
    assert!(!socket.exists());
    #[cfg(target_os = "linux")]
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "owned child not reaped"
    );
    assert!(
        resolve(&socket, &identity).is_err(),
        "stale invocation must not resolve"
    );
    // A stale or missing reference remains a strict prompt rejection, even if
    // an unrelated runtime might be discoverable in the user's environment.
    let stale = Command::new("node").arg("check.mjs").current_dir(project)
        .env(CHANNEL_ENV, &socket)
        .env(ID_ENV, &identity)
        .env("OCG_BRIDGE", env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_PROJECT", project)
        .env("OCG_PROJECT_CONFIG", project.join(".ocg.yaml"))
        .env("OCG_USER_CONFIG", project.join("no-user.yaml"))
        .env("OCG_ORCHESTRATION_ENABLED", "1")
        .env("OCG_LEAD_CONTRACT", r#"{"level":"lead-model","agent":"lead","provider_id":"opencode-go","model_id":"space-bunny-free"}"#)
        .env("OCG_V2_SERVER_URL", format!("http://127.0.0.1:{port}"))
        .env("OCG_V2_SERVER_PASSWORD", "test-local-credential")
        .output().unwrap();
    assert!(!stale.status.success());
    assert!(!String::from_utf8_lossy(&stale.stderr).contains("test-local-credential"));
    stop.store(true, Ordering::Relaxed);
    api.join().unwrap();
    // A different random invocation cannot attach using the old identity.
    let replacement = InvocationChannel::new(project).unwrap();
    assert_ne!(replacement.socket(), socket);
    assert!(resolve(&replacement.socket(), replacement.identity()).is_err());
}
