use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{json, Value};
use tempfile::TempDir;

use super::client::Response;
use super::provider::{ProviderFixture, Reply, API_KEY, MODEL};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const BODY_LIMIT: usize = 1024 * 1024;
const LOG_LIMIT: u64 = 65536;

#[derive(Clone, Copy)]
pub struct Timeouts {
    pub startup: Duration,
    pub readiness_probe: Duration,
    pub http: Duration,
    pub execution: Duration,
    pub scenario: Duration,
    pub shutdown: Duration,
    pub poll: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(15),
            readiness_probe: Duration::from_millis(250),
            http: Duration::from_secs(10),
            execution: Duration::from_secs(30),
            scenario: Duration::from_secs(120),
            shutdown: Duration::from_secs(5),
            poll: Duration::from_millis(25),
        }
    }
}

pub fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|time| !time.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "smoke deadline expired"))
}

fn log(path: &Path) -> String {
    let read = || -> io::Result<String> {
        let mut file = File::open(path)?;
        let size = file.metadata()?.len();
        file.seek(SeekFrom::Start(size.saturating_sub(LOG_LIMIT)))?;
        let mut bytes = Vec::new();
        file.take(LOG_LIMIT).read_to_end(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    };
    read().unwrap_or_else(|error| format!("cannot read {}: {error}", path.display()))
}

struct Process {
    child: Child,
    status: Option<ExitStatus>,
    stdout: PathBuf,
    stderr: PathBuf,
    timeouts: Timeouts,
    graceful: bool,
}

impl Process {
    fn spawn(
        command: &mut Command,
        directory: &Path,
        name: &str,
        timeouts: Timeouts,
    ) -> Result<Self> {
        let stdout = directory.join(format!("{name}.stdout"));
        let stderr = directory.join(format!("{name}.stderr"));
        let child = command
            .stdin(Stdio::null())
            .stdout(File::create(&stdout)?)
            .stderr(File::create(&stderr)?)
            .spawn()?;
        Ok(Self {
            child,
            status: None,
            stdout,
            stderr,
            timeouts,
            graceful: true,
        })
    }

    fn status(&mut self) -> Result<Option<ExitStatus>> {
        if self.status.is_none() {
            self.status = self.child.try_wait()?;
        }
        Ok(self.status)
    }

    fn wait_until(&mut self, deadline: Instant) -> Result<Option<ExitStatus>> {
        loop {
            if let Some(status) = self.status()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(self.timeouts.poll.min(remaining(deadline)?));
        }
    }

    fn stop(&mut self) -> Result<bool> {
        if self.status()?.is_some() {
            return Ok(true);
        }
        #[cfg(unix)]
        if self.graceful {
            let deadline = Instant::now() + self.timeouts.shutdown;
            match self.interrupt(deadline) {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => eprintln!("smoke graceful shutdown: {error}; forcing termination"),
            }
        }
        self.child.kill()?;
        if self
            .wait_until(Instant::now() + self.timeouts.shutdown)?
            .is_none()
        {
            return Err("OCG did not exit after forced kill".into());
        }
        Ok(false)
    }

    #[cfg(unix)]
    fn interrupt(&mut self, deadline: Instant) -> Result<bool> {
        let mut command = Command::new("/bin/kill");
        command.args(["-INT", &self.child.id().to_string()]);
        let mut signal = Self::spawn(
            &mut command,
            self.stdout.parent().ok_or("missing log directory")?,
            &format!("interrupt-{}", self.child.id()),
            self.timeouts,
        )?;
        signal.graceful = false;
        let status = signal
            .wait_until(deadline)?
            .ok_or("shutdown signal timed out")?;
        if !status.success() && self.status()?.is_none() {
            return Err(format!("shutdown signal returned {status}").into());
        }
        Ok(self.wait_until(deadline)?.is_some())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("smoke process {} cleanup: {error}", self.child.id());
        }
    }
}

#[derive(Debug)]
pub struct Chat {
    pub project_id: String,
    pub session_id: String,
    pub job_id: String,
}

pub struct SmokeHarness {
    workspace: Option<TempDir>,
    process: Option<Process>,
    provider: ProviderFixture,
    address: Option<SocketAddr>,
    timeouts: Timeouts,
    deadline: Instant,
    phase: String,
    last_http: String,
    events: Vec<Value>,
    snapshot: Value,
    ids: Vec<String>,
    sequence: u64,
    finished: bool,
}

impl SmokeHarness {
    pub fn start(reply: Reply) -> Result<Self> {
        let timeouts = Timeouts::default();
        let deadline = Instant::now() + timeouts.scenario;
        let workspace = tempfile::Builder::new().prefix("ocg-smoke-").tempdir()?;
        let provider = ProviderFixture::start(reply, timeouts, deadline)?;
        let mut smoke = Self {
            workspace: Some(workspace),
            process: None,
            provider,
            address: None,
            timeouts,
            deadline,
            phase: "prepare".into(),
            last_http: String::new(),
            events: Vec::new(),
            snapshot: Value::Null,
            ids: Vec::new(),
            sequence: 0,
            finished: false,
        };
        for directory in [
            "home", "config", "data", "cache", "state", "runtime", "tmp", "root",
        ] {
            fs::create_dir_all(smoke.path(directory))?;
        }
        let mut key = [0; 32];
        SystemRandom::new()
            .fill(&mut key)
            .map_err(|_| "cannot generate isolated Vault key")?;
        let key_path = smoke.path("vault.key");
        fs::write(&key_path, key)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(key_path, fs::Permissions::from_mode(0o600))?;
        }
        smoke.phase = "spawn".into();
        let mut command = smoke.command();
        command.args(["--disable-proxy", "serve", "127.0.0.1:0"]);
        smoke.process = Some(Process::spawn(
            &mut command,
            &smoke.path(""),
            "ocg",
            timeouts,
        )?);
        smoke.wait_ready()?;
        Ok(smoke)
    }

    pub fn path(&self, relative: &str) -> PathBuf {
        self.workspace
            .as_ref()
            .expect("live smoke workspace")
            .path()
            .join(relative)
    }

    fn command(&self) -> Command {
        // Cargo resolves this for integration tests, including custom target dirs.
        let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
        command
            .env_clear()
            .current_dir(self.path("root"))
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.path("home"))
            .env("USERPROFILE", self.path("home"))
            .env("XDG_CONFIG_HOME", self.path("config"))
            .env("XDG_DATA_HOME", self.path("data"))
            .env("XDG_CACHE_HOME", self.path("cache"))
            .env("XDG_STATE_HOME", self.path("state"))
            .env("XDG_RUNTIME_DIR", self.path("runtime"))
            .env("TMPDIR", self.path("tmp"))
            .env("TMP", self.path("tmp"))
            .env("TEMP", self.path("tmp"))
            .env("OCG_USER_CONFIG", self.path("config/config.yaml"))
            .env("OCG_VAULT_PATH", self.path("data/credentials.enc"))
            .env("OCG_VAULT_KEY_FILE", self.path("vault.key"));
        command
    }

    fn operation_deadline(&self, timeout: Duration) -> Instant {
        (Instant::now() + timeout).min(self.deadline)
    }

    fn alive(&mut self) -> Result<()> {
        if let Some(process) = &mut self.process {
            if let Some(status) = process.status()? {
                return Err(format!(
                    "OCG exited during {}: {status}\nstdout:\n{}\nstderr:\n{}",
                    self.phase,
                    log(&process.stdout),
                    log(&process.stderr)
                )
                .into());
            }
        }
        remaining(self.deadline)?;
        Ok(())
    }

    pub fn wait_ready(&mut self) -> Result<()> {
        self.phase = "readiness".into();
        let deadline = self.operation_deadline(self.timeouts.startup);
        loop {
            self.alive()?;
            if self.address.is_none() {
                if let Some(process) = &self.process {
                    for line in log(&process.stdout).lines() {
                        if let Some(address) = line.strip_prefix("listening on http://") {
                            let address: SocketAddr = address.parse()?;
                            if !address.ip().is_loopback() || address.port() == 0 {
                                return Err("invalid OCG advertised address".into());
                            }
                            self.address = Some(address);
                        }
                    }
                }
            }
            if let Some(address) = self.address {
                match Response::open(
                    address,
                    "GET",
                    "/api/v1/profile",
                    &[],
                    None,
                    deadline.min(self.operation_deadline(self.timeouts.readiness_probe)),
                ) {
                    Ok(mut response) => {
                        let result = response.read_body();
                        self.record_http("GET /api/v1/profile", &response);
                        if let Err(error) = result {
                            self.last_http.push_str(&format!("\n{error}"));
                            thread::sleep(self.timeouts.poll.min(remaining(deadline)?));
                            continue;
                        }
                        if response.status == 200 {
                            serde_json::from_slice::<Value>(&response.body)?;
                            return Ok(());
                        }
                    }
                    Err(error) => self.last_http = error.to_string(),
                }
            }
            thread::sleep(self.timeouts.poll.min(remaining(deadline)?));
        }
    }

    fn record_http(&mut self, request: &str, response: &Response) {
        self.last_http = format!(
            "{request}: HTTP {} {:?}\n{}",
            response.status,
            response.headers,
            String::from_utf8_lossy(&response.body[..response.body.len().min(LOG_LIMIT as usize)])
        );
    }

    pub fn request(
        &mut self,
        method: &str,
        path: &str,
        payload: Option<&Value>,
    ) -> Result<(u16, Value)> {
        self.request_until(
            method,
            path,
            payload,
            self.operation_deadline(self.timeouts.http),
        )
    }

    fn request_until(
        &mut self,
        method: &str,
        path: &str,
        payload: Option<&Value>,
        deadline: Instant,
    ) -> Result<(u16, Value)> {
        self.phase = format!("{method} {path}");
        self.alive()?;
        self.last_http = format!("{method} {path}: awaiting response");
        let mut response = match Response::open(
            self.address.ok_or("no OCG address")?,
            method,
            path,
            &[],
            payload,
            deadline,
        ) {
            Ok(response) => response,
            Err(error) => {
                self.last_http = format!("{method} {path}: {error}");
                return Err(error);
            }
        };
        let result = response.read_body();
        self.record_http(&format!("{method} {path}"), &response);
        result?;
        Ok((response.status, serde_json::from_slice(&response.body)?))
    }

    pub fn json(&mut self, method: &str, path: &str, payload: Option<&Value>) -> Result<Value> {
        let (status, body) = self.request(method, path, payload)?;
        if status != 200 {
            return Err(format!("{method} {path}: HTTP {status}: {body}").into());
        }
        Ok(body)
    }

    pub fn parallel_json(&mut self, requests: Vec<(&str, String, Value)>) -> Result<Vec<Value>> {
        self.phase = "parallel requests".into();
        self.alive()?;
        let address = self.address.ok_or("no OCG address")?;
        let deadline = self.operation_deadline(self.timeouts.http);
        let results = thread::scope(|scope| {
            let mut gates = Vec::new();
            let mut workers = Vec::new();
            for (method, path, payload) in requests {
                let (sender, receiver) = std::sync::mpsc::channel();
                gates.push(sender);
                workers.push(scope.spawn(move || -> Result<Value> {
                    receiver.recv_timeout(remaining(deadline)?)?;
                    let mut response =
                        Response::open(address, method, &path, &[], Some(&payload), deadline)?;
                    response.read_body()?;
                    if response.status != 200 {
                        return Err(format!(
                            "HTTP {}: {}",
                            response.status,
                            String::from_utf8_lossy(&response.body)
                        )
                        .into());
                    }
                    Ok(serde_json::from_slice(&response.body)?)
                }));
            }
            for gate in gates {
                gate.send(())?;
            }
            workers
                .into_iter()
                .map(|worker| worker.join().map_err(|_| "smoke request thread panicked")?)
                .collect::<Result<Vec<_>>>()
        });
        self.last_http = format!("parallel results: {results:?}");
        results
    }

    pub fn configure_provider(&mut self) -> Result<String> {
        self.json(
            "POST",
            "/api/v1/profile/bootstrap",
            Some(&json!({"choice": "new"})),
        )?;
        let connected = self.json("POST", "/api/v1/setup/connect", Some(&json!({
            "name": "Mock", "endpoint": format!("http://{}/v1", self.provider.address), "api_key": API_KEY
        })))?;
        let model = connected["models"]
            .as_array()
            .and_then(|models| models.iter().find(|model| model["id"] == MODEL))
            .ok_or("fixture model was not discovered")?;
        let choice = string(model, "key")?;
        let selected = self.json(
            "POST",
            "/api/v1/setup/models",
            Some(&json!({
                "provider_key": connected["provider_key"], "models": [{"key": choice, "id": MODEL}],
                "default_model": choice, "revision": connected["revision"]
            })),
        )?;
        if !array(&selected, "runnable_choices")?.contains(&json!(choice)) {
            return Err(format!("configured model is not runnable: {selected}").into());
        }
        Ok(choice)
    }

    pub fn provider_reply(&mut self, reply: Reply) -> Result<()> {
        self.provider.set_reply(reply)
    }

    pub fn import_project(&mut self, root: &Path) -> Result<Value> {
        let command = self.id();
        let response = self.json(
            "POST",
            "/api/v1/canonical/projects/import",
            Some(&json!({"command_id": command, "root": root})),
        )?;
        if response["accepted"] != true {
            return Err(format!("Project import refused: {response}").into());
        }
        self.ids.push(string(&response["project"], "project_id")?);
        Ok(response["project"].clone())
    }

    pub fn add_project(&mut self, name: &str) -> Result<Value> {
        let root = self.path(name);
        fs::create_dir_all(&root)?;
        let command = self.id();
        let project = self.json(
            "POST",
            "/api/v1/setup/project",
            Some(&json!({"command_id": command, "root": root})),
        )?;
        self.ids.push(string(&project, "project_id")?);
        Ok(project)
    }

    pub fn create_untitled_session(&mut self, project_id: &str) -> Result<String> {
        self.phase = "canonical untitled session creation".into();
        self.alive()?;
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut command = Command::new("node");
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .current_dir(root)
            .arg(root.join("tests/smoke/canonical-session.cjs"))
            .arg(format!("http://{}", self.address.ok_or("no OCG address")?))
            .arg(project_id);
        let mut process = Process::spawn(
            &mut command,
            &self.path(""),
            "canonical-session",
            self.timeouts,
        )?;
        let status = process
            .wait_until(self.operation_deadline(self.timeouts.http))?
            .ok_or("canonical session creation timed out")?;
        if !status.success() {
            return Err(format!(
                "canonical session creation: {status}\nstdout:\n{}\nstderr:\n{}",
                log(&process.stdout),
                log(&process.stderr)
            )
            .into());
        }
        let session_id = string(&serde_json::from_str(&log(&process.stdout))?, "session_id")?;
        self.ids.push(session_id.clone());
        Ok(session_id)
    }

    pub fn id(&mut self) -> String {
        self.sequence += 1;
        format!("smoke-{}", self.sequence)
    }

    pub fn retry_failed_turn(&mut self, chat: &Chat, message_id: &str) -> Result<Chat> {
        self.alive()?;
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut command = Command::new("node");
        command.env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .current_dir(root)
            .arg(root.join("tests/smoke/canonical-session.cjs"))
            .arg(format!("http://{}", self.address.ok_or("no OCG address")?))
            .args([&chat.project_id, &chat.session_id, message_id]);
        let mut process = Process::spawn(&mut command, &self.path(""), "canonical-retry", self.timeouts)?;
        let status = process.wait_until(self.operation_deadline(self.timeouts.http))?
            .ok_or("canonical retry timed out")?;
        if !status.success() {
            return Err(format!("canonical retry: {status}\n{}", log(&process.stderr)).into());
        }
        Ok(Chat {
            project_id: chat.project_id.clone(), session_id: chat.session_id.clone(),
            job_id: string(&serde_json::from_str(&log(&process.stdout))?, "job_id")?,
        })
    }

    pub fn send_chat(
        &mut self,
        project_id: &str,
        session_id: &str,
        objective: &str,
    ) -> Result<Chat> {
        let command = self.id();
        let response = self.json("POST", "/api/v1/canonical/chat/send", Some(&json!({
            "command_id": command, "draft_id": command, "project_id": project_id, "session_id": session_id,
            "objective": objective, "success_criteria": null, "constraints": null,
            "hard_budget_micros": 0, "resource_commitment": null
        })))?;
        if response["outcome"] != "accepted" {
            return Err(format!("Chat refused: {response}").into());
        }
        let chat = Chat {
            project_id: project_id.into(),
            session_id: session_id.into(),
            job_id: string(&response, "job_id")?,
        };
        self.ids.push(format!("{chat:?}"));
        Ok(chat)
    }

    pub fn consume_chat(&mut self, chat: &Chat) -> Result<String> {
        self.phase = format!("SSE {chat:?}");
        self.alive()?;
        self.events.clear();
        let path = format!(
            "/api/v1/canonical/chat/stream?session_id={}&job_id={}",
            chat.session_id, chat.job_id
        );
        let mut response = Response::open(
            self.address.ok_or("no OCG address")?,
            "GET",
            &path,
            &[("Accept", "text/event-stream")],
            None,
            self.operation_deadline(self.timeouts.execution),
        )?;
        let result = response.consume_sse(&mut self.events);
        self.record_http(&path, &response);
        result
    }

    pub fn assert_terminal_execution(&mut self, chat: &Chat, expected: &str) -> Result<Value> {
        let deadline = self.operation_deadline(self.timeouts.execution);
        loop {
            remaining(deadline)?;
            let (status, snapshot) = self.request_until(
                "GET",
                &format!(
                    "/api/v1/canonical/jobs?project_id={}&job_id={}",
                    chat.project_id, chat.job_id
                ),
                None,
                deadline.min(self.operation_deadline(self.timeouts.http)),
            )?;
            if status != 200 {
                return Err(format!("snapshot HTTP {status}: {snapshot}").into());
            }
            self.snapshot = snapshot.clone();
            let execution = &snapshot["job"];
            let state = string(&execution["job"], "state")?;
            if state == expected {
                let attempts = array(execution, "attempts")?;
                let executors = array(execution, "executors")?;
                let calls = array(execution, "calls")?;
                if attempts.is_empty() || executors.is_empty() || calls.is_empty() {
                    return Err(format!("incomplete terminal execution: {snapshot}").into());
                }
                for attempt in attempts {
                    if attempt["state"] != expected || attempt["finished_at"].is_null() {
                        return Err(format!("inconsistent terminal Attempt: {attempt}").into());
                    }
                }
                for executor in executors {
                    if executor["state"] != expected {
                        return Err(format!("inconsistent terminal Executor: {executor}").into());
                    }
                }
                for call in calls {
                    if !["completed", "failed", "cancelled", "fenced"]
                        .iter()
                        .any(|state| call["state"] == *state)
                    {
                        return Err(format!("nonterminal Call: {call}").into());
                    }
                    if expected == "completed" && call["state"] != "completed" {
                        return Err(format!("unsuccessful Call: {call}").into());
                    }
                }
                for intent in array(execution, "dispatch_intents")? {
                    if ["pending", "queued", "running"]
                        .iter()
                        .any(|state| intent["state"] == *state)
                    {
                        return Err(format!("unsettled DispatchIntent: {intent}").into());
                    }
                }
                return Ok(snapshot);
            }
            if ["completed", "failed", "cancelled", "unknown", "orphaned"].contains(&state.as_str())
            {
                return Err(format!("unexpected terminal Job: {snapshot}").into());
            }
            thread::sleep(self.timeouts.poll.min(remaining(deadline)?));
        }
    }

    pub fn admit_job(&mut self, root: &Path, session: &str) -> Result<String> {
        self.phase = "CLI Job admission".into();
        self.alive()?;
        let mut command = self.command();
        command.arg("--project").arg(root).args([
            "work",
            "admit",
            "--objective",
            "smoke durable fact",
            "--session",
            session,
        ]);
        let name = self.id();
        let mut process = Process::spawn(&mut command, &self.path(""), &name, self.timeouts)?;
        let status = process
            .wait_until(self.operation_deadline(self.timeouts.http))?
            .ok_or("Job admission timed out")?;
        let stdout = log(&process.stdout);
        let stderr = log(&process.stderr);
        self.last_http = format!("CLI admission: {status}\n{stdout}\n{stderr}");
        if !status.success() {
            return Err(self.last_http.clone().into());
        }
        let job = string(&serde_json::from_str::<Value>(&stdout)?, "job_id")?;
        self.ids.push(job.clone());
        Ok(job)
    }

    pub fn control_file(&self, name: &str) -> Result<Value> {
        Ok(serde_json::from_slice(&fs::read(
            self.path(&format!("config/state/control/{name}.json")),
        )?)?)
    }

    pub fn assert_no_temp_files(&self) -> Result<()> {
        for entry in fs::read_dir(self.path("config/state/control"))? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().contains(".tmp-") {
                return Err(format!("leftover atomic-write file: {:?}", entry.path()).into());
            }
        }
        Ok(())
    }

    pub fn worker_threads(&self) -> Result<Option<Vec<String>>> {
        #[cfg(target_os = "linux")]
        {
            let process = self.process.as_ref().ok_or("no OCG process")?;
            let mut names = Vec::new();
            for entry in fs::read_dir(format!("/proc/{}/task", process.child.id()))? {
                let entry = entry?;
                match fs::read_to_string(entry.path().join("comm")) {
                    Ok(name)
                        if name.trim() == "provider-worker"
                            || name.trim() == "native-tool-worker" =>
                    {
                        names.push(name.trim().into())
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            return Ok(Some(names));
        }
        #[cfg(not(target_os = "linux"))]
        Ok(None)
    }

    fn diagnostics(&mut self) {
        eprintln!("SMOKE FAILURE\nworkspace: {}\nphase: {}\nIDs: {:?}\nlast HTTP: {}\nSSE events: {:?}\nexecution: {}\nprovider: {}",
            self.path("").display(), self.phase, self.ids, self.last_http, self.events, self.snapshot, self.provider.diagnostics());
        if let Some(process) = &mut self.process {
            eprintln!(
                "OCG pid: {}, status: {:?}\nstdout:\n{}\nstderr:\n{}",
                process.child.id(),
                process.status(),
                log(&process.stdout),
                log(&process.stderr)
            );
        }
    }

    pub fn finish(mut self) -> Result<()> {
        self.phase = "shutdown".into();
        self.alive()?;
        let process = self.process.as_mut().ok_or("no OCG process")?;
        if !process.stop()? {
            return Err("OCG required forced kill during clean shutdown".into());
        }
        if !process.status()?.is_some_and(|status| status.success()) {
            return Err("OCG graceful shutdown failed".into());
        }
        self.provider.shutdown()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for SmokeHarness {
    fn drop(&mut self) {
        if !self.finished {
            self.diagnostics();
        }
        if let Some(process) = &mut self.process {
            if let Err(error) = process.stop() {
                eprintln!("OCG cleanup: {error}");
            }
        }
        if let Err(error) = self.provider.shutdown() {
            eprintln!("provider cleanup: {error}");
        }
        if !self.finished
            && std::env::var_os("OCG_SMOKE_KEEP").as_deref() == Some(std::ffi::OsStr::new("1"))
        {
            if let Some(workspace) = self.workspace.take() {
                eprintln!("preserved smoke workspace: {}", workspace.keep().display());
            }
        }
    }
}

pub fn string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("missing string {key} in {value}").into())
}

pub fn array<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("missing array {key} in {value}").into())
}

#[cfg(unix)]
fn process_signal(pid: u32, signal: &str, directory: &Path) -> Result<ExitStatus> {
    let timeouts = Timeouts::default();
    let mut command = Command::new("/bin/kill");
    command.args([signal, &pid.to_string()]);
    let mut process = Process::spawn(&mut command, directory, "signal-check", timeouts)?;
    process.graceful = false;
    process
        .wait_until(Instant::now() + timeouts.shutdown)?
        .ok_or_else(|| "signal check timed out".into())
}

#[test]
fn cleanup_after_scenario_panic() -> Result<()> {
    let smoke = SmokeHarness::start(Reply::Text(vec!["OK".into()]))?;
    let path = smoke.path("");
    let address = smoke.address.ok_or("no OCG address")?;
    #[cfg(unix)]
    let pid = smoke.process.as_ref().ok_or("no OCG process")?.child.id();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _smoke = smoke;
        panic!("intentional harness cleanup probe");
    }));
    assert!(panic.is_err());
    assert!(std::net::TcpStream::connect_timeout(&address, Timeouts::default().http).is_err());
    if std::env::var_os("OCG_SMOKE_KEEP").as_deref() != Some(std::ffi::OsStr::new("1")) {
        assert!(
            !path.exists(),
            "workspace survived panic: {}",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        let workspace = tempfile::tempdir()?;
        assert!(
            !process_signal(pid, "-0", workspace.path())?.success(),
            "OCG process survived panic"
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn forced_shutdown_reaps_unresponsive_child() -> Result<()> {
    let mut smoke = SmokeHarness::start(Reply::Text(vec!["OK".into()]))?;
    let directory = smoke.path("");
    let process = smoke.process.as_mut().ok_or("no OCG process")?;
    let pid = process.child.id();
    assert!(process_signal(pid, "-STOP", &directory)?.success());
    process.timeouts.shutdown = process.timeouts.readiness_probe;
    assert!(
        !process.stop()?,
        "stopped OCG unexpectedly handled graceful shutdown"
    );
    assert!(process.status()?.is_some(), "OCG was not reaped");
    assert!(!process_signal(pid, "-0", &directory)?.success());
    // This probe expects a forced exit; normal scenarios require exit success.
    smoke.provider.shutdown()?;
    smoke.finished = true;
    Ok(())
}
