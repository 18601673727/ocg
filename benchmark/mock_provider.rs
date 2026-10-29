use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ntex::util::{Bytes, Stream};
use ntex::web::{self, App, HttpResponse, HttpServer};
use serde_json::{Value, json};

const JOB_KEY_PREFIX: &str = "OCG benchmark job=";

#[derive(Clone, Debug)]
pub struct MockProviderConfig {
    pub model: String,
    pub answer_tokens: usize,
    pub tool_name: String,
    pub tool_path: String,
    pub pace: Duration,
    pub gate_timeout: Duration,
    pub fail_requests: bool,
}

impl Default for MockProviderConfig {
    fn default() -> Self {
        Self {
            model: "benchmark-mock".into(),
            answer_tokens: 4,
            tool_name: "workspace_read_file".into(),
            tool_path: "probe.txt".into(),
            pace: Duration::from_millis(5),
            gate_timeout: Duration::from_secs(30),
            fail_requests: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct ProviderCounters {
    pub requests: AtomicU64,
    pub chunks: AtomicU64,
    pub tool_calls: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderCounterSnapshot {
    pub requests: u64,
    pub chunks: u64,
    pub tool_calls: u64,
}

impl ProviderCounters {
    pub fn snapshot(&self) -> ProviderCounterSnapshot {
        ProviderCounterSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            chunks: self.chunks.load(Ordering::Relaxed),
            tool_calls: self.tool_calls.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct ControlState {
    gates: BTreeMap<String, (flume::Sender<()>, flume::Receiver<()>)>,
    emitted: BTreeMap<String, Instant>,
}

#[derive(Default)]
struct StreamControl {
    state: Mutex<ControlState>,
}

impl StreamControl {
    fn receiver(&self, job_key: &str) -> flume::Receiver<()> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .gates
            .entry(job_key.to_owned())
            .or_insert_with(|| flume::bounded(1))
            .1
            .clone()
    }

    fn release(&self, job_key: &str) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (sender, _) = state
            .gates
            .entry(job_key.to_owned())
            .or_insert_with(|| flume::bounded(1));
        sender
            .try_send(())
            .map_err(|error| format!("could not release provider markers for {job_key}: {error}"))
    }

    fn emitted(&self, marker: &str, at: Instant) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.emitted.insert(marker.to_owned(), at).is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("duplicate upstream marker {marker}"),
            ));
        }
        Ok(())
    }

    fn emitted_at(&self, marker: &str) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .emitted
            .get(marker)
            .copied()
    }
}

pub struct MockProvider {
    address: std::net::SocketAddr,
    counters: Arc<ProviderCounters>,
    control: Arc<StreamControl>,
    server: ntex::rt::JoinHandle<std::io::Result<()>>,
    handle: ntex::server::Server,
}

impl MockProvider {
    pub fn start(
        config: MockProviderConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if config.answer_tokens == 0 {
            return Err("the mock provider needs at least one content marker".into());
        }
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let counters = Arc::new(ProviderCounters::default());
        let control = Arc::new(StreamControl::default());
        let served = Arc::clone(&counters);
        let streaming = Arc::clone(&control);
        let workers = std::thread::available_parallelism().map_or(4, |value| value.get());

        let server = HttpServer::new(async move || {
            let config = config.clone();
            let served = served.clone();
            let streaming = streaming.clone();
            App::new().default_service(web::to(async move |body: web::types::Json<Value>| {
                if config.fail_requests {
                    served.requests.fetch_add(1, Ordering::Relaxed);
                    return Ok::<_, ntex::web::Error>(HttpResponse::InternalServerError().json(
                        &json!({"error": {"message": "benchmark-injected provider failure"}}),
                    ));
                }
                let plan =
                    answer(&body, &config, &served).map_err(ntex::web::error::ErrorBadRequest)?;
                if let Some(job_key) = &plan.wait_for {
                    let receiver = streaming.receiver(job_key);
                    ntex::time::timeout(config.gate_timeout, receiver.recv_async())
                        .await
                        .map_err(|_| {
                            ntex::web::error::ErrorRequestTimeout(
                                "downstream subscription was not established",
                            )
                        })?
                        .map_err(|error| {
                            ntex::web::error::ErrorInternalServerError(error.to_string())
                        })?;
                }
                Ok::<_, ntex::web::Error>(
                    HttpResponse::Ok()
                        .content_type("text/event-stream")
                        .streaming(ProviderBody::new(
                            plan,
                            config.pace,
                            served.clone(),
                            streaming.clone(),
                        )),
                )
            }))
        })
        .workers(workers)
        .disable_signals()
        .listen(listener)?
        .run();

        let finished = ntex::rt::spawn(server.clone());
        Ok(Self {
            address,
            counters,
            control,
            server: finished,
            handle: server,
        })
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}/v1", self.address)
    }

    pub fn counters(&self) -> &ProviderCounters {
        &self.counters
    }

    pub fn release_markers(&self, job_key: &str) -> Result<(), String> {
        self.control.release(job_key)
    }

    pub fn emitted_at(&self, marker: &str) -> Option<Instant> {
        self.control.emitted_at(marker)
    }

    pub async fn stop(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.handle.stop(true).await;
        self.server
            .await
            .map_err(|_| "the mock provider panicked")?
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(())
    }
}

enum PlannedChunk {
    Raw(String),
    Marker { job_key: String, sequence: usize },
}

struct ResponsePlan {
    wait_for: Option<String>,
    chunks: VecDeque<PlannedChunk>,
    usage_tokens: usize,
}

struct ProviderBody {
    chunks: VecDeque<PlannedChunk>,
    pace: Duration,
    sleep: Option<Pin<Box<dyn Future<Output = ()>>>>,
    counters: Arc<ProviderCounters>,
    control: Arc<StreamControl>,
    usage_tokens: usize,
    finished: bool,
}

impl ProviderBody {
    fn new(
        plan: ResponsePlan,
        pace: Duration,
        counters: Arc<ProviderCounters>,
        control: Arc<StreamControl>,
    ) -> Self {
        Self {
            chunks: plan.chunks,
            pace,
            sleep: None,
            counters,
            control,
            usage_tokens: plan.usage_tokens,
            finished: false,
        }
    }
}

impl Stream for ProviderBody {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(sleep) = self.sleep.as_mut() {
            match sleep.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => self.sleep = None,
            }
        }
        let Some(chunk) = self.chunks.pop_front() else {
            if self.finished {
                return Poll::Ready(None);
            }
            self.finished = true;
            let usage = json!({
                "choices": [],
                "usage": {"prompt_tokens": 128, "completion_tokens": self.usage_tokens},
            });
            return Poll::Ready(Some(Ok(Bytes::from(format!(
                "data: {usage}\n\ndata: [DONE]\n\n"
            )))));
        };
        let event = match chunk {
            PlannedChunk::Raw(event) => event,
            PlannedChunk::Marker { job_key, sequence } => {
                let marker = format!("OCGMARK-{job_key}-{sequence:04}");
                if let Err(error) = self.control.emitted(&marker, Instant::now()) {
                    return Poll::Ready(Some(Err(error)));
                }
                self.counters.chunks.fetch_add(1, Ordering::Relaxed);
                let chunk = json!({
                    "choices": [{
                        "index": 0,
                        "delta": {"content": format!("{marker} ")},
                        "finish_reason": null,
                    }],
                });
                if !self.chunks.is_empty() {
                    self.sleep = Some(Box::pin(ntex::time::sleep(self.pace)));
                }
                format!("data: {chunk}\n\n")
            }
        };
        Poll::Ready(Some(Ok(Bytes::from(event))))
    }
}

fn job_key(messages: &[Value]) -> Result<String, String> {
    for message in messages.iter().rev() {
        if message["role"] != "user" {
            continue;
        }
        let encoded = message["content"].to_string();
        if let Some(start) = encoded.find(JOB_KEY_PREFIX) {
            let key = encoded[start + JOB_KEY_PREFIX.len()..]
                .chars()
                .take_while(|character| character.is_ascii_alphanumeric() || *character == '-')
                .collect::<String>();
            if !key.is_empty() {
                return Ok(key);
            }
        }
    }
    Err("the current Job objective has no benchmark identity".into())
}

fn answer(
    body: &Value,
    config: &MockProviderConfig,
    counters: &ProviderCounters,
) -> Result<ResponsePlan, String> {
    let messages = body["messages"]
        .as_array()
        .ok_or_else(|| "provider request omitted messages".to_owned())?;
    let exchanges = messages
        .iter()
        .filter(|message| message["role"] == "tool" && !message["content"].is_null())
        .count();
    if exchanges > 2 {
        return Err(format!(
            "reference job requested a fourth model round ({exchanges} tools)"
        ));
    }
    let job_key = job_key(messages)?;
    counters.requests.fetch_add(1, Ordering::Relaxed);
    let mut chunks = VecDeque::new();
    if exchanges < 2 {
        counters.tool_calls.fetch_add(1, Ordering::Relaxed);
        let chunk = json!({
            "choices": [{
                "index": 0,
                "delta": {
                    "content": "Working.",
                    "tool_calls": [{
                        "index": 0,
                        "id": format!("call-{job_key}-{exchanges}"),
                        "type": "function",
                        "function": {
                            "name": config.tool_name,
                            "arguments": json!({"path": config.tool_path}).to_string(),
                        },
                    }],
                },
                "finish_reason": "tool_calls",
            }],
        });
        chunks.push_back(PlannedChunk::Raw(format!("data: {chunk}\n\n")));
        Ok(ResponsePlan {
            wait_for: None,
            chunks,
            usage_tokens: 1,
        })
    } else {
        for sequence in 0..config.answer_tokens {
            chunks.push_back(PlannedChunk::Marker {
                job_key: job_key.clone(),
                sequence,
            });
        }
        let done = json!({
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        });
        chunks.push_back(PlannedChunk::Raw(format!("data: {done}\n\n")));
        Ok(ResponsePlan {
            wait_for: Some(job_key),
            chunks,
            usage_tokens: config.answer_tokens,
        })
    }
}
