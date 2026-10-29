//! A deterministic mock provider.
//!
//! The backend talks to an OpenAI-compatible endpoint over HTTP. To measure
//! the backend rather than a network, the provider answers a fixed protocol:
//! a job's conversation is read back from the request body, and the number of
//! messages already in it decides which round the job is on. Every answer is
//! a function of the request, so two runs of the same workload see the same
//! bytes, and the timing a run measures is the backend's, not the provider's.

use ntex::web::{self, App, HttpResponse, HttpServer};
use serde_json::{Value, json};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// How the mock provider answers one request.
#[derive(Clone, Debug)]
pub struct MockProviderConfig {
    /// The model name the provider claims to serve.
    pub model: String,
    /// Tokens the final answer carries.
    pub answer_tokens: usize,
    /// The tool the provider asks for while a job still has rounds to run.
    pub tool_name: String,
    /// The file the tool call reads in the fixture workspace.
    pub tool_path: String,
}

impl Default for MockProviderConfig {
    fn default() -> Self {
        Self {
            model: "benchmark-mock".into(),
            answer_tokens: 32,
            tool_name: "workspace_read_file".into(),
            tool_path: "probe.txt".into(),
        }
    }
}

/// Counters for what the provider served.
#[derive(Debug, Default)]
pub struct ProviderCounters {
    pub requests: AtomicU64,
    pub chunks: AtomicU64,
    pub tool_calls: AtomicU64,
}

/// A running mock provider.
pub struct MockProvider {
    address: std::net::SocketAddr,
    counters: Arc<ProviderCounters>,
    server: ntex::rt::JoinHandle<std::io::Result<()>>,
    handle: ntex::server::Server,
}

impl MockProvider {
    /// Starts the provider on an address the operating system picks.
    pub fn start(
        config: MockProviderConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let counters = Arc::new(ProviderCounters::default());
        let served = Arc::clone(&counters);
        let workers = std::thread::available_parallelism().map_or(4, |value| value.get());

        let server = HttpServer::new(async move || {
            let config = config.clone();
            let served = served.clone();
            App::new().default_service(web::to(async move |body: web::types::Json<Value>| {
                let response = answer(&body, &config, &served);
                Ok::<_, ntex::web::Error>(
                    HttpResponse::Ok()
                        .content_type("text/event-stream")
                        .body(response),
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
            server: finished,
            handle: server,
        })
    }

    /// The endpoint the backend is pointed at.
    pub fn endpoint(&self) -> String {
        format!("http://{}/v1", self.address)
    }

    pub fn counters(&self) -> &ProviderCounters {
        &self.counters
    }

    /// Stops the provider and waits for its workers.
    pub async fn stop(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.handle.stop(true).await;
        self.server
            .await
            .map_err(|_| "the mock provider panicked")?
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(())
    }
}

/// The answer one request receives.
///
/// The round is decided by how many messages the conversation already holds:
/// the backend adds one user message and one assistant message per exchange,
/// so the message count says which round of the reference job this is.
fn answer(body: &Value, config: &MockProviderConfig, counters: &ProviderCounters) -> String {
    let messages = body["messages"].as_array().map_or(0, Vec::len);
    // The first message is the system prompt the backend sends; every
    // completed exchange adds the user's question and the assistant's answer.
    let exchanges = messages.saturating_sub(1) / 2;
    // The reference job asks for three model requests and two tool calls: the
    // first two requests ask for a tool, the third one closes the job.
    let asks_for_tool = exchanges < 2;

    counters.requests.fetch_add(1, Ordering::Relaxed);

    let mut body = String::new();
    if asks_for_tool {
        counters.tool_calls.fetch_add(1, Ordering::Relaxed);
        let chunk = json!({
            "choices": [{
                "index": 0,
                "delta": {
                    "content": "Working.",
                    "tool_calls": [{
                        "index": 0,
                        "id": format!("call-{exchanges}"),
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
        body.push_str(&format!("data: {chunk}\n\n"));
    } else {
        for index in 0..config.answer_tokens {
            let chunk = json!({
                "choices": [{
                    "index": 0,
                    "delta": {"content": format!("token{index} ")},
                    "finish_reason": null,
                }],
            });
            body.push_str(&format!("data: {chunk}\n\n"));
            counters.chunks.fetch_add(1, Ordering::Relaxed);
        }
        let done = json!({
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        });
        body.push_str(&format!("data: {done}\n\n"));
    }

    let usage = json!({
        "choices": [],
        "usage": {
            "prompt_tokens": 128,
            "completion_tokens": config.answer_tokens,
        },
    });
    body.push_str(&format!("data: {usage}\n\ndata: [DONE]\n\n"));
    body
}
