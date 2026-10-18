use super::*;

/// Reads one response stream, then admits its tool calls only as far as
/// `conversation` allowed them. The configured timeout applies only while
/// waiting for the next provider chunk; parsing and persistence are outside
/// that interval.
pub(crate) async fn read_response(
    payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    sink: Option<&mut dyn ProviderOutputSink>,
    conversation: &ChatConversation,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut capture = UsageCapture::default();
    let result = parse_stream(
        payload,
        status_code,
        started,
        request_id,
        config,
        sink,
        &mut capture,
    )
    .await?;
    if let Err(error) = conversation.admit_tool_calls(&result.tool_calls) {
        return Err(incomplete_tool_calls(
            error,
            ResponseFacts {
                status_code: result.status_code,
                finish_reason: result.finish_reason,
                raw_finish_reason: result.raw_finish_reason,
            },
        ));
    }
    Ok(result)
}

pub(super) async fn parse_stream(
    payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    sink: Option<&mut dyn ProviderOutputSink>,
    capture: &mut UsageCapture,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut output = StreamOutput::new(sink);
    let result = parse_stream_body(
        payload,
        status_code,
        started,
        request_id,
        config,
        capture,
        &mut output,
    )
    .await;
    let flushed = output.flush().await;
    match (result, flushed) {
        (result, Ok(())) => result,
        (Ok(result), Err(error)) => Err(local_stop(
            &error.message,
            status_code,
            result.finish_reason,
            result.raw_finish_reason.as_deref(),
        )),
        (Err(stream), Err(error)) => Err(ProviderExecutionError::OutputPersistence {
            message: error.message,
            stream: Box::new(stream),
        }),
    }
}

struct StreamOutput<'a> {
    sink: Option<&'a mut dyn ProviderOutputSink>,
    text: String,
    started: Option<Instant>,
}

impl<'a> StreamOutput<'a> {
    fn new(sink: Option<&'a mut dyn ProviderOutputSink>) -> Self {
        Self {
            sink,
            text: String::with_capacity(STREAM_OUTPUT_BUFFER_BYTES),
            started: None,
        }
    }

    fn is_pending(&self) -> bool {
        !self.text.is_empty()
    }

    fn flush_delay(&self) -> Duration {
        self.started
            .map_or(STREAM_OUTPUT_FLUSH_INTERVAL, |started| {
                STREAM_OUTPUT_FLUSH_INTERVAL.saturating_sub(started.elapsed())
            })
    }

    async fn buffer(&mut self, mut delta: &str) -> Result<(), OutputSinkError> {
        if self.sink.is_none() {
            return Ok(());
        }
        while !delta.is_empty() {
            if self.text.is_empty() {
                self.started = Some(Instant::now());
            }
            let available = STREAM_OUTPUT_BUFFER_BYTES - self.text.len();
            let split = floor_char_boundary(delta, available);
            if split == 0 {
                self.flush().await?;
                continue;
            }
            self.text.push_str(&delta[..split]);
            delta = &delta[split..];
            if self.text.len() == STREAM_OUTPUT_BUFFER_BYTES || !delta.is_empty() {
                self.flush().await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), OutputSinkError> {
        if self.text.is_empty() {
            return Ok(());
        }
        let text = std::mem::take(&mut self.text);
        self.started = None;
        let Some(sink) = self.sink.as_deref_mut() else {
            return Ok(());
        };
        sink.append(&text).await
    }
}

impl ProviderOutputSink for StreamOutput<'_> {
    fn append<'a>(
        &'a mut self,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async move { self.buffer(text).await })
    }

    fn observe<'a>(
        &'a mut self,
        observation: ProviderObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            if let Some(sink) = self.sink.as_deref_mut() {
                sink.observe(observation).await;
            }
        })
    }
}

fn floor_char_boundary(text: &str, maximum: usize) -> usize {
    let mut split = text.len().min(maximum);
    while split > 0 && !text.is_char_boundary(split) {
        split -= 1;
    }
    split
}

enum StreamWait {
    Cancelled,
    Flush,
    Idle,
    Chunk(Option<Result<ntex::util::Bytes, ntex::http::error::PayloadError>>),
}

async fn next_stream_event(
    payload: &mut Payload,
    config: &ProviderExecutionConfig,
    output: &StreamOutput<'_>,
    idle_remaining: Duration,
) -> StreamWait {
    #[cfg(test)]
    if ntex::rt::System::try_current().is_none() {
        return match select(config.cancellation.cancelled(), payload.recv()).await {
            Either::Left(()) => StreamWait::Cancelled,
            Either::Right(chunk) => StreamWait::Chunk(chunk),
        };
    }
    let chunk = ntex::time::timeout(idle_remaining, payload.recv());
    if output.is_pending() {
        match select(
            config.cancellation.cancelled(),
            select(ntex::time::sleep(output.flush_delay()), chunk),
        )
        .await
        {
            Either::Left(()) => StreamWait::Cancelled,
            Either::Right(Either::Left(())) => StreamWait::Flush,
            Either::Right(Either::Right(Ok(chunk))) => StreamWait::Chunk(chunk),
            Either::Right(Either::Right(Err(()))) => StreamWait::Idle,
        }
    } else {
        match select(config.cancellation.cancelled(), chunk).await {
            Either::Left(()) => StreamWait::Cancelled,
            Either::Right(Ok(chunk)) => StreamWait::Chunk(chunk),
            Either::Right(Err(())) => StreamWait::Idle,
        }
    }
}

async fn parse_stream_body(
    mut payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    capture: &mut UsageCapture,
    output: &mut StreamOutput<'_>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut buffer = Vec::with_capacity(MAX_SSE_LINE_BYTES);
    let mut response = StreamedResponse::default();
    let mut saw_event = false;
    let mut first_byte = None;
    let mut idle_waited = Duration::ZERO;
    let mut saw_delta = false;

    'stream: loop {
        let remaining = if !saw_delta {
            config
                .first_delta_timeout
                .map(|timeout| timeout.saturating_sub(started.elapsed()))
        } else {
            None
        }
        .unwrap_or_else(|| config.timeout.saturating_sub(idle_waited));
        if remaining.is_zero() {
            return Err(response.unknown(
                "provider stream was idle after it may have been delivered",
                status_code,
            ));
        }
        let wait_started = Instant::now();
        let chunk = match next_stream_event(&mut payload, config, output, remaining).await {
            StreamWait::Cancelled => {
                return Err(local_stop(
                    "provider execution was cancelled while streaming",
                    status_code,
                    response.finish_reason,
                    response.raw_finish_reason.as_deref(),
                ));
            }
            StreamWait::Flush => {
                idle_waited = idle_waited.saturating_add(wait_started.elapsed());
                output.flush().await.map_err(|error| {
                    local_stop(
                        &error.message,
                        status_code,
                        response.finish_reason,
                        response.raw_finish_reason.as_deref(),
                    )
                })?;
                continue;
            }
            StreamWait::Idle => {
                return Err(response.unknown(
                    "provider stream was idle after it may have been delivered",
                    status_code,
                ));
            }
            StreamWait::Chunk(None) => break,
            StreamWait::Chunk(Some(chunk)) => {
                idle_waited = Duration::ZERO;
                chunk
            }
        };
        if config.cancellation.is_cancelled() {
            return Err(local_stop(
                "provider execution was cancelled while streaming",
                status_code,
                response.finish_reason,
                response.raw_finish_reason.as_deref(),
            ));
        }
        let chunk = chunk.map_err(|error| response.unknown(error.to_string(), status_code))?;
        first_byte.get_or_insert_with(Instant::now);
        for byte in chunk {
            buffer.push(byte);
            if buffer.len() > MAX_SSE_LINE_BYTES {
                return Err(
                    response.incomplete("provider SSE line exceeded its limit", status_code)
                );
            }
            if byte != b'\n' {
                continue;
            }
            let line = std::mem::take(&mut buffer);
            let line = line.strip_suffix(b"\n").unwrap_or(&line);
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.len() > MAX_SSE_LINE_BYTES {
                return Err(
                    response.incomplete("provider SSE line exceeded its limit", status_code)
                );
            }
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let data = trim_ascii(data);
            // `[DONE]` ends the response whether or not the connection
            // closes; whatever follows it is not part of the response.
            if data == b"[DONE]" {
                break 'stream;
            }
            let event: Value = serde_json::from_slice(data).map_err(|error| {
                response.incomplete(
                    format!("malformed provider SSE event: {error}"),
                    status_code,
                )
            })?;
            saw_event = true;
            // The usage is taken before anything else of the event, so
            // neither a refused part of the event nor a failed delta write
            // loses its counters.
            if let Some(reported) = reported_usage(&event) {
                response.usage = reported.usage.clone();
                capture.record(Some(output), reported).await;
            }
            let delta = consume_event(&event, &mut response, config.max_output_bytes).map_err(
                |refused| match refused {
                    Refused::ToolCall(error) => {
                        incomplete_tool_calls(error, response.facts(status_code))
                    }
                    Refused::OutputLimit => local_stop(
                        "provider output exceeded its limit",
                        status_code,
                        response.finish_reason,
                        response.raw_finish_reason.as_deref(),
                    ),
                },
            )?;
            if event
                .get("choices")
                .and_then(Value::as_array)
                .and_then(|choices| choices.first())
                .and_then(|choice| choice.get("delta"))
                .is_some_and(|delta| {
                    ["content", "reasoning_content"].iter().any(|key| {
                        delta
                            .get(key)
                            .and_then(Value::as_str)
                            .is_some_and(|text| !text.is_empty())
                    }) || delta
                        .get("tool_calls")
                        .and_then(Value::as_array)
                        .is_some_and(|calls| !calls.is_empty())
                })
            {
                saw_delta = true;
            }
            if let Some(delta) = delta.as_deref() {
                output.buffer(delta).await.map_err(|error| {
                    local_stop(
                        &error.message,
                        status_code,
                        response.finish_reason,
                        response.raw_finish_reason.as_deref(),
                    )
                })?;
            }
        }
    }
    // A stream cut short after cancellation is the local stop, not the provider's incomplete stream.
    let cut_short = !buffer.is_empty() || !saw_event || response.finish_reason.is_none();
    if cut_short && config.cancellation.is_cancelled() {
        return Err(local_stop(
            "provider execution was cancelled before the stream finished",
            status_code,
            response.finish_reason,
            response.raw_finish_reason.as_deref(),
        ));
    }
    if !buffer.is_empty() {
        return Err(response.incomplete(
            "provider stream ended with an incomplete SSE frame",
            status_code,
        ));
    }
    if !saw_event || response.finish_reason.is_none() {
        return Err(
            response.incomplete("provider stream ended without a finish event", status_code)
        );
    }
    let tool_calls = response
        .tool_calls
        .finish(response.finish_reason)
        .map_err(|error| {
            let facts = ResponseFacts {
                status_code,
                finish_reason: response.finish_reason,
                raw_finish_reason: response.raw_finish_reason.clone(),
            };
            incomplete_tool_calls(error, facts)
        })?;
    let finished_in = started.elapsed();
    Ok(ProviderExecutionResult {
        text: response.text,
        reasoning: response.reasoning,
        tool_calls,
        usage: response.usage,
        finish_reason: response.finish_reason,
        raw_finish_reason: response.raw_finish_reason,
        status_code,
        request_id,
        ttfb: first_byte.map_or(finished_in, |first| first.duration_since(started)),
        finished_in,
    })
}

/// A stop on this side after the response head arrived, keeping the facts seen so far.
fn local_stop(
    message: &str,
    status_code: u16,
    finish_reason: Option<ChatFinishReason>,
    raw_finish_reason: Option<&str>,
) -> ProviderExecutionError {
    ProviderExecutionError::Stopped {
        message: message.to_owned(),
        facts: ResponseFacts {
            status_code,
            finish_reason,
            raw_finish_reason: raw_finish_reason.map(str::to_owned),
        },
    }
}

/// What a response stream has produced so far. Only the text is streamed
/// out; reasoning and tool-call fragments wait for the end of the stream.
#[derive(Default)]
struct StreamedResponse {
    text: String,
    reasoning: String,
    tool_calls: ToolCallAssembler,
    usage: NormalizedUsage,
    finish_reason: Option<ChatFinishReason>,
    raw_finish_reason: Option<String>,
}

impl StreamedResponse {
    /// The bytes the response limit covers: text, reasoning and tool calls.
    fn output_bytes(&self) -> usize {
        self.text
            .len()
            .saturating_add(self.reasoning.len())
            .saturating_add(self.tool_calls.bytes())
    }

    /// The facts accepted so far, under the status of the response head.
    fn facts(&self, status_code: u16) -> ResponseFacts {
        ResponseFacts {
            status_code,
            finish_reason: self.finish_reason,
            raw_finish_reason: self.raw_finish_reason.clone(),
        }
    }

    /// A protocol fault of the response, with the facts accepted before it.
    fn incomplete(&self, message: impl Into<String>, status_code: u16) -> ProviderExecutionError {
        ProviderExecutionError::Incomplete {
            message: message.into(),
            facts: self.facts(status_code),
        }
    }

    /// A transport failure or Provider silence after the response head,
    /// with the facts accepted before it.
    fn unknown(&self, message: impl Into<String>, status_code: u16) -> ProviderExecutionError {
        ProviderExecutionError::Unknown {
            message: message.into(),
            facts: Some(self.facts(status_code)),
            finished: self.finish_reason.is_some() && self.tool_calls.is_empty(),
        }
    }
}

/// Why one stream event was refused.
enum Refused {
    /// A tool call fragment the Provider sent is malformed or conflicting.
    ToolCall(InvalidToolCall),
    /// Taking the event would exceed the configured output limit, a local
    /// protection rather than a fault of the Provider.
    OutputLimit,
}

/// Folds the choice of one event into `response` and returns its text delta.
/// The event's usage is not read here, see [`reported_usage`]. An invalid
/// tool-call fragment rejects the whole choice, so none of its text and no
/// finish reason of it is taken either.
fn consume_event(
    event: &Value,
    response: &mut StreamedResponse,
    max_output_bytes: usize,
) -> Result<Option<String>, Refused> {
    let Some(choice) = event
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
    else {
        return Ok(None);
    };
    let mut text = None;
    if let Some(delta) = choice.get("delta") {
        if let Some(deltas) = delta.get("tool_calls") {
            response
                .tool_calls
                .apply(deltas)
                .map_err(Refused::ToolCall)?;
            if response.output_bytes() > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
        }
        if let Some(value) = delta.get("reasoning_content").and_then(Value::as_str) {
            if response.output_bytes().saturating_add(value.len()) > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
            response.reasoning.push_str(value);
        }
        if let Some(value) = delta.get("content").and_then(Value::as_str) {
            if response.output_bytes().saturating_add(value.len()) > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
            response.text.push_str(value);
            text = Some(value.to_owned());
        }
    }
    if let Some(raw) = choice.get("finish_reason").and_then(Value::as_str) {
        response.raw_finish_reason = Some(raw.to_owned());
        response.finish_reason = Some(ChatFinishReason::from_openai_str(raw));
    }
    Ok(text)
}
