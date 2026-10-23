//! OpenAI-compatible provider execution for an admitted Job.

use std::sync::{
    Arc, Mutex, PoisonError, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin};

use ntex::client::{Client, ClientResponse, error::ClientError};
use ntex::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use ntex::http::{Payload, StatusCode};
use ntex::time::Millis;
use ntex::util::{Either, select};
use serde_json::{Value, json};

use crate::openai_compatible::{
    ChatConversation, ChatFinishReason, ChatMessage, CompletedToolCall, ContentPartWire,
    ImageInput, InvalidToolCall, NormalizedUsage, ProviderFailure, ToolCallAssembler,
};
use crate::persistence::{FrozenExecutionConfig, IntermediateOutputKind, LlmParameters, Timestamp};
use crate::token_usage::UsageFinality;

const DEFAULT_ENDPOINT: &str = "https://api.openai.com/v1/chat/completions";
const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
const MAX_PROVIDER_ERROR_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
const STREAM_OUTPUT_BUFFER_BYTES: usize = 4 * 1024;
const STREAM_OUTPUT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);

mod cancellation;
mod conversation;
mod exchange;
mod models;
mod observe;
mod proxy;
mod record;
pub use record::{RequestRecorder, SentRequestState};
mod stream;
mod types;
mod usage;

pub use cancellation::CancellationToken;
pub use conversation::{execute_chat, objective_conversation, objective_conversation_with_images};
pub(crate) use conversation::{ground_project, request_body};
#[cfg(test)]
use exchange::authorization;
use exchange::incomplete_tool_calls;
pub use exchange::{execute_chat_with_sink, execute_conversation};
#[cfg(test)]
pub(crate) use observe::retry_after_ms;
pub(crate) use observe::unannounced_fact;
use observe::{Redactor, announce_at, bounded_body, classify_client_error, trim_ascii};
pub(crate) use observe::{announce, rejected_failure};
pub(crate) fn model_endpoint(endpoint: &str) -> Result<String, ProviderExecutionError> {
    exchange::endpoint(endpoint, "models")
}
pub(crate) use models::{ModelListError, list_models};
pub use observe::{error_observations, metrics, result_value, success_observation};
pub(crate) use proxy::{
    initialize as initialize_proxy, log_configuration as log_proxy_configuration,
};
#[cfg(test)]
use stream::parse_stream;
pub(crate) use stream::read_response;
pub use types::{
    CredentialResolver, EnvironmentCredentialResolver, OutputSinkError, ProviderExecutionConfig,
    ProviderExecutionError, ProviderExecutionResult, ProviderFact, ProviderObservation,
    ProviderOutputSink, ResponseFacts,
};

pub(crate) use usage::UsageCapture;
use usage::reported_usage;

#[cfg(test)]
impl CancellationToken {
    /// How many children this token still registers, live or not yet
    /// pruned.
    #[cfg(test)]
    pub(crate) fn registered_children(&self) -> usize {
        self.0
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// A scripted response body, for tests that drive the real parser.
#[cfg(test)]
pub(crate) mod scripted {
    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::time::Duration;

    use ntex::http::Payload;
    use ntex::http::error::PayloadError;
    use ntex::util::{Bytes, Stream};

    use super::CancellationToken;

    type HttpResult = Result<(u16, Vec<u8>), ()>;
    type HttpScript = Box<dyn FnMut(Option<&ntex::http::header::HeaderValue>) -> HttpResult + Send>;
    static HTTP: std::sync::Mutex<std::collections::BTreeMap<String, HttpScript>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());

    pub(crate) fn install_http(
        endpoint: &str,
        script: impl FnMut(Option<&ntex::http::header::HeaderValue>) -> HttpResult + Send + 'static,
    ) {
        HTTP.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(endpoint.to_owned(), Box::new(script));
    }

    pub(crate) fn http_response(
        endpoint: &str,
        authorization: Option<&ntex::http::header::HeaderValue>,
    ) -> Option<HttpResult> {
        HTTP.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(endpoint)
            .map(|script| script(authorization))
    }

    /// One scripted step of a response body.
    pub(crate) enum Step {
        Chunk(&'static str),
        Fail,
        /// Cancels the execution, then yields the next chunk so the parser sees it.
        Cancel,
        /// Waits before yielding the next scripted step.
        Quiet(Duration),
        /// Yields nothing, ever: only a timeout ends the read.
        Stall,
    }

    struct Body {
        steps: VecDeque<Step>,
        cancellation: CancellationToken,
        quiet: Option<Pin<Box<dyn Future<Output = ()>>>>,
    }

    impl Stream for Body {
        type Item = Result<Bytes, PayloadError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            use std::task::Poll;
            if let Some(quiet) = &mut self.quiet {
                if quiet.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.quiet = None;
            }
            while let Some(step) = self.steps.pop_front() {
                match step {
                    Step::Cancel => self.cancellation.cancel(),
                    Step::Chunk(text) => {
                        return Poll::Ready(Some(Ok(Bytes::from_static(text.as_bytes()))));
                    }
                    Step::Fail => {
                        return Poll::Ready(Some(Err(PayloadError::Incomplete(Some(
                            io::Error::other("connection reset"),
                        )))));
                    }
                    Step::Quiet(duration) => {
                        self.quiet = Some(Box::pin(ntex::time::sleep(duration)));
                        if let Some(quiet) = &mut self.quiet
                            && quiet.as_mut().poll(cx).is_pending()
                        {
                            return Poll::Pending;
                        }
                        self.quiet = None;
                    }
                    Step::Stall => {
                        // Stays at the front, so every poll stalls again.
                        self.steps.push_front(Step::Stall);
                        return Poll::Pending;
                    }
                }
            }
            Poll::Ready(None)
        }
    }

    pub(crate) fn payload(steps: Vec<Step>, cancellation: CancellationToken) -> Payload {
        Payload::from_stream(Body {
            steps: steps.into(),
            cancellation,
            quiet: None,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Instant;

    use super::scripted::{Step, payload};
    use super::*;
    use crate::persistence::{AvailabilityClassification, AvailabilityObservation};
    use crate::test_support::wait;

    #[test]
    fn provider_network_reasoning_alias_is_private_and_counts_as_first_delta() {
        crate::test_support::on_system(|| async {
            let config = ProviderExecutionConfig {
                timeout: Duration::from_millis(20),
                first_delta_timeout: Some(Duration::from_secs(1)),
                ..Default::default()
            };
            let alias = "data: {\"choices\":[{\"delta\":{\"reasoning\":\"think\"}}]}\n\n";
            let mut sink = RecordingSink::default();
            let result = parse_stream(
                payload(
                    vec![Step::Chunk(alias), Step::Chunk(STOP)],
                    config.cancellation.clone(),
                ),
                200,
                Instant::now(),
                None,
                &config,
                Some(&mut sink),
                &mut UsageCapture::default(),
            )
            .await
            .expect("reasoning stream");
            assert_eq!(result.reasoning, "think");
            assert_eq!(sink.deltas, ["think"]);
            let error = parse_stream(
                payload(
                    vec![
                        Step::Chunk(alias),
                        Step::Quiet(Duration::from_millis(100)),
                        Step::Chunk(STOP),
                    ],
                    config.cancellation.clone(),
                ),
                200,
                Instant::now(),
                None,
                &config,
                None,
                &mut UsageCapture::default(),
            )
            .await
            .expect_err("later chunks use the idle timeout");
            assert!(matches!(error, ProviderExecutionError::Unknown { .. }));
        });
    }

    #[test]
    fn provider_network_reasoning_content_supersedes_the_reasoning_alias() {
        let both = "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\",\"reasoning\":\"think\"}}]}\n\n";
        let result =
            parse(vec![Step::Chunk(both), Step::Chunk(STOP)], None).expect("reasoning stream");
        assert_eq!(result.reasoning, "think");
        // The alias is not read when `reasoning_content` is present, so the
        // output limit is charged once, not twice.
        let config = ProviderExecutionConfig {
            max_output_bytes: "think".len(),
            ..ProviderExecutionConfig::default()
        };
        let result = wait(parse_stream(
            payload(
                vec![Step::Chunk(both), Step::Chunk(STOP)],
                config.cancellation.clone(),
            ),
            200,
            Instant::now(),
            None,
            &config,
            None,
            &mut UsageCapture::default(),
        ))
        .expect("reasoning_content fits the limit once");
        assert_eq!(result.reasoning, "think");
    }

    #[test]
    fn provider_network_stream_errors_end_immediately_with_provider_message() {
        for (error, rejected) in [
            (json!({"message":"bad key", "status":401}), true),
            (json!({"message":"bad key", "status_code":"403"}), true),
            (json!({"message":"bad key", "code":429}), true),
            (json!({"message":"bad key", "code":"400"}), true),
            (json!({"message":"bad key", "code":500}), false),
            (json!({"message":"bad key", "code":"overloaded"}), false),
        ] {
            let event = leaked(format!("data: {}\n\n", json!({"error":error})));
            let mut sink = RecordingSink::default();
            let result = parse(
                vec![Step::Chunk(CONTENT), Step::Chunk(event), Step::Stall],
                Some(&mut sink),
            );
            match result.expect_err("provider error ends the stream") {
                ProviderExecutionError::Rejected { failure } if rejected => {
                    assert_eq!(failure.message, "bad key");
                    assert!(!failure.retryable, "streamed output must not be replayed");
                }
                ProviderExecutionError::Unknown { message, .. } if !rejected => {
                    assert_eq!(message, "bad key")
                }
                other => panic!("unexpected error: {other:?}"),
            }
            assert_eq!(sink.deltas.concat(), "hi");
        }
    }

    #[test]
    fn provider_network_error_body_variants_preserve_message_and_code() {
        for (body, message, code) in [
            (
                r#"{"error":{"message":"bad key","code":401}}"#,
                "bad key",
                Some("401"),
            ),
            (
                r#"{"error":{"message":"bad key","code":"auth"}}"#,
                "bad key",
                Some("auth"),
            ),
            (r#"{"message":"gateway failed"}"#, "gateway failed", None),
            (r#"{"detail":"gateway detail"}"#, "gateway detail", None),
            (
                r#"{"error":{"code":429},"message":"busy"}"#,
                "busy",
                Some("429"),
            ),
        ] {
            assert_eq!(
                observe::parse_provider_error(body),
                Some((message.to_owned(), code.map(str::to_owned)))
            );
        }
        assert!(observe::parse_provider_error("{}").is_none());
    }

    #[test]
    fn provider_network_single_tool_call_variants_remain_unambiguous() {
        for index in [None, Some(7)] {
            let mut start = json!({"id":"call", "function":{"name":"f", "arguments":""}});
            if let Some(index) = index {
                start["index"] = json!(index);
            }
            let result = read(
                vec![
                    Step::Chunk(tool_delta(leaked(json!([start]).to_string()))),
                    Step::Chunk(tool_delta(r#"[{"function":{"name":"f","arguments":""}}]"#)),
                    Step::Chunk(TOOL_CALLS),
                ],
                &offering(&["f"]),
                None,
            )
            .expect("single call accepts missing index and echoed name");
            assert_eq!(
                result.tool_calls,
                [call(index.unwrap_or(0), "call", "f", "{}")]
            );
        }
        let message = incomplete_message(read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"id":"a","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":1,"id":"b","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            None,
        ));
        assert!(message.contains("no valid `index`"));
    }

    const CONTENT: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";
    const STOP: &str = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";

    #[derive(Default)]
    struct RecordingSink {
        deltas: Vec<String>,
        fail: Option<&'static str>,
        /// Successful appends before `fail` starts refusing, so a test can
        /// fail the final flush alone.
        fail_after_appends: Option<usize>,
        observations: Vec<ProviderObservation>,
    }

    impl RecordingSink {
        /// The usage facts received, with the finality each was given.
        fn usages(&self) -> Vec<(UsageFinality, NormalizedUsage)> {
            self.observations
                .iter()
                .filter_map(|observation| match &observation.fact {
                    ProviderFact::Usage { usage, finality } => Some((*finality, usage.clone())),
                    _ => None,
                })
                .collect()
        }
    }

    impl ProviderOutputSink for RecordingSink {
        fn append<'a>(
            &'a mut self,
            _kind: IntermediateOutputKind,
            text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
            Box::pin(async move {
                match self.fail {
                    Some(message)
                        if self
                            .fail_after_appends
                            .is_none_or(|count| self.deltas.len() >= count) =>
                    {
                        Err(OutputSinkError {
                            message: message.to_owned(),
                        })
                    }
                    _ => {
                        self.deltas.push(text.to_owned());
                        Ok(())
                    }
                }
            })
        }

        fn observe<'a>(
            &'a mut self,
            observation: ProviderObservation,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
            Box::pin(async move { self.observations.push(observation) })
        }
    }

    fn parse(
        steps: Vec<Step>,
        sink: Option<&mut dyn ProviderOutputSink>,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        let config = ProviderExecutionConfig::default();
        wait(parse_stream(
            payload(steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            sink,
            &mut UsageCapture::default(),
        ))
    }

    fn facts(finish_reason: Option<ChatFinishReason>, raw: Option<&str>) -> ResponseFacts {
        ResponseFacts {
            status_code: 200,
            finish_reason,
            raw_finish_reason: raw.map(str::to_owned),
        }
    }

    #[test]
    fn a_complete_stream_coalesces_deltas_and_reconstructs_exact_text() {
        let mut sink = RecordingSink::default();
        let result = parse(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\" 世界\"}}]}\n\n"),
                Step::Chunk(STOP),
            ],
            Some(&mut sink),
        )
        .expect("a finished stream succeeds");
        assert_eq!(result.text, "hi 世界");
        assert_eq!(sink.deltas, ["hi 世界"]);
    }

    #[test]
    fn a_large_delta_is_split_at_utf8_boundaries_with_a_strict_bound() {
        let content = "世".repeat(STREAM_OUTPUT_BUFFER_BYTES);
        let event = leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":{}}}}}]}}\n\n",
            serde_json::to_string(&content).expect("JSON string")
        ));
        let mut sink = RecordingSink::default();
        let result = parse(vec![Step::Chunk(event), Step::Chunk(STOP)], Some(&mut sink))
            .expect("a finished stream succeeds");
        assert_eq!(sink.deltas.concat(), content);
        assert_eq!(result.text, content);
        assert!(
            sink.deltas
                .iter()
                .all(|delta| delta.len() <= STREAM_OUTPUT_BUFFER_BYTES)
        );
        assert!(sink.deltas.len() > 1);
    }

    #[test]
    fn flush_boundaries_preserve_order() {
        let first = "a".repeat(STREAM_OUTPUT_BUFFER_BYTES - 1);
        let event = |text: &str| {
            leaked(format!(
                "data: {{\"choices\":[{{\"delta\":{{\"content\":{}}}}}]}}\n\n",
                serde_json::to_string(text).expect("JSON string")
            ))
        };
        let mut sink = RecordingSink::default();
        let result = parse(
            vec![
                Step::Chunk(event(&first)),
                Step::Chunk(event("世界")),
                Step::Chunk(event("z")),
                Step::Chunk(STOP),
            ],
            Some(&mut sink),
        )
        .expect("a finished stream succeeds");
        assert_eq!(sink.deltas.concat(), result.text);
        assert_eq!(result.text, format!("{first}世界z"));
        assert!(sink.deltas.len() >= 2);
    }

    #[test]
    fn a_quiet_stream_flushes_pending_output_before_the_next_chunk() {
        let sink =
            ntex::rt::System::new("quiet-flush-test", ntex::rt::DefaultRuntime).block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_secs(2),
                    ..ProviderExecutionConfig::default()
                };
                let mut sink = RecordingSink::default();
                parse_stream(
                    payload(
                        vec![
                            Step::Chunk(CONTENT),
                            Step::Quiet(STREAM_OUTPUT_FLUSH_INTERVAL + Duration::from_millis(100)),
                            Step::Cancel,
                        ],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await
                .expect_err("the scripted cancellation stops the stream");
                sink
            });
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn a_cancelled_stream_keeps_the_response_facts_it_already_had() {
        let error = parse(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Cancel,
                Step::Chunk(CONTENT),
            ],
            None,
        )
        .expect_err("a cancelled stream stops");
        match error {
            ProviderExecutionError::Stopped { facts, .. } => {
                assert_eq!(
                    facts,
                    self::facts(Some(ChatFinishReason::Stop), Some("stop"))
                )
            }
            other => panic!("cancellation after send is a local stop, got {other:?}"),
        }
    }

    #[test]
    fn a_cancelled_stream_that_ends_early_is_a_local_stop_not_an_incomplete_stream() {
        let error = parse(vec![Step::Chunk(CONTENT), Step::Cancel], None)
            .expect_err("a cancelled stream stops");
        match error {
            ProviderExecutionError::Stopped { facts, .. } => {
                assert_eq!(facts, self::facts(None, None))
            }
            other => panic!("a cut-short stream after cancellation is local, got {other:?}"),
        }
    }

    #[test]
    fn a_cancellation_before_the_finish_keeps_only_the_status() {
        let error = parse(
            vec![Step::Chunk(CONTENT), Step::Cancel, Step::Chunk(STOP)],
            None,
        )
        .expect_err("a cancelled stream stops");
        match error {
            ProviderExecutionError::Stopped { facts, .. } => {
                assert_eq!(facts, self::facts(None, None))
            }
            other => panic!("cancellation after send is a local stop, got {other:?}"),
        }
    }

    #[test]
    fn a_local_output_failure_keeps_the_completed_response_and_its_facts() {
        let mut sink = RecordingSink {
            fail: Some("database could not persist provider output"),
            ..RecordingSink::default()
        };
        let error = parse(
            vec![Step::Chunk(
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n",
            )],
            Some(&mut sink),
        )
        .expect_err("a sink failure stops the stream");
        match error {
            ProviderExecutionError::CompletionUnpersisted { message, result } => {
                assert_eq!(message, "database could not persist provider output");
                assert_eq!(result.text, "hi");
                assert_eq!(
                    (
                        result.status_code,
                        result.finish_reason,
                        result.raw_finish_reason
                    ),
                    (200, Some(ChatFinishReason::Stop), Some("stop".to_owned()))
                );
            }
            other => panic!("a completed response keeps its result, got {other:?}"),
        }
    }

    #[test]
    fn a_failed_final_flush_keeps_text_reasoning_and_tool_calls_as_evidence() {
        let size = STREAM_OUTPUT_BUFFER_BYTES;
        let text = "x".repeat(size);
        let first = leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\n"
        ));
        let mut sink = RecordingSink {
            fail: Some("database could not persist provider output"),
            fail_after_appends: Some(1),
            ..RecordingSink::default()
        };
        let error = read(
            vec![
                Step::Chunk(first),
                Step::Chunk(
                    "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n",
                ),
                Step::Chunk(tool_delta(
                    r#"[{"id":"call","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            Some(&mut sink),
        )
        .expect_err("only the final flush fails");
        match error {
            ProviderExecutionError::CompletionUnpersisted { message, result } => {
                assert_eq!(message, "database could not persist provider output");
                assert_eq!(result.text, text);
                assert_eq!(result.reasoning, "think");
                assert_eq!(result.tool_calls.len(), 1);
                assert_eq!(result.finish_reason, Some(ChatFinishReason::ToolCalls));
                assert!(result.tool_calls[0].name == "f");
            }
            other => panic!("the completed response is kept, got {other:?}"),
        }
        assert_eq!(
            sink.deltas,
            [text],
            "the prefix that was persisted once is not written again"
        );
    }

    #[test]
    fn a_transport_failure_mid_stream_stays_an_unknown_provider_outcome() {
        let mut sink = RecordingSink::default();
        let error = parse(vec![Step::Chunk(CONTENT), Step::Fail], Some(&mut sink))
            .expect_err("a broken body fails");
        assert!(
            matches!(error, ProviderExecutionError::Unknown { .. }),
            "a transport failure is reported as it occurred, got {error:?}"
        );
        assert_eq!(sink.deltas, ["hi"], "parsed output is flushed on failure");
    }

    #[test]
    fn a_stream_without_a_finish_event_stays_incomplete() {
        let mut sink = RecordingSink::default();
        let error =
            parse(vec![Step::Chunk(CONTENT)], Some(&mut sink)).expect_err("no finish event");
        assert!(matches!(error, ProviderExecutionError::Incomplete { .. }));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn malformed_sse_flushes_valid_prior_output_without_changing_the_outcome() {
        let mut sink = RecordingSink::default();
        let error = parse(
            vec![Step::Chunk(CONTENT), Step::Chunk("data: {nope}\n\n")],
            Some(&mut sink),
        )
        .expect_err("malformed SSE is incomplete");
        assert!(matches!(error, ProviderExecutionError::Incomplete { .. }));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn a_flush_failure_reports_both_the_stream_outcome_and_local_persistence_gap() {
        let mut sink = RecordingSink {
            fail: Some("unknown durable outcome"),
            ..RecordingSink::default()
        };
        let error = parse(vec![Step::Chunk(CONTENT), Step::Fail], Some(&mut sink))
            .expect_err("transport and persistence both fail");
        match error {
            ProviderExecutionError::OutputPersistence { message, stream } => {
                assert_eq!(message, "unknown durable outcome");
                assert!(matches!(*stream, ProviderExecutionError::Unknown { .. }));
            }
            other => panic!("both outcomes remain visible, got {other:?}"),
        }
        assert!(
            sink.deltas.is_empty(),
            "the uncertain write is never retried"
        );
    }

    #[test]
    fn output_limit_termination_flushes_only_the_accepted_prefix() {
        let config = ProviderExecutionConfig {
            max_output_bytes: 2,
            ..ProviderExecutionConfig::default()
        };
        let mut sink = RecordingSink::default();
        let error = wait(parse_stream(
            payload(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"),
                ],
                config.cancellation.clone(),
            ),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &mut UsageCapture::default(),
        ))
        .expect_err("the output limit stops the stream");
        assert!(
            matches!(&error, ProviderExecutionError::Stopped { message, facts: seen } if message.contains("exceeded its limit") && *seen == facts(None, None)),
            "the output limit is a local stop, got {error:?}"
        );
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn usage_in_the_event_that_reaches_the_output_limit_is_still_observed() {
        let usage = "{\"prompt_tokens\":9,\"completion_tokens\":3,\"extra\":[1,\"a\"]}";
        let cases = [
            ("text", r#"{"content":"x"}"#),
            ("reasoning", r#"{"reasoning_content":"x"}"#),
            ("tool call", r#"{"tool_calls":[{"index":0,"id":"c"}]}"#),
        ];
        for (case, delta) in cases {
            let config = ProviderExecutionConfig {
                max_output_bytes: 2,
                ..ProviderExecutionConfig::default()
            };
            let mut sink = RecordingSink::default();
            let trigger = leaked(format!(
                "data: {{\"choices\":[{{\"delta\":{delta}}}],\"usage\":{usage}}}\n\n"
            ));
            let error = wait(parse_stream(
                payload(
                    vec![
                        Step::Chunk(CONTENT),
                        Step::Chunk(trigger),
                        Step::Chunk(STOP),
                    ],
                    config.cancellation.clone(),
                ),
                200,
                Instant::now(),
                None,
                &config,
                Some(&mut sink),
                &mut UsageCapture::default(),
            ))
            .expect_err("the output limit stops the stream");
            assert!(
                matches!(&error, ProviderExecutionError::Stopped { facts: seen, .. } if *seen == facts(None, None)),
                "{case}: a local stop that invents no finish, got {error:?}"
            );
            assert_eq!(sink.deltas, ["hi"], "{case}: the accepted prefix is kept");
            let raw: Value = serde_json::from_str(usage).expect("usage object");
            assert_eq!(
                sink.usages(),
                [(
                    UsageFinality::Cumulative,
                    NormalizedUsage {
                        input_tokens: Some(9),
                        output_tokens: Some(3),
                        raw: Some(raw),
                        ..NormalizedUsage::default()
                    }
                )],
                "{case}: the usage of the refused event is observed as reported"
            );
            assert_eq!(
                error_observations(&error)
                    .iter()
                    .map(|observation| observation.failure.is_some())
                    .collect::<Vec<_>>(),
                [false],
                "{case}: a local stop is no Provider failure"
            );
        }
    }

    #[test]
    fn failures_after_the_head_keep_the_facts_they_observed() {
        let tool_call =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#);
        let length = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n";
        let stop_facts = || Some(facts(Some(ChatFinishReason::Stop), Some("stop")));
        let unknown = |result: Result<ProviderExecutionResult, ProviderExecutionError>| match result
        {
            Err(ProviderExecutionError::Unknown {
                facts, finished, ..
            }) => (facts, finished),
            other => panic!("expected an unknown outcome, got {other:?}"),
        };
        assert_eq!(
            unknown(parse(
                vec![Step::Chunk(CONTENT), Step::Chunk(STOP), Step::Fail],
                None
            )),
            (stop_facts(), true),
            "a reset after the finish keeps the finish, which stands apart"
        );
        assert_eq!(
            unknown(parse(vec![Step::Chunk(CONTENT), Step::Fail], None)),
            (Some(facts(None, None)), false),
            "a reset before the finish keeps only the status"
        );
        assert_eq!(
            unknown(parse(
                vec![Step::Chunk(tool_call), Step::Chunk(STOP), Step::Fail],
                None
            )),
            (stop_facts(), false),
            "a finish beside unchecked tool calls does not stand apart"
        );

        let incomplete =
            |result: Result<ProviderExecutionResult, ProviderExecutionError>| match result {
                Err(ProviderExecutionError::Incomplete { facts, .. }) => facts,
                other => panic!("expected an incomplete stream, got {other:?}"),
            };
        assert_eq!(
            incomplete(parse(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(STOP),
                    Step::Chunk("data: {nope}\n\n")
                ],
                None
            )),
            facts(Some(ChatFinishReason::Stop), Some("stop")),
            "malformed SSE after the finish keeps the finish"
        );
        assert_eq!(
            incomplete(parse(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(length),
                    Step::Chunk("data: {")
                ],
                None
            )),
            facts(Some(ChatFinishReason::Length), Some("length")),
            "a cut frame keeps the facts accepted before it"
        );
        assert_eq!(
            incomplete(parse(vec![Step::Chunk(CONTENT)], None)),
            facts(None, None),
            "a stream without a finish invents none"
        );
    }

    #[test]
    fn an_idle_stream_after_its_finish_keeps_the_finish_apart_from_the_silence() {
        let result = ntex::rt::System::new("finished-idle-test", ntex::rt::DefaultRuntime)
            .block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                parse_stream(
                    payload(
                        vec![Step::Chunk(CONTENT), Step::Chunk(STOP), Step::Stall],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    None,
                    &mut UsageCapture::default(),
                )
                .await
            });
        assert!(
            matches!(
                &result,
                Err(ProviderExecutionError::Unknown { facts: Some(seen), finished: true, .. })
                    if *seen == facts(Some(ChatFinishReason::Stop), Some("stop"))
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn a_failure_before_any_response_head_observes_no_response_fact() {
        for error in [ClientError::Timeout, ClientError::TunnelNotSupported] {
            let error = classify_client_error(error);
            assert!(
                matches!(
                    error,
                    ProviderExecutionError::Unknown {
                        facts: None,
                        finished: false,
                        ..
                    }
                ),
                "got {error:?}"
            );
            assert_eq!(
                error_observations(&error)
                    .iter()
                    .map(|observation| (
                        observation.status_code,
                        observation.finish_reason,
                        observation.raw_finish_reason.clone()
                    ))
                    .collect::<Vec<_>>(),
                [(None, None, None)]
            );
        }
    }

    #[test]
    fn only_provider_and_transport_facts_become_availability_observations() {
        let stopped = ProviderExecutionError::Stopped {
            message: "cancelled".to_owned(),
            facts: facts(Some(ChatFinishReason::Stop), Some("stop")),
        };
        assert_eq!(
            error_observations(&stopped),
            [AvailabilityObservation {
                status_code: Some(200),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".to_owned()),
                failure: None,
            }],
            "a local stop keeps the response facts and no failure"
        );
        assert_eq!(
            error_observations(&ProviderExecutionError::Build {
                message: "bad".to_owned()
            }),
            []
        );
        assert_eq!(
            error_observations(&ProviderExecutionError::NotDelivered {
                message: "refused".to_owned()
            }),
            []
        );

        let failed = |error: &ProviderExecutionError| ProviderFailure {
            message: error.to_string(),
            ..ProviderFailure::default()
        };
        let before_head = ProviderExecutionError::Unknown {
            message: "reset".to_owned(),
            facts: None,
            finished: false,
        };
        assert_eq!(
            error_observations(&before_head),
            [AvailabilityObservation {
                status_code: None,
                finish_reason: None,
                raw_finish_reason: None,
                failure: Some(failed(&before_head)),
            }],
            "a failure before any response head invents no response fact"
        );

        let unfinished = ProviderExecutionError::Unknown {
            message: "reset".to_owned(),
            facts: Some(facts(None, None)),
            finished: false,
        };
        assert_eq!(
            error_observations(&unfinished),
            [AvailabilityObservation {
                status_code: Some(200),
                finish_reason: None,
                raw_finish_reason: None,
                failure: Some(failed(&unfinished)),
            }],
            "a failure after the head keeps the status it observed"
        );

        let after_finish = ProviderExecutionError::Unknown {
            message: "reset".to_owned(),
            facts: Some(facts(Some(ChatFinishReason::Stop), Some("stop"))),
            finished: true,
        };
        let observations = error_observations(&after_finish);
        assert_eq!(
            observations,
            [
                AvailabilityObservation {
                    status_code: Some(200),
                    finish_reason: Some(ChatFinishReason::Stop),
                    raw_finish_reason: Some("stop".to_owned()),
                    failure: None,
                },
                AvailabilityObservation {
                    status_code: None,
                    finish_reason: None,
                    raw_finish_reason: None,
                    failure: Some(failed(&after_finish)),
                },
            ],
            "a finish observed before a later transport failure is not rewritten to contain it"
        );

        let refused = ProviderExecutionError::Incomplete {
            message: "invalid provider tool call: tool call 0 has no id".to_owned(),
            facts: facts(Some(ChatFinishReason::Stop), Some("stop")),
        };
        let observations = error_observations(&refused);
        assert_eq!(
            observations,
            [AvailabilityObservation {
                status_code: Some(200),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".to_owned()),
                failure: Some(failed(&refused)),
            }],
            "a refused response keeps its facts beside the refusal"
        );
        assert_eq!(
            observations[0].classification(),
            AvailabilityClassification::Unconfirmed,
            "and is never a normal-termination witness"
        );
    }

    const CUMULATIVE_10: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\n";
    const CUMULATIVE_25: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}],\"usage\":{\"prompt_tokens\":25,\"completion_tokens\":2}}\n\n";
    const NULL_USAGE: &str =
        "data: {\"choices\":[{\"delta\":{\"content\":\"c\"}}],\"usage\":null}\n\n";
    const FINAL_40_60: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":60,\"prompt_tokens_details\":{\"cached_tokens\":0}}}\n\n";
    const FINAL_40_61: &str =
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":61}}\n\n";
    const FINAL_INPUT_ONLY: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40}}\n\n";
    const FINAL_ZERO_OUTPUT: &str =
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":0}}\n\n";
    const DONE: &str = "data: [DONE]\n\n";

    fn recorded(
        steps: Vec<Step>,
    ) -> (
        Result<ProviderExecutionResult, ProviderExecutionError>,
        RecordingSink,
    ) {
        let mut sink = RecordingSink::default();
        let result = parse(steps, Some(&mut sink));
        (result, sink)
    }

    fn counters(usage: &NormalizedUsage) -> [Option<u32>; 3] {
        [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
        ]
    }

    #[test]
    fn usage_null_is_absence_and_erases_nothing() {
        let (result, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_10),
            Step::Chunk(NULL_USAGE),
            Step::Chunk(STOP),
        ]);
        let result = result.expect("a finished stream");
        assert_eq!(
            sink.usages().len(),
            1,
            "a null usage reports nothing, so nothing is recorded for it"
        );
        assert_eq!(result.usage.input_tokens, Some(10), "earlier counters stay");
        assert!(result.usage.raw.is_some(), "and so does the raw object");
    }

    #[test]
    fn usage_beside_choices_is_cumulative_and_only_the_empty_choices_chunk_is_final() {
        let (result, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_10),
            Step::Chunk(CUMULATIVE_25),
            Step::Chunk(STOP),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(DONE),
        ]);
        result.expect("a finished stream");
        let usages = sink.usages();
        assert_eq!(
            usages
                .iter()
                .map(|(finality, usage)| (*finality, counters(usage)))
                .collect::<Vec<_>>(),
            [
                (UsageFinality::Cumulative, [Some(10), Some(1), None]),
                (UsageFinality::Cumulative, [Some(25), Some(2), None]),
                (UsageFinality::Final, [Some(40), Some(60), Some(0)]),
            ]
        );
        assert!(
            usages[2].1.raw.is_some(),
            "each reported object is kept raw"
        );
    }

    #[test]
    fn a_normal_finish_end_of_stream_and_done_never_make_usage_final() {
        let (result, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_25),
            Step::Chunk(STOP),
            Step::Chunk(DONE),
        ]);
        result.expect("a finished stream");
        let usages = sink.usages();
        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].0, UsageFinality::Cumulative);
    }

    #[test]
    fn a_final_reports_only_its_own_fields_and_zero_is_not_missing() {
        let (_, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_25),
            Step::Chunk(STOP),
            Step::Chunk(FINAL_INPUT_ONLY),
            Step::Chunk(FINAL_ZERO_OUTPUT),
        ]);
        let usages = sink.usages();
        assert_eq!(
            counters(&usages[1].1),
            [Some(40), None, None],
            "an omitted field is missing, never zero and never the earlier value"
        );
        assert_eq!(
            counters(&usages[2].1),
            [Some(3), Some(0), None],
            "a real zero is a reported count"
        );
    }

    #[test]
    fn duplicate_and_conflicting_finals_all_reach_the_sink() {
        let (_, sink) = recorded(vec![
            Step::Chunk(FINAL_40_60),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(FINAL_40_61),
            Step::Chunk(STOP),
        ]);
        assert_eq!(
            sink.usages()
                .iter()
                .map(|(finality, usage)| (*finality, usage.output_tokens))
                .collect::<Vec<_>>(),
            [
                (UsageFinality::Final, Some(60)),
                (UsageFinality::Final, Some(60)),
                (UsageFinality::Final, Some(61)),
            ]
        );
    }

    #[test]
    fn a_usage_reported_before_each_kind_of_stop_is_not_lost_with_the_parser() {
        let cases: [(&str, Vec<Step>); 3] = [
            (
                "transport failure",
                vec![Step::Chunk(CUMULATIVE_25), Step::Fail],
            ),
            ("stream without a finish", vec![Step::Chunk(CUMULATIVE_25)]),
            (
                "cancellation",
                vec![
                    Step::Chunk(CUMULATIVE_25),
                    Step::Cancel,
                    Step::Chunk(CONTENT),
                ],
            ),
        ];
        for (case, steps) in cases {
            let (result, sink) = recorded(steps);
            assert!(result.is_err(), "{case}");
            assert_eq!(
                sink.usages()
                    .iter()
                    .map(|(finality, usage)| (*finality, counters(usage)))
                    .collect::<Vec<_>>(),
                [(UsageFinality::Cumulative, [Some(25), Some(2), None])],
                "{case}: the usage reached the sink before the error"
            );
        }
    }

    #[test]
    fn a_failed_delta_write_keeps_the_counters_of_its_own_event() {
        let mut sink = RecordingSink {
            fail: Some("database could not persist provider output"),
            ..RecordingSink::default()
        };
        let error = parse(vec![Step::Chunk(CUMULATIVE_25)], Some(&mut sink))
            .expect_err("a sink failure stops the stream");
        assert!(matches!(
            error,
            ProviderExecutionError::OutputPersistence {
                stream,
                ..
            } if matches!(*stream, ProviderExecutionError::Incomplete { .. })
        ));
        assert_eq!(sink.usages().len(), 1);
    }

    #[test]
    fn only_a_failure_before_the_request_could_leave_is_not_delivered() {
        let message = || "x".to_owned();
        assert_eq!(
            unannounced_fact(&ProviderExecutionError::Build { message: message() }),
            ProviderFact::NotDelivered
        );
        assert_eq!(
            unannounced_fact(&ProviderExecutionError::NotDelivered { message: message() }),
            ProviderFact::NotDelivered
        );
        for error in [
            ProviderExecutionError::Unknown {
                message: message(),
                facts: None,
                finished: false,
            },
            ProviderExecutionError::Incomplete {
                message: message(),
                facts: facts(None, None),
            },
            ProviderExecutionError::Rejected {
                failure: ProviderFailure::default(),
            },
        ] {
            assert_eq!(unannounced_fact(&error), ProviderFact::Delivered, "{error}");
        }
    }

    fn leaked(chunk: String) -> &'static str {
        Box::leak(chunk.into_boxed_str())
    }

    /// One content-free chunk reporting `usage` beside choices.
    fn running_total(usage: &str) -> &'static str {
        leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{}}}}],\"usage\":{usage}}}\n\n"
        ))
    }

    /// One usage-only chunk, the protocol's final position.
    fn usage_only(usage: &str) -> &'static str {
        leaked(format!("data: {{\"choices\":[],\"usage\":{usage}}}\n\n"))
    }

    #[test]
    fn every_distinct_usage_object_is_handed_over_as_reported_however_many_there_are() {
        // Non-monotonic fields, so no maximum or latest object can stand in for them.
        let objects: Vec<String> = (0..40u32)
            .map(|n| {
                let input = (n * 7) % 13 + 1;
                let output = (n * 5) % 11;
                match n % 3 {
                    0 => format!("{{\"prompt_tokens\":{input},\"completion_tokens\":{output}}}"),
                    1 => format!("{{\"completion_tokens\":{output},\"extra\":{n}}}"),
                    _ => format!(
                        "{{\"prompt_tokens\":{input},\"prompt_tokens_details\":{{\"cached_tokens\":{}}}}}",
                        n % 4
                    ),
                }
            })
            .collect();
        let mut steps: Vec<Step> = Vec::new();
        for object in &objects {
            steps.push(Step::Chunk(running_total(object)));
        }
        // An identical consecutive repeat carries nothing new.
        steps.push(Step::Chunk(running_total(&objects[39])));
        steps.push(Step::Chunk(STOP));
        let (result, sink) = recorded(steps);
        result.expect("a finished stream");
        let usages = sink.usages();
        assert_eq!(
            usages.len(),
            40,
            "no object was merged, dropped or repeated"
        );
        for ((finality, usage), source) in usages.iter().zip(&objects) {
            assert_eq!(*finality, UsageFinality::Cumulative);
            let raw: Value = serde_json::from_str(source).expect("source object");
            assert_eq!(usage.raw.as_ref(), Some(&raw), "the source object is kept");
            assert_eq!(
                counters(usage),
                [
                    raw.get("prompt_tokens")
                        .and_then(Value::as_u64)
                        .map(|v| v as u32),
                    raw.get("completion_tokens")
                        .and_then(Value::as_u64)
                        .map(|v| v as u32),
                    raw.pointer("/prompt_tokens_details/cached_tokens")
                        .and_then(Value::as_u64)
                        .map(|v| v as u32),
                ],
                "its normalization matches that object alone"
            );
        }
    }

    #[test]
    fn many_final_objects_neither_stop_the_stream_nor_get_dropped() {
        let mut steps: Vec<Step> = (0..20)
            .map(|_| {
                Step::Chunk(usage_only(
                    "{\"prompt_tokens\":40,\"completion_tokens\":60}",
                ))
            })
            .collect();
        for n in 0..20 {
            steps.push(Step::Chunk(usage_only(&format!(
                "{{\"prompt_tokens\":40,\"completion_tokens\":{}}}",
                100 + n
            ))));
        }
        steps.push(Step::Chunk(STOP));
        steps.push(Step::Chunk(CONTENT));
        let (result, sink) = recorded(steps);
        let result = result.expect("a stream with many finals still completes");
        assert_eq!(result.text, "hi", "the response was read to its end");
        let usages = sink.usages();
        assert_eq!(usages.len(), 40);
        assert!(
            usages
                .iter()
                .all(|(finality, _)| *finality == UsageFinality::Final)
        );
        assert_eq!(
            usages
                .iter()
                .map(|(_, usage)| usage.output_tokens)
                .collect::<Vec<_>>()[18..22],
            [Some(60), Some(60), Some(100), Some(101)],
            "identical and distinct finals are all kept, in order"
        );
    }

    static TICKS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

    fn tick() -> Timestamp {
        Timestamp::from_unix_micros(TICKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1)
    }

    /// A sink that takes its own turn on the clock before it keeps an
    /// observation, as a slow sink would let time pass.
    struct SlowSink {
        kept: Vec<(Timestamp, Timestamp)>,
    }

    impl ProviderOutputSink for SlowSink {
        fn append<'a>(
            &'a mut self,
            _kind: IntermediateOutputKind,
            _text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn observe<'a>(
            &'a mut self,
            observation: ProviderObservation,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
            Box::pin(async move {
                let late = tick();
                self.kept.push((observation.observed_at, late));
            })
        }
    }

    #[test]
    fn an_observation_keeps_the_time_it_was_parsed_not_the_time_the_sink_took_it() {
        let config = ProviderExecutionConfig::default();
        let mut sink = SlowSink { kept: Vec::new() };
        let steps = vec![
            Step::Chunk(running_total("{\"prompt_tokens\":1}")),
            Step::Chunk(running_total("{\"prompt_tokens\":2}")),
            Step::Chunk(usage_only("{\"prompt_tokens\":3}")),
            Step::Chunk(STOP),
        ];
        wait(parse_stream(
            payload(steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &mut UsageCapture::with_clock(tick),
        ))
        .expect("a finished stream");
        assert_eq!(sink.kept.len(), 3);
        for (observed, taken) in &sink.kept {
            assert_eq!(
                observed.as_unix_micros() + 1,
                taken.as_unix_micros(),
                "the object was timed on the clock just before the sink took its turn"
            );
        }
        let times: Vec<_> = sink.kept.iter().map(|(observed, _)| *observed).collect();
        assert!(
            times.windows(2).all(|pair| pair[0] < pair[1]),
            "each later object is timed after the earlier sink wait"
        );
    }

    struct SlowOutputSink {
        deltas: Vec<String>,
    }

    impl ProviderOutputSink for SlowOutputSink {
        fn append<'a>(
            &'a mut self,
            _kind: IntermediateOutputKind,
            text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
            Box::pin(async move {
                ntex::time::sleep(Duration::from_millis(250)).await;
                self.deltas.push(text.to_owned());
                Ok(())
            })
        }

        fn observe<'a>(
            &'a mut self,
            _observation: ProviderObservation,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
            Box::pin(async {})
        }
    }

    #[test]
    fn local_persistence_time_is_not_provider_idle_time() {
        let steps = vec![Step::Chunk(CONTENT), Step::Chunk(STOP)];
        let config = ProviderExecutionConfig {
            timeout: Duration::from_millis(100),
            ..ProviderExecutionConfig::default()
        };
        let started = Instant::now();
        let (result, sink) = ntex::rt::System::new("timeout-test", ntex::rt::DefaultRuntime)
            .block_on(async move {
                let mut sink = SlowOutputSink { deltas: Vec::new() };
                let result = parse_stream(
                    payload(steps, config.cancellation.clone()),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await;
                (result, sink)
            });
        assert!(
            result.is_ok(),
            "local persistence does not trip provider idle"
        );
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn a_silent_stream_exceeding_the_idle_timeout_is_unknown_and_flushes_pending_output() {
        let (result, sink) = ntex::rt::System::new("idle-timeout-test", ntex::rt::DefaultRuntime)
            .block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                let mut sink = RecordingSink::default();
                let result = parse_stream(
                    payload(
                        vec![Step::Chunk(CONTENT), Step::Stall],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await;
                (result, sink)
            });
        assert!(matches!(
            result,
            Err(ProviderExecutionError::Unknown { .. })
        ));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn repeated_chunks_within_the_idle_limit_can_exceed_the_old_total_deadline() {
        let (result, sink) = ntex::rt::System::new("long-stream-test", ntex::rt::DefaultRuntime)
            .block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                let mut sink = RecordingSink::default();
                let result = parse_stream(
                    payload(
                        vec![
                            Step::Chunk(CONTENT),
                            Step::Quiet(Duration::from_millis(70)),
                            Step::Chunk(CONTENT),
                            Step::Quiet(Duration::from_millis(70)),
                            Step::Chunk(STOP),
                        ],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await;
                (result, sink)
            });
        let result = result.expect("each provider interval stayed within the idle limit");
        assert_eq!(result.text, "hihi");
        assert_eq!(sink.deltas.concat(), "hihi");
    }

    /// Reads a scripted body with the real reader, a short idle timeout and
    /// timers running, so a stream that is never ended is reported as idle.
    fn read_with_timers(
        steps: Vec<Step>,
        conversation: ChatConversation,
    ) -> (
        Result<ProviderExecutionResult, ProviderExecutionError>,
        RecordingSink,
    ) {
        ntex::rt::System::new("done-test", ntex::rt::DefaultRuntime).block_on(async move {
            let config = ProviderExecutionConfig {
                timeout: Duration::from_millis(100),
                ..ProviderExecutionConfig::default()
            };
            let mut sink = RecordingSink::default();
            let result = read_response(
                payload(steps, config.cancellation.clone()),
                200,
                Instant::now(),
                None,
                &config,
                Some(&mut sink),
                &conversation,
            )
            .await
            .and_then(ProviderExecutionResult::admitted);
            (result, sink)
        })
    }

    #[test]
    fn done_ends_the_response_though_the_connection_stays_open() {
        let (result, sink) = read_with_timers(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Chunk(FINAL_40_60),
                Step::Chunk(DONE),
                Step::Stall,
            ],
            ChatConversation::default(),
        );
        let result = result.expect("the response ended at [DONE]");
        assert_eq!(result.text, "hi");
        assert_eq!(result.finish_reason, Some(ChatFinishReason::Stop));
        assert_eq!(result.usage.input_tokens, Some(40));
        assert_eq!(sink.deltas, ["hi"], "the output is flushed");
        assert_eq!(
            sink.usages()
                .iter()
                .map(|(finality, _)| *finality)
                .collect::<Vec<_>>(),
            [UsageFinality::Final]
        );

        // `[DONE]` inside a transport chunk, and split across two; nothing
        // after it is read.
        for steps in [
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(leaked(format!("{STOP}{DONE}data: {{nope}}\n\n"))),
                Step::Stall,
            ],
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Chunk("data: [DO"),
                Step::Chunk("NE]\n\ndata: {nope}\n\n"),
                Step::Stall,
            ],
        ] {
            let (result, sink) = read_with_timers(steps, ChatConversation::default());
            let result = result.expect("the response ended at [DONE]");
            assert_eq!(result.text, "hi");
            assert_eq!(sink.deltas, ["hi"]);
        }

        let (result, _) = read_with_timers(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
                Step::Chunk(DONE),
                Step::Stall,
            ],
            offering(&["f"]),
        );
        assert_eq!(
            result.expect("tool calls ended at [DONE]").tool_calls,
            [call(0, "c", "f", "{}")]
        );

        // Without `[DONE]` the end of the stream still ends the response.
        let (result, _) = read_with_timers(
            vec![Step::Chunk(CONTENT), Step::Chunk(STOP)],
            ChatConversation::default(),
        );
        assert_eq!(result.expect("a finished stream").text, "hi");
    }

    #[test]
    fn done_completes_nothing_the_response_did_not() {
        let partial =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#);
        let cases: [(&str, Vec<Step>, &str); 3] = [
            (
                "no finish event",
                vec![Step::Chunk(CONTENT), Step::Chunk(DONE), Step::Stall],
                "without a finish event",
            ),
            (
                "an incomplete tool call",
                vec![
                    Step::Chunk(partial),
                    Step::Chunk(TOOL_CALLS),
                    Step::Chunk(DONE),
                    Step::Stall,
                ],
                "not one JSON object",
            ),
            (
                "a tool that was not offered",
                vec![
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"id":"c","function":{"name":"g","arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                    Step::Chunk(DONE),
                    Step::Stall,
                ],
                "invalid provider tool call",
            ),
        ];
        for (case, steps, expected) in cases {
            let (result, _) = read_with_timers(steps, offering(&["f"]));
            let message = incomplete_message(result);
            assert!(message.contains(expected), "{case}: {message}");
        }

        let (result, sink) = read_with_timers(
            vec![Step::Chunk(CONTENT), Step::Fail, Step::Chunk(DONE)],
            ChatConversation::default(),
        );
        assert!(
            matches!(result, Err(ProviderExecutionError::Unknown { .. })),
            "a transport failure before [DONE] stays unknown, got {result:?}"
        );
        assert_eq!(sink.deltas, ["hi"]);

        let (result, _) = read_with_timers(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Cancel,
                Step::Chunk(DONE),
            ],
            ChatConversation::default(),
        );
        assert!(
            matches!(&result, Err(ProviderExecutionError::Stopped { facts: seen, .. }) if *seen == facts(Some(ChatFinishReason::Stop), Some("stop"))),
            "a cancellation before [DONE] is a local stop, got {result:?}"
        );
    }

    #[test]
    fn a_child_is_cancelled_by_its_parent_and_by_nothing_else() {
        let parent = CancellationToken::default();
        let (first, second) = (parent.child(), parent.child());
        let grandchild = first.child();
        first.cancel();
        assert!(first.is_cancelled() && grandchild.is_cancelled());
        assert!(
            !parent.is_cancelled() && !second.is_cancelled(),
            "a child's cancellation reaches neither its parent nor a sibling"
        );
        let waiter = second.clone();
        let waiting = std::thread::spawn(move || wait(waiter.cancelled()));
        parent.cancel();
        assert!(second.is_cancelled());
        waiting.join().expect("the child's waiter was woken");
        assert!(
            parent.child().is_cancelled(),
            "a child of a cancelled parent starts cancelled"
        );
        assert_eq!(parent.registered_children(), 0);
    }

    #[test]
    fn a_cancellation_racing_child_creation_reaches_every_child() {
        for _ in 0..200 {
            let parent = CancellationToken::default();
            let start = Arc::new(std::sync::Barrier::new(2));
            let (making, maker_start) = (parent.clone(), start.clone());
            let making = std::thread::spawn(move || {
                maker_start.wait();
                (0..50).map(|_| making.child()).collect::<Vec<_>>()
            });
            start.wait();
            parent.cancel();
            let children = making.join().expect("children made");
            assert!(
                children.iter().all(CancellationToken::is_cancelled),
                "no child escapes a cancellation made while it was created"
            );
        }
    }

    #[test]
    fn a_parent_keeps_no_child_that_was_dropped() {
        let parent = CancellationToken::default();
        for _ in 0..1_000 {
            drop(parent.child());
        }
        let live = parent.child();
        assert_eq!(
            parent.registered_children(),
            1,
            "dropped children are pruned when another registers"
        );
        let weak = Arc::downgrade(&live.0);
        drop(live);
        assert!(
            weak.upgrade().is_none(),
            "the parent holds its children weakly"
        );
        parent.cancel();
        assert_eq!(parent.registered_children(), 0);
    }

    #[test]
    fn a_cancellation_wakes_every_waiter_at_once() {
        let token = CancellationToken::default();
        let waiter = token.clone();
        let waiting = std::thread::spawn(move || wait(waiter.cancelled()));
        std::thread::sleep(Duration::from_millis(20));
        assert!(!waiting.is_finished(), "nothing was cancelled yet");
        token.cancel();
        waiting.join().expect("the waiter woke");
        assert!(token.is_cancelled());
        wait(token.cancelled());
    }

    #[test]
    fn a_cancellation_ends_a_stalled_stream_at_once_with_the_facts_it_had() {
        let config = ProviderExecutionConfig::default();
        assert!(config.timeout >= Duration::from_secs(60));
        let cancellation = config.cancellation.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancellation.cancel();
        });
        let started = Instant::now();
        let mut sink = RecordingSink::default();
        let result = wait(parse_stream(
            payload(
                vec![Step::Chunk(CONTENT), Step::Stall],
                config.cancellation.clone(),
            ),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &mut UsageCapture::default(),
        ));
        canceller.join().expect("cancelled");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the stalled read did not wait for its timeout"
        );
        assert!(
            matches!(&result, Err(ProviderExecutionError::Stopped { facts: seen, .. }) if *seen == facts(None, None)),
            "a cancelled stream is a local stop, got {result:?}"
        );
        assert_eq!(
            sink.deltas,
            ["hi"],
            "what arrived before the stall was kept"
        );
    }

    #[test]
    fn an_interruption_before_any_response_observes_nothing_but_cannot_rule_out_delivery() {
        let interrupted = ProviderExecutionError::Interrupted {
            message: "provider execution was cancelled before a response arrived".into(),
        };
        assert_eq!(error_observations(&interrupted), []);
        assert_eq!(unannounced_fact(&interrupted), ProviderFact::Delivered);
    }

    #[test]
    fn the_authorization_header_is_sensitive_and_no_header_dump_prints_it() {
        let value = authorization("sk-test-secret").expect("a valid credential");
        assert!(value.is_sensitive());
        assert_eq!(value.to_str().ok(), Some("Bearer sk-test-secret"));
        let mut headers = ntex::http::HeaderMap::new();
        headers.insert(AUTHORIZATION, value);
        let dumped = format!("{headers:?}");
        assert!(!dumped.contains("sk-test-secret"), "{dumped}");

        match authorization("sk-test\nsecret") {
            Err(ProviderExecutionError::Build { message }) => {
                assert!(
                    !message.contains("sk-test"),
                    "the credential is not named: {message}"
                )
            }
            other => panic!("an invalid credential is refused, got {other:?}"),
        }
    }

    // ---- Tool calling ----------------------------------------------------

    use crate::openai_compatible::{
        ChatToolCallWire, FunctionDefinitionWire, MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALLS,
        MAX_TOOLS, NamedFunctionWire, NamedToolChoiceWire, ToolChoiceWire, ToolWire,
    };

    const TOOL_CALLS: &str =
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n";

    /// One chunk whose delta carries the given `tool_calls` array.
    fn tool_delta(calls: &str) -> &'static str {
        leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":{calls}}}}}]}}\n\n"
        ))
    }

    fn tool(name: &str) -> ToolWire {
        ToolWire {
            tool_type: "function".to_owned(),
            function: FunctionDefinitionWire {
                name: name.to_owned(),
                description: Some(format!("the {name} tool")),
                parameters: Some(json!({"type": "object", "properties": {}})),
                strict: None,
            },
        }
    }

    /// A conversation offering the named tools.
    fn offering(names: &[&str]) -> ChatConversation {
        ChatConversation {
            messages: vec![ChatMessage::user("weather?")],
            tools: names.iter().map(|name| tool(name)).collect(),
            ..ChatConversation::default()
        }
    }

    /// Reads a scripted body for `conversation` with the real reader.
    fn read(
        steps: Vec<Step>,
        conversation: &ChatConversation,
        sink: Option<&mut dyn ProviderOutputSink>,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        let config = ProviderExecutionConfig::default();
        wait(read_response(
            payload(steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            sink,
            conversation,
        ))
        .and_then(ProviderExecutionResult::admitted)
    }

    fn call(index: u32, id: &str, name: &str, arguments: &str) -> CompletedToolCall {
        CompletedToolCall {
            index,
            id: id.to_owned(),
            name: name.to_owned(),
            arguments: arguments.to_owned(),
        }
    }

    fn incomplete_message(
        result: Result<ProviderExecutionResult, ProviderExecutionError>,
    ) -> String {
        match result {
            Err(ProviderExecutionError::Incomplete { message, .. }) => message,
            other => panic!("expected an incomplete stream, got {other:?}"),
        }
    }

    #[test]
    fn a_text_only_response_carries_no_tool_calls_or_reasoning() {
        let mut sink = RecordingSink::default();
        let result = read(
            vec![Step::Chunk(CONTENT), Step::Chunk(STOP)],
            &offering(&["get_weather"]),
            Some(&mut sink),
        )
        .expect("a finished text stream");
        assert_eq!(result.text, "hi");
        assert_eq!(sink.deltas, ["hi"]);
        assert!(result.tool_calls.is_empty());
        assert!(!result.requests_tools());
        assert_eq!(result.reasoning, "");
        assert_eq!(
            result_value(&result),
            json!({"text": "hi", "finish_reason": "stop"})
        );
    }

    #[test]
    fn a_refused_call_keeps_its_response_and_is_returned_with_its_reason() {
        let config = ProviderExecutionConfig::default();
        let mut sink = RecordingSink::default();
        let result = wait(read_response(
            payload(vec![
                Step::Chunk(CONTENT),
                Step::Chunk(tool_delta(r#"[{"index":0,"id":"a","function":{"name":"get_weather","arguments":"{}"}},{"index":1,"id":"b","function":{"name":"get_weather","arguments":"[1]"}}]"#)),
                Step::Chunk(TOOL_CALLS),
            ], config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &offering(&["get_weather"]),
        )).expect("a complete response with a refused call");
        assert_eq!(
            result.tool_calls,
            [
                call(0, "a", "get_weather", "{}"),
                call(1, "b", "get_weather", "[1]")
            ]
        );
        assert_eq!(
            result.refused,
            [InvalidToolCall::at(
                crate::openai_compatible::ToolCallRefusal::InvalidArguments,
                1,
                "tool call 1 arguments are not one JSON object"
            )]
        );
        assert_eq!(result.text, "hi");
        assert_eq!(sink.deltas, ["hi"]);
        assert!(incomplete_message(result.admitted()).contains("(invalid arguments)"));
    }

    #[test]
    fn an_unoffered_name_is_unauthorized_even_when_its_arguments_are_not_an_object() {
        let config = ProviderExecutionConfig::default();
        let mut sink = RecordingSink::default();
        let result = wait(read_response(
            payload(vec![
                Step::Chunk(CONTENT),
                Step::Chunk(tool_delta(r#"[{"index":0,"id":"a","function":{"name":"get_weather","arguments":"[1]"}},{"index":1,"id":"b","function":{"name":"g","arguments":"[1]"}}]"#)),
                Step::Chunk(TOOL_CALLS),
            ], config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &offering(&["get_weather"]),
        )).expect("a complete response with two refused calls");
        assert_eq!(
            result.tool_calls,
            [call(0, "a", "get_weather", "[1]"), call(1, "b", "g", "[1]")]
        );
        assert_eq!(
            result.refused,
            [
                InvalidToolCall::at(
                    crate::openai_compatible::ToolCallRefusal::InvalidArguments,
                    0,
                    "tool call 0 arguments are not one JSON object"
                ),
                InvalidToolCall::at(
                    crate::openai_compatible::ToolCallRefusal::Unauthorized,
                    1,
                    "tool call 1 names `g`, which this request did not allow"
                ),
            ]
        );
    }

    #[test]
    fn a_single_tool_call_is_reconstructed_with_its_wire_identity() {
        let mut sink = RecordingSink::default();
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"function":{"arguments":"{\"city\":"}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"function":{"arguments":" \"Paris\"}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["get_weather"]),
            Some(&mut sink),
        )
        .expect("a complete tool call");
        assert_eq!(
            result.tool_calls,
            [call(0, "call_1", "get_weather", "{\"city\": \"Paris\"}")],
            "the arguments are kept exactly as sent, spacing included"
        );
        assert!(result.requests_tools());
        assert_eq!(result.text, "");
        assert!(
            sink.deltas.is_empty(),
            "no tool fragment is published as output"
        );
        assert_eq!(result.finish_reason, Some(ChatFinishReason::ToolCalls));
        assert_eq!(result.raw_finish_reason.as_deref(), Some("tool_calls"));
    }

    #[test]
    fn interleaved_calls_are_kept_apart_by_their_wire_index() {
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":1,"id":"call_b","function":{"name":"get_time","arguments":"{\"tz\""}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_a","function":{"name":"get_weather","arguments":"{\"city\""}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":1,"function":{"arguments":":\"UTC\"}"}},{"index":0,"function":{"arguments":":\"Oslo\"}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["get_weather", "get_time"]),
            None,
        )
        .expect("two complete tool calls");
        assert_eq!(
            result.tool_calls,
            [
                call(0, "call_a", "get_weather", "{\"city\":\"Oslo\"}"),
                call(1, "call_b", "get_time", "{\"tz\":\"UTC\"}"),
            ],
            "ordered by index, not by arrival"
        );
    }

    #[test]
    fn late_metadata_reconstructs_one_call_named_in_one_field() {
        let result = read(
            vec![
                // Arguments arrive before the call's id and name.
                Step::Chunk(tool_delta(r#"[{"index":0,"function":{"arguments":"{\"q\":"}}]"#)),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_9","type":"function","function":{"name":"web_search"}}]"#,
                )),
                // Continuations repeat the id, and carry an empty name or none.
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_9","function":{"name":"","arguments":"\"世界\""}}]"#,
                )),
                Step::Chunk(tool_delta(r#"[{"index":0,"function":{"name":null,"arguments":"}"}}]"#)),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["web_search"]),
            None,
        )
        .expect("a complete tool call");
        assert_eq!(
            result.tool_calls,
            [call(0, "call_9", "web_search", "{\"q\":\"世界\"}")]
        );
    }

    #[test]
    fn a_different_name_field_is_an_ambiguous_identity_and_admits_nothing() {
        let named = |name: &str, arguments: &str| {
            tool_delta(leaked(format!(
                r#"[{{"index":0,"id":"call_1","function":{{"name":"{name}","arguments":{}}}}}]"#,
                serde_json::to_string(arguments).expect("JSON string")
            )))
        };
        let cases: [(&str, Vec<Step>, &[&str]); 2] = [
            (
                "a name sent in two fragments",
                vec![
                    Step::Chunk(named("web_", "")),
                    Step::Chunk(named("search", "{}")),
                ],
                &["web_", "search", "web_search"],
            ),
            (
                "another complete name",
                vec![
                    Step::Chunk(named("get_weather", "{}")),
                    Step::Chunk(named("get_time", "")),
                ],
                &["get_weather", "get_time"],
            ),
        ];
        for (case, mut steps, offered) in cases {
            steps.push(Step::Chunk(TOOL_CALLS));
            let message = incomplete_message(read(steps, &offering(offered), None));
            assert!(
                message.contains("(ambiguous identity)") && message.contains("tool call 0"),
                "{case}: {message}"
            );
        }

        // Each index names its function once; interleaved calls stay apart.
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":1,"id":"b","function":{"name":"abab","arguments":"{"}},{"index":0,"id":"a","function":{"name":"ab","arguments":"{"}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"a","function":{"arguments":"}"}},{"index":1,"id":"b","function":{"arguments":"}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["ab", "abab"]),
            None,
        )
        .expect("two unambiguous calls");
        assert_eq!(
            result.tool_calls,
            [call(0, "a", "ab", "{}"), call(1, "b", "abab", "{}")]
        );
    }

    #[test]
    fn malformed_and_boundary_names_fail_closed() {
        let longest = "n".repeat(64);
        let result = read(
            vec![
                Step::Chunk(tool_delta(leaked(format!(
                    r#"[{{"index":0,"id":"c","function":{{"name":"{longest}","arguments":"{{}}"}}}}]"#
                )))),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&[&longest]),
            None,
        )
        .expect("a name at the limit");
        assert_eq!(result.tool_calls, [call(0, "c", &longest, "{}")]);

        let cases = [
            (
                "a name that is not a string",
                r#"[{"index":0,"id":"c","function":{"name":7,"arguments":"{}"}}]"#,
                "(malformed)",
            ),
            (
                "a name outside the function-name alphabet",
                r#"[{"index":0,"id":"c","function":{"name":"get weather","arguments":"{}"}}]"#,
                "(malformed)",
            ),
            (
                "a name longer than the limit",
                r#"[{"index":0,"id":"c","function":{"name":"nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn","arguments":"{}"}}]"#,
                "(limit exceeded)",
            ),
            (
                "no name at all",
                r#"[{"index":0,"id":"c","function":{"arguments":"{}"}}]"#,
                "(incomplete)",
            ),
            (
                "a name that was not offered",
                r#"[{"index":0,"id":"c","function":{"name":"get_time","arguments":"{}"}}]"#,
                "(unauthorized)",
            ),
            (
                "an offered name with different case",
                r#"[{"index":0,"id":"c","function":{"name":"Get_Weather","arguments":"{}"}}]"#,
                "(unauthorized)",
            ),
        ];
        for (case, calls, expected) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(tool_delta(calls)), Step::Chunk(TOOL_CALLS)],
                &offering(&["get_weather"]),
                None,
            ));
            assert!(message.contains(expected), "{case}: {message}");
            assert!(!message.contains("get weather"), "{case}: {message}");
        }
    }

    /// A conversation offering one function `name` with `parameters`.
    fn offering_schema(name: &str, parameters: Option<Value>) -> ChatConversation {
        ChatConversation {
            tools: vec![ToolWire {
                function: FunctionDefinitionWire {
                    parameters,
                    ..tool(name).function
                },
                ..tool(name)
            }],
            ..offering(&[])
        }
    }

    /// One complete call to `name` with `arguments`, finished with `tool_calls`.
    fn calling(name: &str, arguments: &str) -> Vec<Step> {
        vec![
            Step::Chunk(tool_delta(leaked(format!(
                r#"[{{"index":0,"id":"c","function":{{"name":"{name}","arguments":{}}}}}]"#,
                serde_json::to_string(arguments).expect("JSON string")
            )))),
            Step::Chunk(TOOL_CALLS),
        ]
    }

    #[test]
    fn arguments_must_satisfy_the_offered_parameter_schema() {
        let booking = offering_schema(
            "book",
            Some(json!({
                "type": "object",
                "properties": {
                    "city": {"type": "string", "minLength": 1},
                    "nights": {"type": "integer", "minimum": 1, "maximum": 30},
                    "class": {"enum": ["economy", "business"]},
                    "guests": {
                        "type": "array",
                        "maxItems": 4,
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "age": {"type": "integer", "minimum": 0}
                            },
                            "required": ["name"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["city", "nights"],
                "additionalProperties": false
            })),
        );
        // Kept exactly as sent: spacing and key order included.
        let valid = r#"{ "nights": 2, "city": "Oslo", "class": "economy", "guests": [{"name": "Ada", "age": 36}] }"#;
        let result = read(calling("book", valid), &booking, None).expect("valid arguments");
        assert_eq!(result.tool_calls, [call(0, "c", "book", valid)]);

        let secret = "sk-live-not-to-be-echoed";
        let cases = [
            (
                "a missing required property",
                r#"{"city":"Oslo"}"#,
                "/required",
            ),
            (
                "a wrong type",
                r#"{"city":"Oslo","nights":"2"}"#,
                "/properties/nights/type",
            ),
            (
                "a string where a number is required",
                r#"{"city":"Oslo","nights":"two"}"#,
                "/type",
            ),
            (
                "a value out of range",
                r#"{"city":"Oslo","nights":31}"#,
                "/maximum",
            ),
            (
                "a value outside an enum",
                r#"{"city":"Oslo","nights":2,"class":"first"}"#,
                "/enum",
            ),
            (
                "an unexpected property",
                leaked(format!(
                    r#"{{"city":"Oslo","nights":2,"token":"{secret}"}}"#
                )),
                "/additionalProperties",
            ),
            (
                "a nested object missing its required property",
                r#"{"city":"Oslo","nights":2,"guests":[{"age":3}]}"#,
                "/guests/items/required",
            ),
            (
                "a nested unexpected property",
                leaked(format!(
                    r#"{{"city":"Oslo","nights":2,"guests":[{{"name":"Ada","pin":"{secret}"}}]}}"#
                )),
                "/guests/items/additionalProperties",
            ),
            (
                "a nested wrong type",
                r#"{"city":"Oslo","nights":2,"guests":[{"name":"Ada","age":-1}]}"#,
                "/age/minimum",
            ),
            (
                "too many array items",
                r#"{"city":"Oslo","nights":2,"guests":[{"name":"a"},{"name":"b"},{"name":"c"},{"name":"d"},{"name":"e"}]}"#,
                "/maxItems",
            ),
            ("an array, not an object", "[1]", "not one JSON object"),
            ("malformed JSON", r#"{"city":"#, "not one JSON object"),
        ];
        for (case, arguments, expected) in cases {
            let error = read(calling("book", arguments), &booking, None)
                .expect_err("invalid arguments are refused");
            let ProviderExecutionError::Incomplete {
                message,
                facts: seen,
            } = &error
            else {
                panic!("{case}: expected an incomplete stream, got {error:?}");
            };
            assert!(
                message.contains("(invalid arguments)") && message.contains("tool call 0"),
                "{case}: {message}"
            );
            assert!(message.contains(expected), "{case}: {message}");
            assert!(
                !message.contains(secret),
                "{case}: an argument value is echoed: {message}"
            );
            assert_eq!(
                *seen,
                facts(Some(ChatFinishReason::ToolCalls), Some("tool_calls")),
                "{case}: the response facts are kept"
            );
            let observations = error_observations(&error);
            assert_eq!(observations.len(), 1, "{case}");
            assert_eq!(
                observations[0].classification(),
                AvailabilityClassification::Unconfirmed,
                "{case}: a refused call is neither available evidence nor a Provider failure"
            );
        }
    }

    #[test]
    fn arguments_are_admitted_only_as_strict_i_json() {
        let open = offering_schema("f", Some(json!({"type": "object"})));
        for arguments in [
            "{}",
            r#"{"a":0.1,"b":-0,"c":1e2,"d":2.50,"e":-1.5E-7}"#,
            r#"{"s":"\"}{\\","n":[1,{"m":2}]}"#,
            // The safe-integer bounds, in every notation.
            r#"{"a":9007199254740991,"b":-9007199254740991}"#,
            r#"{"a":9007199254740991.0,"b":-9007199254740991.0}"#,
            r#"{"a":9.007199254740991e15,"b":-9.007199254740991E+15,"c":90071992547409910e-1}"#,
        ] {
            let result = read(calling("f", arguments), &open, None)
                .unwrap_or_else(|error| panic!("{arguments}: {error:?}"));
            assert_eq!(result.tool_calls[0].arguments, arguments);
        }
        let cases = [
            ("a duplicate key", r#"{"a":1,"a":2}"#, "key twice"),
            (
                "a nested duplicate key",
                r#"{"a":{"b":1,"b":1}}"#,
                "key twice",
            ),
            (
                "an integer beyond u64",
                r#"{"a":18446744073709551616}"#,
                "not exactly",
            ),
            (
                "the integer just past the safe range",
                r#"{"a":9007199254740992}"#,
                "not exactly",
            ),
            (
                "the negative integer just past the safe range",
                r#"{"a":-9007199254740992}"#,
                "not exactly",
            ),
            (
                "an integer the Provider meant past 2^53",
                r#"{"a":9007199254740993}"#,
                "not exactly",
            ),
            (
                "the largest u64",
                r#"{"a":18446744073709551615}"#,
                "not exactly",
            ),
            (
                "the smallest i64",
                r#"{"a":-9223372036854775808}"#,
                "not exactly",
            ),
            (
                "the first unsafe integer in decimal notation",
                r#"{"a":9007199254740992.0}"#,
                "not exactly",
            ),
            (
                "the first unsafe integer in exponent notation",
                r#"{"a":9.007199254740992e15}"#,
                "not exactly",
            ),
            (
                "a negative unsafe integer in exponent notation",
                r#"{"a":-9.007199254740992E15}"#,
                "not exactly",
            ),
            (
                "an exactly representable large double",
                r#"{"a":1e300}"#,
                "not exactly",
            ),
            (
                "an integer beyond 2^53 read as a double",
                r#"{"a":9007199254740993.0}"#,
                "not exactly",
            ),
            (
                "more digits than a double keeps",
                r#"{"a":100.00000000000000001}"#,
                "not exactly",
            ),
            (
                "a number underflowing to zero",
                r#"{"a":1e-400}"#,
                "not exactly",
            ),
            (
                "a number overflowing a double",
                r#"{"a":1e400}"#,
                "not valid JSON",
            ),
        ];
        for (case, arguments, expected) in cases {
            let message = incomplete_message(read(calling("f", arguments), &open, None));
            assert!(
                message.contains("(invalid arguments)") && message.contains(expected),
                "{case}: {message}"
            );
        }
    }

    #[test]
    fn large_integers_are_admitted_only_as_strings_a_schema_allows() {
        let counter = offering_schema(
            "count",
            Some(json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string", "pattern": "^[0-9]+$"},
                    "n": {"type": "integer"}
                },
                "required": ["id", "n"],
                "additionalProperties": false
            })),
        );
        let valid = r#"{"id":"18446744073709551616","n":9007199254740991}"#;
        let result = read(calling("count", valid), &counter, None).expect("a safe call");
        assert_eq!(result.tool_calls, [call(0, "c", "count", valid)]);

        // The schema allows any integer, but not every reader takes this one exactly.
        let error = read(
            calling("count", r#"{"id":"1","n":9007199254740993}"#),
            &counter,
            None,
        )
        .expect_err("an unsafe integer is refused");
        let ProviderExecutionError::Incomplete {
            message,
            facts: seen,
        } = &error
        else {
            panic!("expected an incomplete stream, got {error:?}");
        };
        assert!(
            message.contains("(invalid arguments)") && message.contains("not exactly"),
            "{message}"
        );
        assert!(!message.contains("9007199254740993"), "{message}");
        assert_eq!(
            *seen,
            facts(Some(ChatFinishReason::ToolCalls), Some("tool_calls"))
        );
        assert_eq!(
            error_observations(&error)[0].classification(),
            AvailabilityClassification::Unconfirmed
        );
        // A string the schema does not allow is still refused by the schema.
        assert!(
            incomplete_message(read(
                calling("count", r#"{"id":"0x1F","n":1}"#),
                &counter,
                None
            ))
            .contains("/properties/id/pattern")
        );
    }

    #[test]
    fn only_an_explicit_enforceable_parameter_schema_admits_a_call() {
        let parameterless =
            json!({"type": "object", "properties": {}, "additionalProperties": false});
        let conversation = offering_schema("ping", Some(parameterless));
        read(calling("ping", "{}"), &conversation, None).expect("no arguments");
        assert!(
            incomplete_message(read(calling("ping", r#"{"x":1}"#), &conversation, None))
                .contains("/additionalProperties")
        );
        let local = offering_schema(
            "f",
            Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$defs": {"city": {"type": "string", "pattern": "^[A-Z][a-z]+$"}},
                "type": "object",
                "properties": {"city": {"$ref": "#/$defs/city"}, "on": {"type": "string", "format": "date"}}
            })),
        );
        read(
            calling("f", r#"{"city":"Oslo","on":"2026-10-14"}"#),
            &local,
            None,
        )
        .expect("a local reference, a pattern and a format");
        for (case, arguments) in [
            ("a referenced pattern", r#"{"city":"oslo"}"#),
            ("an asserted format", r#"{"on":"14 October"}"#),
        ] {
            assert!(
                incomplete_message(read(calling("f", arguments), &local, None))
                    .contains("(invalid arguments)"),
                "{case}"
            );
        }

        let cases: [(&str, Option<Value>, &str); 13] = [
            ("no schema", None, "(missing parameter schema)"),
            (
                "a remote reference",
                Some(json!({"$ref": "https://example.com/s.json"})),
                "leaves the schema",
            ),
            (
                "a file reference",
                Some(json!({"properties": {"a": {"$ref": "file:///etc/passwd"}}})),
                "leaves the schema",
            ),
            (
                "a relative reference",
                Some(json!({"$ref": "other.json#/a"})),
                "leaves the schema",
            ),
            (
                "a base URI",
                Some(json!({"$id": "https://example.com/s"})),
                "`/$id`",
            ),
            (
                "an unknown keyword",
                Some(json!({"type": "object", "requird": ["a"]})),
                "`/requird`",
            ),
            (
                "a draft-07 container",
                Some(json!({"definitions": {"a": true}})),
                "`/definitions`",
            ),
            (
                "a content annotation",
                Some(json!({"properties": {"a": {"contentMediaType": "application/json"}}})),
                "contentMediaType",
            ),
            (
                "another dialect",
                Some(json!({"$schema": "http://json-schema.org/draft-07/schema#"})),
                "dialect",
            ),
            (
                "an unknown format",
                Some(json!({"properties": {"a": {"format": "bogus"}}})),
                "cannot be enforced",
            ),
            (
                "an unenforced IDN format",
                Some(json!({"properties": {"a": {"format": "idn-hostname"}}})),
                "cannot be enforced",
            ),
            (
                "a backtracking pattern",
                Some(json!({"properties": {"a": {"pattern": "(a)\\1"}}})),
                "cannot be enforced",
            ),
            (
                "a schema that is no schema",
                Some(json!({"properties": {"a": 7}})),
                "not one",
            ),
        ];
        for (case, parameters, expected) in cases {
            let message = incomplete_message(read(
                calling("f", "{}"),
                &offering_schema("f", parameters),
                None,
            ));
            assert!(
                (message.contains("(unsupported parameter schema)")
                    || message.contains("(missing parameter schema)"))
                    && message.contains(expected),
                "{case}: {message}"
            );
        }
        let mut deep = json!({"type": "object"});
        for _ in 0..40 {
            deep = json!({"not": deep});
        }
        assert!(
            incomplete_message(read(
                calling("f", "{}"),
                &offering_schema("f", Some(deep)),
                None
            ))
            .contains("nests deeper")
        );
    }

    #[test]
    fn admission_follows_only_the_request_that_produced_the_response() {
        let two = || offering(&["f", "g"]);
        let forced = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "f".to_owned(),
                },
            })),
            ..two()
        };
        read(calling("f", "{}"), &forced, None).expect("the forced function");
        let required = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Mode("required".to_owned())),
            ..two()
        };
        read(calling("g", "{}"), &required, None).expect("any offered function");
        let serial = ChatConversation {
            parallel_tool_calls: Some(false),
            ..two()
        };
        read(calling("g", "{}"), &serial, None).expect("one call");

        // Conversations admission never trusts to have been validated.
        let mut duplicated = offering(&["f"]);
        duplicated.tools.push(ToolWire {
            function: FunctionDefinitionWire {
                parameters: Some(json!(true)),
                ..tool("f").function
            },
            ..tool("f")
        });
        let named_unoffered = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "h".to_owned(),
                },
            })),
            ..two()
        };
        let unknown_mode = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Mode("anything".to_owned())),
            ..two()
        };
        let not_a_function = ChatConversation {
            tools: vec![ToolWire {
                tool_type: "code_interpreter".to_owned(),
                ..tool("f")
            }],
            ..offering(&[])
        };
        let cases: [(&str, ChatConversation, &str); 6] = [
            ("another function than the forced one", forced, "g"),
            (
                "tool_choice none",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Mode("none".to_owned())),
                    ..two()
                },
                "f",
            ),
            ("a function offered twice", duplicated, "f"),
            (
                "a forced function that was never offered",
                named_unoffered,
                "h",
            ),
            ("an unknown tool_choice mode", unknown_mode, "f"),
            ("a tool that is not a function", not_a_function, "f"),
        ];
        for (case, conversation, name) in cases {
            let message = incomplete_message(read(calling(name, "{}"), &conversation, None));
            assert!(message.contains("(unauthorized)"), "{case}: {message}");
        }
        let message = incomplete_message(read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"b","function":{"name":"g","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &ChatConversation {
                parallel_tool_calls: Some(false),
                ..two()
            },
            None,
        ));
        assert!(
            message.contains("(unauthorized)") && message.contains("parallel"),
            "{message}"
        );
    }

    #[test]
    fn conflicting_or_malformed_fragments_are_refused_not_merged() {
        let cases = [
            (
                "a different id at one index",
                r#"[{"index":0,"id":"call_a","function":{"name":"f","arguments":"{}"}},{"index":0,"id":"call_b"}]"#,
                "changed its id",
            ),
            (
                "one id at two indexes",
                r#"[{"index":0,"id":"call_a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"call_a","function":{"name":"f","arguments":"{}"}}]"#,
                "two different indexes",
            ),
            (
                "no index in multiple calls",
                r#"[{"id":"call_a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"call_b"}]"#,
                "no valid `index`",
            ),
            (
                "a negative index",
                r#"[{"index":-1,"id":"call_a"}]"#,
                "no valid `index`",
            ),
            (
                "a non-function type",
                r#"[{"index":0,"id":"call_a","type":"code_interpreter"}]"#,
                "unsupported type",
            ),
            (
                "arguments that are not a string",
                r#"[{"index":0,"id":"call_a","function":{"name":"f","arguments":{"a":1}}}]"#,
                "not a string",
            ),
            (
                "tool_calls that is not an array",
                r#"{"index":0}"#,
                "not an array",
            ),
        ];
        for (case, calls, expected) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(tool_delta(calls)), Step::Chunk(TOOL_CALLS)],
                &offering(&["f"]),
                None,
            ));
            assert!(message.contains(expected), "{case}: {message}");
        }
    }

    #[test]
    fn missing_or_malformed_arguments_are_never_completed() {
        let cases = [
            (
                "no arguments",
                r#"[{"index":0,"id":"c","function":{"name":"f"}}]"#,
            ),
            (
                "truncated JSON",
                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#,
            ),
            (
                "a JSON array",
                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"[1]"}}]"#,
            ),
            (
                "trailing data",
                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{} {}"}}]"#,
            ),
        ];
        for (case, calls) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(tool_delta(calls)), Step::Chunk(TOOL_CALLS)],
                &offering(&["f"]),
                None,
            ));
            assert!(message.contains("not one JSON object"), "{case}: {message}");
        }
        // Arguments that a later call never had are not borrowed from an earlier one.
        let message = incomplete_message(read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c0","function":{"name":"f","arguments":"{\"a\":1}"}},{"index":1,"id":"c1","function":{"name":"f"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            None,
        ));
        assert!(message.contains("tool call 1"), "{message}");
    }

    #[test]
    fn incomplete_tool_call_streams_stay_incomplete_and_keep_their_evidence() {
        let partial =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#);
        let complete =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#);
        let length = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n";
        let cases: [(&str, Vec<Step>, &str); 5] = [
            (
                "a stream cut before its finish",
                vec![Step::Chunk(CONTENT), Step::Chunk(partial)],
                "without a finish event",
            ),
            (
                "a tool_calls finish without a call",
                vec![Step::Chunk(CONTENT), Step::Chunk(TOOL_CALLS)],
                "sent no tool call",
            ),
            (
                "a call truncated by the length limit",
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(complete),
                    Step::Chunk(length),
                ],
                "finish reason `length`",
            ),
            (
                "a call without an id",
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"function":{"name":"f","arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                ],
                "has no id",
            ),
            (
                "a call without a name",
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"id":"c","function":{"arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                ],
                "no valid function name",
            ),
        ];
        for (case, steps, expected) in cases {
            let mut sink = RecordingSink::default();
            let message = incomplete_message(read(steps, &offering(&["f"]), Some(&mut sink)));
            assert!(message.contains(expected), "{case}: {message}");
            assert_eq!(sink.deltas, ["hi"], "{case}: the streamed text is kept");
        }
    }

    #[test]
    fn a_cancellation_during_argument_reconstruction_is_a_local_stop_without_tool_calls() {
        let mut sink = RecordingSink::default();
        let error = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#,
                )),
                Step::Cancel,
                Step::Chunk(tool_delta(r#"[{"index":0,"function":{"arguments":"1}"}}]"#)),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            Some(&mut sink),
        )
        .expect_err("a cancelled stream stops");
        assert!(
            matches!(&error, ProviderExecutionError::Stopped { facts: seen, .. } if *seen == facts(None, None)),
            "got {error:?}"
        );
        assert!(sink.deltas.is_empty());
    }

    #[test]
    fn a_stalled_stream_during_argument_reconstruction_is_unknown() {
        let result =
            ntex::rt::System::new("tool-idle-test", ntex::rt::DefaultRuntime).block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                read_response(
                    payload(
                        vec![
                            Step::Chunk(tool_delta(
                                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{"}}]"#,
                            )),
                            Step::Stall,
                        ],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    None,
                    &offering(&["f"]),
                )
                .await
            });
        assert!(
            matches!(result, Err(ProviderExecutionError::Unknown { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn text_reasoning_and_tool_calls_in_one_response_are_each_kept() {
        let mut sink = RecordingSink::default();
        let result = read(
            vec![
                Step::Chunk(
                    "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"need \"}}]}\n\n",
                ),
                Step::Chunk(
                    "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"weather\"}}]}\n\n",
                ),
                Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\"Checking.\"}}]}\n\n"),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"get_weather","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["get_weather"]),
            Some(&mut sink),
        )
        .expect("a mixed response");
        assert_eq!(result.text, "Checking.");
        assert_eq!(
            sink.deltas,
            ["need weather", "Checking."],
            "reasoning precedes the answer"
        );
        assert_eq!(result.reasoning, "need weather");
        assert_eq!(result.tool_calls, [call(0, "c", "get_weather", "{}")]);
        assert!(result.requests_tools());
    }

    #[test]
    fn a_tool_call_exchange_is_neither_a_final_answer_nor_available_evidence() {
        let forced = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "f".to_owned(),
                },
            })),
            ..offering(&["f", "g"])
        };
        // A forced function finishes with `stop` on the OpenAI wire.
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(STOP),
            ],
            &forced,
            None,
        )
        .expect("a forced call");
        assert_eq!(result.raw_finish_reason.as_deref(), Some("stop"));
        assert!(
            result.requests_tools(),
            "a stop beside tool calls still asks for tools"
        );

        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            None,
        )
        .expect("a tool call");
        assert_ne!(
            success_observation(&result).classification(),
            crate::persistence::AvailabilityClassification::Available,
            "a tool_calls finish is not a normal terminal answer"
        );
        assert_eq!(
            result_value(&result),
            json!({"text": "", "finish_reason": "tool_calls"}),
            "nothing claims a tool ran or produced output"
        );
    }

    #[test]
    fn tool_calls_beyond_what_the_request_allowed_are_refused() {
        let call_f =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#);
        let none = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Mode("none".to_owned())),
            ..offering(&["f"])
        };
        let serial = ChatConversation {
            parallel_tool_calls: Some(false),
            ..offering(&["f"])
        };
        let cases: [(&str, ChatConversation, &'static str); 4] = [
            (
                "a Job objective offers no tools",
                ChatConversation::default(),
                call_f,
            ),
            ("an undeclared tool", offering(&["g"]), call_f),
            ("tool_choice none", none, call_f),
            (
                "parallel calls turned off",
                serial,
                tool_delta(
                    r#"[{"index":0,"id":"a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"b","function":{"name":"f","arguments":"{}"}}]"#,
                ),
            ),
        ];
        for (case, conversation, calls) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(calls), Step::Chunk(TOOL_CALLS)],
                &conversation,
                None,
            ));
            assert!(
                message.contains("invalid provider tool call"),
                "{case}: {message}"
            );
        }
    }

    #[test]
    fn usage_beside_a_refused_tool_fragment_is_observed_and_the_call_is_not() {
        let usage = r#"{"prompt_tokens":7,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":1},"vendor":{"n":1}}"#;
        let refused = leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"lost\",\"tool_calls\":[{{\"index\":0,\"id\":\"c\",\"type\":\"code_interpreter\"}}]}}}}],\"usage\":{usage}}}\n\n"
        ));
        let mut sink = RecordingSink::default();
        let error = read(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(refused),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            Some(&mut sink),
        )
        .expect_err("a malformed tool call is refused");
        match &error {
            ProviderExecutionError::Incomplete {
                message,
                facts: seen,
            } => {
                assert!(message.contains("unsupported type"), "{message}");
                assert_eq!(*seen, facts(None, None));
            }
            other => panic!("expected an incomplete stream, got {other:?}"),
        }
        assert_eq!(
            sink.deltas,
            ["hi"],
            "no text of the refused event is published"
        );
        let raw: Value = serde_json::from_str(usage).expect("usage object");
        assert_eq!(
            sink.usages(),
            [(
                UsageFinality::Cumulative,
                NormalizedUsage {
                    input_tokens: Some(7),
                    output_tokens: Some(2),
                    cache_read_tokens: Some(1),
                    raw: Some(raw),
                    ..NormalizedUsage::default()
                }
            )]
        );
    }

    #[test]
    fn refused_tool_calls_keep_the_facts_they_observed_and_stay_unconfirmed() {
        let call_g =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"g","arguments":"{}"}}]"#);
        let no_id = tool_delta(r#"[{"index":0,"function":{"name":"f","arguments":"{}"}}]"#);
        let forced = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "f".to_owned(),
                },
            })),
            ..offering(&["f"])
        };
        let cases: [(&str, Vec<Step>, ChatConversation, ResponseFacts); 3] = [
            (
                "an undeclared tool",
                vec![Step::Chunk(call_g), Step::Chunk(TOOL_CALLS)],
                offering(&["f"]),
                facts(Some(ChatFinishReason::ToolCalls), Some("tool_calls")),
            ),
            (
                "a forced call to another tool, finished with stop",
                vec![Step::Chunk(call_g), Step::Chunk(STOP)],
                forced,
                facts(Some(ChatFinishReason::Stop), Some("stop")),
            ),
            (
                "a call without an id, finished with stop",
                vec![Step::Chunk(no_id), Step::Chunk(STOP)],
                offering(&["f"]),
                facts(Some(ChatFinishReason::Stop), Some("stop")),
            ),
        ];
        for (case, steps, conversation, expected) in cases {
            let error = read(steps, &conversation, None).expect_err(case);
            assert!(
                matches!(&error, ProviderExecutionError::Incomplete { facts: seen, .. } if *seen == expected),
                "{case}: got {error:?}"
            );
            let observations = error_observations(&error);
            assert_eq!(observations.len(), 1, "{case}");
            assert_eq!(observations[0].status_code, Some(200), "{case}");
            assert_eq!(
                observations[0].finish_reason, expected.finish_reason,
                "{case}"
            );
            assert!(observations[0].failure.is_some(), "{case}");
            assert_eq!(
                observations[0].classification(),
                AvailabilityClassification::Unconfirmed,
                "{case}: a refused tool response is never available evidence"
            );
        }
    }

    #[test]
    fn streamed_tool_calls_are_bounded_per_call_and_per_response() {
        let oversized = leaked(format!(
            "[{{\"index\":0,\"id\":\"c\",\"function\":{{\"name\":\"f\",\"arguments\":\"{}\"}}}}]",
            "a".repeat(60 * 1024)
        ));
        let mut steps = vec![Step::Chunk(tool_delta(oversized))];
        let more = leaked(format!(
            "[{{\"index\":0,\"function\":{{\"arguments\":\"{}\"}}}}]",
            "a".repeat(60 * 1024)
        ));
        steps.extend(
            (0..MAX_TOOL_ARGUMENTS_BYTES / (60 * 1024)).map(|_| Step::Chunk(tool_delta(more))),
        );
        steps.push(Step::Chunk(TOOL_CALLS));
        let message = incomplete_message(read(steps, &offering(&["f"]), None));
        assert!(message.contains("arguments longer than"), "{message}");

        let calls: Vec<String> = (0..=MAX_TOOL_CALLS)
            .map(|index| format!("{{\"index\":{index},\"id\":\"c{index}\"}}"))
            .collect();
        let message = incomplete_message(read(
            vec![Step::Chunk(tool_delta(leaked(format!(
                "[{}]",
                calls.join(",")
            ))))],
            &offering(&["f"]),
            None,
        ));
        assert!(message.contains("more than"), "{message}");

        let long_name = leaked(format!(
            "[{{\"index\":0,\"id\":\"c\",\"function\":{{\"name\":\"{}\"}}}}]",
            "n".repeat(65)
        ));
        let message = incomplete_message(read(
            vec![Step::Chunk(tool_delta(long_name))],
            &offering(&["f"]),
            None,
        ));
        assert!(message.contains("function name longer"), "{message}");

        // Text, reasoning and tool calls share one response limit: the two
        // text bytes fit, the four bytes of the call that follows do not.
        let config = ProviderExecutionConfig {
            max_output_bytes: 5,
            ..ProviderExecutionConfig::default()
        };
        let mut sink = RecordingSink::default();
        let error = wait(read_response(
            payload(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                ],
                config.cancellation.clone(),
            ),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &offering(&["f"]),
        ))
        .expect_err("the response limit stops the stream");
        assert!(
            matches!(&error, ProviderExecutionError::Stopped { message, .. } if message.contains("exceeded its limit")),
            "got {error:?}"
        );
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn usage_in_a_tool_call_stream_is_observed_exactly_as_in_a_text_stream() {
        let steps = |middle: &'static str, finish: &'static str| {
            vec![
                Step::Chunk(running_total(
                    "{\"prompt_tokens\":5,\"completion_tokens\":1}",
                )),
                Step::Chunk(middle),
                Step::Chunk(running_total(
                    "{\"prompt_tokens\":5,\"completion_tokens\":3}",
                )),
                Step::Chunk(finish),
                Step::Chunk(usage_only("{\"prompt_tokens\":5,\"completion_tokens\":4}")),
                Step::Chunk(DONE),
            ]
        };
        let mut text_sink = RecordingSink::default();
        let text = read(
            steps(CONTENT, STOP),
            &offering(&["f"]),
            Some(&mut text_sink),
        )
        .expect("a text stream");
        let mut tool_sink = RecordingSink::default();
        let tools = read(
            steps(
                tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#),
                TOOL_CALLS,
            ),
            &offering(&["f"]),
            Some(&mut tool_sink),
        )
        .expect("a tool call stream");
        assert_eq!(tool_sink.usages(), text_sink.usages());
        assert_eq!(tool_sink.usages().len(), 3);
        assert_eq!(tools.usage, text.usage);
    }

    #[test]
    fn the_objective_request_body_is_unchanged() {
        let parameters = LlmParameters {
            temperature: Some(0.5),
            max_tokens: Some(64),
            ..LlmParameters::default()
        };
        let conversation = ChatConversation {
            messages: vec![ChatMessage::system("be brief"), ChatMessage::user("hi")],
            ..ChatConversation::default()
        };
        assert_eq!(
            request_body("model-a", &parameters, &conversation).expect("a body"),
            json!({
                "model": "model-a",
                "messages": [
                    {"role": "system", "content": "be brief"},
                    {"role": "user", "content": "hi"},
                ],
                "stream": true,
                "stream_options": {"include_usage": true},
                "temperature": 0.5,
                "max_tokens": 64,
            })
        );
    }

    #[test]
    fn an_assistant_tool_turn_and_its_results_serialize_in_order_with_exact_arguments() {
        let arguments = "{ \"city\" : \"Paris\",\"units\":\"c\" }";
        let mut assistant = ChatMessage::assistant(
            None,
            vec![ChatToolCallWire::from(&call(
                0,
                "call_1",
                "get_weather",
                arguments,
            ))],
        );
        assistant.reasoning_content = Some("need weather".to_owned());
        let conversation = ChatConversation {
            messages: vec![
                ChatMessage::user("weather?"),
                assistant,
                ChatMessage::tool("call_1", "18C"),
            ],
            tool_choice: Some(ToolChoiceWire::Mode("auto".to_owned())),
            parallel_tool_calls: Some(true),
            ..offering(&["get_weather"])
        };
        conversation.validate().expect("a valid conversation");
        let body =
            request_body("model-a", &LlmParameters::default(), &conversation).expect("a body");
        assert_eq!(
            body["messages"],
            json!([
                {"role": "user", "content": "weather?"},
                {
                    "role": "assistant",
                    "reasoning_content": "need weather",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "get_weather", "arguments": arguments},
                    }],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "18C"},
            ])
        );
        assert_eq!(
            body["tools"],
            json!([{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "the get_weather tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            }])
        );
        assert_eq!(body["tool_choice"], json!("auto"));
        assert_eq!(body["parallel_tool_calls"], json!(true));
        assert_eq!(body["model"], json!("model-a"), "the frozen model is sent");
    }

    #[test]
    fn invalid_or_unsupported_conversations_are_refused_before_sending() {
        let answered = |id: &str| {
            vec![
                ChatMessage::user("q"),
                ChatMessage::assistant(None, vec![ChatToolCallWire::from(&call(0, id, "f", "{}"))]),
                ChatMessage::tool(id, "r"),
            ]
        };
        let mut unsupported_type = tool("f");
        unsupported_type.tool_type = "retrieval".to_owned();
        let mut non_object_parameters = tool("f");
        non_object_parameters.function.parameters = Some(json!("string"));
        let mut duplicate_ids = answered("x");
        duplicate_ids.extend(answered("x").into_iter().skip(1));
        let cases: Vec<(&str, ChatConversation, &str)> = vec![
            ("no messages", ChatConversation::default(), "messages"),
            (
                "an unsupported tool type",
                ChatConversation {
                    tools: vec![unsupported_type],
                    ..offering(&[])
                },
                "tools[0].type",
            ),
            (
                "an invalid tool name",
                offering(&["get weather"]),
                "tools[0].function.name",
            ),
            (
                "a tool offered twice",
                offering(&["f", "f"]),
                "tools[1].function.name",
            ),
            (
                "non-object parameters",
                ChatConversation {
                    tools: vec![non_object_parameters],
                    ..offering(&[])
                },
                "tools[0].function.parameters",
            ),
            (
                "tool_choice without tools",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Mode("auto".to_owned())),
                    ..offering(&[])
                },
                "tool_choice",
            ),
            (
                "a forced tool that is not offered",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                        kind: "function".to_owned(),
                        function: NamedFunctionWire {
                            name: "g".to_owned(),
                        },
                    })),
                    ..offering(&["f"])
                },
                "tool_choice.function.name",
            ),
            (
                "an unknown tool_choice mode",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Mode("sometimes".to_owned())),
                    ..offering(&["f"])
                },
                "tool_choice",
            ),
            (
                "an unanswered tool call",
                ChatConversation {
                    messages: answered("x")[..2].to_vec(),
                    ..offering(&["f"])
                },
                "never answered",
            ),
            (
                "a tool result for an unknown call",
                ChatConversation {
                    messages: vec![ChatMessage::user("q"), ChatMessage::tool("x", "r")],
                    ..offering(&["f"])
                },
                "messages[1].tool_call_id",
            ),
            (
                "a message between a call and its result",
                ChatConversation {
                    messages: vec![
                        answered("x")[0].clone(),
                        answered("x")[1].clone(),
                        ChatMessage::user("interrupt"),
                        answered("x")[2].clone(),
                    ],
                    ..offering(&["f"])
                },
                "messages[2]",
            ),
            (
                "a tool call id used twice",
                ChatConversation {
                    messages: duplicate_ids,
                    ..offering(&["f"])
                },
                "messages[3].tool_calls[0].id",
            ),
            (
                "an empty tool_calls array",
                ChatConversation {
                    messages: vec![ChatMessage::user("q"), {
                        let mut message = ChatMessage::assistant(Some("a".to_owned()), Vec::new());
                        message.tool_calls = Some(Vec::new());
                        message
                    }],
                    ..offering(&["f"])
                },
                "messages[1].tool_calls",
            ),
            (
                "malformed tool call arguments",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user("q"),
                        ChatMessage::assistant(
                            None,
                            vec![ChatToolCallWire::from(&call(0, "x", "f", "{oops"))],
                        ),
                        ChatMessage::tool("x", "r"),
                    ],
                    ..offering(&["f"])
                },
                "tool_calls[0].function.arguments",
            ),
        ];
        for (case, conversation, field) in cases {
            let error = conversation.validate().expect_err(case);
            assert!(error.to_string().contains(field), "{case}: {error}");
        }
        answered("x");
        ChatConversation {
            messages: answered("x"),
            ..offering(&["f"])
        }
        .validate()
        .expect("an answered call is valid");
    }

    #[test]
    fn tool_definitions_and_arguments_are_size_limited() {
        let mut huge = tool("f");
        huge.function.description = Some("d".repeat(64 * 1024));
        let names: Vec<String> = (0..=MAX_TOOLS).map(|index| format!("t{index}")).collect();
        let too_many = offering(&names.iter().map(String::as_str).collect::<Vec<_>>());
        let oversized_arguments = format!("{{\"a\":\"{}\"}}", "a".repeat(MAX_TOOL_ARGUMENTS_BYTES));
        let long_id = "i".repeat(257);
        let cases: Vec<(&str, ChatConversation, &str)> = vec![
            (
                "an oversized definition",
                ChatConversation {
                    tools: vec![huge],
                    ..offering(&[])
                },
                "tools[0].function",
            ),
            ("too many tools", too_many, "tools"),
            (
                "oversized arguments",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user("q"),
                        ChatMessage::assistant(
                            None,
                            vec![ChatToolCallWire::from(&call(
                                0,
                                "x",
                                "f",
                                &oversized_arguments,
                            ))],
                        ),
                        ChatMessage::tool("x", "r"),
                    ],
                    ..offering(&["f"])
                },
                "function.arguments",
            ),
            (
                "an oversized id",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user("q"),
                        ChatMessage::assistant(
                            None,
                            vec![ChatToolCallWire::from(&call(0, &long_id, "f", "{}"))],
                        ),
                        ChatMessage::tool(long_id.clone(), "r"),
                    ],
                    ..offering(&["f"])
                },
                "tool_calls[0].id",
            ),
        ];
        for (case, conversation, field) in cases {
            let error = conversation.validate().expect_err(case);
            assert!(error.to_string().contains(field), "{case}: {error}");
        }
    }

    #[test]
    fn an_invalid_conversation_is_never_sent_and_is_known_not_delivered() {
        let frozen = FrozenExecutionConfig {
            agent: None,
            agent_ref: None,
            llm: crate::persistence::FrozenLlmConfig {
                config: crate::persistence::LlmConfig {
                    model: "model-a".to_owned(),
                    context_window: None,
                    parameters: LlmParameters::default(),
                    declared_capabilities: Default::default(),
                },
                model: crate::persistence::ConfigRef {
                    id: crate::persistence::ConfigId::new(2),
                    edit_seq: 1,
                },
                provider: crate::persistence::ProviderSnapshot {
                    id: crate::persistence::LlmProviderId::new(1),
                    edit_seq: 1,
                    name: "local".to_owned(),
                    transport: crate::persistence::LlmTransport::OpenAiChatCompletions,
                    // Nothing listens here; validation must refuse first.
                    endpoint: Some("https://127.0.0.1:9/v1".to_owned()),
                    credential_ref: None,
                },
            },
        };
        let conversation = ChatConversation {
            messages: vec![ChatMessage::user("q"), ChatMessage::tool("x", "r")],
            ..offering(&["f"])
        };
        let mut sink = RecordingSink::default();
        let error = wait(execute_conversation(
            &frozen,
            &conversation,
            &EnvironmentCredentialResolver,
            &ProviderExecutionConfig::default(),
            Some(&mut sink),
        ))
        .expect_err("an invalid conversation is refused");
        assert!(
            matches!(&error, ProviderExecutionError::Build { message } if message.contains("tool_call_id")),
            "got {error:?}"
        );
        assert_eq!(
            sink.observations
                .iter()
                .map(|observation| observation.fact.clone())
                .collect::<Vec<_>>(),
            [ProviderFact::NotDelivered]
        );
        assert_eq!(error_observations(&error), []);

        let objective = objective_conversation(
            &FrozenExecutionConfig {
                agent: Some(crate::persistence::AgentConfig {
                    dispatch_to: Vec::new(),
                    instructions: Some("be brief".to_owned()),
                    ..Default::default()
                }),
                ..frozen
            },
            Some("hi"),
        );
        objective
            .validate()
            .expect("an objective is a valid conversation");
        assert!(objective.tools.is_empty() && objective.tool_choice.is_none());
        assert_eq!(
            serde_json::to_value(&objective.messages).expect("messages"),
            json!([
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": "hi"},
            ])
        );
    }

    /// A frozen configuration whose endpoint nothing listens on.
    #[test]
    fn provider_network_reasoning_first_delta_deadline_and_stop() {
        let mut frozen = unreachable_frozen();
        let base = ProviderExecutionConfig::default();
        assert_eq!(base.for_profile(&frozen).first_delta_timeout, None);
        frozen
            .llm
            .config
            .declared_capabilities
            .insert(crate::persistence::ModelCapability::Reasoning);
        assert_eq!(
            base.for_profile(&frozen).first_delta_timeout,
            Some(Duration::from_secs(600))
        );
        crate::test_support::on_system(move || async move {
            let content = "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
            let role = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n";
            let reasoning =
                "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n";
            let tool = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]}}]}\n\n";
            for (steps, first, succeeds, stopped) in [
                (
                    vec![
                        Step::Chunk(role),
                        Step::Quiet(Duration::from_millis(250)),
                        Step::Chunk(content),
                    ],
                    Some(Duration::from_secs(1)),
                    true,
                    false,
                ),
                (
                    vec![
                        Step::Quiet(Duration::from_millis(250)),
                        Step::Chunk(content),
                    ],
                    None,
                    false,
                    false,
                ),
                (
                    vec![
                        Step::Quiet(Duration::from_millis(200)),
                        Step::Chunk(role),
                        Step::Quiet(Duration::from_millis(200)),
                        Step::Chunk(content),
                    ],
                    Some(Duration::from_millis(300)),
                    false,
                    false,
                ),
                (
                    vec![
                        Step::Chunk(reasoning),
                        Step::Quiet(Duration::from_millis(250)),
                        Step::Chunk(content),
                    ],
                    Some(Duration::from_secs(1)),
                    false,
                    false,
                ),
                (
                    vec![
                        Step::Chunk(tool),
                        Step::Quiet(Duration::from_millis(250)),
                        Step::Chunk(content),
                    ],
                    Some(Duration::from_secs(1)),
                    false,
                    false,
                ),
                (
                    vec![Step::Cancel, Step::Stall],
                    Some(Duration::from_secs(600)),
                    false,
                    true,
                ),
            ] {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    first_delta_timeout: first,
                    ..ProviderExecutionConfig::default()
                };
                let result = parse_stream(
                    payload(steps, config.cancellation.clone()),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    None,
                    &mut UsageCapture::default(),
                )
                .await;
                assert_eq!(result.is_ok(), succeeds);
                if stopped {
                    assert!(matches!(
                        result,
                        Err(ProviderExecutionError::Stopped { .. })
                    ));
                } else if !succeeds {
                    assert!(matches!(
                        result,
                        Err(ProviderExecutionError::Unknown { .. })
                    ));
                }
            }
        });
    }

    fn unreachable_frozen() -> FrozenExecutionConfig {
        FrozenExecutionConfig {
            agent: None,
            agent_ref: None,
            llm: crate::persistence::FrozenLlmConfig {
                config: crate::persistence::LlmConfig {
                    model: "model-v".to_owned(),
                    context_window: None,
                    parameters: LlmParameters::default(),
                    declared_capabilities: Default::default(),
                },
                model: crate::persistence::ConfigRef {
                    id: crate::persistence::ConfigId::new(2),
                    edit_seq: 1,
                },
                provider: crate::persistence::ProviderSnapshot {
                    id: crate::persistence::LlmProviderId::new(1),
                    edit_seq: 1,
                    name: "local".to_owned(),
                    transport: crate::persistence::LlmTransport::OpenAiChatCompletions,
                    endpoint: Some("https://127.0.0.1:9/v1".to_owned()),
                    credential_ref: None,
                },
            },
        }
    }

    fn body_of(frozen: &FrozenExecutionConfig, conversation: &ChatConversation) -> Value {
        let config = &frozen.llm.config;
        request_body(&config.model, &config.parameters, conversation).expect("a body")
    }

    #[test]
    fn images_reach_the_request_body_as_real_parts_and_text_alone_is_unchanged() {
        use crate::openai_compatible::image::tests::{PNG_BASE64, jpeg, png};
        use crate::openai_compatible::{ImageInput, ImageMime};

        let frozen = unreachable_frozen();
        let text_only = body_of(&frozen, &objective_conversation(&frozen, Some("hi")));
        assert_eq!(
            text_only,
            json!({
                "messages": [{"role": "user", "content": "hi"}],
                "model": "model-v",
                "stream": true,
                "stream_options": {"include_usage": true},
            })
        );
        assert_eq!(
            body_of(
                &frozen,
                &objective_conversation_with_images(&frozen, Some("hi"), &[])
            ),
            text_only,
            "no image, no change"
        );

        let images = [
            ImageInput::new(ImageMime::Png, png()).expect("a PNG"),
            ImageInput::new(ImageMime::Jpeg, jpeg()).expect("a JPEG"),
        ];
        let conversation =
            objective_conversation_with_images(&frozen, Some("Read the image."), &images);
        conversation.validate().expect("valid");
        let jpeg_url = format!(
            "data:image/jpeg;base64,{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, jpeg())
        );
        assert_eq!(
            body_of(&frozen, &conversation),
            json!({
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "Read the image."},
                    {"type": "image_url", "image_url": {
                        "url": format!("data:image/png;base64,{PNG_BASE64}")
                    }},
                    {"type": "image_url", "image_url": {"url": jpeg_url}},
                ]}],
                "model": "model-v",
                "stream": true,
                "stream_options": {"include_usage": true},
            })
        );
        let image_only = objective_conversation_with_images(&frozen, Some(""), &images[..1]);
        image_only.validate().expect("an image alone is valid");
        assert_eq!(
            body_of(&frozen, &image_only)["messages"],
            json!([{"role": "user", "content": [{"type": "image_url", "image_url": {
                "url": format!("data:image/png;base64,{PNG_BASE64}")
            }}]}])
        );
    }

    #[test]
    fn an_invalid_image_is_never_sent_and_is_known_not_delivered() {
        use crate::openai_compatible::image::tests::PNG_BASE64;
        use crate::openai_compatible::{ContentPartWire, ImageUrlWire};

        let conversation = ChatConversation {
            messages: vec![ChatMessage::user_parts(vec![ContentPartWire::ImageUrl {
                image_url: ImageUrlWire {
                    url: format!("data:image/jpeg;base64,{PNG_BASE64}"),
                },
            }])],
            ..ChatConversation::default()
        };
        let mut sink = RecordingSink::default();
        let error = wait(execute_conversation(
            &unreachable_frozen(),
            &conversation,
            &EnvironmentCredentialResolver,
            &ProviderExecutionConfig::default(),
            Some(&mut sink),
        ))
        .expect_err("a mislabelled image is refused");
        assert!(
            matches!(&error, ProviderExecutionError::Build { message }
                if message.contains("not the image/jpeg") && !message.contains(&PNG_BASE64[..12])),
            "got {error:?}"
        );
        assert_eq!(
            sink.observations
                .iter()
                .map(|observation| observation.fact.clone())
                .collect::<Vec<_>>(),
            [ProviderFact::NotDelivered]
        );
    }

    /// Conversations whose refusals must keep nothing the Provider said:
    /// one PNG, a PNG and a JPEG, and an image in an earlier turn followed
    /// by a tool call and its answer.
    pub(crate) fn image_conversations() -> Vec<(&'static str, ChatConversation)> {
        use crate::openai_compatible::image::tests::{jpeg, png};
        use crate::openai_compatible::{ChatFunctionCallWire, ChatToolCallWire};
        use crate::openai_compatible::{ImageInput, ImageMime};

        let png = ImageInput::new(ImageMime::Png, png()).expect("a PNG");
        let jpeg = ImageInput::new(ImageMime::Jpeg, jpeg()).expect("a JPEG");
        let text = |text: &str| ContentPartWire::Text {
            text: text.to_owned(),
        };
        let call = ChatToolCallWire {
            id: "call_1".to_owned(),
            tool_type: Some("function".to_owned()),
            function: ChatFunctionCallWire {
                name: "f".to_owned(),
                arguments: "{}".to_owned(),
            },
        };
        let user = |parts| ChatConversation {
            messages: vec![ChatMessage::user_parts(parts)],
            ..ChatConversation::default()
        };
        vec![
            ("one PNG", user(vec![text("Read it."), png.part()])),
            (
                "a PNG and a JPEG",
                user(vec![png.part(), text("and"), jpeg.part()]),
            ),
            (
                "an image before a tool continuation",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user_parts(vec![text("Read it."), jpeg.part()]),
                        ChatMessage::assistant(None, vec![call]),
                        ChatMessage::tool("call_1", "{\"ok\":true}"),
                        ChatMessage::user("Go on."),
                    ],
                    ..offering(&["f"])
                },
            ),
        ]
    }

    /// The `data:` URLs of a conversation's images.
    pub(crate) fn image_urls(conversation: &ChatConversation) -> Vec<String> {
        conversation
            .messages
            .iter()
            .filter_map(|message| match &message.content {
                Some(crate::openai_compatible::MessageContentWire::Parts(parts)) => Some(parts),
                _ => None,
            })
            .flatten()
            .filter_map(|part| match part {
                ContentPartWire::ImageUrl { image_url } => Some(image_url.url.clone()),
                ContentPartWire::Text { .. } => None,
            })
            .collect()
    }

    /// A refusal that echoes every image of the request: in its message,
    /// its code and an extra field, raw and with JSON's optional escapes.
    pub(crate) fn echoing_body(urls: &[String]) -> String {
        let all = urls.join(" ");
        let escaped = all.replace('/', "\\/").replace('+', "\\u002b");
        format!(
            r#"{{"error":{{"message":"invalid image: {all}","code":"{escaped}","type":"invalid_request_error","param":"{escaped}"}}}}"#
        )
    }

    /// Whether `text` holds any part of an image's base64, written raw or
    /// with JSON's escapes.
    pub(crate) fn discloses(text: &str, urls: &[String]) -> bool {
        let text = text.replace("\\/", "/").replace("\\u002b", "+");
        urls.iter().any(|url| {
            let (_, data) = url.split_once(',').expect("a data URL");
            data.as_bytes()
                .chunks(16)
                .filter(|window| window.len() == 16)
                .any(|window| text.contains(std::str::from_utf8(window).expect("ASCII")))
        })
    }

    #[test]
    fn a_refused_image_request_keeps_only_its_status() {
        for (case, conversation) in image_conversations() {
            assert!(conversation.carries_images(), "{case}");
            conversation.validate().expect(case);
            let urls = image_urls(&conversation);
            let body = echoing_body(&urls);
            assert!(discloses(&body, &urls), "the fixture echoes the images");
            for (status, retryable) in [
                (400, false),
                (401, false),
                (413, false),
                (408, true),
                (429, true),
                (500, true),
                (503, true),
            ] {
                let failure = rejected_failure(
                    &conversation,
                    status,
                    Some(urls.join(",")),
                    Some(body.clone()),
                );
                assert_eq!(
                    failure,
                    ProviderFailure {
                        message: format!("provider rejected image request (HTTP {status})"),
                        status_code: Some(status),
                        provider_code: None,
                        response_body: None,
                        request_id: None,
                        retry_after_ms: None,
                        retryable,
                    },
                    "{case}, HTTP {status}"
                );
                let error = ProviderExecutionError::Rejected { failure };
                assert!(!discloses(&error.to_string(), &urls), "{case}");
                for observation in error_observations(&error) {
                    assert!(!discloses(&format!("{observation:?}"), &urls), "{case}");
                }
            }
        }
    }

    #[test]
    fn a_refused_text_request_keeps_what_the_provider_said() {
        let body =
            r#"{"error":{"message":"The model `m` does not exist","code":"model_not_found"}}"#;
        for conversation in [
            ChatConversation {
                messages: vec![ChatMessage::user("hi")],
                ..ChatConversation::default()
            },
            ChatConversation {
                messages: vec![ChatMessage::user_parts(vec![ContentPartWire::Text {
                    text: "hi".to_owned(),
                }])],
                ..ChatConversation::default()
            },
        ] {
            assert!(!conversation.carries_images());
            for (status, retryable) in [(404, false), (503, true)] {
                assert_eq!(
                    rejected_failure(
                        &conversation,
                        status,
                        Some("req_123".to_owned()),
                        Some(body.to_owned())
                    ),
                    ProviderFailure {
                        message: "The model `m` does not exist".to_owned(),
                        status_code: Some(status),
                        provider_code: Some("model_not_found".to_owned()),
                        response_body: Some(body.to_owned()),
                        request_id: Some("req_123".to_owned()),
                        retry_after_ms: None,
                        retryable,
                    }
                );
            }
            assert_eq!(
                rejected_failure(
                    &conversation,
                    502,
                    None,
                    Some("<html>bad gateway</html>".into())
                ),
                ProviderFailure {
                    message: "provider returned HTTP 502".to_owned(),
                    status_code: Some(502),
                    response_body: Some("<html>bad gateway</html>".to_owned()),
                    retryable: true,
                    ..ProviderFailure::default()
                }
            );
        }
    }

    mod credential_redaction {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::Duration;

        use super::*;
        use crate::persistence::{
            ConfigId, ConfigRef, FrozenLlmConfig, LlmConfig, LlmProviderId, LlmTransport,
            ProviderSnapshot,
        };
        use crate::test_support::on_system;

        const SECRET: &str = "sk-C6-redaction-secret-0123456789";

        struct FixedCredential(Option<&'static str>);

        impl CredentialResolver for FixedCredential {
            fn resolve(&self, _: Option<&str>) -> Result<Option<String>, ProviderExecutionError> {
                Ok(self.0.map(str::to_owned))
            }
        }

        fn frozen(endpoint: &str, credential_ref: Option<&str>) -> FrozenExecutionConfig {
            FrozenExecutionConfig {
                agent: None,
                agent_ref: None,
                llm: FrozenLlmConfig {
                    config: LlmConfig {
                        model: "test-model".to_owned(),
                        context_window: None,
                        parameters: LlmParameters::default(),
                        declared_capabilities: Default::default(),
                    },
                    provider: ProviderSnapshot {
                        id: LlmProviderId::new(1),
                        edit_seq: 1,
                        name: "test".to_owned(),
                        transport: LlmTransport::OpenAiChatCompletions,
                        endpoint: Some(endpoint.to_owned()),
                        credential_ref: credential_ref.map(str::to_owned),
                    },
                    model: ConfigRef {
                        id: ConfigId::new(1),
                        edit_seq: 1,
                    },
                },
            }
        }

        fn response(status: &str, headers: &[(&str, &str)], body: &str) -> Vec<u8> {
            let mut text = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
            for (name, value) in headers {
                text.push_str(&format!("{name}: {value}\r\n"));
            }
            text.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
            text.push_str(body);
            text.into_bytes()
        }

        /// Serves one HTTP answer on a local port and returns its endpoint.
        fn serve(answer: Vec<u8>) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
            let address = listener.local_addr().expect("local address");
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("accept");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout");
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                let mut needed = None;
                while needed.is_none_or(|needed| request.len() < needed) {
                    let read = stream.read(&mut buffer).expect("read the request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if needed.is_none()
                        && let Some(end) =
                            request.windows(4).position(|window| window == b"\r\n\r\n")
                    {
                        let head = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                        let length = head
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .and_then(|value| value.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        needed = Some(end + 4 + length);
                    }
                }
                stream.write_all(&answer).expect("write the answer");
                stream.flush().ok();
            });
            format!("http://{address}/v1")
        }

        fn failed(answer: Vec<u8>, secret: Option<&'static str>) -> ProviderExecutionError {
            let endpoint = serve(answer);
            let frozen = frozen(&endpoint, secret.map(|_| "C6_TEST_CREDENTIAL"));
            on_system(move || async move {
                execute_conversation(
                    &frozen,
                    &objective_conversation(&frozen, Some("say hi")),
                    &FixedCredential(secret),
                    &ProviderExecutionConfig::default(),
                    None,
                )
                .await
            })
            .expect_err("the scripted answer is a failure")
        }

        #[test]
        fn an_http_rejection_keeps_its_diagnostics_without_the_credential() {
            let body = format!(
                "{{\"error\":{{\"message\":\"bad key {SECRET}\",\"code\":\"invalid_api_key\"}}}}"
            );
            let error = failed(
                response(
                    "400 Bad Request",
                    &[
                        ("content-type", "application/json"),
                        ("x-request-id", SECRET),
                    ],
                    &body,
                ),
                Some(SECRET),
            );
            assert!(!format!("{error}").contains(SECRET));
            match error {
                ProviderExecutionError::Rejected { failure } => {
                    assert!(failure.message.contains("bad key"));
                    assert!(!failure.message.contains(SECRET));
                    assert_eq!(failure.provider_code.as_deref(), Some("invalid_api_key"));
                    assert!(
                        failure
                            .response_body
                            .as_deref()
                            .is_some_and(|body| !body.contains(SECRET)),
                        "the raw body is kept without the credential"
                    );
                    assert!(
                        failure
                            .request_id
                            .as_deref()
                            .is_some_and(|request| !request.contains(SECRET))
                    );
                }
                other => panic!("an HTTP rejection, got {other:?}"),
            }
        }

        #[test]
        fn a_streamed_error_never_returns_the_credential() {
            let body = format!(
                "data: {{\"error\":{{\"message\":\"invalid api key {SECRET}\",\"status\":401,\"code\":\"invalid_api_key\"}}}}\n\n"
            );
            let error = failed(
                response(
                    "200 OK",
                    &[
                        ("content-type", "text/event-stream"),
                        ("x-request-id", SECRET),
                    ],
                    &body,
                ),
                Some(SECRET),
            );
            assert!(!format!("{error}").contains(SECRET));
            match error {
                ProviderExecutionError::Rejected { failure } => {
                    assert!(failure.message.contains("invalid api key"));
                    assert!(!failure.message.contains(SECRET));
                    assert_eq!(failure.status_code, Some(401));
                    assert_eq!(failure.provider_code.as_deref(), Some("invalid_api_key"));
                    assert!(
                        failure
                            .request_id
                            .as_deref()
                            .is_some_and(|request| !request.contains(SECRET))
                    );
                }
                other => panic!("a streamed rejection, got {other:?}"),
            }
        }

        #[test]
        fn a_json_escaped_echo_is_redacted_before_and_after_parsing() {
            let secret = "sk-\"quoted\"\\key";
            let body = serde_json::json!({
                "error": {"message": format!("key {secret} rejected")}
            })
            .to_string();
            assert!(
                body.contains("quoted"),
                "the body echoes the secret escaped"
            );
            let error = failed(
                response(
                    "400 Bad Request",
                    &[("content-type", "application/json")],
                    &body,
                ),
                Some(secret),
            );
            match error {
                ProviderExecutionError::Rejected { failure } => {
                    assert!(failure.message.contains("rejected"));
                    assert!(!failure.message.contains("quoted"));
                    assert!(!failure.message.contains('\\'));
                    assert!(
                        failure
                            .response_body
                            .as_deref()
                            .is_some_and(|body| !body.contains("quoted"))
                    );
                }
                other => panic!("a JSON-escaped rejection, got {other:?}"),
            }
        }

        #[test]
        fn a_credential_free_request_keeps_the_provider_text() {
            let body = format!("{{\"error\":{{\"message\":\"bad key {SECRET}\"}}}}");
            let error = failed(
                response(
                    "400 Bad Request",
                    &[("content-type", "application/json")],
                    &body,
                ),
                None,
            );
            match error {
                ProviderExecutionError::Rejected { failure } => {
                    assert_eq!(failure.message, format!("bad key {SECRET}"));
                }
                other => panic!("a credential-free rejection, got {other:?}"),
            }
        }

        #[test]
        fn alternative_json_escapes_are_removed_from_retained_bodies() {
            let secret = "sk-synthetic/a";
            for spelling in [r"sk-synthetic\/a", r"\u0073k-synthetic/a"] {
                let body = format!(
                    r#"{{"error":{{"message":"denied {spelling}","nested":["{spelling}"]}}}}"#
                );
                let error = failed(
                    response(
                        "400 Bad Request",
                        &[("content-type", "application/json")],
                        &body,
                    ),
                    Some(secret),
                );
                let ProviderExecutionError::Rejected { failure } = error else {
                    panic!("expected rejection");
                };
                let retained: Value =
                    serde_json::from_str(failure.response_body.as_deref().expect("body"))
                        .expect("valid redacted JSON");
                assert!(!retained.to_string().contains(secret));
                assert_eq!(retained["error"]["nested"][0], "[redacted]");
                assert_eq!(failure.message, "denied [redacted]");
            }
        }
    }
}
