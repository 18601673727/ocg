use std::collections::BTreeMap;
use std::time::Instant;

use super::*;
#[cfg(test)]
use crate::persistence::{ChoiceOption, DecisionCapability, ScoreLevel};
use crate::persistence::{
    DecisionAnswer, DecisionConfidence, DecisionOutcome, DecisionQuestion, DecisionRequest,
    DecisionResult, FrozenDecisionConfig, RequestRef,
};

/// The native TypeSafe System One route. A configured endpoint replaces the
/// base; the resource is appended the same way as for generative transports.
pub(crate) const DEFAULT_ENDPOINT: &str = "https://api.typesafe.ai/v1";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// One accepted structured answer and the facts of the request that produced
/// it. The raw response is kept for provenance; normalized meaning lives in
/// `result`.
pub(crate) struct DecisionExecution {
    pub result: DecisionResult,
    pub raw: Value,
    pub usage: NormalizedUsage,
    pub status_code: u16,
    pub finished_in: Duration,
}

/// Sends one typed decision request and decodes the structured answer.
///
/// Pre-send validation happens before any credential or request is built, so
/// a refused request is unambiguously [`ProviderExecutionError::Build`]. An
/// accepted HTTP response with a missing, extra, mismatched or malformed
/// answer is [`ProviderExecutionError::Incomplete`]: the provider answered,
/// but not with a usable decision.
pub(crate) async fn execute_decision(
    frozen: &FrozenDecisionConfig,
    input: &DecisionRequest,
    body: &Value,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    reference: RequestRef,
) -> Result<DecisionExecution, ProviderExecutionError> {
    input
        .validate(&frozen.model.declared_capabilities)
        .map_err(|message| ProviderExecutionError::Build {
            message: message.to_owned(),
        })?;
    if !matches!(
        input.state,
        Value::String(_) | Value::Object(_) | Value::Array(_)
    ) {
        return Err(ProviderExecutionError::Build {
            message: "decision state must be a string, object or array".to_owned(),
        });
    }
    let endpoint = exchange::endpoint(
        frozen.endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT),
        "systemone",
    )?;
    let credential = resolver.resolve(frozen.credential_ref.as_deref())?;
    let redactor = Redactor::new(credential.as_deref());
    let authorization = credential
        .as_deref()
        .map(exchange::authorization)
        .transpose()?;
    if config.cancellation.is_cancelled() {
        return Err(ProviderExecutionError::NotDelivered {
            message: "decision execution was cancelled before sending".to_owned(),
        });
    }
    // A scripted response stands in for the provider's answer, so a test
    // never reaches the network.
    #[cfg(test)]
    {
        let started = Instant::now();
        if let Some(scripted) = scripted::http_response(&endpoint, authorization.as_ref()) {
            let (status, body) = scripted.map_err(|()| ProviderExecutionError::Unknown {
                message: "decision request failed before a response arrived".to_owned(),
                facts: None,
                finished: false,
            })?;
            let finished_in = started.elapsed();
            if status != StatusCode::OK.as_u16() {
                let body = redactor.body(&String::from_utf8_lossy(&body));
                return Err(ProviderExecutionError::Rejected {
                    failure: observe::provider_failure(status, None, Some(body)),
                });
            }
            return decode_decision(
                frozen,
                input,
                reference,
                status,
                body,
                &redactor,
                finished_in,
            );
        }
    }
    let client = exchange::provider_client(config.timeout).await?;
    let request = client
        .post(endpoint)
        .set_header(CONTENT_TYPE, "application/json")
        .set_header(ACCEPT, "application/json")
        .if_some(authorization, |authorization, request| {
            request.set_header("authorization", authorization)
        });
    let started = Instant::now();
    let sent = select(
        config.cancellation.cancelled(),
        ntex::time::timeout(config.timeout, request.send_json(body)),
    )
    .await;
    let response = match sent {
        Either::Left(()) => {
            return Err(ProviderExecutionError::Interrupted {
                message: "decision execution was cancelled before a response arrived".to_owned(),
            });
        }
        Either::Right(sent) => sent,
    }
    .map_err(|_| ProviderExecutionError::Unknown {
        message: "decision request timed out after it may have been delivered".to_owned(),
        facts: None,
        finished: false,
    })?
    .map_err(observe::classify_client_error)?;
    let status = response.status().as_u16();
    let request_id = response
        .header("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(|value| redactor.text(value));
    let retry_after_ms = response
        .header("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(observe::retry_after_ms);
    let finished_in = started.elapsed();
    if status != StatusCode::OK.as_u16() {
        let body = match select(
            config.cancellation.cancelled(),
            ntex::time::timeout(
                config.timeout,
                observe::bounded_body(response, MAX_PROVIDER_ERROR_BYTES),
            ),
        )
        .await
        {
            Either::Left(()) => None,
            Either::Right(Ok(body)) => body,
            Either::Right(Err(())) => None,
        };
        let body = body.map(|body| redactor.body(&body));
        return Err(ProviderExecutionError::Rejected {
            failure: observe::with_retry_after(
                redactor.failure(observe::provider_failure(status, request_id, body)),
                retry_after_ms,
            ),
        });
    }
    let body = match select(
        config.cancellation.cancelled(),
        ntex::time::timeout(
            config.timeout,
            observe::bounded_bytes(
                response,
                [
                    frozen.model.limits.max_output_tokens_per_request,
                    frozen.model.limits.max_output_tokens_per_question,
                ]
                .into_iter()
                .flatten()
                .min()
                .map_or(MAX_RESPONSE_BYTES, |limit| {
                    (limit as usize)
                        .saturating_mul(64)
                        .saturating_add(4096)
                        .min(MAX_RESPONSE_BYTES)
                }),
            ),
        ),
    )
    .await
    {
        Either::Left(()) => {
            return Err(ProviderExecutionError::Stopped {
                message: "decision execution was cancelled while reading the response".to_owned(),
                facts: ResponseFacts {
                    status_code: status,
                    finish_reason: None,
                    raw_finish_reason: None,
                },
            });
        }
        Either::Right(Ok(Some(body))) => body,
        Either::Right(Ok(None) | Err(())) => {
            return Err(ProviderExecutionError::Incomplete {
                message: "decision response exceeded 1 MiB or could not be read".to_owned(),
                facts: ResponseFacts {
                    status_code: status,
                    finish_reason: None,
                    raw_finish_reason: None,
                },
            });
        }
    };
    let body: Vec<u8> = body;
    decode_decision(
        frozen,
        input,
        reference,
        status,
        body,
        &redactor,
        finished_in,
    )
}

/// The answer of one accepted decision response: the typed result, the
/// provider's own body, and the usage it reported.
fn decode_decision(
    frozen: &FrozenDecisionConfig,
    input: &DecisionRequest,
    reference: RequestRef,
    status: u16,
    body: Vec<u8>,
    redactor: &Redactor,
    finished_in: Duration,
) -> Result<DecisionExecution, ProviderExecutionError> {
    let facts = || ResponseFacts {
        status_code: status,
        finish_reason: None,
        raw_finish_reason: None,
    };
    let raw: Value =
        serde_json::from_slice(&body).map_err(|_| ProviderExecutionError::Incomplete {
            message: "decision response was not valid JSON".to_owned(),
            facts: facts(),
        })?;
    let raw = redactor.json_value(raw);
    let (result, usage) = decode_response(frozen, input, reference, &raw).map_err(|message| {
        ProviderExecutionError::Incomplete {
            message,
            facts: facts(),
        }
    })?;
    result
        .validate(input, frozen, reference)
        .map_err(|message| ProviderExecutionError::Incomplete {
            message: message.to_owned(),
            facts: facts(),
        })?;
    Ok(DecisionExecution {
        result,
        raw,
        usage,
        status_code: status,
        finished_in,
    })
}

pub(crate) fn request_body(
    frozen: &FrozenDecisionConfig,
    input: &DecisionRequest,
) -> Result<Value, ProviderExecutionError> {
    input
        .validate(&frozen.model.declared_capabilities)
        .map_err(|message| ProviderExecutionError::Build {
            message: message.into(),
        })?;
    if frozen.agent.workspace_access != crate::persistence::WorkspaceAccess::None {
        return Err(ProviderExecutionError::Build {
            message: "decision workspace materialization is unavailable".into(),
        });
    }
    if !matches!(
        input.state,
        Value::String(_) | Value::Object(_) | Value::Array(_)
    ) {
        return Err(ProviderExecutionError::Build {
            message: "decision state must be a string, object or array".into(),
        });
    }
    if matches!(&input.question, DecisionQuestion::Choice { options, .. } if options.len() > 255) {
        return Err(ProviderExecutionError::Build {
            message: "TypeSafe Choice supports at most 255 options".into(),
        });
    }
    if matches!(&input.question, DecisionQuestion::OrdinalScore { levels, .. } if levels.len() > 10)
    {
        return Err(ProviderExecutionError::Build {
            message: "TypeSafe Score supports at most ten levels".into(),
        });
    }
    let mut question = match &input.question {
        DecisionQuestion::Choice { prompt, options } => {
            let criteria = options
                .iter()
                .map(|option| (option.id.clone(), Value::String(option.description.clone())))
                .collect::<serde_json::Map<_, _>>();
            json!({
                "type": "choice",
                "instructions": prompt,
                "criteria": criteria,
            })
        }
        DecisionQuestion::BinaryProbability { prompt } => json!({
            "type": "noul",
            "instructions": prompt,
        }),
        DecisionQuestion::OrdinalScore { prompt, levels } => json!({
            "type": "score",
            "instructions": prompt,
            "criteria": levels
                .iter()
                .map(|level| level.description.clone())
                .collect::<Vec<_>>(),
        }),
    };
    if let Some(instructions) = &frozen.agent.instructions {
        question["instructions"] =
            json!({ "profile": instructions, "question": question["instructions"] });
    }
    let body = json!({
        "model": frozen.model.model,
        "state": input.state,
        "questions": { "decision": question },
    });
    // Without the provider's tokenizer these are conservative UTF-8 byte
    // upper bounds, including framing, rather than calibrated token counts.
    let request_bound = serde_json::to_vec(&body)
        .map_err(|_| ProviderExecutionError::Build {
            message: "decision request could not be serialized".into(),
        })?
        .len()
        .saturating_add(32);
    let question_bound = serde_json::to_vec(&json!({ "state": input.state, "question": question }))
        .map_err(|_| ProviderExecutionError::Build {
            message: "decision question could not be serialized".into(),
        })?
        .len()
        .saturating_add(32);
    for (bound, limit) in [
        (
            request_bound,
            frozen.model.limits.max_input_tokens_per_request,
        ),
        (
            question_bound,
            frozen.model.limits.max_input_tokens_per_question,
        ),
    ] {
        if limit.is_some_and(|limit| bound > limit as usize) {
            return Err(ProviderExecutionError::Build {
                message: "decision input exceeds a configured token upper bound".into(),
            });
        }
    }
    if serde_json::to_vec(&body)
        .map_err(|_| ProviderExecutionError::Build {
            message: "decision request could not be serialized".to_owned(),
        })?
        .len()
        > crate::persistence::MAX_DECISION_REQUEST_BYTES
    {
        return Err(ProviderExecutionError::Build {
            message: "decision request exceeds 1 MiB".to_owned(),
        });
    }
    Ok(body)
}

/// Decodes one TypeSafe response. The returned usage keeps the provider's raw
/// object and normalizes only the counters it reports.
fn decode_response(
    frozen: &FrozenDecisionConfig,
    input: &DecisionRequest,
    reference: RequestRef,
    raw: &Value,
) -> Result<(DecisionResult, NormalizedUsage), String> {
    if !frozen
        .model
        .declared_capabilities
        .contains(&input.question.capability())
    {
        return Err("decision primitive is not supported by the model".to_owned());
    }
    let reported_model = raw
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty() && model.len() <= 512)
        .ok_or_else(|| "decision response has no reported model".to_owned())?
        .to_owned();
    let answers = raw
        .get("answers")
        .and_then(Value::as_object)
        .ok_or_else(|| "decision response has no answers".to_owned())?;
    if answers.len() != 1 || !answers.contains_key("decision") {
        return Err("decision response must answer exactly one question".to_owned());
    }
    let answer = answers
        .get("decision")
        .and_then(Value::as_object)
        .ok_or_else(|| "decision answer is not an object".to_owned())?;
    let answer_type = answer
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "decision answer has no type".to_owned())?;
    let decoded = match &input.question {
        DecisionQuestion::Choice { options, .. } => {
            if answer_type != "choice" {
                return Err("decision answer type differs from the request".to_owned());
            }
            let selected = answer
                .get("choice")
                .and_then(Value::as_str)
                .filter(|selected| options.iter().any(|option| option.id == *selected))
                .ok_or_else(|| "decision choice is not one of the requested options".to_owned())?
                .to_owned();
            let distribution = answer
                .get("probabilities")
                .and_then(Value::as_object)
                .map(|probabilities| {
                    if probabilities.len() != options.len() {
                        return Err(
                            "choice distribution must contain exactly the requested options".into(),
                        );
                    }
                    options
                        .iter()
                        .map(|option| {
                            probabilities
                                .get(&option.id)
                                .and_then(Value::as_f64)
                                .map(|probability| (option.id.clone(), probability))
                                .ok_or_else(|| "choice distribution is incomplete".to_owned())
                        })
                        .collect::<Result<std::collections::BTreeMap<_, _>, _>>()
                })
                .transpose()?
                .ok_or_else(|| "choice answer has no distribution".to_owned())?;
            let distribution = Some(distribution);
            if distribution.as_ref().is_some_and(|distribution| {
                distribution.len() != options.len()
                    || probabilities_sum_to_one(distribution.values().copied()).is_err()
            }) {
                return Err("choice probabilities must cover every option and sum to 1".to_owned());
            }
            DecisionAnswer::Choice {
                selected,
                distribution,
                confidence: confidence(answer, "choice")?,
            }
        }
        DecisionQuestion::BinaryProbability { .. } => {
            if answer_type != "noul" {
                return Err("decision answer type differs from the request".to_owned());
            }
            DecisionAnswer::BinaryProbability {
                true_probability: answer
                    .get("noul")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| "decision answer has no probability".to_owned())?,
            }
        }
        DecisionQuestion::OrdinalScore { levels, .. } => {
            if answer_type != "score" {
                return Err("decision answer type differs from the request".to_owned());
            }
            let probabilities = answer
                .get("probabilities")
                .and_then(Value::as_object)
                .ok_or_else(|| "score answer has no distribution".to_owned())?;
            if probabilities.len() != levels.len() {
                return Err("score distribution must contain exactly the requested levels".into());
            }
            let distribution = levels
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    probabilities
                        .get(index.to_string().as_str())
                        .and_then(Value::as_f64)
                        .ok_or_else(|| "score distribution is incomplete".to_owned())
                })
                .collect::<Result<Vec<_>, _>>()?;
            if probabilities_sum_to_one(distribution.iter().copied()).is_err() {
                return Err("score probabilities must sum to 1".to_owned());
            }
            let expectation = levels
                .iter()
                .zip(&distribution)
                .map(|(level, probability)| f64::from(level.value) * probability)
                .sum::<f64>();
            DecisionAnswer::OrdinalScore {
                expectation,
                distribution: Some(distribution),
                confidence: confidence(answer, "score")?,
            }
        }
    };
    let usage = decode_usage(raw.get("usage"));
    let model_version = None;
    for (used, limit) in [
        (
            usage.input_tokens,
            frozen.model.limits.max_input_tokens_per_request,
        ),
        (
            usage.input_tokens,
            frozen.model.limits.max_input_tokens_per_question,
        ),
        (
            usage.output_tokens,
            frozen.model.limits.max_output_tokens_per_request,
        ),
        (
            usage.output_tokens,
            frozen.model.limits.max_output_tokens_per_question,
        ),
    ] {
        if used.zip(limit).is_some_and(|(used, limit)| used > limit) {
            return Err("decision usage exceeded a configured limit".into());
        }
    }
    Ok((
        DecisionResult {
            schema_version: crate::persistence::DECISION_SCHEMA_VERSION,
            outcome: DecisionOutcome::Answer { answer: decoded },
            requested_model: frozen.model.model.clone(),
            reported_model: Some(reported_model),
            model_version,
            model: frozen.model_ref,
            agent: frozen.agent_ref,
            connection: frozen.connection_ref,
            request: reference,
        },
        usage,
    ))
}

fn confidence(
    answer: &serde_json::Map<String, Value>,
    primitive: &str,
) -> Result<Option<DecisionConfidence>, String> {
    match answer.get("confidence") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_f64()
            .map(|value| {
                Some(DecisionConfidence {
                    value,
                    semantics: format!("provider.typesafe.{primitive}.confidence.v1"),
                })
            })
            .ok_or_else(|| "decision confidence is not a number".to_owned()),
    }
}

fn probabilities_sum_to_one(values: impl Iterator<Item = f64>) -> Result<(), ()> {
    let mut sum = 0.0;
    for value in values {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err(());
        }
        sum += value;
    }
    ((sum - 1.0).abs() <= 1e-6).then_some(()).ok_or(())
}

fn decode_usage(usage: Option<&Value>) -> NormalizedUsage {
    let Some(usage) = usage else {
        return NormalizedUsage::default();
    };
    let counter = |key: &str| {
        usage
            .get(key)
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
    };
    NormalizedUsage {
        input_tokens: counter("input_tokens"),
        output_tokens: counter("output_tokens"),
        raw: Some(usage.clone()),
        ..NormalizedUsage::default()
    }
}

/// Lists the model names one TypeSafe account may send. The route reports
/// `models[].name`, unlike the OpenAI-compatible `data[].id` shape.
pub(crate) async fn list_models(
    endpoint: &str,
    credential: Option<&str>,
) -> Result<Vec<ProviderModel>, ModelListError> {
    let base = exchange::endpoint(endpoint, "models").map_err(|_| ModelListError::Unreachable)?;
    let authorization = credential
        .map(exchange::authorization)
        .transpose()
        .map_err(|_| ModelListError::Unreachable)?;
    ntex::time::timeout(Duration::from_secs(30), async {
        let client = exchange::provider_client(Duration::from_secs(30))
            .await
            .map_err(|_| ModelListError::Unreachable)?;
        #[cfg(test)]
        let scripted = scripted::http_response(&base, authorization.as_ref());
        #[cfg(not(test))]
        let scripted: Option<Result<(u16, Vec<u8>), ()>> = None;
        let (status, body) = if let Some(response) = scripted {
            response.map_err(|()| ModelListError::Unreachable)?
        } else {
            let request = client
                .get(&base)
                .set_header(ACCEPT, "application/json")
                .if_some(authorization.clone(), |authorization, request| {
                    request.set_header("authorization", authorization)
                });
            let response = request
                .send()
                .await
                .map_err(|_| ModelListError::Unreachable)?;
            let status = response.status().as_u16();
            if status == 404 {
                return Err(ModelListError::NotListed);
            }
            let body = match response.body().limit(1024 * 1024).await {
                Ok(body) => body.to_vec(),
                Err(_) => return Err(ModelListError::Unreachable),
            };
            (status, body)
        };
        let value = (body.len() <= 1024 * 1024)
            .then(|| serde_json::from_slice::<Value>(&body).ok())
            .flatten();
        if !(200..300).contains(&status) {
            let message = value
                .as_ref()
                .and_then(|value| value.get("detail"))
                .and_then(Value::as_str)
                .or_else(|| value.as_ref()?.get("message")?.as_str())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("provider returned HTTP {status}"));
            let message = credential.filter(|secret| !secret.is_empty()).map_or_else(
                || message.clone(),
                |secret| message.replace(secret, "[redacted]"),
            );
            return Err(ModelListError::Rejected {
                status_code: status,
                message,
            });
        }
        let entries = value
            .as_ref()
            .and_then(|value| value.get("models"))
            .and_then(Value::as_array)
            .ok_or(ModelListError::NotListed)?;
        let mut models = BTreeMap::new();
        for entry in entries {
            let Some(name) = entry
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty() && name.len() <= 512)
            else {
                continue;
            };
            if credential.is_some_and(|secret| !secret.is_empty() && name.contains(secret)) {
                continue;
            }
            models.insert(name.to_owned(), None);
        }
        Ok(models
            .into_iter()
            .map(|(id, context_window)| ProviderModel { id, context_window })
            .collect())
    })
    .await
    .map_err(|_| ModelListError::Unreachable)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::RequestNumber;

    fn frozen() -> FrozenDecisionConfig {
        let id = "00000001-0000-7000-8000-000000000001"
            .parse()
            .expect("UUID");
        FrozenDecisionConfig {
            agent: crate::persistence::DecisionAgentConfig::default(),
            agent_ref: crate::persistence::ConfigRef { id, edit_seq: 1 },
            model: crate::persistence::DecisionModelConfig {
                connection_id: id,
                model: "jev-latest".into(),
                protocol: crate::persistence::DecisionProtocol::TypeSafeSystemOne,
                limits: crate::persistence::DecisionLimits::default(),
                declared_capabilities: [
                    DecisionCapability::Choice,
                    DecisionCapability::BinaryProbability,
                    DecisionCapability::OrdinalScore,
                ]
                .into_iter()
                .collect(),
            },
            model_ref: crate::persistence::ConfigRef { id, edit_seq: 2 },
            connection_ref: crate::persistence::ConfigRef { id, edit_seq: 3 },
            endpoint: None,
            credential_ref: None,
        }
    }

    fn reference() -> RequestRef {
        RequestRef {
            job: "00000002-0000-7000-8000-000000000002"
                .parse()
                .expect("UUID"),
            request: RequestNumber::FIRST,
        }
    }

    fn choice_request() -> DecisionRequest {
        DecisionRequest {
            schema_version: crate::persistence::DECISION_SCHEMA_VERSION,
            state: json!("the turn is growing quickly"),
            question: DecisionQuestion::Choice {
                prompt: "what should happen?".into(),
                options: vec![
                    ChoiceOption {
                        id: "skip".into(),
                        description: "no preparation".into(),
                    },
                    ChoiceOption {
                        id: "prepare".into(),
                        description: "prepare a summary".into(),
                    },
                ],
            },
        }
    }

    #[test]
    fn the_body_uses_named_questions_and_ordered_score_criteria() {
        let body = request_body(&frozen(), &choice_request()).expect("body");
        assert_eq!(body["model"], "jev-latest");
        assert_eq!(body["questions"]["decision"]["type"], "choice");
        assert_eq!(
            body["questions"]["decision"]["criteria"]["prepare"],
            "prepare a summary"
        );
        let score = DecisionRequest {
            schema_version: crate::persistence::DECISION_SCHEMA_VERSION,
            state: json!("x"),
            question: DecisionQuestion::OrdinalScore {
                prompt: "pressure".into(),
                levels: vec![
                    ScoreLevel {
                        value: 10,
                        description: "high".into(),
                    },
                    ScoreLevel {
                        value: 20,
                        description: "higher".into(),
                    },
                ],
            },
        };
        let body = request_body(&frozen(), &score).expect("body");
        assert_eq!(
            body["questions"]["decision"]["criteria"],
            json!(["high", "higher"])
        );
    }

    #[test]
    fn typed_answers_decode_with_provider_semantics() {
        let raw = json!({
            "model": "jev-1.13.0",
            "answers": {"decision": {
                "type": "choice",
                "choice": "prepare",
                "confidence": 0.6,
                "probabilities": {"skip": 0.2, "prepare": 0.8}
            }},
            "usage": {"input_tokens": 120, "output_tokens": 12}
        });
        let (result, usage) =
            decode_response(&frozen(), &choice_request(), reference(), &raw).expect("decoded");
        assert_eq!(usage.input_tokens, Some(120));
        assert_eq!(result.reported_model.as_deref(), Some("jev-1.13.0"));
        assert_eq!(result.model_version, None);
        let DecisionOutcome::Answer {
            answer:
                DecisionAnswer::Choice {
                    selected,
                    confidence,
                    ..
                },
        } = result.outcome
        else {
            panic!("choice");
        };
        assert_eq!(selected, "prepare");
        assert_eq!(
            confidence.expect("confidence").semantics,
            "provider.typesafe.choice.confidence.v1"
        );

        let score = DecisionRequest {
            schema_version: crate::persistence::DECISION_SCHEMA_VERSION,
            state: json!("x"),
            question: DecisionQuestion::OrdinalScore {
                prompt: "pressure".into(),
                levels: vec![
                    ScoreLevel {
                        value: 0,
                        description: "low".into(),
                    },
                    ScoreLevel {
                        value: 10,
                        description: "high".into(),
                    },
                ],
            },
        };
        let raw = json!({
            "model": "jev-latest",
            "answers": {"decision": {
                "type": "score",
                "score": 0.8,
                "confidence": 0.5,
                "legend": {"0": "low", "1": "high"},
                "probabilities": {"0": 0.2, "1": 0.8}
            }},
            "usage": {"input_tokens": 5, "output_tokens": 1}
        });
        let (result, _) = decode_response(&frozen(), &score, reference(), &raw).expect("decoded");
        let DecisionOutcome::Answer {
            answer: DecisionAnswer::OrdinalScore { expectation, .. },
        } = result.outcome
        else {
            panic!("score");
        };
        assert!((expectation - 8.0).abs() < 1e-9);
    }

    #[test]
    fn malformed_answers_are_refused() {
        for raw in [
            json!({"model": "m", "answers": {}, "usage": {"input_tokens": 0, "output_tokens": 0}}),
            json!({"model": "m", "answers": {"decision": {"type": "noul", "noul": 0.5}}}),
            json!({"model": "m", "answers": {"decision": {"type": "choice", "choice": "other", "confidence": 0.5, "probabilities": {}}}}),
            json!({"answers": {"decision": {"type": "choice", "choice": "prepare"}}}),
            json!({"model": "m", "answers": {"decision": {"type": "choice", "choice": "prepare", "confidence": 0.5, "probabilities": {"skip": 0.5, "prepare": 0.4}}}}),
        ] {
            assert!(
                decode_response(&frozen(), &choice_request(), reference(), &raw).is_err(),
                "{raw}"
            );
        }
    }
    #[test]
    fn native_limits_instructions_and_answer_space_are_enforced() {
        let mut config = frozen();
        config.agent.instructions = Some("Prefer deferral when evidence is missing".into());
        let request = choice_request();
        let body = request_body(&config, &request).unwrap();
        assert_eq!(
            body["questions"]["decision"]["instructions"]["profile"],
            config.agent.instructions.as_deref().unwrap()
        );
        config.model.limits.max_input_tokens_per_question = Some(32);
        assert!(request_body(&config, &request).is_err());
        config.model.limits = Default::default();
        let mut too_many = request;
        if let DecisionQuestion::Choice { options, .. } = &mut too_many.question {
            *options = (0..256)
                .map(|id| ChoiceOption {
                    id: format!("option{id}"),
                    description: format!("Choice {id}"),
                })
                .collect();
        }
        assert!(request_body(&config, &too_many).is_err());
        let score = DecisionRequest {
            schema_version: 1,
            state: json!("facts"),
            question: DecisionQuestion::OrdinalScore {
                prompt: "rank".into(),
                levels: (0..11)
                    .map(|value| ScoreLevel {
                        value,
                        description: value.to_string(),
                    })
                    .collect(),
            },
        };
        assert!(request_body(&config, &score).is_err());
        config.agent.workspace_access = crate::persistence::WorkspaceAccess::Full;
        assert!(request_body(&config, &choice_request()).is_err());
    }
    #[test]
    fn extra_distribution_keys_and_over_limit_usage_are_not_accepted() {
        let raw = json!({"model":"jev", "answers":{"decision":{"type":"choice","choice":"prepare","probabilities":{"skip":0.2,"prepare":0.8,"unexpected":0.0}}},"usage":{"input_tokens":12,"output_tokens":4}});
        assert!(decode_response(&frozen(), &choice_request(), reference(), &raw).is_err());
        let mut valid = raw;
        valid["answers"]["decision"]["probabilities"]
            .as_object_mut()
            .unwrap()
            .remove("unexpected");
        let mut config = frozen();
        config.model.limits.max_output_tokens_per_request = Some(3);
        assert!(decode_response(&config, &choice_request(), reference(), &valid).is_err());
    }
    #[test]
    fn credential_echoes_are_redacted_before_normalization_and_persistence() {
        let raw = json!({"model":"reported-test-secret", "answers":{"decision":{"type":"choice","choice":"prepare","probabilities":{"skip":0.2,"prepare":0.8}}},
            "usage":{"input_tokens":12,"output_tokens":4,"provider_note":"test-secret"}});
        let execution = decode_decision(
            &frozen(),
            &choice_request(),
            reference(),
            200,
            serde_json::to_vec(&raw).unwrap(),
            &Redactor::new(Some("test-secret")),
            Duration::ZERO,
        )
        .unwrap();
        assert!(
            !execution
                .result
                .reported_model
                .unwrap()
                .contains("test-secret")
        );
        assert!(
            !execution
                .usage
                .raw
                .unwrap()
                .to_string()
                .contains("test-secret")
        );
        assert!(!execution.raw.to_string().contains("test-secret"));
    }
}
