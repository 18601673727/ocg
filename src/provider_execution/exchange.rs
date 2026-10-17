use super::*;

/// Sends one validated conversation, with the tools it offers, to the
/// frozen Provider and model with the frozen parameters, and reads the
/// streamed response. The conversation is the whole message list sent.
/// Tool calls in the response come back typed in the result once they are
/// complete and allowed; anything else about them is an incomplete stream.
/// No tool is ever run here.
pub async fn execute_conversation(
    frozen: &FrozenExecutionConfig,
    conversation: &ChatConversation,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    mut sink: Option<&mut dyn ProviderOutputSink>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut announced = false;
    let result = exchange(
        frozen,
        conversation,
        resolver,
        config,
        &mut sink,
        &mut announced,
    )
    .await;
    // An error before any response head is classified by what it proves about
    // delivery; a response head has already announced the request.
    if let Err(error) = &result
        && !announced
    {
        announce(borrow_output_sink(&mut sink), unannounced_fact(error)).await;
    }
    result
}

fn borrow_output_sink<'a>(
    sink: &'a mut Option<&mut dyn ProviderOutputSink>,
) -> Option<&'a mut dyn ProviderOutputSink> {
    match sink {
        Some(sink) => Some(&mut **sink),
        None => None,
    }
}

async fn exchange(
    frozen: &FrozenExecutionConfig,
    conversation: &ChatConversation,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    sink: &mut Option<&mut dyn ProviderOutputSink>,
    announced: &mut bool,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    conversation
        .validate()
        .map_err(|error| ProviderExecutionError::Build {
            message: error.to_string(),
        })?;
    let provider = &frozen.llm.provider;
    let endpoint = provider.endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT);
    let endpoint = endpoint.trim_end_matches('/');
    let endpoint = if endpoint.ends_with("/chat/completions") {
        endpoint.to_owned()
    } else {
        format!("{endpoint}/chat/completions")
    };
    if !endpoint.starts_with("https://") {
        return Err(ProviderExecutionError::Build {
            message: "provider endpoint must use https".to_owned(),
        });
    }

    let credential = resolver.resolve(provider.credential_ref.as_deref())?;
    let authorization = credential.as_deref().map(authorization).transpose()?;
    if config.cancellation.is_cancelled() {
        return Err(ProviderExecutionError::NotDelivered {
            message: "provider execution was cancelled before sending".to_owned(),
        });
    }
    let body = request_body(
        &frozen.llm.config.model,
        &frozen.llm.config.parameters,
        conversation,
    )?;
    let client = Client::builder()
        .disable_redirects()
        .response_timeout(Millis(
            config.timeout.as_millis().min(u128::from(u32::MAX)) as u32
        ))
        // The pinned client's payload timeout covers the whole body, not an
        // idle interval. Streaming applies its own timeout to each chunk wait.
        .response_payload_timeout(Millis::ZERO)
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|error| ProviderExecutionError::Build {
            message: error.to_string(),
        })?;

    let request = client
        .post(endpoint)
        .set_header(CONTENT_TYPE, "application/json")
        .set_header(ACCEPT, "text/event-stream")
        .set_header("Cache-Control", "no-cache")
        .if_some(authorization, |authorization, request| {
            request.set_header(AUTHORIZATION, authorization)
        });

    let started = Instant::now();
    let sent = select(
        config.cancellation.cancelled(),
        ntex::time::timeout(config.timeout, request.send_json(&body)),
    )
    .await;
    let response = match sent {
        // The request future is dropped here, with whatever it had sent.
        Either::Left(()) => {
            return Err(ProviderExecutionError::Interrupted {
                message: "provider execution was cancelled before a response arrived".to_owned(),
            });
        }
        Either::Right(sent) => sent,
    }
    .map_err(|_| ProviderExecutionError::Unknown {
        message: "provider request timed out after it may have been delivered".to_owned(),
        facts: None,
        finished: false,
    })?
    .map_err(classify_client_error)?;
    // A response head proves the request reached a Provider, whatever it says.
    *announced = true;
    announce(borrow_output_sink(sink), ProviderFact::Delivered).await;
    let status = response.status().as_u16();
    let request_id = response
        .header("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if response.status() != StatusCode::OK {
        // Nothing of a refused image-bearing request's body is kept, so it is
        // not read; see `rejected_failure`.
        if conversation.carries_images() {
            return Err(ProviderExecutionError::Rejected {
                failure: rejected_failure(conversation, status, request_id, None),
            });
        }
        // A cancelled read keeps the status the Provider answered with.
        let body = match select(
            config.cancellation.cancelled(),
            ntex::time::timeout(
                config.timeout,
                bounded_body(response, MAX_PROVIDER_ERROR_BYTES),
            ),
        )
        .await
        {
            Either::Left(()) => None,
            Either::Right(Ok(body)) => body,
            Either::Right(Err(())) => None,
        };
        return Err(ProviderExecutionError::Rejected {
            failure: rejected_failure(conversation, status, request_id, body),
        });
    }

    let payload = response.take_payload();
    read_response(
        payload,
        status,
        started,
        request_id,
        config,
        borrow_output_sink(sink),
        conversation,
    )
    .await
}

pub(super) fn incomplete_tool_calls(
    error: InvalidToolCall,
    facts: ResponseFacts,
) -> ProviderExecutionError {
    ProviderExecutionError::Incomplete {
        message: error.to_string(),
        facts,
    }
}

pub async fn execute_chat_with_sink(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    sink: &mut dyn ProviderOutputSink,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let conversation = objective_conversation(frozen, objective);
    execute_conversation(frozen, &conversation, resolver, config, Some(sink)).await
}

/// The Authorization header of a request. It is marked sensitive, so no
/// dump of the request's headers, such as the HTTP client's own request
/// tracing, prints the credential. A credential that is no valid header
/// value is refused without being named.
pub(super) fn authorization(credential: &str) -> Result<HeaderValue, ProviderExecutionError> {
    let mut value = HeaderValue::try_from(format!("Bearer {credential}")).map_err(|_| {
        ProviderExecutionError::Build {
            message: "the resolved credential is not a valid HTTP header value".to_owned(),
        }
    })?;
    value.set_sensitive(true);
    Ok(value)
}
