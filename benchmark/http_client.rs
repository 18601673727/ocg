use std::time::Duration;

pub(super) async fn client(timeout: Duration) -> Result<ntex::client::Client, String> {
    ntex::client::Client::builder()
        .response_timeout(ntex::time::Millis(
            timeout.as_millis().min(u128::from(u32::MAX)) as u32,
        ))
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|_| "failed to create benchmark HTTP client".to_owned())
}

/// Minimal HTTP client for benchmark control plane requests.
pub(super) struct BenchmarkClient {
    address: String,
    token: String,
    timeout: Duration,
}

impl BenchmarkClient {
    pub(super) fn new(address: String, token: String) -> Self {
        Self {
            address,
            token,
            timeout: Duration::from_secs(60),
        }
    }

    /// Submit a job and return the job ID.
    pub(super) async fn submit_job(
        &self,
        executor: &str,
        llm_profile: &str,
        agent_profile: &str,
        project: &str,
        objective: &str,
    ) -> Result<String, String> {
        let client = client(self.timeout)
            .await
            .map_err(|e| format!("failed to create HTTP client: {}", e))?;

        let body = serde_json::json!({
            "executor": executor,
            "llm_profile": llm_profile,
            "agent_profile": agent_profile,
            "project": project,
            "objective": objective,
        });

        let response = client
            .post(format!("{}/api/v1/jobs", self.address))
            .set_header("Idempotency-Key", uuid::Uuid::now_v7().to_string())
            .set_header("Authorization", format!("Bearer {}", self.token))
            .set_header("Content-Type", "application/json")
            .send_json(&body)
            .await
            .map_err(|e| format!("HTTP POST failed: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("HTTP status {}", response.status()));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| format!("failed to parse response: {}", e))?;

        json.get("data")
            .and_then(|d| d.get("admission"))
            .and_then(|a| a.get("job"))
            .and_then(|j| j.as_str())
            .map(|s| s.to_owned())
            .ok_or_else(|| "missing job ID in response".to_owned())
    }

    /// Poll job status via the events endpoint until settled.
    pub(super) async fn wait_for_settlement(
        &self,
        job_id: &str,
        timeout: Duration,
    ) -> Result<(), String> {
        let deadline = std::time::Instant::now() + timeout;

        loop {
            if std::time::Instant::now() > deadline {
                return Err("settlement timeout exceeded".to_owned());
            }

            let client = client(Duration::from_secs(10))
                .await
                .map_err(|e| format!("failed to create HTTP client: {}", e))?;

            let response = client
                .get(format!("{}/api/v1/jobs/{}/events", self.address, job_id))
                .set_header("Authorization", format!("Bearer {}", self.token))
                .set_header("Accept", "text/event-stream")
                .send()
                .await
                .map_err(|e| format!("HTTP GET failed: {}", e))?;

            if !response.status().is_success() {
                return Err(format!("HTTP status {}", response.status()));
            }

            // Simple SSE parsing: look for settlement event
            let body = response
                .body()
                .await
                .map_err(|e| format!("failed to read response body: {}", e))?;

            let text = String::from_utf8_lossy(&body);
            if text.contains("event: settlement") {
                return Ok(());
            }

            // Poll again after a short delay
            ntex::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
