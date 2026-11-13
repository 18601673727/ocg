use std::cell::RefCell;
use std::rc::Rc;

use crate::db::entities::{ScheduledExternalPolicyId, ScheduledExternalPolicyRecord, Timestamp};
use crate::system::data_directory::DataDirectory;
use crate::system::execution_controls::ExecutionControl;
use crate::system::scheduled_external::ScheduledExternalPolicyDraft;

pub(super) const COMMAND: &str = "scheduled-external-policy";

enum Action {
    Create {
        draft: ScheduledExternalPolicyDraft,
    },
    List {
        after: Option<ScheduledExternalPolicyId>,
        limit: usize,
    },
    Show {
        id: ScheduledExternalPolicyId,
    },
    Update {
        id: ScheduledExternalPolicyId,
        revision: u32,
        draft: ScheduledExternalPolicyDraft,
    },
    SetEnabled {
        id: ScheduledExternalPolicyId,
        revision: u32,
        enabled: bool,
    },
    Revoke {
        id: ScheduledExternalPolicyId,
        revision: u32,
    },
}

pub(super) fn run(
    directory: &DataDirectory,
    arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = parse(arguments)?;
    let result = Rc::new(RefCell::new(None));
    let returned = result.clone();
    let report =
        crate::system::runtime::run_hosted_reporting(directory, false, move |context| async move {
            let database = context.database();
            let outcome: Result<(), Box<dyn std::error::Error>> = async {
                match action {
                    Action::Create { draft } => {
                        let record = database.create_scheduled_external_policy(draft).await?;
                        print_record(&record)?;
                    }
                    Action::List { after, limit } => {
                        let page = database.scheduled_external_policies(after, limit).await?;
                        let records: Vec<serde_json::Value> =
                            page.records.iter().map(policy_json).collect();
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "records": records,
                                "next_after": page.next.map(|id| id.to_string()),
                                "transport_control": control_json(),
                            }))?
                        );
                    }
                    Action::Show { id } => match database.scheduled_external_policy(id).await? {
                        Some(record) => print_record(&record)?,
                        None => {
                            return Err(format!("scheduled external policy {id} not found").into());
                        }
                    },
                    Action::Update {
                        id,
                        revision,
                        draft,
                    } => {
                        let record = database
                            .update_scheduled_external_policy(id, revision, draft)
                            .await?;
                        print_record(&record)?;
                    }
                    Action::SetEnabled {
                        id,
                        revision,
                        enabled,
                    } => {
                        let record = database
                            .set_scheduled_external_policy_enabled(id, revision, enabled)
                            .await?;
                        print_record(&record)?;
                    }
                    Action::Revoke { id, revision } => {
                        let record = database
                            .revoke_scheduled_external_policy(id, revision)
                            .await?;
                        print_record(&record)?;
                    }
                }
                Ok(())
            }
            .await;
            *returned.borrow_mut() = Some(outcome);
        });
    for failure in report.failures {
        if failure.phase != crate::system::runtime::Phase::Logger {
            return Err(failure.error.into());
        }
    }
    result
        .borrow_mut()
        .take()
        .ok_or("host stopped before the policy command completed")??;
    Ok(())
}

fn parse(arguments: Vec<String>) -> Result<Action, Box<dyn std::error::Error>> {
    let mut arguments = arguments.into_iter();
    let command = arguments.next().ok_or_else(usage_error)?;
    match command.as_str() {
        "create" => Ok(Action::Create {
            draft: parse_draft(&mut arguments)?,
        }),
        "list" => {
            let mut after = None;
            let mut limit = 100usize;
            while let Some(flag) = arguments.next() {
                match flag.as_str() {
                    "--after" => after = Some(parse_id(next_value(&mut arguments, "--after")?)?),
                    "--limit" => limit = parse_limit(next_value(&mut arguments, "--limit")?)?,
                    _ => return Err(other_flag(&flag).into()),
                }
            }
            Ok(Action::List { after, limit })
        }
        "show" => Ok(Action::Show {
            id: parse_id(next_value(&mut arguments, "policy id")?)?,
        }),
        "update" => {
            let id = parse_id(next_value(&mut arguments, "policy id")?)?;
            let mut revision = None;
            let mut fields = PoliciesFields::default();
            while let Some(flag) = arguments.next() {
                match flag.as_str() {
                    "--revision" => {
                        set_once(
                            &mut revision,
                            parse_revision(next_value(&mut arguments, "--revision")?)?,
                            "--revision",
                        )?;
                    }
                    _ => fields.set(&flag, next_value(&mut arguments, &flag)?)?,
                }
            }
            let draft = fields.draft()?;
            let revision = revision.ok_or_else(|| missing("--revision"))?;
            Ok(Action::Update {
                id,
                revision,
                draft,
            })
        }
        "enable" | "disable" => {
            let id = parse_id(next_value(&mut arguments, "policy id")?)?;
            let mut revision = None;
            while let Some(flag) = arguments.next() {
                match flag.as_str() {
                    "--revision" => {
                        set_once(
                            &mut revision,
                            parse_revision(next_value(&mut arguments, "--revision")?)?,
                            "--revision",
                        )?;
                    }
                    _ => return Err(other_flag(&flag).into()),
                }
            }
            let revision = revision.ok_or_else(|| missing("--revision"))?;
            Ok(Action::SetEnabled {
                id,
                revision,
                enabled: command == "enable",
            })
        }
        "revoke" => {
            let id = parse_id(next_value(&mut arguments, "policy id")?)?;
            let mut revision = None;
            while let Some(flag) = arguments.next() {
                match flag.as_str() {
                    "--revision" => {
                        set_once(
                            &mut revision,
                            parse_revision(next_value(&mut arguments, "--revision")?)?,
                            "--revision",
                        )?;
                    }
                    _ => return Err(other_flag(&flag).into()),
                }
            }
            let revision = revision.ok_or_else(|| missing("--revision"))?;
            Ok(Action::Revoke { id, revision })
        }
        other => Err(format!(
            "unknown scheduled-external-policy command `{other}`; see `ocg help`"
        )
        .into()),
    }
}

#[derive(Default)]
struct PoliciesFields {
    endpoint: Option<String>,
    credential_ref: Option<String>,
    model: Option<String>,
    proxy: Option<String>,
    max_request_bytes: Option<String>,
    max_output_tokens: Option<String>,
    timeout_secs: Option<String>,
}

impl PoliciesFields {
    fn set(&mut self, flag: &str, value: String) -> Result<(), Box<dyn std::error::Error>> {
        let slot = match flag {
            "--endpoint" => &mut self.endpoint,
            "--credential-ref" => &mut self.credential_ref,
            "--model" => &mut self.model,
            "--trusted-http-proxy" => &mut self.proxy,
            "--max-request-bytes" => &mut self.max_request_bytes,
            "--max-output-tokens" => &mut self.max_output_tokens,
            "--timeout-secs" => &mut self.timeout_secs,
            other => return Err(other_flag(other).into()),
        };
        if slot.replace(value).is_some() {
            return Err(format!("{flag} was given twice").into());
        }
        Ok(())
    }

    fn draft(self) -> Result<ScheduledExternalPolicyDraft, Box<dyn std::error::Error>> {
        let endpoint = self.endpoint.ok_or_else(|| missing("--endpoint"))?;
        let credential_ref = self
            .credential_ref
            .ok_or_else(|| missing("--credential-ref"))?;
        let model = self.model.ok_or_else(|| missing("--model"))?;
        let proxy = self.proxy.ok_or_else(|| missing("--trusted-http-proxy"))?;
        let max_request_bytes = parse_u32(
            self.max_request_bytes
                .ok_or_else(|| missing("--max-request-bytes"))?,
            "--max-request-bytes",
        )?;
        let max_output_tokens = parse_u32(
            self.max_output_tokens
                .ok_or_else(|| missing("--max-output-tokens"))?,
            "--max-output-tokens",
        )?;
        let timeout_secs = parse_u32(
            self.timeout_secs.ok_or_else(|| missing("--timeout-secs"))?,
            "--timeout-secs",
        )?;
        ScheduledExternalPolicyDraft::new(
            &endpoint,
            &credential_ref,
            &model,
            &proxy,
            max_request_bytes,
            max_output_tokens,
            timeout_secs,
        )
        .map_err(|refusal| format!("{}: {}", refusal.code, refusal.detail).into())
    }
}

fn parse_draft(
    arguments: &mut std::vec::IntoIter<String>,
) -> Result<ScheduledExternalPolicyDraft, Box<dyn std::error::Error>> {
    let mut fields = PoliciesFields::default();
    while let Some(flag) = arguments.next() {
        fields.set(&flag, next_value(arguments, &flag)?)?;
    }
    fields.draft()
}

fn next_value(
    arguments: &mut std::vec::IntoIter<String>,
    flag: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    arguments
        .next()
        .ok_or_else(|| missing(&format!("a value for {flag}")))
}

fn missing(what: &str) -> Box<dyn std::error::Error> {
    format!("missing {what}; see `ocg help`").into()
}

fn other_flag(flag: &str) -> String {
    format!("unexpected argument `{flag}`; see `ocg help`")
}

fn usage_error() -> Box<dyn std::error::Error> {
    "usage: ocg scheduled-external-policy <create|list|show|update|enable|disable|revoke> ...; see `ocg help`"
        .into()
}

fn set_once<T: Copy>(
    slot: &mut Option<T>,
    value: T,
    flag: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if slot.replace(value).is_some() {
        return Err(format!("{flag} was given twice").into());
    }
    Ok(())
}

fn parse_id(value: String) -> Result<ScheduledExternalPolicyId, Box<dyn std::error::Error>> {
    value
        .parse()
        .map_err(|_| "policy id must be a canonical UUID".into())
}

fn parse_revision(value: String) -> Result<u32, Box<dyn std::error::Error>> {
    parse_u32(value, "--revision")
}

fn parse_limit(value: String) -> Result<usize, Box<dyn std::error::Error>> {
    let limit: usize = value
        .parse()
        .map_err(|_| "--limit must be a positive integer")?;
    if limit == 0 {
        return Err("--limit must be a positive integer".into());
    }
    Ok(limit)
}

fn parse_u32(value: String, flag: &str) -> Result<u32, Box<dyn std::error::Error>> {
    value
        .parse::<u32>()
        .map_err(|_| format!("{flag} must be a non-negative 32-bit integer").into())
        .and_then(|number| {
            if number == 0
                && matches!(
                    flag,
                    "--timeout-secs" | "--max-request-bytes" | "--max-output-tokens"
                )
            {
                Err(format!("{flag} must be positive").into())
            } else {
                Ok(number)
            }
        })
}

fn print_record(record: &ScheduledExternalPolicyRecord) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(&policy_json(record))?);
    Ok(())
}

fn policy_json(record: &ScheduledExternalPolicyRecord) -> serde_json::Value {
    serde_json::json!({
        "id": record.id.to_string(),
        "endpoint": record.endpoint,
        "credential_ref": record.credential_ref,
        "model": record.model,
        "proxy": record.proxy,
        "max_request_bytes": record.max_request_bytes,
        "max_output_tokens": record.max_output_tokens,
        "timeout_secs": record.timeout_secs,
        "enabled": record.enabled,
        "revoked": record.revoked,
        "live": record.is_live(),
        "revision": record.revision,
        "created_at_micros": micros(record.created_at),
        "updated_at_micros": micros(record.updated_at),
        "transport_control": control_json(),
    })
}

fn control_json() -> serde_json::Value {
    serde_json::json!({
        "name": ExecutionControl::ScheduledExternalTransport.name(),
        "enabled": ExecutionControl::ScheduledExternalTransport.is_enabled(),
        "note": "a stored policy never enables the control; every dispatch needs both",
    })
}

fn micros(timestamp: Timestamp) -> i64 {
    timestamp.as_unix_micros()
}
