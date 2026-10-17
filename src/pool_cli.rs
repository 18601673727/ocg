use std::num::NonZeroU32;

use ocg::data_directory::DataDirectory;
use ocg::persistence::{ExecutionPoolId, ProfileId};

pub fn run(
    directory: &DataDirectory,
    arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    enum Action {
        List,
        Create(String, Option<NonZeroU32>),
        Enable(ExecutionPoolId, bool),
        Profile(ExecutionPoolId, ProfileId),
        Limit(ExecutionPoolId, NonZeroU32),
    }
    let pool_id = |value: &str| -> Result<ExecutionPoolId, Box<dyn std::error::Error>> {
        Ok(ExecutionPoolId::new(
            value
                .strip_prefix("execution_pool:")
                .unwrap_or(value)
                .parse()?,
        ))
    };
    let action = match arguments.as_slice() {
        [command] if command == "list" => Action::List,
        [command, name] if command == "create" => Action::Create(name.clone(), None),
        [command, name, concurrency] if command == "create" =>
            Action::Create(name.clone(), Some(concurrency.parse()?)),
        [command, pool] if command == "enable" || command == "disable" =>
            Action::Enable(pool_id(pool)?, command == "enable"),
        [command, pool, limit] if command == "set-limit" => Action::Limit(pool_id(pool)?, limit.parse()?),
        [command, pool, profile] if command == "set-profile" =>
            Action::Profile(pool_id(pool)?, ProfileId::new(profile.strip_prefix("profile:").unwrap_or(profile).parse()?)),
        _ => return Err("usage: ocg pool list | create <name> [nonzero-concurrency] | enable <pool> | disable <pool> | set-profile <pool> <profile> | set-limit <pool> <nonzero-concurrency>".into()),
    };
    let result = std::rc::Rc::new(std::cell::RefCell::new(None));
    let returned = result.clone();
    ocg::host::run_hosted(directory, false, move |context| async move {
        let database = context.database();
        let outcome: Result<Vec<String>, ocg::database::DatabaseError> = async {
            match action {
                Action::List => {
                    let mut before = None;
                    let mut lines = Vec::new();
                    loop {
                        let page = database.execution_pools(before, 1_000).await?;
                        for pool in &page {
                            lines.push(format!(
                                "{}\t{}\t{}\t{}",
                                pool.id,
                                pool.name,
                                pool.profile,
                                if pool.enabled { "enabled" } else { "disabled" }
                            ));
                        }
                        if page.len() < 1_000 {
                            break;
                        }
                        before = page.last().map(|pool| pool.id);
                    }
                    Ok(lines)
                }
                Action::Create(name, concurrency) => {
                    use ocg::persistence::*;
                    let profile = database
                        .create_profile(NewProfile {
                            name: name.clone(),
                            config: NewProfileConfig::ExecutionPool(
                                crate::llm_target::pool_config(
                                    LlmTransport::OpenAiChatCompletions,
                                    concurrency,
                                ),
                            ),
                        })
                        .await?;
                    let pool = database
                        .create_execution_pool(NewExecutionPool {
                            name,
                            profile,
                            enabled: true,
                        })
                        .await?;
                    Ok(vec![pool.to_string()])
                }
                Action::Limit(pool, concurrency_limit) => {
                    use ocg::persistence::*;
                    let record = database.execution_pool(pool).await?.ok_or(
                        ocg::database::DatabaseError::Rejected(Violation::NotFound(
                            RecordRef::ExecutionPool(pool),
                        )),
                    )?;
                    let profile = database.profile(record.profile).await?.ok_or(
                        ocg::database::DatabaseError::Rejected(Violation::NotFound(
                            RecordRef::Profile(record.profile),
                        )),
                    )?;
                    let mut typed: ExecutionPoolProfile = profile.try_into().map_err(|error| {
                        ocg::database::DatabaseError::Rejected(Violation::WrongProfileKind(error))
                    })?;
                    typed.config.concurrency_limit = concurrency_limit;
                    let replacement = database
                        .create_profile(NewProfile {
                            name: typed.name,
                            config: NewProfileConfig::ExecutionPool(typed.config),
                        })
                        .await?;
                    database
                        .set_execution_pool_profile(pool, replacement)
                        .await?;
                    Ok(vec![pool.to_string()])
                }
                Action::Enable(pool, enabled) => {
                    database.set_execution_pool_enabled(pool, enabled).await?;
                    Ok(vec![pool.to_string()])
                }
                Action::Profile(pool, profile) => {
                    database.set_execution_pool_profile(pool, profile).await?;
                    Ok(vec![pool.to_string()])
                }
            }
        }
        .await;
        *returned.borrow_mut() = Some(outcome);
    })?;
    let lines = result
        .borrow_mut()
        .take()
        .ok_or("host stopped before Pool command completed")??;
    for line in lines {
        println!("{line}");
    }
    Ok(())
}
