use std::io::Write;

use crate::db::entities::JobId;
use crate::system::data_directory::DataDirectory;

pub fn run(
    directory: &DataDirectory,
    arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut job_arg = None;
    let mut yes = false;

    for arg in arguments {
        if arg == "--yes" {
            yes = true;
        } else if job_arg.is_none() {
            job_arg = Some(arg);
        } else {
            return Err("usage: ocg release-slot <job> [--yes]".into());
        }
    }

    let job_str = job_arg.ok_or("usage: ocg release-slot <job> [--yes]")?;
    let job: JobId = job_str
        .parse()
        .map_err(|_| "job must be a canonical UUID")?;

    let result = std::rc::Rc::new(std::cell::RefCell::new(None));
    let returned = result.clone();
    let report =
        crate::system::runtime::run_hosted_reporting(directory, false, move |context| async move {
            let database = context.database();
            let outcome: Result<(), Box<dyn std::error::Error>> = async {
                let quarantined = database.unresolved_executions().await?;
                if !quarantined.iter().any(|(j, _)| *j == job) {
                    return Err(format!("Job {job} is not quarantined").into());
                }
                println!("{job}");
                if !yes {
                    print!("Confirm that all processes for {job} have terminated [y/N]: ");
                    std::io::stdout().flush()?;
                    let mut response = String::new();
                    std::io::stdin().read_line(&mut response)?;
                    let trimmed = response.trim();
                    if !trimmed.eq_ignore_ascii_case("y") && !trimmed.eq_ignore_ascii_case("yes") {
                        return Err("aborted by operator".into());
                    }
                }
                database
                    .confirm_job_stopped(job, crate::db::entities::StopProof::Operator)
                    .await?;
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
        .ok_or("host stopped before release-slot completed")??;
    Ok(())
}
