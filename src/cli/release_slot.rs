use crate::db::entities::JobId;
use crate::system::data_directory::DataDirectory;

pub fn run(
    directory: &DataDirectory,
    arguments: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut job_arg = None;

    for arg in arguments {
        if arg == "--yes" {
            // Accepted for compatibility: there is nothing left to confirm.
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
    let report = crate::system::runtime::run_hosted_reporting(
        directory,
        false,
        move |context| async move {
            let database = context.database();
            let outcome: Result<(), Box<dyn std::error::Error>> = async {
                // Classified before any ownership read: a native database holds
                // no legacy slot table, and a native Allocation is never
                // released from here. Nothing below writes.
                use crate::db::redb::MutableJobDetailsLookup;
                match database.mutable_job_details(job, None).await? {
                    MutableJobDetailsLookup::Found(_) | MutableJobDetailsLookup::Inaccessible => {
                        Err(format!(
                            "Job {job} is a native Job: release-slot cannot release a native Allocation; \
                             no Allocation, ownership or capacity was changed"
                        )
                        .into())
                    }
                    MutableJobDetailsLookup::NotMutable => {
                        if database.conversation_exists(job).await? {
                            Err(format!(
                                "{job} is a conversation, not a Job holding a slot; nothing was changed"
                            )
                            .into())
                        } else {
                            Err(format!("Job {job} was not found").into())
                        }
                    }
                }
            }
            .await;
            *returned.borrow_mut() = Some(outcome);
        },
    );

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
