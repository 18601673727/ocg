use crate::db::protection::{ProtectionLock, ProtectionWatch};

pub(crate) async fn report_protection(watch: ProtectionWatch) {
    let mut lock = watch.status().lock;
    if let Some(notice) = protection_notice(None, lock.as_ref()) {
        eprintln!("{notice}");
    }
    while let Some(status) = watch.changed().await {
        if let Some(notice) = protection_notice(lock.as_ref(), status.lock.as_ref()) {
            eprintln!("{notice}");
        }
        lock = status.lock;
    }
}

fn protection_notice(
    before: Option<&ProtectionLock>,
    after: Option<&ProtectionLock>,
) -> Option<String> {
    match (before, after) {
        (None, Some(lock)) => Some(format!(
            "OCG storage protection is active; commands are refused and the file log is paused: {lock}"
        )),
        (Some(_), None) => {
            Some("OCG storage protection was lifted; commands are accepted again".to_owned())
        }
        _ => None,
    }
}
