pub(crate) mod command;
pub(crate) mod files;
pub(crate) mod search;

fn failure(error: crate::workspace::ExecutionFailure) -> super::registry::ToolFailure {
    match error {
        crate::workspace::ExecutionFailure::Refused(message) => {
            super::registry::ToolFailure::not_applied(message)
        }
        crate::workspace::ExecutionFailure::Uncertain(message) => {
            super::registry::ToolFailure::uncertain(message)
        }
    }
}
