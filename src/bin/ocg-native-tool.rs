#![forbid(unsafe_code)]

fn main() -> std::process::ExitCode {
    match ocg::remote_execution::run_native_tool_helper() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(_) => {
            eprintln!("confined native tool failed");
            std::process::ExitCode::FAILURE
        }
    }
}
