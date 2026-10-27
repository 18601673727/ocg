use std::process::ExitCode;

fn main() -> ExitCode {
    match ocg::cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("OCG failed: {error}");
            ExitCode::FAILURE
        }
    }
}
