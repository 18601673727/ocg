#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    let code = ocg::cli::run(std::env::args_os().skip(1));
    ExitCode::from(code as u8)
}
