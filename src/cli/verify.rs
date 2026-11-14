pub fn run(arguments: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let (directory, pages) = match arguments.as_slice() {
        [directory] => (directory, false),
        [directory, flag] if flag == "--pages" => (directory, true),
        _ => return Err("usage: ocg verify <data-directory> [--pages]".into()),
    };
    let verification = crate::db::verify_offline(std::path::Path::new(directory), pages)?;
    println!("{}", serde_json::to_string_pretty(&verification.report)?);
    if !verification.passed {
        return Err("offline verification failed".into());
    }
    Ok(())
}
