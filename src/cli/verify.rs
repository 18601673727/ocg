/// Offline integrity check of a stopped database. It never opens the original
/// file for writing, starts no host and resolves no Allocation.
pub fn run(arguments: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let (directory, pages) = match arguments.as_slice() {
        [directory] => (directory, false),
        [directory, flag] if flag == "--pages" => (directory, true),
        _ => return Err("usage: ocg verify <data-directory> [--pages]".into()),
    };
    let report = crate::db::verify_offline(std::path::Path::new(directory), pages)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
