pub fn run(arguments: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    {
        let [directory] = arguments.as_slice() else {
            return Err("usage: ocg inspect-held <data-directory>".into());
        };
        let report = crate::db::inspect_held_allocations_offline(std::path::Path::new(directory))?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }
}
