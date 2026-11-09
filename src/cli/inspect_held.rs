/// Read-only report of the Pool Allocations a stopped database still holds. It takes the data directory explicitly, so it never
/// creates or changes anything there, and it does not open the database for
/// writing, start a host, or resolve any Allocation.
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
