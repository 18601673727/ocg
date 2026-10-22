use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let check = match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => false,
        [argument] if argument == "--check" => true,
        _ => return Err("usage: ocg-rs-ts [--check]".into()),
    };
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("frontend/components/ocg/contracts/generated.ts");
    let generated = ocg::control_plane::contracts::typescript();
    if check {
        if std::fs::read_to_string(&path)? != generated {
            return Err("Control Plane contracts have drifted; run make contracts".into());
        }
    } else {
        std::fs::write(path, generated)?;
    }
    Ok(())
}
