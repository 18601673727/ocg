use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct FixtureConfig {
    pub file_count: usize,
    pub avg_file_size: usize,
    pub probe_file: String,
}

impl FixtureConfig {
    pub fn small() -> Self {
        Self {
            file_count: 10,
            avg_file_size: 1_024,
            probe_file: "probe.txt".into(),
        }
    }

    pub fn medium() -> Self {
        Self {
            file_count: 200,
            avg_file_size: 4_096,
            probe_file: "probe.txt".into(),
        }
    }

    pub fn large() -> Self {
        Self {
            file_count: 2_000,
            avg_file_size: 8_192,
            probe_file: "probe.txt".into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FixtureStats {
    pub file_count: usize,
    pub total_file_bytes: u64,
    pub probe_file: String,
    pub probe_file_bytes: u64,
    pub generation_time_secs: f64,
}

pub struct FixtureGenerator {
    config: FixtureConfig,
    root: PathBuf,
}

impl FixtureGenerator {
    pub fn new(config: FixtureConfig, root: PathBuf) -> Self {
        Self { config, root }
    }

    pub fn generate(&self) -> Result<FixtureStats, Box<dyn std::error::Error + Send + Sync>> {
        let started = std::time::Instant::now();
        let workspace = self.workspace_path();
        std::fs::create_dir_all(&workspace)?;

        let mut total_bytes = 0u64;
        let mut probe_bytes = 0u64;
        for index in 0..self.config.file_count {
            let content = file_content(index, self.config.avg_file_size);
            let name = if index == 0 {
                self.config.probe_file.clone()
            } else {
                format!("file_{index:05}.txt")
            };
            let path = workspace.join(&name);
            std::fs::write(&path, &content)?;
            total_bytes += content.len() as u64;
            if index == 0 {
                probe_bytes = content.len() as u64;
            }
        }

        Ok(FixtureStats {
            file_count: self.config.file_count,
            total_file_bytes: total_bytes,
            probe_file: self.config.probe_file.clone(),
            probe_file_bytes: probe_bytes,
            generation_time_secs: started.elapsed().as_secs_f64(),
        })
    }

    pub fn workspace_path(&self) -> PathBuf {
        self.root.join("workspace")
    }
}

fn file_content(index: usize, size: usize) -> String {
    let mut content = String::with_capacity(size + 128);
    content.push_str("beacon rarebeacon fixture file\n");
    let mut written = content.len();
    let mut line = 0usize;
    while written < size {
        let text = format!(
            "fn unit{index:05x}_line{line:04}(state: &mut State) -> Result<u64, Error> {{ let packet = state.apply(value)?; Ok(packet) }}\n"
        );
        content.push_str(&text);
        written += text.len();
        line += 1;
    }
    content
}

pub fn measure_fixture_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    let mut directories = vec![path.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
}

pub fn count_fixture_files(path: &Path) -> std::io::Result<usize> {
    let mut count = 0;
    let mut directories = vec![path.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() {
                count += 1;
            }
        }
    }
    Ok(count)
}
