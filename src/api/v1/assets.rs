use std::path::{Path, PathBuf};

use ntex::http::StatusCode;
use ntex::http::header;
use ntex::web::HttpResponse;

use super::{NOT_FOUND, Refusal};

const MAX_ASSET_BYTES: u64 = 16 * 1024 * 1024;

pub(super) struct UiAssets {
    root: PathBuf,
}

pub(super) struct Asset {
    pub(super) body: Vec<u8>,
    pub(super) content_type: &'static str,
    pub(super) immutable: bool,
}

impl UiAssets {
    pub(super) fn open(directory: &Path) -> Result<Self, std::io::Error> {
        let root = std::fs::canonicalize(directory)?;
        if !root.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                "the frontend directory is not a directory",
            ));
        }
        Ok(Self { root })
    }

    pub(super) async fn serve(&self, path: &str) -> Result<Asset, Refusal> {
        let root = self.root.clone();
        let path = path.to_owned();
        ntex::rt::spawn_blocking(move || serve_file(&root, &path))
            .await
            .unwrap_or(Err(NOT_FOUND))
    }
}

fn serve_file(root: &std::path::Path, path: &str) -> Result<Asset, Refusal> {
    let Some(relative) = resolved(path) else {
        return Err(NOT_FOUND);
    };
    let (file, immutable) = file_under(root, &relative)?;
    let bytes = std::fs::read(&file).map_err(|_| NOT_FOUND)?;
    let content_type = content_type(&file).ok_or(NOT_FOUND)?;
    Ok(Asset {
        body: bytes,
        content_type,
        immutable,
    })
}

fn file_under(root: &std::path::Path, relative: &str) -> Result<(PathBuf, bool), Refusal> {
    for candidate in candidates(relative) {
        let path = root.join(&candidate);
        let Ok(resolved) = std::fs::canonicalize(&path) else {
            continue;
        };
        if !resolved.starts_with(root) {
            return Err(NOT_FOUND);
        }
        let Ok(metadata) = std::fs::metadata(&resolved) else {
            continue;
        };
        if metadata.is_file() && metadata.len() <= MAX_ASSET_BYTES {
            return Ok((resolved, candidate.starts_with("_next/static/")));
        }
    }
    Err(NOT_FOUND)
}

fn candidates(path: &str) -> Vec<String> {
    if path.is_empty() {
        return vec!["index.html".to_owned()];
    }
    let mut names = vec![path.to_owned()];
    if !path
        .rsplit('/')
        .next()
        .is_some_and(|name| name.contains('.'))
    {
        names.push(format!("{path}.html"));
        names.push(format!("{path}/index.html"));
        names.push("index.html".to_owned());
    }
    names
}

fn resolved(path: &str) -> Option<String> {
    let rest = path.strip_prefix('/')?.trim_end_matches('/');
    if rest.is_empty() {
        return Some(String::new());
    }
    if rest.contains('%') || rest.contains('\\') {
        return None;
    }
    let segments = rest.split('/').collect::<Vec<_>>();
    if segments.first() == Some(&"jobs")
        && segments
            .get(1)
            .and_then(|id| crate::db::entities::parse_entity_id(id))
            .is_some()
    {
        if segments.len() % 2 != 0
            || segments.len() > 2 * (usize::from(crate::db::entities::MAX_JOB_DEPTH) + 1)
        {
            return None;
        }
        let mut seen = std::collections::HashSet::new();
        return segments
            .as_chunks::<2>()
            .0
            .iter()
            .all(|pair| {
                pair[0] == "jobs" && super::routing::job_id(pair[1]).is_ok() && seen.insert(pair[1])
            })
            .then(String::new);
    }
    if !segments.iter().all(|segment| named(segment)) {
        return None;
    }
    Some(segments.join("/"))
}

fn named(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+@".contains(&byte))
}

fn content_type(file: &Path) -> Option<&'static str> {
    Some(match file.extension()?.to_str()? {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "webmanifest" => "application/manifest+json",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "wasm" => "application/wasm",
        _ => return None,
    })
}

pub(super) fn response(asset: Asset) -> HttpResponse {
    HttpResponse::build(StatusCode::OK)
        .header(header::CONTENT_TYPE, asset.content_type)
        .header(
            header::CACHE_CONTROL,
            if asset.immutable {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            },
        )
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .body(asset.body)
}
