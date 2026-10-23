//! The exported frontend, served as files from the directory the operator
//! named in [`super::UI_VARIABLE`].
//!
//! This is not a filesystem server: it serves only regular files under
//! that one directory, by a plain path that names no other, whose type it
//! knows. A path that escapes with `..`, a percent-encoding or a symlink
//! pointing outside the directory is answered as missing, and anything
//! without a known type is never sent. The API's own paths under `/api/`
//! never reach it.
//!
//! Files under `_next/static/` are content-hashed by the build and served
//! immutable; every other file is revalidated on each use, so a rebuilt
//! frontend is picked up without restarting the listener.

use std::path::{Path, PathBuf};

use ntex::http::StatusCode;
use ntex::http::header;
use ntex::web::HttpResponse;

use super::{NOT_FOUND, Refusal};

/// The largest asset served, in bytes; larger ones are answered missing.
const MAX_ASSET_BYTES: u64 = 16 * 1024 * 1024;

/// The directory of the exported frontend, resolved once.
pub(super) struct UiAssets {
    /// The directory as it is on disk, with every symlink resolved.
    root: PathBuf,
}

/// One file to send: its bytes, its media type, and whether the build
/// content-hashed it.
pub(super) struct Asset {
    pub(super) body: Vec<u8>,
    pub(super) content_type: &'static str,
    pub(super) immutable: bool,
}

impl UiAssets {
    /// Resolves `directory` as the root every served file must stay under.
    ///
    /// # Errors
    ///
    /// Returns why `directory` cannot serve as the frontend's root.
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

    /// The file `path` names within the root, or why it is refused. The
    /// path is taken literally: it may not name a parent, carry an
    /// encoding or resolve, through a symlink, outside the root. The
    /// canonicalization, metadata and read run on a blocking worker, so a
    /// slow filesystem never holds the async worker answering the request.
    pub(super) async fn serve(&self, path: &str) -> Result<Asset, Refusal> {
        let root = self.root.clone();
        let path = path.to_owned();
        ntex::rt::spawn_blocking(move || serve_file(&root, &path))
            .await
            .unwrap_or(Err(NOT_FOUND))
    }
}

/// Serves one file under `root`; the blocking half of [`UiAssets::serve`].
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

/// The one regular file `relative` names under the root, resolved on disk
/// and kept under the root however it is reached.
fn file_under(root: &std::path::Path, relative: &str) -> Result<(PathBuf, bool), Refusal> {
    for candidate in candidates(relative) {
        let path = root.join(&candidate);
        let Ok(resolved) = std::fs::canonicalize(&path) else {
            continue;
        };
        // A symlink that leaves the root is refused with everything
        // else that does not name a file under it.
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

/// The relative file names `path` may name, in order: what it names, then
/// the exports of a page path, then the root's own page. A name with a
/// type suffix is only ever itself.
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

/// The path as the root's relative name, or `None` for one that must not
/// be served: encoded, naming an empty or dot part, or carrying a
/// character a path could misread.
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
            .and_then(|id| crate::persistence::parse_entity_id(id))
            .is_some()
    {
        if segments.len() % 2 != 0
            || segments.len() > 2 * (usize::from(crate::persistence::MAX_CONVERSATION_DEPTH) + 1)
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

/// Whether `segment` is one plain file name: no empty part, no dot part,
/// and only characters a build emits and a path cannot misread.
fn named(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~+@".contains(&byte))
}

/// The media type of a file, from the one suffix this serves. Anything
/// else is not sent at all.
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

/// The response for one asset: its whole body, typed, and cached as its
/// build names it.
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

#[cfg(test)]
mod nested_route_tests {
    use super::resolved;

    #[test]
    fn nested_conversation_paths_serve_the_shared_chat_page() {
        assert_eq!(
            resolved(
                "/jobs/00000001-0000-7001-8000-000000000001/jobs/00000003-0000-7001-8000-000000000003/jobs/00000005-0000-7001-8000-000000000005"
            ),
            Some(String::new())
        );
        for path in [
            "/jobs/00000001-0000-7001-8000-000000000001/jobs",
            "/jobs/00000001-0000-7001-8000-000000000001/jobs/00000001-0000-7001-8000-000000000001",
            "/jobs/00000001-0000-7001-8000-000000000001/other/00000002-0000-7001-8000-000000000002",
            "/jobs/job%3A1",
        ] {
            assert!(resolved(path).is_none(), "{path}");
        }
    }
}
