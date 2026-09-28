//! Static product UI embedded by the root build script and served by the
//! loopback control server.

mod generated {
    include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));
}

use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Asset {
    pub path: &'static str,
    pub body: &'static [u8],
    pub content_type: &'static str,
}

pub fn is_packaged() -> bool {
    generated::PACKAGED
}

pub fn build_note() -> &'static str {
    generated::BUILD_NOTE
}

pub fn ui_version() -> &'static str {
    generated::UI_VERSION
}

pub fn backend_version() -> &'static str {
    generated::BACKEND_VERSION
}

pub fn versions_match() -> bool {
    ui_version() == backend_version()
}

pub fn asset(path: &str) -> Option<&'static Asset> {
    if !is_safe_asset_path(path) {
        return None;
    }
    assets()
        .binary_search_by(|asset| asset.path.cmp(path))
        .ok()
        .map(|index| &assets()[index])
}

fn assets() -> &'static [Asset] {
    static TABLE: OnceLock<Vec<Asset>> = OnceLock::new();
    TABLE
        .get_or_init(|| {
            generated::ASSETS
                .iter()
                .map(|(path, body)| Asset {
                    path,
                    body,
                    content_type: content_type_for(path),
                })
                .collect()
        })
        .as_slice()
}

pub fn resolve(path: &str) -> Option<&'static Asset> {
    let path = path.trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    if !is_safe_asset_path(path) {
        return None;
    }
    asset(path)
        .or_else(|| asset(&format!("{path}.html")))
        .or_else(|| asset(&format!("{path}/index.html")))
        .or_else(|| {
            (!path.rsplit('/').next().unwrap_or_default().contains('.'))
                .then(|| asset("index.html"))
                .flatten()
        })
}

pub fn is_control_path(path: &str) -> bool {
    let path = path.trim_start_matches('/');
    path == "api" || path.starts_with("api/") || path == "v1" || path.starts_with("v1/")
}

pub fn is_safe_asset_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.contains('\0')
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

pub fn content_type_for(path: &str) -> &'static str {
    match path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}

pub fn is_immutable_asset(path: &str) -> bool {
    path.starts_with("_next/static/")
}
