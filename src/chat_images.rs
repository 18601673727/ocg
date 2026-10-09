use crate::contracts::{ChatImage, ChatImageUploadRequest};
use crate::error::{OcgError, Result};
use crate::project_store::ProjectDocuments;
use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};
use std::path::Path;

pub const MAX_IMAGE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_IMAGES: usize = 8;
pub const MAX_UPLOAD_BODY_BYTES: usize = 6 * 1024 * 1024;

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}

fn decode_data_url(url: &str) -> Result<(&str, Vec<u8>)> {
    let (header, encoded) = url
        .split_once(',')
        .ok_or_else(|| invalid("invalid image data URL"))?;
    let media_type = header
        .strip_prefix("data:")
        .and_then(|value| value.strip_suffix(";base64"))
        .ok_or_else(|| invalid("images must use base64 data URLs"))?;
    if encoded.len() > MAX_IMAGE_BYTES * 4 / 3 + 4 {
        return Err(invalid("each image must be at most 4 MiB"));
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| invalid("invalid image base64"))?;
    let sniffed = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else {
        return Err(invalid("only PNG, JPEG, GIF and WebP images are supported"));
    };
    if media_type != sniffed || bytes.len() > MAX_IMAGE_BYTES {
        return Err(invalid("image type or size does not match its content"));
    }
    Ok((media_type, bytes))
}

pub fn valid_provider_url(url: &str) -> bool {
    if url.starts_with("data:") {
        return decode_data_url(url).is_ok();
    }
    (url.starts_with("https://") || url.starts_with("http://"))
        && url.len() <= 8192
        && !url
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
}

pub fn provider_image(url: &str) -> Result<ChatImage> {
    if !valid_provider_url(url) {
        return Err(invalid("provider returned an unsupported image URL"));
    }
    let media_type = url
        .strip_prefix("data:")
        .and_then(|value| value.split(';').next())
        .unwrap_or("image/*");
    Ok(ChatImage {
        id: format!("model-{:x}", Sha256::digest(url.as_bytes())),
        name: "Generated image".to_string(),
        media_type: media_type.to_string(),
        url: url.to_string(),
    })
}

/// The document kind images are stored under. Images are resources a frozen
/// Job input may reference, so they are kept for the Project's lifetime.
const IMAGE_KIND: &str = "chat-image";

fn validate_identity(id: &str) -> Result<()> {
    let generated = id
        .strip_prefix("model-")
        .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let uploaded = uuid::Uuid::parse_str(id).is_ok_and(|parsed| parsed.to_string() == id);
    if !generated && !uploaded {
        return Err(invalid("invalid image identity"));
    }
    Ok(())
}

fn store(root: &Path, image: &ChatImage) -> Result<()> {
    validate_identity(&image.id)?;
    let bytes = serde_json::to_vec(image).map_err(|error| invalid(&error.to_string()))?;
    ProjectDocuments::for_root(root)?.put(IMAGE_KIND, &image.id, &bytes)
}

pub fn upload(root: &Path, request: &ChatImageUploadRequest) -> Result<ChatImage> {
    let (media_type, _) = decode_data_url(&request.data_url)?;
    let id = uuid::Uuid::now_v7().to_string();
    let image = ChatImage {
        id: id.clone(),
        name: request
            .name
            .chars()
            .filter(|character| !character.is_control())
            .take(120)
            .collect(),
        media_type: media_type.to_string(),
        url: request.data_url.clone(),
    };
    store(root, &image)?;
    Ok(public_image(image, &request.project_id))
}

fn load(root: &Path, id: &str) -> Result<ChatImage> {
    validate_identity(id)?;
    let bytes = ProjectDocuments::existing(root)?
        .map(|documents| documents.get(IMAGE_KIND, id))
        .transpose()?
        .flatten()
        .ok_or_else(|| invalid("unknown chat image"))?;
    if bytes.len() > MAX_UPLOAD_BODY_BYTES {
        return Err(invalid("stored image exceeds size limit"));
    }
    let image: ChatImage =
        serde_json::from_slice(&bytes).map_err(|error| invalid(&error.to_string()))?;
    if image.id != id {
        return Err(invalid("image identity mismatch"));
    }
    decode_data_url(&image.url)?;
    Ok(image)
}

fn public_image(mut image: ChatImage, project_id: &str) -> ChatImage {
    image.url = format!("/api/v1/canonical/chat/images/{project_id}/{}", image.id);
    image
}

pub fn selected(root: &Path, project_id: &str, ids: &[String]) -> Result<Vec<ChatImage>> {
    if ids.len() > MAX_IMAGES {
        return Err(invalid("at most 8 images may be sent per message"));
    }
    let mut images = Vec::new();
    for id in ids {
        let image = public_image(load(root, id)?, project_id);
        if !images
            .iter()
            .any(|candidate: &ChatImage| candidate.id == image.id)
        {
            images.push(image);
        }
    }
    Ok(images)
}

pub fn read(root: &Path, id: &str) -> Result<(String, Vec<u8>)> {
    let image = load(root, id)?;
    let (media_type, bytes) = decode_data_url(&image.url)?;
    Ok((media_type.to_string(), bytes))
}

pub fn upstream_url(root: &Path, project_id: &str, image: &ChatImage) -> Result<String> {
    if image.url == format!("/api/v1/canonical/chat/images/{project_id}/{}", image.id) {
        return Ok(load(root, &image.id)?.url);
    }
    if valid_provider_url(&image.url) {
        return Ok(image.url.clone());
    }
    Err(invalid("image does not belong to this Project"))
}

pub fn received(root: &Path, project_id: &str, url: &str) -> Result<ChatImage> {
    let image = provider_image(url)?;
    if !url.starts_with("data:") {
        return Ok(image);
    }
    store(root, &image)?;
    Ok(public_image(image, project_id))
}
