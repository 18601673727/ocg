use crate::contracts::{ChatImage, ChatImageUploadRequest};
use crate::error::{OcgError, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};

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

/// A provider image that OCG does not store: a remote URL it passes through.
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

/// The extension a stored image's name gets from its sniffed content type.
fn extension_for(media_type: &str) -> &'static str {
    match media_type {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        _ => "webp",
    }
}

fn data_url(media_type: &str, bytes: &[u8]) -> String {
    format!("data:{media_type};base64,{}", STANDARD.encode(bytes))
}

/// Store an image a user submits for a Job's input as a managed attachment.
/// The same name and content uploaded again is the same attachment.
pub fn upload(request: &ChatImageUploadRequest) -> Result<ChatImage> {
    let (media_type, bytes) = decode_data_url(&request.data_url)?;
    let name: String = request
        .name
        .chars()
        .filter(|character| !character.is_control())
        .take(120)
        .collect();
    let mut key = Sha256::new();
    key.update(name.as_bytes());
    key.update([0]);
    key.update(&bytes);
    let attachment = crate::attachments::publish(crate::attachments::NewAttachment {
        project_id: &request.project_id,
        operation_key: &format!("chat-upload:{:x}", key.finalize()),
        original_name: &name,
        media_type: Some(media_type),
        bytes: &bytes,
        producer: None,
    })?;
    Ok(public_image(
        ChatImage {
            id: attachment.id,
            name,
            media_type: media_type.to_string(),
            url: String::new(),
        },
        &request.project_id,
    ))
}

/// An image the model returned. A data URL is stored as an attachment of the
/// Call that produced it; a remote URL is passed through unstored.
///
/// The publication is keyed by the producing execution as well as the
/// content, so the same image from another Job, Attempt or Call is its own
/// attachment with its own provenance, while the image a Call streamed and
/// the same image settled with that Call remain one.
pub fn received(
    project_id: &str,
    producer: crate::attachments::Producer<'_>,
    url: &str,
) -> Result<ChatImage> {
    if !url.starts_with("data:") {
        return provider_image(url);
    }
    let (media_type, bytes) = decode_data_url(url)?;
    let name = format!("generated-image.{}", extension_for(media_type));
    let attachment = crate::attachments::publish(crate::attachments::NewAttachment {
        project_id,
        operation_key: &format!(
            "model-image:{}:{}:{}:{:x}",
            producer.job_id,
            producer.attempt_id.unwrap_or("-"),
            producer.call_id.unwrap_or("-"),
            Sha256::digest(url.as_bytes())
        ),
        original_name: &name,
        media_type: Some(media_type),
        bytes: &bytes,
        producer: Some(producer),
    })?;
    Ok(public_image(
        ChatImage {
            id: attachment.id,
            name,
            media_type: media_type.to_string(),
            url: String::new(),
        },
        project_id,
    ))
}

/// A stored image with its content, re-checked as an image of the type it
/// was stored as.
fn load(project_id: &str, id: &str) -> Result<(ChatImage, Vec<u8>)> {
    let (attachment, bytes) = crate::attachments::read(project_id, id)?;
    if bytes.len() > MAX_IMAGE_BYTES {
        return Err(invalid("stored image exceeds size limit"));
    }
    let media_type = attachment
        .media_type
        .ok_or_else(|| invalid("attachment is not an image"))?;
    decode_data_url(&data_url(&media_type, &bytes))?;
    Ok((
        ChatImage {
            id: attachment.id,
            name: attachment.original_name,
            media_type,
            url: String::new(),
        },
        bytes,
    ))
}

fn public_image(mut image: ChatImage, project_id: &str) -> ChatImage {
    image.url = format!("/api/v1/canonical/chat/images/{project_id}/{}", image.id);
    image
}

/// The images a message selects, which become references in the Job's input.
pub fn selected(project_id: &str, ids: &[String]) -> Result<Vec<ChatImage>> {
    if ids.len() > MAX_IMAGES {
        return Err(invalid("at most 8 images may be sent per message"));
    }
    let mut images = Vec::new();
    for id in ids {
        let (image, _) = load(project_id, id)?;
        let image = public_image(image, project_id);
        if !images
            .iter()
            .any(|candidate: &ChatImage| candidate.id == image.id)
        {
            images.push(image);
        }
    }
    Ok(images)
}

pub fn read(project_id: &str, id: &str) -> Result<(String, Vec<u8>)> {
    let (image, bytes) = load(project_id, id)?;
    Ok((image.media_type, bytes))
}

/// The URL a provider request carries for an image reference.
pub fn upstream_url(project_id: &str, image: &ChatImage) -> Result<String> {
    if image.url == format!("/api/v1/canonical/chat/images/{project_id}/{}", image.id) {
        let (image, bytes) = load(project_id, &image.id)?;
        return Ok(data_url(&image.media_type, &bytes));
    }
    if valid_provider_url(&image.url) {
        return Ok(image.url.clone());
    }
    Err(invalid("image does not belong to this Project"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::Producer;
    use crate::orchestration::domain::{DomainRepository, JobSpec};

    const IMAGE: &[u8] = b"\x89PNG\r\n\x1a\nprovenance";

    fn image_url() -> String {
        data_url("image/png", IMAGE)
    }

    fn job(domain: &DomainRepository, project_id: &str) -> String {
        domain
            .create_job(
                project_id,
                JobSpec {
                    objective: Some("image".into()),
                    ..JobSpec::default()
                },
            )
            .expect("job")
            .id
    }

    fn attachment(project_id: &str, id: &str) -> crate::attachments::Attachment {
        crate::attachments::get(project_id, id).expect("attachment")
    }

    #[test]
    fn identical_images_from_different_calls_keep_their_own_calls() {
        let directory = tempfile::tempdir().expect("temp");
        let mut domain = DomainRepository::open(directory.path()).expect("domain");
        let project = domain.ensure_project(directory.path()).expect("project");
        let job_id = job(&domain, &project.id);
        domain.set_job_eligible(&job_id).expect("eligible");
        let (attempt, executor) = domain.dispatch_job(&job_id, "provider").expect("dispatch");
        let mut call = || {
            domain
                .create_call(
                    &attempt.id,
                    Some(&executor.id),
                    attempt.generation,
                    false,
                    "{\"executor_transport\":\"provider\"}",
                )
                .expect("call")
                .id
        };
        let first_call = call();
        let second_call = call();
        let publish = |call_id: &str| {
            received(
                &project.id,
                Producer {
                    job_id: &job_id,
                    attempt_id: Some(&attempt.id),
                    call_id: Some(call_id),
                },
                &image_url(),
            )
            .expect("image")
        };

        let streamed = publish(&first_call);
        let settled = publish(&first_call);
        let second = publish(&second_call);

        assert_eq!(
            streamed.id, settled.id,
            "one Call's image is one attachment"
        );
        assert_ne!(settled.id, second.id);
        assert_eq!(
            attachment(&project.id, &settled.id).call_id.as_deref(),
            Some(first_call.as_str())
        );
        assert_eq!(
            attachment(&project.id, &second.id).call_id.as_deref(),
            Some(second_call.as_str())
        );
    }

    #[test]
    fn identical_images_from_different_jobs_keep_their_own_producers() {
        let directory = tempfile::tempdir().expect("temp");
        let domain = DomainRepository::open(directory.path()).expect("domain");
        let project = domain.ensure_project(directory.path()).expect("project");
        let first_job = job(&domain, &project.id);
        let second_job = job(&domain, &project.id);
        let publish = |job_id: &str| {
            received(
                &project.id,
                Producer {
                    job_id,
                    attempt_id: None,
                    call_id: None,
                },
                &image_url(),
            )
            .expect("image")
        };

        let first = publish(&first_job);
        let second = publish(&second_job);

        assert_ne!(first.id, second.id);
        assert_eq!(
            attachment(&project.id, &first.id).job_id.as_deref(),
            Some(first_job.as_str())
        );
        assert_eq!(
            attachment(&project.id, &second.id).job_id.as_deref(),
            Some(second_job.as_str())
        );
    }
}
