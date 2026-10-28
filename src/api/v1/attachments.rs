//! Image Attachments for the Control Plane:
//!
//! - `POST /api/v1/attachments`: stores one PNG or JPEG image, given as
//!   `{"mime": "image/png" | "image/jpeg", "data_base64": "...",
//!   "display_name": "..."}`, as a user-provided Attachment in the existing
//!   attachment store, and answers `201` with its metadata. The image is
//!   checked as a Provider request would check it, by its declared type,
//!   its size ([`MAX_IMAGE_BYTES`]) and its file signature, before anything
//!   is stored.
//! - `GET /api/v1/attachments/{attachment}`: the Attachment's metadata.
//! - `GET /api/v1/attachments/{attachment}/content`: its verified bytes, as
//!   the image they are, for display. Only user-provided images are served.
//!
//! An Attachment is referenced from a Job's input by identity; its bytes are
//! never copied into a Job.

use super::contracts::*;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ntex::http::StatusCode;
use ntex::http::header::{self, HeaderValue};
use ntex::web::HttpResponse;
use serde::Deserialize;

use super::{INTERNAL, INVALID_REQUEST, Refusal, Reply};
use crate::db::attachments::AttachmentError;
use crate::db::entities::{Attachment, AttachmentId, AttachmentOrigin, AttachmentRecord};
use crate::db::{DatabaseError, DatabaseHandle};
use crate::service::ai::providers::openai_compatible::image::MAX_IMAGE_BYTES;
use crate::service::ai::providers::openai_compatible::{ImageError, ImageInput, ImageMime};

const INVALID_UPLOAD: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_body",
    "the body is not an image upload this API accepts",
);
const UNSUPPORTED_IMAGE_TYPE: Refusal = Refusal::new(
    StatusCode::UNSUPPORTED_MEDIA_TYPE,
    "unsupported_image_type",
    "only image/png, image/jpeg, image/gif and image/webp images are accepted; nothing was stored",
);
const MALFORMED_IMAGE_DATA: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "malformed_image_data",
    "data_base64 is not canonical standard base64; nothing was stored",
);
const IMAGE_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::PAYLOAD_TOO_LARGE,
    "image_too_large",
    "the image is larger than 4 MiB; nothing was stored",
);
const IMAGE_EMPTY: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "image_empty",
    "the image is empty; nothing was stored",
);
const IMAGE_MISMATCH: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "image_type_mismatch",
    "the image's content is not the type it declares; nothing was stored",
);
const ATTACHMENT_NOT_STORED: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "attachment_not_stored",
    "the image could not be stored; nothing was stored",
);
pub(super) const ATTACHMENT_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::NOT_FOUND,
    "attachment_not_found",
    "no such image Attachment",
);
const ATTACHMENT_UNAVAILABLE: Refusal = Refusal::new(
    StatusCode::GONE,
    "attachment_unavailable",
    "the Attachment's content is no longer available",
);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadInput {
    mime: String,
    data_base64: String,
    #[serde(default)]
    display_name: Option<String>,
}

/// Parses a canonical Attachment UUID.
pub(super) fn attachment_id(text: &str) -> Result<AttachmentId, Refusal> {
    crate::db::entities::parse_entity_id(text)
        .map(AttachmentId::new)
        .ok_or(INVALID_REQUEST)
}

fn image_refusal(error: ImageError) -> Refusal {
    match error {
        ImageError::UnsupportedType | ImageError::NotDataUrl => UNSUPPORTED_IMAGE_TYPE,
        ImageError::MalformedBase64 => MALFORMED_IMAGE_DATA,
        ImageError::Empty => IMAGE_EMPTY,
        ImageError::TooLarge => IMAGE_TOO_LARGE,
        ImageError::Mismatch(_) => IMAGE_MISMATCH,
    }
}

fn attachment_refusal(error: AttachmentError) -> Refusal {
    match error {
        AttachmentError::Database(DatabaseError::Rejected(_)) => INVALID_UPLOAD,
        AttachmentError::Database(error) => error.into(),
        AttachmentError::NotFound(_) => ATTACHMENT_NOT_FOUND,
        AttachmentError::CleanedUp { .. } => ATTACHMENT_UNAVAILABLE,
        AttachmentError::Content { .. } => ATTACHMENT_UNAVAILABLE,
        AttachmentError::NotStored(_) => ATTACHMENT_NOT_STORED,
    }
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Attachment")]
pub(crate) struct AttachmentView {
    id: String,
    mime: String,
    byte_length: u64,
    sha256: String,
    display_name: Option<String>,
    created_at_us: i64,
    available: bool,
}

fn attachment_json(record: &AttachmentRecord) -> AttachmentView {
    AttachmentView {
        id: record.id.to_string(),
        mime: record.mime.clone(),
        byte_length: record.byte_length,
        sha256: record.sha256.to_string(),
        display_name: record.display_name.clone(),
        created_at_us: record.created_at.as_unix_micros(),
        available: record.is_available(),
    }
}

/// Stores one checked image as a user-provided Attachment.
pub(super) async fn upload(database: &DatabaseHandle, body: &[u8]) -> Result<Reply, Refusal> {
    let wire: UploadInput = serde_json::from_slice(body).map_err(|_| INVALID_UPLOAD)?;
    let mime = ImageMime::from_mime(&wire.mime).ok_or(UNSUPPORTED_IMAGE_TYPE)?;
    if wire.data_base64.len() > MAX_IMAGE_BYTES.div_ceil(3) * 4 {
        return Err(IMAGE_TOO_LARGE);
    }
    let bytes = STANDARD
        .decode(wire.data_base64.as_bytes())
        .map_err(|_| MALFORMED_IMAGE_DATA)?;
    let image = ImageInput::new(mime, bytes).map_err(image_refusal)?;
    let id = database
        .create_user_attachment(Attachment {
            content: image.bytes().to_vec(),
            mime: mime.as_str().to_owned(),
            display_name: wire.display_name,
        })
        .await
        .map_err(attachment_refusal)?;
    let record = database.attachment(id).await?.ok_or(INTERNAL)?;
    log::info!("control plane stored {id} ({} bytes)", record.byte_length);
    Ok(Reply::new(
        StatusCode::CREATED,
        envelope(serde_json::json!({
            "attachment": attachment_json(&record),
        })),
    ))
}

/// A user-provided image Attachment's record.
async fn image_record(
    database: &DatabaseHandle,
    id: AttachmentId,
) -> Result<AttachmentRecord, Refusal> {
    let record = database.attachment(id).await?.ok_or(ATTACHMENT_NOT_FOUND)?;
    if record.origin != AttachmentOrigin::UserProvided
        || ImageMime::from_mime(&record.mime).is_none()
    {
        return Err(ATTACHMENT_NOT_FOUND);
    }
    Ok(record)
}

pub(super) async fn metadata(
    database: &DatabaseHandle,
    id: AttachmentId,
) -> Result<Reply, Refusal> {
    let record = image_record(database, id).await?;
    Ok(Reply::ok(envelope(serde_json::json!({
        "attachment": attachment_json(&record),
    }))))
}

/// The verified bytes of a user-provided image, as that image.
pub(super) async fn content(
    database: &DatabaseHandle,
    id: AttachmentId,
) -> Result<HttpResponse, Refusal> {
    let record = image_record(database, id).await?;
    let mime = ImageMime::from_mime(&record.mime).ok_or(ATTACHMENT_NOT_FOUND)?;
    let content = database
        .attachment_content(id)
        .await
        .map_err(attachment_refusal)?;
    let image = ImageInput::new(mime, content.bytes).map_err(|_| ATTACHMENT_UNAVAILABLE)?;
    let mut response = HttpResponse::build(StatusCode::OK)
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::CONTENT_DISPOSITION, "inline")
        .body(image.bytes().to_vec());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(mime.as_str()),
    );
    Ok(response)
}
