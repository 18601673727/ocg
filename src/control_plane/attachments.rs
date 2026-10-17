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

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ntex::http::StatusCode;
use ntex::http::header::{self, HeaderValue};
use ntex::web::HttpResponse;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{API_VERSION, INTERNAL, INVALID_REQUEST, Refusal, Reply, decimal, micros};
use crate::attachments::AttachmentError;
use crate::database::{DatabaseError, DatabaseHandle};
use crate::openai_compatible::image::MAX_IMAGE_BYTES;
use crate::openai_compatible::{ImageError, ImageInput, ImageMime};
use crate::persistence::{AttachmentId, AttachmentOrigin, AttachmentRecord, NewAttachment};

const INVALID_UPLOAD: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_body",
    "the body is not an image upload this API accepts",
);
const UNSUPPORTED_IMAGE_TYPE: Refusal = Refusal::new(
    StatusCode::UNSUPPORTED_MEDIA_TYPE,
    "unsupported_image_type",
    "only image/png and image/jpeg images are accepted; nothing was stored",
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
struct WireUpload {
    mime: String,
    data_base64: String,
    #[serde(default)]
    display_name: Option<String>,
}

/// An Attachment written as OCG writes it: `attachment:<n>`.
pub(super) fn attachment_id(text: &str) -> Result<AttachmentId, Refusal> {
    text.strip_prefix("attachment:")
        .and_then(decimal::<u64>)
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

fn attachment_json(record: &AttachmentRecord) -> Value {
    json!({
        "id": record.id.to_string(),
        "mime": record.mime,
        "byte_length": record.byte_length,
        "sha256": record.sha256.to_string(),
        "display_name": record.display_name,
        "created_at_us": micros(Some(record.created_at)),
        "available": record.is_available(),
    })
}

/// Stores one checked image as a user-provided Attachment.
pub(super) async fn upload(database: &DatabaseHandle, body: &[u8]) -> Result<Reply, Refusal> {
    let wire: WireUpload = serde_json::from_slice(body).map_err(|_| INVALID_UPLOAD)?;
    let mime = ImageMime::from_mime(&wire.mime).ok_or(UNSUPPORTED_IMAGE_TYPE)?;
    if wire.data_base64.len() > MAX_IMAGE_BYTES.div_ceil(3) * 4 {
        return Err(IMAGE_TOO_LARGE);
    }
    let bytes = STANDARD
        .decode(wire.data_base64.as_bytes())
        .map_err(|_| MALFORMED_IMAGE_DATA)?;
    let image = ImageInput::new(mime, bytes).map_err(image_refusal)?;
    let id = database
        .create_user_attachment(NewAttachment {
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
        json!({"api_version": API_VERSION, "attachment": attachment_json(&record)}),
    ))
}

/// A user-provided image Attachment's record.
async fn image_record(
    database: &DatabaseHandle,
    id: AttachmentId,
) -> Result<AttachmentRecord, Refusal> {
    let record = database.attachment(id).await?.ok_or(ATTACHMENT_NOT_FOUND)?;
    if record.origin != AttachmentOrigin::UserProvided || ImageMime::from_mime(&record.mime).is_none()
    {
        return Err(ATTACHMENT_NOT_FOUND);
    }
    Ok(record)
}

pub(super) async fn metadata(database: &DatabaseHandle, id: AttachmentId) -> Result<Reply, Refusal> {
    let record = image_record(database, id).await?;
    Ok(Reply::ok(
        json!({"api_version": API_VERSION, "attachment": attachment_json(&record)}),
    ))
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::control_plane::profiles;
    use crate::control_plane::submissions::{JobRequest, check_configuration, digest_key, plan};
    use crate::database::DatabaseWorker;
    use crate::openai_compatible::image::tests::PNG_BASE64;
    use crate::orchestration::execution_runtime::{
        ExecutionRuntime,
        provider_seam::{self, Response},
    };
    use crate::persistence::{ExecutorType, LlmTransport, NewExecutor, NewSubmission, ProfileId};
    use crate::provider_execution::scripted::Step;
    use crate::test_support::{data_directory, wait};

    #[test]
    fn image_upload_and_vision_job_submission_work_end_to_end() {
        let (_root, directory) = data_directory("attachments", "vision-upload");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let scheduler = runtime.scheduler();

        // 1. Invalid uploads
        let invalid_mime = json!({
            "mime": "image/gif",
            "data_base64": PNG_BASE64,
        });
        assert_eq!(
            wait(upload(&handle, &serde_json::to_vec(&invalid_mime).unwrap())).err(),
            Some(UNSUPPORTED_IMAGE_TYPE)
        );

        let corrupt_b64 = json!({
            "mime": "image/png",
            "data_base64": "!!!not-valid-base64!!!",
        });
        assert_eq!(
            wait(upload(&handle, &serde_json::to_vec(&corrupt_b64).unwrap())).err(),
            Some(MALFORMED_IMAGE_DATA)
        );

        let mismatch_sig = json!({
            "mime": "image/jpeg",
            "data_base64": PNG_BASE64, // PNG data declared as JPEG
        });
        assert_eq!(
            wait(upload(&handle, &serde_json::to_vec(&mismatch_sig).unwrap())).err(),
            Some(IMAGE_MISMATCH)
        );

        // 2. Valid PNG upload
        let valid_upload = json!({
            "mime": "image/png",
            "data_base64": PNG_BASE64,
            "display_name": "test-diagram.png",
        });
        let upload_reply = wait(upload(&handle, &serde_json::to_vec(&valid_upload).unwrap()))
            .expect("upload png");
        assert_eq!(upload_reply.status, StatusCode::CREATED);
        let attachment_id_str = upload_reply.body["attachment"]["id"]
            .as_str()
            .expect("attachment id");
        let attach_id = attachment_id(attachment_id_str).expect("parse id");
        assert_eq!(
            upload_reply.body["attachment"]["display_name"],
            "test-diagram.png"
        );
        assert_eq!(upload_reply.body["attachment"]["mime"], "image/png");

        // 3. Read metadata
        let meta_reply = wait(metadata(&handle, attach_id)).expect("read metadata");
        assert_eq!(meta_reply.body["attachment"]["id"], attachment_id_str);
        assert_eq!(meta_reply.body["attachment"]["available"], true);

        // 4. Read content
        let content_resp = wait(content(&handle, attach_id)).expect("read content");
        assert_eq!(content_resp.status(), StatusCode::OK);
        assert_eq!(
            content_resp
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "image/png"
        );

        // 5. Executor & Profiles
        let executor = wait(handle.create_executor(NewExecutor {
            name: "vision-executor".to_owned(),
            executor_type: ExecutorType::Llm,
            concurrency_limit: NonZeroU32::MIN,
            llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        }))
        .expect("register executor");

        // Profile without Vision
        let text_llm = wait(profiles::save_profile(
            &handle,
            None,
            serde_json::to_vec(&json!({
                "kind": "llm",
                "name": "Text Only Model",
                "endpoint": "https://vision-test.invalid/v1",
                "credential_ref": "VISION_KEY",
                "credential": "sk-test-secret",
                "model": "text-model-1",
                "declared_capabilities": ["chat"],
            }))
            .unwrap()
            .as_slice(),
        ))
        .expect("create text llm");
        let text_llm_id: ProfileId = text_llm.body["profile_id"]
            .as_str()
            .unwrap()
            .strip_prefix("profile:")
            .unwrap()
            .parse()
            .map(ProfileId::new)
            .unwrap();

        // Profile with Vision
        let vision_llm = wait(profiles::save_profile(
            &handle,
            None,
            serde_json::to_vec(&json!({
                "kind": "llm",
                "name": "Vision Model",
                "endpoint": "https://vision-test.invalid/v1",
                "credential_ref": "VISION_KEY",
                "credential": "sk-test-secret",
                "model": "gpt-4o",
                "declared_capabilities": ["chat", "vision"],
            }))
            .unwrap()
            .as_slice(),
        ))
        .expect("create vision llm");
        let vision_llm_id: ProfileId = vision_llm.body["profile_id"]
            .as_str()
            .unwrap()
            .strip_prefix("profile:")
            .unwrap()
            .parse()
            .map(ProfileId::new)
            .unwrap();

        // 6. Submitting image with text-only profile is refused
        let text_job_req = JobRequest::parse(
            &serde_json::to_vec(&json!({
                "llm_profile": text_llm_id.to_string(),
                "executor": executor.to_string(),
                "objective": "Inspect this image",
                "attachments": [attachment_id_str],
            }))
            .unwrap(),
        )
        .expect("parse req");
        assert!(matches!(
            wait(check_configuration(&handle, &text_job_req)).err(),
            Some(refusal) if refusal.code == "vision_not_supported"
        ));

        // 7. Submitting image with vision-capable profile succeeds and runs
        let seen_conversations = Arc::new(Mutex::new(Vec::new()));
        let captured = seen_conversations.clone();
        provider_seam::install("https://vision-test.invalid/v1", move |conv| {
            captured
                .lock()
                .unwrap()
                .push(serde_json::to_value(conv).unwrap());
            Response {
                gate: None,
                steps: vec![
                    Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\"I see a 1x1 PNG image with white pixels.\"}}]}\n\n"),
                    Step::Chunk("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"),
                ],
            }
        });

        let vision_job_req = JobRequest::parse(
            &serde_json::to_vec(&json!({
                "llm_profile": vision_llm_id.to_string(),
                "executor": executor.to_string(),
                "objective": "What is in this image?",
                "attachments": [attachment_id_str],
            }))
            .unwrap(),
        )
        .expect("parse vision req");
        wait(check_configuration(&handle, &vision_job_req)).expect("vision check ok");

        let fingerprint = vision_job_req.fingerprint();
        let sub_key = digest_key(b"vision-test-key-00000001");
        let plan = plan(&handle, vision_job_req);
        let admitted = wait(scheduler.admit_submitted_provider(
            plan,
            NewSubmission {
                key: sub_key,
                fingerprint,
            },
        ))
        .expect("admit vision job");
        let job = admitted.job();
        wait(admitted.settled()).expect("vision job settled");

        // Verify sent payload contains image_url
        let sent = seen_conversations.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        let messages = sent[0]["messages"].as_array().expect("messages");
        assert_eq!(messages.len(), 1);
        let content_parts = messages[0]["content"].as_array().expect("parts");
        assert_eq!(content_parts.len(), 2);
        assert_eq!(content_parts[0]["type"], "text");
        assert_eq!(content_parts[0]["text"], "What is in this image?");
        assert_eq!(content_parts[1]["type"], "image_url");
        assert_eq!(
            content_parts[1]["image_url"]["url"],
            format!("data:image/png;base64,{PNG_BASE64}")
        );

        // Verify output recorded
        let outputs = wait(handle.job_outputs(job, None, 10)).expect("outputs");
        assert!(outputs.iter().any(|o| {
            o.summary
                .as_deref()
                .unwrap_or("")
                .contains("I see a 1x1 PNG image")
        }));

        runtime.shutdown().expect("shutdown runtime");
        worker.shutdown().expect("shutdown worker");
    }
}

