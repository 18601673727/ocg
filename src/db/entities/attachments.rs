use super::*;

/// Where an Attachment's content came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentOrigin {
    /// Given to OCG by its user, such as an image sent with an objective.
    UserProvided,
    /// Produced by a model. No Provider output creates one yet.
    ModelProduced,
}

/// Who removed an Attachment's content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupKind {
    /// The user asked for it.
    Manual,
    /// A retention policy did.
    Automatic,
}

/// That an Attachment's content was cleaned up: how, and when. Its kind and
/// time are recorded together or not at all. Cleanup removes availability,
/// never history: the Attachment, its metadata and every reference to it
/// remain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachmentCleanup {
    pub kind: CleanupKind,
    pub cleaned_at: Timestamp,
}

/// A stored Attachment: a durable business entity with an identity of its
/// own, independent of its content. Two Attachments of identical bytes are
/// two Attachments with one [`ContentDigest`]; each keeps its own name,
/// origin and cleanup. Attachments have no update path apart from cleanup,
/// see [`crate::db::DatabaseHandle::cleanup_attachment`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentRecord {
    pub id: AttachmentId,
    pub sha256: ContentDigest,
    /// The content's length in bytes, kept after its cleanup.
    pub byte_length: u64,
    /// A lowercase `type/subtype`, without parameters.
    pub mime: String,
    /// A sanitized file name, for display only; never a path.
    pub display_name: Option<String>,
    pub origin: AttachmentOrigin,
    pub created_at: Timestamp,
    /// `None` while the content is available.
    pub cleanup: Option<AttachmentCleanup>,
}

impl AttachmentRecord {
    /// Whether the Attachment's content may still be used. A file that is
    /// missing or does not match is refused when it is read, see
    /// [`crate::db::DatabaseHandle::attachment_content`].
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.cleanup.is_none()
    }
}

/// Content to store as a new user-provided Attachment.
#[derive(Clone, PartialEq, Eq)]
pub struct Attachment {
    pub content: Vec<u8>,
    /// A lowercase `type/subtype`, without parameters.
    pub mime: String,
    /// A file name to display. It is sanitized, see
    /// [`sanitize_display_name`]; an empty result is no name.
    pub display_name: Option<String>,
}

/// Shows the content by its size only.
impl fmt::Debug for Attachment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attachment")
            .field("content", &self.content.len())
            .field("mime", &self.mime)
            .field("display_name", &self.display_name)
            .finish()
    }
}

/// The largest Attachment content OCG stores.
pub const MAX_ATTACHMENT_BYTES: usize = 16 * 1024 * 1024;
/// The longest MIME type and display name, in bytes.
pub const MAX_ATTACHMENT_MIME_BYTES: usize = 255;
pub const MAX_ATTACHMENT_NAME_BYTES: usize = 255;

/// A display name as OCG keeps it: the last component of a path, without
/// control characters or surrounding whitespace, cut on a character
/// boundary to [`MAX_ATTACHMENT_NAME_BYTES`]. `None` when nothing is left,
/// or when it is `.` or `..`.
#[must_use]
pub fn sanitize_display_name(name: &str) -> Option<String> {
    let last = name.rsplit(['/', '\\']).next().unwrap_or_default();
    let cleaned: String = last
        .chars()
        .filter(|character| !character.is_control())
        .collect();
    let mut cleaned = cleaned.trim();
    while cleaned.len() > MAX_ATTACHMENT_NAME_BYTES {
        let mut end = MAX_ATTACHMENT_NAME_BYTES;
        while !cleaned.is_char_boundary(end) {
            end -= 1;
        }
        cleaned = cleaned[..end].trim_end();
    }
    match cleaned {
        "" | "." | ".." => None,
        name => Some(name.to_owned()),
    }
}
