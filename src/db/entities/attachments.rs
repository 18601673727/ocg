use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentOrigin {
    UserProvided,
    ModelProduced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupKind {
    Manual,
    Automatic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachmentCleanup {
    pub kind: CleanupKind,
    pub cleaned_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentRecord {
    pub id: AttachmentId,
    pub sha256: ContentDigest,
    pub byte_length: u64,
    pub mime: String,
    pub display_name: Option<String>,
    pub origin: AttachmentOrigin,
    pub created_at: Timestamp,
    pub cleanup: Option<AttachmentCleanup>,
}

impl AttachmentRecord {
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.cleanup.is_none()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Attachment {
    pub content: Vec<u8>,
    pub mime: String,
    pub display_name: Option<String>,
}

impl fmt::Debug for Attachment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attachment")
            .field("content", &self.content.len())
            .field("mime", &self.mime)
            .field("display_name", &self.display_name)
            .finish()
    }
}

pub const MAX_ATTACHMENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ATTACHMENT_MIME_BYTES: usize = 255;
pub const MAX_ATTACHMENT_NAME_BYTES: usize = 255;

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
