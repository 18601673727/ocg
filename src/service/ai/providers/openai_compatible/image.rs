//! Images a Chat Completions request may carry: PNG or JPEG, inline as a
//! base64 `data:` URL in an `image_url` content part of a user message.
//!
//! A remote URL is never accepted, so nothing is fetched on the request's
//! behalf. An image is checked by its declared type, its size and its file
//! signature (the PNG signature, or the JPEG start-of-image marker); it is
//! not decoded as an image, so a file with a valid signature and corrupt
//! data passes here and is left to the Provider to refuse. The base64 must
//! be canonical: the standard alphabet, required padding and no stray bits
//! or whitespace.
//!
//! An image's bytes and its base64 never appear in a message here, nor in
//! the `Debug` output of the types that hold them.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

use super::request::{ContentPartWire, ImageUrlWire};

/// The largest image, decoded, one content part may carry.
pub const MAX_IMAGE_BYTES: usize = 4 * 1024 * 1024;
/// The most images one request may carry.
pub const MAX_REQUEST_IMAGES: usize = 8;
/// The largest sum of a request's decoded images.
pub const MAX_REQUEST_IMAGE_BYTES: usize = 16 * 1024 * 1024;

/// The longest base64 text of an image within [`MAX_IMAGE_BYTES`].
const MAX_IMAGE_BASE64_BYTES: usize = MAX_IMAGE_BYTES.div_ceil(3) * 4;

/// An image type OCG sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageMime {
    Png,
    Jpeg,
    Gif,
    Webp,
}

impl ImageMime {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Gif => "image/gif",
            Self::Webp => "image/webp",
        }
    }

    /// The type a MIME type names, exactly as written in lowercase.
    #[must_use]
    pub fn from_mime(mime: &str) -> Option<Self> {
        match mime {
            "image/png" => Some(Self::Png),
            "image/jpeg" => Some(Self::Jpeg),
            "image/gif" => Some(Self::Gif),
            "image/webp" => Some(Self::Webp),
            _ => None,
        }
    }

    /// Whether `bytes` matches the signature of this image type.
    fn matches_signature(self, bytes: &[u8]) -> bool {
        match self {
            Self::Png => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
            Self::Jpeg => bytes.starts_with(b"\xff\xd8\xff"),
            Self::Gif => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
            Self::Webp => {
                bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP"
            }
        }
    }
}

/// Why an image is refused. No message repeats any of the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ImageError {
    #[error("only image/png, image/jpeg, image/gif and image/webp images are supported")]
    UnsupportedType,
    #[error("only an inline base64 data: URL is supported, never a remote URL")]
    NotDataUrl,
    #[error("the image data is not canonical base64")]
    MalformedBase64,
    #[error("the image is empty")]
    Empty,
    #[error("the image is larger than {MAX_IMAGE_BYTES} bytes")]
    TooLarge,
    #[error("the image's content is not the {0} its type declares")]
    Mismatch(&'static str),
}

/// Checks decoded image bytes against the type they are declared as.
fn check(declared: ImageMime, bytes: &[u8]) -> Result<(), ImageError> {
    if bytes.is_empty() {
        return Err(ImageError::Empty);
    }
    if bytes.len() > MAX_IMAGE_BYTES {
        return Err(ImageError::TooLarge);
    }
    if !declared.matches_signature(bytes) {
        return Err(ImageError::Mismatch(declared.as_str()));
    }
    Ok(())
}

/// Checks an `image_url` part's URL: a `data:` URL of a supported type
/// whose canonical base64 decodes to an image of that type within the
/// limit. Returns the decoded size.
pub(crate) fn check_data_url(url: &str) -> Result<usize, ImageError> {
    let Some(rest) = url.strip_prefix("data:") else {
        return Err(ImageError::NotDataUrl);
    };
    let Some((header, data)) = rest.split_once(',') else {
        return Err(ImageError::MalformedBase64);
    };
    let Some((mime, "base64")) = header.split_once(';') else {
        return Err(ImageError::UnsupportedType);
    };
    let declared = ImageMime::from_mime(mime).ok_or(ImageError::UnsupportedType)?;
    if data.len() > MAX_IMAGE_BASE64_BYTES {
        return Err(ImageError::TooLarge);
    }
    let bytes = STANDARD
        .decode(data)
        .map_err(|_| ImageError::MalformedBase64)?;
    check(declared, &bytes)?;
    Ok(bytes.len())
}

/// A PNG or JPEG image, checked against its declared type and the size
/// limit, to be sent as a content part. Its `Debug` shows its type and size
/// only.
#[derive(Clone, PartialEq, Eq)]
pub struct ImageInput {
    mime: ImageMime,
    bytes: Vec<u8>,
}

impl ImageInput {
    /// The image `bytes` are, declared as `mime`.
    ///
    /// # Errors
    ///
    /// Refuses an empty or oversized image, and one whose signature is not
    /// that of `mime`.
    pub fn new(mime: ImageMime, bytes: Vec<u8>) -> Result<Self, ImageError> {
        check(mime, &bytes)?;
        Ok(Self { mime, bytes })
    }

    #[must_use]
    pub fn mime(&self) -> ImageMime {
        self.mime
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The image as an `image_url` content part: a base64 `data:` URL.
    #[must_use]
    pub fn part(&self) -> ContentPartWire {
        let mut url = format!("data:{};base64,", self.mime.as_str());
        STANDARD.encode_string(&self.bytes, &mut url);
        ContentPartWire::ImageUrl {
            image_url: ImageUrlWire { url },
        }
    }
}

impl std::fmt::Debug for ImageInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ImageInput")
            .field("mime", &self.mime.as_str())
            .field("bytes", &self.bytes.len())
            .finish()
    }
}
