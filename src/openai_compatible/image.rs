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
}

impl ImageMime {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
        }
    }

    /// The type a MIME type names, exactly as written in lowercase.
    #[must_use]
    pub fn from_mime(mime: &str) -> Option<Self> {
        match mime {
            "image/png" => Some(Self::Png),
            "image/jpeg" => Some(Self::Jpeg),
            _ => None,
        }
    }

    /// The bytes every file of the type begins with.
    fn signature(self) -> &'static [u8] {
        match self {
            Self::Png => b"\x89PNG\r\n\x1a\n",
            Self::Jpeg => b"\xff\xd8\xff",
        }
    }
}

/// Why an image is refused. No message repeats any of the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ImageError {
    #[error("only image/png and image/jpeg images are supported")]
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
    if !bytes.starts_with(declared.signature()) {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A real 1×1 PNG.
    pub(crate) const PNG_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";

    pub(crate) fn png() -> Vec<u8> {
        STANDARD.decode(PNG_BASE64).expect("a PNG")
    }

    /// Bytes that begin as a JFIF JPEG does. Only the signature is checked,
    /// so they stand in for a JPEG here.
    pub(crate) fn jpeg() -> Vec<u8> {
        b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9".to_vec()
    }

    fn url(mime: &str, bytes: &[u8]) -> String {
        format!("data:{mime};base64,{}", STANDARD.encode(bytes))
    }

    #[test]
    fn an_image_becomes_an_inline_data_url_part_of_its_exact_bytes() {
        let image = ImageInput::new(ImageMime::Png, png()).expect("a PNG");
        let ContentPartWire::ImageUrl { image_url } = image.part() else {
            panic!("an image part");
        };
        assert_eq!(image_url.url, format!("data:image/png;base64,{PNG_BASE64}"));
        assert_eq!(check_data_url(&image_url.url), Ok(png().len()));

        let image = ImageInput::new(ImageMime::Jpeg, jpeg()).expect("a JPEG");
        let ContentPartWire::ImageUrl { image_url } = image.part() else {
            panic!("an image part");
        };
        assert_eq!(image_url.url, url("image/jpeg", &jpeg()));
        assert_eq!(check_data_url(&image_url.url), Ok(jpeg().len()));
    }

    #[test]
    fn content_must_match_its_declared_type() {
        assert_eq!(
            ImageInput::new(ImageMime::Jpeg, png()),
            Err(ImageError::Mismatch("image/jpeg"))
        );
        assert_eq!(
            ImageInput::new(ImageMime::Png, jpeg()),
            Err(ImageError::Mismatch("image/png"))
        );
        let gif = b"GIF89a\x01\x00\x01\x00".to_vec();
        assert_eq!(
            ImageInput::new(ImageMime::Png, gif.clone()),
            Err(ImageError::Mismatch("image/png"))
        );
        assert_eq!(
            ImageInput::new(ImageMime::Png, Vec::new()),
            Err(ImageError::Empty)
        );
        assert_eq!(
            check_data_url(&url("image/jpeg", &png())),
            Err(ImageError::Mismatch("image/jpeg"))
        );
        assert_eq!(
            check_data_url(&url("image/png", b"not an image at all")),
            Err(ImageError::Mismatch("image/png"))
        );
    }

    #[test]
    fn an_image_is_limited_in_size_before_it_is_decoded() {
        let mut largest = png();
        largest.resize(MAX_IMAGE_BYTES, 0);
        assert!(ImageInput::new(ImageMime::Png, largest.clone()).is_ok());
        assert_eq!(
            check_data_url(&url("image/png", &largest)),
            Ok(MAX_IMAGE_BYTES)
        );
        largest.push(0);
        assert_eq!(
            ImageInput::new(ImageMime::Png, largest.clone()),
            Err(ImageError::TooLarge)
        );
        assert_eq!(
            check_data_url(&url("image/png", &largest)),
            Err(ImageError::TooLarge)
        );
        // Too long to be within the limit, so never decoded.
        let text = format!(
            "data:image/png;base64,{}",
            "!".repeat(MAX_IMAGE_BASE64_BYTES + 1)
        );
        assert_eq!(check_data_url(&text), Err(ImageError::TooLarge));
    }

    #[test]
    fn only_inline_png_or_jpeg_in_canonical_base64_is_accepted() {
        let data = STANDARD.encode(png());
        for (text, error) in [
            (
                "https://example.com/image.png".to_owned(),
                ImageError::NotDataUrl,
            ),
            ("file:///etc/passwd".to_owned(), ImageError::NotDataUrl),
            (
                format!("DATA:image/png;base64,{data}"),
                ImageError::NotDataUrl,
            ),
            (
                format!("data:image/gif;base64,{data}"),
                ImageError::UnsupportedType,
            ),
            (
                format!("data:image/webp;base64,{data}"),
                ImageError::UnsupportedType,
            ),
            (
                format!("data:image/svg+xml;base64,{data}"),
                ImageError::UnsupportedType,
            ),
            (
                format!("data:image/PNG;base64,{data}"),
                ImageError::UnsupportedType,
            ),
            (
                format!("data:image/png,{data}"),
                ImageError::UnsupportedType,
            ),
            (
                format!("data:image/png;charset=utf-8,{data}"),
                ImageError::UnsupportedType,
            ),
            (format!("data:;base64,{data}"), ImageError::UnsupportedType),
            (
                "data:image/png;base64".to_owned(),
                ImageError::MalformedBase64,
            ),
            (
                format!("data:image/png;base64,{data}!"),
                ImageError::MalformedBase64,
            ),
            (
                format!("data:image/png;base64,{}", data.trim_end_matches('=')),
                ImageError::MalformedBase64,
            ),
            (
                format!("data:image/png;base64,{}\n{}", &data[..8], &data[8..]),
                ImageError::MalformedBase64,
            ),
            (
                // The URL-safe alphabet is not the standard one.
                format!("data:image/png;base64,{}-{}", &data[..20], &data[21..]),
                ImageError::MalformedBase64,
            ),
            // `iVBORw0KGgp=` would decode with stray bits set.
            (
                "data:image/png;base64,iVBORw0KGgp=".to_owned(),
                ImageError::MalformedBase64,
            ),
            ("data:image/png;base64,".to_owned(), ImageError::Empty),
        ] {
            assert_eq!(check_data_url(&text), Err(error), "{text:.60}");
        }
    }

    #[test]
    fn nothing_of_an_image_appears_in_debug_output_or_messages() {
        let image = ImageInput::new(ImageMime::Png, png()).expect("a PNG");
        assert_eq!(
            format!("{image:?}"),
            format!(
                "ImageInput {{ mime: \"image/png\", bytes: {} }}",
                png().len()
            )
        );
        let part = format!("{:?}", image.part());
        assert!(!part.contains(&PNG_BASE64[..12]), "{part}");
        assert!(part.contains("data:image/png;base64,<"), "{part}");
        for error in [
            ImageError::UnsupportedType,
            ImageError::NotDataUrl,
            ImageError::MalformedBase64,
            ImageError::Empty,
            ImageError::TooLarge,
            ImageError::Mismatch("image/png"),
        ] {
            assert!(!error.to_string().contains("iVBOR"));
        }
    }
}
