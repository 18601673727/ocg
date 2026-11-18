use std::sync::atomic::{Ordering, compiler_fence};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};

const MIN_FRAGMENT: usize = 8;

pub(crate) fn wipe_string(text: &mut String) {
    let mut bytes = std::mem::take(text).into_bytes();
    wipe_vec(&mut bytes);
}

pub(crate) fn wipe_vec(bytes: &mut Vec<u8>) {
    bytes.clear();
    bytes.resize(bytes.capacity(), 0);
    std::hint::black_box(&*bytes);
    compiler_fence(Ordering::SeqCst);
    bytes.clear();
}

#[derive(Default)]
pub(crate) struct Spellings(Vec<String>);

impl Spellings {
    pub(crate) fn new(secret: &str) -> Self {
        let mut forms = Vec::new();
        if secret.is_empty() {
            return Self(forms);
        }
        forms.push(secret.to_owned());
        if let Ok(quoted) = serde_json::to_string(secret)
            && let Some(escaped) = quoted
                .strip_prefix('"')
                .and_then(|quoted| quoted.strip_suffix('"'))
        {
            forms.push(escaped.to_owned());
        }
        forms.push(percent_encode(secret, false));
        forms.push(percent_encode(secret, true));
        let bytes = secret.as_bytes();
        forms.push(bytes.iter().map(|byte| format!("{byte:02x}")).collect());
        forms.push(bytes.iter().map(|byte| format!("{byte:02X}")).collect());
        for offset in 0..3usize {
            let mut stream = vec![0u8; offset];
            stream.extend_from_slice(bytes);
            let first = (8 * offset).div_ceil(6);
            let end = 8 * (offset + bytes.len()) / 6;
            for engine in [&STANDARD_NO_PAD, &URL_SAFE_NO_PAD] {
                let encoded = engine.encode(&stream);
                if let Some(window) = encoded.get(first..end.min(encoded.len()))
                    && window.len() >= MIN_FRAGMENT
                {
                    forms.push(window.to_owned());
                }
            }
        }
        forms.sort_by_key(|form| std::cmp::Reverse(form.len()));
        forms.dedup();
        let raw = secret.len();
        forms.retain(|form| form.len() >= MIN_FRAGMENT.min(raw) || form == secret);
        Self(forms)
    }

    pub(crate) fn extend(&mut self, secret: &str) {
        let mut other = Self::new(secret);
        self.0.append(&mut other.0);
        self.0.sort_by_key(|form| std::cmp::Reverse(form.len()));
        self.0.dedup();
    }

    pub(crate) fn found_in(&self, text: &str) -> bool {
        self.0.iter().any(|form| text.contains(form.as_str()))
    }

    pub(crate) fn stream(&self) -> SecretStream<'_> {
        SecretStream {
            patterns: self.iter().map(SecretPrefix::new).collect(),
            found: false,
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct SecretStream<'a> {
    patterns: Vec<SecretPrefix<'a>>,
    found: bool,
}

impl SecretStream<'_> {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> bool {
        self.found = self.found || self.patterns.iter_mut().any(|pattern| pattern.find(bytes));
        self.found
    }
}

struct SecretPrefix<'a> {
    spelling: &'a [u8],
    fallback: Vec<usize>,
    matched: usize,
}

impl<'a> SecretPrefix<'a> {
    fn new(spelling: &'a str) -> Self {
        let spelling = spelling.as_bytes();
        // KMP stores the longest proper prefix that is also a suffix at each offset.
        let mut fallback = vec![0; spelling.len()];
        let mut matched = 0;
        for (index, &byte) in spelling.iter().enumerate().skip(1) {
            matched = Self::advance(spelling, &fallback, matched, byte);
            fallback[index] = matched;
        }
        Self {
            spelling,
            fallback,
            matched: 0,
        }
    }

    fn advance(spelling: &[u8], fallback: &[usize], mut matched: usize, byte: u8) -> usize {
        while matched != 0 && spelling[matched] != byte {
            matched = fallback[matched - 1];
        }
        matched + usize::from(spelling[matched] == byte)
    }

    fn find(&mut self, mut bytes: &[u8]) -> bool {
        while let Some((&byte, rest)) = bytes.split_first() {
            if self.matched == 0 {
                let Some(start) = memchr::memchr(self.spelling[0], bytes) else {
                    break;
                };
                self.matched = 1;
                bytes = &bytes[start + 1..];
            } else {
                self.matched = Self::advance(self.spelling, &self.fallback, self.matched, byte);
                bytes = rest;
            }
            if self.matched == self.spelling.len() {
                return true;
            }
        }
        false
    }
}

impl Drop for Spellings {
    fn drop(&mut self) {
        self.0.iter_mut().for_each(wipe_string);
    }
}

impl std::fmt::Debug for Spellings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

fn percent_encode(text: &str, lower: bool) -> String {
    let mut encoded = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else if lower {
            encoded.push_str(&format!("%{byte:02x}"));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}
