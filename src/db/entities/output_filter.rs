use serde_json::Value;
use std::borrow::Cow;

#[derive(Clone, Copy)]
enum Rule {
    Dsml,
}

const RULES: &[Rule] = &[Rule::Dsml];

impl Rule {
    fn filter(self, text: &str) -> String {
        match self {
            Self::Dsml => TextFilter::default().consume(text, true),
        }
    }
}

pub(crate) fn text(input: &str) -> Cow<'_, str> {
    if !input.contains("DSML") {
        return Cow::Borrowed(input);
    }
    let mut filtered = Cow::Borrowed(input);
    for rule in RULES {
        let next = rule.filter(&filtered);
        if next != filtered {
            filtered = Cow::Owned(next);
        }
    }
    filtered
}

pub(crate) fn value(input: &Value) -> Cow<'_, Value> {
    match input {
        Value::String(string) => match text(string) {
            Cow::Borrowed(_) => Cow::Borrowed(input),
            Cow::Owned(filtered) => Cow::Owned(Value::String(filtered)),
        },
        Value::Array(values) => {
            let mut filtered = None;
            for (index, item) in values.iter().enumerate() {
                if let Cow::Owned(item) = value(item) {
                    filtered.get_or_insert_with(|| values.clone())[index] = item;
                }
            }
            filtered.map_or(Cow::Borrowed(input), |values| {
                Cow::Owned(Value::Array(values))
            })
        }
        Value::Object(values) => {
            let mut filtered = None;
            for (key, item) in values {
                if let Cow::Owned(item) = value(item) {
                    filtered
                        .get_or_insert_with(|| values.clone())
                        .insert(key.clone(), item);
                }
            }
            filtered.map_or(Cow::Borrowed(input), |values| {
                Cow::Owned(Value::Object(values))
            })
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => Cow::Borrowed(input),
    }
}

enum Tag<'a> {
    Plain,
    Pending {
        dsml: bool,
    },
    Complete {
        bytes: usize,
        name: &'a str,
        closing: bool,
        single: bool,
    },
}

fn tag(input: &str) -> Tag<'_> {
    let rest = input[1..].trim_start();
    let closing = rest.starts_with('/');
    let rest = rest.strip_prefix('/').unwrap_or(rest);
    let head = rest.trim_start_matches(|c: char| c == '|' || c == '｜' || c.is_whitespace());
    if !rest[..rest.len() - head.len()].contains(['|', '｜']) {
        return if head.is_empty() {
            Tag::Pending { dsml: false }
        } else {
            Tag::Plain
        };
    }
    let Some(rest) = head.strip_prefix("DSML") else {
        return if "DSML".starts_with(head) {
            Tag::Pending { dsml: false }
        } else {
            Tag::Plain
        };
    };
    let head = rest.trim_start_matches(|c: char| c == '|' || c == '｜' || c.is_whitespace());
    if !rest[..rest.len() - head.len()].contains(['|', '｜']) {
        return if head.is_empty() {
            Tag::Pending { dsml: true }
        } else {
            Tag::Plain
        };
    }
    let Some(end) = head.find('>') else {
        return Tag::Pending { dsml: true };
    };
    let name_end = head
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(end);
    Tag::Complete {
        bytes: input.len() - head.len() + end + 1,
        name: &head[..name_end],
        closing,
        single: head[..end].trim_end().ends_with('/'),
    }
}

#[derive(Default)]
pub(crate) struct TextFilter {
    pending: String,
    block: Option<(String, usize)>,
}

impl TextFilter {
    pub(crate) fn push(&mut self, input: &str) -> String {
        self.consume(input, false)
    }

    pub(crate) fn finish(&mut self) -> String {
        self.consume("", true)
    }

    fn consume(&mut self, input: &str, finished: bool) -> String {
        if self.pending.is_empty() && !input.contains('<') {
            return if self.block.is_none() {
                input.to_owned()
            } else {
                String::new()
            };
        }
        let mut buffered = std::mem::take(&mut self.pending);
        buffered.push_str(input);
        let mut output = String::new();
        let mut position = 0;
        while let Some(offset) = buffered[position..].find('<') {
            let start = position + offset;
            if self.block.is_none() {
                output.push_str(&buffered[position..start]);
            }
            match tag(&buffered[start..]) {
                Tag::Plain => {
                    if self.block.is_none() {
                        output.push('<');
                    }
                    position = start + 1;
                }
                Tag::Pending { dsml } => {
                    if !finished {
                        self.pending.push_str(&buffered[start..]);
                    } else if !dsml && self.block.is_none() {
                        output.push_str(&buffered[start..]);
                    }
                    return output;
                }
                Tag::Complete {
                    bytes,
                    name,
                    closing,
                    single,
                } => {
                    if let Some((block, depth)) = &mut self.block {
                        if name == block {
                            if closing {
                                *depth -= 1;
                            } else if !single {
                                *depth += 1;
                            }
                            if *depth == 0 {
                                self.block = None;
                            }
                        }
                    } else if !closing && !single {
                        self.block = Some((name.to_owned(), 1));
                    }
                    position = start + bytes;
                }
            }
        }
        if self.block.is_none() {
            output.push_str(&buffered[position..]);
        }
        output
    }
}
