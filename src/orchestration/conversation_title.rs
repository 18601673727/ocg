pub(super) const INSTRUCTION: &str = "Name this new conversation alongside your first natural-language reply. Begin that reply with <ocg-title>A concise topic title</ocg-title> on its own line, then answer the user normally. Summarize the task or topic rather than copying the user's first sentence. Use the user's language, at most 80 characters and 256 UTF-8 bytes, with no markup or control characters in the title. Emit this title metadata only once in the conversation. Tool calls may precede the reply.";

pub(super) fn split(text: &str) -> Option<(&str, &str)> {
    let text = text.trim_start().strip_prefix("<ocg-title>")?;
    let (title, answer) = text.split_once("</ocg-title>")?;
    let title = title.trim();
    if title.is_empty()
        || title.len() > 256
        || title.chars().count() > 80
        || title
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '<' | '>'))
    {
        return None;
    }
    Some((title, answer.trim_start()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn title_metadata_is_bounded_and_separate_from_the_answer() {
        assert_eq!(
            super::split("  <ocg-title>  项目结构  </ocg-title>\nAnswer"),
            Some(("项目结构", "Answer"))
        );
        for invalid in [
            "Normal answer",
            "<ocg-title>Incomplete",
            "<ocg-title> </ocg-title>Answer",
            "<ocg-title>A\nB</ocg-title>Answer",
            "<ocg-title><b>Title</b></ocg-title>Answer",
        ] {
            assert_eq!(super::split(invalid), None);
        }
        assert!(
            super::split(&format!("<ocg-title>{}</ocg-title>Answer", "a".repeat(81))).is_none()
        );
        assert!(
            super::split(&format!("<ocg-title>{}</ocg-title>Answer", "😀".repeat(65))).is_none()
        );
    }
}
