use crate::session::{join_segments, ScribeEvent};

/// The live-preview text of a session, built from its events: finished segments plus the
/// partial being spoken now. Pure bookkeeping, so the host's overlay code stays trivial.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreviewText {
    segments: Vec<String>,
    tentative: String,
}

impl PreviewText {
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds one event in. Only `Partial` and `Committed` change the text: a `Committed`
    /// replaces the partial it finishes, and the others (including errors) leave what is
    /// already on screen alone.
    pub fn apply(&mut self, event: &ScribeEvent) {
        match event {
            ScribeEvent::Partial(text) => self.tentative = text.clone(),
            ScribeEvent::Committed(text) => {
                self.tentative.clear();
                self.segments.push(text.clone());
            }
            ScribeEvent::Ready | ScribeEvent::Warning(_) | ScribeEvent::Error(_) => {}
        }
    }

    /// Finished segments joined exactly as `finish` joins them.
    pub fn committed(&self) -> String {
        join_segments(&self.segments)
    }

    pub fn tentative(&self) -> &str {
        &self.tentative
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ScribeError;

    fn partial(text: &str) -> ScribeEvent {
        ScribeEvent::Partial(text.to_string())
    }

    fn committed(text: &str) -> ScribeEvent {
        ScribeEvent::Committed(text.to_string())
    }

    #[test]
    fn starts_empty() {
        let preview = PreviewText::new();
        assert_eq!(preview.committed(), "");
        assert_eq!(preview.tentative(), "");
    }

    #[test]
    fn partial_replaces_the_previous_partial() {
        let mut preview = PreviewText::new();
        preview.apply(&partial("hel"));
        preview.apply(&partial("hello wor"));
        assert_eq!(preview.tentative(), "hello wor");
        assert_eq!(preview.committed(), "");
    }

    #[test]
    fn committed_clears_the_partial_and_appends() {
        let mut preview = PreviewText::new();
        preview.apply(&partial("hello wor"));
        preview.apply(&committed("hello world"));
        assert_eq!(preview.committed(), "hello world");
        assert_eq!(preview.tentative(), "");
    }

    #[test]
    fn segments_join_like_finish_does() {
        let mut preview = PreviewText::new();
        preview.apply(&committed(" first "));
        preview.apply(&committed(""));
        preview.apply(&partial("sec"));
        preview.apply(&committed("second"));
        assert_eq!(preview.committed(), "first second");
    }

    #[test]
    fn partial_after_a_commit_shows_next_to_the_segments() {
        let mut preview = PreviewText::new();
        preview.apply(&committed("one"));
        preview.apply(&partial("tw"));
        assert_eq!(preview.committed(), "one");
        assert_eq!(preview.tentative(), "tw");
    }

    #[test]
    fn other_events_change_nothing() {
        let mut preview = PreviewText::new();
        preview.apply(&committed("one"));
        preview.apply(&partial("tw"));
        let before = preview.clone();
        preview.apply(&ScribeEvent::Ready);
        preview.apply(&ScribeEvent::Warning("note".into()));
        preview.apply(&ScribeEvent::Error(ScribeError::Timeout("connecting")));
        assert_eq!(preview, before);
    }
}
