/// Lowercases, folds every apostrophe variant to `'`, strips other punctuation
/// and splits into words. Uzbek writes o'/g' with several look-alike marks, and a
/// model may pick a different one than the reference without being wrong.
pub fn normalize_words(text: &str) -> Vec<String> {
    let folded: String = text
        .chars()
        .flat_map(char::to_lowercase)
        .map(|c| match c {
            '\u{2018}' | '\u{2019}' | '\u{02BB}' | '\u{02BC}' | '\u{0060}' | '\u{00B4}' => '\'',
            c if c.is_alphanumeric() || c == '\'' => c,
            _ => ' ',
        })
        .collect();
    folded
        .split_whitespace()
        .map(|word| word.trim_matches('\'').to_string())
        .filter(|word| !word.is_empty())
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WordErrors {
    pub edits: usize,
    pub reference_words: usize,
}

impl WordErrors {
    pub fn rate(self) -> f64 {
        if self.reference_words == 0 {
            return if self.edits == 0 { 0.0 } else { 1.0 };
        }
        self.edits as f64 / self.reference_words as f64
    }
}

/// Word-level Levenshtein distance (substitutions + insertions + deletions).
pub fn word_errors(reference: &str, hypothesis: &str) -> WordErrors {
    let reference = normalize_words(reference);
    let hypothesis = normalize_words(hypothesis);
    let mut previous: Vec<usize> = (0..=hypothesis.len()).collect();
    for (i, ref_word) in reference.iter().enumerate() {
        let mut current = vec![i + 1];
        for (j, hyp_word) in hypothesis.iter().enumerate() {
            let substitution = previous[j] + usize::from(ref_word != hyp_word);
            current.push(substitution.min(previous[j + 1] + 1).min(current[j] + 1));
        }
        previous = current;
    }
    WordErrors {
        edits: previous[hypothesis.len()],
        reference_words: reference.len(),
    }
}

pub fn wer(reference: &str, hypothesis: &str) -> f64 {
    word_errors(reference, hypothesis).rate()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_text_is_zero() {
        assert_eq!(wer("Open the file.", "open the FILE"), 0.0);
    }

    #[test]
    fn counts_substitution_insertion_and_deletion() {
        assert_eq!(word_errors("a b c d", "a x c d").edits, 1);
        assert_eq!(word_errors("a b c d", "a b c d e").edits, 1);
        assert_eq!(word_errors("a b c d", "a b d").edits, 1);
        assert!((wer("a b c d", "x y") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn apostrophe_variants_are_the_same_letter() {
        let reference = "o'qidim g'oya";
        for variant in [
            "o\u{02BB}qidim g\u{02BB}oya",
            "o\u{2019}qidim g\u{2018}oya",
            "o`qidim g\u{00B4}oya",
        ] {
            assert_eq!(wer(reference, variant), 0.0, "{variant}");
        }
    }

    #[test]
    fn punctuation_is_ignored_but_a_split_word_is_an_error() {
        assert_eq!(wer("Unity'da, camera!", "unity'da camera"), 0.0);
        assert!(wer("Unity'da", "Unity da") > 0.0);
    }

    #[test]
    fn empty_cases() {
        assert_eq!(wer("", ""), 0.0);
        assert_eq!(wer("", "extra"), 1.0);
        assert_eq!(wer("one two", ""), 1.0);
    }

    #[test]
    fn handles_cyrillic_case() {
        assert_eq!(wer("Салом дунё", "салом ДУНЁ."), 0.0);
    }
}
