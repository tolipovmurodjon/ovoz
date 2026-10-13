use std::fmt;
use std::time::Duration;

use url::Url;

const ENDPOINT: &str = "wss://api.elevenlabs.io/v1/speech-to-text/realtime";
const MODEL_ID: &str = "scribe_v2_realtime";
const MAX_KEYTERMS: usize = 50;
const MAX_KEYTERM_CHARS: usize = 20;

#[derive(Clone)]
pub struct ScribeConfig {
    pub api_key: String,
    pub language_code: Option<String>,
    pub secondary_languages: Vec<String>,
    pub keyterms: Vec<String>,
    pub no_verbatim: bool,
    /// Sent with the first audio chunk only, as the API requires.
    pub previous_text: Option<String>,
    /// Covers the websocket handshake and the wait for `session_started`. Short on
    /// purpose: the worst connect measured is 750 ms, and a dead network should make the
    /// caller fall back to its own recording quickly.
    pub connect_timeout: Duration,
    /// How long `finish` waits for the server after the final commit.
    pub finish_timeout: Duration,
    /// A socket write that takes longer than this fails the session, so a dead
    /// network is noticed during the hold instead of at the key release.
    pub send_timeout: Duration,
    /// Idle time after which the session sends 100 ms of silence. The server closes a
    /// socket that gets no audio for about 15 s (measured 15.4 s, close code 1000), and the
    /// `keepalive_interval_ms` URL parameter does not prevent it (measured with 500 and
    /// 5000). A hold with a thinking pause sends nothing when the caller gates out silence.
    /// A field only so tests can shorten it.
    pub(crate) idle_silence_after: Duration,
    /// Replaces the production endpoint so tests can talk to a local server.
    pub(crate) endpoint: Option<String>,
}

impl ScribeConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            language_code: None,
            secondary_languages: Vec::new(),
            keyterms: Vec::new(),
            no_verbatim: false,
            previous_text: None,
            connect_timeout: Duration::from_secs(3),
            finish_timeout: Duration::from_secs(3),
            send_timeout: Duration::from_secs(3),
            idle_silence_after: Duration::from_secs(8),
            endpoint: None,
        }
    }

    /// Builds the websocket URL. Array parameters are repeated keys
    /// (`keyterms=a&keyterms=b`), exactly as the official Python and JS SDKs send them.
    pub(crate) fn websocket_url(&self) -> Url {
        let mut url = Url::parse(self.endpoint.as_deref().unwrap_or(ENDPOINT))
            .expect("endpoint is a valid URL");
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("model_id", MODEL_ID);
            query.append_pair("audio_format", "pcm_16000");
            query.append_pair("commit_strategy", "manual");
            if let Some(language) = &self.language_code {
                query.append_pair("language_code", language);
            }
            for language in &self.secondary_languages {
                query.append_pair("secondary_languages", language);
            }
            for term in validate_keyterms(&self.keyterms) {
                query.append_pair("keyterms", &term);
            }
            if self.no_verbatim {
                query.append_pair("no_verbatim", "true");
            }
        }
        url
    }
}

// The key must never reach a log line through `{:?}`.
impl fmt::Debug for ScribeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScribeConfig")
            .field("api_key", &"<redacted>")
            .field("language_code", &self.language_code)
            .field("secondary_languages", &self.secondary_languages)
            .field("keyterms", &self.keyterms)
            .field("no_verbatim", &self.no_verbatim)
            .finish_non_exhaustive()
    }
}

/// The API allows 50 keyterms of at most 20 characters. A bad entry in a user's
/// dictionary should cost that entry, not the whole dictation, so extras are dropped.
pub fn validate_keyterms(terms: &[String]) -> Vec<String> {
    let mut kept = Vec::new();
    let (mut too_long, mut over_limit) = (0, 0);
    for term in terms {
        let term = term.trim();
        if term.is_empty() {
            continue;
        }
        if term.chars().count() > MAX_KEYTERM_CHARS {
            too_long += 1;
            continue;
        }
        if kept.len() == MAX_KEYTERMS {
            over_limit += 1;
            continue;
        }
        kept.push(term.to_string());
    }
    // Counts only: the words are the user's private dictionary and must not reach a log.
    if too_long > 0 {
        log::warn!("dropped {too_long} keyterms longer than {MAX_KEYTERM_CHARS} characters");
    }
    if over_limit > 0 {
        log::warn!("dropped {over_limit} keyterms beyond the limit of {MAX_KEYTERMS}");
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn keyterms_drop_empty_and_too_long() {
        let long = "x".repeat(21);
        let kept = validate_keyterms(&strings(&["Unity", " ", &long, " pull request "]));
        assert_eq!(kept, strings(&["Unity", "pull request"]));
    }

    #[test]
    fn keyterms_count_chars_not_bytes() {
        let twenty = "o".repeat(20);
        let twenty_multibyte = "\u{02BB}".repeat(20);
        let kept = validate_keyterms(&[twenty.clone(), twenty_multibyte.clone()]);
        assert_eq!(kept, vec![twenty, twenty_multibyte]);
    }

    #[test]
    fn keyterms_cap_at_fifty() {
        let many: Vec<String> = (0..60).map(|i| format!("term{i}")).collect();
        let kept = validate_keyterms(&many);
        assert_eq!(kept.len(), 50);
        assert_eq!(kept[49], "term49");
    }

    #[test]
    fn url_repeats_array_keys() {
        let mut config = ScribeConfig::new("k");
        config.language_code = Some("uz".into());
        config.secondary_languages = strings(&["en", "ru"]);
        config.keyterms = strings(&["Unity", "pull request"]);
        config.no_verbatim = true;
        let url = config.websocket_url().to_string();
        assert!(url.starts_with("wss://api.elevenlabs.io/v1/speech-to-text/realtime?"));
        assert!(url.contains("model_id=scribe_v2_realtime"));
        assert!(url.contains("audio_format=pcm_16000"));
        assert!(url.contains("language_code=uz"));
        assert!(url.contains("secondary_languages=en&secondary_languages=ru"));
        assert!(url.contains("keyterms=Unity&keyterms=pull+request"));
        assert!(url.contains("no_verbatim=true"));
    }

    #[test]
    fn url_omits_unset_options() {
        let url = ScribeConfig::new("k").websocket_url().to_string();
        assert!(!url.contains("language_code"));
        assert!(!url.contains("keyterms"));
        assert!(!url.contains("no_verbatim"));
        assert!(url.contains("commit_strategy=manual"));
    }

    #[test]
    fn debug_hides_the_key() {
        let shown = format!("{:?}", ScribeConfig::new("super-secret"));
        assert!(!shown.contains("super-secret"));
    }
}
