use thiserror::Error;

/// Errors are `Clone` because the session task stores them and hands them to every
/// later caller.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ScribeError {
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("quota exceeded: {0}")]
    Quota(String),
    #[error("rate limited ({code}): {message}")]
    RateLimited { code: String, message: String },
    /// The server refused a commit for having under 0.3 s of uncommitted audio. Every
    /// commit this crate sends carries at least 0.325 s, so this means the server refused
    /// audio the client believes it sent: the text may be incomplete.
    #[error("commit refused: {0}")]
    CommitThrottled(String),
    #[error("session time limit reached: {0}")]
    SessionTimeLimit(String),
    #[error("network error: {0}")]
    Network(String),
    #[error("timed out while {0}")]
    Timeout(&'static str),
    #[error("server error ({code}): {message}")]
    Server { code: String, message: String },
    /// The websocket closed. `code` is the close code, 1006 when there was no close frame.
    #[error("connection closed (code {code}): {reason}")]
    Closed { code: u16, reason: String },
}

impl ScribeError {
    /// Maps the `message_type` of a server error frame to a typed error.
    pub(crate) fn from_server(code: &str, message: String) -> Self {
        match code {
            "auth_error" | "unaccepted_terms" => Self::Auth(message),
            "quota_exceeded" => Self::Quota(message),
            "commit_throttled" => Self::CommitThrottled(message),
            "session_time_limit_exceeded" => Self::SessionTimeLimit(message),
            "rate_limited" | "queue_overflow" | "resource_exhausted" => Self::RateLimited {
                code: code.to_string(),
                message,
            },
            "error"
            | "input_error"
            | "invalid_request"
            | "chunk_size_exceeded"
            | "insufficient_audio_activity"
            | "transcriber_error" => Self::Server {
                code: code.to_string(),
                message,
            },
            other => {
                log::warn!("unrecognised scribe error frame type: {other}");
                Self::Server {
                    code: other.to_string(),
                    message,
                }
            }
        }
    }

    /// The session cannot continue: nothing more can be sent or received.
    /// Every other error is about one request and leaves the session usable.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Auth(_)
                | Self::Quota(_)
                | Self::SessionTimeLimit(_)
                | Self::Network(_)
                | Self::Closed { .. }
        )
    }

    /// The server's way of saying "there is nothing to transcribe in what you committed".
    /// It means empty text, not a failure.
    pub(crate) fn is_nothing_to_transcribe(&self) -> bool {
        matches!(self, Self::Server { code, .. } if code == "insufficient_audio_activity")
    }
}

pub type Result<T> = std::result::Result<T, ScribeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_throttled_is_a_real_error_not_an_empty_answer() {
        let error = ScribeError::from_server("commit_throttled", "need 0.3s".into());
        assert_eq!(error, ScribeError::CommitThrottled("need 0.3s".into()));
        assert!(!error.is_nothing_to_transcribe());
        assert!(!error.is_fatal());
    }

    #[test]
    fn only_dead_session_errors_are_fatal() {
        for code in [
            "auth_error",
            "quota_exceeded",
            "session_time_limit_exceeded",
        ] {
            assert!(
                ScribeError::from_server(code, String::new()).is_fatal(),
                "{code}"
            );
        }
        for code in [
            "input_error",
            "transcriber_error",
            "rate_limited",
            "brand_new_type",
        ] {
            assert!(
                !ScribeError::from_server(code, String::new()).is_fatal(),
                "{code}"
            );
        }
        assert!(ScribeError::Network("x".into()).is_fatal());
        assert!(ScribeError::Closed {
            code: 1006,
            reason: String::new()
        }
        .is_fatal());
        assert!(!ScribeError::Timeout("x").is_fatal());
    }

    #[test]
    fn insufficient_audio_activity_is_an_empty_answer() {
        let error = ScribeError::from_server("insufficient_audio_activity", String::new());
        assert!(error.is_nothing_to_transcribe());
    }
}
