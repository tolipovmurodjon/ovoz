use thiserror::Error;

/// Errors are `Clone` because the session task stores the first one it sees
/// and hands it to every later caller.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ScribeError {
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("quota exceeded: {0}")]
    Quota(String),
    #[error("rate limited ({code}): {message}")]
    RateLimited { code: String, message: String },
    #[error("network error: {0}")]
    Network(String),
    #[error("timed out while {0}")]
    Timeout(&'static str),
    #[error("server error ({code}): {message}")]
    Server { code: String, message: String },
    #[error("session is closed")]
    Closed,
}

impl ScribeError {
    /// Maps the `message_type` of a server error frame to a typed error.
    pub(crate) fn from_server(code: &str, message: String) -> Self {
        match code {
            "auth_error" | "unaccepted_terms" => Self::Auth(message),
            "quota_exceeded" => Self::Quota(message),
            "rate_limited" | "commit_throttled" | "queue_overflow" | "resource_exhausted" => {
                Self::RateLimited {
                    code: code.to_string(),
                    message,
                }
            }
            _ => Self::Server {
                code: code.to_string(),
                message,
            },
        }
    }
}

pub type Result<T> = std::result::Result<T, ScribeError>;
