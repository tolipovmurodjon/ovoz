//! Client for ElevenLabs Scribe v2 Realtime speech-to-text.
//!
//! Audio goes in as 16 kHz mono f32 frames of any size; partial text arrives as
//! [`ScribeEvent`]s and the final text is the return value of [`ScribeSession::finish`].
//! Only manual commit is supported: the caller decides when the utterance ends.
//!
//! # Logging
//!
//! The websocket libraries log the upgrade request at trace level. The API key travels
//! in a header marked sensitive, which they print as `Sensitive`, but the host app must
//! still cap the `tungstenite` and `tokio_tungstenite` log targets at `Info` so nothing
//! about the connection is written to a log file.
//!
//! # Threads
//!
//! A session is a task on a tokio runtime the caller owns. See
//! [`ScribeSession::connect`] for the exact calling pattern from a plain `std::thread`.

mod config;
mod error;
#[cfg(test)]
mod mock_server;
mod protocol;
mod session;
#[cfg(test)]
mod session_tests;

pub use config::{validate_keyterms, ScribeConfig};
pub use error::{Result, ScribeError};
pub use protocol::f32_to_pcm16_bytes;
pub use session::{ScribeEvent, ScribeSession, Transcript};
