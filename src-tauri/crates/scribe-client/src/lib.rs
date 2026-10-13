//! Client for ElevenLabs Scribe v2 Realtime speech-to-text.
//!
//! Audio goes in as 16 kHz mono f32 frames of any size; partial text arrives as
//! [`ScribeEvent`]s and the final text is the return value of [`ScribeSession::finish`].
//! Only manual commit is supported: the caller decides when the utterance ends.
//!
//! # Logging
//!
//! The websocket libraries log at trace level, and that is not safe: one line prints the
//! raw upgrade request, API key included, and another prints every received message,
//! transcript text included. The key header is marked sensitive, but that only changes
//! how the header prints through `Debug`; it does not stop those lines. The ONLY
//! protection is that the host app caps the `tungstenite` and `tokio_tungstenite` log
//! targets at `Info`, so nothing about the connection is written to a log file.
//!
//! # Threads
//!
//! A session is a task on a tokio runtime the caller owns. [`ScribeSession::start`] is
//! called at key press and returns at once; see it for the exact calling pattern from a
//! plain `std::thread`.

mod config;
mod error;
#[cfg(test)]
mod mock_server;
mod preview;
mod protocol;
mod session;
#[cfg(test)]
mod session_tests;

pub use config::{validate_keyterms, ScribeConfig};
pub use error::{Result, ScribeError};
pub use preview::PreviewText;
pub use protocol::f32_to_pcm16_bytes;
pub use session::{ScribeEvent, ScribeSession, Transcript};
