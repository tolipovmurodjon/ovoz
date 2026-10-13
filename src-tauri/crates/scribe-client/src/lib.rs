//! Client for ElevenLabs Scribe v2 Realtime speech-to-text.
//!
//! Audio goes in as 16 kHz mono f32 (or i16) frames; transcripts come back as
//! [`ScribeEvent`]s and as the return value of [`ScribeSession::finish`].

mod config;
mod error;
mod protocol;
mod session;
pub mod wer;

pub use config::{validate_keyterms, CommitStrategy, ScribeConfig};
pub use error::{Result, ScribeError};
pub use protocol::f32_to_pcm16_bytes;
pub use session::{ScribeEvent, ScribeSession};
