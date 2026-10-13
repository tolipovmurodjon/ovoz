use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Serialize;
use serde_json::Value;

pub(crate) const SAMPLE_RATE: u32 = 16_000;

const ERROR_TYPES: &[&str] = &[
    "error",
    "auth_error",
    "quota_exceeded",
    "commit_throttled",
    "unaccepted_terms",
    "rate_limited",
    "queue_overflow",
    "resource_exhausted",
    "session_time_limit_exceeded",
    "input_error",
    "invalid_request",
    "chunk_size_exceeded",
    "insufficient_audio_activity",
    "transcriber_error",
];

#[derive(Serialize)]
struct AudioChunk<'a> {
    message_type: &'static str,
    audio_base_64: &'a str,
    commit: bool,
    sample_rate: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_text: Option<&'a str>,
}

/// One `input_audio_chunk` frame. An empty `pcm` with `commit` set closes the
/// segment, which is how the official SDKs commit.
pub(crate) fn audio_chunk_json(pcm: &[u8], commit: bool, previous_text: Option<&str>) -> String {
    let encoded = STANDARD.encode(pcm);
    serde_json::to_string(&AudioChunk {
        message_type: "input_audio_chunk",
        audio_base_64: &encoded,
        commit,
        sample_rate: SAMPLE_RATE,
        previous_text,
    })
    .expect("a struct of strings and numbers always serialises")
}

/// Converts Handy-style f32 frames (nominally -1.0..1.0) to little-endian PCM16.
pub fn f32_to_pcm16_bytes(samples: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        // Clamping first: an out-of-range float must clip, not wrap around.
        let value = (sample.clamp(-1.0, 1.0) * f32::from(i16::MAX)).round() as i16;
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

pub(crate) fn i16_to_pcm16_bytes(samples: &[i16]) -> Vec<u8> {
    samples.iter().flat_map(|s| s.to_le_bytes()).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerMessage {
    SessionStarted {
        session_id: String,
    },
    Partial(String),
    Committed(String),
    Edited {
        text: String,
        edited: String,
    },
    Warning(String),
    Error {
        code: String,
        message: String,
    },
    /// Frames this client does not use (final_transcript, entities, ...).
    Ignored(String),
}

pub(crate) fn parse_server_message(raw: &str) -> Result<ServerMessage, serde_json::Error> {
    let value: Value = serde_json::from_str(raw)?;
    let field = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let kind = field("message_type");
    Ok(match kind.as_str() {
        "session_started" => ServerMessage::SessionStarted {
            session_id: field("session_id"),
        },
        "partial_transcript" => ServerMessage::Partial(field("text")),
        "committed_transcript" | "committed_transcript_with_timestamps" => {
            ServerMessage::Committed(field("text"))
        }
        "edited_transcript" => ServerMessage::Edited {
            text: field("text"),
            edited: field("edited_text"),
        },
        "warning" => ServerMessage::Warning(field("warning")),
        code if ERROR_TYPES.contains(&code) => ServerMessage::Error {
            code: code.to_string(),
            message: field("error"),
        },
        _ => ServerMessage::Ignored(kind),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_serialises_with_previous_text_only_when_given() {
        let first: Value =
            serde_json::from_str(&audio_chunk_json(&[1, 0, 2, 0], false, Some("hi"))).unwrap();
        assert_eq!(first["message_type"], "input_audio_chunk");
        assert_eq!(first["audio_base_64"], "AQACAA==");
        assert_eq!(first["commit"], false);
        assert_eq!(first["sample_rate"], 16000);
        assert_eq!(first["previous_text"], "hi");

        let later: Value = serde_json::from_str(&audio_chunk_json(&[], true, None)).unwrap();
        assert_eq!(later["commit"], true);
        assert_eq!(later["audio_base_64"], "");
        assert!(later.get("previous_text").is_none());
    }

    #[test]
    fn f32_converts_clips_and_keeps_little_endian_order() {
        let bytes = f32_to_pcm16_bytes(&[0.0, 1.0, -1.0, 2.5, -2.5, 0.5]);
        let values: Vec<i16> = bytes
            .chunks(2)
            .map(|c| i16::from_le_bytes([c[0], c[1]]))
            .collect();
        assert_eq!(values, vec![0, 32767, -32767, 32767, -32767, 16384]);
    }

    #[test]
    fn i16_converts_little_endian() {
        assert_eq!(i16_to_pcm16_bytes(&[1, -2]), vec![1, 0, 0xFE, 0xFF]);
    }

    #[test]
    fn parses_session_started() {
        let msg = parse_server_message(
            r#"{"message_type":"session_started","session_id":"s1","config":{"x":1}}"#,
        )
        .unwrap();
        assert_eq!(
            msg,
            ServerMessage::SessionStarted {
                session_id: "s1".into()
            }
        );
    }

    #[test]
    fn parses_transcripts() {
        assert_eq!(
            parse_server_message(r#"{"message_type":"partial_transcript","text":"sal"}"#).unwrap(),
            ServerMessage::Partial("sal".into())
        );
        assert_eq!(
            parse_server_message(r#"{"message_type":"committed_transcript","text":"salom"}"#)
                .unwrap(),
            ServerMessage::Committed("salom".into())
        );
        let with_timestamps = r#"{"message_type":"committed_transcript_with_timestamps","text":"salom","language_code":"uzb","words":[]}"#;
        assert_eq!(
            parse_server_message(with_timestamps).unwrap(),
            ServerMessage::Committed("salom".into())
        );
    }

    #[test]
    fn parses_edited_and_warning() {
        let edited =
            r#"{"message_type":"edited_transcript","text":"salom","edited_text":"Salom."}"#;
        assert_eq!(
            parse_server_message(edited).unwrap(),
            ServerMessage::Edited {
                text: "salom".into(),
                edited: "Salom.".into()
            }
        );
        assert_eq!(
            parse_server_message(r#"{"message_type":"warning","warning":"slow audio"}"#).unwrap(),
            ServerMessage::Warning("slow audio".into())
        );
    }

    #[test]
    fn parses_every_error_type() {
        for code in ERROR_TYPES {
            let raw = format!(r#"{{"message_type":"{code}","error":"boom"}}"#);
            assert_eq!(
                parse_server_message(&raw).unwrap(),
                ServerMessage::Error {
                    code: code.to_string(),
                    message: "boom".into()
                },
                "{code}"
            );
        }
    }

    #[test]
    fn unknown_types_are_ignored_not_fatal() {
        assert_eq!(
            parse_server_message(r#"{"message_type":"final_transcript","text":"x"}"#).unwrap(),
            ServerMessage::Ignored("final_transcript".into())
        );
    }

    #[test]
    fn garbage_is_a_parse_error() {
        assert!(parse_server_message("not json").is_err());
    }
}
