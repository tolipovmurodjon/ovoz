use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Serialize;
use serde_json::Value;

pub(crate) const SAMPLE_RATE: u32 = 16_000;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerMessage {
    SessionStarted {
        session_id: String,
    },
    Partial(String),
    Committed(String),
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
        // The crate never asks for timestamps, so `committed_transcript_with_timestamps`
        // would only be a second copy of a segment already delivered.
        "committed_transcript" => ServerMessage::Committed(field("text")),
        "warning" => ServerMessage::Warning(field("warning")),
        // The `error` field, not a list of known types, decides: the vendor adds types.
        _ => match value.get("error") {
            Some(error) => ServerMessage::Error {
                code: kind,
                message: error
                    .as_str()
                    .map_or_else(|| error.to_string(), str::to_string),
            },
            None => ServerMessage::Ignored(kind),
        },
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
    }

    #[test]
    fn timestamped_commit_is_ignored_so_a_segment_is_not_delivered_twice() {
        let with_timestamps = r#"{"message_type":"committed_transcript_with_timestamps","text":"salom","language_code":"uzb","words":[]}"#;
        assert_eq!(
            parse_server_message(with_timestamps).unwrap(),
            ServerMessage::Ignored("committed_transcript_with_timestamps".into())
        );
    }

    #[test]
    fn parses_warning() {
        assert_eq!(
            parse_server_message(r#"{"message_type":"warning","warning":"slow audio"}"#).unwrap(),
            ServerMessage::Warning("slow audio".into())
        );
    }

    #[test]
    fn any_frame_with_an_error_field_is_an_error_even_if_the_type_is_new() {
        for code in ["auth_error", "commit_throttled", "brand_new_error_type"] {
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
