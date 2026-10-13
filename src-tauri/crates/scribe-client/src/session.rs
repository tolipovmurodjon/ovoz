use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::config::ScribeConfig;
use crate::error::{Result, ScribeError};
use crate::protocol::{
    audio_chunk_json, f32_to_pcm16_bytes, i16_to_pcm16_bytes, parse_server_message, ServerMessage,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const CLOSE_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScribeEvent {
    Partial(String),
    Committed(String),
    /// Result of `transcript_edit`: `text` is the original, `edited` the rewritten text.
    Edited {
        text: String,
        edited: String,
    },
    Warning(String),
    Error(ScribeError),
}

/// What `finish` waits on. A watch channel keeps the latest state, so a commit that
/// arrives before `finish` starts waiting is still seen.
#[derive(Debug, Default, Clone)]
struct Progress {
    committed: Vec<String>,
    error: Option<ScribeError>,
    closed: bool,
}

pub struct ScribeSession {
    frames: mpsc::UnboundedSender<String>,
    events: Option<mpsc::UnboundedReceiver<ScribeEvent>>,
    progress: watch::Receiver<Progress>,
    previous_text: Option<String>,
    finish_timeout: Duration,
    _task: JoinHandle<()>,
}

impl ScribeSession {
    /// Opens the websocket and waits for `session_started`, all within `connect_timeout`.
    /// There is no retry: a failed connect is reported once and the caller decides.
    pub async fn connect(config: ScribeConfig) -> Result<Self> {
        let socket = timeout(config.connect_timeout, open_socket(&config))
            .await
            .map_err(|_| ScribeError::Timeout("connecting"))??;

        let (frames, frame_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = mpsc::unbounded_channel();
        let (progress_tx, progress) = watch::channel(Progress::default());
        let task = tokio::spawn(run_session(socket, frame_rx, event_tx, progress_tx));

        Ok(Self {
            frames,
            events: Some(events),
            progress,
            previous_text: config.previous_text,
            finish_timeout: config.finish_timeout,
            _task: task,
        })
    }

    /// Takes the event stream. Call once; events are not buffered for a second receiver.
    pub fn take_events(&mut self) -> Option<mpsc::UnboundedReceiver<ScribeEvent>> {
        self.events.take()
    }

    /// Accepts the f32 frames (16 kHz mono) Handy's recorder produces.
    pub fn send_audio(&mut self, samples: &[f32]) -> Result<()> {
        self.send_pcm(&f32_to_pcm16_bytes(samples), false)
    }

    pub fn send_audio_i16(&mut self, samples: &[i16]) -> Result<()> {
        self.send_pcm(&i16_to_pcm16_bytes(samples), false)
    }

    /// Commits the segment, waits for the transcript of that commit and returns every
    /// committed segment of the session joined with spaces.
    pub async fn finish(&mut self) -> Result<String> {
        let already_committed = self.progress.borrow().committed.len();
        self.send_pcm(&[], true)?;

        let wait = self
            .progress
            .wait_for(|p| p.committed.len() > already_committed || p.error.is_some() || p.closed);
        let state = timeout(self.finish_timeout, wait)
            .await
            .map_err(|_| ScribeError::Timeout("waiting for the final transcript"))?
            .map_err(|_| ScribeError::Closed)?
            .clone();

        if state.committed.len() > already_committed {
            return Ok(join_segments(&state.committed));
        }
        match state.error {
            // With VAD commits everything may already be committed; the extra commit then
            // has nothing to transcribe, which is not a failure.
            Some(ScribeError::Server { code, .. })
                if code == "insufficient_audio_activity" && !state.committed.is_empty() =>
            {
                Ok(join_segments(&state.committed))
            }
            Some(error) => Err(error),
            None => Err(ScribeError::Closed),
        }
    }

    fn send_pcm(&mut self, pcm: &[u8], commit: bool) -> Result<()> {
        let frame = audio_chunk_json(pcm, commit, self.previous_text.take().as_deref());
        self.frames.send(frame).map_err(|_| {
            self.progress
                .borrow()
                .error
                .clone()
                .unwrap_or(ScribeError::Closed)
        })
    }
}

fn join_segments(segments: &[String]) -> String {
    segments
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

async fn open_socket(config: &ScribeConfig) -> Result<Socket> {
    let mut request = config
        .websocket_url()
        .as_str()
        .into_client_request()
        .map_err(|e| ScribeError::Network(e.to_string()))?;
    let key = HeaderValue::from_str(&config.api_key).map_err(|_| {
        ScribeError::Auth("API key contains characters not allowed in a header".into())
    })?;
    request.headers_mut().insert("xi-api-key", key);

    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(map_handshake_error)?;

    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => match parse_server_message(text.as_str()) {
                Ok(ServerMessage::SessionStarted { .. }) => return Ok(socket),
                Ok(ServerMessage::Error { code, message }) => {
                    return Err(ScribeError::from_server(&code, message))
                }
                _ => {}
            },
            Some(Ok(Message::Close(_))) | None => {
                return Err(ScribeError::Network(
                    "closed before the session started".into(),
                ))
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(ScribeError::Network(e.to_string())),
        }
    }
}

fn map_handshake_error(error: WsError) -> ScribeError {
    match &error {
        WsError::Http(response) => match response.status().as_u16() {
            401 | 403 => ScribeError::Auth(format!("HTTP {}", response.status())),
            429 => ScribeError::RateLimited {
                code: "http_429".into(),
                message: "too many requests".into(),
            },
            status => ScribeError::Network(format!("HTTP {status} during handshake")),
        },
        _ => ScribeError::Network(error.to_string()),
    }
}

async fn run_session(
    mut socket: Socket,
    mut frames: mpsc::UnboundedReceiver<String>,
    events: mpsc::UnboundedSender<ScribeEvent>,
    progress: watch::Sender<Progress>,
) {
    let fail = |error: ScribeError| {
        let _ = events.send(ScribeEvent::Error(error.clone()));
        progress.send_modify(|p| p.error = p.error.take().or(Some(error)));
    };

    loop {
        tokio::select! {
            outgoing = frames.recv() => match outgoing {
                Some(frame) => {
                    if let Err(e) = socket.send(Message::Text(frame.into())).await {
                        fail(ScribeError::Network(e.to_string()));
                        break;
                    }
                }
                // The session handle was dropped: close politely, but never hang on it.
                None => {
                    let _ = timeout(CLOSE_GRACE, socket.close(None)).await;
                    break;
                }
            },
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => match parse_server_message(text.as_str()) {
                    Ok(message) => handle_server_message(message, &events, &progress),
                    Err(e) => log::warn!("unparseable scribe frame: {e}"),
                },
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    fail(ScribeError::Network(e.to_string()));
                    break;
                }
            },
        }
    }
    progress.send_modify(|p| p.closed = true);
}

fn handle_server_message(
    message: ServerMessage,
    events: &mpsc::UnboundedSender<ScribeEvent>,
    progress: &watch::Sender<Progress>,
) {
    match message {
        ServerMessage::Partial(text) => {
            let _ = events.send(ScribeEvent::Partial(text));
        }
        ServerMessage::Committed(text) => {
            progress.send_modify(|p| p.committed.push(text.clone()));
            let _ = events.send(ScribeEvent::Committed(text));
        }
        ServerMessage::Edited { text, edited } => {
            let _ = events.send(ScribeEvent::Edited { text, edited });
        }
        ServerMessage::Warning(text) => {
            let _ = events.send(ScribeEvent::Warning(text));
        }
        ServerMessage::Error { code, message } => {
            let error = ScribeError::from_server(&code, message);
            let _ = events.send(ScribeEvent::Error(error.clone()));
            // Only the first error is kept: it is the cause, later ones are fallout.
            progress.send_modify(|p| p.error = p.error.take().or(Some(error)));
        }
        ServerMessage::SessionStarted { .. } | ServerMessage::Ignored(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_skips_empty_segments_and_trims() {
        let segments = vec![" hello ".to_string(), "".to_string(), "world".to_string()];
        assert_eq!(join_segments(&segments), "hello world");
    }
}
