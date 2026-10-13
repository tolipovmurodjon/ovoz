use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::config::ScribeConfig;
use crate::error::{Result, ScribeError};
use crate::protocol::{audio_chunk_json, f32_to_pcm16_bytes, parse_server_message, ServerMessage};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Audio goes out in 100 ms frames; the vendor asks for 0.1 to 1 s per chunk. Nothing
/// larger is ever sent in one frame, so `chunk_size_exceeded` cannot happen.
const CHUNK_SAMPLES: usize = 1600;
/// The server ignores a commit with under 0.3 s of uncommitted audio and then closes the
/// socket (measured: 0.20 s and 0.00 s both did). 0.325 s was accepted, so `finish` pads
/// short tails with silence up to this.
const MIN_COMMIT_SAMPLES: u64 = 5200;
/// The server commits by itself at about 36 s of uncommitted audio. Past this much, an
/// automatic commit may still be on its way when `finish` commits, and it arrives first.
const AUTO_COMMIT_RISK_SAMPLES: u64 = 33 * 16_000;
/// Typical commit reply time is 350 ms, so a second frame is on the wire within this.
const AUTO_COMMIT_GRACE: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScribeEvent {
    Partial(String),
    /// A finished segment. Long holds produce several before `finish`.
    Committed(String),
    /// Non-fatal server notes, including "nothing to transcribe" replies.
    Warning(String),
    Error(ScribeError),
}

/// What `finish` returns. The text is every segment committed in the session, so an
/// error never discards text that already arrived.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Transcript {
    pub text: String,
    /// Set when the final commit could not be completed: the text is then missing its
    /// last stretch. `None` means the transcript is complete.
    pub error: Option<ScribeError>,
}

/// What `finish` waits on. A watch channel keeps the latest state, so a reply that
/// arrives before `finish` starts waiting is still seen.
#[derive(Debug, Default, Clone)]
struct Progress {
    committed: Vec<String>,
    /// First fatal error. Sticky: the session is unusable once it is set.
    fatal: Option<ScribeError>,
    /// Latest non-fatal error and a counter, so `finish` reacts only to errors that
    /// arrive after its own commit.
    last_error: Option<ScribeError>,
    error_seq: u64,
    /// Replies meaning "nothing to transcribe" (counted, never an error).
    empty_replies: u64,
}

/// Sample counters the caller thread and the session task both read.
#[derive(Default)]
struct Shared {
    /// Samples handed to `send_audio` so far.
    accepted: AtomicU64,
    /// `accepted` as it was when the latest committed segment arrived: a lower bound of
    /// the audio the server has not committed yet.
    accepted_at_last_commit: AtomicU64,
}

pub struct ScribeSession {
    handle: Handle,
    frames: mpsc::UnboundedSender<String>,
    progress: watch::Receiver<Progress>,
    shared: Arc<Shared>,
    /// Samples not yet a full frame.
    pending: Vec<f32>,
    accepted: u64,
    previous_text: Option<String>,
    finish_timeout: Duration,
    task: JoinHandle<()>,
}

impl ScribeSession {
    /// Opens the websocket and waits for `session_started`, all within `connect_timeout`.
    /// There is no retry: a failed connect is reported once and the caller decides.
    ///
    /// The session runs as a task spawned on `handle`, so that runtime must keep running
    /// worker threads for as long as the session lives. A current-thread runtime that
    /// only does `block_on(connect)` would upload nothing until `finish`. The calling
    /// pattern for a plain `std::thread` (no runtime of its own) is:
    ///
    /// ```no_run
    /// # use scribe_client::{ScribeConfig, ScribeSession};
    /// // Once, at app start: a multi-thread runtime kept alive in app state.
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .worker_threads(1)
    ///     .enable_all()
    ///     .build()
    ///     .unwrap();
    ///
    /// // On the worker thread, per hold:
    /// # let key = String::new();
    /// # let frame = [0.0f32; 256];
    /// let (mut session, events) =
    ///     ScribeSession::connect_blocking(runtime.handle(), ScribeConfig::new(key)).unwrap();
    /// // `events` is a tokio unbounded receiver: `try_recv()` or `blocking_recv()` from std.
    /// while session.send_audio(&frame).is_ok() { /* until the key is released */ break; }
    /// let transcript = session.finish_blocking();
    /// println!("{} {:?}", transcript.text, transcript.error);
    /// ```
    pub async fn connect(
        handle: &Handle,
        config: ScribeConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<ScribeEvent>)> {
        let socket = timeout(config.connect_timeout, open_socket(&config))
            .await
            .map_err(|_| ScribeError::Timeout("connecting"))??;

        let (frames, frame_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = mpsc::unbounded_channel();
        let (progress_tx, progress) = watch::channel(Progress::default());
        let shared = Arc::new(Shared::default());
        let task = handle.spawn(run_session(
            socket,
            frame_rx,
            event_tx,
            progress_tx,
            shared.clone(),
            config.send_timeout,
        ));

        let session = Self {
            handle: handle.clone(),
            frames,
            progress,
            shared,
            pending: Vec::new(),
            accepted: 0,
            previous_text: config.previous_text,
            finish_timeout: config.finish_timeout,
            task,
        };
        Ok((session, events))
    }

    /// [`connect`](Self::connect) for a thread that is not inside a runtime. Panics if
    /// called from async code; use `connect` there.
    pub fn connect_blocking(
        handle: &Handle,
        config: ScribeConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<ScribeEvent>)> {
        handle.block_on(Self::connect(handle, config))
    }

    /// Accepts f32 frames (16 kHz mono) of any size and never blocks. Samples are
    /// regrouped into 100 ms frames. Once the session is dead this returns the error
    /// that killed it, so the caller learns during the hold, not at the key release.
    pub fn send_audio(&mut self, samples: &[f32]) -> Result<()> {
        if let Some(error) = self.progress.borrow().fatal.clone() {
            return Err(error);
        }
        self.accepted += samples.len() as u64;
        self.shared.accepted.store(self.accepted, Ordering::Relaxed);
        self.pending.extend_from_slice(samples);

        let whole = self.pending.len() / CHUNK_SAMPLES * CHUNK_SAMPLES;
        let frames: Vec<String> = self.pending[..whole]
            .chunks(CHUNK_SAMPLES)
            .map(|chunk| {
                let previous_text = self.previous_text.take();
                audio_chunk_json(&f32_to_pcm16_bytes(chunk), false, previous_text.as_deref())
            })
            .collect();
        self.pending.drain(..whole);
        frames.into_iter().try_for_each(|frame| self.queue(frame))
    }

    /// Commits the last stretch, waits for its transcript and returns every committed
    /// segment of the session joined with spaces. A hold that never sent audio returns
    /// empty text at once, without a round trip. Ends the session.
    pub async fn finish(mut self) -> Transcript {
        if self.accepted == 0 {
            return Transcript::default();
        }
        let before = self.progress.borrow().clone();
        let uncommitted = self
            .accepted
            .saturating_sub(self.shared.accepted_at_last_commit.load(Ordering::Relaxed));
        let auto_commit_may_be_in_flight = uncommitted >= AUTO_COMMIT_RISK_SAMPLES;

        let mut tail = std::mem::take(&mut self.pending);
        let padding = MIN_COMMIT_SAMPLES.saturating_sub(uncommitted) as usize;
        tail.resize(tail.len() + padding, 0.0);
        let commit = audio_chunk_json(
            &f32_to_pcm16_bytes(&tail),
            true,
            self.previous_text.take().as_deref(),
        );
        if let Err(error) = self.queue(commit) {
            return self.transcript_with(error);
        }

        let wait = self.progress.wait_for(|p| {
            p.committed.len() > before.committed.len()
                || p.error_seq > before.error_seq
                || p.empty_replies > before.empty_replies
                || p.fatal.is_some()
        });
        let waited = timeout(self.finish_timeout, wait)
            .await
            .map(|reached| reached.map(|state| state.clone()));
        let mut state = match waited {
            Ok(Ok(state)) => state,
            Ok(Err(_)) => self.progress.borrow().clone(),
            Err(_) => {
                return self
                    .transcript_with(ScribeError::Timeout("waiting for the final transcript"))
            }
        };

        if auto_commit_may_be_in_flight && state.committed.len() > before.committed.len() {
            let seen = state.committed.len();
            let more = self.progress.wait_for(|p| p.committed.len() > seen);
            if let Ok(Ok(later)) = timeout(AUTO_COMMIT_GRACE, more)
                .await
                .map(|reached| reached.map(|state| state.clone()))
            {
                state = later;
            }
        }

        let answered = state.committed.len() > before.committed.len()
            || state.empty_replies > before.empty_replies;
        let error = if answered {
            None
        } else if state.error_seq > before.error_seq {
            state.last_error
        } else {
            state.fatal
        };
        Transcript {
            text: join_segments(&state.committed),
            error,
        }
    }

    /// [`finish`](Self::finish) for a thread that is not inside a runtime. Panics if
    /// called from async code; use `finish` there.
    pub fn finish_blocking(self) -> Transcript {
        let handle = self.handle.clone();
        handle.block_on(self.finish())
    }

    fn queue(&self, frame: String) -> Result<()> {
        self.frames.send(frame).map_err(|_| self.dead_error())
    }

    fn dead_error(&self) -> ScribeError {
        self.progress
            .borrow()
            .fatal
            .clone()
            .unwrap_or(ScribeError::Closed {
                code: 1006,
                reason: "session task ended".into(),
            })
    }

    fn transcript_with(&self, error: ScribeError) -> Transcript {
        Transcript {
            text: join_segments(&self.progress.borrow().committed),
            error: Some(error),
        }
    }
}

/// Dropping a session stops everything at once: the task is aborted, the socket is
/// closed without a handshake and audio still queued is never uploaded.
impl Drop for ScribeSession {
    fn drop(&mut self) {
        self.task.abort();
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
    let mut key = HeaderValue::from_str(&config.api_key).map_err(|_| {
        ScribeError::Auth("API key contains characters not allowed in a header".into())
    })?;
    // tungstenite logs the whole upgrade request at trace level; a sensitive value
    // prints as "Sensitive" there instead of the key.
    key.set_sensitive(true);
    request.headers_mut().insert("xi-api-key", key);

    // `true` disables Nagle's algorithm: 100 ms frames and the commit must not wait
    // for a delayed ACK.
    let (mut socket, _) = tokio_tungstenite::connect_async_with_config(request, None, true)
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
            Some(Ok(Message::Close(frame))) => return Err(closed_error(frame)),
            None => return Err(closed_error(None)),
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

fn closed_error(frame: Option<CloseFrame>) -> ScribeError {
    match frame {
        Some(frame) => ScribeError::Closed {
            code: frame.code.into(),
            reason: frame.reason.to_string(),
        },
        None => ScribeError::Closed {
            code: CloseCode::Abnormal.into(),
            reason: "no close frame".into(),
        },
    }
}

async fn run_session(
    mut socket: Socket,
    mut frames: mpsc::UnboundedReceiver<String>,
    events: mpsc::UnboundedSender<ScribeEvent>,
    progress: watch::Sender<Progress>,
    shared: Arc<Shared>,
    send_timeout: Duration,
) {
    let ended = loop {
        tokio::select! {
            outgoing = frames.recv() => match outgoing {
                Some(frame) => {
                    match timeout(send_timeout, socket.send(Message::Text(frame.into()))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => break ScribeError::Network(e.to_string()),
                        Err(_) => break ScribeError::Network("sending audio timed out".into()),
                    }
                }
                None => break closed_error(None),
            },
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => match parse_server_message(text.as_str()) {
                    Ok(message) => handle_server_message(message, &events, &progress, &shared),
                    Err(e) => log::warn!("unparseable scribe frame: {e}"),
                },
                Some(Ok(Message::Close(frame))) => break closed_error(frame),
                None => break closed_error(None),
                Some(Ok(_)) => {}
                Some(Err(e)) => break ScribeError::Network(e.to_string()),
            },
        }
    };
    report_error(&events, &progress, ended);
}

fn report_error(
    events: &mpsc::UnboundedSender<ScribeEvent>,
    progress: &watch::Sender<Progress>,
    error: ScribeError,
) {
    let _ = events.send(ScribeEvent::Error(error.clone()));
    progress.send_modify(|p| {
        if error.is_fatal() {
            // The first fatal error is the cause, later ones are fallout.
            p.fatal = p.fatal.take().or(Some(error));
        } else {
            p.error_seq += 1;
            p.last_error = Some(error);
        }
    });
}

fn handle_server_message(
    message: ServerMessage,
    events: &mpsc::UnboundedSender<ScribeEvent>,
    progress: &watch::Sender<Progress>,
    shared: &Shared,
) {
    match message {
        ServerMessage::Partial(text) => {
            let _ = events.send(ScribeEvent::Partial(text));
        }
        ServerMessage::Committed(text) => {
            shared
                .accepted_at_last_commit
                .store(shared.accepted.load(Ordering::Relaxed), Ordering::Relaxed);
            progress.send_modify(|p| p.committed.push(text.clone()));
            let _ = events.send(ScribeEvent::Committed(text));
        }
        ServerMessage::Warning(text) => {
            let _ = events.send(ScribeEvent::Warning(text));
        }
        ServerMessage::Error { code, message } => {
            let error = ScribeError::from_server(&code, message);
            if error.is_nothing_to_transcribe() {
                progress.send_modify(|p| p.empty_replies += 1);
                let _ = events.send(ScribeEvent::Warning(error.to_string()));
            } else {
                report_error(events, progress, error);
            }
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
