use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
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
/// The server throttles a commit with under 0.3 s of uncommitted audio and then closes the
/// socket (measured: 0.20 s and 0.00 s both did). 0.325 s was accepted, and audio inside
/// the commit frame itself counts toward it (measured), so `finish` pads short tails with
/// silence up to this.
const MIN_COMMIT_SAMPLES: u64 = 5200;
/// The server commits by itself at about 35.7 s of uncommitted audio (measured twice: the
/// automatic commit arrived when 35.8 s had been sent; it counts audio seconds, not
/// wall-clock time). Past this much, an automatic commit may still be on its way when
/// `finish` commits, and it arrives first.
const AUTO_COMMIT_RISK_SAMPLES: u64 = 33 * 16_000;
/// At this much uncommitted audio (35.7 s) the automatic commit has certainly fired, so
/// two commit replies are due, not one.
const AUTO_COMMIT_SURE_SAMPLES: u64 = 571_200;
/// `accepted_at_last_commit` is taken when a commit reply is processed, which is later than
/// the server's real cut by the reply lag (about 0.3 s measured, longer on a bad network).
/// From the second cycle on the client therefore counts less uncommitted audio than the
/// server holds, so the "sure" threshold is lowered by this much.
const REPLY_LAG_CUSHION_SAMPLES: u64 = 16_000;
/// When the automatic commit and ours collide, our reply follows theirs by 44 to 176 ms
/// (measured), and a lone reply takes about 300 ms, so the second frame is on the wire
/// well within this.
const AUTO_COMMIT_GRACE: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScribeEvent {
    /// The socket is open and the server has accepted the session. Audio fed before this
    /// has been waiting in the queue and now starts to flow.
    Ready,
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
    /// `Some` in every case where the text may be incomplete; `None` only when the text is
    /// known to be complete. The caller should then fall back to its own recording.
    pub error: Option<ScribeError>,
}

/// What `finish` waits on. A watch channel keeps the latest state, so a reply that
/// arrives before `finish` starts waiting is still seen.
#[derive(Debug, Default, Clone)]
struct Progress {
    /// The socket is open and `session_started` has arrived.
    connected: bool,
    committed: Vec<String>,
    /// Samples handed to `send_audio` when the latest committed segment arrived: a lower
    /// bound of the audio the server has not committed yet. Lives here, next to
    /// `committed`, so `finish` reads both in one consistent snapshot.
    accepted_at_last_commit: u64,
    /// First fatal error. Sticky: the session is unusable once it is set.
    fatal: Option<ScribeError>,
    /// Latest non-fatal error since the session started, and a counter so `finish` can
    /// wait for one that arrives after its commit. Any such error means some audio may not
    /// have been transcribed, so it taints the whole transcript, not only the last commit.
    last_error: Option<ScribeError>,
    error_seq: u64,
    /// Replies meaning "nothing to transcribe" (counted, never an error).
    empty_replies: u64,
}

impl Progress {
    /// Server replies to commits since `since`: a transcript or "nothing to transcribe".
    fn answers_since(&self, since: &Progress) -> usize {
        (self.committed.len() - since.committed.len())
            + (self.empty_replies - since.empty_replies) as usize
    }

    /// The error that makes the text untrustworthy: the latest non-fatal one since the
    /// session started, else the fatal one.
    fn failure(&self) -> Option<ScribeError> {
        self.last_error.clone().or_else(|| self.fatal.clone())
    }

    fn has_news_since(&self, since: &Progress) -> bool {
        self.answers_since(since) > 0 || self.error_seq > since.error_seq || self.fatal.is_some()
    }
}

pub struct ScribeSession {
    handle: Handle,
    frames: mpsc::UnboundedSender<String>,
    progress: watch::Receiver<Progress>,
    /// Samples the server has been or will be given: those handed to `send_audio` plus the
    /// idle silence the session task adds. Shared with the task.
    shared_accepted: Arc<AtomicU64>,
    /// Samples not yet a full frame.
    pending: Vec<f32>,
    /// Samples handed to `send_audio` by the caller only.
    accepted: u64,
    previous_text: Option<String>,
    started: Instant,
    connect_timeout: Duration,
    finish_timeout: Duration,
    task: JoinHandle<()>,
}

impl ScribeSession {
    /// Begins a session and returns at once without touching the network: call it at key
    /// press. One task on `handle` connects (within `connect_timeout`, no retry) and then
    /// runs the session. Audio passed to [`send_audio`](Self::send_audio) meanwhile waits
    /// in the queue and goes out in order once the socket is up, so a short tap is not
    /// cut by the half second the connection takes. [`ScribeEvent::Ready`] marks the
    /// moment it is up. A connect failure (auth, timeout, network, an error frame before
    /// `session_started`) is recorded as fatal: `send_audio` and `finish` both report it.
    /// Dropping the session cancels a connect still in flight.
    ///
    /// The session runs as a task on `handle`, so that runtime must keep running worker
    /// threads for as long as the session lives. The calling pattern for a plain
    /// `std::thread` (no runtime of its own) is:
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
    /// // On the worker thread, per hold. At key press:
    /// # let key = String::new();
    /// # let frame = [0.0f32; 256];
    /// let (mut session, events) =
    ///     ScribeSession::start(runtime.handle(), ScribeConfig::new(key));
    /// // `events` is a tokio unbounded receiver: `try_recv()` or `blocking_recv()` from std.
    /// // While the key is held, for every frame the recorder delivers:
    /// while session.send_audio(&frame).is_ok() { /* until the key is released */ break; }
    /// // At key release:
    /// let transcript = session.finish_blocking();
    /// println!("{} {:?}", transcript.text, transcript.error);
    /// ```
    pub fn start(
        handle: &Handle,
        config: ScribeConfig,
    ) -> (Self, mpsc::UnboundedReceiver<ScribeEvent>) {
        let (frames, frame_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = mpsc::unbounded_channel();
        let (progress_tx, progress) = watch::channel(Progress::default());
        let shared_accepted = Arc::new(AtomicU64::new(0));
        let previous_text = config.previous_text.clone();
        let connect_timeout = config.connect_timeout;
        let finish_timeout = config.finish_timeout;
        let task = handle.spawn(run_task(
            config,
            frame_rx,
            event_tx,
            progress_tx,
            shared_accepted.clone(),
        ));

        let session = Self {
            handle: handle.clone(),
            frames,
            progress,
            shared_accepted,
            pending: Vec::new(),
            accepted: 0,
            previous_text,
            started: Instant::now(),
            connect_timeout,
            finish_timeout,
            task,
        };
        (session, events)
    }

    /// Accepts f32 frames (16 kHz mono) of any size and never blocks. Samples are
    /// regrouped into 100 ms frames. Once the session is dead this returns the error
    /// that killed it, so the caller learns during the hold, not at the key release. A
    /// caller may leave out silence: after 8 s without outgoing audio the session sends
    /// 100 ms of silence itself, because the server hangs up on a socket idle for ~15 s.
    pub fn send_audio(&mut self, samples: &[f32]) -> Result<()> {
        // Counted even when the session is dead, so `finish` still reports the failure for
        // audio the caller offered instead of returning a clean empty transcript.
        self.accepted += samples.len() as u64;
        self.shared_accepted
            .fetch_add(samples.len() as u64, Ordering::Relaxed);
        if let Some(error) = self.progress.borrow().fatal.clone() {
            return Err(error);
        }
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
    ///
    /// `Transcript.error` is `Some` whenever the text may be incomplete: a failed
    /// connect, a refused, failed or unanswered commit, a dead session, an automatic
    /// commit whose companion reply never came, or any non-fatal server error at any point
    /// of the session (the latest one is reported; a false alarm is acceptable, silent
    /// loss is not).
    ///
    /// Time budget: the commit round trip gets `finish_timeout`. A session that is still
    /// connecting first waits for what remains of `connect_timeout`, counted from
    /// [`start`](Self::start). A release with 33 to 36 s of uncommitted audio can cost up
    /// to 500 ms more: the server's automatic commit may arrive first, so `finish` waits
    /// for a second reply before it calls the transcript complete.
    pub async fn finish(mut self) -> Transcript {
        if self.accepted == 0 {
            return Transcript::default();
        }

        let connect_left = self.connect_timeout.saturating_sub(self.started.elapsed());
        let connected = timeout(
            connect_left,
            self.progress.wait_for(|p| p.connected || p.fatal.is_some()),
        )
        .await
        .map(|reached| reached.map(|state| state.clone()));
        match connected {
            Err(_) => return self.transcript_with(ScribeError::Timeout("connecting")),
            Ok(Ok(state)) => {
                if let Some(fatal) = state.fatal {
                    return self.transcript_with(fatal);
                }
            }
            // The task is gone; queueing the commit below reports how it ended.
            Ok(Err(_)) => {}
        }

        let before = self.progress.borrow().clone();
        let uncommitted = self
            .shared_accepted
            .load(Ordering::Relaxed)
            .saturating_sub(before.accepted_at_last_commit);
        let auto_commit_may_be_in_flight = uncommitted >= AUTO_COMMIT_RISK_SAMPLES;

        let mut tail = std::mem::take(&mut self.pending);
        // With an automatic commit possibly in flight, the server may already have taken
        // everything before this frame, so the frame must be a valid commit on its own.
        let counted = if auto_commit_may_be_in_flight {
            tail.len() as u64
        } else {
            uncommitted
        };
        let padding = MIN_COMMIT_SAMPLES.saturating_sub(counted);
        tail.resize(tail.len() + padding as usize, 0.0);
        let sure_threshold = if before.accepted_at_last_commit > 0 {
            AUTO_COMMIT_SURE_SAMPLES - REPLY_LAG_CUSHION_SAMPLES
        } else {
            AUTO_COMMIT_SURE_SAMPLES
        };
        let second_reply_due = uncommitted + padding >= sure_threshold;
        let commit = audio_chunk_json(
            &f32_to_pcm16_bytes(&tail),
            true,
            self.previous_text.take().as_deref(),
        );
        if let Err(error) = self.queue(commit) {
            return self.transcript_with(error);
        }

        let Some(mut state) = self.wait_for_news(&before, self.finish_timeout).await else {
            return self.transcript_with(ScribeError::Timeout("waiting for the final transcript"));
        };
        if auto_commit_may_be_in_flight
            && state.answers_since(&before) == 1
            && state.failure().is_none()
        {
            let later = self.wait_for_news(&state, AUTO_COMMIT_GRACE).await;
            if let Some(later) = later {
                state = later;
            }
        }

        let answers = state.answers_since(&before);
        let complete = answers >= 2 || (answers == 1 && !auto_commit_may_be_in_flight);
        // A failure outranks "complete": it may have arrived with the reply or earlier in the
        // hold, and either way some audio may be missing from the text.
        let error = state.failure().or_else(|| {
            if complete {
                None
            } else if answers == 0 {
                // Nothing answered and nothing failed: the task ended without saying why.
                Some(session_task_ended())
            } else {
                // One reply, and it may be the automatic commit with ours still missing.
                second_reply_due.then_some(ScribeError::Timeout(
                    "waiting for the commit after an automatic one",
                ))
            }
        });
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

    /// The state once something new happened since `since`, or `None` if `limit` passes
    /// first. If the task has ended, returns the last state it left.
    async fn wait_for_news(&mut self, since: &Progress, limit: Duration) -> Option<Progress> {
        let waited = timeout(limit, self.progress.wait_for(|p| p.has_news_since(since)))
            .await
            .map(|reached| reached.map(|state| state.clone()));
        match waited {
            Ok(Ok(state)) => Some(state),
            Ok(Err(_)) => Some(self.progress.borrow().clone()),
            Err(_) => None,
        }
    }

    fn queue(&self, frame: String) -> Result<()> {
        self.frames.send(frame).map_err(|_| self.dead_error())
    }

    fn dead_error(&self) -> ScribeError {
        self.progress
            .borrow()
            .fatal
            .clone()
            .unwrap_or_else(session_task_ended)
    }

    fn transcript_with(&self, error: ScribeError) -> Transcript {
        Transcript {
            text: join_segments(&self.progress.borrow().committed),
            error: Some(error),
        }
    }
}

/// Dropping a session stops everything at once: the task is aborted, a connect in flight
/// is cancelled, the socket is closed without a handshake and audio still queued is never
/// uploaded.
impl Drop for ScribeSession {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn session_task_ended() -> ScribeError {
    ScribeError::Closed {
        code: 1006,
        reason: "session task ended".into(),
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
    // This only makes the value print as "Sensitive" through Debug. It does NOT keep the
    // key out of logs: tungstenite's trace line prints the raw upgrade request anyway,
    // and another trace line prints every received message (the transcript text). The
    // only protection is that the host caps the `tungstenite` and `tokio_tungstenite`
    // log targets at Info.
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
                Ok(ServerMessage::SessionStarted) => return Ok(socket),
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

/// Connects, announces [`ScribeEvent::Ready`], then runs the session until it ends.
async fn run_task(
    config: ScribeConfig,
    frames: mpsc::UnboundedReceiver<String>,
    events: mpsc::UnboundedSender<ScribeEvent>,
    progress: watch::Sender<Progress>,
    accepted: Arc<AtomicU64>,
) {
    let socket = match timeout(config.connect_timeout, open_socket(&config)).await {
        Ok(Ok(socket)) => socket,
        // Whatever stopped the connect, the session is unusable: record it as fatal even
        // for error kinds that are not fatal once a session runs.
        Ok(Err(error)) => return record_fatal(&events, &progress, error),
        Err(_) => return record_fatal(&events, &progress, ScribeError::Timeout("connecting")),
    };
    progress.send_modify(|p| p.connected = true);
    let _ = events.send(ScribeEvent::Ready);
    run_session(socket, frames, &events, &progress, &accepted, &config).await;
}

async fn send_frame(socket: &mut Socket, frame: String, send_timeout: Duration) -> Result<()> {
    match timeout(send_timeout, socket.send(Message::Text(frame.into()))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(ScribeError::Network(e.to_string())),
        Err(_) => Err(ScribeError::Network("sending audio timed out".into())),
    }
}

async fn run_session(
    mut socket: Socket,
    mut frames: mpsc::UnboundedReceiver<String>,
    events: &mpsc::UnboundedSender<ScribeEvent>,
    progress: &watch::Sender<Progress>,
    accepted: &AtomicU64,
    config: &ScribeConfig,
) {
    let silence = audio_chunk_json(&f32_to_pcm16_bytes(&[0.0; CHUNK_SAMPLES]), false, None);
    let idle = sleep(config.idle_silence_after);
    tokio::pin!(idle);
    // `previous_text` belongs on the first audio frame, so no idle silence goes out ahead
    // of the caller's first frame.
    let mut caller_audio_sent = false;
    let ended = loop {
        tokio::select! {
            // A closed channel means the session was dropped, which also aborts this task.
            Some(frame) = frames.recv() => {
                if let Err(e) = send_frame(&mut socket, frame, config.send_timeout).await {
                    break e;
                }
                caller_audio_sent = true;
                idle.as_mut().reset(tokio::time::Instant::now() + config.idle_silence_after);
            },
            () = &mut idle => {
                idle.as_mut().reset(tokio::time::Instant::now() + config.idle_silence_after);
                if caller_audio_sent {
                    // The server counts these samples like any others.
                    accepted.fetch_add(CHUNK_SAMPLES as u64, Ordering::Relaxed);
                    if let Err(e) = send_frame(&mut socket, silence.clone(), config.send_timeout).await {
                        break e;
                    }
                }
            },
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => match parse_server_message(text.as_str()) {
                    Ok(message) => handle_server_message(message, events, progress, accepted),
                    Err(e) => log::warn!("unparseable scribe frame: {e}"),
                },
                Some(Ok(Message::Close(frame))) => break closed_error(frame),
                None => break closed_error(None),
                Some(Ok(_)) => {}
                Some(Err(e)) => break ScribeError::Network(e.to_string()),
            },
        }
    };
    report_error(events, progress, ended);
}

fn report_error(
    events: &mpsc::UnboundedSender<ScribeEvent>,
    progress: &watch::Sender<Progress>,
    error: ScribeError,
) {
    if error.is_fatal() {
        return record_fatal(events, progress, error);
    }
    let _ = events.send(ScribeEvent::Error(error.clone()));
    progress.send_modify(|p| {
        p.error_seq += 1;
        p.last_error = Some(error);
    });
}

fn record_fatal(
    events: &mpsc::UnboundedSender<ScribeEvent>,
    progress: &watch::Sender<Progress>,
    error: ScribeError,
) {
    let _ = events.send(ScribeEvent::Error(error.clone()));
    // The first fatal error is the cause, later ones are fallout.
    progress.send_modify(|p| p.fatal = p.fatal.take().or(Some(error)));
}

fn handle_server_message(
    message: ServerMessage,
    events: &mpsc::UnboundedSender<ScribeEvent>,
    progress: &watch::Sender<Progress>,
    accepted: &AtomicU64,
) {
    match message {
        ServerMessage::Partial(text) => {
            let _ = events.send(ScribeEvent::Partial(text));
        }
        ServerMessage::Committed(text) => {
            let accepted_now = accepted.load(Ordering::Relaxed);
            progress.send_modify(|p| {
                p.accepted_at_last_commit = accepted_now;
                p.committed.push(text.clone());
            });
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
        ServerMessage::SessionStarted | ServerMessage::Ignored(_) => {}
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
