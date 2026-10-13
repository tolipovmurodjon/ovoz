//! A one-connection websocket server on 127.0.0.1 that plays the Scribe side for tests.
//! The script closure decides what to answer; the server records every client frame it
//! reads so tests can assert on what went over the wire.

use std::future::Future;
use std::sync::{Arc, Mutex};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub(crate) struct MockServer {
    pub endpoint: String,
    pub received: Arc<Mutex<Vec<Value>>>,
}

pub(crate) struct Conn {
    ws: WebSocketStream<TcpStream>,
    received: Arc<Mutex<Vec<Value>>>,
}

impl MockServer {
    /// Accepts one client, sends `session_started`, then hands the connection to `script`.
    /// The socket closes when the script returns, so scripts end by waiting forever.
    pub(crate) async fn start<F, Fut>(script: F) -> Self
    where
        F: FnOnce(Conn) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self::start_with(true, script).await
    }

    /// Like `start`, but sends nothing first: the script decides when, or whether, the
    /// server says `session_started`.
    pub(crate) async fn start_silent<F, Fut>(script: F) -> Self
    where
        F: FnOnce(Conn) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self::start_with(false, script).await
    }

    /// Answers the websocket upgrade with an HTTP error status.
    pub(crate) async fn start_rejecting(status: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/v1/speech-to-text/realtime",
            listener.local_addr().unwrap()
        );
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // The large `Err` type is dictated by tungstenite's handshake callback.
            #[allow(clippy::result_large_err)]
            let reject = move |_: &Request, _: Response| -> Result<Response, ErrorResponse> {
                Err(http::Response::builder().status(status).body(None).unwrap())
            };
            let _ = tokio_tungstenite::accept_hdr_async(stream, reject).await;
        });
        Self {
            endpoint,
            received: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn start_with<F, Fut>(announce: bool, script: F) -> Self
    where
        F: FnOnce(Conn) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/v1/speech-to-text/realtime",
            listener.local_addr().unwrap()
        );
        let received = Arc::new(Mutex::new(Vec::new()));
        let recorded = received.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let mut conn = Conn {
                ws,
                received: recorded,
            };
            if announce {
                conn.send_session_started().await;
            }
            script(conn).await;
        });
        Self { endpoint, received }
    }
}

impl Conn {
    pub(crate) async fn send_session_started(&mut self) {
        self.send(json!({"message_type": "session_started", "session_id": "mock"}))
            .await;
    }

    /// Sends a websocket close frame with this code and reason.
    pub(crate) async fn close(&mut self, code: u16, reason: &str) {
        let frame = CloseFrame {
            code: code.into(),
            reason: reason.to_string().into(),
        };
        let _ = self.ws.close(Some(frame)).await;
    }

    pub(crate) async fn send(&mut self, frame: Value) {
        let _ = self.ws.send(Message::Text(frame.to_string().into())).await;
    }

    /// The next client frame, or `None` once the client is gone.
    pub(crate) async fn recv(&mut self) -> Option<Value> {
        loop {
            match self.ws.next().await? {
                Ok(Message::Text(text)) => {
                    let frame: Value = serde_json::from_str(text.as_str()).unwrap();
                    self.received.lock().unwrap().push(frame.clone());
                    return Some(frame);
                }
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(_) => {}
            }
        }
    }
}

/// Number of 16-bit samples carried by a client `input_audio_chunk` frame.
pub(crate) fn samples_in(frame: &Value) -> usize {
    STANDARD
        .decode(frame["audio_base_64"].as_str().unwrap())
        .unwrap()
        .len()
        / 2
}

/// The first PCM16 sample of a client frame: tests give every frame its own level to
/// check order.
pub(crate) fn first_sample(frame: &Value) -> i16 {
    let bytes = STANDARD
        .decode(frame["audio_base_64"].as_str().unwrap())
        .unwrap();
    i16::from_le_bytes([bytes[0], bytes[1]])
}

pub(crate) fn is_commit(frame: &Value) -> bool {
    frame["commit"] == true
}

pub(crate) fn partial(text: &str) -> Value {
    json!({"message_type": "partial_transcript", "text": text})
}

pub(crate) fn committed(text: &str) -> Value {
    json!({"message_type": "committed_transcript", "text": text})
}

pub(crate) fn error_frame(kind: &str, message: &str) -> Value {
    json!({"message_type": kind, "error": message})
}
