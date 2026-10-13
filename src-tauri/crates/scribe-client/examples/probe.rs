//! Prints every server frame with a timestamp and the audio seconds sent so far, to see
//! how the live API behaves before relying on it. Cases, given as arguments:
//!   a   zero audio, then commit
//!   b   0.2 s of speech, then commit
//!   d   0.2 s of speech + 0.125 s of silence in one audio frame, then an empty commit
//!   e   0.35 s of speech, then commit
//!   f   0.5 s of pure silence, then commit
//!   c   about 60 s of speech at real-time pace, one commit at the end, then a second
//!       commit with no new audio
//!   p1  one 0.1 s speech frame, then a commit frame that itself carries 0.225 s of audio
//!       (0.1 s speech + 0.125 s silence): does audio inside the commit frame count?
//!   p2  10 s of speech, 12 s of nothing, then speech until the first automatic commit:
//!       does the automatic commit clock count audio seconds or wall-clock seconds?
//!   p3  speech at real-time pace until the first automatic commit arrives, then waits
//!   p3:<s>  speech at real-time pace for <s> audio seconds, then a commit frame carrying
//!       the next 0.325 s of speech: a manual commit racing the automatic one
//! Run from the crate folder with ELEVENLABS_API_KEY set:
//!   cargo run --release --example probe -- a b p1

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use scribe_client::f32_to_pcm16_bytes;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

const URL: &str = "wss://api.elevenlabs.io/v1/speech-to-text/realtime?model_id=scribe_v2_realtime&audio_format=pcm_16000&commit_strategy=manual&language_code=uz&secondary_languages=en";
const CLIPS: &[&str] = &[
    "uz-01", "en-01", "mixed-01", "uz-02", "en-02", "mixed-02", "uz-03", "en-03", "mixed-03",
    "uz-04", "en-04", "mixed-04", "uz-05", "en-05", "mixed-05",
];

fn audio_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("audio")
}

fn read_clip(name: &str) -> Vec<f32> {
    let mut reader = hound::WavReader::open(audio_dir().join(name)).expect("clip opens");
    reader
        .samples::<i16>()
        .map(|s| f32::from(s.expect("sample")) / 32768.0)
        .collect()
}

/// `seconds` of speech, built from the test clips (repeated if needed).
fn speech(seconds: f64) -> Vec<f32> {
    let wanted = (seconds * 16_000.0) as usize;
    let mut audio = Vec::new();
    while audio.len() < wanted {
        for name in CLIPS {
            audio.extend(read_clip(&format!("{name}.wav")));
        }
    }
    audio.truncate(wanted);
    audio
}

fn frame(samples: &[f32], commit: bool) -> Message {
    let encoded = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(f32_to_pcm16_bytes(samples))
    };
    let json = serde_json::json!({
        "message_type": "input_audio_chunk",
        "audio_base_64": encoded,
        "commit": commit,
        "sample_rate": 16000,
    });
    Message::Text(json.to_string().into())
}

#[derive(Clone)]
struct Clock {
    t0: Instant,
    /// Samples handed to the socket so far.
    sent: Arc<AtomicU64>,
}

impl Clock {
    fn log(&self, what: &str) {
        let audio = self.sent.load(Ordering::Relaxed) as f64 / 16_000.0;
        println!(
            "[{:>7.3}s | audio {:>7.3}s] {what}",
            self.t0.elapsed().as_secs_f64(),
            audio
        );
    }
}

type Tx = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

async fn send_audio(tx: &mut Tx, clock: &Clock, samples: &[f32], commit: bool) {
    tx.send(frame(samples, commit)).await.unwrap();
    clock
        .sent
        .fetch_add(samples.len() as u64, Ordering::Relaxed);
}

/// Sends `samples` as 100 ms frames at real-time pace.
async fn stream(tx: &mut Tx, clock: &Clock, samples: &[f32]) {
    let mut pace = interval(Duration::from_millis(100));
    pace.set_missed_tick_behavior(MissedTickBehavior::Delay);
    for chunk in samples.chunks(1600) {
        pace.tick().await;
        send_audio(tx, clock, chunk, false).await;
    }
}

#[tokio::main]
async fn main() {
    let key = std::env::var("ELEVENLABS_API_KEY").expect("ELEVENLABS_API_KEY is not set");
    let cases: Vec<String> = std::env::args().skip(1).collect();
    let cases = if cases.is_empty() {
        vec!["a".into(), "b".into(), "c".into()]
    } else {
        cases
    };
    for case in cases {
        println!("\n===== case {case} =====");
        run(&case, &key).await;
    }
}

async fn run(case: &str, key: &str) {
    let clock = Clock {
        t0: Instant::now(),
        sent: Arc::new(AtomicU64::new(0)),
    };
    let mut request = URL.into_client_request().unwrap();
    let mut key = HeaderValue::from_str(key).unwrap();
    key.set_sensitive(true);
    request.headers_mut().insert("xi-api-key", key);
    let (socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    clock.log("websocket open");
    let (mut tx, mut rx) = socket.split();

    let first_commit_seen = Arc::new(AtomicBool::new(false));
    let seen = first_commit_seen.clone();
    let reader_clock = clock.clone();
    let reader = tokio::spawn(async move {
        while let Some(message) = rx.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    let text = text.as_str();
                    if text.contains("\"committed_transcript\"") {
                        seen.store(true, Ordering::Relaxed);
                    }
                    // Long frames show their head and tail, so a segment's last words are visible.
                    let chars: Vec<char> = text.chars().collect();
                    let shown: String = if chars.len() > 300 {
                        let head: String = chars[..120].iter().collect();
                        let tail: String = chars[chars.len() - 140..].iter().collect();
                        format!("{head} ... {tail}")
                    } else {
                        text.to_string()
                    };
                    reader_clock.log(&format!("<- {shown}"));
                }
                Ok(Message::Close(frame)) => {
                    reader_clock.log(&format!("<- CLOSE {frame:?}"));
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    reader_clock.log(&format!("<- ERR {e}"));
                    break;
                }
            }
        }
    });

    // let session_started arrive
    sleep(Duration::from_millis(800)).await;
    clock.sent.store(0, Ordering::Relaxed);

    match case {
        "a" => {
            clock.log("-> commit, no audio");
            send_audio(&mut tx, &clock, &[], true).await;
            sleep(Duration::from_secs(6)).await;
        }
        "b" => {
            let clip = read_clip("en-01.wav");
            let speech = &clip[6400..9600]; // 0.2 s from inside the sentence
            clock.log("-> 0.2 s of speech in 2 chunks, then commit");
            send_audio(&mut tx, &clock, &speech[..1600], false).await;
            send_audio(&mut tx, &clock, &speech[1600..], false).await;
            send_audio(&mut tx, &clock, &[], true).await;
            sleep(Duration::from_secs(6)).await;
        }
        "d" => {
            let clip = read_clip("en-01.wav");
            let mut speech = clip[6400..9600].to_vec(); // 0.2 s
            speech.extend(std::iter::repeat_n(0.0, 2000)); // + 0.125 s of silence = 0.325 s
            clock.log("-> 0.2 s speech + 0.125 s silence padding, then commit");
            send_audio(&mut tx, &clock, &speech, false).await;
            send_audio(&mut tx, &clock, &[], true).await;
            sleep(Duration::from_secs(6)).await;
        }
        "e" => {
            let clip = read_clip("en-01.wav");
            clock.log("-> 0.35 s of speech, then commit");
            send_audio(&mut tx, &clock, &clip[6400..12000], false).await;
            send_audio(&mut tx, &clock, &[], true).await;
            sleep(Duration::from_secs(6)).await;
        }
        "f" => {
            clock.log("-> 0.5 s of pure silence, then commit");
            send_audio(&mut tx, &clock, &[0.0; 8000], false).await;
            send_audio(&mut tx, &clock, &[], true).await;
            sleep(Duration::from_secs(6)).await;
        }
        "c" => {
            let audio = speech(60.0);
            clock.log("-> streaming 60 s at real-time pace");
            stream(&mut tx, &clock, &audio).await;
            clock.log("-> audio done, sending the one commit");
            send_audio(&mut tx, &clock, &[], true).await;
            sleep(Duration::from_secs(5)).await;
            clock.log("-> second commit, no new audio");
            send_audio(&mut tx, &clock, &[], true).await;
            sleep(Duration::from_secs(5)).await;
        }
        "p1" => {
            let clip = read_clip("en-01.wav");
            clock.log("-> one normal frame of 0.1 s speech");
            send_audio(&mut tx, &clock, &clip[6400..8000], false).await;
            let mut tail = clip[8000..9600].to_vec(); // 0.1 s more speech
            tail.extend(std::iter::repeat_n(0.0, 2000)); // + 0.125 s silence = 0.225 s
            clock.log("-> commit frame carrying 0.225 s (0.1 s speech + 0.125 s silence)");
            send_audio(&mut tx, &clock, &tail, true).await;
            sleep(Duration::from_secs(6)).await;
        }
        "p2" => {
            let audio = speech(50.0);
            clock.log("-> 10 s of speech at real-time pace");
            stream(&mut tx, &clock, &audio[..160_000]).await;
            // The server closes an idle socket after about 15 s (measured with a 30 s
            // gap), so the pause stays under that.
            clock.log("-> sending nothing for 12 s");
            sleep(Duration::from_secs(12)).await;
            clock.log("-> speech again until the first automatic commit arrives");
            let mut pace = interval(Duration::from_millis(100));
            pace.set_missed_tick_behavior(MissedTickBehavior::Delay);
            for chunk in audio[160_000..].chunks(1600) {
                pace.tick().await;
                send_audio(&mut tx, &clock, chunk, false).await;
                if first_commit_seen.load(Ordering::Relaxed) {
                    break;
                }
            }
            clock.log("-> stopped; waiting 3 s");
            sleep(Duration::from_secs(3)).await;
        }
        "p3" => {
            let audio = speech(50.0);
            clock.log("-> streaming speech until the first automatic commit arrives");
            let mut pace = interval(Duration::from_millis(100));
            pace.set_missed_tick_behavior(MissedTickBehavior::Delay);
            for chunk in audio.chunks(1600) {
                pace.tick().await;
                send_audio(&mut tx, &clock, chunk, false).await;
                if first_commit_seen.load(Ordering::Relaxed) {
                    break;
                }
            }
            clock.log("-> stopped; waiting 4 s for whatever else arrives");
            sleep(Duration::from_secs(4)).await;
        }
        other if other.starts_with("p3:") => {
            let stop: f64 = other[3..].parse().expect("p3:<audio seconds>");
            let audio = speech(stop + 0.325);
            let stop_samples = (stop * 16_000.0) as usize;
            clock.log(&format!("-> streaming {stop} s of speech"));
            stream(&mut tx, &clock, &audio[..stop_samples]).await;
            clock.log("-> commit frame carrying the next 0.325 s of speech");
            send_audio(&mut tx, &clock, &audio[stop_samples..], true).await;
            sleep(Duration::from_secs(5)).await;
        }
        _ => println!("unknown case"),
    }
    let _ = tx.close().await;
    reader.abort();
}
