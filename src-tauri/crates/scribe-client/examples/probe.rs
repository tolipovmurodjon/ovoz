//! Prints every server frame with a timestamp for three commit scenarios, to see how the
//! live API behaves before relying on it:
//!   a  zero audio, then commit
//!   b  0.2 s of speech, then commit
//!   c  about 60 s of speech at real-time pace, one commit at the end, then a second commit
//!      with no new audio
//! Run from the crate folder with ELEVENLABS_API_KEY set:
//!   cargo run --release --example probe -- [a|b|c]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use scribe_client::f32_to_pcm16_bytes;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

const URL: &str = "wss://api.elevenlabs.io/v1/speech-to-text/realtime?model_id=scribe_v2_realtime&audio_format=pcm_16000&commit_strategy=manual&language_code=uz&secondary_languages=en";

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

fn log(t0: Instant, what: &str) {
    println!("[{:>7.3}s] {what}", t0.elapsed().as_secs_f64());
}

#[tokio::main]
async fn main() {
    let key = std::env::var("ELEVENLABS_API_KEY").expect("ELEVENLABS_API_KEY is not set");
    let which = std::env::args().nth(1).unwrap_or_else(|| "abc".into());
    for case in which.chars() {
        println!("\n===== case {case} =====");
        run(case, &key).await;
    }
}

async fn run(case: char, key: &str) {
    let t0 = Instant::now();
    let mut request = URL.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("xi-api-key", HeaderValue::from_str(key).unwrap());
    let (socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    log(t0, "websocket open");
    let (mut tx, mut rx) = socket.split();

    let t0 = Arc::new(t0);
    let reader_t0 = t0.clone();
    let reader = tokio::spawn(async move {
        while let Some(message) = rx.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    let text = text.as_str();
                    let shown: String = text.chars().take(260).collect();
                    log(*reader_t0, &format!("<- {shown}"));
                }
                Ok(Message::Close(frame)) => {
                    log(*reader_t0, &format!("<- CLOSE {frame:?}"));
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    log(*reader_t0, &format!("<- ERR {e}"));
                    break;
                }
            }
        }
    });

    // let session_started arrive
    sleep(Duration::from_millis(800)).await;

    match case {
        'a' => {
            log(*t0, "-> commit, no audio");
            tx.send(frame(&[], true)).await.unwrap();
            sleep(Duration::from_secs(6)).await;
        }
        'b' => {
            let clip = read_clip("en-01.wav");
            let speech = &clip[6400..9600]; // 0.2 s from inside the sentence
            log(*t0, "-> 0.2 s of speech in 2 chunks, then commit");
            tx.send(frame(&speech[..1600], false)).await.unwrap();
            tx.send(frame(&speech[1600..], false)).await.unwrap();
            tx.send(frame(&[], true)).await.unwrap();
            sleep(Duration::from_secs(6)).await;
        }
        'd' => {
            let clip = read_clip("en-01.wav");
            let mut speech = clip[6400..9600].to_vec(); // 0.2 s
            speech.extend(std::iter::repeat_n(0.0, 2000)); // + 0.125 s of silence = 0.325 s
            log(
                *t0,
                "-> 0.2 s speech + 0.125 s silence padding, then commit",
            );
            tx.send(frame(&speech, false)).await.unwrap();
            tx.send(frame(&[], true)).await.unwrap();
            sleep(Duration::from_secs(6)).await;
        }
        'e' => {
            let clip = read_clip("en-01.wav");
            log(*t0, "-> 0.35 s of speech, then commit");
            tx.send(frame(&clip[6400..12000], false)).await.unwrap();
            tx.send(frame(&[], true)).await.unwrap();
            sleep(Duration::from_secs(6)).await;
        }
        'f' => {
            log(*t0, "-> 0.5 s of pure silence, then commit");
            tx.send(frame(&[0.0; 8000], false)).await.unwrap();
            tx.send(frame(&[], true)).await.unwrap();
            sleep(Duration::from_secs(6)).await;
        }
        'c' => {
            let mut audio = Vec::new();
            for name in [
                "uz-01", "en-01", "mixed-01", "uz-02", "en-02", "mixed-02", "uz-03", "en-03",
                "mixed-03", "uz-04", "en-04", "mixed-04", "uz-05", "en-05", "mixed-05",
            ] {
                audio.extend(read_clip(&format!("{name}.wav")));
                if audio.len() >= 60 * 16_000 {
                    break;
                }
            }
            log(
                *t0,
                &format!(
                    "-> streaming {:.1} s at real-time pace",
                    audio.len() as f64 / 16000.0
                ),
            );
            let mut pace = interval(Duration::from_millis(100));
            pace.set_missed_tick_behavior(MissedTickBehavior::Delay);
            for chunk in audio.chunks(1600) {
                pace.tick().await;
                tx.send(frame(chunk, false)).await.unwrap();
            }
            log(*t0, "-> audio done, sending the one commit");
            tx.send(frame(&[], true)).await.unwrap();
            sleep(Duration::from_secs(5)).await;
            log(*t0, "-> second commit, no new audio");
            tx.send(frame(&[], true)).await.unwrap();
            sleep(Duration::from_secs(5)).await;
        }
        _ => {}
    }
    let _ = tx.close().await;
    reader.abort();
}
