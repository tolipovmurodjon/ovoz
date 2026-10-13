//! Measures word error rate and latency of Scribe v2 Realtime on the clips in tests/audio,
//! in the one configuration the app uses (Uzbek, English as second language, 12 keyterms).
//!
//! Run from the crate folder with ELEVENLABS_API_KEY set:
//!   cargo run --release --example bench -- [--only uz-01,en-02]
//!   cargo run --release --example bench -- --long    (one 60 s hold; does not write results.json)
//!
//! Audio is streamed in 256-sample (16 ms) frames at real-time pace, the way the app's
//! recorder delivers it. Latency runs from the last frame to the returned transcript.
//! Connect time (websocket open to `session_started`) is timed apart, because a short
//! tap pays for it in full.

mod wer;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use scribe_client::{ScribeConfig, ScribeError, ScribeEvent, ScribeSession};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::time::interval;
use wer::word_errors;

const FRAME_SAMPLES: usize = 256; // 16 ms at 16 kHz
const KEYTERMS: &[&str] = &[
    "Humopedia",
    "PAGEL",
    "Unity",
    "Cinemachine",
    "Blender",
    "ElevenLabs",
    "Registon",
    "Samarqand",
    "bomdod",
    "dashboard",
    "deploy",
    "pull request",
];

#[derive(Deserialize)]
struct Clip {
    id: String,
    lang: String,
    reference: String,
    file: String,
}

#[derive(Serialize)]
struct Row {
    id: String,
    lang: String,
    reference: String,
    hypothesis: String,
    wer: Option<f64>,
    edits: Option<usize>,
    connect_ms: Option<u128>,
    latency_ms: Option<u128>,
    error: Option<String>,
}

#[derive(Serialize)]
struct Summary {
    params: String,
    rows: Vec<Row>,
    mean_wer_by_lang: Vec<(String, f64)>,
    mean_wer_all: f64,
    latency_median_ms: u128,
    latency_worst_ms: u128,
    connect_median_ms: u128,
    connect_worst_ms: u128,
}

fn audio_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("audio")
}

fn config(key: &str) -> ScribeConfig {
    let mut config = ScribeConfig::new(key);
    config.language_code = Some("uz".into());
    config.secondary_languages = vec!["en".into()];
    config.keyterms = KEYTERMS.iter().map(|s| s.to_string()).collect();
    config
}

fn read_clip(path: &PathBuf) -> Vec<f32> {
    let mut reader = hound::WavReader::open(path).expect("clip opens");
    reader
        .samples::<i16>()
        .map(|s| f32::from(s.expect("clip sample")) / 32768.0)
        .collect()
}

struct Outcome {
    text: String,
    connect_ms: u128,
    latency_ms: u128,
    error: Option<ScribeError>,
}

async fn transcribe(key: &str, samples: &[f32]) -> Result<Outcome, ScribeError> {
    let connect_started = Instant::now();
    let (mut session, _events) = ScribeSession::connect(&Handle::current(), config(key)).await?;
    let connect_ms = connect_started.elapsed().as_millis();

    let mut pace = interval(Duration::from_millis(16));
    for frame in samples.chunks(FRAME_SAMPLES) {
        pace.tick().await;
        session.send_audio(frame)?;
    }
    let last_frame_sent = Instant::now();
    let transcript = session.finish().await;
    Ok(Outcome {
        text: transcript.text,
        connect_ms,
        latency_ms: last_frame_sent.elapsed().as_millis(),
        error: transcript.error,
    })
}

fn is_run_ending(error: &ScribeError) -> bool {
    matches!(
        error,
        ScribeError::Auth(_) | ScribeError::Quota(_) | ScribeError::RateLimited { .. }
    )
}

fn median(values: &mut [u128]) -> u128 {
    values.sort_unstable();
    values.get(values.len() / 2).copied().unwrap_or(0)
}

fn summarise(rows: Vec<Row>) -> Summary {
    let mut by_lang = Vec::new();
    for lang in ["uz", "en", "mixed"] {
        let wers: Vec<f64> = rows
            .iter()
            .filter(|r| r.lang == lang)
            .filter_map(|r| r.wer)
            .collect();
        if !wers.is_empty() {
            by_lang.push((
                lang.to_string(),
                wers.iter().sum::<f64>() / wers.len() as f64,
            ));
        }
    }
    let all: Vec<f64> = rows.iter().filter_map(|r| r.wer).collect();
    let mean_all = if all.is_empty() {
        f64::NAN
    } else {
        all.iter().sum::<f64>() / all.len() as f64
    };
    let mut latencies: Vec<u128> = rows.iter().filter_map(|r| r.latency_ms).collect();
    let mut connects: Vec<u128> = rows.iter().filter_map(|r| r.connect_ms).collect();
    let latency_worst_ms = latencies.iter().copied().max().unwrap_or(0);
    let connect_worst_ms = connects.iter().copied().max().unwrap_or(0);
    let latency_median_ms = median(&mut latencies);
    let connect_median_ms = median(&mut connects);

    for (lang, wer) in &by_lang {
        println!("mean WER {lang:<6} {:.1}%", wer * 100.0);
    }
    println!("mean WER all    {:.1}%", mean_all * 100.0);
    println!("latency  median {latency_median_ms} ms, worst {latency_worst_ms} ms");
    println!("connect  median {connect_median_ms} ms, worst {connect_worst_ms} ms");

    Summary {
        params: format!(
            "language_code=uz secondary=[en] keyterms={}",
            KEYTERMS.len()
        ),
        rows,
        mean_wer_by_lang: by_lang,
        mean_wer_all: mean_all,
        latency_median_ms,
        latency_worst_ms,
        connect_median_ms,
        connect_worst_ms,
    }
}

/// Streams the first clips back to back as one 60 s hold, to see the server commit by itself
/// partway through and `finish` join the segments.
async fn long_hold(key: &str, clips: &[Clip]) {
    let mut samples = Vec::new();
    let mut reference = Vec::new();
    for clip in clips {
        samples.extend(read_clip(&audio_dir().join(&clip.file)));
        reference.push(clip.reference.as_str());
        if samples.len() >= 60 * 16_000 {
            break;
        }
    }
    let (mut session, mut events) = ScribeSession::connect(&Handle::current(), config(key))
        .await
        .expect("connect");
    let mut pace = interval(Duration::from_millis(16));
    for frame in samples.chunks(FRAME_SAMPLES) {
        pace.tick().await;
        session.send_audio(frame).expect("send during the hold");
    }
    let last_frame_sent = Instant::now();
    let transcript = session.finish().await;
    let finish_ms = last_frame_sent.elapsed().as_millis();

    let mut segments = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, ScribeEvent::Committed(_)) {
            segments += 1;
        }
    }
    let errors = word_errors(&reference.join(" "), &transcript.text);
    println!(
        "{:.1} s hold: {segments} committed segments, finish {finish_ms} ms, WER {:.1}%, error {:?}",
        samples.len() as f64 / 16_000.0,
        errors.rate() * 100.0,
        transcript.error
    );
    println!("{}", transcript.text);
}

#[tokio::main]
async fn main() {
    let key = std::env::var("ELEVENLABS_API_KEY").expect("ELEVENLABS_API_KEY is not set");
    let args: Vec<String> = std::env::args().collect();
    let only: Option<Vec<String>> = args
        .iter()
        .position(|a| a == "--only")
        .and_then(|i| args.get(i + 1))
        .map(|list| list.split(',').map(str::to_string).collect());

    let manifest =
        std::fs::read_to_string(audio_dir().join("manifest.json")).expect("manifest.json");
    let mut clips: Vec<Clip> = serde_json::from_str(&manifest).expect("manifest parses");
    if let Some(ids) = &only {
        clips.retain(|c| ids.contains(&c.id));
    }

    if args.iter().any(|a| a == "--long") {
        long_hold(&key, &clips).await;
        return;
    }

    let mut rows = Vec::new();
    for clip in &clips {
        let samples = read_clip(&audio_dir().join(&clip.file));
        let result = transcribe(&key, &samples).await;
        let stop = matches!(&result, Err(e) if is_run_ending(e));
        let row = match result {
            Ok(outcome) => {
                let errors = word_errors(&clip.reference, &outcome.text);
                Row {
                    id: clip.id.clone(),
                    lang: clip.lang.clone(),
                    reference: clip.reference.clone(),
                    hypothesis: outcome.text,
                    wer: Some(errors.rate()),
                    edits: Some(errors.edits),
                    connect_ms: Some(outcome.connect_ms),
                    latency_ms: Some(outcome.latency_ms),
                    error: outcome.error.map(|e| e.to_string()),
                }
            }
            Err(error) => Row {
                id: clip.id.clone(),
                lang: clip.lang.clone(),
                reference: clip.reference.clone(),
                hypothesis: String::new(),
                wer: None,
                edits: None,
                connect_ms: None,
                latency_ms: None,
                error: Some(error.to_string()),
            },
        };
        println!(
            "{:<9} wer {:>5}  connect {:>5} ms  finish {:>5} ms  {}",
            row.id,
            row.wer.map_or("-".into(), |w| format!("{w:.2}")),
            row.connect_ms.map_or("-".into(), |c| c.to_string()),
            row.latency_ms.map_or("-".into(), |l| l.to_string()),
            row.error.as_deref().unwrap_or(&row.hypothesis),
        );
        rows.push(row);
        if stop {
            println!("stopping: the error would repeat on every clip");
            break;
        }
    }

    let output = serde_json::json!({ "config": summarise(rows) });
    let path = audio_dir().join("results.json");
    std::fs::write(&path, serde_json::to_string_pretty(&output).unwrap() + "\n")
        .expect("write results");
    println!("saved {}", path.display());
}
