//! Measures word error rate and latency of Scribe v2 Realtime on the clips in tests/audio,
//! with the same 12 keyterms the app sends. The language setup defaults to Uzbek with English
//! as second language; --language and --secondary pick another one.
//!
//! Run from the crate folder with ELEVENLABS_API_KEY set:
//!   cargo run --release --example bench -- [--only uz-01,en-02] [--language en --secondary uz]
//!   cargo run --release --example bench -- --long    (one 60 s hold; does not write results.json)
//!
//! Audio is streamed in 256-sample (16 ms) frames at real-time pace, the way the app's
//! recorder delivers it, starting the instant `start` returns, as at a key press. So the
//! connection overlaps the first words. Three times are reported per clip:
//!   connect  key press to `ScribeEvent::Ready`
//!   finish   last frame to the returned transcript (the wait after the key is released)
//!   total    key press to the returned transcript (audio length + the extra a short tap pays)

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
    audio_ms: u128,
    connect_ms: Option<u128>,
    latency_ms: Option<u128>,
    total_ms: Option<u128>,
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
    /// Key press to text, minus the clip length: what a tap pays after the speech ends.
    extra_over_audio_median_ms: u128,
    extra_over_audio_worst_ms: u128,
}

fn audio_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("audio")
}

/// The language part of the configuration, so two setups can be compared on the same clips.
struct Languages {
    main: String,
    secondary: Vec<String>,
}

impl Languages {
    fn from_args(args: &[String]) -> Self {
        let main = option_value(args, "--language").unwrap_or("uz").to_string();
        let secondary = option_value(args, "--secondary")
            .unwrap_or("en")
            .split(',')
            .filter(|code| !code.is_empty())
            .map(str::to_string)
            .collect();
        Self { main, secondary }
    }

    fn is_default(&self) -> bool {
        self.main == "uz" && self.secondary == ["en"]
    }
}

fn option_value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn config(key: &str, languages: &Languages) -> ScribeConfig {
    let mut config = ScribeConfig::new(key);
    config.language_code = Some(languages.main.clone()).filter(|code| !code.is_empty());
    config.secondary_languages = languages.secondary.clone();
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
    connect_ms: Option<u128>,
    latency_ms: u128,
    total_ms: u128,
    error: Option<ScribeError>,
}

async fn transcribe(
    key: &str,
    languages: &Languages,
    samples: &[f32],
) -> Result<Outcome, ScribeError> {
    let pressed = Instant::now();
    let (mut session, mut events) =
        ScribeSession::start(&Handle::current(), config(key, languages));
    let ready = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if event == ScribeEvent::Ready {
                return Some(Instant::now());
            }
        }
        None
    });

    let mut pace = interval(Duration::from_millis(16));
    for frame in samples.chunks(FRAME_SAMPLES) {
        pace.tick().await;
        session.send_audio(frame)?;
    }
    let last_frame_sent = Instant::now();
    let transcript = session.finish().await;
    let done = Instant::now();
    let ready_at = ready.await.ok().flatten();
    Ok(Outcome {
        text: transcript.text,
        connect_ms: ready_at.map(|at| at.duration_since(pressed).as_millis()),
        latency_ms: done.duration_since(last_frame_sent).as_millis(),
        total_ms: done.duration_since(pressed).as_millis(),
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

fn summarise(rows: Vec<Row>, languages: &Languages) -> Summary {
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
    let mut extras: Vec<u128> = rows
        .iter()
        .filter_map(|r| r.total_ms.map(|t| t.saturating_sub(r.audio_ms)))
        .collect();
    let extra_over_audio_worst_ms = extras.iter().copied().max().unwrap_or(0);
    let extra_over_audio_median_ms = median(&mut extras);

    for (lang, wer) in &by_lang {
        println!("mean WER {lang:<6} {:.1}%", wer * 100.0);
    }
    println!("mean WER all    {:.1}%", mean_all * 100.0);
    println!("latency  median {latency_median_ms} ms, worst {latency_worst_ms} ms");
    println!("connect  median {connect_median_ms} ms, worst {connect_worst_ms} ms");
    println!(
        "key press to text minus clip length: median {extra_over_audio_median_ms} ms, worst {extra_over_audio_worst_ms} ms"
    );

    Summary {
        params: format!(
            "language_code={} secondary={:?} keyterms={}",
            languages.main,
            languages.secondary,
            KEYTERMS.len()
        ),
        rows,
        mean_wer_by_lang: by_lang,
        mean_wer_all: mean_all,
        latency_median_ms,
        latency_worst_ms,
        connect_median_ms,
        connect_worst_ms,
        extra_over_audio_median_ms,
        extra_over_audio_worst_ms,
    }
}

/// Streams the first clips back to back as one 60 s hold, to see the server commit by itself
/// partway through and `finish` join the segments.
async fn long_hold(key: &str, languages: &Languages, clips: &[Clip]) {
    let mut samples = Vec::new();
    let mut reference = Vec::new();
    for clip in clips {
        samples.extend(read_clip(&audio_dir().join(&clip.file)));
        reference.push(clip.reference.as_str());
        if samples.len() >= 60 * 16_000 {
            break;
        }
    }
    let (mut session, mut events) =
        ScribeSession::start(&Handle::current(), config(key, languages));
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
    let only: Option<Vec<String>> =
        option_value(&args, "--only").map(|list| list.split(',').map(str::to_string).collect());
    let languages = Languages::from_args(&args);

    let manifest =
        std::fs::read_to_string(audio_dir().join("manifest.json")).expect("manifest.json");
    let mut clips: Vec<Clip> = serde_json::from_str(&manifest).expect("manifest parses");
    if let Some(ids) = &only {
        clips.retain(|c| ids.contains(&c.id));
    }

    if args.iter().any(|a| a == "--long") {
        long_hold(&key, &languages, &clips).await;
        return;
    }

    let mut rows = Vec::new();
    for clip in &clips {
        let samples = read_clip(&audio_dir().join(&clip.file));
        let audio_ms = samples.len() as u128 * 1000 / 16_000;
        let result = transcribe(&key, &languages, &samples).await;
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
                    audio_ms,
                    connect_ms: outcome.connect_ms,
                    latency_ms: Some(outcome.latency_ms),
                    total_ms: Some(outcome.total_ms),
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
                audio_ms,
                connect_ms: None,
                latency_ms: None,
                total_ms: None,
                error: Some(error.to_string()),
            },
        };
        println!(
            "{:<9} wer {:>5}  audio {:>5} ms  connect {:>5} ms  finish {:>5} ms  total {:>5} ms  {}",
            row.id,
            row.wer.map_or("-".into(), |w| format!("{w:.2}")),
            row.audio_ms,
            row.connect_ms.map_or("-".into(), |c| c.to_string()),
            row.latency_ms.map_or("-".into(), |l| l.to_string()),
            row.total_ms.map_or("-".into(), |t| t.to_string()),
            row.error.as_deref().unwrap_or(&row.hypothesis),
        );
        rows.push(row);
        if stop {
            println!("stopping: the error would repeat on every clip");
            break;
        }
    }

    let output = serde_json::json!({ "config": summarise(rows, &languages) });
    // The default setup keeps the committed results.json; another one writes beside it.
    let file = if languages.is_default() {
        "results.json".to_string()
    } else {
        format!("results-{}.json", languages.main)
    };
    let path = audio_dir().join(file);
    std::fs::write(&path, serde_json::to_string_pretty(&output).unwrap() + "\n")
        .expect("write results");
    println!("saved {}", path.display());
}
