//! Measures word error rate and latency of Scribe v2 Realtime on the clips in tests/audio.
//!
//! Run from the crate folder with ELEVENLABS_API_KEY set:
//!   cargo run --release --example bench -- [--only uz-01,en-02] [--no-extras]
//! Config A (auto detect) and B (uz + en) run first, C adds keyterms to the better one.
//! Audio is streamed in 100 ms chunks at real-time pace so the latency is honest.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use scribe_client::wer::word_errors;
use scribe_client::{ScribeConfig, ScribeError, ScribeEvent, ScribeSession};
use serde::{Deserialize, Serialize};
use tokio::time::{interval, timeout, MissedTickBehavior};

const CHUNK_SAMPLES: usize = 1600; // 100 ms at 16 kHz
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
const EDIT_INSTRUCTION: &str = "Fix punctuation and capitalisation. Do not change any words.";

#[derive(Deserialize)]
struct Clip {
    id: String,
    lang: String,
    reference: String,
    file: String,
}

#[derive(Serialize, Clone)]
struct Row {
    id: String,
    lang: String,
    reference: String,
    hypothesis: String,
    wer: Option<f64>,
    edits: Option<usize>,
    latency_ms: Option<u128>,
    error: Option<String>,
}

#[derive(Clone)]
struct Setup {
    name: &'static str,
    language_code: Option<&'static str>,
    secondary: &'static [&'static str],
    keyterms: bool,
    no_verbatim: bool,
    transcript_edit: bool,
}

#[derive(Serialize)]
struct ConfigResult {
    name: String,
    params: String,
    rows: Vec<Row>,
    mean_wer_by_lang: Vec<(String, f64)>,
    mean_wer_all: f64,
    latency_median_ms: u128,
    latency_worst_ms: u128,
}

#[derive(Serialize)]
struct ExtraResult {
    name: String,
    clip: String,
    hypothesis: String,
    committed_latency_ms: Option<u128>,
    extra_event_latency_ms: Option<u128>,
    error: Option<String>,
}

fn audio_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("audio")
}

fn config_for(setup: &Setup, key: &str) -> ScribeConfig {
    let mut config = ScribeConfig::new(key);
    config.language_code = setup.language_code.map(str::to_string);
    config.secondary_languages = setup.secondary.iter().map(|s| s.to_string()).collect();
    if setup.keyterms {
        config.keyterms = KEYTERMS.iter().map(|s| s.to_string()).collect();
    }
    config.no_verbatim = setup.no_verbatim;
    config.transcript_edit = setup.transcript_edit.then(|| EDIT_INSTRUCTION.to_string());
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
    committed_ms: u128,
    edited_ms: Option<u128>,
}

/// Streams one clip at real-time pace. Latency runs from the last audio chunk to the
/// final committed transcript, so it includes the commit message itself.
async fn transcribe(
    config: ScribeConfig,
    samples: &[f32],
    wait_for_edit: bool,
) -> Result<Outcome, ScribeError> {
    let mut session = ScribeSession::connect(config).await?;
    let mut events = session.take_events().expect("events not yet taken");

    let mut pace = interval(Duration::from_millis(100));
    pace.set_missed_tick_behavior(MissedTickBehavior::Delay);
    for chunk in samples.chunks(CHUNK_SAMPLES) {
        pace.tick().await;
        session.send_audio(chunk)?;
    }
    let last_chunk_sent = Instant::now();
    let text = session.finish().await?;
    let committed_ms = last_chunk_sent.elapsed().as_millis();

    let mut edited_ms = None;
    if wait_for_edit {
        let wait = async {
            while let Some(event) = events.recv().await {
                match event {
                    ScribeEvent::Edited { edited, .. } => return Some(edited),
                    ScribeEvent::Error(e) => eprintln!("  edit wait saw error: {e}"),
                    _ => {}
                }
            }
            None
        };
        if let Ok(Some(edited)) = timeout(Duration::from_secs(10), wait).await {
            edited_ms = Some(last_chunk_sent.elapsed().as_millis());
            return Ok(Outcome {
                text: edited,
                committed_ms,
                edited_ms,
            });
        }
    }
    Ok(Outcome {
        text,
        committed_ms,
        edited_ms,
    })
}

fn is_run_ending(error: &ScribeError) -> bool {
    matches!(
        error,
        ScribeError::Auth(_) | ScribeError::Quota(_) | ScribeError::RateLimited { .. }
    )
}

async fn run_config(setup: &Setup, key: &str, clips: &[Clip]) -> ConfigResult {
    println!("\n=== Config {} ===", setup.name);
    let mut rows = Vec::new();
    for clip in clips {
        let samples = read_clip(&audio_dir().join(&clip.file));
        let result = transcribe(config_for(setup, key), &samples, false).await;
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
                    latency_ms: Some(outcome.committed_ms),
                    error: None,
                }
            }
            Err(error) => Row {
                id: clip.id.clone(),
                lang: clip.lang.clone(),
                reference: clip.reference.clone(),
                hypothesis: String::new(),
                wer: None,
                edits: None,
                latency_ms: None,
                error: Some(error.to_string()),
            },
        };
        println!(
            "{:<9} wer {:>5}  {:>5} ms  {}",
            row.id,
            row.wer.map_or("-".into(), |w| format!("{:.2}", w)),
            row.latency_ms.map_or("-".into(), |l| l.to_string()),
            row.error.as_deref().unwrap_or(&row.hypothesis),
        );
        rows.push(row);
        if stop {
            println!("stopping this configuration: the error would repeat on every clip");
            break;
        }
    }
    summarise(setup, rows)
}

fn median(values: &mut [u128]) -> u128 {
    values.sort_unstable();
    if values.is_empty() {
        0
    } else {
        values[values.len() / 2]
    }
}

fn summarise(setup: &Setup, rows: Vec<Row>) -> ConfigResult {
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
    let worst = latencies.iter().copied().max().unwrap_or(0);
    let med = median(&mut latencies);

    println!("--- {} summary ---", setup.name);
    for (lang, wer) in &by_lang {
        println!("mean WER {lang:<6} {:.1}%", wer * 100.0);
    }
    println!("mean WER all    {:.1}%", mean_all * 100.0);
    println!("latency median  {med} ms, worst {worst} ms");

    let params = format!(
        "language_code={:?} secondary={:?} keyterms={}",
        setup.language_code, setup.secondary, setup.keyterms
    );
    ConfigResult {
        name: setup.name.to_string(),
        params,
        rows,
        mean_wer_by_lang: by_lang,
        mean_wer_all: mean_all,
        latency_median_ms: med,
        latency_worst_ms: worst,
    }
}

async fn run_extra(name: &str, setup: &Setup, key: &str, clip: &Clip) -> ExtraResult {
    let samples = read_clip(&audio_dir().join(&clip.file));
    let result = transcribe(config_for(setup, key), &samples, setup.transcript_edit).await;
    let extra = match result {
        Ok(outcome) => ExtraResult {
            name: name.to_string(),
            clip: clip.id.clone(),
            hypothesis: outcome.text,
            committed_latency_ms: Some(outcome.committed_ms),
            extra_event_latency_ms: outcome.edited_ms,
            error: None,
        },
        Err(error) => ExtraResult {
            name: name.to_string(),
            clip: clip.id.clone(),
            hypothesis: String::new(),
            committed_latency_ms: None,
            extra_event_latency_ms: None,
            error: Some(error.to_string()),
        },
    };
    println!(
        "\n[{}] {}\n  reference : {}\n  returned  : {}\n  committed after {:?} ms, edited event after {:?} ms, error {:?}",
        extra.name, clip.id, clip.reference, extra.hypothesis, extra.committed_latency_ms,
        extra.extra_event_latency_ms, extra.error
    );
    extra
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
    let run_extras = !args.iter().any(|a| a == "--no-extras");

    let manifest =
        std::fs::read_to_string(audio_dir().join("manifest.json")).expect("manifest.json");
    let mut clips: Vec<Clip> = serde_json::from_str(&manifest).expect("manifest parses");
    if let Some(ids) = &only {
        clips.retain(|c| ids.contains(&c.id));
    }

    let a = Setup {
        name: "A auto-detect",
        language_code: None,
        secondary: &[],
        keyterms: false,
        no_verbatim: false,
        transcript_edit: false,
    };
    let b = Setup {
        name: "B uz+en",
        language_code: Some("uz"),
        secondary: &["en"],
        keyterms: false,
        no_verbatim: false,
        transcript_edit: false,
    };

    let result_a = run_config(&a, &key, &clips).await;
    let result_b = run_config(&b, &key, &clips).await;
    let better = if result_b.mean_wer_all < result_a.mean_wer_all {
        &b
    } else {
        &a
    };
    let c = Setup {
        name: if std::ptr::eq(better, &b) {
            "C uz+en + keyterms"
        } else {
            "C auto-detect + keyterms"
        },
        keyterms: true,
        ..better.clone()
    };
    let result_c = run_config(&c, &key, &clips).await;

    let mut extras = Vec::new();
    if run_extras {
        let best_setup = [(&a, &result_a), (&b, &result_b), (&c, &result_c)]
            .into_iter()
            .min_by(|x, y| x.1.mean_wer_all.total_cmp(&y.1.mean_wer_all))
            .map(|(setup, _)| setup.clone())
            .expect("three configs");
        if let Some(clip) = clips.iter().find(|c| c.id == "mixed-05").or(clips.first()) {
            extras.push(run_extra("baseline (best config)", &best_setup, &key, clip).await);
            let verbatim_off = Setup {
                no_verbatim: true,
                ..best_setup.clone()
            };
            extras.push(run_extra("no_verbatim=true", &verbatim_off, &key, clip).await);
            let edit = Setup {
                transcript_edit: true,
                ..best_setup.clone()
            };
            extras.push(run_extra("transcript_edit", &edit, &key, clip).await);
        }
    }

    let output = serde_json::json!({
        "configs": [result_a, result_b, result_c],
        "extras": extras,
    });
    let path = audio_dir().join("results.json");
    std::fs::write(&path, serde_json::to_string_pretty(&output).unwrap() + "\n")
        .expect("write results");
    println!("\nsaved {}", path.display());
}
