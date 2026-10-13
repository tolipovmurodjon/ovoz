//! The session state machine against an in-process mock server: no network, no API key.

use std::future::pending;
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{sleep, timeout};

use crate::mock_server::{committed, error_frame, is_commit, partial, samples_in, MockServer};
use crate::{ScribeConfig, ScribeError, ScribeEvent, ScribeSession, Transcript};

fn config_for(server: &MockServer) -> ScribeConfig {
    let mut config = ScribeConfig::new("test-key");
    config.endpoint = Some(server.endpoint.clone());
    config.finish_timeout = Duration::from_secs(2);
    config.send_timeout = Duration::from_secs(1);
    config
}

async fn open(server: &MockServer) -> (ScribeSession, UnboundedReceiver<ScribeEvent>) {
    ScribeSession::connect(&Handle::current(), config_for(server))
        .await
        .unwrap()
}

/// Feeds `samples` of non-silent audio the way the app does: 256 samples per call.
fn feed(session: &mut ScribeSession, samples: usize) {
    for frame in vec![0.1f32; samples].chunks(256) {
        session.send_audio(frame).unwrap();
    }
}

async fn wait_for(
    events: &mut UnboundedReceiver<ScribeEvent>,
    wanted: impl Fn(&ScribeEvent) -> bool,
) -> ScribeEvent {
    timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.expect("event stream open");
            if wanted(&event) {
                return event;
            }
        }
    })
    .await
    .expect("the wanted event arrives")
}

fn server_error(code: &str, message: &str) -> ScribeError {
    ScribeError::Server {
        code: code.into(),
        message: message.into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partials_then_one_commit_returns_the_text_without_extra_waiting() {
    let server = MockServer::start(|mut conn| async move {
        let mut first = true;
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("hello world")).await;
            } else if first {
                first = false;
                conn.send(partial("hel")).await;
            }
        }
    })
    .await;
    let (mut session, mut events) = open(&server).await;
    feed(&mut session, 16_000);

    let started = Instant::now();
    let transcript = session.finish().await;

    assert_eq!(
        transcript,
        Transcript {
            text: "hello world".into(),
            error: None
        }
    );
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(
        events.recv().await,
        Some(ScribeEvent::Partial("hel".into()))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_commit_during_a_long_hold_is_joined_with_the_final_segment_in_order() {
    let server = MockServer::start(|mut conn| async move {
        let mut total = 0;
        let mut auto_sent = false;
        while let Some(frame) = conn.recv().await {
            total += samples_in(&frame);
            if is_commit(&frame) {
                conn.send(committed("second segment")).await;
            } else if total >= 40_000 && !auto_sent {
                auto_sent = true;
                conn.send(committed("first segment")).await;
            }
        }
    })
    .await;
    let (mut session, mut events) = open(&server).await;
    feed(&mut session, 48_000);
    wait_for(&mut events, |e| matches!(e, ScribeEvent::Committed(_))).await;
    feed(&mut session, 8_000);

    let transcript = session.finish().await;

    assert_eq!(transcript.text, "first segment second segment");
    assert_eq!(transcript.error, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finish_waits_for_its_own_commit_when_an_auto_commit_is_still_in_flight() {
    // The server's automatic commit and the reply to ours both arrive after finish
    // sent its commit: the first frame is not the answer to it.
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("automatic segment")).await;
                sleep(Duration::from_millis(150)).await;
                conn.send(committed("tail of the hold")).await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    feed(&mut session, 35 * 16_000);

    let transcript = session.finish().await;

    assert_eq!(transcript.text, "automatic segment tail of the hold");
    assert_eq!(transcript.error, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_final_commit_still_returns_the_text_committed_earlier() {
    let server = MockServer::start(|mut conn| async move {
        let mut total = 0;
        let mut auto_sent = false;
        while let Some(frame) = conn.recv().await {
            total += samples_in(&frame);
            if is_commit(&frame) {
                conn.send(error_frame("transcriber_error", "boom")).await;
            } else if total >= 20_000 && !auto_sent {
                auto_sent = true;
                conn.send(committed("earlier text")).await;
            }
        }
    })
    .await;
    let (mut session, mut events) = open(&server).await;
    feed(&mut session, 24_000);
    wait_for(&mut events, |e| matches!(e, ScribeEvent::Committed(_))).await;
    feed(&mut session, 8_000);

    let transcript = session.finish().await;

    assert_eq!(transcript.text, "earlier text");
    assert_eq!(
        transcript.error,
        Some(server_error("transcriber_error", "boom"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_fatal_error_mid_hold_does_not_affect_a_good_final_commit() {
    let server = MockServer::start(|mut conn| async move {
        let mut total = 0;
        let mut error_sent = false;
        while let Some(frame) = conn.recv().await {
            total += samples_in(&frame);
            if is_commit(&frame) {
                conn.send(committed("all good")).await;
            } else if total >= 10_000 && !error_sent {
                error_sent = true;
                conn.send(error_frame("input_error", "bad frame")).await;
            }
        }
    })
    .await;
    let (mut session, mut events) = open(&server).await;
    feed(&mut session, 12_000);
    wait_for(&mut events, |e| matches!(e, ScribeEvent::Error(_))).await;
    feed(&mut session, 6_000);

    let transcript = session.finish().await;

    assert_eq!(transcript.text, "all good");
    assert_eq!(transcript.error, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_audio_returns_empty_text_locally_without_a_round_trip() {
    let server =
        MockServer::start(|mut conn| async move { while conn.recv().await.is_some() {} }).await;
    let (session, _events) = open(&server).await;

    let started = Instant::now();
    let transcript = session.finish().await;
    sleep(Duration::from_millis(200)).await;

    assert_eq!(transcript, Transcript::default());
    assert!(started.elapsed() < Duration::from_millis(400));
    assert!(server.received.lock().unwrap().is_empty());
}

async fn finish_after_server_replies_to_commit_with(reply: serde_json::Value) -> Transcript {
    let server = MockServer::start(move |mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(reply.clone()).await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    feed(&mut session, 16_000);
    session.finish().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insufficient_audio_activity_means_empty_text_not_an_error() {
    let reply = error_frame("insufficient_audio_activity", "no speech");
    assert_eq!(
        finish_after_server_replies_to_commit_with(reply).await,
        Transcript::default()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_throttled_means_nothing_more_to_transcribe_not_an_error() {
    let reply = error_frame("commit_throttled", "need 0.3s");
    assert_eq!(
        finish_after_server_replies_to_commit_with(reply).await,
        Transcript::default()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unanswered_commit_returns_within_the_finish_timeout_with_the_text_so_far() {
    let server = MockServer::start(|mut conn| async move {
        let mut total = 0;
        let mut auto_sent = false;
        while let Some(frame) = conn.recv().await {
            total += samples_in(&frame);
            if total >= 10_000 && !auto_sent {
                auto_sent = true;
                conn.send(committed("text so far")).await;
            }
        }
    })
    .await;
    let mut config = config_for(&server);
    config.finish_timeout = Duration::from_millis(400);
    let (mut session, mut events) = ScribeSession::connect(&Handle::current(), config)
        .await
        .unwrap();
    feed(&mut session, 12_000);
    wait_for(&mut events, |e| matches!(e, ScribeEvent::Committed(_))).await;
    feed(&mut session, 8_000);

    let started = Instant::now();
    let transcript = session.finish().await;

    assert_eq!(transcript.text, "text so far");
    assert_eq!(
        transcript.error,
        Some(ScribeError::Timeout("waiting for the final transcript"))
    );
    assert!(started.elapsed() < Duration::from_millis(1_500));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_server_error_reaches_send_audio_and_finish() {
    let server = MockServer::start(|mut conn| async move {
        conn.recv().await;
        conn.send(error_frame("quota_exceeded", "no credits")).await;
        pending::<()>().await;
    })
    .await;
    let (mut session, _events) = open(&server).await;
    session.send_audio(&[0.1; 1600]).unwrap();

    let quota = ScribeError::Quota("no credits".into());
    let failure = timeout(Duration::from_secs(3), async {
        loop {
            if let Err(error) = session.send_audio(&[0.1; 256]) {
                return error;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the caller learns about the dead session during the hold");
    assert_eq!(failure, quota);

    let transcript = session.finish().await;
    assert_eq!(transcript.error, Some(quota));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_socket_fails_with_network_error_within_the_send_timeout_and_drop_is_instant() {
    // The server accepts and then never reads, so the kernel buffers fill and a write blocks.
    let server = MockServer::start(|conn| async move {
        let _unread = conn;
        pending::<()>().await
    })
    .await;
    let mut config = config_for(&server);
    config.send_timeout = Duration::from_millis(300);
    let (mut session, _events) = ScribeSession::connect(&Handle::current(), config)
        .await
        .unwrap();
    let started = Instant::now();
    // Queueing never blocks; once the stalled write times out, sends start failing.
    for _ in 0..12_000 {
        if session.send_audio(&[0.1; 1600]).is_err() {
            break;
        }
    }

    let failure = timeout(Duration::from_secs(5), async {
        loop {
            if let Err(error) = session.send_audio(&[0.1; 256]) {
                return error;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a stalled socket must fail the session, not hang it");

    assert!(matches!(failure, ScribeError::Network(_)), "{failure:?}");
    assert!(started.elapsed() < Duration::from_secs(3));

    let dropped = Instant::now();
    drop(session);
    assert!(dropped.elapsed() < Duration::from_millis(50));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_frames_leave_in_100_ms_frames_and_the_commit_carries_the_remainder() {
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("ok")).await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    // 256 * 25 = 6400 samples, then 256 * 10 more: 8960 in all.
    feed(&mut session, 6_400);
    feed(&mut session, 2_560);

    assert_eq!(session.finish().await.text, "ok");

    let frames = server.received.lock().unwrap().clone();
    let sizes: Vec<usize> = frames.iter().map(samples_in).collect();
    // Five whole frames, then the commit with the 960-sample remainder. 8960 samples
    // are already over the server minimum, so no padding is added.
    assert_eq!(sizes, vec![1600, 1600, 1600, 1600, 1600, 960]);
    assert!(frames[..5].iter().all(|f| !is_commit(f)));
    assert!(is_commit(&frames[5]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tap_under_the_server_minimum_is_padded_with_silence_up_to_it() {
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("hi")).await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    feed(&mut session, 2_560); // 0.16 s

    assert_eq!(session.finish().await.text, "hi");

    let frames = server.received.lock().unwrap().clone();
    assert_eq!(frames.iter().map(samples_in).sum::<usize>(), 5_200);
    // One whole frame went out during the hold; the commit carries the 960 left plus silence.
    assert_eq!(frames.len(), 2);
    assert!(is_commit(&frames[1]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_huge_buffer_is_split_so_no_frame_exceeds_100_ms() {
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("big")).await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    session.send_audio(&vec![0.1f32; 10 * 16_000 + 7]).unwrap();

    assert_eq!(session.finish().await.text, "big");

    let frames = server.received.lock().unwrap().clone();
    assert!(frames.iter().all(|f| samples_in(f) <= 1600));
    assert_eq!(
        frames.iter().map(samples_in).sum::<usize>(),
        10 * 16_000 + 7
    );
}

#[test]
fn a_plain_std_thread_can_run_a_whole_session_and_audio_uploads_during_the_hold() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let server = runtime.block_on(MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("from a thread")).await;
            }
        }
    }));
    let config = config_for(&server);
    let handle = runtime.handle().clone();
    let received = server.received.clone();

    let (transcript, uploaded_before_finish) = std::thread::spawn(move || {
        let (mut session, _events) = ScribeSession::connect_blocking(&handle, config).unwrap();
        for frame in vec![0.1f32; 16_000].chunks(256) {
            session.send_audio(frame).unwrap();
        }
        std::thread::sleep(Duration::from_millis(300));
        let uploaded = received.lock().unwrap().len();
        (session.finish_blocking(), uploaded)
    })
    .join()
    .unwrap();

    assert_eq!(transcript.text, "from a thread");
    assert_eq!(transcript.error, None);
    assert_eq!(
        uploaded_before_finish, 10,
        "ten 100 ms frames, no commit yet"
    );
}
