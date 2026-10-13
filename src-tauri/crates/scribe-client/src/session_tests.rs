//! The session state machine against an in-process mock server: no network, no API key.

use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{sleep, timeout};

use crate::mock_server::{
    committed, error_frame, first_sample, is_commit, partial, samples_in, MockServer,
};
use crate::{ScribeConfig, ScribeError, ScribeEvent, ScribeSession, Transcript};

fn config_for(server: &MockServer) -> ScribeConfig {
    let mut config = ScribeConfig::new("test-key");
    config.endpoint = Some(server.endpoint.clone());
    config.finish_timeout = Duration::from_secs(2);
    config.send_timeout = Duration::from_secs(1);
    config
}

/// Starts a session and waits until it is connected, so the test starts from a live socket.
async fn open_with(config: ScribeConfig) -> (ScribeSession, UnboundedReceiver<ScribeEvent>) {
    let (session, mut events) = ScribeSession::start(&Handle::current(), config);
    wait_for(&mut events, |e| *e == ScribeEvent::Ready).await;
    (session, events)
}

async fn open(server: &MockServer) -> (ScribeSession, UnboundedReceiver<ScribeEvent>) {
    open_with(config_for(server)).await
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
async fn non_fatal_error_mid_hold_marks_the_transcript_as_possibly_incomplete() {
    // The final commit is answered fine, but the server complained about audio earlier in
    // the hold, so some of it may never have been transcribed.
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
    assert_eq!(
        transcript.error,
        Some(server_error("input_error", "bad frame"))
    );
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
async fn commit_throttled_is_an_error_because_the_server_refused_audio_we_sent() {
    let reply = error_frame("commit_throttled", "need 0.3s");
    assert_eq!(
        finish_after_server_replies_to_commit_with(reply).await,
        Transcript {
            text: String::new(),
            error: Some(ScribeError::CommitThrottled("need 0.3s".into()))
        }
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
    let (mut session, mut events) = open_with(config).await;
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
    let (mut session, _events) = open_with(config).await;
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

    // The write timeout itself fired, not some other network failure.
    assert_eq!(
        failure,
        ScribeError::Network("sending audio timed out".into())
    );
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
        let (mut session, _events) = ScribeSession::start(&handle, config);
        for frame in vec![0.1f32; 16_000].chunks(256) {
            session.send_audio(frame).unwrap();
        }
        // The frames go out while the key is still held, not at `finish`.
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.lock().unwrap().len() < 10 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let uploaded = received.lock().unwrap().clone();
        (session.finish_blocking(), uploaded)
    })
    .join()
    .unwrap();

    assert_eq!(transcript.text, "from a thread");
    assert_eq!(transcript.error, None);
    assert_eq!(uploaded_before_finish.len(), 10, "ten 100 ms frames");
    assert!(
        !uploaded_before_finish.iter().any(is_commit),
        "no commit yet"
    );
}

/// 36 s of uncommitted audio: past the point where the server has committed by itself.
const PAST_AUTO_COMMIT: usize = 36 * 16_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn throttle_and_close_after_an_auto_commit_return_the_first_segment_with_an_error() {
    // The release came before the server's automatic commit reached us: it answers the
    // hold with that commit, then refuses ours and hangs up.
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("first segment")).await;
                conn.send(error_frame("commit_throttled", "need 0.3s"))
                    .await;
                conn.close(1008, "throttled").await;
                break;
            }
        }
        pending::<()>().await;
    })
    .await;
    let (mut session, _events) = open(&server).await;
    feed(&mut session, PAST_AUTO_COMMIT);

    let transcript = session.finish().await;

    assert_eq!(transcript.text, "first segment");
    assert_eq!(
        transcript.error,
        Some(ScribeError::CommitThrottled("need 0.3s".into()))
    );
    // With an automatic commit possibly in flight the commit frame carries a full
    // minimum commit by itself, so the server can always answer it.
    let frames = server.received.lock().unwrap().clone();
    let commit = frames.last().unwrap();
    assert!(is_commit(commit));
    assert!(samples_in(commit) >= 5_200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_expiring_with_one_commit_when_two_were_due_sets_an_error() {
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("only segment")).await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    feed(&mut session, PAST_AUTO_COMMIT);

    let started = Instant::now();
    let transcript = session.finish().await;

    assert_eq!(transcript.text, "only segment");
    assert_eq!(
        transcript.error,
        Some(ScribeError::Timeout(
            "waiting for the commit after an automatic one"
        ))
    );
    assert!(started.elapsed() >= Duration::from_millis(450));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hold_near_the_auto_commit_point_pays_the_grace_and_is_still_complete() {
    // 34 s: the automatic commit may have fired, but nothing proves it, so one reply
    // after the grace counts as complete.
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("one segment")).await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    feed(&mut session, 34 * 16_000);

    let started = Instant::now();
    let transcript = session.finish().await;

    assert_eq!(transcript.text, "one segment");
    assert_eq!(transcript.error, None);
    assert!(started.elapsed() >= Duration::from_millis(450));
    assert!(started.elapsed() < Duration::from_millis(1_500));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_answer_after_an_auto_commit_completes_the_transcript() {
    // The tail after the automatic commit held no speech: "nothing to transcribe" is the
    // second reply, and the text is complete.
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("whole hold")).await;
                conn.send(error_frame("insufficient_audio_activity", "no speech"))
                    .await;
            }
        }
    })
    .await;
    let (mut session, _events) = open(&server).await;
    feed(&mut session, PAST_AUTO_COMMIT);

    let transcript = session.finish().await;

    assert_eq!(transcript.text, "whole hold");
    assert_eq!(transcript.error, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_frame_with_a_code_becomes_closed_with_that_code_and_reason() {
    let server = MockServer::start(|mut conn| async move {
        conn.recv().await;
        conn.close(1008, "policy violation").await;
        pending::<()>().await;
    })
    .await;
    let (mut session, _events) = open(&server).await;
    session.send_audio(&[0.1; 1600]).unwrap();

    let closed = ScribeError::Closed {
        code: 1008,
        reason: "policy violation".into(),
    };
    let failure = timeout(Duration::from_secs(3), async {
        loop {
            if let Err(error) = session.send_audio(&[0.1; 256]) {
                return error;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the caller learns about the close during the hold");
    assert_eq!(failure, closed);
    assert_eq!(session.finish().await.error, Some(closed));
}

/// Starts against a server that cannot connect, feeds audio and returns what both
/// `send_audio` and `finish` reported.
async fn failure_seen_by_send_audio_and_by_finish(
    config: ScribeConfig,
) -> (ScribeError, Transcript) {
    let (mut session, _events) = ScribeSession::start(&Handle::current(), config);
    // The first call may beat the connect failure; audio fed while connecting is queued.
    let _ = session.send_audio(&[0.1; 1600]);
    let from_send_audio = timeout(Duration::from_secs(3), async {
        loop {
            if let Err(error) = session.send_audio(&[0.1; 256]) {
                return error;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the caller learns about the failed connect during the hold");
    (from_send_audio, session.finish().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_401_on_upgrade_is_fatal_for_send_audio_and_finish() {
    let server = MockServer::start_rejecting(401).await;

    let (from_send, transcript) =
        failure_seen_by_send_audio_and_by_finish(config_for(&server)).await;

    assert!(matches!(from_send, ScribeError::Auth(_)), "{from_send:?}");
    assert_eq!(transcript.text, "");
    assert_eq!(transcript.error, Some(from_send));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_that_never_sends_session_started_times_out_for_send_audio_and_finish() {
    let server = MockServer::start_silent(|conn| async move {
        let _held_open = conn;
        pending::<()>().await
    })
    .await;
    let mut config = config_for(&server);
    config.connect_timeout = Duration::from_millis(300);

    let started = Instant::now();
    let (from_send, transcript) = failure_seen_by_send_audio_and_by_finish(config).await;

    assert_eq!(from_send, ScribeError::Timeout("connecting"));
    assert_eq!(transcript.error, Some(ScribeError::Timeout("connecting")));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_error_frame_before_session_started_is_fatal_for_send_audio_and_finish() {
    let server = MockServer::start_silent(|mut conn| async move {
        conn.send(error_frame("auth_error", "bad key")).await;
        pending::<()>().await;
    })
    .await;

    let (from_send, transcript) =
        failure_seen_by_send_audio_and_by_finish(config_for(&server)).await;

    assert_eq!(from_send, ScribeError::Auth("bad key".into()));
    assert_eq!(transcript.error, Some(ScribeError::Auth("bad key".into())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audio_fed_before_session_started_arrives_afterwards_complete_and_in_order() {
    let early_frame = Arc::new(AtomicBool::new(false));
    let saw_early_frame = early_frame.clone();
    let server = MockServer::start_silent(|mut conn| async move {
        // The client has audio queued but must not send it before the server says hello.
        if timeout(Duration::from_millis(300), conn.recv())
            .await
            .is_ok()
        {
            saw_early_frame.store(true, Ordering::Relaxed);
        }
        conn.send_session_started().await;
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("queued audio")).await;
            }
        }
    })
    .await;
    let (mut session, mut events) = ScribeSession::start(&Handle::current(), config_for(&server));
    // Five frames of rising level, all fed while the socket is still connecting.
    for level in 1..=5 {
        session.send_audio(&vec![0.1 * level as f32; 1600]).unwrap();
    }

    // `finish` straight away: it has to wait for the connect, then commit after the audio.
    let transcript = session.finish().await;

    assert_eq!(transcript.text, "queued audio");
    assert_eq!(transcript.error, None);
    assert!(
        !early_frame.load(Ordering::Relaxed),
        "a frame reached the server before session_started"
    );
    let frames = server.received.lock().unwrap().clone();
    let levels: Vec<i16> = frames[..5].iter().map(first_sample).collect();
    assert!(
        levels.windows(2).all(|pair| pair[0] < pair[1]),
        "{levels:?}"
    );
    assert_eq!(frames[..5].iter().map(samples_in).sum::<usize>(), 8_000);
    assert!(is_commit(&frames[5]));
    assert_eq!(events.recv().await, Some(ScribeEvent::Ready));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_session_during_connect_ends_the_task() {
    let server = MockServer::start_silent(|conn| async move {
        let _held_open = conn;
        pending::<()>().await
    })
    .await;
    let (mut session, mut events) = ScribeSession::start(&Handle::current(), config_for(&server));
    session.send_audio(&[0.1; 1600]).unwrap();
    sleep(Duration::from_millis(100)).await;

    drop(session);

    // The task owns the only event sender, so the stream ends when the task does.
    let ended = timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("the connect is cancelled at once");
    assert_eq!(ended, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_that_dies_without_a_fatal_error_gives_an_error_not_a_clean_transcript() {
    // The server never answers the commit. The runtime running the session is shut down
    // while `finish` waits, which ends the task without it recording anything.
    let server =
        MockServer::start(|mut conn| async move { while conn.recv().await.is_some() {} }).await;
    let session_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (mut session, mut events) =
        ScribeSession::start(session_runtime.handle(), config_for(&server));
    wait_for(&mut events, |e| *e == ScribeEvent::Ready).await;
    session.send_audio(&[0.1; 16_000]).unwrap();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        session_runtime.shutdown_background();
    });

    let started = Instant::now();
    let transcript = session.finish().await;
    stopper.join().unwrap();

    assert_eq!(transcript.text, "");
    assert_eq!(
        transcript.error,
        Some(ScribeError::Closed {
            code: 1006,
            reason: "session task ended".into()
        })
    );
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_auto_commit_cycle_with_a_late_reply_sets_an_error() {
    // Cycle one: the server cuts at 35.7 s but its reply reaches the client only once 36.7 s
    // have been fed, so the client's count of uncommitted audio runs 1 s short in cycle two.
    // Cycle two: the release comes with 35.3 s counted, the real cut (35.7 s) has fired, and
    // our own reply arrives after the grace. Without the lag cushion this looked complete.
    let server = MockServer::start(|mut conn| async move {
        let mut total = 0;
        let mut first_sent = false;
        while let Some(frame) = conn.recv().await {
            total += samples_in(&frame);
            if is_commit(&frame) {
                conn.send(committed("second automatic")).await;
                sleep(Duration::from_millis(800)).await;
                conn.send(committed("tail")).await;
            } else if total >= 587_200 && !first_sent {
                first_sent = true;
                conn.send(committed("first automatic")).await;
            }
        }
    })
    .await;
    let (mut session, mut events) = open(&server).await;
    feed(&mut session, 587_200);
    wait_for(&mut events, |e| matches!(e, ScribeEvent::Committed(_))).await;
    feed(&mut session, 564_800);

    let started = Instant::now();
    let transcript = session.finish().await;

    assert_eq!(transcript.text, "first automatic second automatic");
    assert_eq!(
        transcript.error,
        Some(ScribeError::Timeout(
            "waiting for the commit after an automatic one"
        ))
    );
    assert!(started.elapsed() >= Duration::from_millis(450));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_normal_close_mid_hold_fails_the_next_send_and_finish_keeps_the_text() {
    // The idle close the live server does: code 1000, so it must not read as a clean end.
    let server = MockServer::start(|mut conn| async move {
        let mut total = 0;
        while let Some(frame) = conn.recv().await {
            total += samples_in(&frame);
            if total >= 16_000 {
                conn.send(committed("so far")).await;
                conn.close(1000, "normal closure").await;
                break;
            }
        }
        pending::<()>().await;
    })
    .await;
    let (mut session, mut events) = open(&server).await;
    feed(&mut session, 16_000);
    wait_for(&mut events, |e| matches!(e, ScribeEvent::Committed(_))).await;

    let closed = ScribeError::Closed {
        code: 1000,
        reason: "normal closure".into(),
    };
    let failure = timeout(Duration::from_secs(3), async {
        loop {
            if let Err(error) = session.send_audio(&[0.1; 256]) {
                return error;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the caller learns about the close during the hold");
    assert_eq!(failure, closed);

    let transcript = session.finish().await;
    assert_eq!(transcript.text, "so far");
    assert_eq!(transcript.error, Some(closed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_hold_sends_silence_that_counts_as_audio() {
    let server = MockServer::start(|mut conn| async move {
        while let Some(frame) = conn.recv().await {
            if is_commit(&frame) {
                conn.send(committed("after the pause")).await;
            }
        }
    })
    .await;
    let mut config = config_for(&server);
    config.idle_silence_after = Duration::from_millis(200);
    let (mut session, _events) = open_with(config).await;
    feed(&mut session, 3_200);
    sleep(Duration::from_millis(750)).await;

    let transcript = session.finish().await;

    assert_eq!(transcript.text, "after the pause");
    assert_eq!(transcript.error, None);
    let frames = server.received.lock().unwrap().clone();
    // Two speech frames, then at least two 100 ms silent frames, then the commit.
    assert!(frames[..2].iter().all(|f| first_sample(f) != 0));
    let silent: Vec<_> = frames[2..frames.len() - 1].iter().collect();
    assert!(silent.len() >= 2, "{} silent frames", silent.len());
    assert!(silent
        .iter()
        .all(|f| !is_commit(f) && samples_in(f) == 1600 && first_sample(f) == 0));
    // The silence counted toward the server minimum, so the commit needed no padding.
    assert_eq!(samples_in(frames.last().unwrap()), 0);
}
