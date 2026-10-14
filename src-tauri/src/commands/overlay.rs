//! Commands the recording overlay (the pill) calls.

use crate::overlay;
use crate::signal_handle;
use crate::tray;
use tauri::AppHandle;

/// The pill's interactive area in CSS pixels of the overlay window. A zero
/// width or height means none: every click passes through.
#[tauri::command]
#[specta::specta]
pub fn set_overlay_hot_area(x: f64, y: f64, width: f64, height: f64) {
    let area = (width > 0.0 && height > 0.0).then_some([x, y, width, height]);
    overlay::set_hot_area(area);
}

/// Starts dictation, or finishes it and pastes, as a press of the shortcut.
#[tauri::command]
#[specta::specta]
pub fn toggle_dictation_from_overlay(app: AppHandle) {
    signal_handle::send_transcription_input(&app, "transcribe", "overlay");
}

#[tauri::command]
#[specta::specta]
pub fn snooze_overlay(app: AppHandle, minutes: u32) {
    overlay::snooze_idle_pill(&app, minutes as u64);
}

#[tauri::command]
#[specta::specta]
pub fn paste_last_transcript(app: AppHandle) {
    tray::paste_last_transcript(&app);
}
