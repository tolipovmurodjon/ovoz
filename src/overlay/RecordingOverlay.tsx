import { listen } from "@tauri-apps/api/event";
import React, { useEffect, useLayoutEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Check, Clipboard, History, Mic, Settings, X } from "lucide-react";
import "./RecordingOverlay.css";
import { commands, events } from "@/bindings";
import type { StreamPhaseEvent } from "@/bindings";
import i18n, { syncLanguageFromSettings } from "@/i18n";
import {
  MAIN_LANGUAGE_CHOICES,
  switchMainLanguage,
} from "@/lib/utils/dictationLanguages";
import { getLanguageDirection } from "@/lib/utils/rtl";

// "idle" is Sadomi's resting pill between dictations (Windows).
type OverlayState =
  | "idle"
  | "recording"
  | "streaming"
  | "transcribing"
  | "processing";

const WAVE_BARS = 9;
// Hover leaves the menu open this long, so a pointer that slips off the edge
// for a moment does not close it.
const MENU_CLOSE_DELAY_MS = 500;
const CANCELLED_TOAST_MS = 1400;

// Short codes shown on the pill's language chip, never translated.
const LANGUAGE_CHIPS: Record<string, string> = {
  en: "EN",
  uz: "UZ",
  ru: "RU",
};

type Tip = "dictate" | "finish" | "cancel" | null;

// Key names as printed on a Windows keyboard, never translated.
const KEY_NAMES: Record<string, string> = {
  ctrl: "Ctrl",
  control: "Ctrl",
  super: "Win",
  meta: "Win",
  alt: "Alt",
  shift: "Shift",
  space: "Space",
};

// "ctrl_left+super_left" -> "Ctrl + Win": the short form Wispr Flow shows.
const shortShortcut = (binding: string) =>
  binding
    .split("+")
    .map((part) => part.trim().replace(/_(left|right)$/, ""))
    .filter(Boolean)
    .map((key) => KEY_NAMES[key] ?? key.charAt(0).toUpperCase() + key.slice(1))
    .join(" + ");

const RecordingOverlay: React.FC = () => {
  const { t } = useTranslation();
  const [state, setState] = useState<OverlayState>("idle");
  // The window is only on screen while shown; this drives the leave animation.
  const [isVisible, setIsVisible] = useState(false);
  const [captureReady, setCaptureReady] = useState(false);
  const [working, setWorking] = useState(false);
  const [hovered, setHovered] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [tip, setTip] = useState<Tip>(null);
  const [cancelledToast, setCancelledToast] = useState(false);
  const [levels, setLevels] = useState<number[]>(Array(WAVE_BARS).fill(0));
  const [position, setPosition] = useState<"top" | "bottom">("bottom");
  const [shortcut, setShortcut] = useState("");
  const [language, setLanguage] = useState("en");
  const [secondary, setSecondary] = useState<string[]>([]);

  const smoothedLevelsRef = useRef<number[]>(Array(16).fill(0));
  const hotRef = useRef<HTMLDivElement>(null);
  const menuCloseTimer = useRef<ReturnType<typeof setTimeout>>();
  const toastTimer = useRef<ReturnType<typeof setTimeout>>();
  const direction = getLanguageDirection(i18n.language);

  const readSettings = async () => {
    try {
      const result = await commands.getAppSettings();
      if (result.status !== "ok") return;
      const settings = result.data;
      setPosition(settings.overlay_position === "top" ? "top" : "bottom");
      setLanguage(settings.cloud_stt_language ?? "en");
      setSecondary(settings.cloud_stt_secondary_languages ?? []);
      const binding = settings.bindings?.transcribe?.current_binding ?? "";
      setShortcut(shortShortcut(binding));
    } catch {
      // Keep the last values if settings cannot be read.
    }
  };

  useEffect(() => {
    readSettings();
    const setup = async () => {
      const unlistenShow = await listen<OverlayState>(
        "show-overlay",
        async (event) => {
          const next = event.payload;
          if (next === "recording" || next === "streaming") {
            setCaptureReady(false);
            smoothedLevelsRef.current = Array(16).fill(0);
            setLevels(Array(WAVE_BARS).fill(0));
          }
          setWorking(next === "transcribing" || next === "processing");
          setMenuOpen(false);
          setTip(null);
          setState(next);
          setIsVisible(true);
          await syncLanguageFromSettings();
          readSettings();
        },
      );
      // The backend sends this when a dictation ends. With the resting pill
      // the window stays, so fall back to idle; otherwise it fades out.
      const unlistenHide = await listen("hide-overlay", () => {
        setState("idle");
        setWorking(false);
        setCaptureReady(false);
        setTip(null);
      });
      const unlistenReady = await listen("recording-ready", () =>
        setCaptureReady(true),
      );
      const unlistenLevel = await listen<number[]>("mic-level", (event) => {
        const smoothed = smoothedLevelsRef.current.map(
          (prev, i) => prev * 0.7 + (event.payload[i] || 0) * 0.3,
        );
        smoothedLevelsRef.current = smoothed;
        setLevels(smoothed.slice(0, WAVE_BARS));
      });
      const unlistenPhase = await events.streamPhaseEvent.listen((event) => {
        const payload: StreamPhaseEvent = event.payload;
        setWorking(payload.phase === "working");
      });
      // From the Rust pointer watcher: the window is click-through, so CSS
      // :hover would miss the moment the pointer leaves.
      const unlistenHover = await listen<boolean>("overlay-hover", (event) =>
        setHovered(event.payload),
      );
      return () => {
        unlistenShow();
        unlistenHide();
        unlistenReady();
        unlistenLevel();
        unlistenPhase();
        unlistenHover();
      };
    };
    const cleanup = setup();
    return () => {
      cleanup.then((fn) => fn());
      clearTimeout(menuCloseTimer.current);
      clearTimeout(toastTimer.current);
    };
  }, []);

  // A menu closes when the pointer has been away for a moment.
  useEffect(() => {
    clearTimeout(menuCloseTimer.current);
    if (!hovered && menuOpen) {
      menuCloseTimer.current = setTimeout(
        () => setMenuOpen(false),
        MENU_CLOSE_DELAY_MS,
      );
    }
    if (!hovered) setTip(null);
  }, [hovered, menuOpen]);

  // Tell Rust where clicks belong. Everything outside passes through.
  useLayoutEffect(() => {
    const el = hotRef.current;
    if (!el) return;
    const report = () => {
      const r = el.getBoundingClientRect();
      commands.setOverlayHotArea(r.left, r.top, r.width, r.height);
    };
    report();
    const observer = new ResizeObserver(report);
    observer.observe(el);
    return () => observer.disconnect();
  }, [state, menuOpen, isVisible]);

  const dictating = state === "recording" || state === "streaming";
  const busy = working || state === "transcribing" || state === "processing";
  const expanded = state === "idle" && (hovered || menuOpen);

  const cancel = () => {
    commands.cancelOperation();
    clearTimeout(toastTimer.current);
    setCancelledToast(true);
    toastTimer.current = setTimeout(
      () => setCancelledToast(false),
      CANCELLED_TOAST_MS,
    );
  };

  const chooseLanguage = async (next: string) => {
    if (next === language) return;
    await switchMainLanguage(language, next, secondary);
    await readSettings();
  };

  const runAndClose = (action: () => void) => () => {
    setMenuOpen(false);
    action();
  };

  // Pointing at the open pill explains the shortcut, as Wispr Flow does.
  const shownTip: Tip = tip ?? (expanded && !menuOpen ? "dictate" : null);
  const tipText =
    shownTip === "dictate"
      ? t("overlay.tip.dictate", { shortcut })
      : shownTip === "finish"
        ? t("overlay.tip.finish")
        : shownTip === "cancel"
          ? t("overlay.tip.cancel")
          : null;

  const waveform = (
    <div className={`wave ${captureReady ? "ready" : "arming"}`}>
      {levels.map((v, i) => (
        <i
          key={i}
          style={{
            height: `${Math.max(3, Math.min(16, 3 + Math.pow(v, 0.7) * 13))}px`,
          }}
        />
      ))}
    </div>
  );

  const menu = menuOpen && (
    <div className="menu" role="menu" onContextMenu={(e) => e.preventDefault()}>
      <div className="menu-label">{t("overlay.menu.language")}</div>
      <div className="menu-langs" role="radiogroup">
        {MAIN_LANGUAGE_CHOICES.map((code) => (
          <button
            key={code || "auto"}
            role="radio"
            aria-checked={language === code}
            className={language === code ? "on" : ""}
            onClick={() => chooseLanguage(code)}
          >
            {code ? LANGUAGE_CHIPS[code] : t("overlay.menu.auto")}
          </button>
        ))}
      </div>
      <div className="menu-rule" />
      <button
        role="menuitem"
        onClick={runAndClose(() => commands.pasteLastTranscript())}
      >
        <Clipboard size={15} />
        {t("overlay.menu.pasteLast")}
      </button>
      <button
        role="menuitem"
        onClick={runAndClose(() => commands.showMainWindowSection("history"))}
      >
        <History size={15} />
        {t("overlay.menu.history")}
      </button>
      <button
        role="menuitem"
        onClick={runAndClose(() => commands.showMainWindowSection("general"))}
      >
        <Settings size={15} />
        {t("overlay.menu.settings")}
      </button>
      <div className="menu-rule" />
      <button
        role="menuitem"
        className="quiet"
        onClick={runAndClose(() => commands.snoozeOverlay(60))}
      >
        {t("overlay.menu.hideHour")}
      </button>
    </div>
  );

  let body: React.ReactNode;
  let pillClass: string;
  if (cancelledToast && state === "idle") {
    pillClass = "toast";
    body = <span className="toast-text">{t("overlay.cancelled")}</span>;
  } else if (busy) {
    pillClass = "busy";
    body = (
      <div className="dots" aria-label={t("overlay.processing")}>
        <i />
        <i />
        <i />
      </div>
    );
  } else if (dictating) {
    pillClass = "live";
    body = (
      <>
        <button
          className="round ghost"
          aria-label={t("overlay.tip.cancel")}
          onClick={cancel}
          onMouseEnter={() => setTip("cancel")}
          onMouseLeave={() => setTip(null)}
        >
          <X size={14} strokeWidth={2.6} />
        </button>
        {waveform}
        <button
          className="round go"
          aria-label={t("overlay.tip.finish")}
          onClick={() => commands.toggleDictationFromOverlay()}
          onMouseEnter={() => setTip("finish")}
          onMouseLeave={() => setTip(null)}
        >
          <Check size={15} strokeWidth={3} />
        </button>
      </>
    );
  } else if (expanded) {
    pillClass = "open";
    body = (
      <>
        <button
          className="round go"
          aria-label={t("overlay.tip.dictate", { shortcut })}
          onClick={() => commands.toggleDictationFromOverlay()}
        >
          <Mic size={15} strokeWidth={2.4} />
        </button>
        <button
          className="chip"
          aria-haspopup="menu"
          aria-expanded={menuOpen}
          onClick={() => setMenuOpen((open) => !open)}
        >
          {language
            ? (LANGUAGE_CHIPS[language] ?? language)
            : t("overlay.menu.auto")}
        </button>
      </>
    );
  } else {
    pillClass = "rest";
    body = null;
  }

  return (
    <div
      dir={direction}
      className={`stage ${position} ${isVisible ? "show" : ""}`}
    >
      <div
        ref={hotRef}
        className="hot"
        onContextMenu={(e) => {
          e.preventDefault();
          if (state === "idle") setMenuOpen((open) => !open);
        }}
      >
        {menu}
        {tipText && !menuOpen && <div className="tip">{tipText}</div>}
        <div className={`pill ${pillClass}`}>{body}</div>
      </div>
    </div>
  );
};

export default RecordingOverlay;
