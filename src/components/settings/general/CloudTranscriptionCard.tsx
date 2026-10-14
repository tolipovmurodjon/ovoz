import React, { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { commands } from "@/bindings";
import { useSettings } from "../../../hooks/useSettings";
import { SettingsGroup } from "../../ui/SettingsGroup";
import { SettingContainer } from "../../ui/SettingContainer";
import { ToggleSwitch } from "../../ui/ToggleSwitch";
import { Input } from "../../ui/Input";
import { Button } from "../../ui/Button";
import {
  MAIN_LANGUAGE_CHOICES,
  secondaryAfterSwitch,
} from "@/lib/utils/dictationLanguages";

// Each language in its own name, never translated.
const LANGUAGE_NAMES: Record<string, string> = {
  en: "English",
  uz: "Oʻzbek",
  ru: "Русский",
};

const normalizeLanguageCode = (text: string) => text.trim().toLowerCase();

// ISO 639-1 or 639-3. A typo like "uzbek" would be rejected by the server on
// every dictation, which silently turns the cloud engine off.
const isLanguageCode = (code: string) => /^[a-z]{2,3}$/.test(code);

const parseLanguageList = (text: string) => [
  ...new Set(text.split(",").map(normalizeLanguageCode).filter(isLanguageCode)),
];

export const CloudTranscriptionCard: React.FC = () => {
  const { t } = useTranslation();
  const { getSetting, updateSetting, isUpdating } = useSettings();
  // null until the backend answers; the status line stays hidden meanwhile.
  const [keyPresent, setKeyPresent] = useState<boolean | null>(null);
  const [keySaved, setKeySaved] = useState(false);
  const [keyDraft, setKeyDraft] = useState("");
  const [keyError, setKeyError] = useState(false);

  const enabled = getSetting("cloud_stt_enabled") ?? true;
  const language = getSetting("cloud_stt_language") ?? "en";
  const secondaryLanguages = getSetting("cloud_stt_secondary_languages") ?? [
    "uz",
  ];
  const secondaryLanguagesText = secondaryLanguages.join(", ");

  // The input keeps a draft and saves on blur, so typing "en," does not get
  // rewritten under the cursor by the parsed list.
  const [secondaryDraft, setSecondaryDraft] = useState(secondaryLanguagesText);

  useEffect(
    () => setSecondaryDraft(secondaryLanguagesText),
    [secondaryLanguagesText],
  );

  const refreshKeyStatus = () =>
    Promise.all([
      commands.isCloudSttKeyPresent(),
      commands.isCloudSttKeySaved(),
    ])
      .then(([present, saved]) => {
        setKeyPresent(present);
        setKeySaved(saved);
      })
      .catch((error) =>
        console.error("Failed to check the ElevenLabs key:", error),
      );

  useEffect(() => {
    refreshKeyStatus();
  }, []);

  // The key goes one way: typed here, saved in Rust, never shown again.
  const saveKey = async (key: string) => {
    const result = await commands.saveCloudSttApiKey(key);
    setKeyError(result.status === "error");
    if (result.status === "ok") setKeyDraft("");
    await refreshKeyStatus();
  };

  const chooseMainLanguage = (next: string) => {
    if (next === language) return;
    updateSetting("cloud_stt_language", next);
    updateSetting(
      "cloud_stt_secondary_languages",
      secondaryAfterSwitch(language, next, secondaryLanguages),
    );
  };

  const saveSecondaryLanguages = () => {
    const parsed = parseLanguageList(secondaryDraft);
    const parsedText = parsed.join(", ");
    setSecondaryDraft(parsedText);
    if (parsedText !== secondaryLanguagesText) {
      updateSetting("cloud_stt_secondary_languages", parsed);
    }
  };

  const blurOnEnter = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.currentTarget.blur();
    }
  };

  return (
    <SettingsGroup title={t("settings.cloudStt.title")}>
      <ToggleSwitch
        checked={enabled}
        onChange={(value) => updateSetting("cloud_stt_enabled", value)}
        isUpdating={isUpdating("cloud_stt_enabled")}
        label={t("settings.cloudStt.enabled.label")}
        description={t("settings.cloudStt.enabled.description")}
        descriptionMode="tooltip"
        grouped={true}
      />
      <div className="px-4 py-3 space-y-2">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="text-sm font-medium">
              {t("settings.cloudStt.apiKey.title")}
            </div>
            {keyPresent !== null && (
              <div
                className={`text-xs ${keyPresent ? "text-text/70" : "text-error"}`}
              >
                {keySaved
                  ? t("settings.cloudStt.keyStatus.saved")
                  : keyPresent
                    ? t("settings.cloudStt.keyStatus.found")
                    : t("settings.cloudStt.keyStatus.missing")}
              </div>
            )}
          </div>
          {keySaved && (
            <Button variant="secondary" size="sm" onClick={() => saveKey("")}>
              {t("settings.cloudStt.apiKey.remove")}
            </Button>
          )}
        </div>
        <form
          className="flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (keyDraft.trim()) saveKey(keyDraft);
          }}
        >
          <Input
            type="password"
            autoComplete="off"
            spellCheck={false}
            className="flex-1"
            value={keyDraft}
            onChange={(e) => setKeyDraft(e.target.value)}
            placeholder={t("settings.cloudStt.apiKey.placeholder")}
            variant="compact"
          />
          <Button type="submit" size="sm" disabled={!keyDraft.trim()}>
            {t("settings.cloudStt.apiKey.save")}
          </Button>
        </form>
        {keyError && (
          <div className="text-xs text-error">
            {t("settings.cloudStt.apiKey.saveFailed")}
          </div>
        )}
      </div>
      <SettingContainer
        title={t("settings.cloudStt.language.title")}
        description={t("settings.cloudStt.language.description")}
        descriptionMode="tooltip"
        grouped={true}
        disabled={!enabled}
      >
        <div
          role="radiogroup"
          className="flex border-2 border-text rounded-lg overflow-hidden"
        >
          {MAIN_LANGUAGE_CHOICES.map((code) => (
            <button
              key={code || "auto"}
              type="button"
              role="radio"
              aria-checked={language === code}
              disabled={!enabled || isUpdating("cloud_stt_language")}
              onClick={() => chooseMainLanguage(code)}
              className={`px-2.5 py-1 text-xs font-semibold border-e-2 border-text last:border-e-0 transition-colors duration-150 cursor-pointer disabled:cursor-not-allowed ${
                language === code
                  ? "bg-logo-primary text-ink"
                  : "bg-surface hover:bg-text/5"
              }`}
            >
              {code
                ? LANGUAGE_NAMES[code]
                : t("settings.cloudStt.language.auto")}
            </button>
          ))}
        </div>
      </SettingContainer>
      <SettingContainer
        title={t("settings.cloudStt.secondaryLanguages.title")}
        description={t("settings.cloudStt.secondaryLanguages.description")}
        descriptionMode="tooltip"
        grouped={true}
        disabled={!enabled}
      >
        <Input
          type="text"
          className="max-w-40"
          value={secondaryDraft}
          onChange={(e) => setSecondaryDraft(e.target.value)}
          onBlur={saveSecondaryLanguages}
          onKeyDown={blurOnEnter}
          placeholder={t("settings.cloudStt.secondaryLanguages.placeholder")}
          variant="compact"
          disabled={!enabled || isUpdating("cloud_stt_secondary_languages")}
        />
      </SettingContainer>
      <div
        className={`px-4 p-2 text-xs text-mid-gray ${enabled ? "" : "opacity-50"}`}
      >
        {t("settings.cloudStt.fallbackNote")}
      </div>
    </SettingsGroup>
  );
};
